import asyncio
import logging
import signal
from contextlib import suppress
from uuid import UUID

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel, ConfigDict, ValidationError

from app.config import WorkerSettings, load_settings
from app.db import create_engine, create_session_factory
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, create_broker, declare_topology
from app.outbox import publish_outbox
from app.processing import PaymentNotFound, Simulator, process_payment, simulate_payment
from app.retries import retry_or_dead_letter
from app.webhooks import create_http_client, deliver_webhook

logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID


async def consume_payment(
    message,
    sessions,
    broker,
    http_client,
    simulator: Simulator = simulate_payment,
    *,
    publish_timeout: float = 5,
) -> None:
    attempt = message.headers.get("x-attempt", 1)
    if type(attempt) is not int or not 1 <= attempt <= 3:
        await retry_or_dead_letter(
            message, broker, attempt=None, reason="invalid_message", publish_timeout=publish_timeout
        )
        return
    try:
        event = PaymentEvent.model_validate_json(message.body)
    except ValidationError:
        await retry_or_dead_letter(
            message,
            broker,
            attempt=attempt,
            reason="invalid_message",
            publish_timeout=publish_timeout,
        )
        return
    reason = "processing_error"
    try:
        await process_payment(sessions, event.payment_id, simulator)
        reason = "webhook_error"
        await deliver_webhook(sessions, event.payment_id, http_client)
    except PaymentNotFound:
        await retry_or_dead_letter(
            message,
            broker,
            attempt=attempt,
            reason="payment_not_found",
            publish_timeout=publish_timeout,
        )
        return
    except Exception:
        await retry_or_dead_letter(
            message, broker, attempt=attempt, reason=reason, publish_timeout=publish_timeout
        )
        return
    await message.ack()


async def raw_body(message):
    # Keep malformed JSON available for explicit validation and transfer to DLQ.
    return message.body


async def run_worker(
    settings: WorkerSettings, stop: asyncio.Event, simulator: Simulator = simulate_payment
) -> None:
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    broker = create_broker(settings)
    http_client = create_http_client()
    failed = asyncio.get_running_loop().create_future()
    sender = None
    stopped = asyncio.create_task(stop.wait())

    @broker.subscriber(
        PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=raw_body,
        no_reply=True,
    )
    async def handle(message: RabbitMessage):
        try:
            await consume_payment(
                message,
                sessions,
                broker,
                http_client,
                simulator,
                publish_timeout=settings.outbox_publish_timeout,
            )
        except Exception as exc:
            logger.error("Consumer transport failed (%s); stopping worker", type(exc).__name__)
            if not failed.done():
                failed.set_result(None)

    try:
        while not stop.is_set():
            try:
                async with asyncio.timeout(10):
                    await broker.connect()
                    await declare_topology(broker)
                    await broker.start()
                break
            except (AMQPException, ChannelInvalidStateError, OSError, TimeoutError) as exc:
                logger.warning("RabbitMQ connection failed (%s); will retry", type(exc).__name__)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=2)
                except TimeoutError:
                    pass
        if stop.is_set():
            return
        logger.info("Payment worker started")
        sender = asyncio.create_task(
            publish_outbox(sessions, broker, settings), name="outbox-sender"
        )
        done, _ = await asyncio.wait({sender, stopped, failed}, return_when=asyncio.FIRST_COMPLETED)
        if failed in done:
            raise RuntimeError("Consumer transport stopped")
        if sender in done:
            await sender
            raise RuntimeError("Outbox sender stopped unexpectedly")
    finally:
        if sender is not None:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
        stopped.cancel()
        with suppress(asyncio.CancelledError):
            await stopped
        failed.cancel()
        try:
            await broker.stop()
        finally:
            try:
                await http_client.aclose()
            finally:
                await engine.dispose()
        logger.info("Payment worker stopped")


async def serve(settings: WorkerSettings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await run_worker(settings, stop)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Transport libraries can include connection URLs in reconnect/exception logs.
    for name in ("aio_pika", "aiormq", "httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    try:
        settings = load_settings(WorkerSettings)
    except RuntimeError as exc:
        logger.critical("%s", exc)
        raise SystemExit(1) from None
    try:
        asyncio.run(serve(settings))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logger.critical(
            "Worker terminated (%s): check configuration and infrastructure", type(exc).__name__
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
