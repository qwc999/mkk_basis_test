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
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, create_broker
from app.outbox import publish_outbox
from app.processing import PaymentNotFound, Simulator, process_payment, simulate_payment

logger = logging.getLogger(__name__)
TECHNICAL_ERROR_PAUSE = 2.0


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID


async def consume_payment(message, sessions, simulator: Simulator = simulate_payment) -> None:
    try:
        event = PaymentEvent.model_validate_json(message.body)
    except ValidationError:
        logger.warning("Invalid payment event discarded")
        await message.reject(requeue=False)
        return
    try:
        await process_payment(sessions, event.payment_id, simulator)
    except PaymentNotFound:
        logger.warning("Payment event discarded: payment_id=%s does not exist", event.payment_id)
        await message.reject(requeue=False)
        return
    except Exception as exc:
        # No ACK on technical failure; with prefetch=1 this pauses consumption.
        logger.error("Payment processing failed (%s); requeue after pause", type(exc).__name__)
        await asyncio.sleep(TECHNICAL_ERROR_PAUSE)
        await message.nack(requeue=True)
        return
    await message.ack()


async def raw_body(message):
    # Validate malformed JSON ourselves so it is explicitly rejected, not left unacked.
    return message.body


async def run_worker(
    settings: WorkerSettings, stop: asyncio.Event, simulator: Simulator = simulate_payment
) -> None:
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    broker = create_broker(settings)
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
            await consume_payment(message, sessions, simulator)
        except Exception as exc:
            logger.error("Consumer transport failed (%s); stopping worker", type(exc).__name__)
            if not failed.done():
                failed.set_result(None)

    try:
        while not stop.is_set():
            try:
                async with asyncio.timeout(10):
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
    for name in ("aio_pika", "aiormq"):
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
