import asyncio
import logging
import signal
from contextlib import suppress

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage

from app.application.consumer import consume_payment
from app.application.outbox import publish_outbox
from app.application.ports import PaymentGateway
from app.config import WorkerSettings, load_settings
from app.infrastructure.database.base import create_engine, create_session_factory
from app.infrastructure.database.uow import create_uow_factory
from app.infrastructure.gateway import SimulatedPaymentGateway
from app.infrastructure.messaging import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    RabbitEventPublisher,
    create_broker,
    declare_topology,
)
from app.infrastructure.webhooks import HttpxWebhookSender, create_http_client

logger = logging.getLogger(__name__)


async def raw_body(message):
    # Keep malformed JSON available for explicit validation and transfer to DLQ.
    return message.body


async def run_worker(
    settings: WorkerSettings, stop: asyncio.Event, gateway: PaymentGateway | None = None
) -> None:
    engine = create_engine(settings)
    uows = create_uow_factory(create_session_factory(engine))
    broker = create_broker(settings)
    http_client = create_http_client()
    webhook_sender = HttpxWebhookSender(http_client)
    publisher = RabbitEventPublisher(broker, settings.outbox_publish_timeout)
    gateway = gateway if gateway is not None else SimulatedPaymentGateway()
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
            await consume_payment(message, uows, publisher, webhook_sender, gateway)
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
            publish_outbox(uows, publisher, settings.outbox_poll_interval), name="outbox-sender"
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
