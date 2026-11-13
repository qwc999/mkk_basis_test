from types import TracebackType
from typing import Self

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.errors import StorageError
from app.application.ports import UnitOfWorkFactory
from app.infrastructure.database.repositories import (
    SqlAlchemyOutboxRepository,
    SqlAlchemyPaymentRepository,
)


class SqlAlchemyUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    async def __aenter__(self) -> Self:
        self.session = self.sessions()
        await self.session.begin()
        self.payments = SqlAlchemyPaymentRepository(self.session)
        self.outbox = SqlAlchemyOutboxRepository(self.session)
        return self

    async def commit(self) -> None:
        try:
            await self.session.commit()
        except SQLAlchemyError as exc:
            raise StorageError from exc

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.session.rollback()
        except SQLAlchemyError as rollback_error:
            raise StorageError from rollback_error
        finally:
            await self.session.close()
        if isinstance(exc, SQLAlchemyError):
            raise StorageError from exc


def create_uow_factory(sessions: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    return lambda: SqlAlchemyUnitOfWork(sessions)
