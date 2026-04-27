from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


engine = None
SessionLocal = None


def configure_engine(database_url: str) -> None:
    global engine, SessionLocal
    engine = create_async_engine(database_url, future=True, echo=False)
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


configure_engine(settings.database_url)


async def get_db() -> AsyncIterator[AsyncSession]:
    if SessionLocal is None:
        raise RuntimeError("Database session factory is not configured")
    async with SessionLocal() as session:
        yield session
