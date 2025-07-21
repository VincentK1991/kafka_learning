from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, AsyncTransaction

from shared.config import Config


class Neo4jConnector:
    _driver: AsyncDriver | None = None

    def __init__(
        self,
        uri: str = Config.NEO4J_URI,
        user: str = Config.NEO4J_USERNAME,
        password: str = Config.NEO4J_PASSWORD,
    ) -> None:
        self.uri = uri
        self.user = user
        self.password = password

    async def connect(self) -> None:
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(
                self.uri, auth=(self.user, self.password)
            )
        await self._driver.verify_connectivity()

    async def close(self) -> None:
        if self._driver is not None:
            await self._driver.close()
            self._driver = None

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        if self._driver is None:
            await self.connect()

        session: AsyncSession = self._driver.session()
        try:
            yield session
        finally:
            await session.close()

    async def execute_query(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        async with self.get_session() as session:
            result = await session.run(query, parameters)
            return [record.data() async for record in result]

    async def execute_transaction(
        self, tx_function: Callable[[AsyncTransaction], Awaitable[Any]]
    ) -> Any:
        async with self.get_session() as session:
            return await session.write_transaction(tx_function)


@asynccontextmanager
async def get_neo4j_connector() -> Neo4jConnector:
    connector = Neo4jConnector(
        Config.NEO4J_URI, Config.NEO4J_USERNAME, Config.NEO4J_PASSWORD
    )
    try:
        await connector.connect()
        yield connector
    finally:
        await connector.close()
