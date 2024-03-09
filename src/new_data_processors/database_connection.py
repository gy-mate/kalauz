import logging
import os
from typing import Final

from sqlalchemy import Engine, create_engine, URL, inspect, text
from sqlalchemy.sql.ddl import CreateSchema

from src.singleton import Singleton

# future: swith to SQLAlchemy ORM when https://youtrack.jetbrains.com/issue/PY-4536 is fixed


class Database(metaclass=Singleton):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

        self._DRIVERNAME: Final = "mysql+mysqlconnector"
        self._HOST: Final = "localhost"
        self._PORT: Final = 3306
        self._USERNAME: Final = "root"
        self._PASSWORD: Final = self._get_password()
        self.DATABASE_NAME: Final = "kalauz"

        self.engine = self._connect_to_database_server()
        self.engine = self._connect_to_database()

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _get_password(self) -> str:
        try:
            database_password = os.getenv("DATABASE_PASSWORD")
            if not database_password:
                raise ValueError(
                    "No password found in the .env file for the database server!"
                )
        except ValueError as error:
            self.logger.critical(error)
            raise
        return database_password

    def _connect_to_database_server(self) -> Engine:
        try:
            database_url = URL.create(
                drivername=self._DRIVERNAME,
                host=self._HOST,
                port=self._PORT,
                username=self._USERNAME,
                password=self._PASSWORD,
            )
            return create_engine(database_url)
        finally:
            self.logger.debug("Successfully connected to the database server!")

    def _connect_to_database(self) -> Engine:
        if not inspect(self.engine).has_schema(self.DATABASE_NAME):
            with self.engine.begin() as connection:
                connection.execute(CreateSchema(self.DATABASE_NAME))

        return self._connect_to_created_database()

    def _connect_to_created_database(self) -> Engine:
        try:
            database_url = URL.create(
                drivername=self._DRIVERNAME,
                host=self._HOST,
                port=self._PORT,
                username=self._USERNAME,
                password=self._PASSWORD,
                database=self.DATABASE_NAME,
            )
            return create_engine(database_url)
        finally:
            self.logger.debug(
                f"Successfully connected to the {self.DATABASE_NAME} database of the server!"
            )
