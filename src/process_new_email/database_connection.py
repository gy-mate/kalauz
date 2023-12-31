import logging
import os
from typing import Final

from sqlalchemy import Connection, Engine, create_engine, URL, text


def _create_database_if_not_exists(connection: Connection, database_name: str):
    connection.execute(text(f"create database if not exists {database_name}"))


class Database:
    def __init__(self, database_name: str) -> None:
        self.logger = logging.getLogger(__name__)

        self._DRIVERNAME: Final = "mysql+mysqlconnector"
        self._HOST: Final = "localhost"
        self._PORT: Final = 3306
        self._USERNAME: Final = "root"
        self._PASSWORD: Final = self._get_password()

        self.engine = self._connect_to_database_server()

        self._connect_to_database(database_name)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _get_password(self) -> str:
        try:
            database_password = os.getenv("DATABASE_PASSWORD")
            if not database_password:
                raise ValueError("No password provided for the database server!")
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
            self.logger.info("Successfully connected to the database server!")

    def _connect_to_database(self, database_name: str) -> None:
        with self.engine.begin() as connection:
            _create_database_if_not_exists(connection, database_name)

        self._connect_to_created_database(database_name)

    def _connect_to_created_database(self, database_name: str) -> None:
        try:
            database_url = URL.create(
                drivername=self._DRIVERNAME,
                host=self._HOST,
                port=self._PORT,
                username=self._USERNAME,
                password=self._PASSWORD,
                database=database_name,
            )
            self.engine = create_engine(database_url)
        finally:
            self.logger.info(
                f"Successfully connected to the {database_name} table of the database server!"
            )
