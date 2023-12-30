import logging
import os

import mysql.connector
from mysql.connector import CMySQLConnection, MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection
from sqlalchemy import Engine, create_engine, URL


class DatabaseServer:
    def __init__(self, database_name: str) -> None:
        self.logger = logging.getLogger(__name__)

        self.database_engine = self._connect_to_database_server()

        self._connect_to_database(database_name)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _connect_to_database_server(
        self,
    ) -> Engine:
        database_url = URL.create(
            drivername="mysql+mysql-connector-python",
            host="localhost",
            port=3306,
            username="root",
            password=os.getenv("DATABASE_PASSWORD"),
        )
        try:
            return create_engine(database_url)
        finally:
            self.logger.info("Successfully connected to the database server!")

    def _connect_to_database(self, database_name: str) -> None:
        self.cursor.execute("show databases")
        existing_databases = [database[0] for database in self.cursor.fetchall()]

        if database_name not in existing_databases:
            self._create_database(database_name)
        self.database_engine.close()

        self._connect_to_created_database(database_name)

    def _create_database(self, database_name: str) -> None:
        self.cursor.execute("create database " + database_name)
        self.database_engine.commit()

    def _connect_to_created_database(self, database_name: str) -> None:
        self.database_engine = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv("DATABASE_PASSWORD"),
            database=database_name,
        )
        self.cursor = self.database_engine.cursor()
