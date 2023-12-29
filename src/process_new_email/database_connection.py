import logging
import os

import mysql.connector
from mysql.connector import CMySQLConnection, MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class DatabaseServer:
    def __init__(self, database_name: str) -> None:
        self.logger = logging.getLogger(__name__)

        self.connection = self._connect_to_server()
        self.cursor = self.connection.cursor()

        self._connect_to_database(database_name)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _connect_to_server(
        self,
    ) -> PooledMySQLConnection | MySQLConnection | CMySQLConnection:
        try:
            return mysql.connector.connect(
                host="localhost", user="root", password=os.getenv("DATABASE_PASSWORD")
            )
        except mysql.connector.errors.ProgrammingError:
            self.logger.critical("Cannot connect to the database server!")
            raise
        finally:
            self.logger.info("Successfully connected to the database server!")

    def _connect_to_database(self, database_name: str) -> None:
        self.cursor.execute("show databases")
        existing_databases = [database[0] for database in self.cursor.fetchall()]

        if database_name not in existing_databases:
            self._create_database(database_name)
        self.connection.close()

        self._connect_to_created_database(database_name)

    def _create_database(self, database_name: str) -> None:
        self.cursor.execute("create database " + database_name)
        self.connection.commit()

    def _connect_to_created_database(self, database_name: str) -> None:
        self.connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv("DATABASE_PASSWORD"),
            database=database_name,
        )
        self.cursor = self.connection.cursor()
