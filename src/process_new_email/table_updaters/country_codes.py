from abc import ABC
from io import BytesIO
from typing import Final, final

from lxml import etree

# noinspection PyProtectedMember
from lxml.etree import _Element
from pandas import DataFrame
import pandas as pd
from requests import HTTPError
from sqlalchemy import text

# TODO: remove comment in the line below when mypy library stubs are created for the module imported
from varname import nameof  # type: ignore
import zipfile

from src.process_new_email.table_updaters.common import HelperTableUpdater


def _uic_code_not_assigned(values: tuple[str, str, str]) -> bool:
    return values[1] is None


def _swap_name(name: str) -> str:
    return " ".join(name.split(", ")[::-1])


@final
class CountryCodesUpdater(HelperTableUpdater, ABC):
    def __init__(self, database, data_url, xsd_url: str) -> None:
        super().__init__(database, data_url)

        self._data_to_validate: _Element = _Element()
        self.data: DataFrame | None = None
        self.namespace: dict | None = None

        self._TAG_ROW: Final = "ns1:Country"
        self._PATH_ROW: Final = f".//{self._TAG_ROW}"
        self._TAG_BEGINNING_COLUMN: Final = f"{self._TAG_ROW}_"

        self.XSD_URL: Final = xsd_url
        try:
            self.xsd_to_process: Final[BytesIO] = super().download_data(xsd_url)
            self._xsd: Final[etree.XMLSchema] = self._process_xsd()
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _process_xsd(self) -> etree.XMLSchema:
        xsd_unzipped = self._unzip(self.xsd_to_process)
        return etree.XMLSchema(etree.parse(xsd_unzipped))

    def _unzip(self, xsd: BytesIO) -> BytesIO:
        with zipfile.ZipFile(xsd, "r") as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self.XSD_URL} has more than one file in it!"
                )
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))

    def process_data(self) -> None:
        self._data_to_validate = etree.parse(self._data_to_process).getroot()
        self.logger.info(
            f"Data downloaded from {self.DATA_URL} successfully processed!"
        )

        self.namespace = self._data_to_validate.nsmap

        try:
            self._validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def _validate_data(self) -> None:
        if self._xsd and self._xsd.validate(self._data_to_validate) is False:
            raise ValueError(
                f"The .xml file downloaded from {self.DATA_URL} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.XSD_URL}!"
            )
        self.logger.info(
            f"Data downloaded from {self.DATA_URL} successfully validated!"
        )

    def store_data(self) -> None:
        self._create_table_if_not_exists()
        self._add_data()

    def _create_table_if_not_exists(self) -> None:
        query = """
        create table if not exists countries (
            ISO_code varchar(2),
            UIC_code int(2),
            name_EN varchar(255),
            name_FR varchar(255),
            name_DE varchar(255),
            primary key (UIC_code)
        )
        """
        with self.database.engine.begin() as connection:
            connection.execute(text(query))
        self.logger.info("Table `countries` sucessfully created (if needed)!")

    def _add_data(self) -> None:
        # TODO: report wrong documentation URL of pd.read_xml() to JetBrains or pandas developers
        self.data = pd.read_xml(
            path_or_buffer=self._data_to_process,
            xpath=self._PATH_ROW,
            namespaces=self.namespace,
        )
        self.data.rename(
            columns={
                "Country_ISO_Code": "ISO_code",
                "Country_UIC_Code": "UIC_code",
                "Country_Name_EN": "name_EN",
                "Country_Name_FR": "name_FR",
                "Country_Name_DE": "name_DE",
            },
            inplace=True,
        )
        self.data.dropna(
            subset=[nameof(self.data.UIC_code)],
            inplace=True,
        )

        columns_to_swap = [
            nameof(self.data.name_EN),
            nameof(self.data.name_FR),
            nameof(self.data.name_DE),
        ]
        self.data[nameof(self.data.name_EN)] = self.data[
            nameof(self.data.name_EN)
        ].apply(lambda x: x.rstrip())
        for column_name in columns_to_swap:
            self.data[column_name] = self.data[column_name].apply(
                lambda x: _swap_name(x)
            )

        with self.database.engine.begin() as connection:
            for index, row in self.data.iterrows():
                query = """
                insert ignore into countries (
                    ISO_code,
                    UIC_code,
                    name_EN,
                    name_FR,
                    name_DE
                )
                values (:ISO_code, :UIC_code, :name_EN, :name_FR, :name_DE)
                """

                connection.execute(
                    text(query),
                    {
                        nameof(row.ISO_code): row.ISO_code,
                        nameof(row.UIC_code): row.UIC_code,
                        nameof(row.name_EN): row.name_EN,
                        nameof(row.name_FR): row.name_FR,
                        nameof(row.name_DE): row.name_DE,
                    },
                )

        self.logger.info(
            f"Successfully added new data downloaded from {self.DATA_URL} to table `countries`!"
        )
