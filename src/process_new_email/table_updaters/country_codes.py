from abc import ABC
from io import BytesIO
from typing import Final, final

from lxml import etree

# TODO: find workaround of using a protected member below
# noinspection PyProtectedMember
from lxml.etree import _Element
from pandas import DataFrame
import pandas as pd
from requests import HTTPError
from sqlalchemy import text
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
        self.data: DataFrame = pd.DataFrame()
        self.namespace: dict = {}

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
        try:
            self._validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise

        self.data = self._read_data_from_xml()

        self._rename_columns()
        self._drop_unnecessary_columns()
        self._swap_names_separated_with_comma()

    def _validate_data(self) -> None:
        self._data_to_validate = etree.parse(self._data_to_process).getroot()
        self.logger.info(f"Data downloaded from {self.DATA_URL} successfully parsed!")

        self.namespace = self._data_to_validate.nsmap

        if not self._is_data_valid():
            raise ValueError(
                f"The .xml file downloaded from {self.DATA_URL} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.XSD_URL}!"
            )
        self.logger.info(
            f"Data downloaded from {self.DATA_URL} successfully validated!"
        )

    def _is_data_valid(self):
        return self._xsd and self._xsd.validate(self._data_to_validate)

    def _read_data_from_xml(self):
        # TODO: report wrong documentation URL of pd.read_xml() to JetBrains or pandas developers
        return pd.read_xml(
            path_or_buffer=self._data_to_process,
            xpath=self._PATH_ROW,
            namespaces=self.namespace,
        )

    def _rename_columns(self):
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

    def _drop_unnecessary_columns(self):
        self.data.dropna(
            subset=["UIC_code"],
            inplace=True,
        )

    def _swap_names_separated_with_comma(self):
        columns_to_swap = [
            "name_EN",
            "name_FR",
            "name_DE",
        ]
        self.data["name_EN"] = self.data["name_EN"].apply(lambda x: x.rstrip())
        for column_name in columns_to_swap:
            self.data[column_name] = self.data[column_name].apply(
                lambda x: _swap_name(x)
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
                        "ISO_code": row.ISO_code,
                        "UIC_code": row.UIC_code,
                        "name_EN": row.name_EN,
                        "name_FR": row.name_FR,
                        "name_DE": row.name_DE,
                    },
                )

        self.logger.info(
            f"Successfully added new data downloaded from {self.DATA_URL} to table `countries`!"
        )
