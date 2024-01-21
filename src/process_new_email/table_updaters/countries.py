from io import BytesIO
from typing import Final, final

from lxml import etree

# TODO: report mandatory usage of protected member to lxml developers at https://bugs.launchpad.net/lxml
#  https://github.com/lxml/lxml/blob/a4a78214506409e5bbb6c4249cac0c0ca6479d3e/src/lxml/etree.pyx#L1877
#  https://github.com/lxml/lxml/blob/a4a78214506409e5bbb6c4249cac0c0ca6479d3e/src/lxml/etree.pyx#L3166
# noinspection PyProtectedMember
from lxml.etree import _Element
import pandas as pd
from requests import HTTPError
from sqlalchemy import Column, MetaData, SmallInteger, String, Table, text
import zipfile

from src.process_new_email.table_updaters.common import UICTableUpdater


def _uic_code_not_assigned(values: tuple[str, str, str]) -> bool:
    return values[1] is None


def _swap_name(name: str) -> str:
    return " ".join(name.split(", ")[::-1])


@final
class CountriesUpdater(UICTableUpdater):
    TABLE_NAME = "countries"
    database_metadata = MetaData()

    table = Table(
        TABLE_NAME,
        database_metadata,
        Column(name="code_iso", type_=String(2), nullable=False, index=True),
        Column(name="code_uic", type_=SmallInteger, nullable=False, primary_key=True),
        Column(name="name_en", type_=String(255), nullable=False),
        Column(name="name_fr", type_=String(255)),
        Column(name="name_de", type_=String(255)),
    )

    def __init__(self) -> None:
        super().__init__()

        self._data_to_validate: _Element = NotImplemented
        self.namespace: dict = NotImplemented

        self.DATA_URL = f"{self.DATA_BASE_URL}322"
        self._TAG_ROW: Final = "ns1:Country"
        self._PATH_ROW: Final = f".//{self._TAG_ROW}"
        self._TAG_BEGINNING_COLUMN: Final = f"{self._TAG_ROW}_"
        self.XSD_URL: Final = f"{self.DATA_BASE_URL}320"

        self._data_to_process = self.get_data(self.DATA_URL)

        try:
            self.xsd_to_process: Final = self.get_data(self.XSD_URL)
            self._xsd: Final[etree.XMLSchema] = self._process_xsd()
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _process_xsd(self) -> etree.XMLSchema:
        xsd_unzipped = self._unzip(self.xsd_to_process)
        return etree.XMLSchema(etree.parse(xsd_unzipped))

    def _unzip(self, xsd: bytes) -> BytesIO:
        with zipfile.ZipFile(BytesIO(xsd), "r") as zipped_file:
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
        self._data_to_validate = etree.parse(BytesIO(self._data_to_process)).getroot()
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
                "Country_ISO_Code": "code_iso",
                "Country_UIC_Code": "code_uic",
                "Country_Name_EN": "name_en",
                "Country_Name_FR": "name_fr",
                "Country_Name_DE": "name_de",
            },
            inplace=True,
        )

    def _drop_unnecessary_columns(self):
        self.data.dropna(
            subset=["code_uic"],
            inplace=True,
        )

    def _swap_names_separated_with_comma(self):
        columns_to_swap = [
            "name_en",
            "name_fr",
            "name_de",
        ]
        self.data["name_en"] = self.data["name_en"].apply(lambda x: x.rstrip())
        for column_name in columns_to_swap:
            self.data[column_name] = self.data[column_name].apply(
                lambda x: _swap_name(x)
            )

    def _create_table_if_not_exists(self) -> None:
        with self.database.engine.begin() as connection:
            self.table.create(
                bind=connection,
                checkfirst=True,
            )
        self.logger.info("Table `countries` sucessfully created (if needed)!")

    def _add_data(self) -> None:
        with self.database.engine.begin() as connection:
            for index, row in self.data.iterrows():
                query = """
                insert ignore into countries (
                    code_iso,
                    code_uic,
                    name_en,
                    name_fr,
                    name_de
                )
                values (
                    :code_iso,
                    :code_uic,
                    :name_en,
                    :name_fr,
                    :name_de
                )
                """
                connection.execute(
                    text(query),
                    row.to_dict(),
                )

        self.logger.info(
            f"Successfully added new data downloaded from {self.DATA_URL} to table `countries`!"
        )
