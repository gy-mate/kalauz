from io import BytesIO
from typing import ClassVar, Final, final

from lxml import etree

# future: report mandatory usage of protected member to lxml developers at https://bugs.launchpad.net/lxml
#  https://github.com/lxml/lxml/blob/a4a78214506409e5bbb6c4249cac0c0ca6479d3e/src/lxml/etree.pyx#L1877
#  https://github.com/lxml/lxml/blob/a4a78214506409e5bbb6c4249cac0c0ca6479d3e/src/lxml/etree.pyx#L3166
# noinspection PyProtectedMember
from lxml.etree import XMLSchema, XMLSyntaxError, _Element
from pandas import DataFrame
import pandas as pd
from requests import HTTPError
from sqlalchemy import Column, MetaData, SmallInteger, String, Table, text
from zipfile import ZipFile

from src.new_data_processors.common import UICTableUpdater


def _uic_code_not_assigned(values: tuple[str, str, str]) -> bool:
    return values[1] is None


def _swap_name(name: str) -> str:
    return " ".join(name.split(", ")[::-1])


@final
class CountriesUpdater(UICTableUpdater):
    TABLE_NAME: ClassVar[str] = "countries"
    database_metadata: ClassVar[MetaData] = MetaData()

    table: ClassVar[Table] = Table(
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

        self.DATA_URL = f"{self.DATA_BASE_URL}3984"
        self._TAG_ROW: Final = "ns1:Country"
        self._PATH_ROW: Final = f".//{self._TAG_ROW}"
        self._TAG_BEGINNING_COLUMN: Final = f"{self._TAG_ROW}_"
        self.XSD_URL: Final = f"{self.DATA_BASE_URL}320"

        self._data_to_process = self.get_data(self.DATA_URL)

        try:
            self.xsd_to_process: Final = self.get_data(self.XSD_URL)
            self._xsd: Final[etree.XMLSchema] = self.process_xsd()
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def process_xsd(self) -> XMLSchema:
        xsd_unzipped = self.unzip(self.xsd_to_process)
        return XMLSchema(etree.parse(xsd_unzipped))

    def unzip(self, xsd: bytes) -> BytesIO:
        with ZipFile(BytesIO(xsd), "r") as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self.XSD_URL} has more than one file in it!"
                )
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))

    def process_data(self) -> None:
        try:
            self.validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise

        self.data = self.read_data_from_xml()

        self.rename_columns_manually()
        self.drop_unnecessary_columns()
        self.swap_names_separated_with_comma()

    def validate_data(self) -> None:
        try:
            parsed_data = etree.parse(BytesIO(self._data_to_process))
            self.logger.debug(
                f"Data downloaded from {self.DATA_URL} successfully parsed!"
            )
            self._data_to_validate = parsed_data.getroot()
        except XMLSyntaxError:
            self._data_to_process = self.remove_first_line(self._data_to_process)
            self.validate_data()

        self.namespace = self._data_to_validate.nsmap

        if not self.is_data_valid():
            raise ValueError(
                f"The .xml file downloaded from {self.DATA_URL} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.XSD_URL}!"
            )
        self.logger.debug(
            f"Data downloaded from {self.DATA_URL} successfully validated!"
        )

    def remove_first_line(self, data: bytes) -> bytes:
        try:
            lines = data.split(b"\n", 1)
            return lines[1]
        finally:
            self.logger.debug(
                f"First line removed from data downloaded from {self.DATA_URL}!"
            )

    def is_data_valid(self) -> bool:
        if self._xsd and self._xsd.validate(self._data_to_validate):
            return True
        else:
            return False

    def read_data_from_xml(self) -> DataFrame:
        # future: report wrong documentation URL of pd.read_xml() to JetBrains or pandas developers
        return pd.read_xml(
            path_or_buffer=BytesIO(self._data_to_process),
            xpath=self._PATH_ROW,
            namespaces=self.namespace,
        )

    def rename_columns_manually(self) -> None:
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

    def drop_unnecessary_columns(self) -> None:
        self.data.dropna(
            subset=["code_uic"],
            inplace=True,
        )

    def swap_names_separated_with_comma(self) -> None:
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

    def add_data(self) -> None:
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
