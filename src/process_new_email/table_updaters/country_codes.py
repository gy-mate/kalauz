from abc import ABC
import contextlib
from io import BytesIO
from typing import Final, final

from lxml import etree

# noinspection PyProtectedMember
from lxml.etree import _Element
from requests import HTTPError

# TODO: remove comment in the line below when mypy library stubs are created for the module imported
from varname import nameof  # type: ignore
import zipfile

from src.process_new_email.table_updaters.common import HelperTableUpdater


def _uic_code_not_assigned(values: list) -> bool:
    return values[1] is None


def _correct_swapped_strings(values: list) -> None:
    if values[2] and values[3] and values[4]:
        values[2], values[3], values[4] = swap_strings_at_comma(values[2:5])


def swap_strings_at_comma(strings: list[str]) -> list[str]:
    swapped_strings = []
    for s in strings:
        parts = s.split(', ')
        swapped_strings.append(' '.join(parts[::-1]))
    return swapped_strings


@final
class CountryCodesUpdater(HelperTableUpdater, ABC):
    def __init__(self, database_connection, data_url: str, xsd_url: str) -> None:
        super().__init__(database_connection, data_url)

        self.data: _Element = _Element()
        self.namespace: dict | None = None

        self._TAG_ROW: Final = "ns1:Country"
        self._PATH_ROW: Final = f".//{self._TAG_ROW}"
        self._TAG_BEGINNING_COLUMN: Final = f"{self._TAG_ROW}_"

        self._XSD_URL: Final = xsd_url
        try:
            self.XSD_TO_PROCESS: Final[BytesIO] = super().download_data(xsd_url)
            self._XSD: Final[etree.XMLSchema] = self._process_xsd()
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)
        
        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _process_xsd(self) -> etree.XMLSchema:
        xsd_unzipped = self._unzip(self.XSD_TO_PROCESS)
        return etree.XMLSchema(etree.parse(xsd_unzipped))

    def _unzip(self, xsd: BytesIO) -> BytesIO:
        with zipfile.ZipFile(xsd, "r") as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self._XSD_URL} has more than one file in it!"
                )
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))

    def process_data(self) -> None:
        downloaded_data = self._DATA_TO_PROCESS
        self.data = etree.parse(downloaded_data).getroot()
        self.logger.info(
            f"Data downloaded from {self._DATA_URL} successfully processed!"
        )

        self.namespace = self.data.nsmap

        try:
            self._validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def _validate_data(self) -> None:
        if self._XSD and self._XSD.validate(self.data) is False:
            raise ValueError(
                f"The .xml file downloaded from {self._DATA_URL} is invalid "
                f"according to the .xsd downloaded and unzipped from {self._XSD_URL}!"
            )
        self.logger.info(
            f"Data downloaded from {self._DATA_URL} successfully validated!"
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
            primary key (UIC_code),
            unique key (ISO_code)
        )
        """
        self.DATABASE.cursor.execute(query)
        self.DATABASE.connection.commit()
        self.logger.info("Table `countries` sucessfully created (if needed)!")
    
    def _add_data(self) -> None:
        query = """
        insert ignore into countries (
            ISO_code,
            UIC_code,
            name_EN,
            name_FR,
            name_DE
        )
        values (%s, %s, %s, %s, %s)
        """
        for country in self.data.findall(self._TAG_ROW, namespaces=self.namespace):
            values = list(self._extract_info(country))
            
            if _uic_code_not_assigned(values):
                continue
            _correct_swapped_strings(values)
            
            self.DATABASE.cursor.execute(query, values)
        self.DATABASE.connection.commit()
        self.logger.info(
            f"Successfully added new data downloaded from {self._DATA_URL} to table `countries`!"
        )
    
    def _extract_info(self, country: _Element) -> tuple:
        iso_code, name_en = self._extract_critical_info(country)
        name_de, name_fr, uic_code = self._extract_extra_info(country)

        return iso_code, uic_code, name_en, name_fr, name_de

    def _extract_extra_info(self, country: _Element) -> tuple:
        # TODO: remove line below when https://youtrack.jetbrains.com/issue/PY-16408/ is fixed
        # noinspection PyUnusedLocal
        uic_code = name_fr = name_de = None
        with contextlib.suppress(AttributeError):
            uic_code = int(self._find_value(country, "UIC_Code"))
            name_fr = self._find_value(country, "Name_FR")
            name_de = self._find_value(country, "Name_DE")
        return name_de, name_fr, uic_code

    def _extract_critical_info(self, country: _Element) -> tuple:
        try:
            iso_code = self._find_value(country, "ISO_Code")
            name_en = self._find_value(country, "Name_EN")
        except TypeError:
            self.logger.critical(
                f"Critical info could not be extracted from {nameof(self.data)}!"
            )
            raise
        return iso_code, name_en

    def _find_value(self, row: _Element, column: str) -> str:
        return row.find(self._TAG_BEGINNING_COLUMN + column, self.namespace).text  # type: ignore
