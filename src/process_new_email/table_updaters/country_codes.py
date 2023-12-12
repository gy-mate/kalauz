from abc import ABC
import contextlib
from io import BytesIO
from typing import Final, final

from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import _Element
from requests import HTTPError
# the comment in the line below can be removed when mypy library stubs are created for the module imported
from varname import nameof  # type: ignore
import zipfile

from src.process_new_email.table_updaters.common import HelperTableUpdater


@final
class CountryCodesUpdater(HelperTableUpdater, ABC):
    
    def __init__(self, data_url: str, xsd_url: str):
        super().__init__(data_url)
        
        self.data: _Element = _Element()
        self.namespace: dict | None = None
        
        self.TAG_ROW: Final = "ns1:Country"
        self.PATH_ROW: Final = f".//{self.TAG_ROW}"
        self.TAG_BEGINNING_COLUMN: Final = f"{self.TAG_ROW}_"
        
        self.XSD_URL: Final = xsd_url
        try:
            self.XSD_TO_PROCESS: Final[BytesIO] = super().download_data(xsd_url)
            self.XSD: Final[etree.XMLSchema] = self.process_xsd()
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)
    
        self.class_created(self)
    
    def process_xsd(self) -> etree.XMLSchema:
        xsd_unzipped = self.unzip(self.XSD_TO_PROCESS)
        return etree.XMLSchema(
            etree.parse(xsd_unzipped)
        )
    
    def unzip(self, xsd: BytesIO) -> BytesIO:
        with zipfile.ZipFile(xsd, 'r') as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self.XSD_URL} has more than one file in it!"
                )
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))
    
    def process_data(self) -> None:
        downloaded_data = self.DATA_TO_PROCESS
        self.data = etree.parse(downloaded_data).getroot()
        self.logger.info(f"Data downloaded from {self.DATA_URL} successfully processed!")
        
        self.namespace = self.data.nsmap
        
        try:
            self.validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise
    
    def validate_data(self) -> None:
        if self.XSD and self.XSD.validate(self.data) is False:
            raise ValueError(
                f"The .xml file downloaded from {self.DATA_URL} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.XSD_URL}!"
            )
        self.logger.info(f"Data downloaded from {self.DATA_URL} successfully validated!")
    
    def store_data(self) -> None:
        self.create_table_if_not_exists()
        self.add_data()
    
    def add_data(self) -> None:
        query = '''
        INSERT IGNORE INTO countries (
            ISO_code,
            UIC_code,
            name_EN,
            name_FR,
            name_DE
        )
        VALUES (%s, %s, %s, %s, %s)
        '''
        for country in self.data.findall(self.TAG_ROW, namespaces=self.namespace):
            values = self.extract_info(country)
            self.CURSOR.execute(query, values)
        self.CONNECTION_TO_DATABASE.commit()
        self.logger.info(f"Successfully added new data downloaded from {self.DATA_URL} to table `countries`!")
    
    def create_table_if_not_exists(self) -> None:
        query = '''
        CREATE TABLE IF NOT EXISTS countries (
            ISO_code VARCHAR(2),
            UIC_code INT(2),
            name_EN VARCHAR(255),
            name_FR VARCHAR(255),
            name_DE VARCHAR(255)
        )
        '''
        self.CURSOR.execute(query)
        self.CONNECTION_TO_DATABASE.commit()
        self.logger.info("Table `countries` sucessfully created (if needed)!")
    
    def extract_info(self, country: _Element) -> tuple:
        iso_code, name_en = self.extract_critical_info(country)
        name_de, name_fr, uic_code = self.extract_extra_info(country)
        
        return iso_code, uic_code, name_en, name_fr, name_de
    
    def extract_extra_info(self, country: _Element) -> tuple:
        # line below can be removed when https://youtrack.jetbrains.com/issue/PY-16408/ is fixed
        # noinspection PyUnusedLocal
        uic_code = name_fr = name_de = None
        with contextlib.suppress(AttributeError):
            uic_code = int(self.find_value(country, "UIC_Code"))
            name_fr = self.find_value(country, "Name_FR")
            name_de = self.find_value(country, "Name_DE")
        return name_de, name_fr, uic_code
    
    def extract_critical_info(self, country: _Element) -> tuple:
        try:
            iso_code = self.find_value(country, "ISO_Code")
            name_en = self.find_value(country, "Name_EN")
        except TypeError:
            self.logger.error(
                f"Critical info could not be extracted from {nameof(self.data)}!"
            )
            raise
        return iso_code, name_en
        
    def find_value(self, row: _Element, column: str) -> str:
        return row.find(self.TAG_BEGINNING_COLUMN + column, self.namespace).text  # type: ignore
