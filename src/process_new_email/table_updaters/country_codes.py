from abc import ABC
import contextlib
from io import BytesIO
from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import _Element
from requests import HTTPError
# the comment in the line below can be removed when mypy library stubs are created for the module imported
from varname import nameof  # type: ignore
import zipfile

from src.process_new_email.table_updaters.common import HelperTableUpdater


class CountryCodesUpdater(HelperTableUpdater, ABC):
    
    def __init__(self, data_url: str, xsd_url: str):
        super().__init__(data_url)
        
        self.data: _Element = _Element()
        self.namespace: dict | None = None
        self.path: str = ".//ns1:Country"
        self.tag_beginning: str = f"{self.path[3:]}_"
        
        self.xsd_url = xsd_url
        try:
            self.xsd_to_process: BytesIO = super().download_data(xsd_url)
            self.xsd: etree.XMLSchema = self.process_xsd(self.xsd_to_process)
        except (HTTPError, IndexError) as exception:
            self.logger.warning(exception)
    
        self.logger.info(f"{self.__class__.__name__} initialized!")
    
    def unzip(self, xsd: BytesIO) -> BytesIO:
        with zipfile.ZipFile(xsd, 'r') as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self.xsd_url} has more than one file in it!"
                )
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))
    
    def process_xsd(self, xsd_to_process) -> etree.XMLSchema:
        xsd_to_process = self.unzip(xsd_to_process)
        return etree.XMLSchema(
            etree.parse(xsd_to_process)
        )
    
    def validate_data(self) -> None:
        if self.xsd and self.xsd.validate(self.data) is False:
            raise ValueError(
                f"The .xml file downloaded from {self.data_url} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.xsd_url}!"
            )
        
    def find_value(self, row: _Element, column: str) -> str:
        return row.find(self.tag_beginning + column, self.namespace).text  # type: ignore
    
    def process_data(self) -> None:
        downloaded_data = self.data_to_process
        self.data = etree.parse(downloaded_data).getroot()
        self.namespace = self.data.nsmap
        
        try:
            self.validate_data()
        except ValueError as exception:
            self.logger.critical(exception)
            raise
    
    def extract_info(self, country: _Element) -> tuple:
        try:
            iso_code: str = self.find_value(country, "ISO_Code")
            name_en: str = self.find_value(country, "Name_EN")
        except TypeError:
            self.logger.error(f"Critical info could not be extracted from {nameof(self.data)}!")
            raise

        # line below can be removed when https://youtrack.jetbrains.com/issue/PY-16408/ is fixed
        # noinspection PyUnusedLocal
        uic_code = name_fr = name_de = None
        with contextlib.suppress(AttributeError):
            uic_code = int(self.find_value(country, "UIC_Code"))
            name_fr = self.find_value(country, "Name_FR")
            name_de = self.find_value(country, "Name_DE")

        return iso_code, uic_code, name_en, name_fr, name_de
    
    def store_data(self) -> None:
        create_table = '''
        CREATE TABLE countries (
            ISO_code VARCHAR(2),
            UIC_code INT,
            name_EN VARCHAR(255),
            name_FR VARCHAR(255),
            name_DE VARCHAR(255)
        )
        '''
        self.cursor.execute(create_table)
        self.connection_to_database.commit()
        
        add_country = '''
        INSERT IGNORE INTO countries (
            ISO_code,
            UIC_code,
            name_EN,
            name_FR,
            name_DE
        )
        VALUES (%s, %s, %s, %s, %s)
        '''
        for country in self.data.findall(self.path, namespaces=self.namespace):
            values = self.extract_info(country)
            self.cursor.execute(add_country, values)
        self.connection_to_database.commit()
