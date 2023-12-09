from abc import ABC
from io import BytesIO
from lxml import etree
import requests
import zipfile

from process_new_email.table_updaters.common import HelperTableUpdater


class CountryCodesUpdater(HelperTableUpdater, ABC):
    
    def __init__(self, data_url: str, xsd_url: str):
        super().__init__(data_url)
        
        self.xsd_url = xsd_url
        # noinspection PyProtectedMember
        self.xsd_to_process: (
            BytesIO |
            None
        ) = None
        self.xsd: etree.XMLSchema = etree.XMLSchema(
            etree.fromstring("<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'></xs:schema>")
        )
        
        self.table: (
            list |
            None
        ) = None
    
    def unzip(self, xsd: BytesIO) -> BytesIO:
        with zipfile.ZipFile(xsd, 'r') as zipped_file:
            file_names = zipped_file.infolist()
            if len(file_names) > 1:
                raise IndexError(
                    f"The .zip file downloaded from {self.xsd_url} has more than one file in it!"
                )
            
            only_file_name = file_names[0]
            return BytesIO(zipped_file.read(only_file_name))
    
    def download_data(self, url: str) -> BytesIO:
        super().download_data()
        self.data = etree.XML(self.data_to_process.getvalue())
        
        response = requests.get(self.xsd_url)
        response.raise_for_status()
        
        self.xsd_to_process = self.unzip(
            BytesIO(response.content)
        )
        self.xsd = etree.XMLSchema(
            etree.parse(self.xsd_to_process)
        )
    
    def validate_data(self) -> None:
        if self.xsd.validate(self.data) is False:
            raise ValueError(
                f"The .xml file downloaded from {self.data_url} is invalid "
                f"according to the .xsd downloaded and unzipped from {self.xsd_url}!"
            )
        
    def import_data(self) -> None:
        self.table = [[None] * len(row) for row in self.data.xpath(".//ns1:Country") if isinstance(row, list)]
    
    def process_data(self) -> None:
        self.validate_data()
        self.import_data()
    
    def store_data(self) -> None:
        pass
