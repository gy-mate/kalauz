from abc import ABC, abstractmethod
from io import BytesIO
# noinspection PyProtectedMember
from lxml.etree import _Element
import requests


class HelperTableUpdater(ABC):

    def __init__(self, data_url: str):
        self.data_url = data_url
        self.data_to_process: BytesIO = BytesIO()
        self.data: _Element = _Element()

    def download_data(self) -> None:
        response = requests.get(
            self.data_url,
            headers={
                "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
            }
        )
        response.raise_for_status()
        self.data_to_process = BytesIO(response.content)

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def store_data(self) -> None:
        pass
