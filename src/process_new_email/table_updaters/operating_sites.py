from abc import ABC
from datetime import datetime
import re
from typing import Final

from bs4 import BeautifulSoup

from src.process_new_email.table_updaters.common import ExcelProcessor, TableUpdater


class OperatingSitesUpdater(ExcelProcessor, TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        today = datetime.today().date()
        self.WEBSITE_DOMAIN: Final = "https://www.kapella2.hu"
        self.WEBSITE_URL: Final = (
            f"/ehuszfelulet/szolgalatihelyek?vizsgalt_idopont="
            f"{today}&vizsgalt_idoszak_kezdo={today}&vizsgalt_idoszak_veg={today}"
        )
        self.INFRA_ID: int = NotImplemented
        self.INFRA_ID_URL: str = NotImplemented
        self.XLS_URL: str = NotImplemented

        self._data_to_process = self.download_data(
            self.WEBSITE_DOMAIN + self.WEBSITE_URL
        )

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def download_data(self, url: str) -> bytes:
        splash_page = super().download_data(url)
        splash_page_soup = BeautifulSoup(
            markup=splash_page,
            features="lxml",
        )

        try:
            select_tag = splash_page_soup.find(
                name="select",
                attrs={"name": "infra_id"},
            )
            if not select_tag:
                raise ValueError(f"No `select` tag found on the splash page at {url}!")
        except ValueError as exception:
            self.logger.critical(exception)
            raise

        # future: report bug (false positive) to mypy developers
        self.INFRA_ID = int(select_tag.find("option")["value"])  # type: ignore
        self.INFRA_ID_URL = f"&infra_id={self.INFRA_ID}"
        list_page = super().download_data(url + self.INFRA_ID_URL)

        self.XLS_URL = re.findall(
            pattern=r"/ehuszfelulet/excelexport\?id_xls=\w+",
            string=str(list_page),
        )[0]
        return super().download_data(self.WEBSITE_DOMAIN + self.XLS_URL)

    def _rename_columns_manually(self):
        pass

    def _delete_data(self):
        pass

    def store_data(self) -> None:
        pass
