from abc import ABC
from datetime import datetime
import re
from typing import Final

from bs4 import BeautifulSoup
from sqlalchemy import text

from src.process_new_email.table_updaters.common import ExcelProcessor, TableUpdater


def _translate_operating_site_type(operating_site_type: str) -> str:
    dictionary = {
        "állomás": "station",
        "egyéb": "other",
        "elágazás": "spur",
        "eldöntő pont": "decision_point",
        "forgalmi kitérő": "crossover",
        "iparvágány": "industrial_track",
        "iparvágány kiágazás": "industrial_track_spur",
        "keresztezés": "crossing",
        "megálló-elágazóhely": "spur_halt",
        "megálló-rakodóhely": "loading_halt",
        "megállóhely": "halt",
        "megállóhely-iparvágány kiágazás": "industrial_track_spur_halt",
        "nem definiált": "undefined",
        "országhatár": "border_crossing",
        "pályavasúti határpont": "railway_border_crossing",
        "rakodóhely": "loading_point",
        "vágányfonódás-elágazás": "gauntlet_spur",
    }
    return dictionary[operating_site_type]


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
        self.TABLE_NAME = "operating_sites"

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
        # future: report wrong display and copying of hyphen (e.g. Fil'akovo) to pandas and JetBrains developers
        self.data.rename(
            columns={
                "Hosszú név": "name",
                "Rövid név": "name_shortened",
                "Polgári név": "name_short",
                "Társaság": "operator",
                "Szolgálati hely típus": "type",
                "PLC kód": "code_uic",
                "Távíró kód": "code_telegraph",
                "Állomáskategória személyvonatok számára": "category_passenger",
                "Állomáskategória tehervonatok számára": "category_freight",
                "Személy szállításra megnyitva": "traffic_passenger",
                "Áru szállításra megnyitva": "traffic_freight",
                "Menetvonal kezdő/végpontja": "terminus",
                "Feltételes megállás lehetséges": "request_stop",
                "Vonattalálkozásra alkalmas": "train_meeting",
                "Szolg. hely nyílt": "open_to_train_operators",
            },
            inplace=True,
        )

    def _correct_data_manually(self):
        self.data["type"] = self.data["type"].apply(
            lambda x: _translate_operating_site_type(str(x))
        )

        country_codes_iso = ["HU", "AT", "SK", "UA", "RO", "RS", "HR", "SI"]
        for country_code_iso in country_codes_iso:
            country_code_uic = self._get_uic_code(country_code_iso)
            self.data["code_uic"] = self.data["code_uic"].str.replace(
                pat=country_code_iso,
                repl=country_code_uic,
            )

    def _get_uic_code(self, country_code_iso: str) -> str:
        with self.database.engine.begin() as connection:
            query = """
                select code_uic
                from countries
                where code_iso = :country_code_iso
            """
            result = connection.execute(
                text(query),
                {"country_code_iso": country_code_iso},
            ).fetchone()
            try:
                assert result is not None
            except AssertionError as exception:
                self.logger.critical(exception)
                raise
            return str(result[0])

    def _correct_boolean_values(self):
        boolean_columns = [
            "traffic_passenger",
            "traffic_freight",
            "terminus",
            "request_stop",
            "train_meeting",
            "open_to_train_operators",
        ]
        for column in boolean_columns:
            self.data[column] = self.data[column].apply(lambda x: x == "igen")

    def _create_table_if_not_exists(self) -> None:
        with self.database.engine.begin() as connection:
            query = """
                create table if not exists :table_name (
                    name varchar(255) not null,
                    name_shortened varchar(255),
                    name_short varchar(255),
                    operator varchar(255),
                    type varchar(255),
                    code_uic int(7) not null,
                    code_telegraph varchar(4),
                    category_passenger int(1),
                    category_freight int(1),
                    traffic_passenger boolean,
                    traffic_freight boolean,
                    terminus boolean,
                    request_stop boolean,
                    train_meeting boolean,
                    open_to_train_operators boolean,
                    
                    index (code_uic),
                    primary key (code_uic)
                )
            """
            connection.execute(
                text(query),
                {"table_name": self.TABLE_NAME},
            )

    def _add_data(self) -> None:
        with self.database.engine.begin() as connection:
            queries = [
                """
                insert ignore into operating_sites (
                    name,
                    name_shortened,
                    name_short,
                    operator,
                    type,
                    code_uic,
                    code_telegraph,
                    category_passenger,
                    category_freight,
                    traffic_passenger,
                    traffic_freight,
                    terminus,
                    request_stop,
                    train_meeting,
                    open_to_train_operators
                )
                values (
                    :name,
                    :name_shortened,
                    :name_short,
                    :operator,
                    :type,
                    :code_uic,
                    :code_telegraph,
                    :category_passenger,
                    :category_freight,
                    :traffic_passenger,
                    :traffic_freight,
                    :terminus,
                    :request_stop,
                    :train_meeting,
                    :open_to_train_operators
                )
                """,
                """
                update operating_sites
                set
                    name = :name,
                    name_shortened = :name_shortened,
                    name_short = :name_short,
                    operator = :operator,
                    type = :type,
                    code_telegraph = :code_telegraph,
                    category_passenger = :category_passenger,
                    category_freight = :category_freight,
                    traffic_passenger = :traffic_passenger,
                    traffic_freight = :traffic_freight,
                    terminus = :terminus,
                    request_stop = :request_stop,
                    train_meeting = :train_meeting,
                    open_to_train_operators = :open_to_train_operators
                where code_uic = :code_uic
                """,
            ]

            for index, row in self.data.iterrows():
                for query in queries:
                    connection.execute(
                        text(query),
                        row.to_dict(),
                    )
