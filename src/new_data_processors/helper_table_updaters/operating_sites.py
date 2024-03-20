import re
from typing import ClassVar, Final

from bs4 import BeautifulSoup
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    text,
)

from src.new_data_processors.common import (
    DataDownloader,
)
from src.new_data_processors.common_excel_processors import ExcelProcessorSimple


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


class OperatingSitesUpdater(ExcelProcessorSimple, DataDownloader):
    TABLE_NAME: ClassVar[str] = "operating_sites"
    database_metadata: ClassVar[MetaData] = MetaData()

    table: ClassVar[Table] = Table(
        TABLE_NAME,
        database_metadata,
        Column(name="name", type_=String(255), nullable=False),
        Column(name="name_shortened", type_=String(255)),
        Column(name="name_short", type_=String(255)),
        Column(name="operator", type_=String(255)),
        Column(name="type", type_=String(255)),
        Column(
            name="code_uic", type_=Integer, nullable=False, index=True, primary_key=True
        ),
        Column(name="code_telegraph", type_=String(4)),
        Column(name="category_passenger", type_=SmallInteger),
        Column(name="category_freight", type_=SmallInteger),
        Column(name="traffic_passenger", type_=Boolean),
        Column(name="traffic_freight", type_=Boolean),
        Column(name="terminus", type_=Boolean),
        Column(name="request_stop", type_=Boolean),
        Column(name="train_meeting", type_=Boolean),
        Column(name="open_to_train_operators", type_=Boolean),
    )

    def __init__(self) -> None:
        super().__init__()

        self.WEBSITE_DOMAIN: Final = "https://www.kapella2.hu"
        self.WEBSITE_URL: Final = (
            f"/ehuszfelulet/szolgalatihelyek?vizsgalt_idopont="
            f"{self.TODAY}&vizsgalt_idoszak_kezdo={self.TODAY}&vizsgalt_idoszak_veg={self.TODAY}"
        )
        self.INFRA_ID: int = NotImplemented
        self.INFRA_ID_URL: str = NotImplemented
        self.XLS_URL: str = NotImplemented

        self._data_to_process = self.get_data(self.WEBSITE_DOMAIN + self.WEBSITE_URL)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def get_data(self, url: str) -> bytes:
        splash_page_soup = self.get_splash_page(url)
        self.get_infra_id(splash_page_soup, url)
        list_page = self.download_list_page(url)
        return self.download_xls_file(list_page)

    def get_splash_page(self, url: str) -> BeautifulSoup:
        splash_page = super().get_data(url)
        splash_page_soup = BeautifulSoup(
            markup=splash_page,
            features="lxml",
        )
        return splash_page_soup

    def get_infra_id(self, splash_page_soup, url) -> None:
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

    def download_list_page(self, url: str) -> bytes:
        self.INFRA_ID_URL = f"&infra_id={self.INFRA_ID}"
        list_page = super().get_data(url + self.INFRA_ID_URL)
        return list_page

    def download_xls_file(self, list_page: bytes) -> bytes:
        self.XLS_URL = re.findall(
            pattern=r"/ehuszfelulet/excelexport\?id_xls=\w+",
            string=str(list_page),
        )[0]
        return super().get_data(self.WEBSITE_DOMAIN + self.XLS_URL)

    def rename_columns_manually(self) -> None:
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

    def correct_data_manually(self) -> None:
        self.data["type"] = self.data["type"].apply(
            lambda x: _translate_operating_site_type(str(x))
        )
        self.replace_code_uic_letters()

    def replace_code_uic_letters(self) -> None:
        country_codes_iso = ["HU", "AT", "SK", "UA", "RO", "RS", "HR", "SI"]
        for country_code_iso in country_codes_iso:
            country_code_uic = self.get_uic_code(country_code_iso)
            self.data["code_uic"] = self.data["code_uic"].str.replace(
                pat=country_code_iso,
                repl=country_code_uic,
            )

    def get_uic_code(self, country_code_iso: str) -> str:
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
    
    def correct_boolean_values(self) -> None:
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

    def add_data(self) -> None:
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
