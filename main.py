from datetime import datetime
import json
import logging
import os
import shutil
import sys

from dotenv import load_dotenv
from pypdf import PdfReader
import requests

from src.new_data_processors.common import TableUpdater
from src.new_data_processors.helper_table_updaters.countries import CountriesUpdater
from src.new_data_processors.helper_table_updaters.companies import CompaniesUpdater
from src.new_data_processors.helper_table_updaters.operating_sites import (
    OperatingSitesUpdater,
)
from src.new_data_processors.SR_processors.companies.MÁV import MavUpdater
from src.new_data_processors.SR_processors.companies.GYSEV import GysevUpdater
from src.OSM_processors.downloader import OsmDownloader


# future: mark all packages as namespace packages in the IDE when https://youtrack.jetbrains.com/issue/PY-55212/ is fixed


def get_pdf_date(pdf_file):
    pdf_reader = PdfReader(pdf_file)
    first_page = pdf_reader.pages[0]
    text = first_page.extract_text()
    date_str = text.split()[-2]
    return datetime.strptime(date_str, "%Y.%m.%d.").date()


def convert_pdf_to_xlsx(file_name, pdf_file):
    api_url = "https://eu-v2.convertapi.com/convert/pdf/to/xlsx"
    parameters = {
        "Secret": "t11qaiFuA4uXj2Zc",
        "EnableOcr": "false",
        "StoreFile": "true",
        "Timeout": "90",
    }
    file = {
        "File": open(f"data/01_received/{file_name}", "rb"),
    }
    print(f"Converting {file_name} to .xlsx started...")
    response = requests.post(
        url=api_url,
        params=parameters,
        files=file,
    )
    print("...finished!")

    if response.status_code == 200:
        json_response = json.loads(response.content)
        file_url = json_response["Files"][0]["Url"]
    else:
        raise requests.RequestException(
            f"Failed to convert .pdf to .xlsx: {response.status_code}: {response.text}"
        )
    
    return requests.get(file_url).content


def main(demonstration=False) -> None:
    if demonstration:
        logging.basicConfig(
            encoding="utf-8",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("kalauz.log"),
            ],
            format="%(asctime)s \t %(message)s",
            level=logging.INFO,
        )
        load_dotenv()
    else:
        logging.basicConfig(
            encoding="utf-8",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("kalauz.log"),
            ],
            format="%(asctime)s \t %(levelname)s \t %(name)s.%(funcName)s(): %(message)s",
            level=logging.DEBUG,
        )
        load_dotenv()

    folder_received = os.path.abspath("data/01_received/")
    folder_converted = os.path.abspath("data/02_converted/")
    with os.scandir(folder_received) as folder:
        for file in folder:
            if file.name.endswith(".pdf"):
                company_name = file.name.split("_")[0]
                with open(file, "rb") as pdf_file:
                    file_date = get_pdf_date(pdf_file)
                    extension = os.path.splitext(file)[1]
                    new_file_name_pdf = (
                        f"{company_name}_{str(file_date)}_ASR{extension}"
                    )
                    new_file_name_xlsx = f"{company_name}_{str(file_date)}_ASR.xlsx"
                    new_file_path_pdf = os.path.join(folder_received, new_file_name_pdf)
                    new_file_path_xlsx = os.path.join(
                        folder_converted, new_file_name_xlsx
                    )
                    os.rename(
                        src=file.path,
                        dst=new_file_path_pdf,
                    )
                    with open(new_file_path_xlsx, "wb") as xlsx_file:
                        xlsx_data = convert_pdf_to_xlsx(new_file_name_pdf, pdf_file)
                        xlsx_file.write(xlsx_data)

                shutil.move(
                    src=new_file_path_pdf,
                    dst=folder_converted,
                )

    # future: remove comments below when https://github.com/python/mypy/issues/10160 or https://github.com/python/mypy/issues/9756 is fixed
    updaters_to_run: list[TableUpdater] = [  # type: ignore
        # CountriesUpdater,  # type: ignore
        # CompaniesUpdater,  # type: ignore
        # OperatingSitesUpdater,  # type: ignore
        # MavUpdater,  # type: ignore
        # GysevUpdater,  # type: ignore
    ]
    for updater in updaters_to_run:
        # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-52210/ is fixed
        # noinspection PyCallingNonCallable

        # future: report bug (false positive) to mypy developers
        updater = updater()  # type: ignore
        updater.process_data()
        updater.store_data()

        updater.logger.info(f"Table `{updater.TABLE_NAME}` sucessfully updated!")

    OsmDownloader().run()
    
    logging.getLogger(__name__).info("All done!")


if __name__ == "__main__":
    main()
