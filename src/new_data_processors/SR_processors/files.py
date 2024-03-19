from datetime import datetime
import json
import os
import shutil

from pypdf import PdfReader
import requests
from requests import HTTPError

from src.new_data_processors.common import DataProcessor


def get_pdf_date(pdf_file):
    pdf_reader = PdfReader(pdf_file)
    first_page = pdf_reader.pages[0]
    text = first_page.extract_text()
    date_str = text.split()[-2]
    return datetime.strptime(date_str, "%Y.%m.%d.").date()


class NewFilesRegistrar(DataProcessor):
    def run(self):
        self.process_received_files()
        self.logger.info("All new files registered!")
    
    def process_received_files(self) -> None:
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
                            xlsx_data = self.convert_pdf_to_xlsx(new_file_name_pdf)
                            xlsx_file.write(xlsx_data)
                    
                    shutil.move(
                        src=new_file_path_pdf,
                        dst=folder_converted,
                    )
    
    def convert_pdf_to_xlsx(self, file_name) -> bytes:
        try:
            api_url = "https://eu-v2.convertapi.com/convert/pdf/to/xlsx"
            parameters = {
                "Secret": self.get_convertapi_secret(),
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
            
            response.raise_for_status()
            json_response = json.loads(response.content)
            file_url = json_response["Files"][0]["Url"]
            
            return requests.get(file_url).content
        except HTTPError:
            self.logger.critical(f"Failed to convert .pdf to .xlsx!")
            raise
    
    def get_convertapi_secret(self) -> str:
        try:
            env_var_name = "CONVERTAPI_SECRET"
            secret = os.getenv(env_var_name)
            if not secret:
                raise ValueError(
                    f"No password found in the `.env` file for {env_var_name}!"
                )
            return secret
        except ValueError as exception:
            self.logger.critical(exception)
            raise
        