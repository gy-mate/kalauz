from typing import final

from src.new_data_processors.SR_processors.common import SRUpdater
from src.new_data_processors.common_excel_processors import ExcelProcessorSimple


@final
class GysevUpdater(SRUpdater, ExcelProcessorSimple):
    def __init__(self) -> None:
        super().__init__(
            company="GYSEV",
            source_extension="xlsx",
        )
        
    def _rename_columns_manually(self) -> None:
        pass
    
    def _correct_boolean_values(self) -> None:
        pass
    
    def _add_data(self) -> None:
        pass
