from src.process_new_email.table_updaters.common import ExcelProcessor


class SpeedRestrictionsUpdater(ExcelProcessor):
    
    def __init__(self):
        super().__init__()
        
        self.TABLE_NAME = "speed_restrictions"

    def process_data(self) -> None:
        pass
    
    def _rename_columns_manually(self) -> None:
        pass
    
    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass
