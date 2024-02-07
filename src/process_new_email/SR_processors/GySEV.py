from src.process_new_email.SR_processors.common import SRUpdater


class GysevUpdater(SRUpdater):
    def __init__(self) -> None:
        super().__init__(
            company="GYSEV",
            source_extension="xlsx",
        )

    def _get_data(self) -> None:
        pass

    def _import_data(self) -> None:
        pass

    def _correct_data_manually(self) -> None:
        pass

    def _rename_columns_manually(self) -> None:
        pass

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass
