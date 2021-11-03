from reports.data_extraction.database.models.database_model import DatabaseReport


def get_data(report_type):
    """
    Executes the data collection pipeline to get all the required data according to 'report_type' parameter.

    :param report_type: (str) report configuration type to load from the 'report_config.json' configuration file.
    :return: (pd.DataFrame) dataset with the clean data generated within the month and year received as parameter.
    """

    report = DatabaseReport(report_type=report_type)

    report.set_db_connexion()
    report.load_db_tables()
    report.merge_tables()

    return report.merged_table
