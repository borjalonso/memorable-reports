import click
import sys
import datetime

sys.path.append(r'C:\Users\borja\PycharmProjects\osoigo_ia')

from reports.monthly_reports.controllers.report_controller import generate_report
from reports.monthly_reports.controllers.excel_controller import to_excel

current_date = datetime.datetime.now()


@click.command()
@click.option('--start', '-s',
              default=None,
              required=False,
              type=click.DateTime(),
              help='start date (yyyy-mm-dd format) for which you want to obtain the stats (Included)')
@click.option('--end', '-e',
              default=None,
              required=False,
              type=click.DateTime(),
              help='end date (yyyy-mm-dd format) for which you want to obtain the stats (Not included)')
@click.option('--country', '-c',
              default=None,
              required=False,
              help='country name for which you want to obtain the stats')
@click.option('--ong', '-o',
              default=None,
              required=False,
              help='client name for which you want to obtain the stats')
def command_line_interface(start: datetime, end: datetime, country: str, ong: str):
    """
    Command line user interface developed used to interact with the monthly report script stack.

    :param start: (datetime) minimum (earlier) date in which leads were created. None to take from the first record.
    :param end: (datetime) maximum (closest) date in which leads were created. None to take until present.
        :example: -e 2021-04-01   --> end = 2021-04-01
        :example:                 --> end = today
    :param country: (string) country for which the data will be obtained. None to consider every country.
        :example: -c España --> country = España
        :example:           --> country = None
    :param ong: (string) ONG name for which the data will be obtained. None to consider every client.
        :example: -o acnur  --> ong = acnur
        :example:           --> ong = None
    :return: void
    """

    start, end, country, client = _format_params(start_date=start, end_date=end, country=country, client=ong)

    # Getting raw data from the database
    data = generate_report(start_date=start, end_date=end, country=country, client=client)

    to_excel(start_date=start, end_date=end, country=country, client=ong, data=data)


def _format_params(start_date: datetime, end_date: datetime, country: str, client: str):
    """
    It takes the month, country and client values selected by the user and transforms it in such a way that they can be
    interpreted by the program:
    - Null values: they are set to None.
    - Unfilled data: it is set to None.

    :param start_date: (datetime) minimum (earlier) date in which leads were created.
    :param end_date: (datetime) maximum (closest) date in which leads were created.
    :param country: (string) country name.
    :param client: (string) client name.
    :return: (int, 2x string) formatted month, country and client values.
    """

    if end_date is None:
        today = datetime.datetime.now()
        end_date = datetime.datetime(today.year, today.month, today.day)

    if country is not None:
        country = country.title()

    if client is not None:
        client = client.lower()

    return start_date, end_date, country, client


if __name__ == '__main__':
    command_line_interface()
