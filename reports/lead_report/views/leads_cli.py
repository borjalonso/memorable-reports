import click
import sys
import datetime
import os
import pandas as pd
from pathlib import Path

sys.path.append(r'C:\Users\borja\PycharmProjects\osoigo_ia')

from reports.lead_report.controllers.lead_report_controller import generate_report
from reports.lead_report.controllers.excel_controller import to_excel

current_directory = Path(os.path.dirname(__file__))
config_file = os.path.join(current_directory.parent.absolute(), "config/client_requirements.json")
config = pd.read_json(config_file, orient='records')

current_date = datetime.datetime.now()
ngos_list = [ngo['alias'] for ngo in config['client']]


@click.command()
@click.option('--date', '-d',
              default=None,
              required=True,
              type=click.DateTime(),
              help='date (yyyy format) the report is generated')
@click.option('--ong', '-o',
              default=None,
              required=True,
              multiple=True,
              type=click.Choice(choices=ngos_list),
              help='client name for which you want to obtain the report. Attention! case sensitive')
def command_line_interface(date, ong):
    """
    Command line user interface developed to interact with the lead report script stack.

    :param date: () date the report is generated.
    :param ong: (list) ONG name/s for which the data will be obtained. Required parameter.
        :example: -o acnur        --> ong   = [acnur]
        :example: -o acnur -o msf --> month = [acnur, msf]
    :return: void
    """

    month, country, client = _format_params(month=month, country=country, client=ong)

    print(month)
    print(year)
    print(country)
    print(ong)

    # Getting raw data from the database
    data = generate_report(month=month, year=year, country=country, client=client)

    to_excel(month=month, year=year, country=country, client=ong, data=data)


def _format_params(month, country, client):
    """
    It takes the month, country and client values selected by the user and transforms it in such a way that they can be
    interpreted by the program:
    - Null values: they are set to None.
    - Unfilled data: it is set to None.

    :param month: (tuple) empty tuple or tuple of int values representing months (1-12).
    :param country: (string) country name.
    :param client: (string) client name.
    :return: (int, 2x string) formatted month, country and client values.
    """

    if len(month) == 0:
        month = None
    else:
        month = list(set(month))

    if country is not None:
        country = country.title()

    if client is not None:
        client = client.lower()

    return month, country, client


if __name__ == '__main__':
    command_line_interface()
