import pandas as pd
import xlsxwriter
import os
import datetime


def to_excel(date=datetime.datetime.now(), data=['uno', 'dos', 'tres', 'cuatro'], clients_config=[{'name': 'Medicos Sin Fronteras', 'alias': 'msf', 'acronym': 'MSF', 'identifier': 2, 'entry_date': '2020-04-23 00:00:00', 'exclude_from_delivery': ['msf'], 'months': [1, 3, 6], 'deliveries': [{'campaign_name': 'Entrega leads mayores de 35 años', 'delivery_type': 'hot_list', 'frequency': 'daily', 'delivery_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'], 'maximum_delivery_time': '09:00:00', 'columns': {'first_name': 'fName', 'last_name_full': 'fSurname', 'email': 'fEmail', 'gender': 'fGender', 'telephone': 'fPhone', 'birthday': 'fBirthdate', 'ip': 'fIp', 'id': 'Id', 'signature_method': 'fOptions', 'question': 'fOptions2', 'date': 'fDate'}, 'filters': {'more_than_35': '(`age` > 35)', 'from_spain': '(`name` == "España")', 'has_telephone': '(`telephone` == `telephone`)', 'share_data': '(`shareMyData` == 1)', 'cant_call': '(`cant_call` != 1)'}, 'date_columns': ['birthday', 'date'], 'date_format': '%m-%d-%Y'}, {'campaign_name': 'Entrega leads entre 30 y 35 años', 'delivery_type': 'hot_list', 'frequency': 'daily', 'delivery_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'], 'maximum_delivery_time': '09:00:00', 'columns': {'first_name': 'fName', 'last_name_full': 'fSurname', 'email': 'fEmail', 'gender': 'fGender', 'telephone': 'fPhone', 'birthday': 'fBirthdate', 'ip': 'fIp', 'id': 'Id', 'signature_method': 'fOptions', 'question': 'fOptions2', 'date': 'fDate'}, 'filters': {'from_30_to_35': '(`age` >= 30 and `age` <=35)', 'from_spain': '(`name` == "España")', 'has_telephone': '(`telephone` == `telephone`)', 'share_data': '(`shareMyData` == 1)', 'cant_call': '(`cant_call` != 1)'}, 'date_columns': ['birthday', 'date'], 'date_format': '%m-%d-%Y'}]}, {
      "name": "Cris Contra El Cancer",
      "alias": "cris-contra-el-cancer",
      "acronym": "CRIS",
      "identifier": 4,
      "entry_date": "2019-10-17 00:00:00",
      "exclude_from_delivery": [],
      "months": [1, 3, 6],
      "deliveries": [
        {
          "campaign_name": "Entrega leads mayores de 35 años",
          "delivery_type": "hot_list",
          "frequency": "daily",
          "delivery_days": [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday"
          ],
          "maximum_delivery_time": "09:00:00",
          "columns": {
            "first_name": "fName",
            "last_name_full": "fSurname",
            "email": "fEmail",
            "gender": "fGender",
            "telephone": "fPhone",
            "birthday": "fBirthdate",
            "ip": "fIp",
            "id": "Id",
            "signature_method": "fOptions",
            "question": "fOptions2",
            "date": "fDate"
          },
          "filters": {
            "more_than_35": "(`age` > 35)",
            "from_spain": "(`name` == \"España\")",
            "has_telephone": "(`telephone` == `telephone`)",
            "share_data": "(`shareMyData` == 1)",
            "cant_call": "(`cant_call` != 1)"
          },
          "date_columns": ["birthday", "date"],
          "date_format": "%m-%d-%Y"
        },
        {
          "campaign_name": "Entrega leads entre 30 y 35 años",
          "delivery_type": "hot_list",
          "frequency": "daily",
          "delivery_days": [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday"
          ],
          "maximum_delivery_time": "09:00:00",
          "columns": {
            "first_name": "fName",
            "last_name_full": "fSurname",
            "email": "fEmail",
            "gender": "fGender",
            "telephone": "fPhone",
            "birthday": "fBirthdate",
            "ip": "fIp",
            "id": "Id",
            "signature_method": "fOptions",
            "question": "fOptions2",
            "date": "fDate"
          },
          "filters": {
            "from_30_to_35": "(`age` >= 30 and `age` <=35)",
            "from_spain": "(`name` == \"España\")",
            "has_telephone": "(`telephone` == `telephone`)",
            "share_data": "(`shareMyData` == 1)",
            "cant_call": "(`cant_call` != 1)"
          },
          "date_columns": ["birthday", "date"],
          "date_format": "%m-%d-%Y"
        }
      ]
    }], output_path='', file_name=''):
    """

    :param date:
    :param data:
    :param clients_config:
    :param output_path:
    :param file_name:
    :return:
    """

    output_path = r'C:\Users\borja\Desktop'

    # Month, year and country formatting
    file_name = _create_filename(date=date, clients=clients_config)

    print('file_name={0}'.format(file_name))

    # Create a workbook and add a worksheet
    workbook = xlsxwriter.Workbook(os.path.join(output_path, file_name),
                                   {'default_date_format': 'dd/mm/yyyy'})

    # Set workbook properties
    _set_workbook_properties(workbook=workbook)

    # Adding data to the worksheet
    _add_workbook_data(workbook=workbook, data=data, clients_config=clients_config)


def _create_filename(date, clients):
    """
    It takes a date and a client and formats the output filename as follows:
    'lead_delivery_[yyyymmdd]_[client].xlsx'

    :param date: (datetime) date to generate the monthly report.
    :param clients: (dict) client configuration details.
    :return: (string, string) output header and filename.
    """

    str_date = date.strftime('%Y%m%d')
    str_clients = '_'.join([x['acronym'] for x in clients])

    file_name = 'lead_delivery_{0}_{1}.xlsx'.format(str_date, str_clients)

    return file_name


def _set_workbook_properties(workbook):
    """
    Sets some important workbook properties about the authority, creation date and company among others.
    Properties that will be filled by this method:
        'title'
        'subject'
        'author'
        'manager'
        'company'
        'created'
        'comments'

    :param workbook: (xlsxwriter.Workbook) xlsxwriter Excel object.
    :return: void
    """

    workbook.set_properties({
        'title': 'Osoigo deliveries report spreadsheet',
        'subject': 'Deliveries report',
        'author': 'Borja Alonso Valderrey',
        'manager': 'Borja Alonso Valderrey',
        'company': 'Osoigo.com',
        'created': datetime.datetime.now(),
        'comments': 'Created with Python and XlsxWriter'})


def _add_workbook_data(workbook, data, clients_config):
    """
    Prepares and formats the data that will be part of the output Excel file.

    :param workbook:
    :param data: (dictionaries list) lead data to fill in the workbook.
    :param clients_config:
    :return: void
    """

    # Formats
    formats = {
        'title': workbook.add_format({'bold': True,
                                      'font_size': 22}),
        'table_header': workbook.add_format({'bold': True,
                                             'bg_color': '#F6BC59',
                                             'font_size': 14,
                                             'font_color': '#FFFFFF'}),
        'highlighted_key': workbook.add_format({'bold': True,
                                                'bg_color': '#FAF1E2',
                                                'font_size': 12,
                                                'top': 1,
                                                'top_color': '#F6BC59',
                                                'bottom': 1,
                                                'bottom_color': '#F6BC59',
                                                'valign': 'vcenter'}),
        'odd_key': workbook.add_format({'bg_color': '#F5F5F5',
                                        'font_size': 12}),
        'even_key': workbook.add_format({'font_size': 12})
    }

    # Worksheets
    worksheets = {
        'worksheet_1': workbook.add_worksheet('Lead details'),
        'worksheet_2': workbook.add_worksheet('Deliveries params & config.')
    }

    for key in worksheets.keys():
        worksheets[key].hide_gridlines(option=2)

    # Print header
    row_num = _write_header(worksheets=worksheets, formats=formats, clients_settings=clients_config)

    # Print lead data
    counter = 0
    for client_config in clients_config:

        for delivery in client_config['deliveries']:
            delivery_header = '📌 {0} - {1}'.format(client_config['name'], delivery['campaign_name'])

            row_num = _write_df(dataframe=data[counter], header=delivery_header, worksheet=worksheets['worksheet_1'],
                                row_number=row_num, formats=formats)
            counter += 1

        row_num += 1

    # Close the Workbook object and write the XLSX file
    workbook.close()


def _write_header(worksheets, formats, clients_settings):
    """
    It takes an empty Excel project and formats its header worksheets.
    - 1st worksheet contains the lead data.
    - 2nd worksheet contains the configuration and parameters that apply to the report.

    :param worksheets: (dict) worksheets that made up the Excel project.
    :param formats: (dict) workbook formats.
    :param clients_settings:
    :return: (int) the last row number in which data is written.
    """

    worksheets_keys = list(worksheets.keys())
    current_date = datetime.datetime.now().strftime('%d/%m/%Y')

    # 1) Lead details header
    header = 'LEAD DELIVERY REPORT {}'.format(current_date)
    table_header = ('CLIENT', 'DELIVERY NAME', 'DELIVERY TYPE')

    worksheets[worksheets_keys[0]].write_string(0, 0, header, formats['title'])

    worksheets[worksheets_keys[0]].write(1, 0, None)
    worksheets[worksheets_keys[0]].write(2, 0, None)

    worksheets[worksheets_keys[0]].write_row(3, 0, table_header, formats['table_header'])

    row_num = 4
    for client_config in clients_settings:
        # CLIENT
        merged_row_num = row_num + len(client_config['deliveries']) - 1
        worksheets[worksheets_keys[0]].merge_range(row_num, 0, merged_row_num, 0,
                                                   client_config['name'], formats['highlighted_key'])

        for delivery in client_config['deliveries']:
            if row_num % 2 == 0:
                # DELIVERY NAME
                worksheets[worksheets_keys[0]].write_string(row_num, 1, delivery['campaign_name'], formats['even_key'])
                # DELIVERY TYPE
                worksheets[worksheets_keys[0]].write_string(row_num, 2, delivery['delivery_type'], formats['even_key'])
            else:
                # DELIVERY NAME
                worksheets[worksheets_keys[0]].write_string(row_num, 1, delivery['campaign_name'], formats['odd_key'])
                # DELIVERY TYPE
                worksheets[worksheets_keys[0]].write_string(row_num, 2, delivery['delivery_type'], formats['odd_key'])

            row_num += 1

    return row_num + 2


def _write_df(dataframe, header, worksheet, row_number, formats, highlight_rows=[]):
    """

    :param dataframe:
    :param header:
    :param worksheet:
    :param row_number:
    :param formats:
    :param highlight_rows:
    :return:
    """

    # TODO Borrar
    dataframe = pd.DataFrame({'product_name': ['laptop', 'printer', 'tablet', 'desk', 'chair'],
                              'price': [1200, 150, 300, 450, 200]})

    # Delivery header
    worksheet.merge_range(row_number, 0, row_number, len(dataframe.columns), header, formats['table_header'])
    row_number += 2

    # Table column headers
    worksheet.write_row(row_number, 0, tuple(dataframe.columns.to_list()), formats['table_header'])
    row_number += 1

    # Table data
    for col_num in range(dataframe.shape[1]):
        aux_counter = row_number
        for row, item in enumerate(dataframe.iloc[:, col_num].tolist()):
            if row in highlight_rows:
                worksheet.write(aux_counter, col_num, item, formats['highlighted_value'])
            elif aux_counter % 2 == 0:
                worksheet.write(aux_counter, col_num, item, formats['even_key'])
            else:
                worksheet.write(aux_counter, col_num, item, formats['odd_key'])

            aux_counter += 1

    row_number += (dataframe.shape[0] + 1)

    return row_number
