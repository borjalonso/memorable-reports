import pandas as pd
import xlsxwriter
import os
import datetime


def to_excel(start_date: datetime, end_date: datetime, country: str, client: str, data: list, output_path='',
             file_name=''):
    """
    Exports all metrics and statistics calculated in an excel sheet.

    :param start_date: (datetime) minimum (earlier) date in which leads were created. None to take the entire database.
    :param end_date: (datetime) maximum (closest) date in which leads were created. None to take the entire database.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :param client: (str) client to which the leads have been sent. None to omit this filter.
    :param data: (list) list of dictionaries and dataframes that contain the metrics and statistics.
    :param output_path:
    :param file_name:
    :return: void
    """

    output_path = r'C:\Users\borja\Desktop'

    # Month, year and country formatting
    header, file_name = _create_filename(start_date=start_date, end_date=end_date, country=country, client=client)

    print('header={0}'.format(header))
    print('file_name={0}'.format(file_name))

    # Create a workbook and add a worksheet
    workbook = xlsxwriter.Workbook(os.path.join(output_path, file_name),
                                   {'default_date_format': 'dd/mm/yyyy'})

    # Set workbook properties
    _set_workbook_properties(workbook=workbook)

    # Adding data to the worksheet
    _add_workbook_data(workbook=workbook, header=header, data=data, start_date=start_date, end_date=end_date,
                       country=country)


def _create_filename(start_date: datetime, end_date: datetime, country: str, client: str):
    """
    It formats the output header and filename as follows:
    Header  : 'DATABASE REPORT [From: [start_date] | To: [end_date] | Country: [country]]'
    Filename: 'all_data_month_from_[start_date]_to_[end_date]_[country].xlsx'

    :param start_date: (datetime) minimum (earlier) date in which leads were created. None to take the entire database.
    :param end_date: (datetime) maximum (closest) date in which leads were created. None to take the entire database.
    :param country: (string) country to generate the monthly report.
    :return: (string, string) output header and filename.
    """

    header = 'DATABASE REPORT'

    if (start_date is None) & (end_date is None):
        header += ' [From: All | To: All |'
        file_name = 'all_data'

    else:

        if start_date is None:
            from_str = 'First record'
        else:
            from_str = start_date.strftime('%d-%m-%Y')

        if end_date is None:
            to_str = 'Last record'
        else:
            end_date = end_date - datetime.timedelta(days=1)
            to_str = end_date.strftime('%d-%m-%Y')

        header += ' [From: {0} | To: {1} |'.format(from_str.replace('-', '/'), to_str.replace('-', '/'))
        file_name = 'all_data_from_{0}_to_{1}'.format(from_str.lower().replace(' ', '-'),
                                                      to_str.lower().replace(' ', '-'))

    # Country formatting
    if country is None:
        header += ' Country: All'
        file_name += '_all_countries'.format(country)

    else:
        header += ' Country: {0}'.format(country)
        file_name += '_{0}'.format(country)

    # Client formatting
    if client is None:
        header += ']'

    else:
        header += ' | Client: {0}]'.format(client)
        file_name += '_{0}'.format(client)

    return header, file_name + '.xlsx'


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
        'title': 'Osoigo monthly report spreadsheet',
        'subject': 'Monthly report',
        'author': 'Borja Alonso Valderrey',
        'manager': 'Borja Alonso Valderrey',
        'company': 'Osoigo.com',
        'created': datetime.datetime.now(),
        'comments': 'Created with Python and XlsxWriter'})


def _add_workbook_data(workbook, header: str, data: list, start_date: datetime, end_date: datetime, country: str):
    """
    Prepares and formats the data that will be part of the output Excel file.

    :param workbook: (xlsxwriter.Workbook) xlsxwriter Excel object.
    :param header: (str) workbook header.
    :param data: (dictionaries list) statistics to fill in the workbook.
    :param start_date: (datetime) minimum (earlier) date in which leads were created. None to take the entire database.
    :param end_date: (datetime) maximum (closest) date in which leads were created. None to take the entire database.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :return: void
    """

    # Formats
    formats = {
        'header': workbook.add_format({'bold': True,
                                       'font_size': 22}),
        'leads_total': workbook.add_format({'bold': True,
                                            'bg_color': '#F6BC59',
                                            'font_size': 22,
                                            'font_color': '#FFFFFF',
                                            'align': 'right'}),
        'title': workbook.add_format({'bold': True,
                                      'bg_color': '#F6BC59',
                                      'font_size': 14,
                                      'font_color': '#FFFFFF'}),
        'warning': workbook.add_format({'bold': True,
                                        'font_color': '#FC3003'}),
        'bold': workbook.add_format({'bold': True,
                                     'font_size': 12}),
        'highlighted_key': workbook.add_format({'bold': True,
                                                'bg_color': '#FAF1E2',
                                                'font_size': 12,
                                                'top': 1,
                                                'top_color': '#F6BC59',
                                                'bottom': 1,
                                                'bottom_color': '#F6BC59'}),
        'odd_key': workbook.add_format({'bold': True,
                                        'bg_color': '#F5F5F5',
                                        'font_size': 12}),
        'even_key': workbook.add_format({'bold': True,
                                        'font_size': 12}),
        'highlighted_value': workbook.add_format({'num_format': '#,##0',
                                                  'bg_color': '#FAF1E2',
                                                  'font_size': 12,
                                                  'top': 1,
                                                  'top_color': '#F6BC59',
                                                  'bottom': 1,
                                                  'bottom_color': '#F6BC59'}),
        'odd_value': workbook.add_format({'num_format': '#,##0',
                                          'bg_color': '#F5F5F5',
                                          'font_size': 12}),
        'even_value': workbook.add_format({'num_format': '#,##0',
                                          'font_size': 12}),
        'df_header': workbook.add_format({'bold': True,
                                          'bg_color': '#F6E5C6',
                                          'font_size': 12,
                                          'align': 'center'})
    }

    # Worksheets
    worksheets = {
        'worksheet_1': workbook.add_worksheet('Main metrics'),
        'worksheet_2': workbook.add_worksheet('Lead delivery metrics')
    }

    for key in worksheets.keys():
        worksheets[key].hide_gridlines(option=2)

    # Print header
    worksheets['worksheet_1'].write_string(0, 0, header, formats['header'])
    worksheets['worksheet_1'].write(1, 0, None)

    # Excel is formatted differently whether there are registers to write in or not
    if len(data) == 0:
        if start_date is not None:
            start_date = start_date.strftime('%d/%M/%Y')
        if end_date is not None:
            end_date = end_date.strftime('%d/%M/%Y')

        worksheets['worksheet_1'].write_string(2, 0, 'No records found for your parameter selection.',
                                               formats['warning'])
        worksheets['worksheet_1'].write_string(3, 0, 'Your selection: month={0} year={1} country={2}'.format(
            start_date, end_date, country), formats['warning'])

    else:
        worksheets['worksheet_1'].write(2, 1, data[0]['number_users'], formats['leads_total'])
        worksheets['worksheet_1'].write(3, 0, None)

        # 1) Connectivity statistics
        worksheets['worksheet_1'].merge_range('A5:B5', 'CONNECTIVITY AND PARTICIPATION', formats['title'])
        worksheets['worksheet_1'].write(5, 0, None)

        row_num = 6
        row_num = _write_dict(dictionary=data[0], worksheet=worksheets['worksheet_1'], row_number=row_num,
                              formats=formats)

        # 2) Typology statistics
        row_num += 2
        worksheets['worksheet_1'].merge_range('A{0}:B{0}'.format(row_num), 'USER TYPOLOGY', formats['title'])
        worksheets['worksheet_1'].write(row_num, 0, None)

        row_num += 1
        row_num = _write_df(dataframe=data[1], worksheet=worksheets['worksheet_1'], row_number=row_num,
                            formats=formats, add_index=True, highlight_rows=list(range(0, 25, 4)))

        # 3) Delivered leads
        row_num += 2
        worksheets['worksheet_1'].merge_range('A{0}:B{0}'.format(row_num), 'DELIVERED LEADS', formats['title'])
        worksheets['worksheet_1'].write(row_num, 0, None)

        # 3.1) Writing group3_dict
        row_num += 1
        row_num = _write_dict(dictionary=data[2][0], worksheet=worksheets['worksheet_1'], row_number=row_num,
                              formats=formats)

        # 3.2) Writing times_delivered_df
        _write_df(dataframe=data[2][1], worksheet=worksheets['worksheet_2'], row_number=2, formats=formats)

        # 3.3) Writing leads_by_times_delivered_df
        row_num += 1
        row_num = _write_df(dataframe=data[2][2], worksheet=worksheets['worksheet_1'], row_number=row_num,
                            formats=formats)

        # 3.4) Writing leads_by_ong_df
        row_num += 1

        row_num = _write_df(dataframe=data[2][3], worksheet=worksheets['worksheet_1'], row_number=row_num,
                            formats=formats)

        # 4) User recurrence statistics
        row_num += 2
        worksheets['worksheet_1'].merge_range('A{0}:B{0}'.format(row_num), 'USER RECURRENCE', formats['title'])
        worksheets['worksheet_1'].write(row_num, 0, None)

        row_num += 1
        row_num = _write_dict(dictionary=data[3], worksheet=worksheets['worksheet_1'], row_number=row_num,
                              formats=formats)

        # 5) User age statistics
        row_num += 2
        worksheets['worksheet_1'].merge_range('A{0}:B{0}'.format(row_num), 'USER AGE', formats['title'])
        worksheets['worksheet_1'].write(row_num, 0, None)

        row_num += 1
        row_num = _write_dict(dictionary=data[4], worksheet=worksheets['worksheet_1'], row_number=row_num, formats=formats)

        # 6) User privacy statistics
        row_num += 2
        worksheets['worksheet_1'].merge_range('A{0}:B{0}'.format(row_num), 'USER-SPONSORED', formats['title'])
        worksheets['worksheet_1'].write(row_num, 0, None)

        row_num += 1
        _write_df(dataframe=data[5], worksheet=worksheets['worksheet_1'], row_number=row_num,
                  formats=formats, add_index=True, highlight_rows=list(range(0, 6, 3)))

    # Close the Workbook object and write the XLSX file
    workbook.close()


def _write_dict(dictionary: dict, worksheet, row_number: int, formats: dict):
    """
    Writes a dictionary in the output excel file.

    :param dictionary: (dict) dictionary to be printed in the excel file.
    :param worksheet: (xlsxwriter.worksheet) excel worksheet in which to write.
    :param row_number: (int) first row number to write the dictionary in.
    :params formats: (dict) dictionary that contains the whole set of style parameters to format the excel.
    :return: (int) last row number in which there is data.
    """

    for key, value in zip(dictionary.keys(), dictionary.values()):
        if row_number % 2 == 0:
            worksheet.write(row_number, 0, key, formats['odd_key'])
            worksheet.write(row_number, 1, value, formats['odd_value'])
        else:
            worksheet.write(row_number, 0, key, formats['even_key'])
            worksheet.write(row_number, 1, value, formats['even_value'])

        row_number += 1

    worksheet.write(row_number, 0, None)

    return row_number


def _write_df(dataframe: pd.DataFrame, worksheet, row_number: int, formats: dict, add_index=False, highlight_rows=[]):
    """
    Writes a dataframe in the output excel file.

    :param dataframe: (dataframe) dataframe to be printed in the excel file.
    :param worksheet: (xlsxwriter.worksheet) excel worksheet in which to write.
    :param row_number: (int) first row number to write the dictionary in.
    :param formats: (dict) dictionary that contains the whole set of style parameters to format the excel.
    :param add_index: (bool) flag used for the function to decide if dataframe indexes must be also printed or not.
    :param highlight_rows: (list) list of row numbers that will be printed with the highlight format.
    :return: (int) last row number in which there is data.
    """

    for col_num, col_name in enumerate(dataframe.columns.to_list()):
        worksheet.write(row_number, col_num + int(add_index), col_name, formats['df_header'])

    row_number += 1
    col_increment = 0

    if add_index:
        col_increment += 1

        for row, index in enumerate(dataframe.index.to_list()):
            if row in highlight_rows:
                worksheet.write(row + row_number, 0, index, formats['highlighted_key'])
            elif (row + row_number) % 2 == 0:
                worksheet.write(row + row_number, 0, index, formats['odd_key'])
            else:
                worksheet.write(row + row_number, 0, index, formats['even_key'])

    for col_num in range(dataframe.shape[1]):
        aux_counter = row_number
        for row, item in enumerate(dataframe.iloc[:, col_num].tolist()):
            if row in highlight_rows:
                worksheet.write(aux_counter, col_num + col_increment, item, formats['highlighted_value'])
            elif aux_counter % 2 == 0:
                worksheet.write(aux_counter, col_num + col_increment, item, formats['odd_value'])
            else:
                worksheet.write(aux_counter, col_num + col_increment, item, formats['even_value'])

            aux_counter += 1

    row_number += dataframe.shape[0]

    worksheet.write(row_number, 0, None)

    return row_number
