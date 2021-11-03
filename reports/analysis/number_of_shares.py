import pandas as pd
import pyinputplus
import datetime
import os
from reports.monthly_reports.controllers.database_controller import get_data

pd.options.mode.chained_assignment = None

"""
This script provides an API to explore stats about NGOs lead shares.
"""


def filter_columns(data: pd.DataFrame):
    """
    Returns a DataFrame only with those columns that are needed to proceed with the Deliveries Analysis.
    Columns accepted:
        - user_id
        - created_at
        - age
        - telephone
        - name
        - given_to

    :param data: (pd.DataFrame) input lead data.
    :return: (pd.DataFrame) only with the columns needed to create the Deliveries Analysis
    """

    required_columns = ['user_id', 'created_at', 'age', 'telephone', 'name', 'given_to']

    return data.loc[:, required_columns]


def general_stats(data: pd.DataFrame, country):
    """
    Returns general stats about the number of leads stored in the database and the number of leads that meet all
    delivery requirements.

    :param data: (pd.DataFrame) input lead data.
    :param country: (str) country to segment on.
    :return: (dict) output metrics
    """

    metrics = ['total_leads',               # Total number of leads
               'total_leads_>30',           # Total number of leads aged 30 years or more
               'total_leads_>30_with_tlf']  # Total number of leads aged 30 years or more that registered a tlf

    if country is not None:
        metrics.append('total_leads_{0}'.format(country))
        metrics.append('total_leads_>30_{0}'.format(country))
        metrics.append('total_leads_>30_with_tlf_{0}'.format(country))

    # Creating dictionary from keys
    metrics_dict = dict.fromkeys(metrics, None)

    more_than_30 = 'age >= 30'
    telephone_registered = 'telephone.notna()'

    metrics_dict['total_leads'] = data['user_id'].nunique()
    metrics_dict['total_leads_>30'] = data.query(more_than_30)['user_id'].nunique()
    metrics_dict['total_leads_>30_with_tlf'] = data.query(more_than_30 + '&'
                                                          + telephone_registered)['user_id'].nunique()

    if country is not None:
        data = data[data['name'] == country]

        metrics_dict['total_leads_{0}'.format(country)] = data['user_id'].nunique()
        metrics_dict['total_leads_>30_{0}'.format(country)] = data.query(more_than_30)['user_id'].nunique()
        metrics_dict['total_leads_>30_with_tlf_{0}'.format(country)] = data.query(
            more_than_30 + '&' + telephone_registered)['user_id'].nunique()

    return metrics_dict


def deliveries_stats(data: pd.DataFrame, year: int, country):
    """
    Returns metrics about the average number of lead deliveries and the times each lead has been shared with an NGO.

    :param data: (pd.DataFrame) input lead data.
    :param year: (int) year for which statistics are to be obtained.
    :param country: (str) country to segment on.
    :return:
    """

    metrics = ['average_deliveries',        # Average number of deliveries
               'average_deliveries_year']   # Average number of deliveries in the selected year

    # Creating dictionary from keys
    metrics_dict = dict.fromkeys(metrics, None)

    # Delivery requirements
    more_than_30 = 'age >= 30'
    telephone_registered = 'telephone.notna()'

    if country is not None:
        data = data[data['name'] == country]

    deliverable_leads = data.query(more_than_30 + '&' + telephone_registered)

    # Creating column 'year'
    deliverable_leads['year'] = pd.DatetimeIndex(deliverable_leads['created_at']).year

    # Getting times each lead has been delivered.
    # Zero delivered leads are also counted
    deliveries = deliverable_leads.loc[deliverable_leads['given_to'].notna()]['user_id'].value_counts().\
        rename_axis('user_id').reset_index(name='times_delivered')
    zero_deliveries = pd.DataFrame(data=deliverable_leads.loc[deliverable_leads['given_to'].isna()]['user_id'],
                                   columns=['user_id']).drop_duplicates().reset_index(drop=True)
    zero_deliveries['times_delivered'] = 0
    deliveries = pd.concat([deliveries, zero_deliveries]).sort_values(by='times_delivered', ascending=False)

    # Getting times each lead has been delivered for those leads created on a certain year.
    # Zero delivered leads are also counted
    deliveries_year = deliverable_leads.loc[deliverable_leads['given_to'].notna()].query('year == @year')['user_id']\
        .value_counts().rename_axis('user_id').reset_index(name='times_delivered')
    zero_deliveries = pd.DataFrame(deliverable_leads.loc[deliverable_leads['given_to'].isna()].query('year == @year')
                                   ['user_id'], columns=['user_id']).drop_duplicates().reset_index(drop=True)
    zero_deliveries['times_delivered'] = 0
    deliveries_year = pd.concat([deliveries_year, zero_deliveries]).sort_values(by='times_delivered', ascending=False)

    # Getting average deliveries stats
    metrics_dict['average_deliveries'] = deliveries['times_delivered'].mean()
    metrics_dict['average_deliveries_year'] = deliveries_year['times_delivered'].mean()

    return deliveries, deliveries_year, metrics_dict


def main():

    month = None
    year = None
    country = None

    # Taking input
    # 1) Monthly data or all available data
    response = pyinputplus.inputMenu(prompt='Do you wish to use monthly data? (Yes/No)\n'
                                            'NOTE: in case you want to use the whole database type \'No\'.\n',
                                     choices=['Yes', 'No'],
                                     numbered=True)

    # 2) Monthly data option
    #   2.1) Ask for a month
    #   2.2) Ask for a year
    if response == 'Yes':
        month = pyinputplus.inputMonth(prompt='Enter the month you wish to obtain the data: ')
        year = pyinputplus.inputInt(prompt='Enter the year you wish to obtain the data '
                                           '(format: yyyy, min: 2014): ',
                                    min=2014,
                                    max=datetime.datetime.now().year)

    data = filter_columns(get_data(month=month, year=year))

    # 3) All available data option
    #   3.1) Ask for a year to get the average number of deliveries
    if year is None:
        year = pyinputplus.inputInt(prompt='Enter the year you wish to get the average number of deliveries '
                                           '(format: yyyy, min: 2014): ',
                                    min=2014,
                                    max=datetime.datetime.now().year)

    # 4) Segment by country or get all countries data
    countries = list(data['name'].dropna().unique())
    countries.insert(0, 'No segment')
    response = pyinputplus.inputMenu(prompt='Choose a country to segment on:\n',
                                     choices=countries,
                                     numbered=True)

    if response != 'No segment':
        country = response

    dict_general_stats = general_stats(data=data, country=country)

    df_deliveries, df_deliveries_year, dict_average_deliveries = deliveries_stats(data=data,
                                                                                  year=year,
                                                                                  country=country)

    display_menu = 1

    while display_menu:
        # Menu to display the results
        choices = ['Print general statistics.',
                   'Print a deliveries summary.',
                   'Print {0} deliveries summary.'.format(year),
                   'Save as csv file (You will choose the destination folder).']
        response = pyinputplus.inputMenu(prompt='Choose an option:\n',
                                         choices=choices,
                                         numbered=True)

        if response == choices[0]:
            print(dict_general_stats)

        elif response == choices[1]:
            print(df_deliveries)

        elif response == choices[2]:
            print(df_deliveries_year)

        elif response == choices[3]:
            folder = pyinputplus.inputMenu(prompt='Where do you want to store the data?\n',
                                           choices=[r'C:\Users\borja\Desktop',
                                                    r'C:\Users\borja\Desktop\Borja\2. Data\4. Proyectos '
                                                    r'internos\20210408_Analisis_Entregas'],
                                           numbered=True)

            df_deliveries.to_csv(os.path.join(folder, 'all_deliveries.csv'), index=False)
            df_deliveries_year.to_csv(os.path.join(folder, '{0}_deliveries.csv'.format(year)), index=False)

            print('all_deliveries.csv and {0}_deliveries.csv files created in {1}'.format(year, folder))

        response = pyinputplus.inputMenu(prompt='Do you wish to perform any other operation?\n',
                                         choices=['Yes',
                                                  'No'],
                                         numbered=True)

        if response == 'No':
            display_menu = 0

            print('Exiting the program')


if __name__ == "__main__":
    main()
