import pandas as pd
import numpy as np
import os
import datetime
from reports.data_extraction.database.controllers.database_controller import get_data
from reports.lead_report.controllers.clean_controller import clean_pipeline
from pathlib import Path


def generate_report(clients: list):
    """
    Main function in this script. It gets the lead delivery report for the client or set of clients given as parameters.

    :param clients: (list of strings) the list of clients as they appear in the client 'alias' whose lead delivery
    report is to be obtained.
    :return: (2x DataFrame) lead report DataFrame with the delivery format wanted by the recipient NGO and the config
    DataFrame that contains the whole set of characteristics applied in the present lead delivery report.
    """

    cleaned_data = get_report_data()
    leads_report, client_config_list = get_leads_info(data=cleaned_data, clients=clients)

    return leads_report, client_config_list


def get_report_data():
    """
    Loads road data from the SQL database as it is stored on it.
    It uses the 'lead_report' configuration file to decide which tables to load and how to merge them.
    It uses 'clean_pipeline' to perform some basic transformations and verifications on personal data.

    :return: (list of DataFrames) personal, deliveries, campaigns and privacy policy loaded DataFrames.
    """

    data = get_data(report_type='lead_report')
    personal_data = clean_pipeline(data[0])
    deliveries_data = data[1]
    campaigns_data = data[2]
    privacy_data = data[3]

    return [personal_data, deliveries_data, campaigns_data, privacy_data]


def get_leads_info(data: list, clients: list):
    """
    Collects all the information for each different delivery type from the ones included for each of the client in the
    client list. It gathers information regarding privacy policies, NGO deliveries, campaigns and it merges all of this
    together with the leads' personal data.

    :param data: (list of DataFrames) personal, deliveries, campaigns and privacy policy loaded DataFrames.
    :param clients: (list of strings) the list of clients as they appear in the client 'alias' whose lead delivery
    report is to be obtained.
    :return: (list of DataFrames, list of JSONs) list data structure. Each position stores data from a different client
    from the client list received as parameter. Client positions are also lists in which is position is occupied by a
    DataFrame with one of the delivery types that admits.

    Data schema [client_1_data: [delivery_df_1, delivery_df_2], client_2_data: [delivery_df_1], etc.]
    """

    current_directory = Path(os.path.dirname(__file__))
    config_file = os.path.join(current_directory.parent.absolute(), "config/client_requirements.json")

    # Load client requirements
    config = pd.read_json(config_file, orient='records')

    # Unpacking DataFrames from 'data' list
    personal_data = data[0]
    deliveries_data = data[1]
    campaigns_data = data[2]
    privacy_data = data[3]

    # Store the whole data structure
    # lead_report_dfs = [client_1, client_2, ..., client_n]
    # client_1 = [delivery_1, delivery_2, ..., delivery_n]
    lead_report_dfs = list()

    # Store clients' configuration
    client_config_list = list()

    for client in clients:
        for client_config in config['client']:
            if client == client_config['alias']:

                client_config_list.append(client_config)

                privacy_data = filter_privacy_data(data=privacy_data, filters=client_config)
                deliveries_data = filter_deliveries_data(data=deliveries_data, filters=client_config,
                                                         months=client_config['months'])
                campaigns_data = filter_campaigns_data(data=[campaigns_data, privacy_data])

                # Create a list of dataframes with all the delivery types of each client
                client_deliveries = list()

                # The only data that may vary between deliveries of the same client is personal data
                for delivery in client_config['deliveries']:
                    personal_data_delivery = personal_data

                    client_deliveries.append(merge_data(personal_data=personal_data_delivery,
                                                        campaigns_data=campaigns_data,
                                                        deliveries_data=deliveries_data,
                                                        delivery=delivery))

                lead_report_dfs.append(client_deliveries)
                break

    return lead_report_dfs, client_config_list


def filter_personal_data(data: pd.DataFrame, delivery):
    """
    Filters the data and selects only those registers (from leads' personal data) that complies with the NGO listed
    requirements.

    :param data: (DataFrame) leads' personal data.
    :param delivery: (JSON) list of requirements imposed by the NGO that will received the leads report.
    :return: (DataFrame) data that meets the NGO requirements.
    """

    filters = list(delivery['filters'].values())

    # Apply delivery requirements
    data = data.query('&'.join(filters))

    return data


def filter_privacy_data(data: pd.DataFrame, filters):
    """
    Filters the data and selects only those registers (from leads' privacy policy choices) that complies with the NGO
    listed requirements.

    :param data: (DataFrame) leads' privacy policy choices.
    :param filters: (JSON) list of client characteristics.
    :return: (DataFrame) data that meets the NGO requirements.
    """

    client_id = filters['identifier']

    # Conditions
    privacy_policy_id = data['privacy_policy_id'] == client_id
    policy_accepted = data['checked'] == 1

    data = data[privacy_policy_id & policy_accepted]
    data = data.drop_duplicates(subset=['id'], keep='first')

    return data


def filter_deliveries_data(data: pd.DataFrame, filters, months):
    """
    Filters the data and selects only those registers (from leads' deliveries data) that complies with the NGO listed
    requirements.

    :param data: (DataFrame) leads' times delivered.
    :param filters: (JSON) list of client characteristics.
    :param months: (list) differences in months that should be taken into account. Examples:
        - months = [1]: will determine the number of times a lead has been delivered in the last month.
        - months = [1, 3]: will determine the number of times a lead has been delivered in the last month and in the
        last three months
    :return: (DataFrame) data that meets the NGO requirements.
    """

    current_date = datetime.datetime.now()

    # 1. Delete from data all the clients delivered to: the NGO received as parameter or listed in exclude_from_delivery
    excluded_ngos = filters['exclude_from_delivery']
    excluded_ngos.append(filters['alias'])

    delivered_users = data.loc[data['given_to'].isin(excluded_ngos)]['id'].unique().tolist()
    data = data[~data.id.isin(delivered_users)]

    # 2. Difference in months between current date and delivery dates
    data['months_number'] = ((current_date - data.loc[:, ['given_at']]) / np.timedelta64(1, 'M'))

    # 3. Number of times a user has been delivered
    result = data.loc[:, ['id', 'given_at']].groupby(['id']).count().reset_index()

    for i in months:
        result = result.join(
            data.loc[data['months_number'] <= i].loc[:, ['id', 'months_number']].groupby(['id']).count(),
            on='id',
            rsuffix='_{}month'.format(str(i)))

    # 4. Change column names
    columns = ['id', 'total_deliveries']
    columns.extend(['deliveries_{}m'.format(i) for i in months])
    result.columns = columns

    # 5. List of ONGs to which a user has been submitted
    submitted_ngos = data.groupby(['id'])['given_to'].apply(list).reset_index(name='given_to')
    result = result.merge(submitted_ngos, how='left', on='id')

    result.fillna(0, inplace=True)

    return result


def filter_campaigns_data(data: list, check_privacy_policy=False):
    """
    Filters the data and selects only those registers (from leads' supported campaigns) that complies with the NGO
    listed requirements.

    :param data: (list of DataFrames) [campaigns_data, privacy_data] leads' campaigns and privacy data.
    :param check_privacy_policy: (bool) flag to apply privacy policy restrictions or not.
    :return: (DataFrame) data that meets the NGO requirements.
    """

    required_columns = ['date', 'user_id', 'creator', 'ip', 'question', 'name_es']

    # Number of supports each lead has made
    supports = data[0].loc[:, ['user_id', 'question_id']].groupby(['user_id']).count().reset_index()
    supports.columns = ['user_id', 'total_supports']

    # Conditions
    # 1. Campaign must not have a parent_id (it should be the original campaign)
    # 2. User has had to accept the privacy policy terms
    condition_1 = data[0]['parent_id'] == 0

    if check_privacy_policy:
        condition_2 = data[0]['id'].isin(data[1]['user_id'].to_list())
        data[0] = data[0].loc[condition_1 & condition_2]
    else:
        data[0] = data[0].loc[condition_1]

    # Customize the 'creator' and 'question' columns
    data[0]['creator'] = data[0]['first_name_push'] + ' ' + data[0]['last_name_push']
    data[0]['question'] = data[0]['question_id'].astype(float).astype(int).astype(str) + ' - ' + data[0]['question']

    # Merge campaigns and privacy dataframes
    if check_privacy_policy:
        merged_data = data[0].merge(right=data[1],
                                    left_on=['user_id', 'privacy_policy_id'],
                                    right_on=['user_id', 'privacy_policy_id'],
                                    how='inner')

        merged_data = merged_data.loc[:, required_columns]
    else:
        merged_data = data[0].loc[:, required_columns]

    # In case of multiple supported campaigns by a single lead, we only keep the last (most recent) supported one
    merged_data = merged_data.drop_duplicates(subset='user_id', keep='last')

    # Add total number of supports
    merged_data = merged_data.merge(right=supports, on='user_id', how='left')

    return merged_data


def merge_data(personal_data: pd.DataFrame, campaigns_data: pd.DataFrame, deliveries_data: pd.DataFrame, delivery):
    """
    Merges and stacks together all the partial DataFrames into a single one that gathers all the information.

    :param personal_data: (DataFrame) leads' personal data.
    :param campaigns_data: (DataFrame) leads' supported campaigns.
    :param deliveries_data: (DataFrame) leads' times delivered.
    :param delivery: (JSON) list of requirements imposed by the NGO that will received the leads report.
    :return: (DataFrame) merged data.
    """

    columns = delivery['columns']
    extra_columns = ['telephoneConfirmed', 'total_supports']
    extra_columns.extend(deliveries_data.columns)
    extra_columns.remove('id')

    # Merge dataframes
    merged_df = personal_data.merge(campaigns_data, how='inner', left_on='id', right_on='user_id',
                                    suffixes=(None, "_campaign"))
    merged_df = merged_df.merge(deliveries_data, how='inner', on='id', suffixes=(None, "_delivery"))

    # Dates special formatting
    for date_column in delivery['date_columns']:
        merged_df[date_column] = merged_df[date_column].dt.strftime(delivery['date_format'])

    # Apply custom filters
    merged_df = filter_personal_data(data=merged_df, delivery=delivery)

    # Drop duplicated columns
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Drop unnecessary columns and create the ones that do not exist
    columns_list = list(columns.keys())
    columns_list.extend(extra_columns)

    merged_df = merged_df.reindex(columns_list, axis=1, fill_value='')

    # Rename and sort values
    merged_df = merged_df.rename(columns=columns)
    merged_df = merged_df.sort_values(['total_deliveries', 'telephoneConfirmed'], ascending=[True, False])

    return merged_df
