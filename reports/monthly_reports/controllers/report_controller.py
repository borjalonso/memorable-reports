import pandas as pd
from reports.data_extraction.database.controllers.database_controller import get_data
from reports.monthly_reports.controllers.clean_controller import clean_pipeline, filter_pipeline


def generate_report(start_date, end_date, country: str, client: str):
    """
    Gets the data ready to generate the report. It is responsible for collecting the data and get the stats.

    :param start_date: (datetime) minimum (earlier) date in which leads were created. None to take the entire database.
    :param end_date: (datetime) maximum (closest) date in which leads were created. None to take the entire database.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :param client: (str) client to which the leads have been sent. None to omit this filter.
    :return:
    """

    report_data = get_report_data(start_date=start_date, end_date=end_date, country=country, client=client)
    report_stats = get_stats(data=report_data)

    return report_stats


def get_report_data(start_date, end_date, country: str, client: str):
    """
    Executes the data collection pipeline to get all the required data to generate the 'Database monthly report' with
    all the lead data collected and generated within the month and year received as parameter.

    :param start_date: (datetime) minimum (earlier) date in which leads were created. None to take the entire database.
    :param end_date: (datetime) maximum (closest) date in which leads were created. None to take the entire database.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :param client: (str) client to which the leads have been sent. None to omit this filter.
    :return: (pd.DataFrame) dataset with the clean data generated within the month and year received as parameter.
    """

    data = get_data(report_type='database_dashboard')

    # 1) PERSONAL DATA
    # Filter the data
    filtered_data = filter_pipeline(data=data[0], start_date=start_date, end_date=end_date, country=country,
                                    client=client)
    # Clean the data
    cleaned_data = clean_pipeline(filtered_data)

    # 2) PRIVACY POLICY DATA

    return [cleaned_data, data[1]]


def get_stats(data: list):
    """
    Generates all the stats and gets aggregated data to compose the 'Database monthly report'.
    It works by creating a dictionary of pairs key-value for each of these topics:
        1. Total volume of users.
        2. Connectivity and user participation.
        3. User typology.
        4. Count of leads delivered.
        5. Users by age.
    In the end, all of the partial dictionaries are stacked together and will contribute to the generation of the final
    report.

    :param data: (list) datasets with the clean data that will be used to generate the report.
    :return: (list) datasets containing the calculated metrics.
    """

    result = []

    # 1) Personal data
    if not data[0].empty:
        connectivity_results = _get_connectivity_stats(data[0])
        typology_results = _get_typology_stats(data[0])
        delivery_results_list = _get_deliveries_stats(data[0])
        recurrence_results = _get_recurrence_stats(data[0])
        age_results = _get_age_stats(data[0])
        privacy_results = _get_privacy_stats(personal_data=data[0], privacy_data=data[1])

        result = [connectivity_results, typology_results, delivery_results_list, recurrence_results, age_results,
                  privacy_results]

    return result


def _get_connectivity_stats(data: pd.DataFrame):
    """
    Gets all required connectivity stats from the given data.

    :param data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dict) connectivity stats.
    """

    # Group 1: connectivity and participation
    group1_metrics = ['number_users',
                      'null_id',
                      'share_data_with_sponsors',
                      'dont_share_data_with_sponsors',
                      'null_share_data',
                      'robinson',
                      'null_robinson',
                      'allow_notifications',
                      'null_allow_notifications',
                      'verified_email',
                      'not_verified_email',
                      'null_verified_email',
                      'null_valued_leads']

    # Creating dictionary from keys
    group1_dict = dict.fromkeys(group1_metrics, None)

    # Filling the dictionary with values
    group1_dict['number_users'] = data['id'].nunique()

    group1_data = data.loc[:, ['id', 'shareMyData', 'robinson', 'emailSubscribed', 'emailConfirmed']].drop_duplicates(
        subset=['id'])

    group1_dict['null_id'] = group1_data['id'].isna().sum()
    group1_dict['share_data_with_sponsors'] = group1_data['shareMyData'].value_counts().reindex([1], fill_value=0)[1]
    group1_dict['dont_share_data_with_sponsors'] = group1_data['shareMyData'].value_counts().reindex([0], fill_value=0)[0]
    group1_dict['null_share_data'] = group1_data['shareMyData'].isna().sum()
    group1_dict['robinson'] = group1_data['robinson'].value_counts().reindex([1], fill_value=0)[1]
    group1_dict['null_robinson'] = group1_data['robinson'].isna().sum()
    group1_dict['allow_notifications'] = group1_data['emailSubscribed'].value_counts().reindex([1], fill_value=0)[1]
    group1_dict['null_allow_notifications'] = group1_data['emailSubscribed'].isna().sum()
    group1_dict['verified_email'] = group1_data['emailConfirmed'].value_counts().reindex([1], fill_value=0)[1]
    group1_dict['not_verified_email'] = group1_data['emailConfirmed'].value_counts().reindex([0], fill_value=0)[0]
    group1_dict['null_verified_email'] = group1_data['emailConfirmed'].isna().sum()

    null_valued_leads = set()
    for column in group1_data.columns:
        null_valued_leads.update(group1_data[group1_data[column].isnull()].id.tolist())

    group1_dict['null_valued_leads'] = len(null_valued_leads)

    return group1_dict


def _get_typology_stats(data: pd.DataFrame):
    """
    Gets user typology metrics.

    :param data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dataframe) dataframe containing the whole set of metrics related with user typology.
    """

    group2_data = data.loc[:, ['id', 'telephone', 'emailConfirmed', 'telephoneConfirmed', 'facebook_id',
                               'twitter_id', 'given_to', 'age', 'gender']].drop_duplicates(subset=['id'])

    # Conditions
    verified_email = group2_data['emailConfirmed'] == 1
    verified_phone = group2_data['telephoneConfirmed'] == 1
    facebook_user = group2_data['facebook_id'].notna()
    twitter_user = group2_data['twitter_id'].notna()
    form_user = (group2_data['facebook_id'].isna()) & (group2_data['twitter_id'].isna())
    phone_included = group2_data['telephone'].notna()
    delivered = group2_data['given_to'].notna()
    age_enough = group2_data['age'] >= 30
    man = group2_data['gender'] == 'male'
    woman = group2_data['gender'] == 'female'

    # Group 2: user typology
    group2_metrics = {'DELIVERABLE_LEADS': phone_included & age_enough,
                      'deliverable_leads_men': phone_included & age_enough & man,
                      'deliverable_leads_women': phone_included & age_enough & woman,
                      'deliverable_leads_without_gender': phone_included & age_enough & ~(man | woman),
                      'VERIFIED_MAIL_VERIFIED_PHONE': verified_email & verified_phone,
                      'verified_mail_verified_phone_facebook': verified_email & verified_phone & facebook_user,
                      'verified_mail_verified_phone_twitter': verified_email & verified_phone & twitter_user,
                      'verified_mail_verified_phone_form': verified_email & verified_phone & form_user,
                      'VERIFIED_PHONE': ~verified_email & verified_phone,
                      'verified_phone_facebook': ~verified_email & verified_phone & facebook_user,
                      'verified_phone_twitter': ~verified_email & verified_phone & twitter_user,
                      'verified_phone_form': ~verified_email & verified_phone & form_user,
                      'VERIFIED_MAIL_INCLUDED_PHONE': verified_email & ~verified_phone & phone_included,
                      'verified_mail_included_phone_facebook': verified_email & ~verified_phone & phone_included & facebook_user,
                      'verified_mail_included_phone_twitter': verified_email & ~verified_phone & phone_included & twitter_user,
                      'verified_mail_included_phone_form': verified_email & ~verified_phone & phone_included & form_user,
                      'VERIFIED_MAIL_MISSING_PHONE': verified_email & ~verified_phone & ~phone_included,
                      'verified_mail_missing_phone_facebook': verified_email & ~verified_phone & ~phone_included & facebook_user,
                      'verified_mail_missing_phone_twitter': verified_email & ~verified_phone & ~phone_included & twitter_user,
                      'verified_mail_missing_phone_form': verified_email & ~verified_phone & ~phone_included & form_user,
                      'INCLUDED_PHONE': ~verified_email & ~verified_phone & phone_included,
                      'included_phone_facebook': ~verified_email & ~verified_phone & phone_included & facebook_user,
                      'included_phone_twitter': ~verified_email & ~verified_phone & phone_included & twitter_user,
                      'included_phone_form': ~verified_email & ~verified_phone & phone_included & form_user,
                      'MISSING_PHONE': ~verified_email & ~verified_phone & ~phone_included,
                      'missing_phone_facebook': ~verified_email & ~verified_phone & ~phone_included & facebook_user,
                      'missing_phone_twitter': ~verified_email & ~verified_phone & ~phone_included & twitter_user,
                      'missing_phone_form': ~verified_email & ~verified_phone & ~phone_included & form_user,
                      'null_email_confirmed': None,
                      'null_telephone_confirmed': None,
                      'null_valued_leads': None
                      }

    # Creating dictionary from keys
    group2_dict = dict.fromkeys(group2_metrics.keys(), [0, 0])

    # Creating dataframe from dictionary
    group2_df = pd.DataFrame.from_dict(group2_dict, orient='index',
                                       columns=['total', 'delivered'])

    for key in group2_metrics.keys():
        if group2_metrics[key] is not None:
            group2_df.loc[key, 'total'] = group2_data[group2_metrics[key]].shape[0]
            group2_df.loc[key, 'delivered'] = group2_data[delivered & group2_metrics[key]].shape[0]

    group2_df['undelivered'] = group2_df['total'] - group2_df['delivered']

    group2_df.loc['null_email_confirmed', 'total'] = group2_data['emailConfirmed'].isna().sum()
    group2_df.loc['null_telephone_confirmed', 'total'] = group2_data['telephoneConfirmed'].isna().sum()

    null_valued_leads = set()
    for column in ['emailConfirmed', 'telephoneConfirmed']:
        null_valued_leads.update(group2_data[group2_data[column].isnull()].id.tolist())

    group2_df.loc['null_valued_leads', 'total'] = len(null_valued_leads)

    return group2_df


def _get_deliveries_stats(data: pd.DataFrame):
    """
    Gets all required deliveries stats from the given data.

    :param data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dataframe) dataframe containing the whole set of metrics related with deliveries.
    """

    # Group 3: leads delivered
    group3_metrics = ['delivered_leads',
                      'undelivered_leads']

    # Creating dictionary from keys
    group3_dict = dict.fromkeys(group3_metrics, None)

    group3_data = data.loc[:, ['id', 'given_to']]

    undelivered = group3_data['given_to'].isna()

    group3_dict['delivered_leads'] = group3_data.drop_duplicates(subset=['id'], keep='first').loc[~undelivered].shape[0]
    group3_dict['undelivered_leads'] = group3_data.drop_duplicates(subset=['id'], keep='first').loc[
        undelivered].shape[0]

    # Volume of leads by times delivered
    #   Leads with nan have not been delivered
    times_delivered_df = group3_data.dropna()['id'].value_counts().reset_index()
    times_delivered_df.columns = ['user_id', 'times_delivered']
    leads_by_times_delivered_df = times_delivered_df.set_index(keys='user_id', drop=True).value_counts().sort_index().\
        reset_index()
    leads_by_times_delivered_df.columns = ['times_delivered', 'leads_volume']
    # volume_delivered.add_prefix('delivered_to_').add_suffix('_ONGs')

    # Volume of leads delivered by ONG
    leads_by_ong_df = group3_data['given_to'].value_counts().reset_index()
    leads_by_ong_df.columns = ['ong_name', 'leads_volume']

    return [group3_dict, times_delivered_df, leads_by_times_delivered_df, leads_by_ong_df]


def _get_recurrence_stats(data: pd.DataFrame):
    """
    Gets all required recurrence stats from the given data.

    :param data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dict) dictionary containing the whole set of metrics related with user recurrence.
    """

    # Group 4: recurrence of users (time between deliveries)
    group4_metrics = ['one_month',
                      'one_to_three_months',
                      'six_months',
                      'one_year',
                      'two_years',
                      'more_than_two_years']

    # Creating dictionary from keys
    group4_dict = dict.fromkeys(group4_metrics, None)

    # Tidy data attending to:
    #   1) It only has delivered registers.
    #   2) Lead deliveries are sorted by date in descending order.
    #   3) We only keep a maximum of two deliveries for each lead.
    group4_data = data[data.given_to.notnull()].loc[:, ['user_id', 'given_to', 'given_at']]. \
        sort_values(by=['user_id', 'given_at'], ascending=[True, False]). \
        groupby('user_id').head(2)

    # Getting difference (in months) between the two latest deliveries
    group4_data['diff_days'] = group4_data.groupby('user_id')['given_at'].diff(periods=-1)
    group4_data.dropna(subset=['diff_days'], inplace=True)

    group4_data['diff_days'] = group4_data['diff_days'].dt.days

    one_month = group4_data['diff_days'] <= 30
    one_to_three_months = group4_data['diff_days'].between(31, 3 * 30)
    six_months = group4_data['diff_days'].between((3 * 30) + 1, 6 * 30)
    one_year = group4_data['diff_days'].between((6 * 30) + 1, 365)
    two_years = group4_data['diff_days'].between(366, 2 * 365)
    more_than_two_years = group4_data['diff_days'] > (2 * 365) + 1

    group4_dict['one_month'] = group4_data.loc[one_month].shape[0]
    group4_dict['one_to_three_months'] = group4_data.loc[one_to_three_months].shape[0]
    group4_dict['six_months'] = group4_data.loc[six_months].shape[0]
    group4_dict['one_year'] = group4_data.loc[one_year].shape[0]
    group4_dict['two_years'] = group4_data.loc[two_years].shape[0]
    group4_dict['more_than_two_years'] = group4_data.loc[more_than_two_years].shape[0]

    return group4_dict


def _get_age_stats(data: pd.DataFrame):
    """
    Gets all required age stats from the given data.

    :param data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dict) dictionary containing the whole set of metrics related with age.
    """

    # Group 5: birthday analysis
    group5_metrics = ['without_age',
                      'with_age',
                      'less_than_25',
                      'from_25_to_29',
                      'from_30_to_39',
                      'from_40_to_49',
                      'from_50_to_59',
                      'more_than_59']

    # Creating dictionary from keys
    group5_dict = dict.fromkeys(group5_metrics, None)

    group5_data = data.loc[:, ['id', 'age']].drop_duplicates(subset=['id'])

    less_than_25 = group5_data['age'] < 25
    from_25_to_29 = group5_data['age'].between(25, 29)
    from_30_to_39 = group5_data['age'].between(30, 39)
    from_40_to_49 = group5_data['age'].between(40, 49)
    from_50_to_59 = group5_data['age'].between(50, 59)
    more_than_59 = group5_data['age'] > 59

    group5_dict['without_age'] = group5_data['age'].isna().sum()
    group5_dict['with_age'] = group5_data['age'].notna().sum()
    group5_dict['less_than_25'] = group5_data.loc[less_than_25].shape[0]
    group5_dict['from_25_to_29'] = group5_data.loc[from_25_to_29].shape[0]
    group5_dict['from_30_to_39'] = group5_data.loc[from_30_to_39].shape[0]
    group5_dict['from_40_to_49'] = group5_data.loc[from_40_to_49].shape[0]
    group5_dict['from_50_to_59'] = group5_data.loc[from_50_to_59].shape[0]
    group5_dict['more_than_59'] = group5_data.loc[more_than_59].shape[0]

    return group5_dict


def _get_privacy_stats(personal_data: pd.DataFrame, privacy_data: pd.DataFrame):
    """
    Gets all required privacy policy stats from the given data.

    :param personal_data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :param privacy_data: (pd.DataFrame) dataset with the clean data that will be used to generate the report.
    :return: (dict) (dataframe) dataframe containing the whole set of metrics related with privacy policy terms.
    """

    group3_data = personal_data.loc[:, ['id', 'given_to']].drop_duplicates(subset=['id'])
    group6_data = privacy_data.loc[:, ['id', 'privacy_policies_checkbox_id', 'checked']]

    # Get from group6 data, those leads from the selected time period (those included in group3 data)
    group6_data = group6_data.loc[group6_data['id'].isin(group3_data.id.to_list())]

    # Conditions
    osoigo_policy_displayed = group6_data['privacy_policies_checkbox_id'] == 1
    privacy_policy_accepted = group6_data['checked'] == 1
    privacy_policy_rejected = ~group6_data.id.isin(list(set(group6_data[privacy_policy_accepted]['id'])))
    id_non_sponsored_users = list(set(group6_data[osoigo_policy_displayed]['id']))
    non_sponsored_users = group6_data.id.isin(id_non_sponsored_users)

    id_delivered = list(set(group3_data.loc[group3_data['given_to'].notna()]['id']))
    delivered = group6_data.id.isin(id_delivered)

    # Group 6: privacy policy analysis
    group6_metrics = {'SPONSORED USERS': ~non_sponsored_users,
                      'sponsored_users_ACCEPTED_ANY_privacy_policy': ~non_sponsored_users & privacy_policy_accepted,
                      'sponsored_users_REJECTED_ALL_privacy_policy': ~non_sponsored_users & privacy_policy_rejected,
                      'NON-SPONSORED_USERS': non_sponsored_users,
                      'non-sponsored_users_ACCEPTED_ANY_privacy_policy': non_sponsored_users & privacy_policy_accepted,
                      'non-sponsored_users_REJECTED_ALL_privacy_policy': non_sponsored_users & privacy_policy_rejected}

    # Creating dictionary from keys
    group6_dict = dict.fromkeys(group6_metrics.keys(), [0, 0])

    # Creating dataframe from dictionary
    group6_df = pd.DataFrame.from_dict(group6_dict, orient='index',
                                       columns=['total', 'delivered'])

    for key in group6_metrics.keys():
        if group6_metrics[key] is not None:
            group6_df.loc[key, 'total'] = group6_data[group6_metrics[key]].drop_duplicates(['id']).shape[0]
            group6_df.loc[key, 'delivered'] = group6_data[delivered & group6_metrics[key]].drop_duplicates(['id']).shape[0]

    group6_df['undelivered'] = group6_df['total'] - group6_df['delivered']

    return group6_df
