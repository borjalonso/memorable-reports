import pandas as pd
import numpy as np
import datetime


def clean_pipeline(data: pd.DataFrame):
    """
    It performs all cleaning tasks included in the pipeline.

    Pipeline:
        1. Clean telephone
        2. Clean age

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned data.
    """

    data = clean_telephone(data=data)
    data = clean_age(data=data)

    return data


def filter_pipeline(data: pd.DataFrame, start_date: datetime, end_date: datetime, country: str, client: str):
    """
    It performs all filtering tasks included in the pipeline.

    Pipeline:
        1. Filter lead creation and delivery date
        2. Filter country
        3. Filter client

    :param data: (DataFrame) input data.
    :param start_date: (datetime) minimum (earlier) date in which leads were created.
    :param end_date: (datetime) maximum (closest) date in which leads were created.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :param client: (str) client to which the leads have been sent. None to omit this filter.
    :return: (DataFrame) cleaned data.
    """

    data = filter_leads_creation_date(lead_dataset=data, start_date=start_date, end_date=end_date)
    data = filter_leads_country(data, country=country)
    data = filter_leads_client(data, client=client)

    return data


def clean_telephone(data: pd.DataFrame):
    """
    Takes the monthly report compound dataset and changes all empty valued telephones to null.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned age data.
    """

    data['telephone'].replace('', np.nan, inplace=True)

    return data


def clean_age(data: pd.DataFrame):
    """
    Takes the monthly report compound dataset filtering the two age-related columns (age, birthday). By comparing those
    columns values, the function will clean the resultant age column.

    - Replace 0 valued ages by null.
    - Replace all negative valued ages by null.
    - Create a new column age_birthday.
    - Compare age and age_birthday columns:
        - If they are equal, then do nothing.
        - If one of them is null but the other is not, then take the one that is not null.
        - If they are not null but they are not equal, then take age_birthday.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned age data.
    """

    # Getting the age out of the birthday column
    age_df = data.loc[:, ['age', 'birthday']]
    age_df['birthday'] = pd.to_datetime(age_df['birthday'])
    age_df['age_birthday'] = age_df['birthday'].apply(_get_age)

    # Combining the 'age' and 'age_birthday' columns into one single column prioritizing the 'age_birthday' value
    condition_1 = age_df['age'].isnull()    # The age is null
    condition_2 = age_df['age'] == 0        # No age provided by the user
    condition_3 = (age_df['age'].notnull()) & (age_df['age_birthday'].notnull())    # Age provided and included in facebook's user profile

    age_df['age'] = np.where(condition_1 | condition_2 | condition_3, age_df['age_birthday'], age_df['age'])

    # Deleting negative ages, ages below 10 and ages above 90
    condition_4 = age_df['age'] < 10
    condition_5 = age_df['age'] > 90

    age_df['age'] = np.where(condition_4 | condition_5, np.nan, age_df['age'])

    # Replacing the 'age' column in 'data' DataFrame by the 'age' column 'age_df' DataFrame
    data['age'] = age_df['age']

    return data


def _get_age(birthday: datetime):
    """
    Converts given date to age.

    :param birthday: (datetime) birthday date to be converted to age.
    :return: (int) age.
    """

    now = datetime.datetime.now()

    return now.year - birthday.year - ((now.month, now.day) < (birthday.month, birthday.day))


def filter_leads_creation_date(lead_dataset: pd.DataFrame, start_date: datetime, end_date: datetime):
    """
    Returns the lead dataset with only those leads whose 'creation_date' matches the month and year given as parameters.

    :param lead_dataset: (pd.DataFrame) dataset to filter.
    :param start_date: (datetime) minimum (earlier) date in which leads were created.
    :param end_date: (datetime) maximum (closest) date in which leads were created.
    :return: (pd.DataFrame) filtered dataset. In this dataset will only appear those leads whose 'creation_date' is
    between start_date and end_date.
    """

    # If no start_date nor end_date is provided, then the whole database is considered
    if (start_date is None) & (end_date is None):
        result_df = lead_dataset

    # Otherwise, the data must be filtered according to the start_date - end_date period
    else:
        if start_date is None:
            result_df = lead_dataset[(lead_dataset['created_at'] <= end_date)]

        else:
            result_df = lead_dataset[(lead_dataset['created_at'] >= start_date) &
                                     (lead_dataset['created_at'] < end_date)]

        # Filter delivery date: undelivered leads and leads delivered within a valid time range must be on the report
        result_df = result_df[(result_df['given_at'] <= end_date) | (result_df['given_at'].isna())]

    return result_df


def filter_leads_country(lead_dataset: pd.DataFrame, country: str):
    """
    Returns the lead dataset with only those leads whose 'country_name' matches the country given as parameter.

    :param lead_dataset: (pd.DataFrame) dataset to filter.
    :param country: (str) country where leads were created. None to take the entire database of leads.
    :return: (dataframe) dataframe with leads matching the country given as parameter.
    """

    result_df = lead_dataset

    if country is not None:
        result_df = lead_dataset[lead_dataset['name'] == country]

    return result_df


def filter_leads_client(lead_dataset: pd.DataFrame, client: str):
    """
    Returns the lead dataset with only those leads whose delivery client matches the client given as parameter.

    :param lead_dataset: (pd.DataFrame) dataset to filter.
    :param client: (str) client to which the leads have been sent. None to omit this filter.
    :return: (dataframe) dataframe with leads matching the client given as parameter.
    """

    result_df = lead_dataset

    if client is not None:
        result_df = lead_dataset[lead_dataset['given_to'] == client]

    return result_df


def _add_months(source_date: datetime, months: int):
    """
    Creates a date from a given source date by adding the specified increment in months.
    NOTE: It will always return a date with day=1st.

    :param source_date: (datetime) baseline date.
    :param months: (int) number of months to add to the source date.
    :return: (datetime) resultant date.
    """

    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = 1
    return datetime.datetime(year, month, day)
