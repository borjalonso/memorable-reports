import pandas as pd
import numpy as np
import datetime
from nameparser import HumanName


def clean_pipeline(data: pd.DataFrame):
    """
    It performs all cleaning tasks included in the pipeline.

    Pipeline:
        1. Drop unwanted columns
        2. Clean telephone
        3. Clean age
        4. Clean first_name
        5. Clean last_name
        6. Get surname
        7. Get signature method

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned data.
    """

    required_columns = ['id',
                        'telephone',
                        'birthday',
                        'birthday_facebookuser',
                        'first_name',
                        'first_name_facebookuser',
                        'last_name',
                        'last_name_facebookuser',
                        'facebook_id',
                        'twitter_id']
    # Lead data may contain duplicates. It is important not to repeat the same transformations with duplicated registers
    aux_data = data.loc[:, required_columns].drop_duplicates(subset=['id'], keep='first')

    # 'required_columns' from the original dataset must be deleted except 'id' column. It will become the merging key
    required_columns.remove('id')
    data.drop(required_columns, axis=1, inplace=True)

    aux_data = clean_telephone(data=aux_data)
    aux_data = clean_age(data=aux_data)
    aux_data = clean_first_name(data=aux_data)
    aux_data = clean_last_name(data=aux_data)
    aux_data = get_surname(data=aux_data)
    aux_data = get_signature_method(data=aux_data)

    # Merging transformations on the original DataFrame
    data = data.merge(aux_data,
                      how='left',
                      left_on='id',
                      right_on='id')

    return data


def clean_telephone(data: pd.DataFrame):
    """
    Takes the lead report compound dataset and changes all empty valued telephones to null.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned age data.
    """

    data['telephone'].replace('', np.nan, inplace=True)

    return data


def clean_age(data: pd.DataFrame):
    """
    Takes the lead report compound dataset two age-related columns (profile.birthday, facebookuser.birthday). By
    comparing those columns values, the function will get the most suitable age value.

    - Compare profile.birthday and facebookuser.birthday columns:
        - If they are equal, then do nothing.
        - If one of them is null but the other is not, then take the one that is not null.
        - If they are not null but they are not equal, then take facebookuser.birthday.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned age data.
    """

    # Getting age-related columns
    birthday_df = data.loc[:, ['birthday', 'birthday_facebookuser']]

    # Combining the 'profile.birthday' and 'facebook.birthday' columns into one single column prioritizing the
    # 'facebook.birthday' value
    condition_1 = birthday_df['birthday'].isnull()
    condition_2 = (birthday_df['birthday'].notnull()) & (birthday_df['birthday_facebookuser'].notnull())

    birthday_df['birthday'] = np.where(condition_1 | condition_2, birthday_df['birthday_facebookuser'],
                                       birthday_df['birthday'])

    # Deleting unnecessary columns
    birthday_df.drop('birthday_facebookuser', axis=1, inplace=True)
    data.drop('birthday_facebookuser', axis=1, inplace=True)

    # Getting the age out of the birthday column
    birthday_df['age'] = birthday_df['birthday'].apply(_get_age)

    # Deleting negative ages, ages below 10 and ages above 90
    condition_3 = birthday_df['age'] < 10
    condition_4 = birthday_df['age'] > 90

    birthday_df['age'] = np.where(condition_3 | condition_4, np.nan, birthday_df['age'])

    # Setting the 'age' column in 'data'
    data['birthday'] = birthday_df['birthday']
    data['age'] = birthday_df['age']

    return data


def clean_first_name(data: pd.DataFrame):
    """
    Takes Osoigo's and Faccebook's lead names and compare them. In case some is missing, it takes the other prioritizing
    the one used in Osoigo's platoform.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned first name data.
    """

    # Getting name columns
    first_names_df = data.loc[:, ['first_name', 'first_name_facebookuser']]

    # Combining the 'profile.first_name' and 'facebook.first_name' columns into one single column prioritizing the
    # profile one
    condition_1 = first_names_df['first_name_facebookuser'].isnull()
    condition_2 = (first_names_df['first_name'].notnull()) & (first_names_df['first_name_facebookuser'].notnull())

    first_names_df['first_name'] = np.where(condition_1 | condition_2, first_names_df['first_name'],
                                            first_names_df['first_name_facebookuser'])

    # Setting the 'first_name' column in 'data'
    data['first_name'] = first_names_df['first_name'].str.title().str.strip()

    # Removing multiple whitespace with single whitespace
    data['first_name'] = data['first_name'].apply(lambda x: ' '.join(x.split()))
    data.drop('first_name_facebookuser', axis=1, inplace=True)

    # Changing in-between space in compound names to underscore sign.
    # This makes easy to divide the surname into 1st surname and 2nd surname
    data['first_name'] = data.first_name.str.replace(' ', '_')

    return data


def clean_last_name(data: pd.DataFrame):
    """
    Takes Osoigo's and Faccebook's lead names and compare them. In case some is missing, it takes the other prioritizing
    the one used in Osoigo's platoform.

    :param data: (DataFrame) input data.
    :return: (DataFrame) cleaned last name data.
    """

    # Getting name columns
    last_names_df = data.loc[:, ['last_name', 'last_name_facebookuser']]

    # Combining the 'profile.last_name' and 'facebook.last_name' columns into one single column prioritizing the
    # profile one
    condition_1 = last_names_df['last_name_facebookuser'].isnull()
    condition_2 = (last_names_df['last_name'].notnull()) & (last_names_df['last_name_facebookuser'].notnull())

    last_names_df['last_name'] = np.where(condition_1 | condition_2, last_names_df['last_name'],
                                          last_names_df['last_name_facebookuser'])

    # Last name to title
    last_names_df['last_name'] = last_names_df['last_name'].str.title()

    # Removing multiple whitespace with single whitespace
    last_names_df['last_name'] = last_names_df['last_name'].apply(lambda x: ' '.join(x.split()))

    data['last_name_full'] = last_names_df['last_name']
    data.drop('last_name_facebookuser', axis=1, inplace=True)

    return data


def get_surname(data: pd.DataFrame):
    """
    Gets first and second surnames (if they exist) from the registered leads' first and last names.

    :param data: (DataFrame) input data.
    :return: (DataFrame) first and second surname divided data.
    """

    aux_df = pd.DataFrame()

    aux_df['full_name'] = data['first_name'] + ' ' + data['last_name_full']

    # Getting middle name and last name from 'last_name_full'
    data['middle_name'] = aux_df['full_name'].apply(lambda x: HumanName(x).middle)
    data['last_name'] = aux_df['full_name'].apply(lambda x: HumanName(x).last)

    # Removing '_' sign in compound names
    data['first_name'] = data.first_name.str.replace('_', ' ')

    return data


def get_signature_method(data: pd.DataFrame):
    """
    Creates a new column with the signature method (Facebook, Twitter, Form).

    :param data: (DataFrame) input data.
    :return: (DataFrame) signature method additional column data.
    """

    data['signature_method'] = np.where(data['facebook_id'].notna(), 'Facebook',
                                        np.where(data['twitter_id'].notna(), 'Twitter', 'Formulario'))

    return data


def _get_age(birthday: datetime):
    """
    Converts given date to age.

    :param birthday: (datetime) birthday date to be converted to age.
    :return: (int) age.
    """

    now = datetime.datetime.now()

    return now.year - birthday.year - ((now.month, now.day) < (birthday.month, birthday.day))
