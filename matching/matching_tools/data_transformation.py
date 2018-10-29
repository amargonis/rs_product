import os, re, pickle
import pandas as pd
from decimal import Decimal
from pprint import pprint
from matching_tools.data_model import Transaction, IGNORE


def to_pkl(file_name: str) -> str:
    """
    convert file name "file_name.smth" -> "file_name.pkl"
    :param file_name: file_name.smth
    :return: file_name.pkl
    """
    return file_name.split('.')[0] + '.pkl'


def to_tuple(data_frame: pd.DataFrame) -> Transaction:
    """
    Function converts a pandas row into a named tuple with predefined schema.
    As part of conversion in merges reference fields into a simple string and
    removes trailing spaces, double spaces and possible "nan". As part of
    conversion it splits "Item Side" into source code ('S' - source, 'L' - ledger).

    :param data_frame: pandas data frame
    :return: Transaction
    """

    sl_type, item_type = data_frame['Item Side'].split()

    amount = abs(Decimal(data_frame['Amount']))
    if item_type == 'DR':
        amount = 0 - amount

    xs = data_frame['Ref 1'] + ' ' + \
         data_frame['Ref 2'] + ' ' + \
         data_frame['Ref 3'] + ' ' + \
         data_frame['Ref 4']

    xs = re.sub(r"nan", "", xs)
    xs = re.sub(r"^\s+", "", xs)
    xs = re.sub(r"\s+$", "", xs)
    xs = re.sub(r"\s+", " ", xs)
    set_id = data_frame['Set ID']

    return Transaction(set_id=set_id,
                       amount=amount,
                       v_date=data_frame['Value Date'],
                       type_id=item_type,
                       match_id=set_id + "|" + data_frame['Match ID'],
                       # match_id='X', # for testing only
                       item_id=set_id + "|" + data_frame['Item ID'],
                       ref_key=xs,
                       sl_type=sl_type,
                       tt_match_id=None,
                       audit="")


if __name__ == "__main__":

    # All the data
    PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC2/"
    PATH_OUT = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC2_PK/"

    # PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST/"
    # PATH_OUT = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST_PK/"

    # All the data
    # PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST_REM/"
    # PATH_OUT = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST_REM_PK/"

    FILES = os.listdir(PATH)
    pprint(FILES)

    for file_in in FILES:
        if not (file_in in IGNORE):
            try:
                print("Processing file:", file_in)
                trans = pd.read_excel(PATH + file_in, dtype=str, engine='xlrd')
                pprint(trans.shape)
                # trans = pd.read_csv(PATH + file_in, dtype=str, keep_default_na=False, encoding='latin1')
                trans_ = trans[trans['Item Side'].isin(['L CR', 'L DR', 'S CR', 'S DR'])]
                trans_ = trans_[trans_['Match ID'] != 'nan']
                arr = [to_tuple(trans_.iloc[i]) for i in range(trans_.shape[0])]
                print("Length after transformation:", len(arr))

                with open(PATH_OUT + to_pkl(file_in), 'wb') as f:
                    pickle.dump(arr, f)
            except Exception as e:
                print(e)
