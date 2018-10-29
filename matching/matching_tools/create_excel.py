from matching_tools.utils import *
import pandas as pd

IGNORE = {".DS_Store"}


if __name__ == "__main__":

    PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC2_PK/"
    # arr_all = read_pkl_from_folder(PATH, file_id=3)
    arr_all = read_pkl_from_folder(PATH)

    # removing duplicates
    dict_item_id = group_by_list(arr_all, key=lambda x: x.item_id)
    arr_all_xs = []
    for val in dict_item_id.values():
        arr_all_xs.append(val[0])

    data = pd.DataFrame(arr_all, columns=['Set ID', 'Amount', 'Value Date', 'Type',
                                          'Match ID', 'Item ID', 'REF_KEY', 'SL_type', 'TT_MATCH_KEY', 'AUDIT'])
    PATH2 = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC2_EX/"
    data.to_excel(PATH2 + 'ALL_DATA.xlsx', index=False, header=True)
