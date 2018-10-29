from matching_tools.data_model import *
from collections import deque
from decimal import Decimal
from pprint import pprint
from time import clock
import os, re, pickle, itertools
from matching_tools.print_highlighted import print_highlighted, format_highlighted


def read_pkl_from_folder(PATH, file_id=None):

    def cond(i, file_id):
        if file_id is not None:
            return i == file_id
        return True

    FILES = os.listdir(PATH)
    pprint(FILES)
    # file_dict = {str(i): FILES[i] for i in range(len(FILES))}
    arr_all = []
    for i in range(len(FILES)):
        # if FILES[i] == 'User Set 1.pkl':
        if not (FILES[i] in IGNORE):
            if cond(i, file_id):
                with open(PATH + FILES[i], 'rb') as f:
                    arr = pickle.load(f)
                print(len(arr))
                arr_ = []
                if file_id is not None:
                    for x in arr:
                        arr_.append(Transaction(set_id=x.set_id,  # + "|" + str(i),
                                                amount=x.amount,
                                                v_date=x.v_date,
                                                type_id=x.type_id,
                                                match_id=x.match_id,  # + "|" + str(i),
                                                item_id=x.item_id,  # + "|" + str(i),
                                                ref_key=x.ref_key,
                                                sl_type=x.sl_type,
                                                tt_match_id=x.tt_match_id,
                                                audit=""))
                else:
                    for x in arr:
                        arr_.append(Transaction(set_id=x.set_id + "|" + str(i),
                                                amount=x.amount,
                                                v_date=x.v_date,
                                                type_id=x.type_id,
                                                match_id=x.match_id + "|" + str(i),
                                                item_id=x.item_id + "|" + str(i),
                                                ref_key=x.ref_key,
                                                sl_type=x.sl_type,
                                                tt_match_id=x.tt_match_id,
                                                audit=""))
                arr_all += arr_
    return arr_all


def timedcall(fn, *args):
    t0 = clock()
    result = fn(*args)
    t1 = clock()
    return result, t1 - t0


def timedcalls(n, fn, *args):
    times = [timedcall(fn, *args)[1] for _ in range(n)]
    return min(times), sum(times)/float(len(times)), max(times)


def group_by_list(arr_xs: [Transaction], key=None) -> {str: [Transaction]}:
    arr_xs_tmp = sorted(arr_xs, key=key)
    return {k: list(v) for k, v in itertools.groupby(arr_xs_tmp, key=key)}


def get_net_amount(arr_xs: [Transaction]) -> Decimal:
    """
    Function calculates the total amount in a list of transactions
    :param arr_xs: [Transaction / PartialMatch / Transaction_uk]
    :return: Decimal()
    """
    return sum(x.amount for x in arr_xs)


def get_unique_match_ids(arr_xs: [Transaction]) -> {str}:
    """
    Function calculates the total number of unique match_ids
    :param arr_xs: [Transaction / PartialMatch / Transaction_uk]
    :return: set()
    """
    return set([x.match_id for x in arr_xs])


def display_match(trans: [Transaction], audit=None, comment=None, col="green"):

    trans_sorted = sorted(trans, key=lambda x: x.match_id)
    if comment is not None:
        print(comment)

    ref_key=re.sub(r'(SREF|LREF|:)', '', audit)
    for x in trans_sorted:
        print("{0:13.2f}, {1}, {2}, {3}".
              format(x.amount, x.v_date, x.match_id,
                     format_highlighted(x.ref_key, ref_key.split('|'), col)))

    if audit is not None:
        print()
        print_highlighted(audit, ref_key.split('|'), col)
    print()
    print("----------")


def unique_pair(transactions):
    """
    TODO: Tests
    :param transactions:
    :return:
    """

    arr_matched = []

    amt_dict = group_by_list(transactions, key=lambda x: x.amount)
    amt_dict_unk = {key: amt_dict[key] for key in amt_dict.keys()
                    if len(amt_dict[key]) == 1}

    gr_keys = amt_dict_unk.keys()
    for key in gr_keys:
        if key < 0:
            key_neg = 0 - key
            if key_neg in gr_keys:
                tr1 = amt_dict_unk[key][0]
                tr2 = amt_dict_unk[key_neg][0]
                arr_matched += [tr1, tr2]

    matched_ids = set([x.item_id for x in arr_matched])
    arr_unmatched = [x for x in transactions if x.item_id not in matched_ids]

    return arr_matched, arr_unmatched


def is_disjoint(connected_comps: [CC]) -> bool:
    """
    validation that all connected components are disjoint
    :param connected_comps:
    :return:
    """

    if len(connected_comps) == 0:
        return True

    num_joint = 0
    for i in range(len(connected_comps)):
        for j in range(i + 1, len(connected_comps)):
            x1 = connected_comps[i].ids
            x2 = connected_comps[j].ids
            if len(x1.intersection(x2)) != 0:
                num_joint += 1
    return num_joint == 0


def graph_merging(connected_comps: [CC]) -> [CC]:

    if len(connected_comps) == 0:
        return []

    # building a connectivity graph
    all_ids = set()
    for ccr in connected_comps:
        all_ids = all_ids.union(ccr.ids)

    graph = {}
    for item_id in all_ids:
        connected_to = set()
        unq_keys = set()
        for ccr in connected_comps:
            if item_id in ccr.ids:
                connected_to = connected_to.union(ccr.ids)
                unq_keys.add(ccr.comment)
        graph[item_id] = (connected_to - {item_id}, unq_keys)

    connected_comps_proc = []
    visited_all = set()

    for key in graph.keys():

        if key in visited_all:
            continue

        qt = deque()
        qt.append(key)
        visited_current = {key}
        unq_keys = set()

        while len(qt) != 0:
            element = qt.pop()
            states = graph[element]
            unq_keys = unq_keys.union(states[1])
            for state in states[0]:
                if state not in visited_current:
                    visited_current.add(state)
                    visited_all.add(state)
                    qt.append(state)
        cc = CC(ids=visited_current, comment="|".join(list(unq_keys)))
        connected_comps_proc.append(cc)

    return connected_comps_proc


def link_ledger(transactions,
                pattern: str,
                extractor=None,
                stats=False,
                verbose=False):
    """
    link_ledger creates connected components based on provided unique key
    :param transactions: list of transactions
    :param pattern: a specific transaction pattern / transaction type
    :param extractor: a function to extract a key
    :param stats: a logical key whether or not to display accuracy statistics
    :param verbose: a logical key whether or not to display auxiliary comments during execution
    :return: connected components (CC)
    """

    pairs = []  # a list with item id, unique_key pairs
    for tran in transactions:
        assert isinstance(tran, Transaction)

        match = re.search(pattern, tran.ref_key)
        if match:
            key_raw = match.group()
            key = key_raw if extractor is None else extractor(key_raw)
            pairs.append((tran, key))

            if verbose:
                print_highlighted(key_raw, key.split("|"), color="green")

    groups_dict = group_by_list(pairs, key=lambda x: x[1])

    connected_comps = []
    for value in groups_dict.values():
        if len(value) > 1:
            ids_set = set([x[0].item_id for x in value])
            comment = "LREF:{}".format(value[0][1])
            connected_comps.append(CC(ids=ids_set, comment=comment))

    if stats:
        # TODO: implement stats module
        pass

    return connected_comps


def link_source_ledger(transactions,
                stats=True,
                silent=True):

    """
    link_sorce_ledger creates connected components based on the unique
    key extracted from a source
    :param transactions:
    :param stats:
    :param silent:
    :return:
    """

    source_keys = extract_source_keys_ll(transactions)

    connected_comps = []
    for s_key in source_keys:
        if not silent:
            print(s_key)
        match = [trans for trans in transactions if re.search(s_key, trans.ref_key)]
        if len(match) > 1:
            cc = CC(ids=set([x.item_id for x in match]),
                    comment="SREF:{}".format(s_key))
            connected_comps.append(cc)

        if stats:
            # TODO: implement stats module
            pass

    return connected_comps


def extract_source_keys_ll(arr_xs):

    unique_keys = []
    for x in arr_xs:
        if x.sl_type == 'S':
            isninn = re.search(r"^ISNINN 0{4}[1-9]\d{9}", x.ref_key)
            if isninn:
                unique_keys.append(isninn.group()[11:])
            else:
                tmp = x.ref_key.split()[:2]
                if len(tmp) == 2:
                    unique_keys += [tmp[0], tmp[1]]

    return set([extract_key(unq_key) for unq_key in unique_keys if is_valid_key(unq_key)])


def is_credit_number_valid(x):
    try:
        nums = [int(i) for i in x]
    except:
        return False

    sum1 = 0
    sum2 = 0
    for i in nums[1::2]:
        sum1 += int(i)
    for i in nums[0::2]:
        sum2 += ((i * 2) % 10 + (i * 2) / 10)
    check_digit = sum1 + sum2
    if check_digit % 10 == 0:
        return True
    else:
        return False


def is_valid_key(key):

    #TODO: validate SREF keys MX0000000000 and IPUNINPSG0732AGRABCSG
    if re.search("AGRABCHH", key):
        return False

    if is_credit_number_valid(key):
        return False

    if re.search(r"IFT0\d", key):
        return False

    if re.search(r"(\(|\))", key):
        return False

    if re.search(r"^PIF\d+-ME", key):
        return False

    if re.search(r"[A-Z]{2}1\d{5}", key):
        return False

    if re.search(r"\d{2}/\d{2}/18", key):
        return False

    if re.search(r"\d{10}HE", key):
        return False

    if re.search(r"\+", key):
        return False

    if len(key) < 8:
        return False

    if re.search(r"\.", key):
        return False

    num_digits = sum(x.isdigit() for x in key)
    if num_digits <= 1:
        return False

    num_unique = set([x for x in key])
    if len(num_unique) == 1:
        return False

    return True


def extract_key(key):

    if re.search(r"^[A-Z]{2}00[1-9]\d{7}", key):
        return key[4:12]

    if re.search(r"^EFX\d{4}", key):
        return key[:7]
    return key