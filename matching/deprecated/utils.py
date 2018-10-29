from deprecated.data_model import *
from deprecated.prematch_analytics import is_disjoint, graph_merging
from decimal import Decimal
import itertools, collections, re
import random

from pprint import pprint


IGNORE = {".DS_Store"}


def generate_hash():
    return random.getrandbits(128)


def groupby_list(arr_xs: [Transaction], key=None) -> {str: [Transaction]}:
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


def display_stats(arr_matched_len, matched_c_len, matched_n_len):
    print("Number of matched transactions: {}".format(arr_matched_len))
    if arr_matched_len > 0:
        print("Correct: {} ({}%)".format(matched_c_len, round(matched_c_len / arr_matched_len * 100, 2)))
        print("Incorrect: {} ({}%)".format(matched_n_len, round(matched_n_len / arr_matched_len * 100, 2)))


def remove_set_ids_duplicated(arr_xs):

    print("Removing set_ids with duplicates")
    xs = groupby_list(arr_xs, key=lambda x: x.item_id)
    set_id_black_list = set()
    for x in xs.values():
        if len(x) > 1:
            set_id_black_list.add(x[0].set_id)

    print("Original length", len(arr_xs))
    arr_xs_new = [x for x in arr_xs if x.set_id not in set_id_black_list]
    print("Final length after removing duplicates", len(arr_xs_new))

    return arr_xs_new


def check_duplicates(arr_xs):
    xs = groupby_list(arr_xs, key=lambda x: x.item_id)
    out = 0
    for x in xs.values():
        if len(x) > 1:
            pprint(x)
            out += 1
    return out == 0


def sss(arr_xs, max_len=20):

    matched_transactions_key = []
    matched_transactions = []
    good_item_ids = set()
    arr_dict_set = groupby_list(arr_xs, key=lambda x: x.set_id)

    for items in arr_dict_set.values():
        if len(items) < max_len:
            for length in range(3, len(items)):
                possible_solutions = []
                for subset in itertools.combinations(items, length):
                    if get_net_amount(subset) == 0:
                        cur_item_ids = set([x.item_id for x in subset])
                        if len(good_item_ids.intersection(cur_item_ids)) == 0:
                            possible_solutions.append(subset)

                # creating black list:
                card = collections.Counter([y.item_id for sol in possible_solutions for y in sol])
                vls = list(card.values())
                kss = list(card.keys())
                black_list = set([kss[i] for i in range(len(vls)) if vls[i] > 1])

                for pss in possible_solutions:
                    count = 0
                    for ps in pss:
                        if ps.item_id in black_list:
                            break
                        else:
                            count += 1
                    if count == len(pss):
                        matched_transactions.append(pss)
                        good_item_ids = good_item_ids.union([x.item_id for x in pss])

    if len(matched_transactions) > 0:
        for matched_part in matched_transactions:
            unq_key = generate_hash()
            matched_transactions_key += \
                [update_tt_key(tran, unq_key, "SSS") for tran in matched_part]

    unmatched_trans = [x for x in arr_xs if x.item_id not in good_item_ids]
    return matched_transactions_key, unmatched_trans


def update_tt_key(trans, match_key, audit, pm=False):
    if isinstance(trans, Transaction):
        new_tran = Transaction(
            set_id=trans.set_id,
            amount=trans.amount,
            type_id=trans.type_id,
            match_id=trans.match_id,
            item_id=trans.item_id,
            ref_key=trans.ref_key,
            sl_type=trans.sl_type,
            tt_match_id=match_key,
            audit=trans.audit + "_" + audit)
        return new_tran

    if isinstance(trans, PartialMatch):
        if pm:
            new_tran = PartialMatch(
                set_id=trans.set_id,
                amount=trans.amount,
                type_id=trans.type_id,
                match_id=trans.match_id,
                item_id=trans.item_id,
                item_ids=trans.item_ids,
                ref_key=trans.ref_key,
                sl_type=trans.sl_type,
                tt_partial_match_id=match_key,
                tt_match_id=trans.tt_match_id,
                audit=trans.audit + "_" + audit)
        else:
            new_tran = PartialMatch(
                set_id=trans.set_id,
                amount=trans.amount,
                type_id=trans.type_id,
                match_id=trans.match_id,
                item_id=trans.item_id,
                item_ids=trans.item_ids,
                ref_key=trans.ref_key,
                sl_type=trans.sl_type,
                tt_partial_match_id=trans.tt_partial_match_id,
                tt_match_id=match_key,
                audit=trans.audit + "_" + audit)
        return new_tran

    print("Unknown type, nothing to do...")
    return trans


def single_dr_cr(arr_xs):
    """
    TODO: Unit tests!!!
    :param arr_xs: [Transaction / PartialMatch]
    :param stats: Indicator calculate statistics or not
    :return: ...
    """

    arr_groups = groupby_list(arr_xs, key=lambda x: x.set_id)
    arr_matched = []

    for group in arr_groups.values():

        num_of_cr = 0
        num_of_dr = 0

        for element in group:
            if element.amount <= 0:
                num_of_dr += 1
            if element.amount > 0:
                num_of_cr += 1

        # match_key_created = False
        if (num_of_cr == 1 or num_of_dr == 1) and \
                get_net_amount(group) == 0:

            # match_key_created = True
            match_key = generate_hash()
            arr_matched += [update_tt_key(x, match_key, "SCSD") for x in group]

        # if len(group) <= 3 and not match_key_created:
        #     pprint(group)
        #     print(num_of_cr)
        #     print(num_of_dr)

    matched_ids = set([x.item_id for x in arr_matched])
    arr_unmatched = [x for x in arr_xs if x.item_id not in matched_ids]

    return arr_matched, arr_unmatched


def unique_pair(arr_xs):
    """
    TODO: Tests
    :param arr_xs:
    :param stats:
    :return:
    """

    arr_groups = groupby_list(arr_xs, key=lambda x: x.set_id)
    arr_matched = []

    for group in arr_groups.values():

        amt_dict = groupby_list(group, key=lambda x: x.amount)
        amt_dict_unk = {key: amt_dict[key] for key in amt_dict.keys() if len(amt_dict[key]) == 1}

        gr_keys = amt_dict_unk.keys()
        for key in gr_keys:
            if key < 0:
                key_neg = 0 - key
                if key_neg in gr_keys:
                    tr1 = amt_dict_unk[key][0]
                    tr2 = amt_dict_unk[key_neg][0]
                    match_key = generate_hash()
                    arr_matched += [update_tt_key(tr1, match_key, "UP"),
                                    update_tt_key(tr2, match_key, "UP")]

    matched_ids = set([x.item_id for x in arr_matched])
    arr_unmatched = [x for x in arr_xs if x.item_id not in matched_ids]

    return arr_matched, arr_unmatched


def extract_source_keys(arr_xs):

    xs_s = []
    for x in arr_xs:
        if x.sl_type == 'S':
            isninn = re.search(r"^ISNINN 0{4}[1-9]\d{9}", x.ref_key)
            if isninn:
               xs_s.append(('0', isninn.group()[11:], x.set_id))
            else:
                tmp = x.ref_key.split()[:2]
                if len(tmp) == 2:
                    xs_s.append((tmp[0], tmp[1], x.set_id))

    xs_dict = groupby_list(xs_s, key=lambda x: x[2])

    xs_dict_exact = {}
    for key in xs_dict.keys():
        data = xs_dict[key]
        unk = []
        for x in data:
            unk += x[:2]
        xs_dict_exact[key] = set([extract_key(x) for x in unk if is_valid_key(x)])

    return xs_dict_exact


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


# assert not is_valid_key('+1234')
# assert not is_valid_key('1234')
# assert not is_valid_key('.A1B234')
# assert not is_valid_key('.ABCDE4')
# assert not is_valid_key('000000000')
# assert is_valid_key('A1B23456')


def extract_key(key):

    if re.search(r"^[A-Z]{2}00[1-9]\d{7}", key):
        return key[4:12]

    if re.search(r"^EFX\d{4}", key):
        return key[:7]
    return key


# assert extract_key("MX00123456789") == '12345678'

# def match_imymu(arr_xs):
#     original_prematches = []
#     arr_xs_ = []
#
#     for tran in arr_xs:
#         if isinstance(tran, PartialMatch):
#             original_prematches.append(tran)
#         if isinstance(tran, Transaction):
#             arr_xs_.append(tran)
#
#     # source_keys = extract_source_keys(arr_xs_)
#
#     arr_xs_dict = groupby_list(arr_xs_, key=lambda x: x.set_id)
#     connected_comps_raw = []




def match_sl_by_unq_key(arr_xs, look_table, stats=True):

    original_prematches = []
    arr_xs_ = []

    for tran in arr_xs:
        if isinstance(tran, PartialMatch):
            original_prematches.append(tran)
        if isinstance(tran, Transaction):
            arr_xs_.append(tran)

    source_keys = extract_source_keys(arr_xs_)
    arr_xs_dict = groupby_list(arr_xs_, key=lambda x: x.set_id)
    connected_comps_raw = []

    for set_id in source_keys.keys():
        trans = arr_xs_dict[set_id]
        keys = source_keys[set_id]

        if len(keys) != 0:
            for key in keys:
                try:
                    match = [x for x in trans if re.search(key, x.ref_key)]
                    if len(match) > 1:
                        pre_match = PreMatch(item_ids=set([x.item_id for x in match]),
                                             unq_key=key)
                        connected_comps_raw.append(pre_match)
                except Exception as e:
                    print(e)
                    pprint(trans)
                    print("------")
                    pprint(key)

    if is_disjoint(connected_comps_raw):
        print("All component are disjoint")
        connected_comps = connected_comps_raw
    else:
        print("Conducting repartitioning")
        connected_comps = graph_merging(connected_comps_raw)
        assert is_disjoint(connected_comps)

    # this part should be reused in other functions
    item_ids_remove = set()
    for comp in connected_comps:
        # pprint(comp)
        item_ids_remove = item_ids_remove.union(comp[0])

    # pprint(item_ids_remove)
    complete_matches = []
    partial_matches = []
    for comp in connected_comps:

        transactions = [look_table[item_id] for item_id in comp[0]]
        pre_match_sum = get_net_amount(transactions)
        match_key = generate_hash()

        if pre_match_sum == 0:
            complete_matches += [update_tt_key(x, match_key, "SREF:" + str(comp[1])) for x in transactions]
        else:
            par_match = PartialMatch(set_id=transactions[0].set_id,
                                     amount=pre_match_sum,
                                     type_id='PM',
                                     match_id=set([x.match_id for x in transactions]),
                                     item_id="-".join([x.item_id for x in transactions]),
                                     item_ids=comp[0],
                                     ref_key=comp[1],
                                     sl_type=None,
                                     tt_partial_match_id=match_key,
                                     tt_match_id=None,
                                     audit="SREF:" + str(comp[1]))
            partial_matches.append(par_match)

    print("Number of complete matches: {}".format(len(complete_matches)))
    print("Number of pre-matches: {}".format(len(partial_matches)))

    assert check_duplicates(partial_matches)
    assert check_duplicates(original_prematches)

    trans_rest = [x for x in arr_xs_ if x.item_id not in item_ids_remove] + \
                 partial_matches + \
                 original_prematches

    return complete_matches, trans_rest


def match_ledger(arr_xs,
                 pattern: str,
                 extractor=None,
                 stats=True,
                 silent=True):
    """
    Creates pre-matches based on provided unique key
    :param arr_xs:
    :param pattern:
    :param extractor:
    :param stats:
    :param silent
    :return:
    """

    original_prematches = []
    arr_xs_ = []

    for tran in arr_xs:
        if isinstance(tran, PartialMatch):
            original_prematches.append(tran)
        if isinstance(tran, Transaction):
            arr_xs_.append(tran)

    # print('PM', len(original_prematches))
    # print('TR', len(arr_xs_))

    pairs = [] # array of pairs (item_id, key)
    for tran in arr_xs_:
        if not isinstance(tran, Transaction):
            continue

        match = re.search(pattern, tran.ref_key)
        if match:
            key = match.group()
            if not silent:
                print("Original key: {}".format(key))
            if extractor is not None:
                key = extractor(key)
                if not silent:
                    print("Extracted key: {}".format(key))
                pairs.append((tran.item_id, tran.set_id, key))

    groups_dict = groupby_list(pairs, key=lambda x: (x[1], x[2]))

    connected_comps_raw = []
    for value in groups_dict.values():
        if len(value) > 1:
            pre_match = PreMatch(
                item_ids=set([item[0] for item in value]),
                unq_key=set([item[2] for item in value]))
            connected_comps_raw.append(pre_match)

    if is_disjoint(connected_comps_raw):
        print("All component are disjoint")
        connected_comps = connected_comps_raw
    else:
        print("Conducting repartitioning")
        connected_comps = graph_merging(connected_comps_raw)
        assert is_disjoint(connected_comps)


    # accuracy calculation
    # TODO: move to a separate function
    look_table = {x.item_id: x for x in arr_xs_}

    # for x in arr_xs:
    #     xs = IIDLookUp(
    #         set_id=x.set_id,
    #         amount=x.amount,
    #         match_id=x.match_id)
    #     look_table[x.item_id] = xs

    # if stats:
    #     bad_matches = []
    #     for comp in connected_comps:
    #         st_matches = set([look_table[item_id].match_id for item_id in comp[0]])
    #         if len(st_matches) != 1:
    #             bad_matches.append(comp)
    #             # pprint(comp)
    #             # print("-----")
    #
    #     l_tot = sum([len(comp[0]) for comp in connected_comps])
    #     l_bad = sum([len(comp[0]) for comp in bad_matches])
    #     print("Total number pre-matched: {}".format(l_tot))
    #     if l_tot != 0:
    #         print("Correctly pre-matched: {} ({}%)".
    #               format(l_tot - l_bad, round(100*(1 - l_bad / l_tot), 2)))

    # this part should be reused in other functions
    item_ids_remove = set()
    for comp in connected_comps:
        item_ids_remove = item_ids_remove.union(comp[0])

    complete_matches = []
    partial_matches = []

    for comp in connected_comps:

        transactions = [look_table[item_id] for item_id in comp[0]]
        pre_match_sum = get_net_amount(transactions)
        match_key = generate_hash()

        if pre_match_sum == 0:
            complete_matches += [update_tt_key(x, match_key, "LREF:" + str(comp[1])) for x in transactions]
        else:
            if not silent:
                pprint(transactions)
            par_match = PartialMatch(set_id=transactions[0].set_id,
                                     amount=pre_match_sum,
                                     type_id='PM',
                                     match_id=set([x.match_id for x in transactions]),
                                     item_id="-".join([x.item_id for x in transactions]),
                                     item_ids=comp[0],
                                     ref_key=comp[1],
                                     sl_type=None,
                                     tt_partial_match_id=match_key,
                                     tt_match_id=None,
                                     audit="LREF:" + str(comp[1]))
            partial_matches.append(par_match)

    print("Number of complete matches: {}".format(len(complete_matches)))
    print("Number of pre-matches: {}".format(len(partial_matches)))

    trans_rest = [x for x in arr_xs_ if x.item_id not in item_ids_remove] + \
                 partial_matches + \
                 original_prematches

    return complete_matches, trans_rest


def match_ledger_amount(arr_xs,
                        pattern: str,
                        extractor=None,
                        stats=True,
                        silent=True):
    """
    Creates pre-matches based on provided unique key
    :param arr_xs:
    :param pattern:
    :param extractor:
    :param stats:
    :param silent
    :return:
    """

    original_prematches = []
    arr_xs_ = []

    for tran in arr_xs:
        if isinstance(tran, PartialMatch):
            original_prematches.append(tran)
        if isinstance(tran, Transaction):
            arr_xs_.append(tran)

    pairs = [] # array of pairs (item_id, key)
    for tran in arr_xs_:
        if not isinstance(tran, Transaction):
            continue

        match = re.search(pattern, tran.ref_key)
        if match:
            key = match.group()
            if not silent:
                print("Original key: {}".format(key))
            if extractor is not None:
                if extractor == 'amount':
                    key = abs(tran.amount)
                else:
                    key = extractor(key)
                if not silent:
                    print("Extracted key: {}".format(key))
                pairs.append((tran.item_id, tran.set_id, key))

    groups_dict = groupby_list(pairs, key=lambda x: (x[1], x[2]))

    connected_comps_raw = []
    for value in groups_dict.values():
        if len(value) > 1:
            pre_match = PreMatch(
                item_ids=set([item[0] for item in value]),
                unq_key=set([item[2] for item in value]))
            connected_comps_raw.append(pre_match)

    if is_disjoint(connected_comps_raw):
        print("All component are disjoint")
        connected_comps = connected_comps_raw
    else:
        print("Conducting repartitioning")
        connected_comps = graph_merging(connected_comps_raw)
        assert is_disjoint(connected_comps)

    look_table = {x.item_id: x for x in arr_xs_}

    # this part should be reused in other functions
    item_ids_remove = set()
    for comp in connected_comps:
        item_ids_remove = item_ids_remove.union(comp[0])

    complete_matches = []
    partial_matches = []

    for comp in connected_comps:

        transactions = [look_table[item_id] for item_id in comp[0]]
        pre_match_sum = get_net_amount(transactions)
        match_key = generate_hash()

        if pre_match_sum == 0:
            complete_matches += [update_tt_key(x, match_key, "LREF:AMT") for x in transactions]
        else:
            par_match = PartialMatch(set_id=transactions[0].set_id,
                                     amount=pre_match_sum,
                                     type_id='PM',
                                     match_id=set([x.match_id for x in transactions]),
                                     item_id="-".join([x.item_id for x in transactions]),
                                     item_ids=comp[0],
                                     ref_key=comp[1],
                                     sl_type=None,
                                     tt_partial_match_id=match_key,
                                     tt_match_id=None,
                                     audit="LREF:AMT")
            partial_matches.append(par_match)

    print("Number of complete matches: {}".format(len(complete_matches)))
    print("Number of pre-matches: {}".format(len(partial_matches)))

    trans_rest = [x for x in arr_xs_ if x.item_id not in item_ids_remove] + \
                 partial_matches + \
                 original_prematches

    return complete_matches, trans_rest


# def is_crc(key):
#     num_digits = sum([x.isdigit() for x in key])
#     if len(key) == num_digits and num_digits == 16:
#         return True
#     return False
#
#
# def is_ipuninp(key):
#     if re.search(r"^IPUNINP", key):
#         return True
#     return False
