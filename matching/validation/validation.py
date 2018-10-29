from matching_tools.utils import *
from matching_tools.data_model import *

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

    dict_item_id = group_by_list(arr_all_xs, key=lambda x: x.item_id)

    arr_all = arr_all_xs
    arr_all = list(group_by_list(arr_all, key=lambda x: x.set_id).values())
    LN0 = len(arr_all_xs)

    matched_count_c = 0
    partial_count_c = 0
    matched_count_n = 0
    partial_count_n = 0

    len_remained = 0

    for trans_set_id in arr_all:
        component = []

        # testing part
        # reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-EQUIT-[A-Z0-9_]+"
        # component += link_ledger(trans_set_id, reg_expr,
        #                          lambda x: x.split()[1] + "|" + x.split("-")[-1])

        # reg_expr = r"ITDSR001 \d{2}-[A-Z]+-\d{2} SGD BASE 104 SRS A \d{8}"
        # component += link_ledger(trans_set_id, reg_expr, lambda x: x.split()[7])

        # reg_expr = r"PET\d{9}"
        # component += link_ledger(trans_set_id, reg_expr, lambda x: x)

        # reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-BOND -[A-Z0-9]+"
        # component += link_ledger(trans_set_id, reg_expr, lambda x: x.split("-")[-3])

        # reg_expr = r"^ISLIG001 \d{2}-[A-Z]+-\d{2} 104 IBG A \d{8} * "
        # component += link_ledger(trans_set_id, reg_expr, lambda x: x.split()[5])

        reg_expr = "^ITDCF001 \d{2}-[A-Z]+-\d{2} SGD BASE 104 CPF INVESTMENT A \d{8}"
        component += link_ledger(trans_set_id, reg_expr, lambda x: x[-8:])

        # reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-BOND -[A-Z0-9]+"
        # component += link_ledger(trans_set_id, reg_expr, lambda x: x.split("-")[-3])

        if not is_disjoint(component):
            component = graph_merging(component)
            assert is_disjoint(component)

        # creating complete and partial matches:
        complete_matches = []
        partial_matches = []
        taken_ids = set()

        for cc in component:
            taken_ids = taken_ids.union(cc.ids)
            audit = cc.comment
            trans = [dict_item_id[ids][0] for ids in cc.ids]
            net_amount = get_net_amount(trans)
            match_ids = get_unique_match_ids(trans)

            if net_amount == 0:
                complete_matches += trans
                if len(match_ids) == 1:
                    display_match(trans, audit=audit, comment="Correct match:", col='green')
                    matched_count_c += len(trans)
                else:
                    display_match(trans, audit=audit, comment="Incorrect match:", col='red')
                    matched_count_n += len(trans)
            else:
                tr_tmp = Transaction(set_id=trans[0].set_id,     # + "|" + str(i),
                                     amount=get_net_amount(trans),
                                     v_date=trans[0].v_date,
                                     type_id="",
                                     match_id="", # + "|" + str(i),
                                     item_id="",   # + "|" + str(i),
                                     ref_key="",
                                     sl_type="",
                                     tt_match_id="",
                                     audit=len(trans))

                partial_matches.append(tr_tmp)
                if len(match_ids) == 1:
                    display_match(trans, audit=audit, comment="Correct partial match:", col='green')
                    partial_count_c += len(trans)
                else:
                    display_match(trans, audit=audit, comment="Incorrect partial match:", col='red')
                    partial_count_n += len(trans)

        unmatched = [x for x in trans_set_id if x.item_id not in taken_ids] + partial_matches
        arr_matched, arr_unmatched = unique_pair(unmatched)

        len_unamtched = len(arr_unmatched)
        len_remained += len_unamtched

    len_matched_count = float(matched_count_c + matched_count_n)
    len_partial_count = float(partial_count_c + partial_count_n)

    if len_matched_count != 0:
        pprint("Matched: correctly {} ({}%), incorrectly {} ({}%)".
               format(matched_count_c, round(100 * matched_count_c/len_matched_count, 2),
                      matched_count_n, round(100 * matched_count_n/len_matched_count, 2)))

    if len_partial_count != 0:
        pprint("Partial: correctly {} ({}%), incorrectly {} ({}%)".
               format(partial_count_c, round(100 * partial_count_c / len_partial_count, 2),
                      partial_count_n, round(100 * partial_count_n / len_partial_count, 2)))

    covr = matched_count_c + matched_count_n + partial_count_c + partial_count_n
    pprint("Coverage: {}%".format(round(covr / LN0 * 100, 2)))
    pprint("Prop Matched: {}%".format(round(100 - len_remained / LN0 * 100, 2)))
