import os, pickle
from deprecated.utils import *

if __name__ == "__main__":

    # PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST_PK/"
    # PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC2_PK/"
    PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_TEST_REM_PK/"

    FILES = os.listdir(PATH)
    pprint(FILES)
    file_dict = {str(i): FILES[i] for i in range(len(FILES))}

    arr_all = []
    for i in range(len(FILES)):
        # if FILES[i] == 'User Set 1.pkl':
        if not (FILES[i] in IGNORE):
            if i == 1:
                with open(PATH + FILES[i], 'rb') as f:
                    arr = pickle.load(f)
                print(len(arr))
                arr_ = []
                for x in arr:
                    arr_.append(Transaction(set_id=x.set_id,     # + "|" + str(i),
                                            amount=x.amount,
                                            type_id=x.type_id,
                                            match_id=x.match_id, # + "|" + str(i),
                                            item_id=x.item_id,   # + "|" + str(i),
                                            ref_key=x.ref_key,
                                            sl_type=x.sl_type,
                                            tt_match_id=x.tt_match_id,
                                            audit=""))
                arr_all += arr_

    # removing duplicates
    # arr_all = remove_set_ids_duplicated(arr_all)
    # assert check_duplicates(arr_all)
    # arr_all = [x for x in arr_all if x.amount != 0]

    # removing duplicates
    arr_all_dict_setid = groupby_list(arr_all, key=lambda x: x.item_id)
    arr_all_xs = []
    for val in arr_all_dict_setid.values():
        arr_all_xs.append(val[0])

    arr_all = arr_all_xs

    # arr_all_dict_setid = groupby_list(arr_all, key=lambda x: x.set_id)
    # arr_all_xx = []
    # for group in arr_all_dict_setid.values():
    #     if get_net_amount(group) == 0:
    #         arr_all_xx += group
    #
    # arr_all = arr_all_xx

    arr_all_dict_itemid = {x.item_id: x for x in arr_all}
    arr_all_dict_setid = groupby_list(arr_all, key=lambda x: x.set_id)

    LN0 = len(arr_all)

    print("Total number of trans:", LN0)
    print()

    complete_matches = []

    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    arr_match, arr_all = match_sl_by_unq_key(arr_all, arr_all_dict_itemid)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    # reg_expr = r"^ISLIG001 \d{2}-[A-Z]+-\d{2} 104 IBG A \d{8} * "
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[5])
    # complete_matches += arr_match
    # arr_match, arr_all = unique_pair(arr_all)
    # complete_matches += arr_match
    # arr_match, arr_all = single_dr_cr(arr_all)
    # complete_matches += arr_match
    # assert check_duplicates(arr_all)

    # reg_expr = r"^IMYRM001 \d{2}-[A-Z]+-\d{2} MYR BASE SOB MY [A-Z]+ A \d{8}"
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[8])
    # complete_matches += arr_match
    # arr_match, arr_all = unique_pair(arr_all)
    # complete_matches += arr_match
    # arr_match, arr_all = single_dr_cr(arr_all)
    # complete_matches += arr_match
    # assert check_duplicates(arr_all)

    # reg_expr = r"ITDRM001 \d{2}-[A-Z]+-\d{2} SGD BASE 104 INWARD REMITTANCE A \d{8} [A-Za-z0-9]{6}"
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[9])
    # complete_matches += arr_match
    # arr_match, arr_all = unique_pair(arr_all)
    # complete_matches += arr_match
    # arr_match, arr_all = single_dr_cr(arr_all)
    # complete_matches += arr_match
    # assert check_duplicates(arr_all)

    # TODO: complete migration for IMXMX00 - family
    #
    # IMXMX00 - family
    #
    # ('EQUIT', 4878), +
    # ('FXD ', 1554), +
    # ('BOND ', 1270), 8 digit number +
    # ('LN_BR', 463), 8 digit number +
    # ('IRS ', 285), +
    # ('EQS ', 107), Amount +
    # ('SCF ', 89), +
    # ('CS ', 49), +
    # ('REPO ', 35), +
    # ('OPT ', 32), +
    # ('CF ', 16), 8 digit number +

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-EQUIT-[A-Z0-9_]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-FXD -[A-Z0-9_]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-BOND -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-3])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-LN_BR-[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-3])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-IRS -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-EQS -[A-Z0-9]+"
    arr_match, arr_all = match_ledger_amount(arr_all, reg_expr, 'amount')
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-SCF -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-SC -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-REPO -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-OPT -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-1])
    complete_matches += arr_match

    reg_expr = r"IMXMX\d{3} \d{2}-[A-Z]+-\d{2}.*-\d{8}-CF -[A-Z0-9]+"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split("-")[-3])
    complete_matches += arr_match

    #
    # IMYMU\d{3} - family
    #
    # ('A311_FX_SWP', 882),*
    # ('A287_FX_COMM_TIMEOPT', 278),*
    # ('A285_FX_COMM_FWD', 166),*
    # ('BOND', 157),*
    # ('A286_FX_SPOT', 79),*
    # ('A286_FX_SPOT_SARF', 18),*
    # ('A285_FX_FWD_WAAD', 15),*
    # ('A286_FX_COMM_SPOT', 15),*
    # ('A285_FX_COMM_NDFDCI', 12)*,
    # ('A256_INTERAANK_LEND', 10),
    # ('A316_IRS', 9),
    # ('A240_INTERAANK_BORR', 9),
    # ('A240_IBK_BOR_QARD', 8),
    # ('A314_CROSS_CCY_SWAP', 7),
    # ('A492_STMMD', 5),
    # ('A312_FX_INV_SWP', 5),
    # ('A311_FX_SWP_WAAD', 4),
    # ('A285_FX_NDFDCI_SARF', 2),
    # ('F321_CDS_CLI', 1),
    # ('D310_OPT_SMP', 1),
    # ('A285_FX_FWD', 1)

    # reg_expr = r"^IMYMU\d{3} .*\d{8};BOND"
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split(";")[0][-9:])
    # complete_matches += arr_match
    #
    # reg_expr = r"^IMYMU\d{3} .*;(A285_FX_[A-Z0-9_]+|A287_FX_[A-Z0-9_]+|A311_FX_[A-Z0-9_]);[A-Z0-9]+"
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split(";")[2])
    # complete_matches += arr_match
    #
    # reg_expr = r"^IMYMU\d{3} .*;A286_FX_[A-Z0-9_]+;[A-Z0-9]+"
    # arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split(";")[0][-9:])
    # complete_matches += arr_match


    reg_expr = r"^ICNCP001"
    arr_match, arr_all = match_ledger_amount(arr_all, reg_expr, 'amount')
    complete_matches += arr_match
    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    print()

    # migrated
    # validate this part
    reg_expr = r"ITDSR001 \d{2}-[A-Z]+-\d{2} SGD BASE 104 SRS A \d{8}"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[7])
    complete_matches += arr_match
    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    # migrated
    reg_expr = r"A \d{8} INW CLRG-\d{3}-"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[1])
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    #migrated
    reg_expr = r"A \d{8} \d{16}BILL PAYMENT VIA A33XS TERMINAL"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x.split()[1])
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    #migrated
    reg_expr = r"^ISLRD002 \d{2}-[A-Z]+-\d{2} \d{3} RETTIL (AANKING|AANNKXPUNIN)"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: "KEY", silent=True)
    complete_matches += arr_match
    assert check_duplicates(arr_all)
    print("Here2")

    #migrated
    reg_expr = r"^\d{6} ITDRT001 \d{2}-[A-Z]+-\d{2} SGD BASE 104 RETAIL A \d{8}"
    arr_match, arr_all = match_ledger(arr_all, reg_expr, lambda x: x[-9:])
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    reg_expr = r"PET\d{9}"
    arr_match, arr_all = match_ledger(arr_all, reg_expr)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    print()

    arr_match, arr_all = sss(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = unique_pair(arr_all)
    complete_matches += arr_match
    arr_match, arr_all = single_dr_cr(arr_all)
    complete_matches += arr_match
    assert check_duplicates(arr_all)

    cms = 0
    for x in complete_matches:
        if isinstance(x, PartialMatch):
            cms += len(x.item_ids)
        if isinstance(x, Transaction):
            cms += 1

    print("Fully matched transactions: {} ({}%)".format(cms, round(cms / LN0 * 100, 2)))

    pms = sum(len(x.item_ids) for x in arr_all if isinstance(x, PartialMatch))
    print("Partially matched transactions: {} ({}%)".format(pms, round(pms / LN0 * 100, 2)))

    ums = sum(1 for x in arr_all if isinstance(x, Transaction))
    print("Unmatched transactions: {} ({}%)".format(ums, round(ums / LN0 * 100, 2)))
    print("Coverage: {}".format(round((cms + pms) / LN0 * 100, 2)))

    all_matched = []
    for match in complete_matches:
        if isinstance(match, PartialMatch):
            for item_id in match.item_ids:
                all_matched.append((item_id, arr_all_dict_itemid[item_id].match_id, match.tt_match_id))
        if isinstance(match, Transaction):
            all_matched.append((match.item_id, match.match_id, match.tt_match_id))

    for match in arr_all:
        if isinstance(match, PartialMatch):
            for item_id in match.item_ids:
                all_matched.append((item_id, arr_all_dict_itemid[item_id].match_id, match.tt_partial_match_id))

    all_matched_dict = groupby_list(all_matched, key=lambda x: x[2])

    pred_correct = 0
    pred_incorrect = 0
    for val in all_matched_dict.values():
        if len(set([x[1] for x in val])) == 1:
            pred_correct += len(val)
        else:
            pred_incorrect += len(val)

    print("Correctly matched / pre-matched: {} ({}%)".
          format(pred_correct, round(100 * pred_correct / len(all_matched), 2)))
    print("Incorrectly matched / pre-matched: {} ({}%)".
          format(pred_incorrect, round(100 * pred_incorrect / len(all_matched), 2)))

    assert pred_correct + pred_incorrect == len(all_matched)
    assert cms + pms + ums == LN0

    # exporting data for Shalini
    PATH = "/Users/sasha/Work/Projects/matching_experiment/data/OCBC_ATANU_tmp/"
    with open(PATH + FILES[0].split(".")[0] + "_results.txt", 'a') as f:
        for trans in complete_matches:
            if isinstance(trans, Transaction):
                f.write("C|{}|{}|{}\n".format(trans.item_id, trans.tt_match_id, trans.audit))
            else:
                for x in trans.item_ids:
                    f.write("C|{}|{}|{}\n".format(x, trans.tt_match_id, trans.audit))
        for trans in arr_all:
            if isinstance(trans, Transaction):
                f.write("U|{}||\n".format(trans.item_id))
            else:
                for x in trans.item_ids:
                    f.write("P|{}|{}|{}\n".format(x, trans.tt_partial_match_id, trans.audit))





