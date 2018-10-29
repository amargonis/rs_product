# def single_dr_cr_old(arr_xs, stats=True):
#     """
#     TODO: Unit tests!!!
#     :param arr_xs: [Transaction / PartialMatch / Transaction_uk]
#     :param stats: Indicator calculate statistics or not
#     :return: ...
#     """
#
#     arr_groups = groupby_list(arr_xs, key=lambda x: x.set_id)
#
#     # arr_unmatched = []
#     arr_matched = []
#
#     # matched_c = []
#     # matched_n = []
#
#     for group in arr_groups.values():
#         num_of_cr = 0
#         num_of_dr = 0
#         #only_trans = True
#
#         for element in group:
#             # if isinstance(element, Transaction):
#             # if element.type_id == 'DR':
#             #     num_of_dr += 1
#             # if element.type_id == 'CR':
#             #     num_of_cr += 1
#             if element.amount <= 0:
#                 num_of_dr += 1
#             if element.amount > 0:
#                 num_of_cr += 1
#             #else:
#             #    only_trans = False
#             #    # print("Currently, only Transactions are supported")
#             #    break
#
#         #if not only_trans:
#         #    continue
#
#         if (num_of_cr == 1 or num_of_dr == 1) and \
#                 get_net_amount(group) == 0:
#
#             # if stats:
#             #     if len(get_unique_match_ids(group)) == 1:
#             #         matched_c += group
#             #     else:
#             #         matched_n += group
#             # for x in group:
#             #     if isinstance(x, PartialMatch):
#             #         pprint(group)
#             #         break
#             match_key = generate_hash()
#             arr_matched += [update_tt_key(x, match_key) for x in group]
#
#     # if stats:
#     #     display_stats(len(arr_matched), len(matched_c), len(matched_n))
#
#     matched_ids = set([x.item_id for x in arr_matched])
#     arr_unmatched = [x for x in arr_xs if x.item_id not in matched_ids]
#
#     return arr_matched, arr_unmatched


def unique_pair(arr_xs, stats=True):
    """
    TODO: Tests
    :param arr_xs:
    :param stats:
    :return:
    """

    arr_groups = groupby_list(arr_xs, key=lambda x: x.set_id)

    # arr_unmatched = [] defined later
    arr_matched = []

    # matched_c = []
    # matched_n = []

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
                    arr_matched += [update_tt_key(tr1, match_key),
                                    update_tt_key(tr2, match_key)]
                    # if stats:
                    #     tr1_match_id = tr1.match_id if isinstance(tr1.match_id, set) else {tr1.match_id}
                    #     tr2_match_id = tr2.match_id if isinstance(tr2.match_id, set) else {tr2.match_id}
                    #     if tr1_match_id == tr2_match_id:
                    #         matched_c += [tr1, tr2]
                    #     else:
                    #         # pprint(tr1)
                    #         # pprint(tr2)
                    #         # print('---------')
                    #         if isinstance(tr1.match_id, set) or \
                    #            isinstance(tr2.match_id, set):
                    #             matched_n += [tr1, tr2]

    # if stats:
    #     display_stats(len(arr_matched), len(matched_c), len(matched_n))

    matched_ids = set([x.item_id for x in arr_matched])
    arr_unmatched = [x for x in arr_xs if x.item_id not in matched_ids]

    return arr_matched, arr_unmatched



def get2s(xs):
    """
    Function returns cross product of a vector with the unique amount
    :param xs:
    :return:
    """
    output = []
    for i in range(len(xs)):
        for j in range(i+1, len(xs)):
            x = Cmb2(amount=abs(xs[i].amount + xs[j].amount),
                     item_id=[xs[i].item_id, xs[j].item_id],
                     # match_id=[xs[i].match_id, xs[j].match_id],
                     match_id=None,
                     ref_key=[xs[i].ref_key, xs[j].ref_key])
            output.append(x)

    output_dict = groupby_list(output, key=lambda x: x.amount)
    return {key: output_dict[key] for key in output_dict.keys() if len(output_dict[key]) == 1}


def get1s(xs):
    """
    Function returns cross product of a vector with the unique amount
    :param xs:
    :return:
    """
    output = [Cmb2(amount=abs(x.amount),
                   item_id=[x.item_id],
                   # match_id=[x.match_id],
                   match_id=None,
                   ref_key=[x.ref_key]) for x in xs]

    output_dict = groupby_list(output, key=lambda x: x.amount)
    return {key: output_dict[key] for key in output_dict.keys() if len(output_dict[key]) == 1}


def eliminate_threes(arr_xs, cut_off=200, stats=True):
    """
    TODO: Finish this part
    :param arr_xs:
    :param cut_off:
    :param stats:
    :return:
    """

    matched_trans = []
    # original_prematches = [x for x in arr_xs if isinstance(x, PartialMatch)]
    # arr_xs_ = [x for x in arr_xs if isinstance(x, Transaction)]

    look_up = {x.item_id: x for x in arr_xs}
    arr_dict = groupby_list(arr_xs, key=lambda x: x.set_id)

    for key in arr_dict.keys():
        trans = arr_dict[key]
        #cr = [x for x in trans if x.type_id == 'CR']
        #dr = [x for x in trans if x.type_id == 'DR']

        cr = [x for x in trans if x.amount >= 0]
        dr = [x for x in trans if x.amount < 0]


        if len(cr) >= cut_off and len(dr) >= cut_off:
            continue

        cr_dict1 = get1s(cr)
        dr_dict1 = get2s(dr)

        cr_dict2 = get2s(cr)
        dr_dict2 = get1s(dr)

        intrs1 = set(dr_dict1.keys()).intersection(set(cr_dict1.keys()))
        intrs2 = set(dr_dict2.keys()).intersection(set(cr_dict2.keys()))

        item_ids1 = []
        if len(intrs1) != 0:
            for intr in intrs1:
                crs = cr_dict1[intr][0]
                drs = dr_dict1[intr][0]
                item_ids1 += crs.item_id
                item_ids1 += drs.item_id

        item_ids2 = []
        if len(intrs2) != 0:
            for intr in intrs2:
                crs = cr_dict2[intr][0]
                drs = dr_dict2[intr][0]
                item_ids2 += crs.item_id
                item_ids2 += drs.item_id

        item_ids = item_ids1 + item_ids2
        # print(key)
        if len(item_ids) != 0:
            counter = collections.Counter(item_ids)
            vls = list(counter.values())
            kls = list(counter.keys())
            blk_list = set([kls[i] for i in range(len(vls)) if vls[i] > 1])

            if len(intrs1) != 0:
                for intr in intrs1:
                    crs = cr_dict1[intr][0]
                    drs = dr_dict1[intr][0]
                    items = set(crs.item_id + drs.item_id)

                    if len(items.intersection(blk_list)) == 0:
                        match_key = generate_hash()
                        matched_trans += [update_tt_key(look_up[itm], match_key) for itm in items]

            if len(intrs2) != 0:
                for intr in intrs2:
                    crs = cr_dict2[intr][0]
                    drs = dr_dict2[intr][0]
                    items = set(crs.item_id + drs.item_id)

                    if len(items.intersection(blk_list)) == 0:
                        match_key = generate_hash()
                        matched_trans += [update_tt_key(look_up[itm], match_key) for itm in items]

    matched_trans_ids = set([x.item_id for x in matched_trans])
    arr_unmatched = [x for x in arr_xs if x.item_id not in matched_trans_ids] # + original_prematches

    return matched_trans, arr_unmatched



def match_sl_by_unq_key(arr_xs, stats=True):

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
                match = [x for x in trans if re.search(key, x.ref_key)]
                if len(match) > 1:
                    pre_match = PreMatch(item_ids=set([x.item_id for x in match]),
                                         unq_key=key)
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
    #
    #     l_tot = sum([len(comp[0]) for comp in connected_comps])
    #     l_bad = sum([len(comp[0]) for comp in bad_matches])
    #     print("Total number pre-matched: {}".format(l_tot))
    #     print("Correctly pre-matched: {} ({}%)".format(l_tot - l_bad, round(100*(1 - l_bad / l_tot), 2)))

    # this part should be reused in other functions
    item_ids_remove = set()
    for comp in connected_comps:
        item_ids_remove = item_ids_remove.union(comp[0])

    complete_matches = []
    partial_matches = []
    for comp in connected_comps:

        # unique_key = comp[1]
        transactions = [look_table[item_id] for item_id in comp[0]]
        pre_match_sum = get_net_amount(transactions)
        match_key = generate_hash()

        if pre_match_sum == 0:
            complete_matches += [update_tt_key(x, match_key) for x in transactions]
        else:
            par_match = PartialMatch(set_id=transactions[0].set_id,
                                     amount=pre_match_sum,
                                     type_id='PM',
                                     match_id=set([x.match_id for x in transactions]),
                                     #item_id="-".join([x.item_id for x in transactions]),
                                     item_id="-".join(get_unique_match_ids(transactions)),
                                     item_ids=comp[0],
                                     ref_key=comp[1],
                                     sl_type=None,
                                     tt_partial_match_id=match_key,
                                     tt_match_id=None)
            partial_matches.append(par_match)

    print("Number of complete matches: {}".format(len(complete_matches)))
    print("Number of pre-matches: {}".format(len(partial_matches)))

    trans_rest = [x for x in arr_xs_ if x.item_id not in item_ids_remove] + \
                 partial_matches + \
                 original_prematches

    return complete_matches, trans_rest