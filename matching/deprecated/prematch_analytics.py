"""
A collection of function for merging and validating pre-matches obtained by S key extraction
"""
from deprecated.data_model import *
from collections import deque


def is_disjoint(connected_comps: [PreMatch]) -> bool:
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
            x1 = connected_comps[i][0]
            x2 = connected_comps[j][0]
            if len(x1.intersection(x2)) != 0:
                num_joint += 1
    return num_joint == 0


def graph_merging(connected_comps: [PreMatch]) -> [PreMatch]:

    if len(connected_comps) == 0:
        return []

    # building a connectivity graph
    all_ids = set()
    for ccr in connected_comps:
        all_ids = all_ids.union(ccr.item_ids)

    graph = {}
    for item_id in all_ids:
        connected_to = set()
        unq_keys = set()
        for ccr in connected_comps:
            if item_id in ccr.item_ids:
                connected_to = connected_to.union(ccr.item_ids)
                unq_keys.add(ccr.unq_key)
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
        connected_comps_proc.append((visited_current, unq_keys))

    return connected_comps_proc

