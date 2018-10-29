from collections import namedtuple


PreMatch = namedtuple('PreMatch', ['item_ids', 'unq_key'])

IIDLookUp = namedtuple('IIDLookUp', ['set_id', 'amount', 'match_id'])

Transaction = namedtuple('Transaction',
                         ['set_id', 'amount', 'type_id', 'match_id', 'item_id',
                          'ref_key', 'sl_type', 'tt_match_id', 'audit'])

CC = namedtuple("CC", ['ids', 'comment'])


PartialMatch = namedtuple('PartialMatch',
                          ['set_id', 'amount', 'type_id', 'match_id',
                           'item_id', 'item_ids', 'ref_key', 'sl_type',
                           'tt_partial_match_id', 'tt_match_id', 'audit'])

Cmb2 = namedtuple('Cmb2',
                  ['amount', 'item_id', 'match_id', 'ref_key'])


ConnectedComponent = namedtuple("ConnectedComponent",
                                ['amount', 'item_ids', 'match_ids', 'unk_keys'])
