from collections import namedtuple


IGNORE = {".DS_Store"}


Transaction = namedtuple('Transaction',
                         ['set_id', 'amount', 'v_date', 'type_id', 'match_id', 'item_id',
                          'ref_key', 'sl_type', 'tt_match_id', 'audit'])

CC = namedtuple("CC", ['ids', 'comment'])