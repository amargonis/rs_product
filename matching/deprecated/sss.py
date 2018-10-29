"""
Diansheng's implementation of SSS
"""

import numpy as np

def build_dict(arr, index):
   sum_dict = {}
   for j in range(len(arr)):
       x = arr[j];
       y = index[j]
       keys = list(sum_dict.keys())
       for i in range(len(sum_dict)):
           k = keys[i];
           v = sum_dict[k]
           value = k + x
           if value in sum_dict:
               sum_dict[value] += [a + [y] for a in v]
           else:
               sum_dict[value] = [a + [y] for a in v]
       if x in sum_dict:
           sum_dict[x] += [[y], ]
       else:
           sum_dict[x] = [[y, ], ]
   return sum_dict


def subset_sum(arr, target=0, index_arr=None):
   if index_arr is None:
       index_arr = list(range(len(arr)))
   debug('subset sum array length %i' % len(arr))
   if len(arr) == 0:
       return []
   if len(arr) > 24:
       # raise Warning('The array is too long')
       debug('The array is too long')
       return None

   # build dictionary
   sum_dict = build_dict(arr, index_arr)
   result = sum_dict[target] if target in sum_dict else []
   # if target=0, there is probably union of sets cases. keep only the minimal subsets
   if target == 0:
       alr_take_set = set()
       final_result = []
       for r in result:
           for e in r:
               if e in alr_take_set:
                   break
           else:
               for e in r:
                   alr_take_set.add(e)
               final_result.append(r)
       return final_result
   else:
       return result


def subset_sum_v2(arr, target=0, index_arr=None):
   if index_arr is None:
       index_arr = np.array(list(range(len(arr))))
   debug('subset sum array length %i' % len(arr))
   if len(arr) == 0:
       return []

   # build dictionary
   neg_size = len(arr[np.where(arr < 0)])
   pos_size = len(arr) - neg_size
   if neg_size > SSS_LIMIT or pos_size > SSS_LIMIT:
       debug('The array is too long')
       return None
   sum_dict_1 = build_dict(arr[np.where(arr < 0)], index_arr[np.where(arr < 0)])
   sum_dict_2 = build_dict(arr[np.where(arr >= 0)], index_arr[np.where(arr >= 0)])

   result = []
   for k, v in sum_dict_1.iteritems():
       if -k in sum_dict_2:
           result += [a + b
                      for a in v
                      for b in sum_dict_2[-k]
                      ]

   # if target=0, there is probably union of sets cases. keep only the minimal subsets
   if target == 0:
       alr_take_set = set()
       final_result = []
       for r in result:
           for e in r:
               if e in alr_take_set:
                   break
           else:
               for e in r:
                   alr_take_set.add(e)
               final_result.append(r)
       return final_result
   else:
       return result

def get_unique_triple_matches(raw_df, cutoff=1000):

   def _get_triples_two_sides(df1, df2):
       if len(df2) > cutoff:
           return []
       index_pairs = combinations(df2[INDEX].values.tolist(), 2)
       amount_dict = {}
       # build dict
       for pair in index_pairs:
           k = int(df2.loc[df2[INDEX].isin(pair), 'Amount Signed'].sum())
           amount_dict[k] = pair
       # match dict
       match_list = []
       for index in df1.index:
           k = int(-df1['Amount Signed'].at[index])
           if k in amount_dict:
               match_list.append(list(amount_dict[k]) + [df1[INDEX].at[index], ])
       return get_clean_triples(match_list)

   unique_triple = []
   for set_id in tqdm(raw_df['Set ID'].unique().tolist()):
       df = raw_df[raw_df['Set ID'] == set_id]
       if len(df) == 3 and df['Amount Signed'].sum() == 0:
           unique_triple.append(df[INDEX].tolist())
           continue

       credit_df = df[df['Item Side'].str.contains('C')]
       debit_df = df[df['Item Side'].str.contains('D')]

       matches_1 = _get_triples_two_sides(credit_df, debit_df)
       matches_2 = _get_triples_two_sides(debit_df, credit_df)

       unique_triple += get_clean_triples(matches_1 + matches_2)

   return unique_triple