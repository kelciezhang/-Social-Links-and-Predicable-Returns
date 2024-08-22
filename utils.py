import pandas as pd
import numpy as np
# from rapidfuzz import fuzz
# from rapidfuzz import process
import configparser
from tqdm import tqdm

class ARGS():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
        
    
def load_params():
    conf = configparser.ConfigParser()
    conf.read('params.ini')
    args = ARGS(
        organization_composition_path=conf.get("cleanco","organization_composition_path"),
        trade_path=conf.get('cleanco', 'trade_path'),
        organization_composition_output_path=conf.get('cleanco', 'organization_composition_output_path'),
        trade_output_path=conf.get('cleanco', 'trade_output_path'),
        company_profile_path=conf.get('prep_director', 'company_profile_path'),
        director_output=conf.get('prep_director', 'director_output'),
        stock_ret_output=conf.get('prep_stock_ret','stock_ret_output'),
        year_start=conf.getint('prep_stock_ret', 'year_start'),
        year_end=conf.getint('prep_stock_ret', 'year_end'),
        network_lag=conf.getint('merging', 'network_lag'),
        score_criteria_path=conf.get('merging', 'score_criteria_path'),
        data_path=conf.get('merging', 'data_path'), 
        fast=conf.getint('merging', 'fast'),
        mth_start = conf.getint('prep_stock_ret', 'mth_start'), 
        mth_end = conf.getint('prep_stock_ret', 'mth_end'), 
        mkt_median_path=conf.get('cleanco', 'mkt_median_path')
    )
    return args

def get_lag_start(d, lag):
    lag = lag -1
    ret_year = d.year
    ret_month = d.month
    if lag >=  d.month:
        lag = lag - ret_month
        ret_month=12
        ret_year-=1
        return pd.Timestamp(ret_year-lag//12, ret_month-lag%12, 1) - pd.Timedelta(days=1)
    return pd.Timestamp(ret_year, ret_month-lag, 1) - pd.Timedelta(days=1)

# def dup_merge(data, merger, on, val, new_col):
#     old_len =len(data)
#     ret = pd.merge(data, merger, on=on, how='left')
#     dup_list = ret[ret.duplicated(data.columns)][on].tolist()
#     for dup in dup_list:
#         slc = ret[ret[on] == dup] 
#         val_list = slc[val].tolist()
#         val_str = '|'.join(val_list)
#         line = slc.iloc[0,:].copy()
#         line[val] = val_str
#         ret = ret[ret[on] != dup]
#         ret = ret.append(line)
#         ret = ret.reset_index(drop=True)
#     ret.columns = ret.columns[:-1].tolist() + [new_col]
#     assert old_len == len(ret)
#     return ret

def dup_merge(data, merger, on, val, new_col):
    old_len =len(data)
    dup_list = merger[merger.duplicated(on)][on].unique()
    for dup in dup_list:
        slc = merger[merger[on] == dup]
        val_list = slc[val].tolist()
        val_str = '|'.join(val_list)
        line = slc.iloc[0,:].copy()
        line[val] = val_str
        merger = merger[merger[on] != dup]
        merger = merger.append(line)
        merger = merger.reset_index(drop=True)
    ret = pd.merge(data, merger, on=on, how='left')
    ret.columns = ret.columns[:-1].tolist() + [new_col]
    assert old_len == len(ret)
    return ret

#def fuzzy_merge(data1, data2, key1, key2, target, new_col, threshold=90):
   # def get_cloest_match(name):
       # candidates = list(data2[key2].unique())
       # choice = process.extract(name, candidates, limit=1)[0]
       # if choice[1] >= threshold:
       #     target_list = list(data2[data2[key2] == choice[0]][target].unique())
        #    return '!'.join(target_list)
       # return np.nan
    #new = pd.Series(np.nan, index=range(len(data1)))
   # for i in tqdm(range(len(data1))):
   #     company_name = data1.iloc[i, :][key1]
   #     new.iloc[i] = get_cloest_match(company_name)
  #  data1[new_col] = new
  #  return data1

def score(data, score_criteria):
    score_crit = score_criteria.copy()
    score_crit.columns = [score_crit.columns[0]] + list('companyid_' + score_crit.columns[1:])
    assert set(score_crit.columns[1:]).issubset(set(data.columns))
    def extract(row, criteria_i):
        flag=True
        for c in criteria_i.index[1:]:
            if pd.isnull(row[c]):
                flag=False
        if not flag:
            return np.nan, np.nan
        
        candidate = set()
        for c in criteria_i.index[1:]:
            if pd.notnull(row[c]):
                candidate = candidate.union(set(row[c].split('|')))
        for c in criteria_i.index[1:]:
            if pd.notnull(row[c]):
                candidate = candidate.intersection(set(row[c].split('|')))
        if len(list(candidate))==1:
            return list(candidate)[0], criteria_i.score
        elif len(list(candidate))==0:
            return np.nan,np.nan
        else:
            return list(candidate)[0], criteria_i.score
        
    score_crit=score_crit.sort_values('score', ascending=True).reset_index(drop=True)
    data['companyid'] = np.nan
    data['score'] = np.nan
    for i in range(len(score_crit)):
        criteria = score_crit.iloc[i, :].dropna()
        data['companyid_tmp'], data['score_tmp'] = zip(*data.apply(lambda x: extract(x, criteria), axis=1))
        to_change = (data['companyid'].map(lambda x: pd.isnull(x))) & (data['companyid_tmp'].map(lambda x: pd.notnull(x)))
        data.loc[to_change,'companyid'] = data.loc[to_change, 'companyid_tmp']
        data.loc[to_change,'score'] = data.loc[to_change, 'score_tmp']
    data = data.drop(['companyid_tmp','score_tmp'], axis=1)
    return data
    
    
    
        
        
    

        
                    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        