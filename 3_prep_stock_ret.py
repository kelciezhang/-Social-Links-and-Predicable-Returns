import pandas as pd
import numpy as np
from utils import get_lag_start, dup_merge, load_params
from time import time
from tqdm import tqdm

params = load_params()

# close price data sample
print('...', 'close price data sample')
stock_ret = pd.read_stata(params.trade_output_path) 
stock_ret = stock_ret[['gvkey','cusip','tic','cik','conm','conml','conm_cleanco', 'conml_cleanco','datadate', 'trt1m', 'prccm', 'cshoq']]
stock_ret.cik = stock_ret.cik.apply(lambda x: np.nan if x == '' else x)
stock_ret['cusip_6d'] = stock_ret.cusip.map(lambda x: x[:6])
old_len = len(stock_ret)
stock_ret = stock_ret.drop_duplicates(['cusip','cik','tic', 'conm','conml','datadate'])
print(old_len - len(stock_ret), 'rows have been dropped. ')
years_tmp = stock_ret.datadate.map(lambda x: x.year)
stock_ret = stock_ret[(years_tmp >= (params.year_start + 1)) & (years_tmp <= params.year_end + 2)]
stock_ret = stock_ret.reset_index(drop=True) # 2667374
assert len(stock_ret) == len(stock_ret.drop_duplicates(['tic', 'datadate']))
assert len(stock_ret) == len(stock_ret.drop_duplicates(['cusip', 'datadate']))

# fill in cshoq 
stock_ret = stock_ret.sort_values(['gvkey', 'datadate'])
stock_ret = stock_ret.reset_index(drop=True)
for key in tqdm(stock_ret['gvkey'].unique()):
    d = stock_ret.loc[stock_ret['gvkey'] == key][['datadate', 'cshoq']]
    d = d.set_index('datadate') 
    d['year'] = d.index.year
    d['qrt'] = np.ceil(d.index.month/3)
    merger = d.groupby(['year', 'qrt']).sum()
    cshoqq = pd.merge(d[['year', 'qrt']], merger, how='left', on=['year', 'qrt']).iloc[:, -1]
    cshoqq.index = d.index
    stock_ret.loc[stock_ret['gvkey']==key, 'cshoq'] = cshoqq.values

stock_ret['cshoq'] = stock_ret['cshoq'].replace(0, np.nan)
stock_ret.to_stata(params.stock_ret_output, write_index=False)