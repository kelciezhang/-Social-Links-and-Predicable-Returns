import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from utils import load_params
from tqdm import tqdm
import random
from scipy import stats

cut = 10
L_list = [1, 3, 6, 12]
H_list = [1, 6, 12, 24, 36]
gap = 0

params = load_params()

date_list = pd.Series(list(os.walk(params.data_path))[0][2]).map(lambda x: x[:-5])
date_list = pd.to_datetime(date_list)

factor = pd.DataFrame()

def port_cut(N, nport=5):
    additional = random.sample(list(range(nport)), N % nport)
    nums = [N // nport] * nport
    for i in additional:
        nums[i] = nums[i] + 1
    port_ind = []
    for i in range(1, nport + 1):
        port_ind = port_ind + nums[i-1] * [i]
    return port_ind

index = []
for L in L_list:
    index.append(L)
    index.append(str(L) + '_t')
    
ew_summary = pd.DataFrame(np.nan, index=index, columns=H_list)
vw_summary = pd.DataFrame(np.nan, index=index, columns=H_list)

factor = pd.DataFrame()
for d in tqdm(date_list):
    data = pd.read_excel(params.data_path + d.strftime('%Y%m%d') + '.xlsx')
    data ['mcap'] = data['prccm'] * data['cshoq']
    data = data[['gvkey','mcap', 'trt1m',  'datadate','DN_factor']]
    data = data.loc[data['DN_factor'].notna()].reset_index(drop=True)
    data = data.sort_values('DN_factor').reset_index(drop=True)
    factor = pd.concat([factor, data])
factor.to_csv('factor.csv', index=None)
factor=pd.read_csv('factor.csv')

factor['datadate'] = pd.to_datetime(factor['datadate'])
factor = factor.drop_duplicates(['gvkey', 'datadate']).reset_index(drop=True)

# portfolio form on t-1, hold from t to t + H -1
for L in L_list:
    for H in H_list:
        print('* L: ', L, ' H: ', H)
        ew_holding_matrix = pd.DataFrame(np.nan, index=date_list[L:], columns=date_list[(L + gap):])
        vw_holding_matrix = pd.DataFrame(np.nan, index=date_list[L:], columns=date_list[(L + gap):])
        for i in tqdm(range(L,len(date_list))):
            t = date_list[i]
            data = factor[(factor.datadate >= date_list[i-L]) & (factor.datadate <= date_list[i-1])]
            data_mat = pd.DataFrame()
            for d in data.datadate.unique():
                tmp = data.loc[data.datadate == d, ['gvkey', 'DN_factor']]
                tmp.columns = ['gvkey', d]
                if data_mat.empty:
                    data_mat = tmp
                    continue
                data_mat = pd.merge(data_mat, tmp, how='outer', on='gvkey')
            data_mat = data_mat.set_index('gvkey')
            
            # define lagged return 
            l_ret = pd.DataFrame((data_mat/100 + 1).prod(axis=1) - 1) 
            # l_ret = pd.DataFrame(data_mat.mean(axis=1)) 
            
            l_ret = l_ret.sort_values(0)
            l_ret['port'] = port_cut(len(l_ret), cut)
            port1 = l_ret.loc[l_ret['port'] == 1].index.tolist()
            portk = l_ret.loc[l_ret['port'] == cut].index.tolist()
            for j in  range(i + gap,i + gap + H):
                if j >  len(date_list) - 1:
                    continue
                h = date_list[j]
                data = factor.loc[factor['datadate'] == h].copy()
                data['ret_mcap'] = data['mcap'] * data['trt1m']
                port1_data = data[data['gvkey'].map(lambda x: x in port1)]
                portk_data = data[data['gvkey'].map(lambda x: x in portk)]
                # equal-weighted
                equal1 = port1_data['trt1m'].mean()
                equalk = portk_data['trt1m'].mean()
                equal_ls = equalk - equal1
                # value-weighted
                value1 = port1_data['ret_mcap'].sum() / port1_data['mcap'].sum()
                valuek = portk_data['ret_mcap'].sum() / portk_data['mcap'].sum()
                value_ls = valuek - value1
                ew_holding_matrix.loc[t, h] = equal_ls
                vw_holding_matrix.loc[t, h] = value_ls
        ew_series = ew_holding_matrix.mean()[:]
        vw_series = vw_holding_matrix.mean()[:]
        ew_summary.loc[L, H] = ew_series.mean()
        vw_summary.loc[L, H] = vw_series.mean()
        ew_summary.loc[str(L) + '_t', H] = stats.ttest_1samp(ew_series, 0)[0]
        vw_summary.loc[str(L) + '_t', H] = stats.ttest_1samp(vw_series, 0)[0]
            
            
ew_summary.to_csv('0405_ew_summary2.csv')                
vw_summary.to_csv('0405_vw_summary2.csv')
                
            
                
    














