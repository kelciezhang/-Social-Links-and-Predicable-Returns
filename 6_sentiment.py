import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from utils import load_params
from tqdm import tqdm
import random

cut = 10
period = 12


params = load_params()
score_criteria=pd.read_excel(params.score_criteria_path)

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

factor = pd.DataFrame()
for d in tqdm(date_list):
    data = pd.read_excel(params.data_path + d.strftime('%Y%m%d') + '.xlsx')
    data ['mcap'] = data['prccm'] * data['cshoq']
    data = data[['gvkey','mcap', 'trt1m',  'datadate','DN_factor']]
    data = data.loc[data['DN_factor'].notna()].reset_index(drop=True)
    data = data.sort_values('DN_factor').reset_index(drop=True)
    data['port'] = pd.Series(port_cut(len(data), cut))
    factor = pd.concat([factor, data])

ew_summary = pd.DataFrame()
vw_summary = pd.DataFrame()
for i in tqdm(range(len(date_list) - period)):
    d = date_list[i]
    data = factor.loc[factor['datadate'] == d].copy()
    port1 = data.loc[(data['datadate']==d) & (data['port']==1), 'gvkey'].tolist()
    portk = data.loc[(data['datadate']==d) & (data['port']==cut), 'gvkey'].tolist()
    ew_ls = []
    vw_ls = []
    for t in range(1, period+1):
        data = factor.loc[factor['datadate'] == date_list[i + t]].copy()
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
        ew_ls.append(equal_ls)
        vw_ls.append(value_ls)
    ew_summary = pd.concat([ew_summary, pd.Series(ew_ls, index=list(range(1, period+1)), name=d)], axis=1)
    vw_summary = pd.concat([vw_summary, pd.Series(vw_ls, index=list(range(1, period+1)), name=d)], axis=1)

ew_summary.index = ew_summary.index.map(lambda x: 't+'+str(x))
vw_summary.index = vw_summary.index.map(lambda x: 't+'+str(x))
ew_cum = (ew_summary.mean(axis=1) + 1).cumprod()
vw_cum = (vw_summary.mean(axis=1) + 1).cumprod()
ew_summary.to_csv('ew_summary10.csv')
vw_summary.to_csv('vw_summary10.csv')

fig = plt.figure()
plt.plot(ew_cum, '^-',color='black', label='Equal-Weighted Cumulative Returns')
plt.plot(vw_cum, 'o--',color='black', label='Value-Weighted Cumulative Returns')
plt.legend(frameon=False)
plt.savefig('sentiment_ew_vw_10port.png')

fig = plt.figure()
plt.plot(ew_cum, '^-',color='black', label='Equal-Weighted Cumulative Returns')
plt.legend(frameon=False)
plt.savefig('sentiment_ew_10port.png')


