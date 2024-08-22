'''
The original file: 4_merging.py
-------------------------------------------------------
This program is memory friendly but more time-consuming.
'''
import pandas as pd
import numpy as np
from utils import *
from time import time
from tqdm import tqdm 
import pyreadstat
import os

params = load_params()
score_criteria=pd.read_excel(params.score_criteria_path)

if not os.path.exists(params.data_path):
    os.makedirs(params.data_path)

# calculate factors
for yr in range(params.year_end, params.year_start-1, -1): 
    mth_start = 12
    mth_end = 1
    if yr == params.year_end:
        mth_start = params.mth_end
    elif yr == params.year_start:
        mth_end = params.mth_start
    else:
        pass
    
    for mth in range(mth_start, mth_end - 1, -1):
        print('*'*10, yr,'-',mth, '*'*10) 
        director = pd.read_stata(params.director_output)
        stock_ret = pd.read_stata(params.stock_ret_output)
        director =  director.drop(['companyname_cleanco','companyname','directorname','rolename','seniority'],axis = 1)
        stock_ret = stock_ret.drop(['conml_cleanco','conm_cleanco'],axis = 1)
        
        # slicing 
        network_start = pd.Timestamp(yr - params.network_lag, mth, 1)
        network_end = pd.Timestamp(yr, mth, 1)
        director_slice = director[(director.datestartrole < network_end) & (network_start <= director.dateendrole)]
                              
        # del director  # delete to aviod memory error
        stock_ret_slice = stock_ret[(stock_ret.datadate.map(lambda x: x.year == yr)) & (stock_ret.datadate.map(lambda x: x.month == mth))] 
        
        director_slice = director_slice.applymap(lambda x: np.nan if x=="" else x)
        stock_ret_slice = stock_ret_slice.applymap(lambda x: np.nan if x=="" else x)
        
        director_count = director_slice[['companyid', 'directorid']].groupby('directorid').count().iloc[:, 0]
        director_list = director_count.index.tolist() # 192901
        director_list_valid = director_count.index[director_count > 1]  # The records of directors who
                                                                        # appear only once in data slice won't be used
                                                                        # delete them to aviod memory error
        
        del director_count 
        del director_list
    
        if params.fast:
            network_yr = pd.DataFrame(False, index=director_slice.companyid.unique(), columns=director_list_valid) # boolean matrix 
            for i in range(len(director_slice)):
                line = director_slice.iloc[i, :]
                if line.directorid in director_list_valid:
                    network_yr.loc[line.companyid, line.directorid] = True # no increment if a director worked in the company
                                                                   # multiple times     
        else:
            print('calculate network using python')
            network_yr = director_slice [['companyid', 'directorid']]
            network_yr = network_yr.drop_duplicates()
            network_yr = network_yr[network_yr.directorid.map(lambda x: x in director_list_valid)]
            network_yr = network_yr.reset_index(drop=True)
            network_yr_panal = pd.DataFrame()
            for cid in tqdm(network_yr.companyid.unique()):
                director_list_i = network_yr[network_yr.companyid == cid].directorid.unique()
                friend_data_i = network_yr[network_yr.directorid.map(lambda x: x in director_list_i)]
                friend_count_i = friend_data_i.groupby('companyid').count()
                network_i = pd.DataFrame.from_dict({'company_i':cid,'company_j': friend_count_i.index, 'count':friend_count_i.iloc[:, 0].values})
                network_i = network_i[network_i.company_j != cid]
                network_yr_panal = pd.concat([network_yr_panal, network_i])
            network_yr_panal = network_yr_panal.reset_index(drop=True)
        
        del director_list_valid
    
        ret_slice = stock_ret_slice 
        if len(ret_slice)==0:
            continue
        
        # drop if prccm < 5. drop the stocks with low liquidity 
        ret_slice = ret_slice[ret_slice.prccm >= 5]
        ret_slice = ret_slice.reset_index(drop=True) 
        
        # cusip 
        cusip_merger = director_slice[['companyid','cusip_6d']]
        cusip_merger = cusip_merger.drop_duplicates()
        cusip_merger = cusip_merger.dropna()
        ret_slice = dup_merge(ret_slice, cusip_merger, on='cusip_6d', val='companyid', new_col='companyid_cusip_6d')
        del cusip_merger
        
        # ticker
        tic_merger = director_slice[['companyid','ticker']]
        tic_merger = tic_merger.drop_duplicates()
        tic_merger = tic_merger.dropna()
        ret_slice = ret_slice.rename(columns = {'tic':'ticker'})
        ret_slice = dup_merge(ret_slice, tic_merger, on='ticker', val='companyid', new_col='companyid_ticker')
        del tic_merger
        
        # cik
        cik_merger = director_slice[['companyid','cikcode']]
        cik_merger = cik_merger.rename(columns={'cikcode': 'cik'})
        cik_merger = cik_merger.drop_duplicates()
        cik_merger = cik_merger.dropna()
        ret_slice = dup_merge(ret_slice, cik_merger, on='cik',  val='companyid', new_col='companyid_cik')
        del cik_merger 
        
        # name matching: identical
        # identical_name_merger = director_slice[['companyid','companyname']]
        # identical_name_merger = identical_name_merger.rename(columns={'companyname': 'conm'})
        # identical_name_merger = identical_name_merger.drop_duplicates()
        # identical_name_merger = identical_name_merger.dropna()
        # ret_slice = dup_merge(ret_slice, identical_name_merger, on='conm',  val='companyid', new_col='companyid_name_identical')
        # del identical_name_merger
        
        # identical_legal_name_merger = director_slice[['companyid','companyname']]
        # identical_legal_name_merger = identical_legal_name_merger.rename(columns={'companyname': 'conml'})
        # identical_legal_name_merger = identical_legal_name_merger.drop_duplicates()
        # identical_legal_name_merger = identical_legal_name_merger.dropna()
        # ret_slice = dup_merge(ret_slice, identical_legal_name_merger, on='conml',  val='companyid', new_col='companyid_legalname_identical')
        # del identical_legal_name_merger
        
        # name matching: cleanco
        # cleanco_name_merger = director_slice[['companyid','companyname_cleanco']]
        # cleanco_name_merger = cleanco_name_merger.rename(columns={'companyname_cleanco': 'conm_cleanco'})
        # cleanco_name_merger = cleanco_name_merger.drop_duplicates()
        # cleanco_name_merger = cleanco_name_merger.dropna()
        # ret_slice = dup_merge(ret_slice, cleanco_name_merger, on='conm_cleanco',  val='companyid', new_col='companyid_name_cleanco')
        # del cleanco_name_merger
        
        # cleanco_legal_name_merger = director_slice[['companyid','companyname_cleanco']]
        # cleanco_legal_name_merger = cleanco_legal_name_merger.rename(columns={'companyname_cleanco': 'conml_cleanco'})
        # cleanco_legal_name_merger = cleanco_legal_name_merger.drop_duplicates()
        # cleanco_legal_name_merger = cleanco_legal_name_merger.dropna()
        # ret_slice = dup_merge(ret_slice, cleanco_legal_name_merger, on='conml_cleanco',  val='companyid', new_col='companyid_legalname_cleanco')
        # del cleanco_legal_name_merger
        
        # name matching: fuzzywuzzy
        # ret_slice = fuzzy_merge(ret_slice, director_slice, "conm", "companyname", "companyid", "companyid_name_fuzzywuzzy")
        # ret_slice = fuzzy_merge(ret_slice, director_slice, "conml", "companyname", "companyid", "companyid_legalname_fuzzywuzzy")
        
        # judge
        ret_slice = score(ret_slice, score_criteria)
        
        # calculate the factor
        ret_slice['DN_factor'] = np.nan
        ret_slice = ret_slice.reset_index(drop=True)
        for i in tqdm(range(len(ret_slice))):
            companyid_i = ret_slice.loc[i, 'companyid']
            if pd.isnull(companyid_i):
                continue
            
            if params.fast:
                shared_director = network_yr[network_yr.columns[network_yr.loc[companyid_i]]].sum(axis=1)
                shared_director[companyid_i] = 0
                shared_director = shared_director[shared_director > 0]
            # zkx: comment
            # director_list_i = director_slice.loc[director_slice.companyid == companyid_i, :].directorid.unique() # zkx: add
            # friend_data = director_slice[director_slice.directorid.map(lambda x: x in director_list_i)][['companyid', 'directorid']] # zkx: add
            # friend_data = friend_data.drop_duplicates() # zkx: add
            # friend_data = friend_data[friend_data.companyid != companyid_i] # zkx: add
            # friend_data = friend_data.reset_index(drop=True) # zkx: add
            # shared_director = friend_data.groupby('companyid').count().iloc[:,0] # zkx: add
            
            else:
                network_yr_panal_slice = network_yr_panal[network_yr_panal.company_i==companyid_i]
                shared_director = pd.Series(network_yr_panal_slice['count'].values, index = network_yr_panal_slice.company_j.values)
            
            ret_i = pd.DataFrame()
            for company_i in shared_director.index:
                ret_i_j = ret_slice.loc[ret_slice.companyid == company_i, 'trt1m'].tolist()
                if len(ret_i_j) == 1: 
                    ret_i = pd.concat([ret_i,pd.Series(ret_i_j[0], index=[company_i])])
                elif len(ret_i_j) > 1: # notice there may be duplicated companyid
                    ret_tmp = ret_slice.loc[ret_slice.companyid == company_i, :]
                    if len(ret_tmp.cusip_6d.unique()) == 1: # A, B stocks
                        ret_tmp_i = ret_tmp.loc[ret_tmp.prccm == ret_tmp.prccm.min(), 'trt1m'].iloc[0]
                        ret_i = pd.concat([ret_i,pd.Series(ret_tmp_i, index=[company_i])])
                    else: # contradiction 
                        ret_i = pd.concat([ret_i,pd.Series(np.nan, index=[company_i])])
                else: 
                    ret_i = pd.concat([ret_i,pd.Series(np.nan, index=[company_i])])
            ret_i = ret_i.dropna()
            
            shared_director = shared_director[ret_i.index]
            if shared_director.empty:
                ret_slice.loc[i, 'DN_factor'] = np.nan
            else:
                ret_slice.loc[i, 'DN_factor'] = sum(ret_i.iloc[:, 0].values * shared_director.values) / sum(shared_director.values)
            ret_slice.loc[i, 'sum_shared_director'] = sum(shared_director)
            ret_slice.loc[i, 'numfriend'] =len(shared_director)
        print('Coverage: ', round((len(ret_slice['DN_factor'].dropna())/ len(ret_slice)), 4))
        if mth == 12:
            next_date = pd.Timestamp(yr+1, 1, 1)
        else:
            next_date = pd.Timestamp(yr, mth + 1, 1)
        date = next_date - pd.Timedelta(days=1)
        ret_slice.to_excel(params.data_path + date.strftime('%Y%m%d') + '.xlsx', index=None)
        del ret_slice
        if params.fast:
            del network_yr 
        else:
            del network_yr_panal
