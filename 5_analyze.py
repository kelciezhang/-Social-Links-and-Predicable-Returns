import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from utils import load_params
from tqdm import tqdm

params = load_params()
score_criteria=pd.read_excel(params.score_criteria_path)

date_list = pd.Series(list(os.walk(params.data_path))[0][2]).map(lambda x: x[:-5])
date_list = pd.to_datetime(date_list)
all_score = score_criteria.score.unique()
results = pd.DataFrame(np.nan, index=date_list, columns=list(score_criteria.score.unique()) + ['#', 'coverage'])

for d in tqdm(date_list):
    data = pd.read_excel(params.data_path + d.strftime('%Y%m%d') + '.xlsx')
    results.loc[d, '#'] = len(data)
    for s in all_score:
        results.loc[d, s] = len(data[data.score==s])
    results.loc[d, 'coverage'] =  sum(data.DN_factor.notna())
    del data
    
results_portion = pd.DataFrame(np.nan, index=date_list, columns=list(score_criteria.score.unique()) + ['coverage'])

for col in results_portion.columns:
    results_portion[col]=results[col] /  results['#']

# plot
pd.concat([results[all_score].cumsum(axis=1),results['#']], axis=1).plot()
plt.ylim((0,7000))
plt.title('matching coverage abs')
plt.show()

results[['coverage', '#']].plot()
plt.title('factor coverage abs')
plt.ylim((0,7000))
plt.show()
    
results_portion[all_score].cumsum(axis=1).plot()
plt.title('matching coverage')
plt.ylim((0,1))
plt.show()

results_portion['coverage'].plot()
plt.title('factor coverage')
plt.ylim((0,1))
plt.show()


    
