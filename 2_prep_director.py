import pandas as pd
import numpy as np
from utils import get_lag_start, dup_merge
from time import time
from utils import load_params

params = load_params()

# company profile data sample
print('...', 'company profile data sample ')
match = pd.read_stata(params.company_profile_path)
match = match[['boardname','cikcode','ticker','boardid', 'isin']]
match['cusip'] = match['isin'].map(lambda x: x[2:][:-1])
match['cusip_6d'] = match['isin'].map(lambda x: x[2:][:-1][:6])

match = match.applymap(lambda x: x if x != "" else np.nan)
match.boardid = match.boardid.map(lambda x: str(int(x)))
match.cikcode = match.cikcode.map(lambda x: '0' * (10 - len(str(int(x)))) + str(int(x)) if pd.notnull(x) else np.nan) # 958728

match = match.drop_duplicates()
match = match.reset_index(drop=True) # 926506

#--check
sample_match = match[['boardname','cikcode','boardid']]
sample_match = sample_match.drop_duplicates()
assert len(sample_match.boardid.unique()) == len(sample_match)
print(len(match.boardid) - len(match.boardid.unique()), ' duplicated boardids due to different ticker or isin.')

# director data sample
print('...', 'director data sample')
director = pd.read_stata(params.organization_composition_output_path) 
director.companyid = director.companyid.map(lambda x: str(int(x)))
director.directorid = director.directorid.map(lambda x: str(int(x))) # 1689735

# merge
print('...', 'merging director data sample and company profile data sample') 
director = pd.merge(director, match, how='left', left_on='companyid', right_on='boardid') # 1874373 
print(sum(director.boardid.isna()), 'rows in director data sample are not matched.')
director = director[director.boardid.notna()].reset_index(drop=True) # 1874262
director = director.drop(['boardid', 'boardname'], axis=1)
del match

director.to_stata(params.director_output, write_index=False)
