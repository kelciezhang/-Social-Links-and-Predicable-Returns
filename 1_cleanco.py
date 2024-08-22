import pandas as pd
import numpy as np
import cleanco
from utils import load_params

params = load_params()

director = pd.read_stata(params.organization_composition_path)
director['companyname_cleanco'] = director['companyname'].map(lambda x: cleanco.basename(x))
director.to_stata(params.organization_composition_output_path, write_index=False)

stock_ret = pd.read_stata(params.trade_path)
stock_ret['conm_cleanco'] = stock_ret['conm'].map(lambda x: cleanco.basename(x))
stock_ret['conml_cleanco'] = stock_ret['conml'].map(lambda x: cleanco.basename(x))
stock_ret.to_stata(params.trade_output_path, write_index=False)