# encoding: utf_8

#%%
import pandas as pd
from ddf_utils.str import to_concept_id

# %%
source_file =  "../source/WPP2022_Demographic_Indicators_Medium.zip"

# %%
data = pd.read_csv(source_file)
# %%
data.head()
# %%
for c in data.columns:
    print(c)
# %%
use_indicators = [
    "PopSexRatio",
    "MedianAgePop",
    "NatChange",
    "NatChangeRT",
    "PopChange",
    "PopGrowthRate",
    "DoublingTime",
    "Births1519",
    "NNR",
    "MAC",
    "SRB",
    "CDR",
    "LExMale",
    "LExFemale",
    "NetMigrations",
    "CNMR"
]
# %%
df = data.set_index(['LocTypeName', 'LocID', 'Time', 'Variant'])[use_indicators].copy()
# %%
df.head()
# %%
df.index.get_level_values(3).unique()
# %%
df.columns = df.columns.map(to_concept_id)
# %%
df = df.reset_index('Variant', drop=True)
# %%
df
# %%
gs = df.groupby('LocTypeName')
# %%
gs.get_group('World')
# %%
for g, df in gs:
    df_new = df.reset_index('LocTypeName', drop=True)
    gid = to_concept_id(g)
    df_new.index.names = [gid, 'time']

    for c in df_new.columns:
        df_serve = df_new[c].dropna()
        df_serve.to_csv(f'../../demographic_indicators/ddf--datapoints--{c}--by--{gid}--time.csv')
# %%
df.columns.to_frame()

# %%