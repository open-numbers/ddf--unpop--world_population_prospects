# encoding: utf_8

#%%
import os
import pandas as pd
from ddf_utils.str import to_concept_id

# %%
# as of 7/13/2024, csv file for wpp 2024 demographic indicators is not available.
# FIXME: double check next time because csv files are much easier to deal with
source_file =  "../source/WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx"

# output dir
output_dir = '../../demographic_indicators'
os.makedirs(output_dir, exist_ok=True)

# %%
data = pd.concat([pd.read_excel(source_file, sheet_name="Estimates", skiprows=16),
                  pd.read_excel(source_file, sheet_name="Medium variant", skiprows=16)], ignore_index=True)
# %%
data.head()
# %%
for c in data.columns:
    print(c)

data['Year'].unique()  # there are nans in data. I checked and it's because there are some "Label/Separator" lines


data = data.dropna(how='any', subset=['Year'])

data

# util functions
import re

def strip_parentheses(text):
    return re.sub(r'\s*\([^)]*\)', '', text).strip()

strip_parentheses("Population Sex Ratio, as of 1 July (males per 100 females)")

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

use_indicators = [x.lower() for x in use_indicators]

# load metadata
metadata_file = '../source/metadata.xlsx'

metadata = pd.read_excel(metadata_file, sheet_name='indicators')

# create column name mapping
colname_mapping = metadata.set_index(['Name'])['concept'].to_dict()

colname_mapping

def map_colname(col):
    return colname_mapping.get(strip_parentheses(col), col)

data.columns.map(map_colname)

data.columns = data.columns.map(map_colname)

# %%
# df = data.set_index(['LocTypeName', 'LocID', 'Time', 'Variant'])[use_indicators].copy()
df = data.set_index(['Type', 'Location code', 'Year', 'Variant'])[use_indicators].copy()
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
# gs = df.groupby('LocTypeName')
gs = df.groupby('Type')
# %%
gs.get_group('Special other')
# %%
for g, subdf in gs:
    # df_new = df.reset_index('LocTypeName', drop=True)
    df_new = subdf.reset_index('Type', drop=True)
    # rename some type to match metadata
    if g == 'Special other':
        gid = "development_group"
    elif g == 'Region':
        gid = "geographic_region"
    else:
        gid = to_concept_id(g)
    df_new.index.names = [gid, 'time']

    for c in df_new.columns:
        df_serve = df_new[c].replace('...', None).dropna()
        df_serve = df_serve.reset_index()
        df_serve['time'] = df_serve['time'].astype('int')
        df_serve.to_csv(f'../../demographic_indicators/ddf--datapoints--{c}--by--{gid}--time.csv', index=False)

df_new
# %%
df.columns.to_frame()

# %%
