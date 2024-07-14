# %%
import pandas as pd
from ddf_utils.str import to_concept_id, format_float_digits
from functools import partial
from glob import glob
import dask.dataframe as dd
import os

# we will calculate from other indicators.
# alternativly, there are already calculated indicators in WPP
# but it's in excel file and we need to deal with possible change of format in different WPP versions
# so it's better to just use the data from CSV downloads.
# TODO: each time we have new data, double check the output with excel file from WPP

# %%
source = glob('../../population_age1/ddf--datapoints--population--*country*[!x].csv')
# %%
source
# %%
data = dd.read_csv(source, dtype={'age_group_1year': str})
# %%
data.head()
# %%
data = data.compute()
# %%
data.head()
# %%
# 1. total dependency ratio
# %%
df = data.set_index(['country_area', 'time', 'age_group_1year'])['population']
# %%
df = df.sort_index()
# %%
list1564 = [str(x) for x in range(15, 65)]
# %%
list1564
# %%
df1 = df.loc[:, :, list1564]
# %%
listother = list(filter(lambda x: x not in list1564, data['age_group_1year'].unique()))
# %%
df2 = df.loc[:, :, listother]
# %%
df2
# %%
df1 = df1.groupby(['country_area', 'time']).sum()
# %%
df1
# %%
df2 = df2.groupby(['country_area', 'time']).sum()
# %%
df2
# %%
df_dep = df2 / df1 * 100
# %%
df_dep
# %%
df_dep.loc[480, 1970]
# %%
format_func = partial(format_float_digits, digits=3)
# %%
df_dep = df_dep.map(format_func)
# %%
df_dep.name = 'total_dependency_ratio1564'
# %%
df_dep.to_csv('../../ratios/ddf--datapoints--total_dependency_ratio1564--by--country_area--time.csv')
# %%
# sex ratio
source = glob('../../population_age1/ddf--datapoints--population--*country*sex.csv')
# %%
source
# %%
data = dd.read_csv(source, dtype={'age_group_1year': str})
# %%
data = data.compute()
# %%
data
# %%
dfmale = data[data.sex == 1].set_index(['country_area', 'time', 'age_group_1year'])['population']
# %%
dffemale = data[data.sex == 2].set_index(['country_area', 'time', 'age_group_1year'])['population']
# %%
dfmale
# %%
g1 = [str(x) for x in range(15)]
g1
# %%
dfm1 = dfmale.loc[:, :, g1].groupby(['country_area', 'time']).sum()
dff1 = dffemale.loc[:, :, g1].groupby(['country_area', 'time']).sum()
# %%
df1 = dfm1 / dff1 * 100
# %%
df1.loc[764, 2089]
# %%
df1.name = "popsexratio"
# %%
g2 = [str(x) for x in range(15, 25)]
dfm2 = dfmale.loc[:, :, g2].groupby(['country_area', 'time']).sum()
dff2 = dffemale.loc[:, :, g2].groupby(['country_area', 'time']).sum()
# %%
df2 = dfm2 / dff2 * 100
df2.name = 'popsexratio'
# %%
df2
# %%
df2.loc[704, 2024]
# %%
g3 = [str(x) for x in range(15, 50)]
dfm3 = dfmale.loc[:, :, g3].groupby(['country_area', 'time']).sum()
dff3 = dffemale.loc[:, :, g3].groupby(['country_area', 'time']).sum()
# %%
df3 = dfm3 / dff3 * 100
df3.name = 'popsexratio'
# %%
data['age_group_1year'].unique()
# %%
g4 = [str(x) for x in range(50, 100)]
g4.append('100plus')
dfm4 = dfmale.loc[:, :, g4].groupby(['country_area', 'time']).sum()
dff4 = dffemale.loc[:, :, g4].groupby(['country_area', 'time']).sum()
# %%
df4 = dfm4 / dff4 * 100
df4.name = 'popsexratio'
# %%
df4.loc[704, 2023]
# %%
# TODO: separated indicators or one indicator with more dimension?
df1 = df1.to_frame().assign(age_group_broad='0_14').set_index('age_group_broad', append=True)
df2 = df2.to_frame().assign(age_group_broad='15_24').set_index('age_group_broad', append=True)
df3 = df3.to_frame().assign(age_group_broad='15_49').set_index('age_group_broad', append=True)

# %%
df4 = df4.to_frame().assign(age_group_broad='50plus').set_index('age_group_broad', append=True)
# %%
df_sr = pd.concat([df1, df2, df3, df4])
# %%
df_sr
# %%
df_sr['popsexratio'] = df_sr['popsexratio'].map(format_func)
# %%
df_sr.to_csv('../../ratios/ddf--datapoints--popsexratio--by--country_area--time--age_group_broad.csv')
# %%
