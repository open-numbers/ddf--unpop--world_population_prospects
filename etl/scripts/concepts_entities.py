# %%
import pandas as pd
from ddf_utils.str import to_concept_id
# %%
source_file =  "../source/WPP2022_F01_LOCATIONS.XLSX"
# %%
data = pd.read_excel(source_file, sheet_name='DB')
# %%
data.columns
# %%
gs = data.groupby('LocTypeName')
# %%
esets = dict()

esets['world'] = [data.iloc[0]]
# %%
esets['world']
# %%
locType = None
for i, row in data.iloc[1:].iterrows():
    if row['LocTypeName'].strip() == "Label/Separator":
        print(f'handling: {row["Location"]}')
        locType = None
        continue
    if locType is None:
        locType = to_concept_id(row['LocTypeName'])
    
    if locType not in esets:
        esets[locType] = list()
    esets[locType].append(row)

# %%
# continue here! Next: check if we should do extra things to the subregions/geo regions columns.
esets.keys()
# %%
esets['sdg_region'][0]
# %%
sdg_regions = pd.concat(esets['sdg_region'], axis=1)
# %%
df_sdg = sdg_regions.T.dropna(how='all')
# %%
df_sdg = df_sdg[['LocID', 'Location', 'SDMX_Code', 'ParentID']]
# %%
df_sdg
# %%
df_sdg.columns = ['sdg_region', 'name', 'sdmx_code', 'parent_id']
# %%
df_sdg['is--sdg_region'] = "TRUE"

# %%
def fix_int_str(df, cols):
    def run(s):
        if not pd.isnull(s):
            return str(int(s))
        return ""
    for c in cols:
        df[c] = df[c].map(run)

    return df
# %%
df_sdg = fix_int_str(df_sdg, ['sdmx_code', 'parent_id'])

# %%
df_sdg

# %%
df_sdg.to_csv('../../ddf--entities--location--sdg_region.csv', index=False)
# %%
df_world = esets['world'][0].to_frame().T
# %%
df_world = df_world.dropna(axis=1)
# %%
df_world
# %%
df_world = df_world[['LocID', 'Location', 'SDMX_Code']].copy()
# %%
df_world.columns = ['world', 'name', 'sdmx_code']
# %%
df_world['is--world'] = "TRUE"
# %%
df_world = fix_int_str(df_world, ['sdmx_code'])

df_world.to_csv('../../ddf--entities--location--world.csv', index=False)
# %%
esets.keys()
# %%
development_group = pd.concat(esets['development_group'], axis=1)
# %%
development_group
# %%
df_devel = development_group.dropna(how='all').T
# %%
df_devel
# %%
df_devel = df_devel[['LocID', 'Location', 'SDMX_Code', 'ParentID']].copy()
# %%
df_devel.columns = ['development_group', 'name', 'sdmx_code', 'parent_id']
# %%
df_devel
# %%
df_devel['is--development_group'] = "TRUE"
# %%
df_devel = fix_int_str(df_devel, ['sdmx_code', 'parent_id'])

df_devel.to_csv('../../ddf--entities--location--development_group.csv', index=False)
# %%
esets.keys()
# %%
income = pd.concat(esets['income_group'], axis=1).dropna().T
# %%
income
# %%
df_income = income[['LocID', 'Location', 'ParentID']].copy()
# %%
df_income.columns = ['income_group', 'name', 'parent_id']
# %%
df_income['is--income_group'] = "TRUE"
# %%
df_income = fix_int_str(df_income, ['parent_id'])

df_income.to_csv('../../ddf--entities--location--income_group.csv', index=False)
# %%
esets.keys()
# %%
geos = pd.concat(esets['geographic_region'], axis=1).dropna(how='all').T
# %%
geos
# %%
geos.columns
# %%
# cols = ['LocID', 'Location', 'ISO3_Code', 'ISO2_Code', 'SDMX_Code', ]
# %%
gs = geos.groupby('LocTypeName')
# %%
df1 = gs.get_group('Geographic region').dropna(how='all', axis=1)
# %%
df1
# %%
df1 = df1[['LocID', 'Location', 'SDMX_Code', 'ParentID']].copy()
# %%
df1.columns = ['geographic_region', 'name', 'sdmx_code', 'parent_id']
# %%
df1['is--geographic_region'] = "TRUE"
# %%
df1 = fix_int_str(df1, ['sdmx_code', 'parent_id'])
df1.to_csv('../../ddf--entities--location--geographic_region.csv', index=False)
# %%
df2 = gs.get_group('Subregion').dropna(how='all', axis=1)
# %%
df2
# %%
# df2 = df2[['LocID', 'Location', 'SDMX_Code', 'ParentID', 'SDGRegID']].copy()
# we don't include the SDG region ID for now
df2 = df2[['LocID', 'Location', 'SDMX_Code', 'ParentID']].copy()
# %%
df2.columns = ['subregion', 'name', 'sdmx_code', 'parent_id']
# %%
df2
# %%
df2['is--subregion'] = "TRUE"
# %%
df2 = fix_int_str(df2, ['sdmx_code', 'parent_id'])
df2.to_csv('../../ddf--entities--location--subregion.csv', index=False)
# %%
df3 = gs.get_group('Country/Area').dropna(how='all', axis=1)
# %%
df3
# %%
df3.columns
# %%
df3 = df3[['LocID', 'Location', 'ISO3_Code', 'ISO2_Code',
       'SDMX_Code', 'ParentID', 'WorldID',
       'SubRegID', 'SDGSubRegID', 'SDGSubRegName', 'SDGRegID',
       'SDGRegName', 'GeoRegID', 'GeoRegName', 'MoreDev', 'LessDev',
       'LeastDev', 'oLessDev', 'LessDev_ExcludingChina', 'LLDC', 'SIDS',
       'WB_HIC', 'WB_MIC', 'WB_UMIC', 'WB_LMIC', 'WB_LIC', 'WB_NoIncomeGroup']].copy()
# %%
df3
# %%
df3['SDGSubRegID'].dropna()
# %%
# NOTE: There is 2 SDG sub-region in data which do not have their own rows. i.e only shown in properties of other rows.
# For simpility we only include sub regions and geo regions as drill ups for countries.

# %%
df3 = df3[['LocID', 'Location', 'ISO3_Code', 'ISO2_Code',
           'SDMX_Code', 'ParentID', 
           'SubRegID', 'GeoRegID']].copy()
# %%
df3
# %%
df3.columns = ['country_area', 'name', 'iso3_code', 'iso2_code', 'sdmx_code', 'parent_id', 'sub_region', 'geographic_region']
# %%
df3['is--country_area'] = "TRUE"
# %%
df3 = fix_int_str(df3, ['sdmx_code', 'parent_id', 'sub_region', 'geographic_region'])
df3.to_csv('../../ddf--entities--location--country_area.csv', index=False)
# %%
# Now create age1 and age5 entity domain

source_file = '../source/WPP2022_DeathsBySingleAgeSex_Medium_1950-2021.zip'
# %%
data2 = pd.read_csv(source_file)
# %%
data2.head()
# %%
age1 = data2['AgeGrp'].drop_duplicates()
# %%
age1 = age1.to_frame()
# %%
age1
# %%
def age_group_id(x):
    if '+' in x:
        return x[:-1] + 'plus'
    return x

# %%
age1['age_group_1year'] = age1['AgeGrp'].map(age_group_id)
# %%
age1.columns = ['name', 'age_group_1year']
# %%
age1 = age1[['age_group_1year', 'name']].copy()
# %%
age1['is--age_group_1year'] = 'TRUE'
# %%
age1
# %%
age1.to_csv('../../ddf--entities--age_group_1year.csv', index=False)
# %%
source_file = '../source/WPP2022_PopulationExposureByAge5GroupSex_Medium.zip'
# %%
data3 = pd.read_csv(source_file)
# %%
age5 = data3['AgeGrp'].drop_duplicates()
# %%
age5
# %%
age5.index = age5.map(age_group_id)
# %%
age5
# %%
age5.index.name = 'age_group_5year'
age5.name = 'name'
# %%
age5 = age5.reset_index()
# %%
age5['is--age_group_5year'] = 'TRUE'

# %%
age5['age_group_5year'] = age5['age_group_5year'].map(to_concept_id)
# %%
age5.to_csv('../../ddf--entities--age_group_5year.csv', index=False)
# %%
# Age Broad
source_file = "../source/WPP2022_Life_Table_Abridged_Medium_1950-2021.zip"

# %%
data4 = pd.read_csv(source_file)
# %%
age_b = data4['AgeGrp'].drop_duplicates()
# %%
age_b.index = age_b.map(age_group_id)
# %%
age_b.index.name = 'age_group_broad'
age_b.name = 'name'
# %%
age_b = age_b.reset_index()
# %%
age_b['is--age_group_broad'] = 'TRUE'
age_b['age_group_broad'] = age_b['age_group_broad'].map(to_concept_id)
# %%
age_b
# %%
age_b.to_csv('../../ddf--entities--age_group_broad.csv', index=False)

# Gender
# %%
gender = pd.DataFrame([[1, 'male'], [2, 'female']])
# %%
gender
# %%
gender.columns = ['sex', 'name']
# %%
gender['is--sex'] = 'TRUE'
# %%
gender.to_csv('../../ddf--entities--sex.csv', index=False)
# %%
source_file = '../source/metadata.xlsx'
# %%
concepts1 = pd.read_excel(source_file, sheet_name='indicators')
concepts2 = pd.read_excel(source_file, sheet_name='other_cols')
# %%
concepts2
# %%
concepts1
# %%
concepts1['concept_type'] = 'measure'
# %%
concepts1 = concepts1.drop(columns=['wpp_column_name'])
# %%
concepts1.columns = ['concept_id', 'name', 'unit', 'concept_type']
# %%
concepts2
# %%
concepts = pd.concat([concepts1, concepts2])
# %%
concepts
# %%
concepts.to_csv('../../ddf--concepts.csv', index=False)
# %%
