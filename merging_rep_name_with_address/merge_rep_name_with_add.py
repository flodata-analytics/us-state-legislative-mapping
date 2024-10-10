import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import re

def add_rep_name_to_add_low(address_excel_path,shapefile_path,disctrict_rep_data):
    df = pd.read_excel(address_excel_path)
    gdf = gpd.read_file(f"zip://{shapefile_path}")

    df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    points_gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    if points_gdf.crs != gdf.crs:
        points_gdf = points_gdf.to_crs(gdf.crs)

    joined_gdf = gpd.sjoin(points_gdf, gdf[['geometry', 'BASENAME', 'STATE']], how="left", predicate="within")
    df['STATE'] = joined_gdf['STATE'].fillna('')

    # Add the BASENAME and STATE columns from the shapefile to your DataFrame
    df['BASENAME'] = joined_gdf['BASENAME']
    df['STATE'] = joined_gdf['STATE']
    df.drop(columns=['geometry'], inplace=True)
    excel_df = pd.read_excel(disctrict_rep_data)

    # Step 8: Clean and prepare the columns for merging
    df['BASENAME'] = df['BASENAME'].str.strip().str.lower()
    df['STATE'] = df['STATE'].astype(str).str.zfill(2)

    excel_df['dist_num'] = excel_df['dist_num'].str.strip().str.lower()

    # Define the function to replace ', ' and ' and ' with '-' only if a comma exists
    def replace_comma_and(text):
        if ',' in text:
            text = re.sub(r'\s*and\s*', '-', text)  # Replace 'and' with '-'
            text = text.replace(', ', '-')
            text = text.replace(',-', '-')
        return text

    # Apply the replacement only for rows where 'state_code' is 25
    excel_df.loc[excel_df['state_code'] == 25, 'dist_num'] = excel_df.loc[
        excel_df['state_code'] == 25, 'dist_num'].apply(replace_comma_and)
    
    if any(excel_df['state_code'] == 16):
        excel_df_1 = excel_df[excel_df['state_code'] == 16].copy()
        
        # Apply digit extraction on dist_num
        excel_df_1['dist_num'] = excel_df_1['dist_num'].str.extract(r'(\d+)')
        # Ensure matching row count and update the original dataframe
        excel_df.loc[excel_df['state_code'] == 16, 'dist_num'] = excel_df_1['dist_num']
        excel_df.loc[excel_df['state_code'] == 16, 'lh_rep_name'] = excel_df_1['lh_rep_name']

    # Ensure correct formatting for state_code
    excel_df['state_code'] = excel_df['state_code'].fillna(0).astype(int)
    excel_df['state_code'] = excel_df['state_code'].astype(str).str.zfill(2)
    # Group by 'dist_num' and 'state_code', combining 'uh_rep_name' with "///"
    excel_df = excel_df.groupby(['dist_num', 'state_code', 'state_name'], as_index=False).agg({
        'lh_rep_name': lambda x: '///'.join(x)
    })

    merged_df = df.merge(excel_df, left_on=['BASENAME', 'STATE'], right_on=['dist_num', 'state_code'], how='left')
    merged_df.drop(columns=['dist_num', 'state_code'], inplace=True)
    merged_df.rename(columns={'BASENAME':'Dist Num Lower House', 'STATE':'State Code', 'state_name':'State Name','State':'State Abb','lh_rep_name':'Rep Lower House'}, inplace=True)
    print(merged_df.head())
    return merged_df

def add_rep_name_to_add_upper(address_excel_path,shapefile_path,disctrict_rep_data):
    df = pd.read_excel(address_excel_path)
    gdf = gpd.read_file(f"zip://{shapefile_path}")

    df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    points_gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    if points_gdf.crs != gdf.crs:
        points_gdf = points_gdf.to_crs(gdf.crs)

    joined_gdf = gpd.sjoin(points_gdf, gdf[['geometry', 'BASENAME', 'STATE']], how="left", predicate="within")
    df['STATE'] = joined_gdf['STATE'].fillna('')

    # Add the BASENAME and STATE columns from the shapefile to your DataFrame
    df['BASENAME'] = joined_gdf['BASENAME']
    df['STATE'] = joined_gdf['STATE']
    df.drop(columns=['geometry'], inplace=True)
    excel_df = pd.read_excel(disctrict_rep_data)

    # Step 8: Clean and prepare the columns for merging
    df['BASENAME'] = df['BASENAME'].str.strip().str.lower()
    df['STATE'] = df['STATE'].astype(str).str.zfill(2)

    excel_df['dist_num'] = excel_df['dist_num'].str.strip().str.lower()

    # Define the function to replace ', ' and ' and ' with '-' only if a comma exists
    def replace_comma_and(text):
        if ',' in text:
            text = re.sub(r'\s*and\s*', '-', text)  # Replace 'and' with '-'
            text = text.replace(', ', '-')
            text = text.replace(',-', '-')
        return text

    # Apply the replacement only for rows where 'state_code' is 25
    excel_df.loc[excel_df['state_code'] == 25, 'dist_num'] = excel_df.loc[
        excel_df['state_code'] == 25, 'dist_num'].apply(replace_comma_and)

    # Group by 'dist_num' and 'state_code', combining 'uh_rep_name' with "///"
    excel_df = excel_df.groupby(['dist_num', 'state_code', 'state_name'], as_index=False).agg({
        'uh_rep_name': lambda x: '///'.join(x)
    })

    # Ensure the 'state_code' column is correctly formatted
    excel_df['state_code'] = excel_df['state_code'].astype(str).str.zfill(2)
    merged_df = df.merge(excel_df, left_on=['BASENAME', 'STATE'], right_on=['dist_num', 'state_code'], how='left')
    merged_df.drop(columns=['dist_num', 'state_code'], inplace=True)
    merged_df.rename(columns={'BASENAME':'Dist Num Upper House', 'STATE':'State Code', 'state_name':'State Name','State':'State Abb','uh_rep_name':'Rep Upper House'}, inplace=True)
    print(merged_df.head())
    return merged_df

import pandas as pd

import pandas as pd

def merge_upper_lower_full(df1, df2):
    # Merge on specified columns
    merged_df = pd.merge(
        df1,
        df2,
        on=[
            'Corp', 'Street Address', 'Street Address - Secondary', 'City',
            'State Abb', 'Zip Code', 'County', 'latitude', 'longitude'
        ],
        how='outer',
        suffixes=('_upper', '_lower')
    )
    
    # Print the columns of the merged DataFrame for debugging
    print("Columns in merged DataFrame:", merged_df.columns.tolist())
    
    # Create the final DataFrame with the desired columns
    final_df = pd.DataFrame()

    # Fill the final DataFrame with columns from the merged DataFrame
    final_df['Corp'] = merged_df.get('Corp', pd.Series()).fillna('')
    final_df['Street Address'] = merged_df.get('Street Address', pd.Series()).fillna('')
    final_df['Street Address - Secondary'] = merged_df.get('Street Address - Secondary', pd.Series()).fillna('')
    final_df['City'] = merged_df.get('City', pd.Series()).fillna('')
    final_df['State Abb'] = merged_df.get('State Abb', pd.Series()).fillna('')
    final_df['Zip Code'] = merged_df.get('Zip Code', pd.Series()).fillna('')
    final_df['County'] = merged_df.get('County', pd.Series()).fillna('')
    final_df['latitude'] = merged_df.get('latitude', pd.Series()).fillna('')
    final_df['longitude'] = merged_df.get('longitude', pd.Series()).fillna('')

    # Handle State Code and State Name
    state_code_upper = merged_df.get('State Code_upper', pd.Series()).fillna('')
    state_code_lower = merged_df.get('State Code_lower', pd.Series()).fillna('')
    final_df['State Code'] = state_code_upper.where(state_code_upper != '', state_code_lower)

    state_name_upper = merged_df.get('State Name_upper', pd.Series()).fillna('')
    state_name_lower = merged_df.get('State Name_lower', pd.Series()).fillna('')
    final_df['State Name'] = state_name_upper.where(state_name_upper != '', state_name_lower)

    # Handle Dist Num Upper House and Lower House
    final_df['Dist Num Lower House'] = merged_df.get('Dist Num Lower House', pd.Series()).fillna('')
    final_df['Dist Num Upper House'] = merged_df.get('Dist Num Upper House', pd.Series()).fillna('')

    # Combine representative names
    final_df['Rep Lower House'] = merged_df.get('Rep Lower House', pd.Series()).fillna('')
    final_df['Rep Upper House'] = merged_df.get('Rep Upper House', pd.Series()).fillna('')
    
    # Ensure the column order is as desired
    final_df = final_df[['Corp', 'Street Address', 'Street Address - Secondary', 'City',
                          'State Abb', 'Zip Code', 'County', 'latitude', 'longitude',
                          'State Code', 'State Name', 'Dist Num Lower House', 
                          'Dist Num Upper House', 'Rep Upper House', 'Rep Lower House']]

    return final_df



if __name__=="__main__":
    disctrict_rep_data = r"representative_data/us_upper_dist_representatives.xlsx"
    disctrict_rep_data_lh = r"representative_data/us_lower_dist_representatives.xlsx"
    address_excel_path = "coordinate_output.xlsx"
    shapefile_path = "State_Legislative_Upper.zip"
    shapefile_path_lh = "State_Legislative_Lower.zip"
    output_excel_name_lh = "merged_name_add_upper_house.xlsx"
    output_excel_name_uh = "merged_name_add_lower_house.xlsx"
    df1 = add_rep_name_to_add_upper(address_excel_path,shapefile_path,disctrict_rep_data)
    df2 = add_rep_name_to_add_low(address_excel_path,shapefile_path_lh,disctrict_rep_data_lh)
    print(df1.columns)
    print(df2.columns)
        # Merge both DataFrames
    final_merged_df = merge_upper_lower_full(df1, df2)

    # Save the result to an Excel file
    output_excel_name = "merged_upper_and_lower_house.xlsx"
    final_merged_df.to_excel(output_excel_name, index=False)

    print(final_merged_df.head())