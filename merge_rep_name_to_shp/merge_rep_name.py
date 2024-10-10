import geopandas as gpd
import pandas as pd
import zipfile
import os
import re

def lower_house_shape_file_modification(lower_zip,excel_file_lower,output_zip_lower):
    extract_dir = "extracted_shp"
    output_dir = "modified_shp"

    os.makedirs(output_folder, exist_ok=True)
    with zipfile.ZipFile(lower_zip, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    shapefile_path = os.path.join(extract_dir, "State_Legislative_Lower.shp")
    gdf = gpd.read_file(shapefile_path)
    excel_df = pd.read_excel(excel_file_lower)

    gdf['BASENAME'] = gdf['BASENAME'].str.strip().str.lower()
    gdf['STATE'] = gdf['STATE'].astype(str).str.zfill(2)

    excel_df['dist_num'] = excel_df['dist_num'].str.strip().str.lower()
    def replace_comma_and(text):
        if ',' in text:
            text = re.sub(r'\s*and\s*', '-', text)
            text = text.replace(', ', '-')
            text = text.replace(',-', '-')
        return text

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

    # Ensure state_code is a string with leading zeros
    excel_df['state_code'] = excel_df['state_code'].astype(str).str.zfill(2)
    excel_df = excel_df.groupby(['dist_num', 'state_code', 'state_name'], as_index=False).agg({'lh_rep_name': lambda x: '///'.join(x)})
    merged_gdf = gdf.merge(excel_df, left_on=['BASENAME', 'STATE'], right_on=['dist_num', 'state_code'], how='left')
    for i in range(10):
        merged_gdf[f'REP{i+1}'] = merged_gdf['lh_rep_name'].apply(lambda x: x.split('///')[i] if isinstance(x, str) and len(x.split('///')) > i else None)
    merged_gdf.drop(columns=['dist_num', 'state_code', 'lh_rep_name'], inplace=True)
    merged_gdf.rename(columns={'state_name': 'STATE NAME'}, inplace=True)
    os.makedirs(output_dir, exist_ok=True)
    output_shapefile = os.path.join(output_dir, "State_Legislative_Lower_Modified.shp")
    merged_gdf.to_file(output_shapefile)
    with zipfile.ZipFile(output_zip_lower, 'w') as new_zip:
        for file in os.listdir(output_dir):
            new_zip.write(os.path.join(output_dir, file), os.path.basename(file))

    import shutil
    shutil.rmtree(extract_dir)
    shutil.rmtree(output_dir)

    print(f"Modified shapefile has been saved to {output_zip_lower}")



def upper_house_shape_file_modification(upper_zip,excel_file,output_zip):
    # Temporary directories for extracting and saving shapefiles
    extract_dir = "extracted_shp"
    output_dir = "modified_shp"

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Step 1: Extract shapefile from the zip
    with zipfile.ZipFile(upper_zip, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Step 2: Read the shapefile using GeoPandas
    shapefile_path = os.path.join(extract_dir, "State_Legislative_Upper.shp")
    gdf = gpd.read_file(shapefile_path)

    # Step 3: Read the Excel file into a DataFrame
    excel_df = pd.read_excel(excel_file)

    gdf['BASENAME'] = gdf['BASENAME'].str.strip().str.lower()
    gdf['STATE'] = gdf['STATE'].astype(str).str.zfill(2)

    excel_df['dist_num'] = excel_df['dist_num'].str.strip().str.lower()

    # Define the function to replace ', ' and ' and ' with '-' only if a comma exists
    def replace_comma_and(text):
        if ',' in text:
            # Replace ', ' and ' and ' only if there is a comma in the text
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

    # Step 5: Merge the shapefile data with the Excel data
    merged_gdf = gdf.merge(excel_df, left_on=['BASENAME', 'STATE'], right_on=['dist_num', 'state_code'], how='left')

    # Step 6: Split the 'uh_rep_name' into separate columns REP1 through REP9
    for i in range(10):
        merged_gdf[f'REP{i+1}'] = merged_gdf['uh_rep_name'].apply(lambda x: x.split('///')[i] if isinstance(x, str) and len(x.split('///')) > i else None)

    # Step 7: Drop the columns used for merging (optional)
    merged_gdf.drop(columns=['dist_num', 'state_code', 'uh_rep_name'], inplace=True)

    # Step 8: Rename 'state_name' to 'STATE NAME'
    merged_gdf.rename(columns={'state_name': 'STATE NAME'}, inplace=True)

    # Step 9: Save the modified shapefile to a new folder
    os.makedirs(output_dir, exist_ok=True)
    output_shapefile = os.path.join(output_dir, "State_Legislative_Upper_Modified.shp")
    merged_gdf.to_file(output_shapefile)

    # Step 10: Create a new zip file with the modified shapefile in the output folder
    with zipfile.ZipFile(output_zip, 'w') as new_zip:
        for file in os.listdir(output_dir):
            new_zip.write(os.path.join(output_dir, file), os.path.basename(file))

    # Clean up temporary directories (optional)
    import shutil
    shutil.rmtree(extract_dir)
    shutil.rmtree(output_dir)

    print(f"Modified shapefile has been saved to {output_zip}")


if __name__=="__main__":
    # File paths
    output_folder = "zip_output_folder"
    upper_zip = r"us-state-legislative-mapping/State_Legislative_Upper.zip"
    excel_file = r"ballotpedia/representative_data/us_upper_dist_representatives.xlsx"
    output_zip = os.path.join(output_folder, "State_Legislative_Upper_Modified.zip")
    lower_zip = r"us-state-legislative-mapping/State_Legislative_Lower.zip"
    excel_file_lower = r"ballotpedia/representative_data/us_lower_dist_representatives.xlsx"
    output_zip_lower = os.path.join(output_folder, "State_Legislative_Lower_Modified.zip")
    lower_house_shape_file_modification(lower_zip,excel_file_lower,output_zip_lower)
    upper_house_shape_file_modification(upper_zip,excel_file,output_zip)




