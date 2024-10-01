import zipfile
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import geopandas as gpd
from shutil import make_archive

# Mapping dictionary for state names to state codes
state_code_mapping = {
    'alabama': '01',
    'alaska': '02',
    'arizona': '04',
    'arkansas': '05',
    'california': '06',
    'colorado': '08',
    'connecticut': '09',
    'delaware': '10',
    'district of columbia': '11',
    'florida': '12',
    'georgia': '13',
    'hawaii': '15',
    'idaho': '16',
    'illinois': '17',
    'indiana': '18',
    'iowa': '19',
    'kansas': '20',
    'kentucky': '21',
    'louisiana': '22',
    'maine': '23',
    'maryland': '24',
    'massachusetts': '25',
    'michigan': '26',
    'minnesota': '27',
    'mississippi': '28',
    'missouri': '29',
    'montana': '30',
    'nebraska': '31',
    'nevada': '32',
    'new hampshire': '33',
    'new jersey': '34',
    'new mexico': '35',
    'new york': '36',
    'north carolina': '37',
    'north dakota': '38',
    'ohio': '39',
    'oklahoma': '40',
    'oregon': '41',
    'pennsylvania': '42',
    'rhode island': '44',
    'south carolina': '45',
    'south dakota': '46',
    'tennessee': '47',
    'texas': '48',
    'utah': '49',
    'vermont': '50',
    'virginia': '51',
    'washington': '53',
    'west virginia': '54',
    'wisconsin': '55',
    'wyoming': '56'
}

# List of states to exclude from the merge
excluded_states = ['Idaho', 'Massachusetts', 'Maryland', 'North Dakota', 'South Dakota',
                   'Washington', 'Arizona', 'Vermont', 'New Hampshire', 'Minnesota']

# Clean and deduplicate the list
excluded_states = list(set([state.lower().strip() for state in excluded_states]))

# Function to extract office and name from the correct table
def extract_office_name(url):
    office_name_data = []
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table')
            for table in tables:
                thead = table.find('thead')
                if thead:
                    headers = [header.text.strip() for header in thead.find_all('th')]
                    if "Office" in headers and "Name" in headers:
                        rows = table.find_all('tr')[1:]  # Skip header row
                        for row in rows:
                            columns = row.find_all('td')
                            if len(columns) >= 2:
                                office = columns[0].text.strip()
                                name = columns[1].text.strip()
                                office_name_data.append([office, name])
                        break
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return office_name_data

# Process house/senate links and create merged shapefile in zip
def process_legislature(legislature_name, zip_path, links):
    # Unzip the shapefile
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(f'temp_{legislature_name}')

    # Fetch office and representative names
    office_name_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        office_name_data = list(tqdm(executor.map(extract_office_name, links), total=len(links)))
    
    # Flatten the list of lists
    office_name_data = [item for sublist in office_name_data for item in sublist]

    # Create a DataFrame for the results
    result_df = pd.DataFrame(office_name_data, columns=['Office', 'Name'])
    result_df = result_df.rename(columns = {'Name':'representative_name'})

    # Add State and dist_num columns based on the Office column
    result_df['State'] = result_df['Office'].apply(lambda x: x.split(' ')[0])
    result_df['dist_num'] = result_df['Office'].apply(lambda x: x.split(' ')[-1])

    # Convert state names to lowercase and strip whitespace
    result_df['state_name'] = result_df['State'].str.lower().str.strip()

    # Map state names to state codes and add a 'STATE' column
    result_df['STATE'] = result_df['state_name'].map(state_code_mapping)

    # Read the shapefile
    shapefile_path = [file for file in os.listdir(f'temp_{legislature_name}') if file.endswith('.shp')][0]
    gdf = gpd.read_file(os.path.join(f'temp_{legislature_name}', shapefile_path))

    # Ensure 'STATE' and 'BASENAME' are properly formatted
    gdf['STATE'] = gdf['STATE'].astype(str)
    gdf['BASENAME'] = gdf['BASENAME'].astype(str).str.strip()
    result_df['STATE'] = result_df['STATE'].astype(str)
    result_df['dist_num'] = result_df['dist_num'].astype(str).str.strip()

    # Exclude states that are in the exclusion list
    gdf = gdf[~gdf['STATE'].isin(result_df[result_df['state_name'].isin(excluded_states)]['STATE'])]

    # Perform the merge on 'STATE' and 'BASENAME' from shapefile with 'STATE' and 'dist_num' from DataFrame
    merged_gdf = gdf.merge(result_df, left_on=['STATE', 'BASENAME'], right_on=['STATE', 'dist_num'], how='left')

    # Drop the extra columns from the merge
    merged_gdf = merged_gdf.drop(columns=['State'])

    # Save the merged shapefile to the temp directory
    merged_shapefile_path = os.path.join(f'temp_{legislature_name}', f"{legislature_name}_merged.shp")
    merged_gdf.to_file(merged_shapefile_path)

    # Create the zip file with only the merged shapefile
    output_zip = f"{legislature_name}_merged"
    make_archive(output_zip, 'zip', f'temp_{legislature_name}')

    # Clean up the temporary folder (ensure only the merged shapefile is saved in the zip)
    for file in os.listdir(f'temp_{legislature_name}'):
        os.remove(os.path.join(f'temp_{legislature_name}', file))
    os.rmdir(f'temp_{legislature_name}')

    print(f"{legislature_name.capitalize()} shapefile merged and zipped as {output_zip}.zip")

# Main function to read zip files, unzip, and process shapefiles
def main():
    # Find the zip files in the current directory
    lower_zip = "State_Legislative_Lower.zip"
    upper_zip = "State_Legislative_Upper.zip"

    # URL of the page
    url = "https://ballotpedia.org/List_of_United_States_state_legislatures"
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table_rows = soup.find_all('tr')

        data = []
        for row in table_rows:
            columns = row.find_all('td')
            if len(columns) == 3:
                state_legislature = columns[0].a.get('title')
                house_link = columns[1].a.get('href') if columns[1].a else "N/A"
                senate_link = columns[2].a.get('href') if columns[2].a else "N/A"
                house_link = f"https://ballotpedia.org{house_link}" if house_link != "N/A" else house_link
                senate_link = f"https://ballotpedia.org{senate_link}" if senate_link != "N/A" else senate_link
                data.append([state_legislature, house_link, senate_link])

        df = pd.DataFrame(data, columns=["State Legislature", "House Link", "Senate Link"])

        # Separate lists for house and senate links
        house_links = [row['House Link'] for _, row in df.iterrows() if row['House Link'] != "N/A"]
        senate_links = [row['Senate Link'] for _, row in df.iterrows() if row['Senate Link'] != "N/A"]

        # Process lower house zip if it exists
        if os.path.exists(lower_zip):
            process_legislature('lower_house', lower_zip, house_links)

        # Process upper house zip if it exists
        if os.path.exists(upper_zip):
            process_legislature('upper_house', upper_zip, senate_links)

# Run the main function
main()
