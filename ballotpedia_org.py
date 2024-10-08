import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import geopandas as gpd
from shutil import make_archive
import re
import inflect
p = inflect.engine()

# Mapping dictionary for state names to state codes
state_code_mapping = {'alabama': '01','alaska': '02','arizona': '04','arkansas': '05','california': '06','colorado': '08','connecticut': '09','delaware': '10','district of columbia': '11','florida': '12','georgia': '13','hawaii': '15','idaho': '16','illinois': '17','indiana': '18','iowa': '19','kansas': '20','kentucky': '21','louisiana': '22','maine': '23','maryland': '24','massachusetts': '25','michigan': '26','minnesota': '27','mississippi': '28','missouri': '29','montana': '30','nebraska': '31','nevada': '32','new hampshire': '33','new jersey': '34','new mexico': '35','new york': '36','north carolina': '37','north dakota': '38','ohio': '39','oklahoma': '40','oregon': '41','pennsylvania': '42','rhode island': '44','south carolina': '45','south dakota': '46','tennessee': '47','texas': '48','utah': '49','vermont': '50','virginia': '51','washington': '53','west virginia': '54','wisconsin': '55','wyoming': '56'}

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

def convert_ordinal_to_word(ordinal_str):
    try:
        # Extract the number from the ordinal (e.g., '1st' becomes '1')
        number = re.search(r'\d+', ordinal_str).group()
        # Convert the number to its word form (e.g., '1' becomes 'first')
        word = p.number_to_words(p.ordinal(number))
        return ordinal_str.replace(number + ordinal_str[-2:], word)  # Replace only the ordinal part
    except Exception as e:
        print(f"Error converting ordinal: {e}")
        return ordinal_str  # Return the original string if conversion fails

# Function to extract and process the district number and name
def extract_massachusetts_district(row):
    office = row['Office']
    state = row['State'].lower()

    # For upper house (State Senate), extract between 'Senate' and 'District'
    if 'senate' in office.lower():
        match = re.search(r'Senate (.*?) District', office)
        if match:
            dist_name = match.group(1).strip()
            # Split the first part (which is expected to be an ordinal) and convert it to words
            parts = dist_name.split(' ', 1)  # Split into ordinal part and rest of the name
            if len(parts) > 1:
                return f"{convert_ordinal_to_word(parts[0])} {parts[1]}"
            else:
                return convert_ordinal_to_word(parts[0])  # If there's no additional name

    # For lower house (House of Representatives), extract between 'Representatives' and 'District'
    elif 'representatives' in office.lower():
        match = re.search(r'Representatives (.*?) District', office)
        if match:
            dist_name = match.group(1).strip()
            # Split the first part (which is expected to be an ordinal) and convert it to words
            parts = dist_name.split(' ', 1)
            if len(parts) > 1:
                return f"{convert_ordinal_to_word(parts[0])} {parts[1]}"
            else:
                return convert_ordinal_to_word(parts[0])
    return row['dist_num']

def extract_vermont_district(row):
    office = row['Office']
    state = row['State'].lower()

    # For Vermont Upper House (State Senate), extract between 'Senate' and 'District'
    if 'senate' in office.lower():
        match = re.search(r'Senate (.*?) District', office)
        if match:
            dist_name = match.group(1).strip()
            return dist_name  # Return the full district name (e.g., 'Addison')

    # For Vermont Lower House (House of Representatives), extract between 'Representatives' and 'District'
    elif 'representatives' in office.lower():
        if len(office.split('Representatives')) > 1:
            dist_name = office.split('Representatives')[1].split('District')[0].strip()
            return '-'.join(dist_name.split())  # Join parts with a hyphen (e.g., 'Windsor 6' becomes 'Windsor-6')

    # If no match is found, return the original dist_num
    return row['dist_num']

def extract_state_from_office(office):
    # Modified regex to capture cases like 'General Assembly' along with 'House', 'State', and 'Senate'
    match = re.match(r"^(.*?)\s(House|State|Senate|General Assembly)", office)
    if match:
        return match.group(1).strip()  # Return the extracted state name
    return ''  # Return an empty string if no match is found

def process_legislature(links, name):
    # Fetch office and representative names
    if name == "us_lower_dist_representatives":
        name_column = "lh_rep_name"
    else:
        name_column = "uh_rep_name"

    office_name_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        office_name_data = list(tqdm(executor.map(extract_office_name, links), total=len(links)))
    # Flatten the list of lists
    office_name_data = [item for sublist in office_name_data for item in sublist]
    result_df = pd.DataFrame(office_name_data, columns=['Office', f"{name_column}"])
    result_df['State'] = result_df['Office'].apply(extract_state_from_office)
    result_df['dist_num'] = result_df.apply(lambda row: row['Office'].split('District ')[-1].split('-')[0].strip() if row['State'].lower() == 'washington' else row['Office'].split(' ')[-1], axis=1)
    result_df['dist_num'] = result_df.apply(lambda row: extract_vermont_district(row) if row['State'].lower() == 'vermont' else row['dist_num'], axis=1)
    # For New Hampshire, extract everything after 'Representatives'
    result_df['dist_num'] = result_df.apply(lambda row: row['Office'].split('Representatives')[-1].strip() if row['State'].lower() == 'new hampshire' and len(row['Office'].split('Representatives')) > 1 else row['dist_num'], axis=1)
    result_df['dist_num'] = result_df.apply(lambda row: extract_massachusetts_district(row) if row['State'].lower() == 'massachusetts' else row['dist_num'], axis=1)
    result_df['state_name'] = result_df['State'].str.lower().str.strip()
    result_df['state_code'] = result_df['state_name'].map(state_code_mapping)
    result_df['state_name'] = result_df['state_name'].str.title()
    output_folder = "representative_data"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    # Save the resulting DataFrame to an Excel file in the specified folder
    result_df.drop(columns=['State', 'Office'], inplace=True)
    result_df.to_excel(os.path.join(output_folder, f"{name}.xlsx"), index=False)
    
# Main function to read zip files, unzip, and process shapefiles
def main():
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

#         # Separate lists for house and senate links
        house_links = [row['House Link'] for _, row in df.iterrows() if row['House Link'] != "N/A"]
        senate_links = [row['Senate Link'] for _, row in df.iterrows() if row['Senate Link'] != "N/A"]

        process_legislature(house_links, "us_lower_dist_representatives")
        process_legislature(senate_links, "us_upper_dist_representatives")

main()