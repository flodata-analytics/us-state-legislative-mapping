import pandas as pd
import requests
import time

def get_location_freeform(query):
    # API endpoint
    url = "https://nominatim.openstreetmap.org/search"
    
    # Set parameters for the free-form query
    params = {
        'q': query,
        'format': 'json',  # response format
        'addressdetails': 1,  # include address breakdown in the response
        'limit': 1  # get only the top result
    }

    # Set headers including a User-Agent
    headers = {'User-Agent': 'MyPythonApp/1.0 (myemail@example.com)'}

    # Send GET request to the Nominatim API
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if data:
            lat = round(float(data[0]['lat']), 7)
            lng = round(float(data[0]['lon']), 7)
            return lat, lng
        else:
            return None, None
    else:
        return None, None

# Read CSV file
df = pd.read_csv(r"coordinate.csv")

def build_address(row):
    street = row['Street Address']
    city = row['City']
    state = row['State']
    zipcode = str(row['Zip Code'])[:5]  # Use only the first 5 digits of the ZIP code
    return f"{street}, {city}, {state} {zipcode}".strip()

df['Full_Address'] = df.apply(build_address, axis=1)

# Create empty columns for latitude and longitude
df['latitude'] = None
df['longitude'] = None

# Loop through each address and get coordinates
for index, row in df.iterrows():
    address = row['Full_Address']
    lat, lng = get_location_freeform(address)
    df.at[index, 'latitude'] = lat
    df.at[index, 'longitude'] = lng
    print(f"Processed: {address} -> lat: {lat}, lng: {lng}")
    time.sleep(1)  # Respect the rate limits

# Remove the Full_Address column before saving
df = df.drop(columns=['Full_Address'])

# Save to new Excel file
df.to_excel('coordinate_output.xlsx', index=False)

print("Output saved to coordinate_output.xlsx")