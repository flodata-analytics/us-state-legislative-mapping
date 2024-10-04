# Legislature Data Extraction Script

## Overview

This Python script automates the extraction of representative data for the lower and upper houses of the state legislatures in the United States. It scrapes data from Ballotpedia.org, processes the district names and representative details, and saves the output into Excel files.

## Functionality

1. **Web Scraping**:
   - The script scrapes representative data (office name and representative name) from the Ballotpedia website.
   - It extracts links for both the House of Representatives and State Senate for each U.S. state legislature.

2. **Data Processing**:
   - Based on the state and the type of legislature (upper or lower house), the script applies specific logic to extract and standardize the district names.
   - The district numbers and representative names are processed, particularly for states like Vermont, New Hampshire, and Massachusetts, where district name formats are unique.
   - It maps state names to state codes using a pre-defined dictionary.

3. **Output**:
   - Two Excel files (`lh_rep_name.xlsx` and `uh_rep_name.xlsx`) are generated, containing representative names, district numbers, and corresponding state codes.

## Requirements

### Python Libraries
The script requires the following Python libraries:
- `requests`: For sending HTTP requests.
- `beautifulsoup4`: For parsing HTML and scraping data from tables.
- `pandas`: For handling data frames and processing.
- `tqdm`: For displaying progress bars during data extraction.
- `concurrent.futures`: For managing concurrent requests.
- `inflect`: For converting ordinal numbers into words.
- `re`: For handling regular expressions to extract district information.

You can install all dependencies by running:

```bash
pip install requests beautifulsoup4 pandas tqdm inflect
```