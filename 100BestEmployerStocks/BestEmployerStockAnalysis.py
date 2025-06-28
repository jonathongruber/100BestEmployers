import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import time
import random
import re
import os 
from tqdm import tqdm # progress bars

# List of user agents to rotate, helping to avoid being blocked by websites.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
]

def get_forbes_employers():
    """
    Fetches the list of "America's Best Large Employers 2025" from Forbes.com.
    Parses the HTML to extract company names and performs basic cleaning and filtering.
    Returns a pandas DataFrame with 'Rank' and 'Company' columns.
    """
    url = "https://www.forbes.com/sites/rachelpeachman/2025/03/19/the-top-100-americas-best-large-employers-of--2025/"
    headers = {'User-Agent': random.choice(USER_AGENTS)}

    try:
        # Send an HTTP GET request to the URL
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Select <strong> tags that have the data-ga-track attribute.
        # This was identified by inspecting the Forbes page structure.
        companies_raw = soup.select("strong[data-ga-track]")
        print(f" Raw <strong> tags found: {len(companies_raw)}")

        companies = []
        excluded = []

        # Keywords to exclude, as they are likely not company names.
        exclude_keywords = [
            "contributor", "editor", "subscribe", "photo", "watch", "video", "2025"
        ]

        # Iterate through the raw items and filter out non-company names.
        for item in companies_raw:
            name = item.get_text(strip=True)
            reason = None

            if not name:
                reason = "empty"
            elif len(name) >= 60: # Company names are unlikely to be this long.
                reason = "too long"
            elif any(kw in name.lower() for kw in exclude_keywords):
                reason = "excluded keyword"
            elif name.replace('.', '', 1).isdigit(): # Exclude numeric strings.
                reason = "numeric"
            elif name.lower().startswith("by "): # Exclude author names.
                reason = "starts with 'by '"

            if reason:
                excluded.append((name, reason))
            else:
                companies.append(name)

        # De-duplicate the list of companies while preserving order.
        unique_companies = list(dict.fromkeys(companies))

        # Fix specific parsing issues where apostrophes (e.g., in "McDonald's")
        # might be split into separate entries due to HTML structure.
        fixed_companies = []
        skip_next = False

        for i in range(len(unique_companies)):
            if skip_next:
                skip_next = False
                continue

            current = unique_companies[i]

            # Check if the next entry is just "s", "'s", or "’s" (common apostrophe variants).
            if i + 1 < len(unique_companies) and unique_companies[i + 1].lower() in ["s", "'s", "’s"]:
                merged = current + "'s" # Merge them back together.
                fixed_companies.append(merged)
                skip_next = True
            else:
                fixed_companies.append(current)

        unique_companies = fixed_companies

        print(f"\n Valid companies extracted: {len(unique_companies)}")
        if len(unique_companies) < 100:
            print(f" Warning: Only {len(unique_companies)} companies found (expected 100). You may want to revise filters.")

        if excluded:
            print(f"\n First 5 excluded (of {len(excluded)}):")
            for name, reason in excluded[:5]:
                print(f" - {name}  [{reason}]")

        # Take only the top 100 companies found.
        top_100 = unique_companies[:100]

        # Create a pandas DataFrame from the cleaned list.
        forbes_df = pd.DataFrame({
            "Rank": range(1, len(top_100) + 1),
            "Company": top_100
        })

        print(f"\n Forbes 2025 Top {len(forbes_df)} Companies:")
        # In a real environment, you might use df.head() for display or save to CSV/Excel
        # display(forbes_df)

        return forbes_df

    except requests.exceptions.RequestException as e:
        print(f" Error fetching Forbes data: {e}")
        return pd.DataFrame(columns=["Rank", "Company"])

def get_greatplacetowork_employers():
    """
    Fetches the list of "100 Best Companies to Work For 2025" from GreatPlaceToWork.com.
    Parses the HTML to extract company names.
    Returns a pandas DataFrame with 'Rank' and 'Company' columns.
    """
    url = "https://www.greatplacetowork.com/best-workplaces/100-best/2025"
    headers = {'User-Agent': random.choice(USER_AGENTS)}

    try:
        # Send an HTTP GET request to the URL
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Inspected the page, company names are inside <a class="link h5">.
        companies_raw = soup.select("a.link.h5")
        print(f" Company <a> tags with class 'link h5' found: {len(companies_raw)}")

        # Extract text from the selected tags.
        companies = [c.get_text(strip=True) for c in companies_raw]

        # Basic cleanup: filter empty or excessively long names.
        filtered_companies = [name for name in companies if name and len(name) < 100]

        if len(filtered_companies) < 100:
            print(f" Warning: Only {len(filtered_companies)} companies found (expected 100).")

        # Create DataFrame
        gpwt_df = pd.DataFrame({
            "Rank": range(1, len(filtered_companies) + 1),
            "Company": filtered_companies
        })

        print(f"\n Great Place To Work 2025 Top {len(gpwt_df)} Companies:")
        # In a real environment, you might use df.head() for display or save to CSV/Excel
        # display(gpwt_df)

        return gpwt_df

    except requests.exceptions.RequestException as e:
        print(f" Error fetching Great Place To Work data: {e}")
        return pd.DataFrame(columns=["Rank", "Company"])

def get_ticker(company_name):
    """
    Attempts to find the stock ticker symbol for a given company name using Yahoo Finance's search API.
    """
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    # Yahoo Finance search API endpoint for company tickers.
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={company_name}"
    try:
        res = requests.get(url, headers=headers, timeout=5).json()
        # Iterate through search results to find an equity (stock) ticker.
        for result in res.get("quotes", []):
            if result.get("quoteType") == "EQUITY":
                return result.get("symbol") # Return the first equity ticker found.
    except requests.exceptions.RequestException as e:
        # print(f"Error getting ticker for {company_name}: {e}")
        pass # Suppress error for cleaner output if ticker not found
    return None

def get_stock_info(ticker, retries=3, min_delay=3, max_delay=8):
    """
    Fetches detailed stock information for a given ticker using the yfinance library.
    Includes retry logic with random delay for robustness.
    Returns available info, with None for missing fields.

    Args:
        ticker (str): The stock ticker symbol.
        retries (int): Maximum number of retries if an error occurs.
        min_delay (int): Minimum delay in seconds between retries.
        max_delay (int): Maximum delay in seconds between retries.

    Returns:
        dict: A dictionary of selected financial metrics, with None for missing fields.
              It will *always* return a dictionary with all expected keys, even if empty.
              Errors during fetch are handled by retries, but if all retries fail,
              the fields will simply be None.
    """
    # Initialize a dictionary with all expected keys and None values
    # This will be returned, populated with data if available.
    info_to_return = {
        "Ticker": ticker,
        "Name": None,
        "Sector": None,
        "Industry": None,
        "Price": None,
        "Market Cap": None,
        "PE Ratio": None,
        "Error": None # Add an error field to capture the last error if all retries fail
    }

    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info # Access the .info attribute for comprehensive data.

            # Populate the info_to_return with whatever is available from yfinance's info
            info_to_return["Name"] = info.get("shortName")
            info_to_return["Sector"] = info.get("sector")
            info_to_return["Industry"] = info.get("industry")
            info_to_return["Price"] = info.get("currentPrice")
            info_to_return["Market Cap"] = info.get("marketCap")
            info_to_return["PE Ratio"] = info.get("trailingPE")

            # If at least the Name is retrieved, consider it a success for this fetch chain.
            # We return it immediately, even if other fields are None.
            if info_to_return["Name"]:
                info_to_return["Error"] = None # Clear any previous error if successful
                return info_to_return
            else:
                # If even the Name is missing, it's a significant failure for this attempt.
                # Re-raise an error to trigger a retry.
                raise ValueError(f"Minimal info (Name) not received for {ticker}")

        except Exception as e:
            error_message = str(e)
            info_to_return["Error"] = error_message # Store the error message
            if attempt < retries - 1:
                current_delay = random.uniform(min_delay, max_delay)
                print(f"Error fetching stock info for {ticker} (Attempt {attempt+1}/{retries}): {error_message}. Retrying in {current_delay:.2f} seconds...")
                time.sleep(current_delay)
            else:
                print(f"Failed to fetch stock info for {ticker} after {retries} attempts: {error_message}")
                # After all retries, if we still couldn't get a name, return the info_to_return
                # It will have the last error message, and None for missing data.
                return info_to_return
    
    # This line should ideally not be reached if retries > 0 and the loop completes
    # It ensures a fallback if something unexpected happens.
    info_to_return["Error"] = info_to_return.get("Error", "Unknown error after all retries.")
    return info_to_return


def filter_by_strategy(df, strategy_type):
    """
    Filters a DataFrame of stock information based on predefined investment strategies.

    Args:
        df (pd.DataFrame): DataFrame containing stock data with 'Sector' and 'PE Ratio' columns.
        strategy_type (str): The investment strategy ("defensive", "cyclical", "growth").

    Returns:
        pd.DataFrame: A filtered DataFrame of companies matching the strategy.
    """
    if df.empty:
        return pd.DataFrame()

    filtered_df = pd.DataFrame()

    # Define common defensive sectors.
    defensive_sectors = ['Healthcare', 'Utilities', 'Consumer Defensive', 'Communication Services']
    # Define common cyclical sectors.
    cyclical_sectors = ['Industrials', 'Consumer Cyclical', 'Financial Services', 'Real Estate', 'Basic Materials']
    # Define common growth sectors.
    growth_sectors = ['Technology', 'Communication Services', 'Healthcare', 'Industrials'] # Some overlap with defensive for tech in healthcare

    # Filter based on whether 'Sector' and 'PE Ratio' are available and meet criteria
    # This assumes that if these fields are None, they won't meet the criteria
    if strategy_type.lower() == "defensive":
        filtered_df = df[
            (df['Sector'].isin(defensive_sectors)) &
            (df['PE Ratio'].notna()) & # Ensure PE Ratio is not None
            (df['PE Ratio'] < 20)
        ].sort_values(by='PE Ratio', ascending=True)
    elif strategy_type.lower() == "cyclical":
        filtered_df = df[
            (df['Sector'].isin(cyclical_sectors)) &
            (df['Market Cap'].notna()) # Ensure Market Cap is not None for sorting
        ].sort_values(by='Market Cap', ascending=False)
    elif strategy_type.lower() == "growth":
        filtered_df = df[
            (df['Sector'].isin(growth_sectors)) &
            (df['PE Ratio'].notna()) & # Ensure PE Ratio is not None
            (df['PE Ratio'] > 25)
        ].sort_values(by='PE Ratio', ascending=False)
    else:
        print(f"Warning: Unknown strategy type '{strategy_type}'. No filtering applied.")
        return df # Return original DataFrame if strategy is not recognized

    return filtered_df

# === MAIN EXECUTION BLOCK ===

print("Fetching data from Forbes...")
forbes_df = get_forbes_employers()
print(f"Forbes companies found: {len(forbes_df)}")
time.sleep(random.uniform(1, 3)) # Be polite to the servers

print("\nFetching data from Great Place To Work (Fortune list)...")
greatplaces_df = get_greatplacetowork_employers()
print(f"Great Place To Work companies found: {len(greatplaces_df)}")
time.sleep(random.uniform(1, 3))

# Convert company names to lists for set operations.
forbes_companies_list = forbes_df["Company"].tolist()
greatplaces_companies_list = greatplaces_df["Company"].tolist()

# Find common companies and all unique companies across both lists.
common_companies = set(forbes_companies_list) & set(greatplaces_companies_list)
all_unique_companies = set(forbes_companies_list) | set(greatplaces_companies_list)

print(f"\nCompanies in BOTH lists ({len(common_companies)}): {common_companies}")
print(f"\nCompanies in EITHER list ({len(all_unique_companies)}): {all_unique_companies}")

# Initialize a list to store all the collected stock data for ALL unique companies with tickers.
all_stock_data = []

# Loop through each unique company to get its ticker and stock information.
# Using tqdm for a progress bar during this potentially long process.
for i, company_name in tqdm(enumerate(list(all_unique_companies), 1), total=len(all_unique_companies), desc="Processing Companies"):
    ticker = get_ticker(company_name) # Get the stock ticker

    if ticker: # ONLY proceed if a ticker is found
        # Get stock info (will return partial data with None or full data)
        info = get_stock_info(ticker, retries=3, min_delay=3, max_delay=8)
        
        # Add the original company name to the info dictionary for later filtering
        info["Original Company Name"] = company_name
        all_stock_data.append(info) # Add all available data (even if partial) to the list

        # Optional: Print error message if a specific fetch failed after retries
        if info["Error"]:
            print(f"Partial data fetched for {company_name} ({ticker}) with error: {info['Error']}")
        # else:
            # print(f"Successfully added data for {info.get('Name', company_name)} ({ticker}).")
    # else:
        # print(f"No ticker found for: {company_name}. Skipping for stock analysis.") # Indicate why it's skipped

    # Introduce a random delay to avoid overwhelming the APIs and appearing like a bot.
    sleep_time = random.uniform(3, 8)
    time.sleep(sleep_time)

print("\nFinished processing all unique companies.") # Custom message after tqdm completes

# Convert the collected data into a comprehensive pandas DataFrame.
all_stock_data_df = pd.DataFrame(all_stock_data)

# Define the columns to be included in the Excel output.
excel_output_cols = ["Name", "Ticker", "PE Ratio", "Price", "Sector", "Industry", "Market Cap", "Original Company Name"]

# Ensure all excel_output_cols exist in the DataFrame, adding them with None if not present.
# This handles cases where yfinance might not return all fields for a "successful" fetch.
for col in excel_output_cols:
    if col not in all_stock_data_df.columns:
        all_stock_data_df[col] = None

# Reorder columns to ensure desired order in output
all_stock_data_df = all_stock_data_df[excel_output_cols]


# --- Prepare DataFrames for Excel Sheets ---

# 1. Forbes Companies Sheet - Filter based on companies that *made it* to all_stock_data
# Also select only the output columns requested by the user previously
output_cols_for_sheets = ["Name", "Ticker", "PE Ratio", "Price", "Sector"]

forbes_final_df = all_stock_data_df[
    all_stock_data_df['Original Company Name'].isin(forbes_companies_list)
][output_cols_for_sheets]

print(f"\nPrepared {len(forbes_final_df)} companies for Forbes sheet.")

# 2. Great Place To Work Companies Sheet - Filter based on companies that *made it* to all_stock_data
greatplaces_final_df = all_stock_data_df[
    all_stock_data_df['Original Company Name'].isin(greatplaces_companies_list)
][output_cols_for_sheets]
print(f"Prepared {len(greatplaces_final_df)} companies for Great Place To Work sheet.")

# 3. Companies on Both Lists Sheet - Filter based on companies that *made it* to all_stock_data
common_final_df = all_stock_data_df[
    all_stock_data_df['Original Company Name'].isin(common_companies)
][output_cols_for_sheets]
print(f"Prepared {len(common_final_df)} companies for Common Companies sheet.")


# --- Export to Excel ---
# Construct the full path for the Excel file
excel_file_name = os.path.join("C:\\Users\\grube\\Documents", "Employer_Stock_Analysis5.xlsx")
try:
    with pd.ExcelWriter(excel_file_name, engine='xlsxwriter') as writer:
        forbes_final_df.to_excel(writer, sheet_name='Forbes Employers', index=False)
        greatplaces_final_df.to_excel(writer, sheet_name='GPTW Employers', index=False)
        common_final_df.to_excel(writer, sheet_name='Common Employers', index=False)
    print(f"\nSuccessfully exported data to '{excel_file_name}' with three sheets.")
except Exception as e:
    print(f"Error exporting to Excel: {e}")


# Original strategy filtering (kept for demonstration, but not part of the Excel export request)
#if not all_stock_data_df.empty:
   #print("\n--- Investment Strategy Analysis (Optional) ---")
    #print("\nDEFENSIVE STRATEGY:")
    # Ensure columns exist before displaying strategy results, for safety
    #strategy_display_cols = ["Ticker", "Name", "Sector", "Price", "PE Ratio"]
    #for col in strategy_display_cols:
        #if col not in all_stock_data_df.columns:
            #all_stock_data_df[col] = None # Add missing columns

    #print(filter_by_strategy(all_stock_data_df, "defensive")[strategy_display_cols].head(5))

    #print("\nCYCLICAL STRATEGY:")
    #print(filter_by_strategy(all_stock_data_df, "cyclical")[strategy_display_cols].head(5))

    #print("\nGROWTH STRATEGY:")
    #print(filter_by_strategy(all_stock_data_df, "growth")[strategy_display_cols].head(5))
#else:
    #print("\nNo valid company data to apply strategies.")
