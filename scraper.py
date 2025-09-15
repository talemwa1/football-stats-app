import requests from bs4 import BeautifulSoup
import pandas as pd
import time
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import re

# Configure Selenium WebDriver
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# Initialize the WebDriver
driver = webdriver.Chrome(options=chrome_options)

def scrape_fbref_stats(league_url, season):
    """
    Scrape player statistics from FBref for a given league and season
    """
    print(f"Scraping FBref data for {season} season from: {league_url}")
    
    # Get the base URL for the league
    base_url = league_url.rsplit('/', 1)[0]
    
    # Define the tables we want to scrape
    tables = {
        "standard": "Standard Stats",
        "shooting": "Shooting",
        "passing": "Passing",
        "gca": "Goal and Shot Creation",
        "defense": "Defensive Actions",
        "possession": "Possession",
        "misc": "Miscellaneous Stats"
    }
    
    # Dictionary to store all dataframes
    dfs = {}
    
    # Scrape each table
    for table_id, table_name in tables.items():
        print(f"Scraping {table_name} table...")
        
        # Construct the URL for this table
        if table_id == "standard":
            table_url = f"{base_url}/stats/{season}-Stats"
        else:
            table_url = f"{base_url}/{table_id}/{season}-Stats"
        
        try:
            # Use pandas to read the HTML table
            df_list = pd.read_html(table_url)
            if not df_list:
                print(f"No table found for {table_name}")
                continue
                
            df = df_list[0]
            
            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten the multi-index
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Remove non-player rows (squad totals, etc.)
            if 'Rk' in df.columns:
                df = df[df['Rk'].apply(lambda x: str(x).isdigit())]
                df['Rk'] = df['Rk'].astype(int)
            
            # Store the dataframe
            dfs[table_id] = df
            
            # Be respectful - add a delay
            time.sleep(3)
            
        except Exception as e:
            print(f"Error scraping {table_name} table: {e}")
            continue
    
    return dfs

def scrape_whoscored_stats(league_url):
    """
    Scrape player statistics from WhoScored
    """
    print(f"Scraping WhoScored data from: {league_url}")
    driver.get(league_url)
    
    # Wait for the page to load
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "statistics-table-summary"))
        )
    except TimeoutException:
        print("Timed out waiting for WhoScored page to load")
        return None
    
    # Get all available tabs
    tabs = driver.find_elements(By.CSS_SELECTOR, "#stage-statistics-tabs li")
    tab_names = [tab.text.strip() for tab in tabs]
    
    all_data = {}
    
    # Scrape each tab
    for tab in tabs:
        tab_name = tab.text.strip()
        print(f"Scraping {tab_name} tab...")
        tab.click()
        time.sleep(2)  # Wait for tab content to load
        
        # Get table ID based on tab name
        table_id = f"statistics-table-{tab_name.lower().replace(' ', '-')}"
        
        # Wait for table to load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, table_id))
            )
        except TimeoutException:
            print(f"Timed out waiting for {tab_name} table to load")
            continue
        
        # Get table data
        table = driver.find_element(By.ID, table_id)
        headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
        
        # Get all rows
        rows = []
        for tr in table.find_elements(By.TAG_NAME, "tr")[1:]:  # Skip header row
            cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
            if cells:
                rows.append(cells)
        
        # Store data
        all_data[tab_name] = {
            "headers": headers,
            "rows": rows
        }
    
    return all_data

def scrape_understat_stats(league_url):
    """
    Scrape xG and xA data from Understat
    """
    print(f"Scraping Understat data from: {league_url}")
    response = requests.get(league_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the players table
    table = soup.find('table', {'class': 'players'})
    if not table:
        print("Could not find players table on Understat")
        return None
    
    # Extract headers
    headers = []
    header_row = table.find('tr')
    for th in header_row.find_all('th'):
        headers.append(th.text.strip())
    
    # Extract player data
    players_data = []
    for row in table.find_all('tr')[1:]:  # Skip header row
        cells = row.find_all('td')
        if len(cells) < 2:
            continue
            
        player_data = {}
        player_name = cells[0].text.strip()
        team_name = cells[1].text.strip()
        
        player_data['Player'] = player_name
        player_data['Team'] = team_name
        player_data['Games'] = cells[2].text.strip()
        player_data['Goals'] = cells[3].text.strip()
        player_data['xG'] = cells[4].text.strip()
        player_data['Assists'] = cells[5].text.strip()
        player_data['xA'] = cells[6].text.strip()
        player_data['Shots'] = cells[7].text.strip()
        player_data['Key Passes'] = cells[8].text.strip()
        player_data['Yellow Cards'] = cells[9].text.strip()
        player_data['Red Cards'] = cells[10].text.strip()
        
        players_data.append(player_data)
    
    return pd.DataFrame(players_data)

def merge_data_sources(fbref_dfs, whoscored_data, understat_df):
    """
    Merge data from all sources into a single dataframe
    """
    # Start with FBref standard stats as the base
    if 'standard' not in fbref_dfs:
        print("No FBref standard data available")
        return None
        
    merged_df = fbref_dfs['standard'].copy()
    
    # Merge other FBref tables
    for table_name, df in fbref_dfs.items():
        if table_name == 'standard':
            continue
            
        # Identify common columns (excluding Player and Squad)
        common_cols = set(merged_df.columns) & set(df.columns)
        common_cols = [col for col in common_cols if col not in ['Player', 'Squad']]
        
        # Drop common columns from the df we're about to merge
        if common_cols:
            df = df.drop(columns=common_cols)
        
        # Merge on Player and Squad
        merged_df = pd.merge(merged_df, df, on=['Player', 'Squad'], how='outer')
    
    # Process WhoScored data
    if whoscored_data and 'Summary' in whoscored_data:
        # Convert WhoScored data to DataFrame
        summary_data = whoscored_data['Summary']
        headers = summary_data['headers']
        rows = summary_data['rows']
        
        whoscored_df = pd.DataFrame(rows, columns=headers)
        
        # Rename columns to match FBref format
        if 'Player' in whoscored_df.columns and 'Team' in whoscored_df.columns:
            whoscored_df = whoscored_df.rename(columns={'Team': 'Squad'})
            
            # Merge with main dataframe
            common_cols = set(merged_df.columns) & set(whoscored_df.columns)
            common_cols = [col for col in common_cols if col not in ['Player', 'Squad']]
            
            if common_cols:
                whoscored_df = whoscored_df.drop(columns=common_cols)
                
            merged_df = pd.merge(merged_df, whoscored_df, on=['Player', 'Squad'], how='outer')
    
    # Merge Understat data
    if understat_df is not None and not understat_df.empty:
        # Rename columns to match
        understat_df = understat_df.rename(columns={'Team': 'Squad'})
        
        # Merge with main dataframe
        common_cols = set(merged_df.columns) & set(understat_df.columns)
        common_cols = [col for col in common_cols if col not in ['Player', 'Squad']]
        
        if common_cols:
            understat_df = understat_df.drop(columns=common_cols)
            
        merged_df = pd.merge(merged_df, understat_df, on=['Player', 'Squad'], how='outer')
    
    return merged_df

def calculate_derived_metrics(df):
    """
    Calculate derived metrics from the base statistics
    """
    # Calculate shooting conversion rate
    if 'Goals' in df.columns and 'Sh' in df.columns:
        try:
            df['Shooting_Conversion_Rate'] = df.apply(
                lambda row: f"{(float(row['Goals']) / float(row['Sh']) * 100):.2f}%" 
                if float(row['Sh']) > 0 else "0.00%", 
                axis=1
            )
        except (ValueError, ZeroDivisionError):
            df['Shooting_Conversion_Rate'] = "0.00%"
    
    # Calculate pass completion rate
    if 'Cmp' in df.columns and 'Att' in df.columns:
        try:
            df['Pass_Completion_Rate'] = df.apply(
                lambda row: f"{(float(row['Cmp']) / float(row['Att']) * 100):.2f}%" 
                if float(row['Att']) > 0 else "0.00%", 
                axis=1
            )
        except (ValueError, ZeroDivisionError):
            df['Pass_Completion_Rate'] = "0.00%"
    
    # Calculate xG overperformance
    if 'Goals' in df.columns and 'xG' in df.columns:
        try:
            df['xG_Overperformance'] = df.apply(
                lambda row: f"{float(row['Goals']) - float(row['xG']):.2f}" 
                if pd.notna(row['xG']) else "N/A", 
                axis=1
            )
        except (ValueError, TypeError):
            df['xG_Overperformance'] = "N/A"
    
    # Calculate xA overperformance
    if 'Assists' in df.columns and 'xA' in df.columns:
        try:
            df['xA_Overperformance'] = df.apply(
                lambda row: f"{float(row['Assists']) - float(row['xA']):.2f}" 
                if pd.notna(row['xA']) else "N/A", 
                axis=1
            )
        except (ValueError, TypeError):
            df['xA_Overperformance'] = "N/A"
    
    # Calculate dribble completion rate
    if 'Dribbles Succ' in df.columns and 'Dribbles Att' in df.columns:
        try:
            df['Dribble_Completion_Rate'] = df.apply(
                lambda row: f"{(float(row['Dribbles Succ']) / float(row['Dribbles Att']) * 100):.2f}%" 
                if float(row['Dribbles Att']) > 0 else "0.00%", 
                axis=1
            )
        except (ValueError, ZeroDivisionError):
            df['Dribble_Completion_Rate'] = "0.00%"
    
    # Calculate aerial duel win percentage
    if 'Aerial Duels Won' in df.columns and 'Aerial Duels Lost' in df.columns:
        try:
            df['Aerial_Duels_Win_Percentage'] = df.apply(
                lambda row: f"{(float(row['Aerial Duels Won']) / (float(row['Aerial Duels Won']) + float(row['Aerial Duels Lost'])) * 100):.2f}%" 
                if (float(row['Aerial Duels Won']) + float(row['Aerial Duels Lost'])) > 0 else "0.00%", 
                axis=1
            )
        except (ValueError, ZeroDivisionError):
            df['Aerial_Duels_Win_Percentage'] = "0.00%"
    
    # Add placeholders for metrics not available from these sources
    df['Top_Speed'] = "N/A"
    df['Distance_Covered'] = "N/A"
    df['Big_Chances_Created'] = "N/A"
    df['Big_Chances_Missed'] = "N/A"
    df['Usage_Rate'] = "N/A"
    df['Ground_Duels_Won'] = "N/A"
    df['Passes_Leading_to_Shots'] = "N/A"
    
    return df

def save_to_spreadsheet(df, filename="footballers_stats.csv"):
    """
    Save player data to a CSV spreadsheet
    """
    # Define the desired column order
    desired_columns = [
        "Player", "Squad", "Age", "Pos", "MP", "Min", "90s",
        "Goals", "Assists", "G+A", "G-PK", "PK", "PKatt", 
        "xG", "npxG", "xAG", "xG_Overperformance", "xA_Overperformance",
        "Shooting_Conversion_Rate", "Sh", "SoT", "SoT%", "G/Sh", "G/SoT",
        "Pass_Completion_Rate", "Cmp", "Att", "Cmp%", "TotDist", "PrgDist",
        "Key_Passes", "Passes_Leading_to_Shots", "xA", "A-xA",
        "SCA", "GCA", "Big_Chances_Created", "Big_Chances_Missed",
        "Tkl", "TklW", "Blocks", "Int", "Clr", "Err",
        "Dribble_Completion_Rate", "Dribbles Succ", "Dribbles Att",
        "Aerial_Duels_Won", "Aerial_Duels_Win_Percentage", "Ground_Duels_Won",
        "Top_Speed", "Distance_Covered", "Usage_Rate",
        "CrdY", "CrdR", "Fls", "Fld", "Off", "Crs", "TklW", "Int", "Blocks",
        "Rating"
    ]
    
    # Filter to only include columns that exist in our DataFrame
    available_columns = [col for col in desired_columns if col in df.columns]
    df = df[available_columns]
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    return df

def main():
    # URLs for Premier League 2022-2023 season
    premier_league_fbref_url = "https://fbref.com/en/comps/9/Premier-League-Stats "
    premier_league_whoscored_url = "https://www.whoscored.com/Regions/252/Tournaments/2/Seasons/9019/Stages/21135/PlayerStatistics/England-Premier-League-2022-2023 "
    premier_league_understat_url = "https://understat.com/league/EPL "
    
    # Scrape data from all sources
    fbref_dfs = scrape_fbref_stats(premier_league_fbref_url, "Premier-League")
    whoscored_data = scrape_whoscored_stats(premier_league_whoscored_url)
    understat_df = scrape_understat_stats(premier_league_understat_url)
    
    # Merge data from all sources
    merged_df = merge_data_sources(fbref_dfs, whoscored_data, understat_df)
    
    if merged_df is None:
        print("Failed to merge data sources")
        return
    
    # Calculate derived metrics
    merged_df = calculate_derived_metrics(merged_df)
    
    # Save to spreadsheet
    df = save_to_spreadsheet(merged_df)
    
    # Print summary
    print(f"Successfully scraped data for {len(df)} players")
    print(f"Data includes {len(df.columns)} different metrics")
    
    # Close the WebDriver
    driver.quit()

if __name__ == "__main__":
    main()
