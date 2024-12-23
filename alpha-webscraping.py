#from playwright.sync_api import sync_playwright

#with sync_playwright() as p:
#	for browser_type in [p.chromium]:
#		browser = browser_type.launch()
#		page = browser.new_page()
#		page.goto('https://alphabadminton.yepbooking.com.au/')
#		close_button = page.query_selector(".ui-button-text-only")
#		close_button.click()
#		page.screenshot(path=f'example1-{browser_type.name}.png')
#		browser.close()
        
        
        
from playwright.sync_api import sync_playwright

playwright = sync_playwright().start()
# Use playwright.chromium, playwright.firefox or playwright.webkit
# Pass headless=False to launch() to see the browser UI
browser = playwright.chromium.launch()
page = browser.new_page()
page.goto('https://alphabadminton.yepbooking.com.au/')

try:
    close_button = page.query_selector(".ui-button-text-only")
    close_button.click()
except:
    print("button already clicked")

page.get_by_text("Alpha Slough (13 Courts)").click()

try:
    close_button = page.query_selector(".ui-button-text-only")
    close_button.click()
except:
    print("button already clicked")

with open('slough.html', "w+", encoding="utf-8") as f:
    f.write(page.content())

browser.close()
playwright.stop()

playwright = sync_playwright().start()
# Use playwright.chromium, playwright.firefox or playwright.webkit
# Pass headless=False to launch() to see the browser UI
browser = playwright.chromium.launch()
page = browser.new_page()
page.goto('https://alphabadminton.yepbooking.com.au/')

try:
    close_button = page.query_selector(".ui-button-text-only")
    close_button.click()
except:
    print("button already clicked")

page.get_by_text("Alpha Auburn (22 Courts)").click()

try:
    close_button = page.query_selector(".ui-button-text-only")
    close_button.click()
except:
    print("button already clicked")

with open('auburn.html', "w+", encoding="utf-8") as f:
    f.write(page.content())

browser.close()
playwright.stop()



from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np

def extract_data_from_html_file(filepath):
    """Extracts court, class, and titles from an HTML file.

    Args:
        filepath: The path to the HTML file.

    Returns:
        A Pandas DataFrame or None if an error occurs.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:  # Handle potential encoding issues
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except Exception as e:
        print(f"An error occurred reading the file: {e}")
        return None

    # Use regex to split HTML content by trSchemaLane tags
    tr_tags = re.findall(r"(<tr class=\"trSchemaLane.*?</tr>)", html_content, re.DOTALL)

    combined_html = {}
    data = []
    for tr_html in tr_tags:
        soup_tr = BeautifulSoup(tr_html, "html.parser")
        tr_tag = soup_tr.find('tr', class_=lambda x: x and x.startswith('trSchemaLane'))
        if tr_tag:
            court_span = tr_tag.find('span')
            court_number = court_span.text if court_span else None
            class_name = tr_tag.get('class')[0] if tr_tag and tr_tag.get('class') else None
            title_texts = [td.get('title') for td in tr_tag.find_all('td', {'class': 'tooltip'}) if td.get('title')] #finds all titles within current tr
            data.append({"Court": court_number, "Class": class_name, "Titles": title_texts})


    #for html_string in combined_html.values():
    ##    soup = BeautifulSoup(html_string, 'html.parser')
    #    tr_tag = soup.find('tr', class_=lambda x: x and x.startswith('trSchemaLane'))
    #    if tr_tag:
    #        court_span = tr_tag.find('span')
    #        court_number = court_span.text if court_span else None
    #        title_texts = [td.get('title') for td in tr_tag.find_all('td', {'class': 'tooltip'}) if td.get('title')]
    #        data.append({"Court": court_number, "Class": class_name, "Titles": title_texts})

    df = pd.DataFrame(data)
    return df


# Example usage:
filepath = 'slough.html'  # Replace with the actual path to your HTML file
df = extract_data_from_html_file(filepath)

filepath = 'auburn.html'
df1 = extract_data_from_html_file(filepath)

if df is not None:
    # Use pandas to combine rows based on Class
    df['Court'] = df['Court'].ffill()

    def combine_titles(group):
        all_titles = []
        for titles in group['Titles']:
            if titles is not None:
                all_titles.extend(titles)
        return pd.Series({'Titles': all_titles, 'Court': group['Court'].iloc[0], 'Class': group['Class'].iloc[0]})

    df = df.groupby('Class').apply(combine_titles).reset_index(drop=True)
    df = df[['Court','Class','Titles']]

if df1 is not None:
    # Use pandas to combine rows based on Class
    df1['Court'] = df1['Court'].ffill()

    def combine_titles(group):
        all_titles = []
        for titles in group['Titles']:
            if titles is not None:
                all_titles.extend(titles)
        return pd.Series({'Titles': all_titles, 'Court': group['Court'].iloc[0], 'Class': group['Class'].iloc[0]})

    df1 = df1.groupby('Class').apply(combine_titles).reset_index(drop=True)
    df1 = df1[['Court','Class','Titles']]

df_union = pd.concat([df, df1]).reset_index(drop=True)
df_union_max = df_union.groupby('Class')['Titles'].max().reset_index()
df_union_final = df_union[['Court','Class']].drop_duplicates().merge(df_union_max, on='Class', how='left')
df_union_final['Class_Number'] = df_union_final['Class'].str.extract(r'_(\d+)').astype(int)
df_union_final['Location'] = np.where(df_union_final['Class_Number'] < 37, 'Alpha Slough',  np.where(df_union_final['Class_Number'] < 65, 'Alpha Egerton', 'Alpha Auburn'))
df_union_final = df_union_final[['Location','Court','Titles']]
df_union_final = df_union_final.explode('Titles')
df_union_final[['Time', 'Status']] = df_union_final['Titles'].str.split(' - ', n=1, expand=True)
df_union_final.drop(columns='Titles',axis=1,inplace=True)
df_union_final['Time'] = df_union_final['Time'].str.extract(r'(\d+:\d+[ap]m)')

df_union_final.to_csv('alpha_bookings.csv', index=False)

