from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
from datetime import datetime

from google.cloud import storage
import io

##### Functions ###############

def upload_dataframe_to_gcs(bucket_name, df, destination_blob_name):
    """Uploads a Pandas DataFrame to Google Cloud Storage as a CSV."""

    try:
        # Convert DataFrame to CSV string in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')  # Important: Use UTF-8 encoding
        csv_content = csv_buffer.getvalue()

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(csv_content, content_type='text/csv')

        print(f"DataFrame uploaded to gs://{bucket_name}/{destination_blob_name}")
        return True # Return true if successfull
    except Exception as e:
        print(f"An error occurred: {e}")
        return False # Return false if there is an error

def extract_data_from_html(html_content):
    """Extracts court, class, and titles from an HTML file.

    Args:
        filepath: The path to the HTML file.

    Returns:
        A Pandas DataFrame or None if an error occurs.
    """


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

def split_html_by_date(html_content):
    """Splits HTML content into parts based on date headings.

    Args:
        html_content: The HTML content as a string.

    Returns:
        A dictionary where keys are dates and values are the corresponding HTML parts.
        Returns None if no dates are found.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    date_headings = soup.find_all('h3')

    if not date_headings:
        return None

    split_data = {}
    for i, heading in enumerate(date_headings):
        date_text = heading.text.strip()
        start_tag = heading
        end_tag = date_headings[i + 1] if i + 1 < len(date_headings) else None

        html_part = ""
        current_tag = start_tag
        while current_tag != end_tag:
            html_part += str(current_tag)
            current_tag = current_tag.next_sibling
            if current_tag is None:
                break

        split_data[date_text] = extract_data_from_html(html_part)

    return split_data

def convert_date_string(date_string):
    """Converts a date string like "Monday, 23th&nbsp;Dec&nbsp;2024" to "YYYY-MM-DD".

    Args:
        date_string: The date string to convert.

    Returns:
        The date in "YYYY-MM-DD" format as a string, or None if parsing fails.
    """
    date_string = date_string.replace(u'\xa0', " ") #This is the fix.
    match = re.match(r"(\w+), (\d+)(?:st|nd|rd|th) (\w+) (\d+)", date_string)
    if match:
        day_name = match.group(1)
        day = int(match.group(2))
        month_name = match.group(3)
        year = int(match.group(4))

        try:
          date_object = datetime.strptime(f"{day} {month_name} {year}", "%d %b %Y")
          return date_object.strftime("%Y-%m-%d")
        except ValueError:
          return None
    else:
        return None

##### Scrape the bookings ####

playwright = sync_playwright().start()
browser = playwright.chromium.launch()
page = browser.new_page()
page.goto('https://alphabadminton.yepbooking.com.au/')

try:
    close_button = page.query_selector(".ui-icon-closethick")
    close_button.click()
except:
    print("button already clicked")

page.get_by_text("Alpha Auburn (22 Courts)").click()

try:
    close_button = page.query_selector(".ui-icon-closethick")
    close_button.click()
except:
    print("button already clicked")
page.wait_for_timeout(2000)
page.select_option("#prehled", value="week")
page.wait_for_timeout(2000)

auburn_bookings = ""
auburn_bookings += page.content()
#with open('slough_week_0.html', "w+", encoding="utf-8") as f:
#    f.write(page.content())

for i in range(3):
    page.wait_for_timeout(2000)
    page.locator("#nextDateMover").nth(-1).click()
    page.wait_for_timeout(2000)
    week = i + 1
    #html_name = 'slough_week_' + str(week) + '.html'

    #with open(html_name, "w+", encoding="utf-8") as f:
    #    f.write(page.content())
    auburn_bookings += page.content()

browser.close()
playwright.stop()

######## Create dataframe ##############################

split_schemas = split_html_by_date(auburn_bookings)

dfs = []

for key, df in split_schemas.items():
    df['key'] = key
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

df_union_max = combined_df.groupby(['Class','key'])['Titles'].max().reset_index()
df_union_final = combined_df[['Court','Class','key']].drop_duplicates().merge(df_union_max, on=['Class','key'], how='left')
df_union_final['Class_Number'] = df_union_final['Class'].str.extract(r'_(\d+)').astype(int)
df_union_final['Location'] = np.where(df_union_final['Class_Number'] < 37, 'Alpha Slough',  np.where(df_union_final['Class_Number'] < 65, 'Alpha Egerton', 'Alpha Auburn'))
df_union_final = df_union_final[['Location','Court','Titles','key']]
df_union_final = df_union_final.explode('Titles')
df_union_final[['Time', 'Status']] = df_union_final['Titles'].str.split(' - ', n=1, expand=True)
df_union_final.drop(columns='Titles',axis=1,inplace=True)
df_union_final['Time'] = df_union_final['Time'].str.extract(r'(\d+:\d+[ap]m)')
df_union_final = df_union_final[df_union_final['Location'] == 'Alpha Auburn']
df_union_final = df_union_final[df_union_final['Court'] != ""]
df_union_final = df_union_final[df_union_final['Court'].notna()]
df_union_final = df_union_final[df_union_final['Status'].notna()]
df_union_final['Date'] = df_union_final['key'].apply(convert_date_string)
df_union_final.drop(columns='key',axis=1,inplace=True)
df_union_final['DateTime'] = df_union_final['Date'] + " " + df_union_final['Time']


min_date = df_union_final['Date'].min()
max_date = df_union_final['Date'].max()

daily_range = pd.date_range(start=min_date, end=max_date, freq='D')

times = [
    "9:00am", "10:00am", "11:00am", "12:00pm",
    "1:00pm", "2:00pm", "3:00pm", "4:00pm", "5:00pm", "6:00pm",
    "7:00pm", "8:00pm", "9:00pm", "10:00pm"
]

courts = [
    "Court 1", "Court 2", "Court 3", "Court 4", "Court 5", "Court 6",
    "Court 7", "Court 8", "Court 9", "Court 10", "Court 11", "Court 12",
    "Court 13", "Court 14", "Court 15", "Court 16", "Court 17", "Court 18",
    "Court 19", "Court 20", "Court 21", "Court 22"
]

date_time_list = []
date_list = []
time_list = []
courts_list = []

for date in daily_range:
    for time_str in times:
        for court in courts:
            # Combine date and time
            date_time_str = str(date.date()) + " " + time_str
            date_time_list.append(date_time_str)
            date_list.append(date.date())
            time_list.append(time_str)
            courts_list.append(court)

date_time_df = pd.DataFrame({'Date': date_list, 'Time': time_list, 'DateTime': date_time_list, 'Court':courts_list})
date_time_df['Location'] = 'Alpha Auburn'


df_union_final = date_time_df.merge(df_union_final, on=['DateTime','Court','Location'], how='left')
df_union_final['Status'] = np.where(df_union_final['Status'].isnull(), 'available', df_union_final['Status'])
df_union_final['Status'] = np.where(df_union_final['Status'] != 'available' , 'unavailable', df_union_final['Status'])
df_union_final = df_union_final.rename(columns={'Date_x': 'Date', 'Time_x': 'Time'})
df_union_final = df_union_final[['Location','Court','Date','Time','Status']]

#df_union_final.to_csv('alpha_auburn_bookings.csv', index=False)
#print(df_union_final)


bucket_name = "badminton-bookings"
blob_name = "alpha_auburn_bookings.csv" 

if upload_dataframe_to_gcs(bucket_name, df_union_final, blob_name):
    print("Upload successful")
else:
    print("Upload failed")