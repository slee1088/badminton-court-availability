from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
from datetime import datetime

##### Functions ###############

def format_time(time_str):
    try:
        hour = int(time_str[:2])
        minute = int(time_str[2:])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            period = "am" if hour < 12 or hour == 24 else "pm"
            if hour == 0:
                display_hour = 12
            elif hour > 12:
                display_hour = hour - 12
            else:
                display_hour = hour

            return f"{display_hour}:{minute:02}{period}"
        else:
            return "Invalid Time"  # Handle invalid time strings
    except (ValueError, IndexError):
        return "Invalid Time"  # Handle cases where conversion fails


def create_df_from_html(html_content): 

    soup = BeautifulSoup(roketto_bookings, 'html.parser')

    data = []

    # Extract location
    location_element = soup.find('h2')
    location = location_element.text.strip() if location_element else None
    #print(location)

    # Extract date
    date_element = soup.find('span', id='date_heading')
    date_text = date_element.text.strip() if date_element else None
    #print(date_text)

    for tr in soup.find_all('tr'):
        court_td = tr.find('td') #Gets the court td
        if court_td: #Checks if the td exists
            court_name = court_td.text.strip() #Gets the court name
            for td in tr.find_all('td')[1:]: #Gets all the time slots, starting from the second td
                id_value = td.get('id')
                class_value = td.get('class')
                if id_value: #Check if ID exists
                    data.append({'Court': court_name, 'ID': id_value, 'Class': class_value})

    df = pd.DataFrame(data)
    df['Location'] = location
    df['Date'] = date_text
    df['Time'] = df['ID'].str[-4:].apply(format_time)
    df['Class'] = df['Class'].str[0] 
    df = df.rename(columns={'Class': 'Status'})
    df = df[['Location','Court','Date','Time','Status']]

    df['Date'] = pd.to_datetime(df['Date'].str[:10], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    #print(df)
    return df

##### Scrape the bookings ####

bookings_final = pd.DataFrame(columns=['Location','Court','Date','Time','Status'])

playwright = sync_playwright().start()
browser = playwright.chromium.launch()
page = browser.new_page()
page.goto('https://roketto.sportlogic.net.au/secure/customer/booking/v1/public/show')
page.wait_for_timeout(2000)


roketto_bookings = page.content()

booking = create_df_from_html(roketto_bookings)

bookings_final = pd.concat([bookings_final, booking], ignore_index=True)

for i in range(30):
    page.wait_for_timeout(2000)
    page.get_by_text("Next ").nth(0).click()
    page.wait_for_timeout(2000)
    roketto_bookings = page.content()
    booking = create_df_from_html(roketto_bookings)
    bookings_final = pd.concat([bookings_final, booking], ignore_index=True)

browser.close()
playwright.stop()


bookings_final.to_csv('roketto_bookings.csv', index=False)