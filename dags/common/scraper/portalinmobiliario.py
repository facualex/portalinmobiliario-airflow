"""
@author: Facundo Alexandre B.
@date: 23-08-2023
@description:  
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from utilities import generate_apartments_urls_to_scrape
import pandas as pd
from datetime import datetime
import re

def scrape_apartments(options):
    """
    Scrapes apartment data from a website using Selenium and saves it to a CSV file.

    Parameters:
        options (dict): A dictionary containing options for the scraping process.
            verbose (bool): If True, prints additional information during the scraping process.
            headless (bool): If True, runs the Chrome browser in headless mode.

    Returns:
        None
    """

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-popup-blocking")
    is_verbose = options['verbose']
    
    if (options['headless']):
        chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()

    dataframe_data = []

    apartment_urls_to_scrape = generate_apartments_urls_to_scrape(driver = driver, verbose = is_verbose)

    if is_verbose:
        print(f'APARTMENT URLs: {apartment_urls_to_scrape}')

    # Main scraping loop
    for index, apartment_url in enumerate(apartment_urls_to_scrape):
        # Open the URL
        driver.get(apartment_url)

        print(f'Extrayendo datos del departamento {index}/{len(apartment_urls_to_scrape)}{(": " + apartment_url) if is_verbose else "."}')

        # Sleep 2 seconds for every new apartment page to scrape
        sleep(2)

        # Get 'price'
        try:
            price = driver.find_element(By.CSS_SELECTOR, 'span[class="andes-money-amount__fraction"]')
            price = int(price.text.replace(".", ""))
        except Exception as e:
            price = -1

        # Get 'common_expenses'
        try:
            common_expenses = driver.find_element(By.CSS_SELECTOR,
                                                  'p[class="ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-maintenance-fee-ltr"]')

            common_expenses = int(common_expenses.text.split("$ ")[1].replace(".", ""))
        except Exception as e:
            common_expenses = -1

        # Get 'location_1', 'location_2', 'location_3' and 'location_4'
        # Ejemplo: Hipódromo Chile 1876, Plaza Chacabuco, Independencia
        location_string_element = driver.find_element(By.XPATH,
                                                      "//div[@class='ui-pdp-media ui-vip-location__subtitle ui-pdp-color--BLACK']/div[@class='ui-pdp-media__body']/p")
        location_string = location_string_element.text
        location_string_list = location_string.split(',')

        try:
            location_1 = location_string_list[0].strip()
        except Exception as e:
            location_1 = ''
                
        try:
            location_2 = location_string_list[1].strip()
        except Exception as e:
            location_2 = ''
               
        try:
            location_3 = location_string_list[2].strip()
        except Exception as e:
            location_3 = ''

        try:
            location_4 = location_string_list[3].strip()
        except Exception as e:
            location_4 = ''

        try:
            surface_and_rooms = driver.find_elements(By.CSS_SELECTOR,
                                                  'div[class="ui-pdp-highlighted-specs-res__icon-label"]')

            try:
                surface = surface_and_rooms[0].text
            except Exception as e:
                surface = -1
                
            try:
                rooms = surface_and_rooms[1].text
            except Exception as e:
                rooms = -1

            try:
                bathrooms = surface_and_rooms[2].text
            except Exception as e:
                bathrooms = -1
        except Exception as e:
            surface = -1
            rooms = -1
            bathrooms = -1

        row_data  = {
            "fecha_scrape": datetime.now().strftime('%d-%m-%Y'),
            "location_1": location_1,
            "location_2": location_2,
            "location_3": location_3,
            "location_4": location_4,
            "precio": price,
            "gastos_comunes": common_expenses,
            "superficie": surface,
            "dormitorios": rooms,
            "baños": bathrooms
        }

        if is_verbose:
            print(row_data)

        dataframe_data.append(row_data)

    apartments_df = pd.DataFrame(dataframe_data)

    print("Escribiendo los datos en el archivo 'apartments.csv'...")
    apartments_df.to_csv('./data/apartments.csv', index = False)

    driver.quit()

scrape_apartments(options = {
    "headless": True,
    "verbose": True,
})