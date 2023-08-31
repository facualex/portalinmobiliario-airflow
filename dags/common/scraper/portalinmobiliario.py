"""
@author: Facundo Alexandre B.
@date: 23-08-2023
@description:  
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from utilities import get_base_url, get_total_pages, generate_urls_to_scrape 
import pandas as pd
from datetime import datetime
import re

# TODO: Add logging (success page scrape, errors (page, element))
"""
    @name: scrape_apartments
    @description: A
"""
def scrape_apartments(options):

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-popup-blocking")

    if (options['headless']):
        chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(options=chrome_options)

    driver.maximize_window()

    dataframe_data = []

    sleep(5)

    urls_to_scrape = generate_urls_to_scrape()

    for url in urls_to_scrape:
        # Main scraping loop

        # Sleep 5 seconds for every new page to scrape
        sleep(5)

        # Open the URL
        driver.get(url)

        elementos_li = driver.find_elements(By.CSS_SELECTOR,'li[class="ui-search-layout__item shops__layout-item"]')

        for elemento in elementos_li:
            try:
                precio = elemento.find_element(By.CSS_SELECTOR, 'span[class="andes-money-amount__fraction"]')
                precio = precio.text.replace(".", "")
            except Exception as e:
                precio = -1
                continue

            try:
                # dirección, zona, comuna
                location_string_element = elemento.find_element(By.CSS_SELECTOR, 'p[class="ui-search-item__group__element ui-search-item__location shops__items-group-details"]')

                # Hipódromo Chile 1876, Plaza Chacabuco, Independencia
                location_string = location_string_element.text
                location_string_list = location_string.split(',')

                try:
                    direccion = location_string_list[0].strip()
                except Exception as e:
                    direccion = ''
                    continue
                
                try:
                    zona = location_string_list[1].strip()
                except Exception as e:
                    zona = ''
                    continue
               
                try:
                    comuna = location_string_list[2].strip()
                except Exception as e:
                    comuna = ''
                    continue

            except Exception as e:
                direccion = ''
                zona = ''
                comuna = ''
                continue

            try:
                surface_and_rooms = elemento.find_elements(By.CSS_SELECTOR, 'li[class="ui-search-card-attributes__attribute"]')

                try:
                    surface_element = surface_and_rooms[0] 
                    match_surface = re.search(r'\d+', surface_element.text)
                except Exception as e:
                    superficie = -1
                    continue
                
                try:
                    rooms_element = surface_and_rooms[1]
                    match_rooms = re.search(r'\d+', rooms_element.text)
                except Exception as e:
                    nro_dormitorios = -1
                    continue

                if match_surface:
                    superficie = int(match_surface.group())
                else:
                    superficie = -1
                    continue
                
                if match_rooms:
                    nro_dormitorios = int(match_rooms.group())
                else:
                    nro_dormitorios = -1
                    continue
            except Exception as e:
                superficie = -1
                nro_dormitorios = -1
                continue

            row_data  = {
                "fecha_scrape": datetime.now().strftime('%m-%d-%Y'),
                "direccion": direccion,
                "zona": zona,
                "comuna": comuna,
                "precio": precio,
                "dormitorios": nro_dormitorios,
                "superficie": superficie
            }

            if options['verbose']:
                print(row_data)

            dataframe_data.append(row_data)

    apartments_df = pd.DataFrame(dataframe_data)
    apartments_df.to_csv('./data/apartments.csv', index = False)

    driver.quit()

scrape_apartments(options = {
    "headless": False,
    "verbose": False,
})