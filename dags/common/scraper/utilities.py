"""
    @description: Utility functions used in the main scraping script.
"""

from selenium.webdriver.common.by import By
from time import sleep

"""
    @name: get_base_url
    @description: 
"""
def get_base_url(load_more = 1):
    return f'https://www.portalinmobiliario.com/arriendo/departamento/metropolitana/_Desde_51_NoIndex_True'

"""
    @name: generate_urls_to_scrape
    @description: 
"""
def generate_urls_to_scrape():
    urls_to_scrape = []
    NUMBER_OF_ELEMENTS_PER_PAGE = 50

    # FIXME: Get total pages dinamically
    NUMBER_OF_PAGES_TO_SCRAPE = 40 

    for page_number in range (1, NUMBER_OF_PAGES_TO_SCRAPE):
        desde = (page_number * NUMBER_OF_ELEMENTS_PER_PAGE) + 1
        urls_to_scrape.append(f'https://www.portalinmobiliario.com/arriendo/departamento/metropolitana/_Desde_{desde}_NoIndex_True')
    
    return urls_to_scrape

def get_total_pages(driver):
    try:
        TOTAL_PAGES = driver.find_element(By.CSS_SELECTOR, 'li[class="andes-pagination__page-count"]')
        return int(TOTAL_PAGES.text.split(" ")[1])
    except Exception as e:
        print("Failed to get total amount of pages to scrape!")
        TOTAL_PAGES = 0 
