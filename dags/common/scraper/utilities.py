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
def generate_pages_urls_to_scrape(verbose = False):
    urls_to_scrape = []

    NUMBER_OF_ELEMENTS_PER_PAGE = 50

    # FIXME: Get total pages dinamically
    NUMBER_OF_PAGES_TO_SCRAPE = 40 

    for page_number in range (1, NUMBER_OF_PAGES_TO_SCRAPE):

        desde = (page_number * NUMBER_OF_ELEMENTS_PER_PAGE) + 1
        urls_to_scrape.append(f'https://www.portalinmobiliario.com/arriendo/departamento/metropolitana/_Desde_{desde}_NoIndex_True')
    
    return urls_to_scrape

def generate_apartments_urls_to_scrape(driver, verbose = False):
    """
    Generates a list of URLs for scraping apartment information.

    Parameters:
    - driver (WebDriver): The Selenium WebDriver object.
    - verbose (bool, optional): If True, additional information will be printed. Defaults to False.

    Returns:
    - List[str]: A list of URLs for scraping apartment information.
    """

    print("Obteniendo URLs de cada departamento...")

    page_urls = generate_pages_urls_to_scrape(verbose)
    apartment_urls = []

    for page_url in page_urls:
        driver.get(page_url)

        if (verbose):
            print(f'Obteniendo URLs de deptos de la página: {page_url}')

        sleep(3) # Sleep 3 seconds for each page

        # Links a depto: Array: <a> => class: ui-search-result__image ui-search-link
        apartments_links_in_current_page = driver.find_elements(By.CSS_SELECTOR,'a[class="ui-search-result__image ui-search-link"]')
        
        for apartment_link in apartments_links_in_current_page:
            apartment_urls.append(apartment_link.get_attribute('href')) 
    
    if len(apartment_urls) == 0:
        print("Error al obtener URLs de los departamentos")

    print("URLs de departamentos obtenidos con éxito!")

    return apartment_urls

# TODO: Get total pages dinamically
def get_total_pages(driver):
    try:
        TOTAL_PAGES = driver.find_element(By.CSS_SELECTOR, 'li[class="andes-pagination__page-count"]')
        return int(TOTAL_PAGES.text.split(" ")[1])
    except Exception as e:
        print("Failed to get total amount of pages to scrape!")
        TOTAL_PAGES = 0 
