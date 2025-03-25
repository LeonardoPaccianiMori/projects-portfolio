'''
Filename: datalake_population_webscraping.py
Author: Leonardo Pacciani-Mori

Description:
    This script scrapes the website https://www.immobiliare.it to get raw data on
    listings for houses on RENT, SALE, and AUCTION, and uses it to populate a MongoDB datalake 
    hosted locally on the same machine where this script is executed.
    This script uses Apache Airflow to divide the task of scraping of the whole
    website into smaller, independent tasks that can be executed in parallel.

Disclaimer:
    This script has been edited to avoid out-of-the-box reproducibility.
    Thos code is shared only as a demonstration of my programming skills,
    problem-solving approach, and technical implementation abilities.
    This code is not being shared for the purpose of enabling others to
    scrape websites, but rather to showcase my understanding of data collection
    and handling techniques as part of my professional portfolio.

License:
    This code is released under the GNU General Public License v3.
    See https://www.gnu.org/licenses/gpl-3.0.en.html for details.
'''

# -------------------------------------------------------------------------------
# --------------LIBRARY IMPORTS AND GLOBAL VARIABLES DEFINITION------------------
# -------------------------------------------------------------------------------

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import numpy as np
import asyncio
import aiohttp
import requests
import time
from bs4 import BeautifulSoup
import json
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

import pymongo
# Opens mongoDB connection to the datalake and defines all collections
mongoDB_client = pymongo.MongoClient("127.0.0.1", 27017)
datalake = mongoDB_client.immobiliare_datalake
sale_coll = datalake.sale
rent_coll = datalake.rent
auction_coll = datalake.auction

import logging
# Sets the logging level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

todays_date = datetime.today().strftime('%Y-%m-%d')

import pandas as pd
# Imports data on all of Italy's provinces (e.g., their names and their two-letter codes), stored in a csv file
prov_df = pd.read_csv("/home/lpm/airflow/dags/provinces.csv", sep=",", na_filter=False)

# Creates two dictionaries, one to convert a province's name into its two-letter
# code, and another to get the name of the region within which a province resides
prov_to_code = dict(zip(prov_df["Province"], prov_df["Code"]))
prov_to_region = dict(zip(prov_df["Province"], prov_df["Region"]))

# Sets a limit of 50 concurrent HTTP requests
semaphore = asyncio.Semaphore(50)

# -------------------------------------------------------------------------------
# ---------------------------FUNCTION DEFINITIONS--------------------------------
# -------------------------------------------------------------------------------

async def get_single_url(url, session):
    '''
    This is an asynchronous function that makes a single HTTP request. It is
    used by the get_multiple_urls function to make several asynchronous requests.
    
    Parameters:
    - url: str, the URL to fetch
    - session: aiohttp.ClientSession object
    
    Returns:
    - str: the HTML content of the page
    '''
    
    # Uses the semaphore object defined above to limit concurrent requests
    async with semaphore:
        # Sets a timeout (in seconds) for the HTTP request
        timeout = 60

        try:
            # Makes HTTP request
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), 
                                  allow_redirects=True) as response:
            
                logger.info(f"Fetching data for url {url}")

                # If there are errors, returns None (will NOT be inserted in the datalake later)
                if response.status >= 400:
                    logger.error(f"Error {response.status} when fetching {url}")
                    return None, {
                        'status': response.status,
                        'message': f'Error {response.status} when fetching URL'
                    }
                
                # In case there are no issues, proceeds with the request
                else:
                    resp = await response.read()
                    resp_string = resp.decode("utf-8")
                    
                    logger.info(f"Data fetched successfully for url {url}")
                    return resp_string, {
                        'status': 200,
                        'message': 'Success'
                    }

        # Handles some likely errors that can occur during the HTTP request
        except (aiohttp.ClientOSError, aiohttp.ClientConnectionError, 
                aiohttp.client_exceptions.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
            
            logger.error(f"ERROR: Connection issue for {url}: {str(e)}")
            
            return None, {
                'status': 'connection_error',
                'message': f'Connection error: {str(e)}'
            }
            
        except asyncio.exceptions.TimeoutError:
            logger.error(f"ERROR: Timeout error for {url}")
            
            return None, {
                'status': 'timeout_error',
                'message': 'Timeout error'
            }
            
        except aiohttp.client_exceptions.ClientPayloadError:
            logger.error(f"ERROR: Payload error for {url}")
            
            return None, {
                'status': 'payload_error',
                'message': 'Payload error'
            }

# -------------------------------------------------------------------------------

async def get_multiple_urls(urls):
    '''
    This function uses the get_single_url function to make many asynchronous
    HTTP requests. Using this function dramatically speeds up the data
    extraction compared to simply using requests.get()
    
    Parameters:
    - urls: list of URLs to fetch
    
    Returns:
    - list of HTML content strings
    '''

    # Sets a timeout (in seconds) for the HTTP request
    timeout = 60

    try:
        # Process URLs in batches
        batch_size = min(50, len(urls))
        all_responses = []
        
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(urls)-1)//batch_size + 1} ({len(batch_urls)} URLs)")
            
            # Creates an aiohttp connector with a connection limit
            connector = aiohttp.TCPConnector(force_close=True, limit=25)
            
            # Creates a session
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session:
                tasks = []
                for url in batch_urls:
                    tasks.append(get_single_url(url, session))
                
                # Makes asynchronous HTTP requests
                results = await asyncio.gather(*tasks)
                batch_responses = [response for response, _ in results if response is not None]
                
                # If a response is None, adds None to the list to maintain ordering
                for i, (response, _) in enumerate(results):
                    if response is None:
                        batch_responses.insert(i, None)
                
                all_responses.extend(batch_responses)
                    
        return all_responses

    # If any error is raised, waits 5 seconds and retries
    except Exception as error:
        logger.error(f"Error in get_multiple_urls: {str(error)}")
        logger.error("Retrying in 5s...")
        time.sleep(5)
        await get_multiple_urls(urls)

# -------------------------------------------------------------------------------

def has_sublistings(html_source):
    '''
    This function takes the html source code of a page and checks if it has
    sub-listings. It does so by looking at some of the listing's metadata
    that carries information about how many real estate properties are shown
    in the page. If the number is larger than one it means that the listing
    indeed has sub-listing and therefore the function returns True, otherwise it
    returns False

    Parameters:
    - html_source: HTML source code to test

    Returns:
    - True if the listing has sublistings, False otherwise
    '''

    try:
        # Parses the HTML source code with BeautifulSoup
        soup = BeautifulSoup(html_source,'html.parser')

        # Gets the dictionary with the relevant metadata
        script = soup.find(REDACTED)

        # Imports the metadata as a dictionary
        data_dict = json.loads(script)

        # Gets the number of real estate properties shown in the listing
        number_of_properties = ### REDACTED ###

        if number_of_properties == 1:
            # In this cases there are no sub-listings, therefore return False
            return False
        else:
            # There are sub-listings, return True
            return True

    except:
        # If any error is raised (e.g., in case the page does not contain the metadata
        # that it's supposed to contain), return False
        return False

# -------------------------------------------------------------------------------

def is_auction_listing(html_source):
    '''
    This function takes the html source code of a page and checks if its
    corresponding listing is an auction sale or not. It does so by looking at some
    of the page's metadata that carries information about itself, in a way similar
    to the function defined above. This function is needed when scraping data in
    the "sales" category to avoid accidentally getting auction listings (since
    the website returns auction listings also when looking for houses on sale)

    Parameters:
    - html_source: HTML source code to test

    Returns:
    - True if the listing is an aucton listing, False otherwise
    '''

    # In case the request returns an empty page, returns "False"
    if html_source is None:
        return False

    else:
        # Parses the HTML source code with BeautifulSoup
        soup = BeautifulSoup(html_source,'html.parser')

        # Gets the dictionary with the relevant metadata
        script = soup.find(REDACTED)

        # Imports the metadata as a dictionary
        data_dict = json.loads(script)

        # The following code takes care of the (rare) eventuality that one of the
        # keys in data_dict that we would normally expect does not exist. The outcome
        # is defaulted to True so that in those (rare) cases the listing is skipped
        outcome = True

        detail_data = data_dict.get(REDACTED)
        real_estate = detail_data.get(REDACTED)
        properties = real_estate.get(REDACTED)

        if properties and isinstance(properties, list) and isinstance(properties[0], dict):
            outcome = 'auction' in properties[0].keys()

        # Returns the outcome (True or False)
        return outcome

# -------------------------------------------------------------------------------

async def extract_html_source_code(url, html_source, province_name, listing_type, date, parent_listing=None):
    '''
    This function extracts the raw data of a web page and puts it in the
    relevant MongoDB collection of the datalake. In particular, this function
    inserts the listing's web page if it doesn't already exist in the datalake,
    and timestamps it so that we know when the data was collected. If a listing
    already exists, no new data is inserted/overwritten, but a new timestamp is
    added; this allows us to keep track of different listings over time.
    The "parent_listing" argument is non-null only for sub-listings. Similarly,
    the "child_listings" field is non-null only for listings with sub-listings

    Parameters:
    - url: listing's URL
    - html_source: listing's HTML source
    - province_name: name of the listing's province
    - listing_type: listing type (rent/sale/auction)
    - date: scraping date
    - parent_listing: ID of the parent listing (if present)
    '''

    # Checks that the "listing_type" argument is one of the accepted values
    valid_listing_types = ["sale", "rent", "auction"]
    if listing_type not in valid_listing_types:
        raise ValueError("listing_type must be one of %r." % valid_listing_types)

    # Defines which collection we are going to use based on the argument "listing_type" passed
    if listing_type == "sale":
        collection = sale_coll
    elif listing_type == "rent":
        collection = rent_coll
    elif listing_type == "auction":
        collection = auction_coll

    # Uses the url to define the listing's id (the numbers at the end of the url)
    listing_id = int(url.split('/')[-2])

    # Checks if the listing is already present in auction collection
    # (only for sales listings, because when looking for houses on sale the
    # website can also returns auction listings)
    if listing_type == "sale":
        if is_auction_listing(html_source):
            # If the listing is already in the auction collection, then
            # simply exit this function (no need to do anything)
            return
        else:
            # Otherwise, continue
            pass
    else:
        # If listing is of type "rent" or "auction", continues (no need to do anything)
        pass

    if html_source is not None:
        # Parses the html source code passed to the function with BeautifulSoup
        soup_page = BeautifulSoup(html_source, 'html.parser')

        # Checks if page has sublistings, and if there are extracts and loads their data
        if has_sublistings(html_source):
            # If the page has sub-listings, re-opens the page with selenium to click
            # the button that shows all the sub-listings' links, then parses the
            # resulting page with BeautifulSoup

            # The soup object defined above belongs to the parent's listing
            soup_page_parent = soup_page

            # Starts selenium with headless option
            options = Options()
            options.add_argument('--headless')
            
            # Creates the driver
            driver = webdriver.Firefox(options=options)
            
            try:
                # Loads the page
                driver.get(url)
                
                # The button that shows all the sub-listings' can take two forms: one
                # for when there is only one additional listing, and one for when there
                # are multiple additional listings. This code below first checks if
                # there is only one single additional listing, and if not checks if
                # there are more than one. In either case it clicks on the button and
                # parses the page's source code with BeautifulSoup
                try:
                    # Looks for button for when there is only one additonal sublisting
                    btn_sing = driver.find_element(By.XPATH, REDACTED)
                    driver.execute_script(REDACTED, btn_sing)
                    
                    # Parses resulting page with BeautifulSoup
                    soup_page = BeautifulSoup(driver.page_source, 'html.parser')

                except:
                    # If there is no button (driver.find_element raises exception), continues
                    pass

                # Tries the same as above but with the button for when there are
                # multiple additional sub-listings instead of just one
                try:
                    # Looks for button for when there are multiple additonal sublistings
                    btn_plur = driver.find_element(By.XPATH, REDACTED)
                    driver.execute_script(REDACTED, btn_plur)
                    
                    # Parses resulting page with BeautifulSoup
                    soup_page = BeautifulSoup(driver.page_source, 'html.parser')

                except:
                    # If there is no button (driver.find_element raises exception), continues
                    pass
                
            except Exception as e:
                logger.error(f"Error during Selenium session: {str(e)}")
            
            finally:
                # Always ensures that the driver object is closed
                try:
                    driver.quit()
                except:
                    pass

            # Gets all the sub-listings' links
            sub_listings_links = ### REDACTED ###

            # Gets all the sub-listings' IDs
            sub_listings_ids = ### REDACTED ###

            # Checks if we already have data for this listing on this date
            existing_entry = collection.find_one({"_id": listing_id, "data.scraping_date": date})

            if not existing_entry:
                # If the listing does not already exist or we don't have data for this date, inserts/updates the data in the datalake
                collection.update_one(
                    {"_id": listing_id},
                    {
                        # This information is added only when a new record is created
                        "$setOnInsert": {
                            "province_name": province_name,
                            "province_code": prov_to_code[province_name],
                            "parent_listing": parent_listing
                        },

                        # This only adds previously non-existing sub-listings' IDs
                        "$addToSet": {"child_listings": {"$each": sub_listings_ids}},

                        # This appends the scraping date and the HTML source code only if it doesn't exist
                        "$push": {"data": {"scraping_date": date, "html_source": soup_page_parent.prettify()}}
                    },
                    upsert=True
                )

            # Processes sub-listings
            all_sub_listings_html = await get_multiple_urls(sub_listings_links)
            
            # Iterates over the links obtained above and calls this same function recursively to insert all the sub-listings' data in the datalake
            try:
                for i in range(len(sub_listings_links)):
                    await extract_html_source_code(
                        sub_listings_links[i], 
                        all_sub_listings_html[i], 
                        province_name, 
                        listing_type, 
                        date, 
                        parent_listing=listing_id
                    )
            except Exception as e:
                logger.error(f"Error processing sub-listings: {str(e)}")

        else:
            # If there are no sub-listings, continues
            pass

        # It is unlikely (can happen to 1 page every many tens of thousands of
        # pages) but it can happen that in the time between getting all the sub-listings'
        # links and getting all their data one of the pages no longer exists (e.g.
        # because the listing is removed). To avoid loading a 404 error landing page,
        # we check if the loaded page actually has any information at all. This is what
        # the code below does, and if indeed the page does not include any information
        # it simply exits the function, as no action is necessary

        try:
            # Gets the page's metadata
            script = soup_page.find(REDACTED)
            
            # Handles the (rare) case where the script element doesn't exist
            if not script or not script.contents:
                logger.warning(f"No metadata found in page for listing {listing_id}, skipping...")
                return
                
            script_content = script.### REDACTED ###

            # Loads the metadata as a dictionary
            data_dict = json.loads(script_content)

            if "detailData" not in data_dict["props"]["pageProps"].keys():
                # This condition is True when the page contains for example a 404 error and
                # no information about a listing, and therefore does not contain any useful
                # information at all. In this case we need to remove the record that was
                # initially inserted, because it will only contain the id and scraping date
                logger.warning(f"Listing {listing_id} returned a page with no detail data, removing from database...")
                collection.delete_one({"_id": listing_id})
                return
                
        # If it can't parse the metadata, assumes the page is invalid
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError) as e:
            logger.error(f"Error parsing page metadata for listing {listing_id}: {str(e)}")
            collection.delete_one({"_id": listing_id})
            return
        
        # Handles other generic errors
        except Exception as e:
            logger.error(f"Unexpected error for listing {listing_id}: {str(e)}")
            return

        # Checks if we already have data for this listing on this date
        existing_entry = collection.find_one({"_id": listing_id, "data.scraping_date": date})

        if not existing_entry:
            # If the listing does not already exist or we don't have data for this date, inserts/updates the data in the datalake
            try:
                collection.update_one(
                    {"_id": listing_id},
                    {
                        # This information is added only when a new record is created
                        "$setOnInsert": {
                            "province_name": province_name,
                            "province_code": prov_to_code[province_name],
                            "parent_listing": parent_listing,
                            "child_listings": None
                        },

                        # This appends the scraping date and the HTML source code only if it doesn't exist
                        "$push": {"data": {"scraping_date": date, "html_source": soup_page.prettify()}}
                    },
                    upsert=True
                )
                
            except Exception as e:
                logger.error(f"Error updating MongoDB for listing {listing_id}: {str(e)}")

    # The html we are trying to extract information from is a None object, so no action is necessary (we skip it)
    else:
        logger.warning(f"Received None HTML source for URL: {url}, skipping...")

# -------------------------------------------------------------------------------

async def datalake_populate_links_range(province, listing_type, mode, price1, price2=None):
    '''
    This functions gets a province name, listing type and price range, then
    finds all the links of the listings that correspond to those parameters
    and fetchen the source code of those pages to populate the datalake.
    The "mode" argument determines how the price range should be treated:
    - if "mode" == "up", gets the listings whose price is larger than "price1"
    - if "mode" == "between", gets the listings whose price is between "price1" and "price2"
    - if "mode" == "down", gets the listings whose price is below price1

    Parameters:
    - province: name of the province being scraped
    - listing_type: listing type (rent/auction/sale)
    - mode: explained above
    - price1 and price2: explained above
    '''

    # Checks that the "mode" argument is one of the accepted values
    mode_valid_values = ["up", "down", "between"]
    if mode not in mode_valid_values:
        raise ValueError("mode must be one of %r." % mode_valid_values)

    # Defines which string we are going to use in the HTTP requests based on the arguments passed
    if listing_type == "sale":
        search_string = "vendita-case"
    elif listing_type == "rent":
        search_string = "affitto-case"
    elif listing_type == "auction":
        search_string = "aste-immobiliari"

    # Defines the url needed to fetch all the listings' links and the opening/closing message for the task's log
    if mode == "up":
        opening_message = f"Starting to fetch data for province {province}, listing type {listing_type}, price above {price1}"
        closing_message = f"Finished fetching data for province {province}, listing type {listing_type}, price above {price1}"
        logger.info(opening_message)
        url = ### REDACTED ###
    elif mode == "down":
        opening_message = f"Starting to fetch data for province {province}, listing type {listing_type}, price below {price1}"
        closing_message = f"Finished fetching data for province {province}, listing type {listing_type}, price below {price1}"
        logger.info(opening_message)
        url = ### REDACTED ###
    elif mode == "between":
        opening_message = f"Starting to fetch data for province {province}, listing type {listing_type}, price between {price1} and {price2}"
        closing_message = f"Finished fetching data for province {province}, listing type {listing_type}, price between {price1} and {price2}"
        logger.info(opening_message)
        url = ### REDACTED ###

    logger.info("Getting HTML source of request's webpage")
    
    # Uses a persistent session for HTTP requests
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:        
        # Makes the request using our session
        req = session.get(url, timeout=60)
        
        # Raises exception for HTTP errors
        req.raise_for_status()  

    # If any errors are raised, log and retries once
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching main listings page: {str(e)}")
        logger.info("Retrying...")
        
        try:
            req = session.get(url, timeout=120)
            req.raise_for_status()
        except requests.exceptions.RequestException as e2:
            logger.error(f"Retry failed: {str(e2)}")
            return
    
    logger.info(f"HTML source obtained with status code {req.status_code}")

    # Parses the page's content with Beautifulsoup
    soup = BeautifulSoup(req.content, 'html.parser')

    # For this parsed page, looks for how many additional pages are linked to at the bottom
    logger.info("Checking how many pages the request returned")
    last_page_data = soup.find_all(REDACTED)
    page_number_data = soup.find_all(REDACTED)

    if len(last_page_data) == 0:
        # If no other pages are shown, it means that the search only returned one page
        page_number = 1
        logger.info("1 page returned")
    else:
        # Otherwise, it computes the number of pages to look through
        if len(page_number_data) == 1:
            page_number = int(last_page_data[-2].text)
            logger.info(f"{page_number} pages returned")
        else:
            page_number = int(soup.find_all(REDACTED)[-1].text)
            logger.info(f"{page_number} pages returned")

    if page_number < 80:
        # The website is designed to cap the number of returned pages to 80,
        # which corresponds to 2k listings. If the request would technically return
        # 3k listings, for example, the website will cut the results at 2k and
        # not show the last 1k listings. Therefore, if the number of returned pages
        # is <80 then we are below this limit imposed by the website, and we can
        # go on and get all the links shown in all pages. However, if this is not
        # the case then the website is not showing us all the listings that match our
        # criteria, so we have to retry with a narrower price window (see below)
        logger.info(f"Starting to extract data from the {page_number} page(s) returned")

        # Defines the url of all the pages returned by the request
        pages_urls = [f"{url}&pag={i}" for i in range(1, page_number+1)]

        logger.info(f"Fetching data for {page_number} page(s) (might take a while)")

        # Makes asynchronous HTTP requests for all pages
        all_pages_data = await get_multiple_urls(pages_urls)
        
        # Parses all these pages' data with BeautifulSoup
        logger.info(f"Parsing data for {page_number} page(s) fetched")
        soups_pages = [BeautifulSoup(x, 'html.parser') for x in all_pages_data if x is not None]
        logger.info(f"Data fetched and parsed for {len(soups_pages)} page(s)")

        # Extracts all the listings' links from these parsed pages
        # First, gets all the listings' links
        logger.info("Extracting listings' links from fetched data")
        listing_links = []
        for i in range(len(soups_pages)):
            listing_links.append([x["href"] for x in soups_pages[i].find_all(REDACTED)])
        listing_links = [x for elements in listing_links for x in elements]

        # Then, computes how many listings there are in total
        listing_number = len(listing_links)
        logger.info(f"Listing links extracted. There are {listing_number} listing(s) in total")

        # Check if there are any listings to process
        if listing_number == 0:
            logger.info("No listings found for this price range, skipping...")
            logger.info(closing_message)
            return

        logger.info(f"Fetching data for {listing_number} listing(s) (might take a while)")
        
        # Gets HTML for all listings, processing them in batches
        all_listings_data = []
        batch_size = min(50, listing_number)
        
        for i in range(0, listing_number, batch_size):
            batch_urls = listing_links[i:i+batch_size]
            logger.info(f"Processing listings batch {i//batch_size + 1}/{(listing_number-1)//batch_size + 1} ({len(batch_urls)} listings)")
            
            # Makes asynchronous HTTP requests
            batch_data = await get_multiple_urls(batch_urls)
            all_listings_data.extend(batch_data)
        
        logger.info(f"Data fetched for {len(all_listings_data)} listing(s)")

        # Populates the datalake with the data fetched for all the listings
        logger.info("Datalake population started for this batch")
        for i in range(len(all_listings_data)):
            await extract_html_source_code(listing_links[i], all_listings_data[i], province, listing_type, todays_date)
            logger.info(f"Processed listing # {i+1}/{len(all_listings_data)}")
                            
        logger.info("Datalake population finished for this batch")
        logger.info(closing_message)

    else:
        # If the number of returned pages is greater or equal to 80 (or alternatively,
        # if the number of returned listings is greater or equal to 2k), then
        # we call this function recursively but with a narrower price window.
        # The recursive call ensures that this goes on until a narrow enough window
        # is reached and we can fetch the data without missing any listing
        logger.info("Too many results returned. Trying again with a narrower price window")
        if mode == "down":
            mid_point = str(int(int(price1)/2))
            await datalake_populate_links_range(province, listing_type, "down", mid_point)
            await datalake_populate_links_range(province, listing_type, "between", mid_point, price1)
        elif mode == "between":
            mid_point = str(int((int(price1)+int(price2))/2))
            await datalake_populate_links_range(province, listing_type, mode, price1, mid_point)
            await datalake_populate_links_range(province, listing_type, mode, mid_point, price2)
        elif mode == "up":
            mid_point = str(int(int(price1)*2))
            await datalake_populate_links_range(province, listing_type, "between", price1, mid_point)
            await datalake_populate_links_range(province, listing_type, "up", mid_point)

# -------------------------------------------------------------------------------

async def datalake_populate_links_province_async(province, listing_type):
    '''
    This function uses all the functions defined above to scrape all the listings
    of a given province for a given listing type and loads the raw html source
    code into the datalake

    Parameters:
    - province: name fo the provicne to be scraped
    - listing_type: listing type (rent/sale/auction)
    '''

    # For sales and auctions listings, we scan prices from 50k€ to 1M€ in steps of 50k€
    min_price_sale = int(5e4)
    max_price_sale = int(1e6)
    step_sale = 50000
    prices_sale = np.arange(min_price_sale, max_price_sale+step_sale, step_sale).astype("int").astype("str")

    # For rent listings, we scan from 200€ to 5k€ in steps of 400€
    min_price_rent = int(2e2)
    max_price_rent = int(5e3)
    step_rent = 400
    prices_rent = np.arange(min_price_rent, max_price_rent+step_rent, step_rent).astype("int").astype("str")

    logger.info(f"Starting extraction process for province {province}, listing type {listing_type}")
    # Determines which price range and steps we need to use depending on the "listing_type" argument
    if listing_type == "rent":
        prices = prices_rent
    else:
        prices = prices_sale

    # Gets the listings whose price is below the range's minimum
    await datalake_populate_links_range(province, listing_type, "down", prices[0])

    # Gets the listings whose price is between all the range's steps
    for i in range(1, len(prices)):
        await datalake_populate_links_range(province, listing_type, "between", prices[i-1], prices[i])

    # Gets the listings whose price is above the range's maximum
    await datalake_populate_links_range(province, listing_type, "up", prices[-1])

    logger.info(f"Finished extracting data for province {province}, listing type {listing_type}")

# -------------------------------------------------------------------------------

def datalake_populate_links_province(province, listing_type):
    '''
    This function is simply a wrapper on the async function defined above. It
    is needed in order for Airflow's PythonOperator to run it correctly
    '''
    asyncio.run(datalake_populate_links_province_async(province, listing_type))

# -------------------------------------------------------------------------------

def log_datalake_statistics():
    '''
    This function connects to the MongoDB database and logs basic statistics 
    about how many listings are in each collection of the datalake
    '''
        
    # Logs the header for the statistics
    logger.info("=" * 30)
    logger.info("DATALAKE LISTING COUNTS")
    logger.info("=" * 30)
    
    # Gets the number of documents in each collection
    sale_count = sale_coll.count_documents({})
    rent_count = rent_coll.count_documents({})
    auction_count = auction_coll.count_documents({})
    total_count = sale_count + rent_count + auction_count
    
    # Logs the results
    logger.info(f"Sale listings:     {sale_count}")
    logger.info(f"Rent listings:     {rent_count}")
    logger.info(f"Auction listings:  {auction_count}")
    logger.info("-" * 30)
    logger.info(f"TOTAL LISTINGS:    {total_count}")
    logger.info("=" * 30)
    
    # Closes the MongoDB connection
    mongoDB_client.close()

    # Logs a final success message
    logger.info("Datalake population completed successfully.")
    

# -------------------------------------------------------------------------------
# -----------------------------DAG DEFINITION------------------------------------
# -------------------------------------------------------------------------------

# Defines the DAG's default arguments
def_args = {
            "owner": "Leonardo Pacciani-Mori",
            "start_date": days_ago(0),
            "max_active_tasks": 16,
            "retries": 5,
            "retry_delay": timedelta(minutes=1)
           }

# Defines the DAG object
datalake_dag = DAG(
                     'immobiliare.it_datalake_population_DAG_webscraping',
                     default_args = def_args,
                     description = "DAG to populate mongoDB datalake with raw data from rent, auction, and sale listings",
                     schedule = None
                  )

# Defines the task that prints the final datalake statistics log and success message
finalize_task = PythonOperator(
    task_id="finalize_task",
    python_callable=log_datalake_statistics,
    dag=datalake_dag
)

# Uses the csv file to get all the provinces' names and their total number
provinces = prov_df["Province"]
provinces_num = len(provinces)

# Defines the sequence of tasks for each province
for i in range(provinces_num):
    province = provinces[i]
    
    # Creates three tasks in sequence for each province: rent → auction → sale
    rent_task = PythonOperator(
        task_id=f"{province}_rents",
        python_callable=datalake_populate_links_province,
        op_kwargs={"province": province, "listing_type": "rent"},
        dag=datalake_dag
    )
    
    auction_task = PythonOperator(
        task_id=f"{province}_auctions",
        python_callable=datalake_populate_links_province,
        op_kwargs={"province": province, "listing_type": "auction"},
        dag=datalake_dag
    )
    
    sale_task = PythonOperator(
        task_id=f"{province}_sales",
        python_callable=datalake_populate_links_province,
        op_kwargs={"province": province, "listing_type": "sale"},
        dag=datalake_dag
    )
    
    # Sets up the task dependencies: rent → auction → sale → finalize
    rent_task >> auction_task >> sale_task >> finalize_task