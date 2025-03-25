'''
Filename: datalake_ETL_warehouse_mongoDB.py
Author: Leonardo Pacciani-Mori

Description:
    This script gets data from the MongoDB datalake and moves it into the
    MongoDB warehouse (both hosted locally on the same machine where this
    script is executed) while performing a simple ETL pipeline.
    This script uses Apache Airflow to divide the data migration into smaller,
    independent tasks that can be executed in parallel.

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

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import asyncio
from datetime import datetime
import pymongo
from bs4 import BeautifulSoup
import json
from dateutil import parser

import logging

# Sets the logging level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------------
# ---------------------------FUNCTION DEFINITIONS--------------------------------
# -------------------------------------------------------------------------------

def remove_non_numbers(string):
    '''
    This function simply takes a string and formats it in a way that can be
    cast into int/float for applications below. It transorms strings like
    "€300000,00" into "300000"

    Parameters:
    - string: the string to be processed

    Returns:
    - temp_string: processed string
    '''
    # Replaces commas with dots for decimal notation
    string = string.replace(",",".")

    # Only extracts numeric information from the string
    temp_string = ""
    for x in string:
        if x.isnumeric() or x == ".":
            temp_string += x
        else:
            continue
    if temp_string == "":
        return None
    else:
        return temp_string

#-------------------------------------------------------------------------------

def mortgage_monthly_payment(principal,interest,term):
    '''
    This function calculates the monthly payment of a mortgage given the principal
    amount, the interest (in absolute value) and the term (in years)
    
    Parameters:
    - principal: the mortgage's principal
    - interest: the mortgage's interest rate
    - term: the mortgage's term

    Returns:
    - the monthly payment
    '''
    if (principal is not None) and (interest is not None):
        P = principal
        i = interest/12
        n = term * 12
        return round(P*(i*(i+1)**n)/((i+1)**n-1),2)
    else:
        # If either the vales for principal or interest are null, returns null
        return None

#-------------------------------------------------------------------------------

async def extract_and_transform_data(listing,listing_type):
    '''
    This function gets a listing's data and type and returns a dictionary with
    all its features extracted from the web page source code
    
    Parameters:
    - listing: the listing's MongoDB document from the datalake
    - listing_type: listing type (rent/sale/auction)

    Returns:
    - features: a dictionary with all the listing's information, to be inserted
                in the data warehouse
    '''

    # Opens mongoDB connection
    mongoDB_client= pymongo.MongoClient("localhost", 27017)

    # Defines datalake and its collections
    datalake = mongoDB_client.immobiliare_datalake
    datalake_sale_coll= datalake.sale
    datalake_rent_coll = datalake.rent
    datalake_auction_coll = datalake.auction

    # Checks that the listing type is one of the accepted values
    valid_listing_types = ["sale","rent","auction"]
    if listing_type not in valid_listing_types:
        raise ValueError("listing_type must be one of %r." % valid_listing_types)

    # Defines which collection we are going to use based on the arguments passed
    if listing_type == "sale":
        datalake_collection = datalake_sale_coll
    elif listing_type == "rent":
        datalake_collection = datalake_rent_coll
    elif listing_type == "auction":
        datalake_collection = datalake_auction_coll

    # Gets HTML source code
    source_code = listing["data"][-1]["html_source"]

    # Gets last scraping date
    last_date_scraped = listing["data"][-1]["scraping_date"]

    # Parses the HTML source code with BeautifulSoup
    soup = BeautifulSoup(source_code,'html.parser')

    # Gets the dictionary containing all information
    script = soup.find(REDACTED)
    data_dict = json.loads(script)

    # The following four definitions are introduced to make the code below more readable
    properties_general = ### REDACTED ###
    prop_gen_keys = properties_general.keys()
    properties_deeper = ### REDACTED ###
    prop_deeper_keys = properties_deeper.keys()

    # Now we collect all data, without discriminating by listing type. In other
    # words, we collect all possible data irrespective of if that listing type
    # admits that particular value, and use None when no data is present (for
    # example, no listing in the "rent" or "sale" collection will have data for
    # the "auction_info" field, so we leave that field blank for those listings)

    # Let's start with general properties of the listing itself

    # "listing_creation_date": when the listing ID was first created. With the last
    # scraping date we can compute how long the listing was up
    if 'createdAt' in prop_gen_keys:
        timestamp = properties_general["createdAt"]
        listing_creation_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    else:
        listing_creation_date = None

    # "listing_age": we compute the listing's age (in days) as the difference
    # between the scraping date and the creation date
    if listing_creation_date is not None:
        listing_age = (datetime.strptime(last_date_scraped, "%Y-%m-%d")-datetime.strptime(listing_creation_date, "%Y-%m-%d")).days
    else:
        listing_age = None

    # "last_update": when listing was updated last. With the last scraping date
    # we can compute how many days the listing was up since the last update
    if 'lastUpdate' in prop_deeper_keys:
        date_data = properties_deeper["lastUpdate"].split(" ")[-1].split("/")
        last_update = date_data[-1]+"-"+date_data[-2]+"-"+date_data[-3]
        if len(last_update)==8:
            date_data = last_update.split("-")
            last_update = "20"+date_data[0]+"-"+date_data[1]+"-"+date_data[2]
        else:
            pass
    else:
        last_update = None

    # "listing_last_update": we compute the number of days between the last update
    # of the listing's page and the scraping date
    if last_update is not None:
        listing_last_update = (datetime.strptime(last_date_scraped, "%Y-%m-%d")-datetime.strptime(last_update, "%Y-%m-%d")).days
    else:
        listing_last_update = None

    # "is_listing_new": if listing is new or has been updated since creation
    if "isNew" in prop_gen_keys:
        is_listing_new = properties_general["isNew"]
    else:
        is_listing_new = None

    # "has_parent": True if the listing IS a sub-listing
    if listing["parent_listing"] is not None:
        has_parent = True
    else:
        has_parent = False

    # "has_child": True if the listing HAS sub-listings
    if listing["child_listings"] is not None:
        has_child = True
    else:
        has_child = False

    # "seller_type": type of seller for the listing (e.g., agency or direct sale from owner)
    if 'advertiser' in prop_gen_keys:
        seller_type = properties_general["advertiser"]
        if seller_type is not None:
            if "agency" in seller_type.keys():
                # This can either be "agenzia" (real estate agency), "impresa edile" (construction company)
                # or "tribunale" (court, for auctions)
                seller_type = seller_type["agency"]["label"]
            else:
                seller_type = "privato"
        else:
            pass
    else:
        seller_type = None

    # "number_of_photos": how many pictures does the listing have
    if "multimedia" in prop_deeper_keys:
        if "photos" in properties_deeper["multimedia"].keys():
            number_of_photos = len(properties_deeper["multimedia"]["photos"])
        else:
            number_of_photos = None
    else:
        number_of_photos = None

    # Let's now look into the price and other costs

    # "price": asking price for sales, minimum offer for auctions, rent for rents
    if 'price' in prop_deeper_keys:
        if "value" in properties_deeper["price"].keys():
            price = properties_deeper["price"]["value"]
        else:
            price = None
    else:
        price = None

    # "additional_costs": additional costs (e.g., condominium expenses, heating expenses, mortgage rate etc.)
    # The mortgage rate is the average of the current 30-year mortgage rate with a 20% down payment
    if 'costs' in prop_deeper_keys:
        additional_costs = properties_deeper["costs"]
        if additional_costs is None:
            pass
        else:
            temp_dict = {}
            if "condominiumExpenses" in additional_costs.keys():
                temp_dict["condominium_expenses"] = additional_costs["condominiumExpenses"]
            else:
                temp_dict["condominium_expenses"] = None
            if "heatingExpenses" in additional_costs.keys():
                temp_dict["heating_expenses"] = additional_costs["heatingExpenses"]
            else:
                temp_dict["heating_expenses"] = None

            additional_costs = temp_dict
    else:
        additional_costs = None

    if ("mortgage" in prop_gen_keys) and (properties_general["mortgage"]["mortgageWidget"] is not None):
        data_dict = properties_general["mortgage"]["mortgageWidget"]["rates"]
        rates_list = next(item for item in data_dict if item["percent"] == 80)["rates"]
        mortgage_rate = next(item for item in rates_list if item["year"] == 30)["rate"]
    else:
        mortgage_rate = None

    # This field is left empty and will be filled in the migration to the PostgreSQL data warehouse
    mortgage_payment = None

    # "auction_info": information on auctions (non-null only for auction listings)
    if "auction" in prop_deeper_keys:
        auction_info = properties_deeper["auction"]
        if auction_info is None:
            pass
        else:
            temp_dict = {}
            if "saleDateValue" in auction_info.keys():
                temp_dict["auction_end_date"] = auction_info["saleDateValue"]
            else:
                temp_dict["auction_end_date"] = None

            if "modalityDeposit" in auction_info.keys():
                temp_dict["deposit_modality"] = auction_info["modalityDeposit"]
            else:
                temp_dict["deposit_modality"] = None

            if "saleType" in auction_info.keys():
                temp_dict["auction_type"] = auction_info["saleType"]
            else:
                temp_dict["auction_type"] = None

            if "saleState" in auction_info.keys():
                temp_dict["is_open"] = auction_info["saleState"]["isAvailable"]
            else:
                temp_dict["is_open"] = None

            if "minimumOffer" in auction_info.keys():
                temp_dict["minimum_offer"] = float(remove_non_numbers(auction_info["minimumOffer"].split(",")[0].replace(".","")))
            else:
                temp_dict["minimum_offer"] = None

            if "procedureNumber" in auction_info.keys():
                temp_dict["procedure_number"] = auction_info["procedureNumber"]
            else:
                temp_dict["procedure_number"] = None

            if "auctionCourt" in auction_info.keys():
                temp_dict["auction_court"] = auction_info["auctionCourt"]
            else:
                temp_dict["auction_court"] = None

            if "lotCategory" in auction_info.keys():
                temp_dict["lot_category"] = auction_info["lotCategory"]
            else:
                temp_dict["lot_category"] = None

            auction_info = temp_dict
    else:
        auction_info = None

    # "cadastral_info": cadastral information (non-null only for auction listings)
    if 'cadastrals' in prop_deeper_keys:
        cadastral_info = properties_deeper["cadastrals"]
        if len(cadastral_info) == 0:
            cadastral_info = None
        else:
            cadastral_info = cadastral_info[0]
            temp_dict = {}
            if 'cadastral' in cadastral_info:
                temp_dict["cadastral"] = cadastral_info["cadastral"]
            else:
                temp_dict["cadastral"] = None

            if 'cadastralInfo' in cadastral_info:
                temp_dict["cadastral_additional_info"] = cadastral_info["cadastralInfo"]
            else:
                temp_dict["cadastral_additional_info"] = None

            if 'subCadastral' in cadastral_info:
                temp_dict["sub_cadastral_info"] = cadastral_info["subCadastral"]
            else:
                temp_dict["sub_cadastral_info"] = None

            cadastral_info = temp_dict
    else:
        cadastral_info = None

    #Let us now look into the features of the property

    # "total_surface": total surface of property
    if 'surface' in prop_deeper_keys:
        total_surface = properties_deeper["surface"]
        if total_surface is None:
            pass
        else:
            total_surface = float(remove_non_numbers(total_surface).replace("²","").replace(".",""))
    elif 'surfaceValue' in prop_deeper_keys:
        total_surface = properties_deeper["surfaceValue"]
        if total_surface is None:
            pass
        else:
            total_surface = float(remove_non_numbers(total_surface).replace("²","").replace(".",""))
    else:
        total_surface = None

    # "price_per_sq_mt": here we compute the price (or rent) per square meter
    if (price is not None) and (total_surface is not None):
        price_per_sq_mt = round(price/total_surface,2)
    else:
        price_per_sq_mt = None

    # "surface_composition": how the total surface is divided into different parts
    # (e.g., apartment, garage, garden etc.)
    if 'surfaceConstitution' in prop_deeper_keys:
        surface_composition = properties_deeper["surfaceConstitution"]
        if surface_composition is None:
            pass
        else:
            number_of_elements = len(properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'])
            for i in range(number_of_elements):
                temp_dict = {}
                temp_keys = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i].keys()
                if "constitution" in temp_keys:
                    temp_dict["element"] = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['constitution']
                else:
                    temp_dict["element"] = None
                if "floor" in temp_keys:
                    temp_dict["floor"] = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['floor']['value']
                else:
                    temp_dict["floor"] = None
                if "surface" in temp_keys:
                    total_surf = remove_non_numbers(properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['surface'].split(",")[0])
                    if total_surf is not None:
                        total_surf = total_surf.replace(".","")
                    else:
                        pass
                    if (total_surf is None) or (total_surf == ''):
                        temp_dict["surface"] = None
                    else:
                        temp_dict["surface"] = float(total_surf)
                else:
                    temp_dict["surface"] = None
                if "percentage" in temp_keys:
                    temp_dict["percentage"] = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['percentage']
                else:
                    temp_dict["percentage"] = None
                if "commercialSurface" in temp_keys:
                    if "commercialSurface" in temp_keys:
                        comm_surf = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['commercialSurface'].split(",")[0].replace(".","")
                        if comm_surf == '':
                            temp_dict["commercial_surface"] = None
                        else:
                            temp_dict["commercial_surface"] = float(comm_surf)
                else:
                    temp_dict["commercial_surface"] = None
                if "surfaceType" in temp_keys:
                    temp_dict["surface_type"] = properties_deeper["surfaceConstitution"]['surfaceConstitutionElements'][i]['surfaceType']
                else:
                    temp_dict["surface_type"] = None

                surface_composition = temp_dict
    else:
        surface_composition = None

    # "availability": if the property is immediately available, or the future
    # date in which the property will be available
    if 'availability' in prop_deeper_keys:
        availability = properties_deeper["availability"]
    else:
        availability = None

    # "category": residential, commercial, office etc.
    if 'category' in prop_deeper_keys:
        category = properties_deeper["category"]["name"]
        if category is not None:
            category = category.lower()
        else:
            pass
    else:
        category = None

    # "type_of_property": type of property (apartment, single-family house, villa etc.)
    # Also contains info on type of ownership (e.g., nuda proprietà, intera proprietà etc.)
    if 'typologyValue' in prop_deeper_keys:
        type_of_property = properties_deeper["typologyValue"]
        if type_of_property is not None:
            type_of_property = type_of_property.lower()
        else:
            pass
    else:
        type_of_property = None

    # "is_luxury": True if listing is luxury real estate
    if "luxury" in prop_gen_keys:
        is_luxury = properties_general["luxury"]
    else:
        is_luxury = None

    # "condition": condition of the property (e.g., new, good, needs renovation etc.)
    if 'condition' in prop_deeper_keys:
        condition = properties_deeper["condition"]
        if condition is not None:
            condition = condition.lower()
        else:
            pass
    else:
        condition = None

    # "bathrooms_number": number of bathrooms
    if 'bathrooms' in prop_deeper_keys:
        bathrooms_number = properties_deeper["bathrooms"]
        if bathrooms_number is None:
            pass
        elif bathrooms_number == "3+":
            bathrooms_number = 4
        else:
            bathrooms_number = int(bathrooms_number)
    else:
        bathrooms_number = None

    # "bedrooms_number": number of bedrooms
    if 'bedRoomsNumber' in prop_deeper_keys:
        bedrooms_number = properties_deeper["bedRoomsNumber"]
        if bedrooms_number is None:
            pass
        else:
            bedrooms_number = int(bedrooms_number)
    else:
        bedrooms_number = None

    # "total_room_number": total number of rooms
    if 'rooms' in prop_deeper_keys:
        total_room_number = properties_deeper["rooms"]
        if total_room_number is None:
            pass
        else:
            #If this variable is "5+" this is saved as 6
            if total_room_number == '5+':
                total_room_number = 6
            else:
                try:
                    total_room_number = int(total_room_number)
                except:
                    pass
    else:
        total_room_number = None

    # "rooms_description": textual description of the type and number of rooms
    if 'roomsValue' in prop_deeper_keys:
        rooms_description = properties_deeper["roomsValue"]
        if rooms_description is not None:
            rooms_description = rooms_description.lower()
        else:
            pass
    else:
        rooms_description = None

    # "kitchen_status": e.g. if kitchen is full kitchen or just kitchenette
    if 'kitchenStatus' in prop_deeper_keys:
        kitchen_status = properties_deeper["kitchenStatus"]
        if kitchen_status is not None:
            kitchen_status = kitchen_status.lower()
        else:
            pass
    else:
        kitchen_status = None

    # "garage": quantity and type of garage(s)
    if 'garage' in prop_deeper_keys:
        garage = properties_deeper["garage"]
    else:
        garage = None

    # "property_floor": on which floor is the listed property
    if 'floor' in prop_deeper_keys:
        property_floor = properties_deeper["floor"]
        if property_floor is None:
            pass
        else:
            # Ground floor ("piano terra") is encoded as 0
            if (property_floor["abbreviation"] == 'T') or ("terra" in property_floor["floorOnlyValue"]):
                property_floor = 0
            else:
                if type(property_floor["floorOnlyValue"]) != str:
                    property_floor = int(property_floor["floorOnlyValue"])
                else:
                    property_floor = property_floor["floorOnlyValue"]
    else:
        property_floor = None

    # "additional_features": additional features of the listing (e.g., has balcony and/or
    # basement, type of fixtures etc.)
    if 'ga4features' in prop_deeper_keys:
        additional_features = properties_deeper["ga4features"]
    else:
        additional_features = None

    # "energy_info": info about the property's energy consumption and efficiency
    if 'energy' in prop_deeper_keys:
        energy_info = properties_deeper["energy"]
        if energy_info is None:
            pass
        else:
            temp_dict = {}
            if "zeroEnergyBuilding" in energy_info.keys():
                temp_dict["is_zero_energy_building"] = energy_info["zeroEnergyBuilding"]
            else:
                temp_dict["is_zero_energy_building"] = None

            if "heatingType" in energy_info.keys():
                temp_dict["heating_type"] = energy_info["heatingType"]
            else:
                temp_dict["heating_type"] = None

            if "class" in energy_info.keys():
                temp_dict["energy_class"] = energy_info["class"]
            else:
                temp_dict["energy_class"] = None

            if "epi" in energy_info.keys():
                if type(energy_info["epi"]) == str:
                    temp_dict["energy_consumption"] = float(remove_non_numbers(energy_info["epi"]))   #units are kWh/m^2
                else:
                    temp_dict["energy_consumption"] = energy_info["epi"]
            else:
                temp_dict["energy_consumption"] = None

            if "airConditioning" in energy_info.keys():
                temp_dict["air_conditioning"] = energy_info["airConditioning"]
            else:
                temp_dict["air_conditioning"] = None

            energy_info = temp_dict
    else:
        energy_info = None

    # "location_info": info on the property's location
    if 'location' in prop_deeper_keys:
        location_info = properties_deeper["location"]
        if location_info is None:
            pass
        else:
            temp_dict = {}
            if "latitude" in location_info.keys():
                temp_dict["latitude"] = location_info["latitude"]
            else:
                temp_dict["latitude"] = None

            if "longitude" in location_info.keys():
                temp_dict["longitude"] = location_info["longitude"]
            else:
                temp_dict["longitude"] = None

            if "region" in location_info.keys():
                temp_dict["region"] = location_info["region"].lower().replace(" ","-")
            else:
                temp_dict["region"] = None

            if "province" in location_info.keys():
                temp_dict["province"] = location_info["province"].lower().replace(" ","-")
            else:
                temp_dict["province"] = None

            if "provinceId" in location_info.keys():
                temp_dict["province_code"] = location_info["provinceId"]
            else:
                temp_dict["province_code"] = None

            if "city" in location_info.keys():
                temp_dict["city"] = location_info["city"]
            else:
                temp_dict["city"] = None

            if "macrozone" in location_info.keys():
                temp_dict["macrozone"] = location_info["macrozone"]
            else:
                temp_dict["macrozone"] = None

            if "microzone" in location_info.keys():
                temp_dict["microzone"] = location_info["microzone"]
            else:
                temp_dict["microzone"] = None

            if "locality" in location_info.keys():
                temp_dict["locality"] = location_info["locality"]
            else:
                temp_dict["locality"] = None

            if "address" in location_info.keys():
                temp_dict["address"] = location_info["address"]
            else:
                temp_dict["address"] = None

            if "streetNumber" in location_info.keys():
                temp_dict["street_number"] = location_info["streetNumber"]
            else:
                temp_dict["street_number"] = None

            location_info = temp_dict
    else:
        location_info = None

    # "has_elevator": if the property is accessible via elevator
    if 'elevator' in prop_deeper_keys:
        has_elevator = properties_deeper["elevator"]
    else:
        has_elevator = None

    #Let's now look into the properties of the building where the listed property is

    # "building_usage": how the building is used (e.g., residential, office, commercial, industrial etc.)
    if 'buildingUsage' in prop_deeper_keys:
        building_usage = properties_deeper["buildingUsage"]
        if building_usage is None:
            pass
        else:
            building_usage = building_usage["value"]
    else:
        building_usage = None

    # "building_year": year when the building was built
    if 'buildingYear' in prop_deeper_keys:
        building_year = properties_deeper["buildingYear"]
        if building_year is None:
            pass
        else:
            building_year = int(building_year)
    else:
        building_year = None

    # "total_building_floors": total number of floors in the building
    if 'floors' in prop_deeper_keys:
        total_building_floors = properties_deeper["floors"]
        if total_building_floors is None:
            pass
        else:
            total_building_floors = int(remove_non_numbers(total_building_floors))
    else:
        total_building_floors = None

    # "total_number_of_residential_units": total number of residential units in the building
    if 'residentialUnits' in prop_deeper_keys:
        total_number_of_residential_units = properties_deeper["residentialUnits"]
    else:
        total_number_of_residential_units = None

    # "work_start_date" and "work_end_date": start and end of construction works (only for new/planned buildings)
    if 'workDates' in prop_deeper_keys:
        dates = properties_deeper["workDates"].replace(" ","").split("-")
        if len(dates) == 1:
            work_start_date = dates[0].split("/")
            work_start_date = work_start_date[-1]+"-"+work_start_date[-2]+"-"+work_start_date[-3]
            work_end_date = None
        else:
            work_start_date = dates[0].split("/")
            work_start_date = work_start_date[-1]+"-"+work_start_date[-2]+"-"+work_start_date[-3]
            work_end_date = dates[1].split("/")
            work_end_date = work_end_date[-1]+"-"+work_end_date[-2]+"-"+work_end_date[-3]
    else:
        work_start_date = None
        work_end_date = None

    # "work_completion": we use work_start_date, work_end_date and the scraping date to
    # calculate how far along the construction work is, with 0 corresponding to the
    # work's starting date and 1 to the finishing date. Negative values mean that
    # the construction works have not started yet
    if (work_start_date is not None) and (work_end_date is not None):
        days_from_start = parser.parse(last_date_scraped, dayfirst=True)-parser.parse(work_start_date, dayfirst=True)
        total_work_duration = parser.parse(work_end_date, dayfirst=True)-parser.parse(work_start_date, dayfirst=True)
        try:
            work_completion = days_from_start/total_work_duration
        except:
            work_completion = None
    else:
        work_completion = None

    # "work_progress": progress of construction work and projected end (only for new/planned buildings)
    if 'workProgress' in prop_deeper_keys:
        work_progress = properties_deeper["workProgress"]
    else:
        work_progress = None

    #Let us now look at the textual descriptions of the listed property

    # "title": title of the text description
    if 'title' in prop_gen_keys:
        title = properties_general["title"]
    else:
        title = None

    # "caption": caption of the text description
    if 'caption' in prop_deeper_keys:
        caption = properties_deeper["caption"]
    else:
        caption = None

    # "description": full text description of the listing
    if 'description' in prop_deeper_keys:
        description = properties_deeper["description"]
    else:
        description = None

    # Puts all the information into one dictionary
    features = {
                    "price":price,"total_surface":total_surface,"price_per_sq_mt":price_per_sq_mt,
                    "listing_info":{"listing_age":listing_age,"listing_last_update":listing_last_update,"is_listing_new":is_listing_new,"has_parent":has_parent,"has_child":has_child,"seller_type":seller_type,"number_of_photos":number_of_photos},
                    "additional_costs":{"other_costs":additional_costs,"mortgage_rate":mortgage_rate},
                    "auction_info":auction_info,
                    "cadastral_info":cadastral_info,
                    "surface_composition":surface_composition,
                    "availability":availability,
                    "type_of_property":{"class":type_of_property,"category":category,"is_luxury":is_luxury},
                    "condition":condition,
                    "rooms_info":{"bathrooms_number":bathrooms_number,"bedrooms_number":bedrooms_number,"total_room_number":total_room_number,"rooms_description":rooms_description,"kitchen_status":kitchen_status,"garage":garage,"property_floor":property_floor},
                    "additional_features":additional_features,
                    "building_info":{"has_elevator":has_elevator,"building_usage":building_usage,"building_year":building_year,"total_building_floors":total_building_floors,"total_number_of_residential_units":total_number_of_residential_units,"work_start_date":work_start_date,"work_end_date":work_end_date,"work_completion":work_completion,"work_progress":work_progress},
                    "energy_info":energy_info,
                    "location_info":location_info,
                    "text_info": {"title":title,"caption":caption,"description":description}
               }
    
    # Closes mongoDB connection
    mongoDB_client.close()

    return features

#-------------------------------------------------------------------------------

async def migrate_async(listing_type):
    '''
    This function gets the name of a listing type (i.e., sale, auction or rent),
    then it iterates over all documents in the selected collection of the mongoDB
    datalake, extracts the relevant data information and puts it in the mongoDB
    data warehouse

    Parameters:
    - listing_type: type of the listings to be migrated (rent/sale/auction)
    '''
    
    # Opens mongoDB connection
    mongoDB_client= pymongo.MongoClient("localhost", 27017)

    # Defines datalake and its collections
    datalake = mongoDB_client.immobiliare_datalake
    datalake_sale_coll= datalake.sale
    datalake_rent_coll = datalake.rent
    datalake_auction_coll = datalake.auction

    # Defines data warehouse and its collections
    warehouse = mongoDB_client.immobiliare_warehouse
    warehouse_sale_coll = warehouse.sale
    warehouse_rent_coll = warehouse.rent
    warehouse_auction_coll = warehouse.auction

    # Checks that the listing type is one of the accepted values
    valid_listing_types = ["sale","rent","auction"]
    if listing_type not in valid_listing_types:
        raise ValueError("listing_type must be one of %r." % valid_listing_types)

    logger.info(f"Initializing migration from data lake to data warehouse for {listing_type} collection")

    # Defines which collection and warehouse we are going to use based on the "listing_type" argument
    if listing_type == "sale":
        datalake_collection = datalake_sale_coll
        warehouse_collection = warehouse_sale_coll
    elif listing_type == "rent":
        datalake_collection = datalake_rent_coll
        warehouse_collection = warehouse_rent_coll
    elif listing_type == "auction":
        datalake_collection = datalake_auction_coll
        warehouse_collection = warehouse_auction_coll

    # Gets all listings' IDs from the selected collection, and counts how many documents there are
    all_ids = datalake_collection.distinct("_id")
    documents_number = len(all_ids)
    logger.info(f"There are {documents_number} documents in the {listing_type} collection")

    # Logs beginning of data migration
    logger.info(f"Migration to data warehouse started for {listing_type} collection (might take a while)")

    # This for loop does all the work, and the "counter" variable is needed to keep track of the progress
    counter = 1
    for listing_data in datalake_collection.find():
        logger.info(f"Processing listing #{counter}/{documents_number}")

        # Gets the listing's ID
        listing_id = listing_data["_id"]

        # Gets the province name/code and the last scraping date from retrieved data
        province_name = listing_data["province_name"]
        province_code = listing_data["province_code"]
        parent_listing_id = listing_data["parent_listing"]
        child_listings_id = listing_data["child_listings"]
        last_scraping_date = listing_data["data"][-1]["scraping_date"]

        # First, check if a document with this id and scraping date already exists
        existing_doc = warehouse_collection.find_one(
            {
                "_id": listing_id,
                "data.scraping_date": last_scraping_date
            }
        )

        # If this exact scraping date doesn't exist for this listing, extract and add it
        if not existing_doc:
            # Uses the function defined above to extract all relevant information
            listing_features = await extract_and_transform_data(listing_data, listing_type)

            # Checks if the document itself exists first
            doc_exists = warehouse_collection.find_one({"_id": listing_id})

            if not doc_exists:
                # Inserts a new document if it doesn't exist
                warehouse_collection.insert_one({
                    "_id": listing_id,
                    "province_name": province_name,
                    "province_code": province_code,
                    "parent_listing": parent_listing_id,
                    "child_listings": child_listings_id,
                    "data": [{
                        "scraping_date": last_scraping_date,
                        "listing_features": listing_features
                    }]
                })
            else:
                # Adds new data entry to existing document
                warehouse_collection.update_one(
                    {"_id": listing_id},
                    {
                        "$push": {
                            "data": {
                                "scraping_date": last_scraping_date,
                                "listing_features": listing_features
                            }
                        }
                    }
                )

        counter = counter + 1

    # Closes mongoDB connection
    mongoDB_client.close()
    logger.info(f"Migration to data warehouse finished for {listing_type} collection")


#-------------------------------------------------------------------------------

def migrate(listing_type):
    '''
    This function is simply a wrapper on the async function defined above. It
    is needed in order for Airflow's PythonOperator to run it correctly
    '''
    asyncio.run(migrate_async(listing_type))

#-------------------------------------------------------------------------------

def fix_empty_child_listings():
    '''
    This function is needed to do a little bit of data cleaning. In fact, some listings
    that should have null values in child_listings actually end up having empty arrays.
    This function iterates over all documents in all collections and replaces empty arrays
    in child_listings with null values
    '''

    logger.info("Starting to fix empty child_listings arrays")
    
    # Opens mongoDB connection
    mongoDB_client = pymongo.MongoClient("localhost", 27017)
    
    # Defines data warehouse and its collections
    warehouse = mongoDB_client.immobiliare_warehouse
    warehouse_sale_coll = warehouse.sale
    warehouse_rent_coll = warehouse.rent
    warehouse_auction_coll = warehouse.auction
    
    collections = {
        "sale": warehouse_sale_coll,
        "rent": warehouse_rent_coll,
        "auction": warehouse_auction_coll
    }
    
    # Counter for total number of fixed listings
    total_fixed = 0

    # Fixes empty child_listings arrays
    for coll_name, collection in collections.items():
        fixed_count = 0
        # Finds documents with empty arrays in child_listings and
        # replaces the empty arrai with a null value
        cursor = collection.find({"child_listings": []})
        for doc in cursor:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"child_listings": None}}
            )
            fixed_count += 1
        
        logger.info(f"Fixed {fixed_count} documents with empty child_listings arrays in {coll_name} collection")
        total_fixed += fixed_count
    
    # Closes mongoDB connection
    mongoDB_client.close()
    
    logger.info(f"Completed fixing empty child_listings arrays. Total fixed: {total_fixed}")
    
    return total_fixed

#-------------------------------------------------------------------------------

def get_warehouse_stats():
    '''
    This function collects and reports summary statistics for the data warehouse.
    It is executed at the very end of the DAG
    '''

    logger.info("Starting to generate data warehouse statistics")
    
    # Opens mongoDB connection
    mongoDB_client = pymongo.MongoClient("localhost", 27017)
    
    # Defines data warehouse and its collections
    warehouse = mongoDB_client.immobiliare_warehouse
    warehouse_sale_coll = warehouse.sale
    warehouse_rent_coll = warehouse.rent
    warehouse_auction_coll = warehouse.auction
    
    collections = {
        "sale": warehouse_sale_coll,
        "rent": warehouse_rent_coll,
        "auction": warehouse_auction_coll
    }
    
    collection_counts = {}
    
    for coll_name, collection in collections.items():
        # Counts the total documents in each collection
        total_docs = collection.count_documents({})
        collection_counts[coll_name] = total_docs
    
    # Calculates total document number count across all collections
    total_documents = sum(collection_counts.values())
    
    # Generates statistics report
    stats_msg = f'''
                MongoDB data warehouse statistics:
                -------------------------
                Total records: {total_documents}
                Collection breakdown:
                - Sale: {collection_counts['sale']} records
                - Rent: {collection_counts['rent']} records
                - Auction: {collection_counts['auction']} records
                    '''
    
    logger.info(stats_msg)
    
    # Closes mongoDB connection
    mongoDB_client.close()
    
    logger.info("Success! All tasks were executed successfully")
    
    return stats_msg

#-------------------------------------------------------------------------------
#-----------------------------DAG DEFINITION------------------------------------
#-------------------------------------------------------------------------------

# Defines the DAG's default arguments
def_args = {
            "owner": "Leonardo Pacciani-Mori",
            "start_date": days_ago(0),
            "max_active_tasks": 16,
            "retries": 5,
            "retry_delay": timedelta(minutes=1)
           }

# Defines the DAG
ETL_dag = DAG(
                'immobiliare.it_datalake_ETL_warehouse_MongoDB_DAG',
                 default_args = def_args,
                 description = "DAG to extract, transform and load data from mongoDB datalake to mongoDB warehouse",
                 schedule = None
             )

# Defines the tasks for each listing type
for L in ["rent","auction","sale"]:
    single_listing_type_task = PythonOperator(task_id=L+"_ETL",python_callable=migrate,op_kwargs={"listing_type":L}, dag=ETL_dag)

# Add the new task for fixing empty child_listings and generating statistics
cleaning_task = PythonOperator(
    task_id="cleaning_task",
    python_callable=fix_empty_child_listings,
    dag=ETL_dag
)

#Defines the task that prints the final success message
finalize_task = PythonOperator(
    task_id="finalize_task",
    python_callable=get_warehouse_stats,
    dag=ETL_dag
)

#Set up the task dependencies
for L in ["rent","auction","sale"]:
    ETL_dag.get_task(L+"_ETL") >> cleaning_task

cleaning_task >> finalize_task