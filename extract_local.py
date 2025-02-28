# NOTE: rename to `extract_provincial.py` later on

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import pdfplumber
import pdfplumber.table
import re
import psycopg
from dotenv import load_dotenv

load_dotenv()

db_key = os.getenv("DB_KEY")

def remove_line_breaks(val):
    if type(val) != str:
        return val
    # remove line breaks
    val = val.replace("\n", " ")
    return val

def shorten_sex(val):
    if type(val) != str:
        return val
    # return only first letter
    return val[0]

def extract_provincial(filename:str, province:str, region:str):
    master_list = []

    with pdfplumber.open(filename) as s_list:
        for page in s_list.pages:
            # get tables in page
            tables = page.find_tables()

            for table in tables:
                master_list = [*master_list, *table.extract()]

    # return master_list
    # Precedence of extracted positions
    # 1. Governor
    # 2. Vice-Governor
    # 3. Sangguniang Panlalawigan
    # 4. District Representative
    
    position_count = 0
    district_count = 0
    curr_pos = ""
    previous = None
    provincial_district_count = 0
    legislative_district_count = 0
    for row in master_list:
        
        # use regex to detect if it's a number
        if (row[0] == "#"):
            # skip hashes
            previous = row
            continue

        if row[0].split(" ")[-1] == "WPIAGNANLALAWIGAN" and position_count < 3 and curr_pos != "PROVINCIAL_COUNCIL":
            position_count = 3 # `3` denotes that we are parsing provincial council
            district_count = 0
            curr_pos = "PROVINCIAL_COUNCIL"
        elif row[0].split(" ")[-1] == "RENETPARTIEVESSENTATIVES" and position_count < 4 and curr_pos != "DISTRICT_REPRESENTATIVE":
            position_count = 4 # `4` denotes that we are parsing district representatives
            provincial_district_count = district_count
            district_count = 0
            curr_pos = "DISTRICT_REPRESENTATIVE"

        if (re.match(r'^[0-9]+$', row[0]) == None):
            # No match found
            previous = row
            continue

        if previous[0] == "#" and row[0] == "1" and position_count < 2:
            position_count += 1
        elif previous[0] == "#" and row[0] == "1" and position_count >= 2:
            district_count += 1
        
        if position_count == 1:
            row.append("GOVERNOR")
            row.append(None)
        elif position_count == 2:
            row.append("VICE-GOVERNOR")
            row.append(None)
        elif position_count == 3:
            row.append("PROVINCIAL_COUNCIL")
            row.append(str(district_count))
        elif position_count == 4:
            row.append("DISTRICT_REPRESENTATIVE")
            row.append(str(district_count))
        
        previous = row
            
    legislative_district_count = district_count
    master_list = pd.DataFrame(master_list)

    master_list.columns = ["#", "BALLOT NAME", "SEX", "NAME", "POLITICAL PARTY", "POSITION", "DISTRICT"]

    master_list["BALLOT NAME"] = master_list["BALLOT NAME"].apply(remove_line_breaks)
    master_list["SEX"] = master_list["SEX"].apply(shorten_sex)
    master_list["NAME"] = master_list["NAME"].apply(remove_line_breaks)
    master_list["POLITICAL PARTY"] = master_list["POLITICAL PARTY"].apply(remove_line_breaks)
    master_list["LGU"] = None
    master_list["PROVINCE"] = province
    
    return (master_list[(master_list["POSITION"] == "GOVERNOR") | (master_list["POSITION"] == "VICE-GOVERNOR") | (master_list["POSITION"] == "PROVINCIAL_COUNCIL") | (master_list["POSITION"] == "DISTRICT_REPRESENTATIVE")], provincial_district_count, legislative_district_count)


def extract_local(filename:str, lgu:str, province:str, region:str):
    master_list = []

    with pdfplumber.open(filename) as s_list:
        for page in s_list.pages:
            # get tables in page
            tables = page.find_tables()

            for table in tables:
                master_list = [*master_list, *table.extract()]
    # Precedence of extracted positions
    # 1. Mayor
    # 2. Vice-Mayor
    # 3. Councilors per district
    position_count = 0
    district_count = 0
    previous = None
    for row in master_list:
        # use regex to detect if it's a number
        if (row[0] == "#"):
            # skip hashes
            previous = row
            continue
        
        if (re.match(r'^[0-9]+$', row[0]) == None):
            # No match found, increment position_count
            # position_count += 1
            previous = row
            continue

        if previous[0] == "#" and row[0] == "1":
            position_count += 1
        
        if position_count == 1:
            row.append("MAYOR")
            row.append(None)
        elif position_count == 2:
            row.append("VICE-MAYOR")
            row.append(None)
        else:
            row.append(f"COUNCILOR")
            
            row.append(str(position_count-2))
            district_count = position_count - 2 # can be optimized
        
        row.append("")
            
    master_list = pd.DataFrame(master_list)

    # fix column naming
    master_list.columns = ["#", "BALLOT NAME", "SEX", "NAME", "POLITICAL PARTY", "POSITION", "DISTRICT", "LGU"]

    master_list["BALLOT NAME"] = master_list["BALLOT NAME"].apply(remove_line_breaks)
    master_list["NAME"] = master_list["NAME"].apply(remove_line_breaks)
    master_list["POLITICAL PARTY"] = master_list["POLITICAL PARTY"].apply(remove_line_breaks)
    master_list["LGU"] = lgu
    master_list["PROVINCE"] = province
    master_list["SEX"] = master_list["SEX"].apply(shorten_sex)
    return (master_list[(master_list["POSITION"] == "MAYOR") | (master_list["POSITION"] == "VICE-MAYOR") | (master_list["POSITION"] == "COUNCILOR")], district_count)

download_path = os.getcwd() + "/temp"
options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    'profile.default_content_setting_values.automatic_downloads': 1
})
driver = webdriver.Chrome(options=options)


def extract_politicians_in_province(link:str, region:str) -> pd.DataFrame|None:
    
    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    # open source page
    driver.get(link)

    # wait for site to load (3s)
    time.sleep(3)

    # get accordions that contain provincial data
    root = driver.find_element(by=By.ID, value="accordionFlushExample")
    province_divs = root.find_elements(by=By.CLASS_NAME, value="accordion-item")
    province_count = len(province_divs)
    
    i=0
    for i in range(province_count):
        lgus_wrapper_xpath = f"/html/body/div[3]/div[2]/div/div[2]/div/div[1]/div/div/div/div[{i+1}]/div/div/ul"
        province_name_wrapper = f"/html/body/div[3]/div[2]/div/div[2]/div/div[1]/div/div/div/div[{i+1}]/h2/button"

        # get links of lgus
        lgus = driver.find_element(by=By.XPATH, value=lgus_wrapper_xpath).find_elements(by=By.TAG_NAME, value="li")
        province_name = driver.find_element(by=By.XPATH, value=f"{province_name_wrapper}/span").get_attribute("innerHTML")

        # open accordion
        accordion_header = driver.find_element(by=By.XPATH, value=province_name_wrapper)
        driver.execute_script("arguments[0].click()", accordion_header)

        # extract one by one
        j = 0
        for j in range(len(lgus)):

            # get file link
            lgu_link = lgus[j].find_element(by=By.TAG_NAME, value="a")

            # get lgu name
            lgu_name = lgu_link.find_element(by=By.TAG_NAME, value="span").get_attribute("innerHTML")
            
            filename = f"{province_name}_{lgu_name}.pdf"

            # get file
            try: 
                driver.execute_script(f"arguments[0].setAttribute('download', '{filename}');", lgu_link)
            except:
                print(f"arguments[0].setAttribute('download', '{filename}');")
                return

            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((lgu_link)))
            # lgu_link.click()
            driver.execute_script("arguments[0].click()", lgu_link)

            # wait for download to finish
            time.sleep(3)

            cur = db.cursor()

            # extract data
            if (lgu_name == "PROVINCIAL POSITIONS"):
                # deal accordingly as provincial position
                extracted = extract_provincial(f"temp/{filename}", province_name, region)
                
                # save data straight to db
                with cur.copy("COPY local_candidate (ballot_number, ballot_name, sex, name, partylist, position, district, lgu, province) FROM STDIN") as copy:
                    for candidate in extracted[0].values.tolist():
                        copy.write_row(candidate)
                
                # save province data to separate table
                db.execute("""
                            INSERT INTO province_summary (name, total_provincial_district, total_legislative_district, region)
                            VALUES (%s, %s, %s, %s);
                           """, (province_name, extracted[1], extracted[2], region))
                db.commit()
            else:
                extracted = extract_local(f"temp/{filename}", lgu_name, province_name, region)

                # save data straight to db
                with cur.copy("COPY local_candidate (ballot_number, ballot_name, sex, name, partylist, position, district, lgu, province) FROM STDIN") as copy:
                    for candidate in extracted[0].values.tolist():
                        copy.write_row(candidate)
                
                # save lgu data to separate table
                db.execute("""
                            INSERT INTO lgu_summary (name, province_name, region, total_districts)
                            VALUES (%s, %s, %s, %s);
                           """, (lgu_name, province_name, region, extracted[1]))
                db.commit()

            os.remove(f"temp/{filename}")

    return None

regions = ({"region_name": "NCR", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_NCR"},
    {"region_name": "CAR", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_CAR"},
    {"region_name": "NIR", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_NIR"},
    {"region_name": "I", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R1"},
    {"region_name": "II", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R2"},
    {"region_name": "III", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R3"},
    {"region_name": "IV-A", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R4A"},
    {"region_name": "IV-B", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R4B"},
    {"region_name": "V", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R5"},
    {"region_name": "VI", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R6"},
    {"region_name": "VII", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R7"},
    {"region_name": "VIII", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R8"},
    {"region_name": "IX", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R9"},
    {"region_name": "X", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R10"},
    {"region_name": "XI", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R11"},
    {"region_name": "XII", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R12"},
    {"region_name": "CARAGA", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_R13"},
    {"region_name": "BARMM", "link": "https://comelec.gov.ph/?r=2025NLE/CLC2025/CLC_BARMM"})

# extract and load candidates from CAR, NIR and Region I
# extract_politicians_in_province(regions[1]["link"], regions[1]["region_name"])
# extract_politicians_in_province(regions[2]["link"], regions[2]["region_name"])
# extract_politicians_in_province(regions[3]["link"], regions[3]["region_name"])

# extract_politicians_in_province(regions[4]["link"], regions[4]["region_name"])
# extract_politicians_in_province(regions[5]["link"], regions[5]["region_name"])
# extract_politicians_in_province(regions[6]["link"], regions[6]["region_name"])

# extract_politicians_in_province(regions[7]["link"], regions[7]["region_name"])
# extract_politicians_in_province(regions[8]["link"], regions[8]["region_name"])
# extract_politicians_in_province(regions[9]["link"], regions[9]["region_name"])

# extract_politicians_in_province(regions[10]["link"], regions[10]["region_name"])
# extract_politicians_in_province(regions[11]["link"], regions[11]["region_name"])
# extract_politicians_in_province(regions[12]["link"], regions[12]["region_name"])

# extract_politicians_in_province(regions[13]["link"], regions[13]["region_name"])
# extract_politicians_in_province(regions[14]["link"], regions[14]["region_name"])
# extract_politicians_in_province(regions[15]["link"], regions[15]["region_name"])

# extract_politicians_in_province(regions[16]["link"], regions[16]["region_name"])
# extract_politicians_in_province(regions[17]["link"], regions[17]["region_name"])