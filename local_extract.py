
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
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

correctTitle = {
    "MEMMBEEMR,B SEARN,G SGAUNNIGANGGU PNAINALNALGA WPIAGNANLALAWIGAN": "PROVINCIAL BOARD MEMBER",
    "MEMMBEEMR,B HEORU,S HE OOUF RSEEP OREFS RENETPARTIEVESSENTATIVES": "REPRESENTATIVE",
    "PROPVRINOCVIAINL CGOIAVLE RGNOOVRERNOR": "GOVERNOR",
    "PROPVRINOCVIAINL CVIICAEL-G VOIVCEER-NGOORVERNOR": "VICE-GOVERNOR",
    "MAYMOARYOR": "MAYOR",
    "VICVEI-MCAEY-MORAYOR": "VICE-MAYOR",
    "COUCNOCUILNOCRILOR": "COUNCILOR"
}

with_provincial_board = ("LAGUNA_CITYOFCALAMBA.pdf", "CITY OF CALAMBA", "LAGUNA", "IV-A")
with_own_legislative = ("DAVAODELSUR_DAVAOCITY.pdf", "CITY OF DAVAO", "DAVAO DEL SUR", "XI")
provincial = ("LAGUNA_OPES.pdf", None, "LAGUNA", "IV-A")
basic_lgu = ("LAGUNA_LILIW.pdf", "LILIW", "LAGUNA", "IV-A")

def extract_local(file_info:tuple):
    master_list = []
    filename = file_info[0]
    lgu = file_info[1]
    province = file_info[2]
    region = file_info[3]
    district_count = 0
    legislative_count = 0
    position_count = 0
    current_position = None

    # put everything in one big list
    with pdfplumber.open(filename) as s_list:
        for page in s_list.pages:
            # get tables in page
            tables = page.find_tables()

            for table in tables:
                # attach extracted table to master list
                master_list = [*master_list, *table.extract()]

    for i in range(len(master_list)):
        row = master_list[i]

        if row[0] in correctTitle.keys():
            previous_position = current_position
            current_position = correctTitle[row[0]]
            # look ahead 2 rows
            if i+2>=len(master_list):
                break   # reached end
            
            next_row = master_list[i+2]

            if previous_position == current_position:   # e.g. treated 1st district councilors then proceeding to 2nd district
                if (next_row[0] == "1"):
                    # handle start of a new count
                    if current_position == "REPRESENTATIVE":
                        legislative_count += 1
                    elif current_position == "COUNCILOR":
                        district_count += 1
                continue
            else:   # e.g. moved from representative to mayor
                # start of new 
                position_count += 1
            
                if current_position == "REPRESENTATIVE":
                    legislative_count += 1
                elif current_position == "COUNCILOR":
                    district_count += 1

                continue # proceed to next

        if (row[0].strip() == "#"):
            # skip hashes
            continue
        
        # print(row)
        row.append(current_position)

        if current_position == "REPRESENTATIVE":
            row.append(str(legislative_count))
        elif current_position == "COUNCILOR":
            row.append(str(district_count))
        else:
            row.append(None)

        row.append(lgu)
        row.append(province)

    # iterate thru master list
    df = pd.DataFrame(master_list)

    df.columns = ["#", "BALLOT NAME", "SEX", "NAME", "POLITICAL PARTY", "POSITION", "DISTRICT", "LGU", "PROVINCE"]
    df["BALLOT NAME"] = df["BALLOT NAME"].apply(remove_line_breaks)
    df["NAME"] = df["NAME"].apply(remove_line_breaks)
    df["POLITICAL PARTY"] = df["POLITICAL PARTY"].apply(remove_line_breaks)
    df["SEX"] = df["SEX"].apply(shorten_sex)

    return (df[(df["POSITION"] == "GOVERNOR") | (df["POSITION"] == "VICE-GOVERNOR") | (df["POSITION"] == "PROVINCIAL BOARD MEMBER") | (df["POSITION"] == "REPRESENTATIVE") | (df["POSITION"] == "MAYOR") | (df["POSITION"] == "VICE-MAYOR") | (df["POSITION"] == "COUNCILOR")], district_count)

# extract_local(with_provincial_board).to_csv("calamba_city.csv")
# extract_local(with_own_legislative).to_csv("davao_city.csv")
# extract_local(provincial).to_csv("LAGUNA.csv")

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
                extracted = extract_local((f"temp/{filename}", None, province_name, region))
                
                # save data straight to db
                with cur.copy("COPY local_candidate (ballot_number, ballot_name, sex, name, partylist, position, district, lgu, province) FROM STDIN") as copy:
                    for candidate in extracted[0].values.tolist():
                        copy.write_row(candidate)
                
                # save province data to separate table
                db.execute("""
                            INSERT INTO province_summary (name, total_provincial_district, total_legislative_district, region)
                            VALUES (%s, %s, %s, %s);
                           """, (province_name, extracted[1], extracted[1], region))
                db.commit()
            else:
                extracted = extract_local((f"temp/{filename}", lgu_name, province_name, region))

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

            print(f"Successfully loaded candidates from {lgu_name}, {province_name}, Region {region}")

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

# extract from CAR, NIR and Region I-IVA
# extract_politicians_in_province(regions[1]["link"], regions[1]["region_name"])
# extract_politicians_in_province(regions[2]["link"], regions[2]["region_name"])
# extract_politicians_in_province(regions[3]["link"], regions[3]["region_name"])
# extract_politicians_in_province(regions[4]["link"], regions[4]["region_name"])
# extract_politicians_in_province(regions[5]["link"], regions[5]["region_name"])
# extract_politicians_in_province(regions[6]["link"], regions[6]["region_name"])

extract_politicians_in_province(regions[7]["link"], regions[7]["region_name"])
extract_politicians_in_province(regions[8]["link"], regions[8]["region_name"])
extract_politicians_in_province(regions[9]["link"], regions[9]["region_name"])

extract_politicians_in_province(regions[10]["link"], regions[10]["region_name"])
extract_politicians_in_province(regions[11]["link"], regions[11]["region_name"])
extract_politicians_in_province(regions[12]["link"], regions[12]["region_name"])

extract_politicians_in_province(regions[13]["link"], regions[13]["region_name"])
extract_politicians_in_province(regions[14]["link"], regions[14]["region_name"])
extract_politicians_in_province(regions[15]["link"], regions[15]["region_name"])

extract_politicians_in_province(regions[16]["link"], regions[16]["region_name"])
extract_politicians_in_province(regions[17]["link"], regions[17]["region_name"])