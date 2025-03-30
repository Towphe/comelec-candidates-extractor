import pdfplumber
import pdfplumber.table
import pandas as pd
import psycopg
import os
from dotenv import load_dotenv
import re
import time

load_dotenv()

db_key = os.getenv("DB_KEY")

print(db_key)

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

db_key = os.getenv("DB_KEY")

files = [
    ("CITY OF BAGUIO", "BENGUET", "CAR", 2, "BENGUET_BAGUIOCITY.pdf"),
    ("CITY OF BIÑAN", "LAGUNA", "IV-A", 2, "LAGUNA_CITYOFBINAN.pdf"),
    ("CITY OF CALAMBA", "LAGUNA", "IV-A", 2, "LAGUNA_CITYOFCALAMBA.pdf"),
    # ("CITY OF DASMARIÑAS", "CAVITE", "IV-A", 2, "CAVITE_DASMARINASCITY.pdf"),
    ("CITY OF SAN JOSE DEL MONTE", "BULACAN", "III", 2, "BULACAN_CityofSanJoseDelMonte.pdf"),
    ("CITY OF SANTA ROSA", "LAGUNA", "IV-A", 2, "LAGUNA_CITYOFSANTAROSA.pdf"),
    # ("CITY OF SANTIAGO", "ISABELA", "II", 2, "ISABELA_SANTIAGOCITY.pdf"),
    ("CITY OF BACOLOD", "NEGROS OCCIDENTAL", "VI", 2, "NEGOCC_BACOLODCITY.pdf"),
    ("CITY OF LAPU-LAPU", "CEBU", "VII", 2, "CEBU_LAPULAPUCITY.pdf"),
    ("CITY OF MANDAUE", "CEBU", "VII", 2, "CEBU_MANDAUECITY.pdf"),
    # ("CITY OF TACLOBAN", "LEYTE", "VIII", 2, "LEYTE_CITYOFTACLOBAN.pdf"),
    ("CITY OF BUTUAN", "AGUSAN DEL NORTE", "XIII", 2, "AGUSANDELNORTE_CITYOFBUTUAN.pdf"),
    ("CITY OF CAGAYAN DE ORO", "MISAMIS ORIENTAL", "X", 2, "MISOR_CDOCITY.pdf"),
    ("CITY OF DAVAO", "DAVAO DEL SUR", "XI", 2, "DAVAODELSUR_DAVAOCITY.pdf"),
    # ("CITY OF GENERAL SANTOS", "SOUTH COTABATO", "XII", 2, "COTABATO_CITY.pdf"),
    ("CITY OF ILIGAN", "LANAO DEL NORTE", "X", 2, "LDN_ILIGANCITY.pdf"),
    ("CITY OF ZAMBOANGA", "ZAMBOANGA DEL SUR", "IX", 2, "ZAMBOANGADELSUR_ZAMBOANGACITY.pdf"),
]

def extract_independent_cities(filename:str, lgu:str, province:str, region:str):
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
    print("Here")
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
            row.append("PROVINCIAL REPRESENTATIVE")
            row.append(None)
        elif position_count == 2:
            row.append("LEGISLATIVE REPRESENTATIVE")
            row.append(None)
        elif position_count == 3:
            row.append("MAYOR")
            row.append(None)
        elif position_count == 4:
            row.append("VICE-MAYOR")
            row.append(None)
        else:
            row.append(f"COUNCILOR")
            row.append(str(position_count-4))
            district_count = position_count - 4 # can be optimized later on
        
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

    print(district_count)

    return (master_list[(master_list["POSITION"] == "PROVINCIAL REPRESENTATIVE") | ( master_list["POSITION"] == "LEGISLATIVE REPRESENTATIVE") | (master_list["POSITION"] == "MAYOR") | (master_list["POSITION"] == "VICE-MAYOR") | (master_list["POSITION"] == "COUNCILOR")], district_count)

def extract_local_candidates_in_independent_cities_in_province() -> pd.DataFrame|None:
    
    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)
    
    # files = os.listdir("./lone-district-cities")

    i=0
    for i in range(len(files)):
        cur = db.cursor()
        fileInfo = files[i]

        extracted = extract_independent_cities(f"./lone-district-cities/{fileInfo[4]}", fileInfo[0], fileInfo[1], fileInfo[2])

        # # save data straight to db
        with cur.copy("COPY local_candidate (ballot_number, ballot_name, sex, name, partylist, position, district, lgu, province) FROM STDIN") as copy:
            for candidate in extracted[0].values.tolist():
                copy.write_row(candidate)
            
            # time.sleep(2)

            # save lgu data to separate table
        db.execute("""
                    INSERT INTO lgu_summary (name, province_name, region, total_districts, is_lone_district)
                    VALUES (%s, %s, %s, %s, TRUE);
                    """, (fileInfo[0], fileInfo[1], fileInfo[2], extracted[1]))
        db.commit()

    return None

extract_local_candidates_in_independent_cities_in_province()