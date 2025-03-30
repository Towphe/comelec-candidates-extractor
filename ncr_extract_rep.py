
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

def extract_ncr_rep(filename: str):
    master_list = []
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

        row.append("lgu")
        row.append(None)

    # iterate thru master list
    df = pd.DataFrame(master_list)

    df.columns = ["#", "BALLOT NAME", "SEX", "NAME", "POLITICAL PARTY", "POSITION", "DISTRICT", "LGU", "PROVINCE"]
    df["BALLOT NAME"] = df["BALLOT NAME"].apply(remove_line_breaks)
    df["NAME"] = df["NAME"].apply(remove_line_breaks)
    df["POLITICAL PARTY"] = df["POLITICAL PARTY"].apply(remove_line_breaks)
    df["SEX"] = df["SEX"].apply(shorten_sex)

    return (df[(df["POSITION"] == "GOVERNOR") | (df["POSITION"] == "VICE-GOVERNOR") | (df["POSITION"] == "PROVINCIAL BOARD MEMBER") | (df["POSITION"] == "REPRESENTATIVE") | (df["POSITION"] == "MAYOR") | (df["POSITION"] == "VICE-MAYOR") | (df["POSITION"] == "COUNCILOR")], district_count, legislative_count)

filename = "temp/NCR_HOUSE OF REPRESENTATIVES.pdf"

extracted = extract_ncr_rep(filename)

extracted[0].to_csv("ncr_rep.csv", index=False)