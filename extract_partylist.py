import pdfplumber
import pdfplumber.table
import pandas as pd
import psycopg
import os
from dotenv import load_dotenv
import re

load_dotenv()

db_key = os.getenv("DB_KEY")

def extract_partylist(filename:str):
    # initialize db connection
    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    with pdfplumber.open(filename) as s_list:
        table = pd.DataFrame()

        for page in s_list.pages:
            new_table = page.find_table().extract()

            if new_table == None:
                continue

            table = pd.concat([table, pd.DataFrame(new_table)])
        
        # create cursor
        cur = db.cursor()

        # save data straight to db
        with cur.copy("COPY partylist (ballot_number, ballot_name, name) FROM STDIN") as copy:
            for partylist in table.values.tolist():
                if re.match(r'^[0-9]+$', partylist[0]) != None:
                    copy.write_row(partylist)
                
        # save senator data
        db.commit()

filename = "CLC2025_Partylist.pdf"
extract_partylist(filename)