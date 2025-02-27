import pdfplumber
import pdfplumber.table
import pandas as pd

filename = "CLC2025_Partylist.pdf"

with pdfplumber.open(filename) as pl_list:
    print(len(pl_list.pages))

    table = pd.DataFrame()

    for page in pl_list.pages:
        new_table = page.find_table().extract()

        if new_table == None:
            continue

        table = pd.concat([table, pd.DataFrame(new_table)])
    
    table.to_csv("partylists.csv", index=False)