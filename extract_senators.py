import pdfplumber
import pdfplumber.table
import pandas as pd

filename = "CLC2025_Senator.pdf"

with pdfplumber.open(filename) as s_list:
    print(len(s_list.pages))

    table = pd.DataFrame()

    for page in s_list.pages:
        new_table = page.find_table().extract()

        if new_table == None:
            continue

        table = pd.concat([table, pd.DataFrame(new_table)])
    
    table.to_csv("senators.csv", index=False)