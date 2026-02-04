import duckdb
import os
import math

os.chdir("../data/lake/")

tab = duckdb.read_parquet("part-00000-4567c5c2-6767-412f-9ddc-41a3f7820cff-c000.snappy.parquet")

tab.show()
for i in range(len(tab.columns)):
    print(tab.description[i][0], tab.description[i][1])

'''
con = duckdb.connect()
result = con.execute("""
    SELECT lineplanningnumber, journeynumber, COUNT(vehiclenumber) as active_buses, rd_x, rd_y,  straight_line_distance
    FROM '*.parquet'
    WHERE lineplanningnumber IS NOT NULL
    GROUP BY lineplanningnumber, journeynumber, rd_x, rd_y, straight_line_distance
""").fetchdf()
'''
