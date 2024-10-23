import pandas as pd
import pymysql

# Connect to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='mens_wearhouse_db'
)

query = f"""
SELECT `Store No.`,`Name` ,`Latitude`, `Longitude`,`Street`,`City`,`Country`,`Zip_Code`,`Address`,`Phone`,`Open_Hour`,`URL`,
`Email`,`Provider`,`Banner`, `Updated_Date`,`Status`,`Direction_URL` FROM store_details
"""

df = pd.read_sql(query, conn)

# Close the database connection
conn.close()

output_file_path="men's wearhouse.xlsx"
df.to_excel(output_file_path, index=False)

print(f"Data has been exported to {output_file_path}")
