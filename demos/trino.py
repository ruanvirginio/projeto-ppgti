
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    host="https://trino.ifpbapps.online",
    port=443,
    user="trino",
    auth=BasicAuthentication("trino", "iGejndrYQkbWMHOcej7MWHoAJA7myZ6VFCgiXlfwC2bb8/8sRr3haRJk"),
    catalog="indimap",
    schema="data",
)
cur = conn.cursor()
cur.execute("SELECT AVG(CAST(frp as DECIMAL)) FROM indimap.data.focos_modis")
rows = cur.fetchall()
for row in rows:
    print(row[0])