import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Verbindung zur Datenbank herstellen
conn = psycopg2.connect(database="fahrraddiebstahlinberlin", user='postgres', password='015775570018', host='127.0.0.1', port='5432')

# SQLAlchemy-Verbindung erstellen
engine = create_engine('postgresql://postgres:015775570018@localhost:5432/fahrraddiebstahlinberlin')

# Abfrage ausführen und Ergebnisse in DataFrame laden
query = '''
SELECT b.gemeinde_name, SUM(fd.schadenshoehe) AS schadenhoehe
FROM bezirksgrenz b
JOIN lor_planung lp ON b.gemeinde_schluessel = lp.bez
JOIN fahrraddieb fd ON lp.plr_id = fd.lor
GROUP BY b.gemeinde_name
ORDER BY schadenhoehe DESC;
'''

df = pd.read_sql_query(query, engine)

# Verbindung schließen
conn.close()

# Laden die GeoJSON-Datei mit den geografischen Grenzen der Bezirke in ein GeoDataFrame
gdf = gpd.read_file("bezirksgrenzen.geojson")

# Leerzeichen am Anfang oder Ende der Werte entfernen und in beiden DataFrames speichern
gdf['Gemeinde_name'] = gdf['Gemeinde_name'].str.strip()
df['gemeinde_name'] = df['gemeinde_name'].str.strip()

# Führen Sie einen Join zwischen dem DataFrame mit den Schadenhöhen und dem GeoDataFrame der Bezirke durch
merged_gdf = gdf.merge(df, left_on="Gemeinde_name", right_on="gemeinde_name", how="left")

# Plotten Sie die Karte mit den farbigen Bezirken entsprechend der Schadenhöhe
fig, ax = plt.subplots(figsize=(12, 8))
merged_gdf.plot(column='schadenhoehe', cmap='Reds', linewidth=0.8, ax=ax, edgecolor='0.8', legend=True)
plt.title('Schadenhöhe nach Bezirk')
plt.axis('off')
# Speichere das Diagramm als PNG-Datei
plt.savefig('Schadenhöhe_nach_Bezirk.png', dpi=300)
plt.show()
