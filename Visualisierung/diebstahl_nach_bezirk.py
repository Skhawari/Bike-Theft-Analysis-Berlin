import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

def connect_to_database():
    # Verbindung zur Datenbank herstellen
    conn = psycopg2.connect(database="fahrraddiebstahlinberlin", user='postgres', password='015775570018', host='127.0.0.1', port='5432')
    return conn

def execute_query_and_get_dataframe(engine, query):
    # Abfrage ausführen und Ergebnisse in DataFrame laden
    df = pd.read_sql_query(query, engine)
    return df

def plot_map(gdf, df):
    # Leerzeichen am Anfang oder Ende der Werte entfernen und in beiden DataFrames speichern
    gdf['Gemeinde_name'] = gdf['Gemeinde_name'].str.strip()
    df['gemeinde_name'] = df['gemeinde_name'].str.strip()

    # Führen Sie einen Join zwischen dem DataFrame mit den Fahrraddiebstahl-Daten und dem GeoDataFrame der Bezirke durch
    merged_gdf = gdf.merge(df, left_on="Gemeinde_name", right_on="gemeinde_name", how="left")

    # Plotten Sie die Karte mit den farbigen Bezirken entsprechend der Anzahl der gestohlenen Fahrräder
    fig, ax = plt.subplots(figsize=(12, 8))
    merged_gdf.plot(column='fahrrad_count', cmap='Reds', linewidth=0.8, ax=ax, edgecolor='0.8', legend=True)
    plt.title('Fahrraddiebstähle nach Bezirk')
    plt.axis('off')
    # Speichere das Diagramm als PNG-Datei
    plt.savefig('Fahrraddiebstähle_nach_Bezirk', dpi=300)
    plt.show()

if __name__ == '__main__':
    conn = connect_to_database()
    engine = create_engine('postgresql://postgres:015775570018@localhost:5432/fahrraddiebstahlinberlin')

    query = '''
    SELECT b.gemeinde_name, COUNT(*) AS fahrrad_count
    FROM bezirksgrenz b
    JOIN lor_planung lp ON b.gemeinde_schluessel = lp.bez
    JOIN fahrraddieb fd ON lp.plr_id = fd.lor
    GROUP BY b.gemeinde_name
    ORDER BY fahrrad_count DESC;
    '''

    df = execute_query_and_get_dataframe(engine, query)
    conn.close()

    gdf = gpd.read_file("bezirksgrenzen.geojson")

    plot_map(gdf, df)
