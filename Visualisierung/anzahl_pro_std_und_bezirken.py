import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def connect_to_database():
    # Verbindung zur Datenbank herstellen
    conn = psycopg2.connect(database="fahrraddiebstahlinberlin", user='postgres', password='015775570018', host='127.0.0.1', port='5432')
    return conn

def execute_query_and_get_dataframe(conn, query):
    # Cursor erstellen
    cur = conn.cursor()

    # Abfrage ausführen
    cur.execute(query)

    # Ergebnisse abrufen und in DataFrame laden
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=['bezirk', 'stunde', 'anzahl_fahrraeder'])

    # Cursor und Verbindung schließen
    cur.close()
    return df

def create_heatmap(df):
    # Pivot-Tabelle erstellen
    pivot_table = df.pivot_table(index='bezirk', columns='stunde', values='anzahl_fahrraeder', fill_value=0)

    # Heatmap erstellen
    plt.figure(figsize=(12, 6))
    sns.heatmap(pivot_table, cmap='YlGnBu')
    plt.xlabel('Stunde')
    plt.ylabel('Bezirken')
    plt.title('Anzahl gestohlener Fahrräder pro Stunde in Bezirken')
    # Speichere das Diagramm als PNG-Datei
    plt.savefig('Anzahl_pro_std_in_Bezirken', dpi=300)
    plt.show()

if __name__ == '__main__':
    query = '''
    SELECT b.gemeinde_name, fd.tatzeit_anfang_stunde % 24 AS stunde, COUNT(*) AS anzahl_fahrraeder
    FROM bezirksgrenz b
    JOIN lor_planung lp ON b.gemeinde_schluessel = lp.bez
    JOIN fahrraddieb fd ON lp.plr_id = fd.lor
    GROUP BY b.gemeinde_name, stunde
    ORDER BY b.gemeinde_name, stunde;
    '''

    conn = connect_to_database()
    df = execute_query_and_get_dataframe(conn, query)
    conn.close()

    create_heatmap(df)
