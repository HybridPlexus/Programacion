import pandas as pd
import pyodbc
from datetime import datetime


def process_netflix_data(csv_file_path, server_name, database_name=None):
    try:
        df = pd.read_csv("C:\\Users\\hp\\PycharmProjects\\PythonProject1\\Dataset\\netflix_titles.csv")
        print("Archivo CSV cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el archivo CSV: {e}")
        return None, None

    df['date_added'] = pd.to_datetime(df['date_added'], format='%B %d, %Y', errors='coerce')

    df['duration_min'] = df['duration'].apply(lambda x: int(x.split()[0]) if 'min' in x else None)
    df['duration_seasons'] = df['duration'].apply(lambda x: int(x.split()[0]) if 'Season' in x else None)

    if database_name:
        try:
            conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str, autocommit=True)
            cursor = conn.cursor()

            cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'")
            if not cursor.fetchone():
                print(f"La base de datos '{database_name}' no existe. Creándola...")
                cursor.execute(f"CREATE DATABASE {database_name}")
                print(f"Base de datos '{database_name}' creada exitosamente.")

            conn.close()
            conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'netflix_titles')
            CREATE TABLE netflix_titles (
                show_id VARCHAR(10) PRIMARY KEY,
                type VARCHAR(20),
                title VARCHAR(100),
                director VARCHAR(100),
                cast VARCHAR(MAX),
                country VARCHAR(100),
                date_added DATE,
                release_year INT,
                rating VARCHAR(10),
                duration VARCHAR(20),
                listed_in VARCHAR(100),
                description VARCHAR(MAX),
                duration_min INT,
                duration_seasons INT
            )
            """)

            cursor.execute("DELETE FROM netflix_titles")
            conn.commit()

            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO netflix_titles 
                (show_id, type, title, director, cast, country, date_added, release_year, 
                 rating, duration, listed_in, description, duration_min, duration_seasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                               row['show_id'], row['type'], row['title'], row['director'], row['cast'],
                               row['country'], row['date_added'], row['release_year'], row['rating'],
                               row['duration'], row['listed_in'], row['description'],
                               row['duration_min'], row['duration_seasons'])

            conn.commit()
            print("Datos almacenados en SQL Server exitosamente.")

            query = "SELECT * FROM netflix_titles"
            df_from_db = pd.read_sql(query, conn)
            print("Datos recuperados de SQL Server exitosamente.")

        except Exception as e:
            print(f"Error al conectar con SQL Server: {e}")
            print("Continuando con el DataFrame original para el análisis...")
            df_from_db = df.copy()
        finally:
            if 'conn' in locals():
                conn.close()
    else:
        print("No se proporcionó nombre de base de datos. Continuando con el DataFrame original para el análisis...")
        df_from_db = df.copy()


    df_countries = df_from_db.assign(country=df_from_db['country'].str.split(', ')).explode('country')
    titles_per_country = df_countries['country'].value_counts().reset_index()
    titles_per_country.columns = ['country', 'title_count']

    avg_duration = df_from_db[df_from_db['type'] == 'Movie'].groupby('country')['duration_min'].mean().reset_index()
    avg_duration.columns = ['country', 'avg_duration_min']
    avg_duration['avg_duration_min'] = avg_duration['avg_duration_min'].round(2)

    return titles_per_country, avg_duration



if __name__ == "__main__":
    csv_path = "netflix_titles.csv"
    server = "DESKTOP-OCJKB3R"

    count_df, duration_df = process_netflix_data(csv_path, server)

    print("\nConteo de títulos por país:")
    print(count_df)

    print("\nDuración promedio de películas por país (minutos):")
    print(duration_df)