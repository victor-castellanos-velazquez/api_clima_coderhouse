import subprocess
import sys
import os
import psycopg2
import pandas as pd
import requests
from translate import Translator
import unidecode

def install_package(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except subprocess.CalledProcessError:
        pass

# Lista de paquetes a instalar
packages = [
    "pandas", 
    "unidecode", 
    "requests", 
    "sqlalchemy", 
    "psycopg2-binary", 
    "beautifulsoup4", 
    "tqdm",
    "pymysql",
    "redshift_connector",
    "boto3",
    "translate"
]

# Instalar cada paquete de la lista
for package in packages:
    install_package(package)

# Añadir la ruta donde está el archivo redshift_credenciales.py
sys.path.append('C:/Users/victo/Cursos/Coderhouse/Proyecto final/api_clima')  # Reemplaza con la ruta correcta

# Intentar cargar las variables desde redshift_credenciales.py
try:
    from redshift_credenciales import r_user, r_password, r_host, r_port, r_dbname, api_key
except ImportError as e:
    pass

# Conectar a la base de datos para consultar la tabla de pronósticos y datos de sequía
with psycopg2.connect(
    dbname=r_dbname,
    user=r_user,
    password=r_password,
    host=r_host,
    port=r_port
) as conn:
    # Consultar la tabla de pronósticos
    query = "SELECT * FROM pronosticos_clima"
    df_pronosticos_clima = pd.read_sql_query(query, conn)

    # Consultar la tabla de datos de sequía
    query = "SELECT cve_mun, nombre_mun, cve_ent, entidad, fecha, valor FROM sequia"
    df_sequia = pd.read_sql_query(query, conn)
    df_sequia['fecha'] = pd.to_datetime(df_sequia['fecha']).dt.date

# Crear un diccionario para mapear 'cve_mun' a su 'indicador_sequia' más reciente
df_sequia_max_fecha = df_sequia.loc[df_sequia.groupby('cve_mun')['fecha'].idxmax()]
sequia_dict = df_sequia_max_fecha.set_index('cve_mun')['valor'].to_dict()

# Rellenar la columna 'indicador_sequia' en 'df_pronosticos_clima' usando el diccionario
df_pronosticos_clima['indicador_sequia'] = df_pronosticos_clima['cve_mun'].map(sequia_dict)

# Verificar los datos finales
print(df_pronosticos_clima.head(2))

# Conectar a la base de datos para actualizar los valores de 'indicador_sequia'
with psycopg2.connect(
    dbname=r_dbname,
    user=r_user,
    password=r_password,
    host=r_host,
    port=r_port
) as conn:
    cursor = conn.cursor()
    for index, row in df_pronosticos_clima.iterrows():
        update_query = """
        UPDATE pronosticos_clima
        SET indicador_sequia = %s
        WHERE cve_mun = %s AND fecha_pronostico = %s
        """
        cursor.execute(update_query, (row['indicador_sequia'], row['cve_mun'], row['fecha_pronostico']))
    conn.commit()

print("Actualización de la tabla pronósticos_clima completada.")