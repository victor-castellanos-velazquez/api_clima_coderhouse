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

# Conectando con la base de datos de municipios
with psycopg2.connect(
    dbname=r_dbname,
    user=r_user,
    password=r_password,
    host=r_host,
    port=r_port
) as conn:
    # Consultar la base de datos para obtener los municipios
    query = "SELECT cve_mun, nom_mun, lat_decimal, lon_decimal FROM municipios"
    df_municipios = pd.read_sql_query(query, conn)

# Redondear las coordenadas decimales
df_municipios['lat_decimal'] = df_municipios['lat_decimal'].round(2)
df_municipios['lon_decimal'] = df_municipios['lon_decimal'].round(2)

# Definir una función para obtener el pronóstico del clima usando latitud y longitud
def obtener_pronostico(lat, lon):
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={lat},{lon}&days=1"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Crear una lista para almacenar los pronósticos
pronosticos = []

# Obtener el pronóstico para cada municipio usando lat y lon
for index, row in df_municipios.iterrows():
    lat = row['lat_decimal']
    lon = row['lon_decimal']
    cve_mun = row['cve_mun']
    municipio = row['nom_mun']
    pronostico = obtener_pronostico(lat, lon)
    if pronostico and pronostico['location']['country'] == 'Mexico':
        for forecast_day in pronostico['forecast']['forecastday']:
            forecast = forecast_day['day']
            pronosticos.append({
                'cve_mun':cve_mun,
                'municipio': municipio,
                'forecast_date': forecast_day['date'],
                'maxtemp_c': forecast['maxtemp_c'],
                'mintemp_c': forecast['mintemp_c'],
                'avgtemp_c': forecast['avgtemp_c'],
                'totalprecip_mm': forecast['totalprecip_mm'],
                'condition_text': forecast['condition']['text'],
                'wind_kph': forecast.get('maxwind_kph'),
                'humidity': forecast.get('avghumidity')
            })
    else:
        for i in range(3):  # Añadir tres días de datos vacíos si no hay pronóstico disponible
            pronosticos.append({
                'cve_mun':cve_mun,
                'municipio': municipio,
                'forecast_date': None,
                'maxtemp_c': None,
                'mintemp_c': None,
                'avgtemp_c': None,
                'totalprecip_mm': None,
                'condition_text': None,
                'wind_kph': None,
                'humidity': None
            })

# Convertir la lista de pronósticos en un DataFrame
df_pronosticos = pd.DataFrame(pronosticos)

# Eliminar los registros con valores nulos
df_pronosticos = df_pronosticos.dropna()

# Crear un traductor del inglés al español
translator = Translator(to_lang="es")

# Función para traducir texto
def traducir(texto):
    try:
        return translator.translate(texto)
    except Exception as e:
        return texto

# Aplicar la traducción a la columna 'condition_text'
df_pronosticos['condition_text'] = df_pronosticos['condition_text'].apply(traducir)

# Cambiar los nombres de las columnas a español
df_pronosticos.columns = ['cve_mun','municipio', 'fecha_pronostico', 'temp_max_c', 'temp_min_c', 'temp_prom_c', 'precipitacion_mm', 'condicion', 'viento_kph', 'humedad']

# Quitar acentos de la columna 'condicion'
df_pronosticos['condicion'] = df_pronosticos['condicion'].apply(lambda x: unidecode.unidecode(x))

# Poner los nombres de los municipios con la primera letra en mayúscula, tipo oración
df_pronosticos['municipio'] = df_pronosticos['municipio'].apply(lambda x: x.title())

# Conectar a la base de datos y crear la tabla de pronósticos si no existe
with psycopg2.connect(
    dbname=r_dbname,
    user=r_user,
    password=r_password,
    host=r_host,
    port=r_port
) as conn:
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS pronosticos_clima (
        cve_mun INTEGER,
        municipio VARCHAR(255),
        fecha_pronostico DATE,
        temp_max_c FLOAT,
        temp_min_c FLOAT,
        temp_prom_c FLOAT,
        precipitacion_mm FLOAT,
        condicion VARCHAR(255),
        viento_kph FLOAT,
        humedad FLOAT,
        indicador_sequia VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insertar los datos del DataFrame en la tabla
    for index, row in df_pronosticos.iterrows():
        insert_query = """
        INSERT INTO pronosticos_clima (cve_mun, municipio, fecha_pronostico, temp_max_c, temp_min_c, temp_prom_c, precipitacion_mm, condicion, viento_kph, humedad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (row['cve_mun'],row['municipio'], row['fecha_pronostico'], row['temp_max_c'], row['temp_min_c'], row['temp_prom_c'], row['precipitacion_mm'], row['condicion'], row['viento_kph'], row['humedad']))

    conn.commit()
