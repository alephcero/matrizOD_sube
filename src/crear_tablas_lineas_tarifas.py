import json
import os
import pandas as pd
import shlex
from subprocess import Popen, PIPE
import numpy as np
from h3 import h3
import psycopg2
from sqlalchemy import create_engine
import geopandas as gpd

DB_USERNAME = 'sube_user'
DB_PASSWORD = 'sube_pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sube'
DB_SCHEMA = 'public'


os.system(
    'qri get body alephcero/sube_lineas -a -f json -o data/lineas.json')

os.system(
    'qri get body alephcero/sube_tarifas -a -f json -o data/tarifas.json')

# leer data de lineas y de tarifas
lineas = pd.read_json('data/lineas.json')
lineas.columns = ['id_linea', 'linea']

tarifas = pd.read_json('data/tarifas.json')
tarifas.columns = ['id_tarifa', 'tarifa']


lineas

print('creando db')
# Crear conexion a la db
conn = psycopg2.connect(user=DB_USERNAME,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME)

engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                       .format(DB_USERNAME, DB_PASSWORD, DB_HOST,
                               DB_PORT, DB_NAME))

lineas.to_sql('lineas', engine, schema=DB_SCHEMA,
              method='multi', index=False)

tarifas.to_sql('tarifas', engine, schema=DB_SCHEMA,
               method='multi', index=False)


os.system('rm data/lineas.json')
os.system('rm data/tarifas.json')
