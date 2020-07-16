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


def h3_from_row(row, res, x, y):
    '''
    Esta funcion toma una fila, un nivel de resolucion de h3
    y los nombres que contienen las coordenadas xy
    y devuelve un id de indice h3
    '''
    return h3.geo_to_h3(row[y], row[x], res=res)


def h3_indexing(df, res, lat='lat', lon='lon'):
    """
    Esta funcion toma una tabla con dos pares de coordenadas para origen y destino
    Un nivel de resolucion h3
    y devuelve la tabla con los ids de h3
    """

    df['h3_res_' + str(res)] = df.apply(h3_from_row,
                                        axis=1, args=[res, lon, lat])
    return df


def limpiar_nombre_parada(s):
    s = s.replace('á', 'a')
    s = s.replace('é', 'e')
    s = s.replace('í', 'i')
    s = s.replace('ó', 'o')
    s = s.replace('ú', 'u')
    s = s.replace('ñ', 'n')
    s = s.replace('Á', 'A')
    s = s.replace('É', 'E')
    s = s.replace('Í', 'I')
    s = s.replace('Ó', 'O')
    s = s.replace('Ú', 'U')
    s = s.replace('Ñ', 'N')
    s = s.replace('°', '')

    return s


DB_USERNAME = 'sube_user'
DB_PASSWORD = 'sube_pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sube'
DB_SCHEMA = 'public'
# Crear conexion a la db
conn = psycopg2.connect(user=DB_USERNAME,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME)

engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                       .format(DB_USERNAME, DB_PASSWORD, DB_HOST,
                               DB_PORT, DB_NAME))


query = """
create sequence id_parada_seq start 1;

CREATE TABLE paradas (
    id bigint PRIMARY KEY DEFAULT nextval('id_parada_seq'::regclass),
    id_linea int,
    nombre_linea text,
    nombre_ramal text,
    id_ramal int,
    modo text,
    nombre_parada text,
    lat numeric,
    lon numeric,
    h3_res_10 text
    );
"""


cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()


os.system(
    'qri get body alephcero/sube_lineas -a -f json -o data/lineas.json')

os.system(
    'qri get body alephcero/sube_tarifas -a -f json -o data/tarifas.json')
# leer data de lineas y de tarifas

tarifas = pd.read_json('data/tarifas.json')
tarifas.columns = ['id_tarifa', 'tarifa']

ramales = pd.read_json('data/lineas.json')
ramales.columns = ['id_ramal', 'nombre', 'jurisdiccion', 'modo']

ramales['id_linea'] = ramales['id_ramal'].copy()


# asignar a todo el subte la misma linea
ramales.loc[lineas.modo == 'SUB',
            'id_linea'] = lineas.loc[lineas.modo == 'SUB', 'id_linea'].min()


# SUBTE Y PREMETRO
ramales_subte = ramales.loc[ramales.modo == 'SUB', :]
ramales_subte
ramales_dict = {
    'A': 32,
    'B': 33,
    'C': 269,
    'D': 54,
    'E': 148,
    'H': 220,
}
id_linea_subte = ramales.loc[ramales.modo == 'SUB', 'id_linea'].unique()[0]

subte = gpd.read_file('carto/insumos/subterraneo-estaciones')
subte.rename(columns={'LINEA': 'nombre_ramal',
                      'ESTACION': 'nombre_parada'}, inplace=True)
subte['nombre_linea'] = 'Subte'
subte['modo'] = 'SUB'
subte['lat'] = subte.geometry.y
subte['lon'] = subte.geometry.x
subte['id_linea'] = id_linea_subte
subte['id_ramal'] = subte.nombre_ramal.replace(ramales_dict)
subte = subte.reindex(
    columns=['id_linea', 'nombre_linea', 'modo', 'nombre_ramal', 'id_ramal', 'nombre_parada', 'lat', 'lon'])
subte = h3_indexing(subte, res=10)
subte['nombre_parada'] = subte['nombre_parada'].map(limpiar_nombre_parada)
subte

subte.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
             method='multi', index=False)

premetro = pd.read_csv('carto/insumos/estaciones-premetro.csv')
premetro['nombre_linea'] = 'Subte'
premetro['id_ramal'] = 367
premetro['nombre_ramal'] = 'Premetro'

premetro.rename(columns={'long': 'lon',
                         'nombre': 'nombre_parada'}, inplace=True)

premetro['modo'] = 'SUB'
premetro['id_linea'] = id_linea_subte

premetro = premetro.reindex(
    columns=['id_linea', 'nombre_linea',
             'id_ramal', 'nombre_ramal', 'modo', 'nombre_parada', 'lat', 'lon'])
premetro = h3_indexing(premetro, res=10)

premetro['nombre_parada'] = premetro['nombre_parada'].map(
    limpiar_nombre_parada)

premetro.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
                method='multi', index=False)


# FFCC
# asignr por linea, no ramal
ign = gpd.read_file('carto/insumos/ffcc/lineas ig no en mintrans.geojson')
ign = ign.reindex(columns=['nam', 'geometry'])
ign.columns = ['nombre_parada', 'geometry']
ign['nombre_linea'] = 'Roca'
ign['nombre_ramal'] = 'Roca - Universitario'

est_ramal_chas = ['Domselaar', 'Chascomús',
                  'Brandsen', 'Altamirano', 'Jeppener']
ign.loc[ign.nombre_parada.isin(est_ramal_chas),
        'nombre_ramal'] = 'Roca - Chascomus'

mintrans = gpd.read_file('carto/insumos/ffcc/rmba-ferrocarril-estaciones/')
mintrans = mintrans.reindex(columns=['Línea', 'ETIQUETA', 'geometry'])
mintrans.columns = ['nombre_linea', 'nombre_parada', 'geometry']

ffcc = pd.concat([mintrans, ign])

lineas = gpd.read_file('carto/insumos/ffcc/rmba-ferrocarril-lineas')
lineas = lineas.reindex(columns=['Linea', 'Descrip', 'geometry'])
lineas.columns = ['linea', 'ramal', 'geometry']
lineas['ramal'] = lineas['linea'] + ' - ' + lineas['ramal']
lineas.geometry = lineas.geometry.to_crs(
    'EPSG:3857').buffer(500).to_crs('EPSG:4326')


ffcc = gpd.sjoin(ffcc, lineas, how='left')
ffcc.loc[ffcc.nombre_ramal.isnull(
), 'nombre_ramal'] = ffcc.loc[ffcc.nombre_ramal.isnull(), 'ramal']
ffcc = ffcc.reindex(
    columns=['nombre_linea', 'nombre_parada', 'geometry', 'nombre_ramal'])
ffcc['modo'] = 'TRE'
ffcc['lat'] = ffcc.geometry.y
ffcc['lon'] = ffcc.geometry.x
ffcc = h3_indexing(ffcc, res=10)
ffcc.drop('geometry', axis=1, inplace=True)
ffcc['nombre_parada'].fillna('', inplace=True)
ffcc['nombre_parada'] = ffcc['nombre_parada'].map(limpiar_nombre_parada)
ffcc['nombre_ramal'] = ffcc['nombre_ramal'].map(limpiar_nombre_parada)

# crear tabla lineas

tabla_lineas = pd.DataFrame(ffcc.nombre_linea.unique())
tabla_lineas['id_linea'] = range(ramales.id_ramal.max(
) + 1, ramales.id_ramal.max() + 1 + len(tabla_lineas))
tabla_lineas.columns = ['nombre_linea', 'id_linea']
tabla_lineas = tabla_lineas.append(pd.DataFrame({'id_linea': id_linea_subte,
                                                 'nombre_linea': 'Subte'}, index=[tabla_lineas.index.max() + 1]))


ffcc = ffcc.merge(tabla_lineas)
ffcc['id_ramal'] = None

ffcc.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
            method='multi', index=False)


ramales_a_lineas_ffcc = {
    'FFCC ROCA': 405,
    'FFCC_BELG_SUR': 406,
    'FFCC_SUAREZ': 403,
    'FFCC SAR  MERLO - LOBOS': 404,
    'FFCC SAR MORENO - MERCEDES': 404,
    'FFCC_TREN_COSTA': 403,
    'FFCC SAR': 404,
    'FFCC URQUIZA': 409,
    'FCC MITRE_TIGRE': 403,
    'FFCC_MITRE': 403,
    'FFCC_MITRE_CAPILLA': 403,
    'FFCC_MITRE_ZARATE': 403,
    'FFCC_BELGRANO_NORTE': 408,
    'FFCC_ROCA_CANUELAS-MONTE': 405,
    'FFCC_ROCA_KORN-CHASCOMUS': 405,
    'FFCC_ROCA_UNIVERSITARIO': 405,
    'FFCC_SANM': 407,
    'FFCC_ROCA_CANUELAS-LOBOS': 405,
    'FFCC TREN DEL VALLE': None}

# poner la linea a los ramales ffcc
ramales.loc[ramales.modo == 'TRE', 'id_linea'] =\
    ramales.loc[ramales.modo == 'TRE', 'nombre'].replace(ramales_a_lineas_ffcc)


ramales.to_sql('ramales', engine, schema=DB_SCHEMA,
               method='multi', index=False)

tarifas.to_sql('tarifas', engine, schema=DB_SCHEMA,
               method='multi', index=False)


os.system('rm data/lineas.json')
os.system('rm data/tarifas.json')

conn.close()
