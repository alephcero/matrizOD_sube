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
from shapely.geometry import Polygon, Point
import re


def sacar_3_numeros_linea(s):
    try:
        return int(re.findall(r"\d{3}", s)[0])
    except:
        return np.nan


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


def vertices_cada_Xmetros(geom, metros):
    n_puntos = int((geom.length / metros) + 1)
    percentiles = np.linspace(0, geom.length, n_puntos)
    return [geom.interpolate(percentil, normalized=False) for percentil in percentiles]


def convertir_recorridos_buses_paradas(fila, metros=400):
    paradas = vertices_cada_Xmetros(fila.geometry, metros=metros)
    paradas = gpd.GeoSeries(paradas).map(lambda g: Point(g.coords[0][0:2]))
    crs = 'EPSG:3857'
    gdf = gpd.GeoDataFrame(
        np.repeat(fila.id_linea, len(paradas)), crs=crs, geometry=paradas)
    gdf.columns = ['id_linea', 'geometry']
    return gdf


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
print('crear tabla paradas')

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

print('Procesando subte')

# asignar a todo el subte la misma linea
ramales.loc[ramales.modo == 'SUB',
            'id_linea'] = ramales.loc[ramales.modo == 'SUB', 'id_linea'].min()


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

print('Subiendo subte')

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

print('Subiendo premetro')

premetro.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
                method='multi', index=False)

print('Procesando FFCC')

# FFCC
# crear una cartografia de estacions en base a IGN y ministerio
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

# asignar lineas a estaciones
lineas = gpd.read_file('carto/insumos/ffcc/rmba-ferrocarril-lineas')
lineas = lineas.reindex(columns=['Linea', 'Descrip', 'geometry'])
lineas.columns = ['linea', 'ramal', 'geometry']
lineas['ramal'] = lineas['linea'] + ' - ' + lineas['ramal']
lineas.geometry = lineas.geometry.to_crs(
    'EPSG:3857').buffer(500).to_crs('EPSG:4326')

ffcc = gpd.sjoin(ffcc, lineas, how='left')
ffcc.loc[ffcc.nombre_ramal.isnull(
), 'nombre_ramal'] = ffcc.loc[ffcc.nombre_ramal.isnull(), 'ramal']

# formatear para la db
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
                                                 'nombre_linea': 'Subte'},
                                                index=[tabla_lineas.index.max() + 1]))


ffcc = ffcc.merge(tabla_lineas)
ffcc['id_ramal'] = None

print('Subiendo FFCC')

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

print('Procesando Buses Nacionales')

# BUSES
# NACIONALES
filtro_buses_nac = (ramales.modo == 'COL') & (
    ramales.jurisdiccion == 'NACIONAL')

ramales_bus_nac = ramales.loc[filtro_buses_nac].copy()

ramales_bus_nac.nombre = ramales_bus_nac.nombre.map(
    lambda s: s.replace('_AMBA', ''))
ramales_bus_nac.nombre = ramales_bus_nac.nombre.map(lambda s: s.split('_')[-1])
ramales_bus_nac.nombre = ramales_bus_nac.nombre.map(lambda s: s.split(' ')[-1])
ramales_bus_nac.nombre = 'Linea ' + ramales_bus_nac.nombre.map(int).map(str)

ramales_bus_nac = ramales_bus_nac.drop('jurisdiccion', axis=1)
ramales_bus_nac = ramales_bus_nac.rename(columns={'nombre': 'nombre_linea'})
ramales_bus_nac
# nacionales
bus_nac = gpd.read_file('carto/insumos/lineas-nacionales/')
bus_nac.rename(columns={'LINEA': 'nombre_linea'}, inplace=True)
bus_nac = bus_nac.reindex(columns=['nombre_linea', 'geometry'])
bus_nac['nombre_linea'] = 'Linea ' + bus_nac['nombre_linea']
bus_nac = bus_nac.to_crs('EPSG:3857')

bus_nac = bus_nac.merge(ramales_bus_nac)

bus_nac.head()


paradas_bus_nac = pd.concat(
    [convertir_recorridos_buses_paradas(fila) for i, fila in bus_nac.iterrows()])
paradas_bus_nac.crs = 'EPSG:3857'
paradas_bus_nac = paradas_bus_nac.to_crs('EPSG:4326')

paradas_bus_nac = paradas_bus_nac.merge(bus_nac.drop('geometry', axis=1))
paradas_bus_nac['nombre_ramal'] = None
paradas_bus_nac['nombre_parada'] = None
paradas_bus_nac['lat'] = paradas_bus_nac.geometry.y
paradas_bus_nac['lon'] = paradas_bus_nac.geometry.x
paradas_bus_nac = paradas_bus_nac.drop('geometry', axis=1)
paradas_bus_nac = h3_indexing(paradas_bus_nac, res=10)
paradas_bus_nac.head()

print('Subiendo Buses Nacionales')

for linea in paradas_bus_nac['nombre_linea'].unique():
    subir = paradas_bus_nac.loc[paradas_bus_nac.nombre_linea == linea, :]
    subir.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
                 method='multi', index=False)

# reemplazando los nombres en lineas nacionales
ramales.loc[filtro_buses_nac, 'nombre'] = ramales.loc[filtro_buses_nac, 'nombre'].map(
    lambda s: s.replace('_AMBA', ''))

ramales.loc[filtro_buses_nac, 'nombre'] = ramales.loc[filtro_buses_nac,
                                                      'nombre'].map(lambda s: s.split('_')[-1])
ramales.loc[filtro_buses_nac, 'nombre'] = ramales.loc[filtro_buses_nac,
                                                      'nombre'].map(lambda s: s.split(' ')[-1])
ramales.loc[filtro_buses_nac, 'nombre'] = 'Linea ' + \
    ramales.loc[filtro_buses_nac, 'nombre'] .map(int).map(str)


# PROVINCIALES
filtro_buses_prov = (ramales.modo == 'COL') & (
    ramales.jurisdiccion == 'PROVINCIAL')

ramales_bus_prov = ramales.loc[filtro_buses_prov].copy()

ramales_bus_prov = ramales_bus_prov.drop('jurisdiccion', axis=1)
ramales_bus_prov = ramales_bus_prov.rename(columns={'nombre': 'nombre_linea'})

ramales_bus_prov.nombre_linea = ramales_bus_prov.nombre_linea.map(
    lambda s: int(re.findall(r"\d{3}", s)[0]))


carto_bus_prov = gpd.read_file('carto/insumos/lineas-provinciales/')

# ramales_bus_prov.nombre_linea.isin(carto_bus_prov.LINEA.unique()).sum()/len(ramales_bus_prov)
# ramales_bus_prov[~ramales_bus_prov.nombre_linea.isin(carto_bus_prov.LINEA.unique())]['nombre_linea'].unique()

carto_bus_prov.rename(columns={'LINEA': 'nombre_linea'}, inplace=True)
carto_bus_prov = carto_bus_prov.reindex(columns=['nombre_linea', 'geometry'])
carto_bus_prov = carto_bus_prov.to_crs('EPSG:3857')
carto_bus_prov = carto_bus_prov.merge(ramales_bus_prov)
carto_bus_prov['nombre_linea'] = 'Linea ' + \
    carto_bus_prov['nombre_linea'].map(str)

paradas_bus_prov = pd.concat(
    [convertir_recorridos_buses_paradas(fila) for i, fila in carto_bus_prov.iterrows()])

paradas_bus_prov.crs = 'EPSG:3857'
paradas_bus_prov = paradas_bus_prov.to_crs('EPSG:4326')

paradas_bus_prov = paradas_bus_prov.merge(
    carto_bus_prov.drop('geometry', axis=1))
paradas_bus_prov['nombre_ramal'] = None
paradas_bus_prov['nombre_parada'] = None
paradas_bus_prov['lat'] = paradas_bus_prov.geometry.y
paradas_bus_prov['lon'] = paradas_bus_prov.geometry.x
paradas_bus_prov = paradas_bus_prov.drop('geometry', axis=1)
paradas_bus_prov = h3_indexing(paradas_bus_prov, res=10)


print('Subiendo Buses Provinciales')

for linea in paradas_bus_prov['nombre_linea'].unique():
    subir = paradas_bus_prov.loc[paradas_bus_prov.nombre_linea == linea, :]
    subir.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
                 method='multi', index=False)

# modificando nombres en tabla de ramales
ramales.loc[filtro_buses_prov, 'nombre'] = ramales.loc[filtro_buses_prov, 'nombre'].map(
    lambda s: int(re.findall(r"\d{3}", s)[0]))

# MUNICIPALES
filtro_buses_muni = (ramales.modo == 'COL') & (
    ramales.jurisdiccion == 'MUNICIPAL')

ramales_bus_muni = ramales.loc[filtro_buses_muni].copy()

carto_bus_muni = gpd.read_file('carto/insumos/lineas-municipales/')
carto_bus_muni.rename(columns={'LINEA': 'nombre_linea'}, inplace=True)

ramales_bus_muni = ramales_bus_muni.drop('jurisdiccion', axis=1)
ramales_bus_muni = ramales_bus_muni.rename(columns={'nombre': 'nombre_linea'})

ramales_bus_muni.nombre_linea = ramales_bus_muni.nombre_linea.map(
    sacar_3_numeros_linea)
ramales_bus_muni = ramales_bus_muni.dropna(subset=['nombre_linea'])
ramales_bus_muni.nombre_linea = ramales_bus_muni.nombre_linea.map(int)


carto_bus_muni = carto_bus_muni.reindex(columns=['nombre_linea', 'geometry'])
carto_bus_muni = carto_bus_muni.to_crs('EPSG:3857')
carto_bus_muni.shape
ramales_bus_muni.shape

carto_bus_muni = carto_bus_muni.merge(ramales_bus_muni)

carto_bus_muni['nombre_linea'] = 'Linea ' + \
    carto_bus_muni['nombre_linea'].map(str)

paradas_bus_muni = pd.concat(
    [convertir_recorridos_buses_paradas(fila) for i, fila in carto_bus_muni.iterrows()])


paradas_bus_muni.crs = 'EPSG:3857'
paradas_bus_muni = paradas_bus_muni.to_crs('EPSG:4326')

paradas_bus_muni = paradas_bus_muni.merge(
    carto_bus_muni.drop('geometry', axis=1))
paradas_bus_muni['nombre_ramal'] = None
paradas_bus_muni['nombre_parada'] = None

paradas_bus_muni['lat'] = paradas_bus_muni.geometry.y
paradas_bus_muni['lon'] = paradas_bus_muni.geometry.x
paradas_bus_muni = paradas_bus_muni.drop('geometry', axis=1)

paradas_bus_muni = h3_indexing(paradas_bus_muni, res=10)

print('Subiendo Buses Municipales')

for linea in paradas_bus_muni['nombre_linea'].unique():
    subir = paradas_bus_muni.loc[paradas_bus_muni.nombre_linea == linea, :]
    subir.to_sql('paradas', engine, if_exists='append', schema=DB_SCHEMA,
                 method='multi', index=False)

# modificando nombres en tabla de ramales
ramales.loc[filtro_buses_muni, 'nombre'] = ramales.loc[filtro_buses_muni,
                                                       'nombre'].map(sacar_3_numeros_linea)


# subir a bd
ramales.to_sql('ramales', engine, schema=DB_SCHEMA,
               method='multi', index=False)

tarifas.to_sql('tarifas', engine, schema=DB_SCHEMA,
               method='multi', index=False)


os.system('rm data/lineas.json')
os.system('rm data/tarifas.json')

conn.close()
