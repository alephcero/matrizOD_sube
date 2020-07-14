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


def leer_jsons(numero, columnas):
    df = pd.read_json('data/qri_%s.json' % numero)
    df.columns = columnas
    return df


print('Bajando datos de qri')
#os.system('qri get body alephcero/sube_transacciones -a -f json -o data/qri_1.json')
# os.system(
#    'qri get body alephcero/sube_transacciones_2de3 -a -f json -o data/qri_2.json')
# os.system(
#    'qri get body alephcero/sube_transacciones_3de3 -a -f json -o data/qri_3.json')

# obtener columnas
print('obtener columnas')
command = 'qri get structure.schema.items alephcero/sube_transacciones -f json'
proc = Popen(shlex.split(command), stdin=PIPE, stdout=PIPE, stderr=PIPE)
stdoutdata, err = proc.communicate()
json_out = json.loads(stdoutdata)
dtypes_dict = {d['title']: d['type'] for d in json_out['items']}
columnas = dtypes_dict.keys()

print('leyendo jsons de qri')
df = pd.concat([leer_jsons(numero, columnas) for numero in range(1, 4)])
#df = leer_jsons(3, columnas)

tarjetas = len(df.id_tarjeta.unique())
print('La base tiene %i transacciones y %i tarjetas' % (len(df), tarjetas))

print('eliminando casos incorrectos')

df.id_tarjeta = df.id_tarjeta.replace('', np.nan)
print('tarjeta id null %s' % df.id_tarjeta.isnull().sum())
df = df.loc[df.id_tarjeta.notnull(), :]

# eliminar tarjetas con transaccion unica
tarjetas_unica_trx = df.groupby('id_tarjeta').count()['hora'] == 1
tarjetas_unica_trx = tarjetas_unica_trx.index[tarjetas_unica_trx]

print('Borrar los datos de %i tarjetas con solo 1 trx:' %
      len(tarjetas_unica_trx))
df = df.loc[~df.id_tarjeta.isin(tarjetas_unica_trx), :]

# datos con lar long mal
df.lon = df.lon.replace('', np.nan)
tarjetas_lon_null = df.loc[df.lon.isnull(), 'id_tarjeta'].unique()

print('Borrar los datos de %i tarjetas con lon null: %s registros' % (
    len(tarjetas_lon_null), df.id_tarjeta.isin(tarjetas_lon_null).sum()))

df = df.loc[~df.id_tarjeta.isin(tarjetas_lon_null), :]

df.lat = df.lat.replace('', np.nan)
tarjetas_lat_null = df.loc[df.lat.isnull(), 'id_tarjeta'].unique()

print('Borrar los datos de %i tarjetas con lat null: %s registros' % (
    len(tarjetas_lat_null), df.id_tarjeta.isin(tarjetas_lat_null).sum()))

df = df.loc[~df.id_tarjeta.isin(tarjetas_lat_null), :]


# eliminar casos con coordenadas muy desfazadas
tarjetas_mal_latlon = df.loc[(df.lat < -36) | (df.lat > -33) |
                            (df.lon < -61) | (df.lon > -57), 'id_tarjeta'].unique()


print('Borrar los datos de %i tarjetas fuera del bbox: %s registros' % (
    len(tarjetas_mal_latlon),
      (df.id_tarjeta.isin(tarjetas_mal_latlon)).sum()))

df = df.loc[~df.id_tarjeta.isin(tarjetas_mal_latlon), :]

# borrar casos que repiten tarjeta, hora, etapa e interno
tramos_simultaneos = df.duplicated(
    subset=['id_tarjeta', 'interno_bus', 'hora', 'etapa_red_sube'])

print('Borrar %i trx repetidas tarjeta, hora, etapa e interno' %
      tramos_simultaneos.sum())

df = df.loc[~tramos_simultaneos, :]

# eliminando checjouts sin checkins
tarjetas_solo_checkout = df.loc[(
    df.tipo_trx_tren == 'CHECK OUT SIN CHECKIN'), 'id_tarjeta'].unique()
print('Borrar los datos de %i tarjetas con solo checkin: %s registros' % (
    len(tarjetas_solo_checkout), df.id_tarjeta.isin(tarjetas_solo_checkout).sum()))

df = df.loc[~df.id_tarjeta.isin(tarjetas_solo_checkout), :]

# TODO: organizar un modo se sumar la info de checkout en tramos y viajes
checkouts = (df.tipo_trx_tren == 'CHECK OUT')
print('Eliminando %i transacciones de CHECK OUT', checkouts.sum())
df = df.loc[df.tipo_trx_tren != 'CHECK OUT', :]

print('leyendo rio y obteniendo indices')
# archivo poducido por crar_indices_h3_rio.py
indices_rio = pd.read_csv('carto/indices_rio.csv')


print('obteniendo indices h3 para las celdas')
# aplicar indice de hexgrid jerarquico h3 para diferentes resoluciones
trx_h3 = h3_indexing(df, res=10)

trx_h3 = trx_h3.rename(columns={'h3_res_10': 'o_h3',
                                'lat': 'o_lat',
                                'lon': 'o_lon',
                                })
trx_h3['d_h3'] = None
trx_h3['d_lat'] = None
trx_h3['d_lon'] = None

print('fin indexado h3')

tarjetas_rio = trx_h3.loc[trx_h3.o_h3.isin(
    indices_rio.indices_rio), 'id_tarjeta'].unique()

print('Borrar los datos de %i tarjetas con trx en el rio: %s registros' % (
    len(tarjetas_rio), trx_h3.id_tarjeta.isin(tarjetas_rio).sum()))

trx_h3 = trx_h3.loc[~trx_h3.id_tarjeta.isin(tarjetas_rio), :]


# for i in range(1, 4):
#    os.system('rm data/qri_%i.json' % i)

trx_h3 = trx_h3.sort_values(['id_tarjeta', 'hora', 'etapa_red_sube'])

print('creando id de tramos y viajes')

# TODO: organizar un modo se sumar la info de checkout en tramos y viajes
#checkout_mask = df.tipo_trx_tren == 'CHECK OUT'

# trx_h3['id_tramo'] = trx_h3.loc[~checkout_mask].groupby(
#    'id_tarjeta').cumcount() + 1
trx_h3['id_tramo'] = trx_h3.groupby('id_tarjeta').cumcount() + 1

trx_h3['nro_viaje_temp'] = trx_h3.id_tramo - trx_h3.etapa_red_sube

temp = trx_h3.groupby(['id_tarjeta', 'nro_viaje_temp']).size().reset_index()
del temp[0]
temp['id_viaje'] = temp.groupby(['id_tarjeta']).cumcount() + 1

trx_h3 = trx_h3.merge(temp)

del temp
del trx_h3['nro_viaje_temp']

trx_h3.to_csv('data/tramos.csv', index=False)

print('creando db')
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
CREATE TABLE tramos (
    id_tarjeta bigint,
    hora int,
    etapa_red_sube int,
    id_tramo int,
    id_viaje int,
    modo text,
    id_linea int,
    interno_bus int,
    o_lat numeric,
    o_lon numeric,
    o_h3 text,
    d_lat numeric,
    d_lon numeric,
    d_h3 text,
    sexo text,
    tipo_trx_tren text,
    id_tarifa int
    )
"""

cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()
conn.close()

print('subiendo a db')
#trx_h3.to_sql('tramos', engine, schema=DB_SCHEMA,method='multi', index=False)

print('terminado')
