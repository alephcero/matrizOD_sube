import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import psycopg2

DB_USERNAME = 'sube_user'
DB_PASSWORD = 'sube_pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sube'
DB_SCHEMA = 'public'

# Conectar a la db
conn = psycopg2.connect(user=DB_USERNAME,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME)

engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                       .format(DB_USERNAME, DB_PASSWORD, DB_HOST,
                               DB_PORT, DB_NAME))
print('creando tabla viajes')
query_crear_viajes = """
CREATE TABLE viajes (
    id_tarjeta    bigint,
    id_viaje      int,
    hora_inicio   int,
    n_tramos      int,
    n_tramos_col  int,
    n_tramos_tre  int,
    n_tramos_sub  int,
    o_lon         numeric,
    o_lat         numeric,
    o_h3          text,
    d_lon         numeric,
    d_lat         numeric,
    d_h3          text,
    CONSTRAINT viaje_pk PRIMARY KEY(id_tarjeta,id_viaje)
);
"""
cur = conn.cursor()
cur.execute(query_crear_viajes)
cur.close()
conn.commit()

print('descargando tramos')
q = """
select id_tarjeta, hora, etapa_red_sube, id_tramo, id_viaje, modo,
       o_lat, o_lon, o_h3, d_lat, d_lon,
       d_h3, id_tarifa
from tramos;
"""
trx = pd.read_sql(q, conn)
print('descargados %i tramos' % len(trx))

# separar modos en columnas
tramos = pd.concat([trx, pd.get_dummies(trx.modo)], axis=1)


# contar para cada tarjeta cantidad de tramos por modo
modos_por_viajes = tramos.groupby(['id_tarjeta', 'id_viaje']).agg(
    {'COL': 'sum', 'SUB': 'sum', 'TRE': 'sum'}).reset_index()

# obtener origens y destinos
group_df_od = tramos.reindex(columns=['id_tarjeta', 'id_viaje', 'id_tramo'])\
    .groupby(['id_tarjeta', 'id_viaje'])
origenes = group_df_od.min().reset_index()
destinos = group_df_od.max().reset_index()
print('origenes:', origenes.shape[0])
print('detinos:', destinos.shape[0])

# obtener informacion de cada uno
destinos_info = tramos.reindex(
    columns=['id_tarjeta', 'id_viaje', 'id_tramo', 'd_lon', 'd_lat', 'd_h3'])
origenes_info = tramos.reindex(
    columns=['id_tarjeta', 'id_viaje', 'id_tramo', 'hora', 'o_lon', 'o_lat', 'o_h3'])

# agregar
origenes = origenes.merge(origenes_info,
                          how='inner', on=['id_tarjeta', 'id_viaje', 'id_tramo'])
origenes = origenes.drop('id_tramo', axis=1)

destinos = destinos.merge(destinos_info,
                          how='inner', on=['id_tarjeta', 'id_viaje', 'id_tramo'])
destinos = destinos.drop('id_tramo', axis=1)
print('origenes:', origenes.shape[0])
print('detinos:', destinos.shape[0])


# crear tabla viajes
viajes = origenes.merge(destinos,
                        how='inner', on=['id_tarjeta', 'id_viaje'])
viajes = viajes.merge(modos_por_viajes,
                      how='inner', on=['id_tarjeta', 'id_viaje'])

n_tramos = tramos.reindex(columns=['id_tarjeta', 'id_viaje']).groupby(
    ['id_tarjeta', 'id_viaje']).size().reset_index()
n_tramos.columns = ['id_tarjeta', 'id_viaje', 'n_tramos']
viajes = viajes.merge(n_tramos,
                      how='inner', on=['id_tarjeta', 'id_viaje'])

print('tabla viajes con %i viajes' % viajes.shape[0])

# renombrar columnas
viajes = viajes.rename(columns={
    'hora': 'hora_inicio',
    'COL': 'n_tramos_col',
    'TRE': 'n_tramos_tre',
    'SUB': 'n_tramos_sub'
})
# eliminar el destino del viaje que que tiene dentro etapas con destinos
# sin predecir
viajes_con_tramo_sin_predecir = tramos.loc[tramos.d_h3.isnull(), [
    'id_tarjeta', 'id_viaje']]
viajes_con_tramo_sin_predecir = viajes_con_tramo_sin_predecir.drop_duplicates()
viajes_con_tramo_sin_predecir.loc[:, 'keep'] = False

viajes = viajes.merge(viajes_con_tramo_sin_predecir, on=[
                      'id_tarjeta', 'id_viaje'], how='left')
viajes.loc[:, 'keep'] = viajes.loc[:, 'keep'].fillna(True)
# eliminar destino de los viajes cuya cadena entera no puede reconstruirse
viajes.loc[~viajes.keep, 'd_h3'] = None
viajes = viajes.drop('keep', axis=1)
viajes.to_csv('viajesfinal.csv', index=False)
viajes.to_sql('viajes', engine, if_exists='append', schema=DB_SCHEMA,
              chunksize=50000, index=False, method='multi')
