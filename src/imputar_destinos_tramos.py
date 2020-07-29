import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
from h3 import h3


def imputar_detino_por_tarjeta(tramos_tarjeta, paradas, tolerancia_hex=12):
    try:
        for i in range(len(tramos_tarjeta)):
            # tomo un tramo
            tramo = tramos_tarjeta.iloc[i]

            # tomo la linea correspondiente al ramal del tramo
            ramal_trx = tramo.id_ramal
            lineas_trx = paradas.loc[paradas.id_ramal ==
                                     ramal_trx, 'id_linea'].unique()

            # y sus posibles paradas de la cartografia
            paradas_destino_posibles = paradas.loc[paradas.id_linea.isin(
                lineas_trx), 'h3_res_10']

            # tomo las coordenadas del siguiente origen
            if i < (len(tramos_tarjeta) - 1):
                # cuando hay un O_2
                O_2 = tramos_tarjeta.iloc[i + 1]['o_h3']
            else:
                # si no tomar el primero del dia
                O_2 = tramos_tarjeta.iloc[0]['o_h3']

            # calculo la distancias
            distancias_a_paradas = paradas_destino_posibles.map(
                lambda h: h3.h3_distance(
                    h3_address_origin=O_2,
                    h3_address_h3=h))

            # evaluo tolerancia
            if any(distancias_a_paradas < tolerancia_hex):
                parada_destino = paradas_destino_posibles.loc[distancias_a_paradas.idxmin(
                )]
            else:
                parada_destino = None

            # asigno el destino
            tramos_tarjeta.loc[tramo.name, 'd_h3'] = parada_destino
            tramos_tarjeta.loc[tramo.name,
                               'd_lat'] = detino_h3_to_lat(parada_destino)
            tramos_tarjeta.loc[tramo.name,
                               'd_lon'] = detino_h3_to_lon(parada_destino)
    except:
        print('tarjeta:', tramos_tarjeta.id_tarjeta.unique())

    return tramos_tarjeta


def detino_h3_to_lat(h):
    if h is None:
        return None
    else:
        return round(h3.h3_to_geo(h)[0], 5)


def detino_h3_to_lon(h):
    if h is None:
        return None
    else:
        return round(h3.h3_to_geo(h)[1], 5)


# seteo el nivel de resolucion de h3 y la tolerancia en metros
resolucion = 10
tolerancia_metros = 1500
distancia_entre_hex = h3.edge_length(resolution=resolucion, unit='m') * 2
tolerancia_hex = np.ceil(tolerancia_metros / distancia_entre_hex)

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

q = """
select *
from paradas p
"""
paradas = pd.read_sql(q, conn)

# traer tramos de 10 tarjetas que hayan usado la linea b
query = """
select *
from tramos
where id_tarjeta in (37030200,
  37030272,
  37030633,
  37032074,
  37032172,
  37032419,
  37032508,
  37033376,
  37034039,
  37034278)
order by id_tarjeta,id_viaje,id_tramo;
"""
tramos = pd.read_sql(query, conn)

destinos = tramos.groupby(['id_tarjeta']).apply(
    imputar_detino_por_tarjeta, paradas=paradas, tolerancia_hex=tolerancia_hex)

destinos = destinos.reindex(
    columns=['id_tarjeta', 'id_viaje', 'id_tramo', 'd_h3', 'd_lat', 'd_lon'])

destinos

destinos.to_sql('destinos', engine, if_exists='append', schema=DB_SCHEMA,
                chunksize=50000, index=False, method='multi')

update_query = """
UPDATE tramos
SET d_h3 = d.d_h3,
    d_lat = d.d_lat,
	d_lon = d.d_lon
FROM destinos d
WHERE tramos.id_tarjeta = d.id_tarjeta
and tramos.id_viaje = d.id_viaje
and tramos.id_tramo = d.id_tramo;

delete from destinos;
"""

cur = conn.cursor()
cur.execute(update_query)
cur.close()
conn.commit()
