# Matriz OD con datos SUBE
Este proyecto intenta construir una matriz Origen Destino a partir de datos SUBE obtenidos por pedido de información pública.


# Instalar h3

Para poder imputar destinos utilizando el esquema de hexagrillas [h3](https://eng.uber.com/h3/) es necesario instalar algunas dependencias. Consultar [aca](https://github.com/uber/h3-py).

# Crear ambiente
```
virtualenv venv --python=python3.7
source venv/bin/activate
pip install -r requirements.txt
```

# Crear una base de datos
Este proceso trabaja con un esquema de base de datos PostgreSQL. Asumimos que fue creada una db con las siguientes caracteristicas

```

DB_USERNAME = 'sube_user'
DB_PASSWORD = 'sube_pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sube'
DB_SCHEMA = 'public'
```

La base original contaba con 4922654 transacciones y 4447406 tarjetas, pero luego de lo siguientes filtros:

- Borrar los datos de 1154505 tarjetas con solo 1 trx
- Borrar nulls en `id_tarjeta`: 2189277 registros
- Borrar nulls en `lon`: 137527 tarjetas con 578460 registros
- Borrar nulls en `lat`: 3 tarjetas con 11 refistros
- Borrar coordenadas fuera de un bbox razonable: 261 tarjetas con 539 registros
- Borrar 550823 trx repetidas tarjeta, hora, etapa e interno
- En el rio de la plata: 8137 con 29712 registros

El resultado es 10419327 de transacciones con 3146972
