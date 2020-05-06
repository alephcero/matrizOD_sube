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
Este proceso trabaja con un esquema de base de datos PostgreSQL


