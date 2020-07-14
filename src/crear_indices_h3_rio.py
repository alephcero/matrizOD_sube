import geopandas as gpd
import pandas as pd
from h3 import h3

rio = gpd.read_file('carto/insumos/rio_de_la_plata.geojson')
rio_geojson = gpd.GeoSeries([rio.geometry.iloc[0]]).__geo_interface__
rio_geojson = rio_geojson['features'][0]['geometry']

indices_rio = h3.polyfill(
    geo_json=rio_geojson,
    res=10, geo_json_conformant=True)

indices_rio = pd.DataFrame(indices_rio)
indices_rio.columns = ['indices_rio']
indices_rio.to_csv('carto/indices_rio.csv', index=False)
