import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os
import datashader
import datashader.transfer_functions
from datashader.utils import lnglat_to_meters
import holoviews
import geoviews
from holoviews.operation.datashader import datashade

os.chdir('/data')

# https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data
geo_data = dd.read_csv('311_Service_Requests_from_2010_to_Present.csv', usecols=['Latitude','Longitude'])
# simple lat-longitude plot. skip step 3 (transformation)
with ProgressBar():
    x_range = (-74.3, -73.6)
    y_range = (40.4, 41)
    scene = datashader.Canvas(plot_width=600, plot_height=600, x_range=x_range, y_range=y_range)
    aggregation  = scene.points(geo_data, 'Longitude', 'Latitude')
    image = datashader.transfer_functions.shade(aggregation)

# prepare data for map tiling
with ProgressBar():
    web_mercator_x, web_mercator_y = lnglat_to_meters(geo_data['Longitude'], geo_data['Latitude'])
    projected_coordinates = dd.concat([web_mercator_x, web_mercator_y], axis=1).dropna()
    transformed = projected_coordinates.rename(columns={'Longitude':'x', 'Latitude': 'y'})
    dd.to_parquet(path='nyc311_webmercator_coords', df=transformed, compression="SNAPPY")

nyc311_geo_data = dd.read_parquet('nyc311_webmercator_coords')

# plot interactive heatmap
holoviews.extension('bokeh')
stamen_api_url = 'http://tile.stamen.com/terrain/{Z}/{X}/{Y}.jpg'
plot_options  = dict(width=900, height=700, show_grid=False)
tile_provider  = geoviews.WMTS(stamen_api_url).opts(style=dict(alpha=0.8), plot=plot_options)
points = holoviews.Points(nyc311_geo_data, ['x', 'y'])
service_calls = datashade(points, x_sampling=1, y_sampling=1, width=900, height=700)
tile_provider * service_calls
