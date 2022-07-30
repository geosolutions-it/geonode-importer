# geonode-importer
## C256-METAMEDIA-2022-GEOPACKAGE

### Dependencies:
```
GDAL >= 3.2.2.1
```
To check your version please run: `gdalinfo --version`

The binary for installing gdal is available [here](https://gdal.org/download.html)

-----
## Installation: 

```
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

Add to geonode settings.py:

```python
INSTALLED_APPS += ('dynamic_models', 'importer', 'importer.handlers',)

CELERY_TASK_QUEUES += (
    Queue('importer.import_orchestrator', GEONODE_EXCHANGE, routing_key='importer.import_orchestrator'),
    Queue('importer.import_resource', GEONODE_EXCHANGE, routing_key='importer.import_resource', max_priority=8),
    Queue('importer.publish_resource', GEONODE_EXCHANGE, routing_key='importer.publish_resource', max_priority=8),
    Queue('importer.create_geonode_resource', GEONODE_EXCHANGE, routing_key='importer.create_geonode_resource', max_priority=8),
    Queue('importer.gpkg_ogr2ogr', GEONODE_EXCHANGE, routing_key='importer.gpkg_ogr2ogr', max_priority=10),
    Queue('importer.import_next_step', GEONODE_EXCHANGE, routing_key='importer.import_next_step', max_priority=3),
    Queue('importer.create_dynamic_structure', GEONODE_EXCHANGE, routing_key='importer.create_dynamic_structure', max_priority=10),
    Queue('importer.copy_geonode_resource', GEONODE_EXCHANGE, routing_key='importer.copy_geonode_resource', max_priority=0),
    Queue('importer.copy_dynamic_model', GEONODE_EXCHANGE, routing_key='importer.copy_dynamic_model'),
    Queue('importer.copy_geonode_data_table', GEONODE_EXCHANGE, routing_key='importer.copy_geonode_data_table'),
)

DATABASE_ROUTERS = ["importer.db_router.DatastoreRouter"]

SIZE_RESTRICTED_FILE_UPLOAD_ELEGIBLE_URL_NAMES += ('importer_upload',)

```

Run migrations:

```
python manage.py migrate
python manage.py migrate --database datastore
```
---

## Environment variables:

To change the task rate limit, please update the following env_variables:

```
IMPORTER_GLOBAL_RATE_LIMIT= # default 5
IMPORTER_PUBLISHING_RATE_LIMIT= # default 5
IMPORTER_RESOURCE_CREATION_RATE_LIMIT= # default 10
IMPORTER_RESOURCE_COPY_RATE_LIMIT = # default 10
```
---

## Supported file format

The importer will accept only:
- Vector GPKG

## Limitation

- The XML file and the SLD file uploaded along with the GPKG are ignored
- Every upload will create a new layer. There is no option for overwriting/skipping the existing layer
- The number of the layer in the GPKG should be lower than the max_parallel_upload configuration
---

## Troubleshooting

Validation is performed on the gpkg provided. 
Below is possible to find the schema that explains the error codes returned by the web app

| Code   |   Description |
|----------|:-------------|
| RQ1 | Layer names must start with a letter, and valid characters are lowercase a-z, numbers, or underscores.|
| RQ2 | Layers must have at least one feature.|
| RQ13 | It is required to give all GEOMETRY features the same default spatial reference system|
| RQ14 | The geometry_type_name from the gpkg_geometry_columns table must be one of POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, or MULTIPOLYGON|
| RQ15 | All table geometries must match the geometry_type_name from the gpkg_geometry_columns table|
| RC18 | It is recommended to give all GEOMETRY type columns the same name.|
