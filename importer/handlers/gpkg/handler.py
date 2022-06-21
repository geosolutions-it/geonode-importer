import logging
import os
from subprocess import PIPE, Popen

from celery import chord, group
from django.conf import settings
from django.utils import timezone
from dynamic_models.exceptions import InvalidFieldNameError, DynamicModelError
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.resource.models import ExecutionRequest
from importer.celery_tasks import ErrorBaseTaskClass
from importer.handlers.base import AbstractHandler
from importer.handlers.gpkg.tasks import SingleMessageErrorHandler
from importer.handlers.gpkg.utils import (GEOM_TYPE_MAPPING,
                                          STANDARD_TYPE_MAPPING)
from importer.handlers.utils import should_be_imported
from osgeo import ogr

logger = logging.getLogger(__name__)
from importer.celery_app import importer_app


class GPKGFileHandler(AbstractHandler):
    '''
    Handler to import GPK files into GeoNode data db
    It must provide the task_lists required to comple the upload
    '''
    TASKS_LIST = (
        "start_import",
        "importer.import_resource",
        "importer.publish_resource",
        "importer.create_gn_resource",
        # "importer.validate_upload", last task that will evaluate if there is any error coming from the execution. Maybe a chord?
    )

    def is_valid(self, files):
        """
        Define basic validation steps
        """        
        return all([os.path.exists(x) for x in files.values()])

    def create_error_log(self, task_name, *args):
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}"

    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        '''
        Main function to import the resource.
        Internally will cal the steps required to import the 
        data inside the geonode_data database
        '''
        layers = ogr.Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        layer_count = len(layers)
        logger.info(f"Total number of layers available: {layer_count}")
        _exec = self._get_execution_request_object(execution_id)
        for index, layer in enumerate(layers, start=1):
            layer_name = layer.GetName()
            should_be_overrided = _exec.input_params.get("override_existing_layer")
            # should_be_imported check if the user+layername already exists or not
            if should_be_imported(
                layer_name, _exec.user,
                skip_existing_layer=_exec.input_params.get("skip_existing_layer"),
                override_existing_layer=should_be_overrided
            ):
                #update the execution request object
                self._update_execution_request(
                    execution_id=execution_id,
                    last_updated=timezone.now(),
                    log=f"setting up dynamic model for layer: {layer_name} complited: {(100*index)/layer_count}%"
                )
                # setup dynamic model and retrieve the group job needed for tun the async workflow
                _, use_uuid, layer_res = self._setup_dynamic_model(layer, execution_id, should_be_overrided)
                # evaluate if a new alternate is created by the previous flow
                alternate = layer_name if not use_uuid else f"{layer_name}_{execution_id.replace('-', '_')}"
                # create the async task for create the resource into geonode_data with ogr2ogr
                ogr_res = gpkg_ogr2ogr.s(execution_id, files, layer.GetName(), should_be_overrided, alternate)

                # prepare the async chord workflow with the on_success and on_fail methods
                workflow = chord(
                    [layer_res.set(link_error=['gpkg_error_callback']), ogr_res.set(link_error=['gpkg_error_callback'])],
                    body=execution_id
                )(gpkg_next_step.s(execution_id, "importer.import_resource", layer_name, alternate))

        return

    def _setup_dynamic_model(self, layer, execution_id, should_be_overrided):
        '''
        Extract from the geopackage the layers name and their schema
        after the extraction define the dynamic model instances
        '''
        use_uuid = False
        # TODO: finish the creation, is raising issues due the NONE value of the table
        layer_name = layer.GetName()
        foi_schema, created = ModelSchema.objects.get_or_create(
            name=layer.GetName(),
            db_name="datastore",
            managed=False,
            db_table_name=layer_name
        )
        if not created and not should_be_overrided:
            use_uuid = True
            layer_name = f"{layer.GetName()}_{execution_id.replace('-', '_')}"
            foi_schema, created = ModelSchema.objects.get_or_create(
                name=f"{layer.GetName()}_{execution_id.replace('-', '_')}",
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        # define standard field mapping from ogr to django
        dynamic_model, res = self.create_dynamic_model_fields(
            layer=layer,
            dynamic_model_schema=foi_schema,
            overwrite=should_be_overrided,
            execution_id=execution_id,
            layer_name=layer_name
        )
        return dynamic_model, use_uuid, res

    def create_dynamic_model_fields(self, layer, dynamic_model_schema, overwrite, execution_id, layer_name):
        layer_schema = [
            {"name": x.name.lower(), "class_name": self._get_type(x), "null": True}
            for x in layer.schema
        ]
        if layer.GetGeometryColumn():
            layer_schema += [
                {
                    "name": layer.GetGeometryColumn(),
                    "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
                }
            ]

        list_chunked = [layer_schema[i:i + 30] for i in range(0, len(layer_schema), 30)]
        job = group(gpkg_handler.s(execution_id, schema, dynamic_model_schema.id, overwrite, layer_name) for schema in list_chunked)
        return dynamic_model_schema.as_model(), job

    def _update_execution_request(self, execution_id, **kwargs):
        ExecutionRequest.objects.filter(exec_id=execution_id).update(
            status=ExecutionRequest.STATUS_RUNNING, **kwargs
        )

    def _get_execution_request_object(self, execution_id):
        return ExecutionRequest.objects.filter(exec_id=execution_id).first()

    def _get_type(self, _type):
        '''
        Used to get the standard field type in the dynamic_model_field definition
        '''
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.gpkg_handler",
    queue="importer.gpkg_handler",
    max_retries=1,
    acks_late=False,
    ignore_result=False
)
def gpkg_handler(execution_id, fields, dynamic_model_schema_id, overwrite, layer_name):
    def _create_field(dynamic_model_schema, field, _kwargs):
        return FieldSchema(
                    name=field['name'],
                    class_name=field['class_name'],
                    model_schema=dynamic_model_schema,
                    kwargs=_kwargs
                )
    '''
    Create the single dynamic model field for each layer. Is made by a batch of 50 field
    '''
    dynamic_model_schema = ModelSchema.objects.filter(id=dynamic_model_schema_id)
    if not dynamic_model_schema.exists():
        raise DynamicModelError(f"The model with id {dynamic_model_schema_id} does not exists. It may be deleted from the error callback tasks")

    dynamic_model_schema = dynamic_model_schema.first()

    row_to_insert = []
    for field in fields:
        # setup kwargs for the class provided
        if field['class_name'] is None or field['name'] is None:
            logger.error(f"Error during the field creation. The field or class_name is None {field}")
            raise InvalidFieldNameError(f"Error during the field creation. The field or class_name is None {field}")

        _kwargs = {"null": field.get('null', True)}
        if field['class_name'].endswith('CharField'):
            _kwargs = {**_kwargs, **{"max_length": 255}}
    
        # if is a new creation we generate the field model from scratch
        if not overwrite:
            row_to_insert.append(_create_field(dynamic_model_schema, field, _kwargs))
        else:
            # otherwise if is an overwrite, we update the existing one and create the one that does not exists
            _field_exists = FieldSchema.objects.filter(name=field['name'], model_schema=dynamic_model_schema)
            if _field_exists.exists():
                _field_exists.update(
                    class_name=field['class_name'],
                    model_schema=dynamic_model_schema,
                    kwargs=_kwargs
                )
            else:    
                row_to_insert.append(_create_field(dynamic_model_schema, field, _kwargs))
    
    if row_to_insert:
        FieldSchema.objects.bulk_create(row_to_insert, 50)

    del row_to_insert


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.gpkg_ogr2ogr",
    queue="importer.gpkg_ogr2ogr",
    max_retries=1,
    acks_late=False,
    ignore_result=False
)
def gpkg_ogr2ogr(execution_id, files, original_name, override_layer=False, alternate=None):
    '''
    Perform the ogr2ogr command to import he gpkg inside geonode_data
    If the layer should be overwritten, the option is appended dynamically
    '''

    ogr_exe = "/usr/bin/ogr2ogr"
    _uri = settings.GEODATABASE_URL.replace("postgis://", "")
    db_user, db_password = _uri.split('@')[0].split(":")
    db_host, db_port = _uri.split('@')[1].split('/')[0].split(":")
    db_name = _uri.split('@')[1].split("/")[1]

    options = '--config PG_USE_COPY YES '
    options += '-f PostgreSQL PG:" dbname=\'%s\' host=%s port=%s user=\'%s\' password=\'%s\' " ' \
                % (db_name, db_host, db_port, db_user, db_password)
    options += files.get("base_file") + " "
    options += '-lco DIM=2 '
    options += f"-nln {alternate} {original_name}"

    if override_layer:
        options += " -overwrite"

    commands = [ogr_exe] + options.split(" ")
    
    process = Popen(' '.join(commands), stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stderr is not None and stderr != b'':
        raise Exception(stderr)
    return stdout.decode()


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.gpkg_next_step",
    queue="importer.gpkg_next_step"
)
def gpkg_next_step(_, execution_id, actual_step, layer_name, alternate):
    '''
    If the ingestion of the resource is successfuly, the next step for the layer is called
    '''
    from importer.views import import_orchestrator, orchestrator

    _exec = orchestrator.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("store_spatial_files")
    _user = _exec.user
    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, _store_spatial_files, _user.username, execution_id, actual_step, layer_name, alternate)
    )

@importer_app.task(name='gpkg_error_callback')
def error_callback(*args, **kwargs):
    from dynamic_models.schema import ModelSchemaEditor
    # rever eventually the import in ogr2ogr or the creation of the model in case of failing
    alternate = args[0].args[-1]
    
    schema_model = ModelSchema.objects.filter(name=alternate)
    if schema_model.exists():
        schema = ModelSchemaEditor(
            initial_model=alternate,
            db_name="datastore"
        )
        try:
            schema.drop_table(schema_model.first().as_model())
        except Exception as e:
            logger.warning(e.args[0])

        schema_model.delete()

    return 'error'