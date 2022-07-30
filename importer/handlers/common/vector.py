import json
import logging
import os
from subprocess import PIPE, Popen
from typing import List
from celery import chord, group

from django.conf import settings
from dynamic_models.models import ModelSchema
from geonode.base.models import ResourceBase
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace
from geonode.layers.models import Dataset
from importer.celery_tasks import create_dynamic_structure
from importer.handlers.base import BaseHandler
from importer.handlers.gpkg.tasks import SingleMessageErrorHandler
from importer.handlers.utils import GEOM_TYPE_MAPPING, STANDARD_TYPE_MAPPING, drop_dynamic_model_schema
from geonode.resource.manager import resource_manager
from geonode.base.models import ResourceBase
from geonode.resource.models import ExecutionRequest
from osgeo import ogr
from importer.api.exception import ImportException
from importer.celery_app import importer_app

from importer.handlers.utils import create_alternate, should_be_imported

logger = logging.getLogger(__name__)


class BaseVectorFileHandler(BaseHandler):
    '''
    Handler to import GeoJson files into GeoNode data db
    It must provide the task_lists required to comple the upload
    '''

    @property
    def default_geometry_column_name(self):
        return "geometry"

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps
        """
        return NotImplementedError
    
    @staticmethod
    def can_handle(_data) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        return False

    @staticmethod
    def can_do(action) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        return action in BaseHandler.ACTIONS

    @staticmethod
    def create_error_log(exc, task_name, *args):
        '''
        This function will handle the creation of the log error for each message.
        This is helpful and needed, so each handler can specify the log as needed
        '''
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}: {exc}"

    @staticmethod
    def extract_params_from_data(_data, action=None):
        '''
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        '''
        if action == exa.COPY.value:
            title = json.loads(_data.get("defaults"))
            return {
                "title": title.pop('title')
            }, _data

        return {
            "skip_existing_layers": _data.pop('skip_existing_layers', "False"),
            "override_existing_layer": _data.pop('override_existing_layer', "False"),
            "store_spatial_file": _data.pop("store_spatial_files", "True"),
        }, _data

    def extract_resource_to_publish(self, files, action, layer_name, alternate):
        if action == exa.COPY.value:
            workspace = get_geoserver_cascading_workspace(create=False)
            full_alternate = alternate if ':' in alternate else f"{workspace.name}:{alternate}"
            return [
                {
                    "name": alternate,
                    "crs": ResourceBase.objects.get(alternate=full_alternate).srid
                }
            ]

        layers = self.get_ogr2ogr_driver().Open(files.get("base_file"))
        if not layers:
            return []
        return [
            {
                "name": alternate or layer_name,
                "crs" : (
                    f"{_l.GetSpatialRef().GetAuthorityName(None)}:{_l.GetSpatialRef().GetAuthorityCode('PROJCS') or 4326}"
                    if _l.GetSpatialRef() else "EPSG:4326"
                )
            } 
            for _l in layers
            if self.fixup_name(_l.GetName()) == layer_name
        ]

    def get_ogr2ogr_driver(self):
        '''
        Should return the Driver object that is used to open the layers via OGR2OGR
        '''
        return None

    @staticmethod
    def create_ogr2ogr_command(files, original_name, override_layer, alternate):
        '''
        Define the ogr2ogr command to be executed.
        This is a default command that is needed to import a vector file
        '''
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

        return options


    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        '''
        Main function to import the resource.
        Internally will call the steps required to import the 
        data inside the geonode_data database
        '''
        layers = self.get_ogr2ogr_driver().Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        layer_count = len(layers)
        logger.info(f"Total number of layers available: {layer_count}")
        _exec = self._get_execution_request_object(execution_id)
        dynamic_model = None
        try:
            # start looping on the layers available
            for index, layer in enumerate(layers, start=1):

                layer_name = self.fixup_name(layer.GetName())

                should_be_overrided = _exec.input_params.get("override_existing_layer")
                # should_be_imported check if the user+layername already exists or not
                if should_be_imported(
                    layer_name, _exec.user,
                    skip_existing_layer=_exec.input_params.get("skip_existing_layer"),
                    override_existing_layer=should_be_overrided
                ) and layer.GetGeometryColumn() is not None:
                    #update the execution request object
                    # setup dynamic model and retrieve the group task needed for tun the async workflow
                    dynamic_model, alternate, celery_group = self.setup_dynamic_model(layer, execution_id, should_be_overrided, username=_exec.user)
                    # evaluate if a new alternate is created by the previous flow
                    # create the async task for create the resource into geonode_data with ogr2ogr
                    ogr_res = self.get_ogr2ogr_task_group(execution_id, files, layer.GetName().lower(), should_be_overrided, alternate)
                    # prepare the async chord workflow with the on_success and on_fail methods
                    workflow = chord(
                        group(celery_group.set(link_error=['dynamic_model_error_callback']), ogr_res.set(link_error=['dynamic_model_error_callback']))
                    )(import_next_step.s(
                        execution_id,
                        str(self), # passing the handler module path
                        "importer.import_resource",
                        layer_name,
                        alternate
                    ))
        except Exception as e:
            logger.error(e)
            if dynamic_model:
                '''
                In case of fail, we want to delete the dynamic_model schema and his field
                to keep the DB in a consistent state
                '''
                drop_dynamic_model_schema(dynamic_model)
            raise e
        return

    def setup_dynamic_model(self, layer: ogr.Layer, execution_id: str, should_be_overrided: bool, username: str):
        '''
        Extract from the geopackage the layers name and their schema
        after the extraction define the dynamic model instances
        Returns:
            - dynamic_model as model, so the actual dynamic instance
            - alternate -> the alternate of the resource which contains (if needed) the uuid
            - celery_group -> the celery group of the field creation
        '''

        layer_name = self.fixup_name(layer.GetName())
        workspace = get_geoserver_cascading_workspace(create=False)
        user_datasets = Dataset.objects.filter(owner=username, alternate=f'{workspace.name}:{layer_name}')
        dynamic_schema = ModelSchema.objects.filter(name=layer_name)

        dynamic_schema_exists = dynamic_schema.exists()
        dataset_exists = user_datasets.exists()

        if dataset_exists and dynamic_schema_exists and should_be_overrided:
            '''
            If the user have a dataset, the dynamic model has already been created and is in override mode,
            we just take the dynamic_model to override the existing one
            '''
            dynamic_schema = dynamic_schema.get()
        elif (
                dataset_exists and not dynamic_schema_exists
            ) or (
                not dataset_exists and not dynamic_schema_exists
            ):
            '''
            cames here when is a new brand upload or when (for any reasons) the dataset exists but the
            dynamic model has not been created before
            '''
            dynamic_schema = ModelSchema.objects.create(
                name=layer_name,
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        elif (
            not dataset_exists and dynamic_schema_exists
        ) or (
            dataset_exists and dynamic_schema_exists and not should_be_overrided
        ):
            '''
            it comes here when the layer should not be overrided so we append the UUID
            to the layer to let it proceed to the next steps
            '''
            layer_name = create_alternate(layer_name, execution_id)
            dynamic_schema, _ = ModelSchema.objects.get_or_create(
                name=layer_name,
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        else:
            raise ImportException("Error during the upload of the gpkg file. The dataset does not exists")

        # define standard field mapping from ogr to django
        dynamic_model, celery_group = self.create_dynamic_model_fields(
            layer=layer,
            dynamic_model_schema=dynamic_schema,
            overwrite=should_be_overrided,
            execution_id=execution_id,
            layer_name=layer_name
        )
        return dynamic_model, layer_name, celery_group

    def create_dynamic_model_fields(self, layer: str, dynamic_model_schema: ModelSchema, overwrite: bool, execution_id: str, layer_name: str):
        # retrieving the field schema from ogr2ogr and converting the type to Django Types
        layer_schema = [
            {"name": x.name.lower(), "class_name": self._get_type(x), "null": True}
            for x in layer.schema
        ]
        if layer.GetGeometryColumn() or self.default_geometry_column_name:
            # the geometry colum is not returned rom the layer.schema, so we need to extract it manually
            layer_schema += [
                {
                    "name": layer.GetGeometryColumn() or self.default_geometry_column_name,
                    "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
                }
            ]

        # ones we have the schema, here we create a list of chunked value
        # so the async task will handle max of 30 field per task
        list_chunked = [layer_schema[i:i + 30] for i in range(0, len(layer_schema), 30)]

        # definition of the celery group needed to run the async workflow.
        # in this way each task of the group will handle only 30 field
        celery_group = group(create_dynamic_structure.s(execution_id, schema, dynamic_model_schema.id, overwrite, layer_name) for schema in list_chunked)

        return dynamic_model_schema, celery_group

    @staticmethod
    def publish_resources(resources: List[str], catalog, store, workspace):
        '''
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        '''
        for _resource in resources:
            try:
                catalog.publish_featuretype(
                    name=_resource.get("name"),
                    store=store,
                    native_crs=_resource.get("crs"),
                    srs=_resource.get("crs"),
                    jdbc_virtual_table=_resource.get("name")
                )
            except Exception as e:
                if f"Resource named {_resource.get('name')} already exists in store:" in str(e):
                    continue
                raise e
        return True

    def create_geonode_resource(self, layer_name, alternate, execution_id, resource_type: Dataset = Dataset):
        '''
        Base function to create the resource into geonode. Each handler can specify
        and handle the resource in a different way
        '''
        saved_dataset = resource_type.objects.filter(alternate__icontains=alternate)

        _exec = self._get_execution_request_object(execution_id)

        workspace = getattr(settings, "DEFAULT_WORKSPACE", getattr(settings, "CASCADE_WORKSPACE", "geonode"))
            # if the layer exists, we just update the information of the dataset by
            # let it recreate the catalogue
        if saved_dataset.exists():
            saved_dataset = saved_dataset.first()
        else:
                # if it not exists, we create it from scratch
            if not saved_dataset.exists() and _exec.input_params.get("override_existing_layer", False):
                logger.warning(f"The dataset required {alternate} does not exists, but an overwrite is required, the resource will be created")
            saved_dataset = resource_manager.create(
                    None,
                    resource_type=resource_type,
                    defaults=dict(
                        name=alternate,
                        workspace=workspace,
                        store=os.environ.get('GEONODE_GEODATABASE', 'geonode_data'),
                        subtype='vector',
                        alternate=f'{workspace}:{alternate}',
                        dirty_state=True,
                        title=layer_name,
                        owner=_exec.user,
                        files=list(_exec.input_params.get("files", {}).values()),
                    )
                )

        self.handle_xml_file(saved_dataset, _exec)
        self.handle_sld_file(saved_dataset, _exec)

        resource_manager.set_thumbnail(None, instance=saved_dataset)

        ResourceBase.objects.filter(alternate=alternate).update(dirty_state=False)

        saved_dataset.refresh_from_db()
        return saved_dataset

    def handle_xml_file(self, saved_dataset, _exec):
        _path = _exec.input_params.get("files", {}).get("xml_file", "")
        resource_manager.update(None,
                    instance=saved_dataset,
                    xml_file=_path,
                    metadata_uploaded=True if _path else False,
                    vals={"dirty_state": True}
                )

    def handle_sld_file(self, saved_dataset, _exec):
        _path = _exec.input_params.get("files", {}).get("sld_file", "")
        resource_manager.exec(
                'set_style',
                None,
                instance=saved_dataset,
                sld_file=_exec.input_params.get("files", {}).get("sld_file", ""),
                sld_uploaded=True if _path else False,
                vals={"dirty_state": True}
            )

    def get_ogr2ogr_task_group(self, execution_id, files, layer, should_be_overrided, alternate):
        '''
        In case the OGR2OGR is different from the default one, is enough to ovverride this method
        and return the celery task object needed
        '''
        handler_module_path = str(self)
        return import_with_ogr2ogr.s(execution_id, files, layer.lower(), handler_module_path, should_be_overrided, alternate)

    def _get_execution_request_object(self, execution_id: str):
        return ExecutionRequest.objects.filter(exec_id=execution_id).first()

    def _get_type(self, _type: str):
        '''
        Used to get the standard field type in the dynamic_model_field definition
        '''
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))


@importer_app.task(
    name="importer.import_next_step",
    queue="importer.import_next_step",
    task_track_started=True
)
def import_next_step(_, execution_id: str, handlers_module_path, actual_step: str, layer_name: str, alternate:str):
    '''
    If the ingestion of the resource is successfuly, the next step for the layer is called
    '''
    from importer.celery_tasks import import_orchestrator, orchestrator

    _exec = orchestrator.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, execution_id, handlers_module_path, actual_step, layer_name, alternate, exa.IMPORT.value)
    )
    return "import_next_step", alternate, execution_id


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.import_with_ogr2ogr",
    queue="importer.import_with_ogr2ogr",
    max_retries=1,
    acks_late=False,
    ignore_result=False,
    task_track_started=True
)
def import_with_ogr2ogr(execution_id: str, files: dict, original_name:str, handler_module_path, override_layer=False, alternate=None):
    '''
    Perform the ogr2ogr command to import he gpkg inside geonode_data
    If the layer should be overwritten, the option is appended dynamically
    ''' 
    from importer.celery_tasks import orchestrator

    ogr_exe = "/usr/bin/ogr2ogr"

    options = orchestrator.load_handler(handler_module_path)\
        .create_ogr2ogr_command(files, original_name, override_layer, alternate)

    commands = [ogr_exe] + options.split(" ")
    
    process = Popen(' '.join(commands), stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stderr is not None and stderr != b'' and b'Warning' not in stderr:
        raise Exception(stderr)
    return "ogr2ogr", alternate, execution_id
