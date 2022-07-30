import logging
from typing import Optional
from uuid import UUID

from celery import Task
from django.db import connections, transaction
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import ugettext
from dynamic_models.exceptions import DynamicModelError, InvalidFieldNameError
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.base.models import ResourceBase
from geonode.resource.enumerator import ExecutionRequestAction as exa

from importer.api.exception import (CopyResourceException,
                                    InvalidInputFileException,
                                    PublishResourceException,
                                    ResourceCreationException,
                                    StartImportException)
from importer.celery_app import importer_app
from importer.datastore import DataStoreManager
from importer.handlers.gpkg.tasks import SingleMessageErrorHandler
from importer.handlers.utils import create_alternate, drop_dynamic_model_schema
from importer.models import ResourceHandlerInfo
from importer.orchestrator import orchestrator
from importer.publisher import DataPublisher
from importer.settings import (IMPORTER_GLOBAL_RATE_LIMIT,
                               IMPORTER_PUBLISHING_RATE_LIMIT,
                               IMPORTER_RESOURCE_CREATION_RATE_LIMIT)
from importer.utils import error_handler

logger = logging.getLogger(__name__)


class ErrorBaseTaskClass(Task):
    '''
    Basic Error task class. Is common to all the base tasks of the import pahse
    it defines a on_failure method which set the task as "failed" with some extra information
    '''
    max_retries = 3
    track_started=True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.
        _uuid = self._get_uuid(args)
        reason = f"Task FAILED with ID: {_uuid}, reason: {exc}"
        logger.error(reason)
        orchestrator.set_as_failed(
            execution_id=_uuid, reason=str(exc.detail if hasattr(exc, "detail") else exc.args[0])
        )
        self.update_state(
            task_id=task_id,
            state="FAILURE",
            meta={
                "exec_id": _uuid,
                "reason": reason
            }
        )

    def _get_uuid(self, _list):
        for el in _list:
            try:
                UUID(el)
                return el
            except:
                continue


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    max_retries=1,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT,
    task_track_started=True
)
def import_orchestrator(
    self, files: dict, execution_id: str, handler=None, step='start_import', layer_name=None, alternate=None, action=exa.IMPORT.value, **kwargs
):

    '''
    Base task. Is the task responsible to call the orchestrator and redirect the upload to the next step
    mainly is a wrapper for the Orchestrator object.

            Parameters:
                    user (UserModel): user that is performing the request
                    execution_id (UUID): unique ID used to keep track of the execution request
                    step (str): last step performed from the tasks
                    layer_name (str): layer name
                    alternate (str): alternate used to naming the layer
            Returns:
                    None
    '''
    try:
       # extract the resource_type of the layer and retrieve the expected handler

        orchestrator.perform_next_step(
            execution_id=execution_id,
            step=step,
            layer_name=layer_name,
            alternate=alternate,
            handler_module_path=handler,
            action=action,
            kwargs=kwargs
        )

    except Exception as e:
        raise StartImportException(detail=error_handler(e, execution_id))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.import_resource",
    queue="importer.import_resource",
    max_retries=1,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def import_resource(self, execution_id, /, handler_module_path, action, **kwargs):  
    '''
    Task to import the resources.
    NOTE: A validation if done before acutally start the import

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    resource_type (str): extension of the resource type that we want to import
                    The resource type is needed to retrieve the right handler for the resource
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="import_resource",
            step=ugettext("importer.import_resource"),
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        # initiating the data store manager
        _datastore = DataStoreManager(_files, handler_module_path, _exec.user, execution_id)

        # starting file validation
        if not _datastore.input_is_valid():
            raise Exception("dataset is invalid")

        _datastore.start_import(execution_id)

        '''
        The orchestrator to proceed to the next step, should be called by the hander
        since the call to the orchestrator can changed based on the handler
        called. See the GPKG handler gpkg_next_step task
        '''
        return self.name, execution_id

    except Exception as e:
        raise InvalidInputFileException(detail=error_handler(e, execution_id))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.publish_resource",
    queue="importer.publish_resource",
    max_retries=3,
    rate_limit=IMPORTER_PUBLISHING_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def publish_resource(
    self,
    execution_id: str,
    /,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None,
    handler_module_path: str = None,
    action: str = None,
    **kwargs
):
    '''
    Task to publish a single resource in geoserver.
    NOTE: If the layer should be overwritten, for now we are skipping this feature
        geoserver is not ready yet

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    step_name (str): step name example: importer.publish_resource
                    layer_name (UUID): name of the resource example: layer
                    alternate (UUID): alternate of the resource example: layer_alternate
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="publish_resource",
            step=ugettext("importer.publish_resource"),
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)
        _files = _exec.input_params.get("files")
        _overwrite = _exec.input_params.get("override_existing_layer")
        
        # for now we dont heve the overwrite option in GS, skipping will we talk with the GS team
        if not _overwrite:
            _publisher = DataPublisher(handler_module_path)

            # extracting the crs and the resource name, are needed for publish the resource
            _metadata = _publisher.extract_resource_to_publish(_files, action, layer_name, alternate)
            if _metadata:
                # we should not publish resource without a crs

                _publisher.publish_resources(_metadata)

                # updating the execution request status
                orchestrator.update_execution_request_status(
                    execution_id=execution_id,
                    last_updated=timezone.now(),
                    celery_task_request=self.request
                )
            else:
                logger.error("Only resources with a CRS provided can be published")
                raise PublishResourceException("Only resources with a CRS provided can be published")

        # at the end recall the import_orchestrator for the next step

        import_orchestrator.apply_async(
            (_files, execution_id, handler_module_path, step_name, layer_name, alternate, action)
        )
        return self.name, execution_id

    except Exception as e:
        raise PublishResourceException(detail=error_handler(e, execution_id))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.create_geonode_resource",
    queue="importer.create_geonode_resource",
    max_retries=1,
    rate_limit=IMPORTER_RESOURCE_CREATION_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def create_geonode_resource(
    self,
    execution_id: str,
    /,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None,
    handler_module_path: str = None,
    action: str = None,
    **kwargs
):
    '''
    Create the GeoNode resource and the relatives information associated
    NOTE: for gpkg we dont want to handle sld and XML files

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    resource_type (str): extension of the resource type that we want to import
                        The resource type is needed to retrieve the right handler for the resource
                    step_name (str): step name example: importer.publish_resource
                    layer_name (UUID): name of the resource example: layer
                    alternate (UUID): alternate of the resource example: layer_alternate
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="create_geonode_resource",
            step=ugettext("importer.create_geonode_resource"),
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        hander = import_string(handler_module_path)()

        resource = hander.create_geonode_resource(
            layer_name=layer_name,
            alternate=alternate, 
            execution_id=execution_id
        )

        ResourceHandlerInfo.objects.create(
            handler_module_path=handler_module_path,
            resource=resource
        )
        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, execution_id, handler_module_path, step_name, layer_name, alternate, action)
        )
        return self.name, execution_id

    except Exception as e:
        raise ResourceCreationException(detail=error_handler(e))


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.copy_geonode_resource",
    queue="importer.copy_geonode_resource",
    max_retries=1,
    rate_limit=IMPORTER_RESOURCE_CREATION_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def copy_geonode_resource(exec_id, actual_step, layer_name, alternate, handler_module_path, action, **kwargs):
    '''
    Copy the geonode resource and create a new one. an assert is performed to be sure that the new resource
    have the new generated alternate
    '''
    from importer.celery_tasks import import_orchestrator
    from importer.utils import custom_resource_manager
    try:
        resource = ResourceBase.objects.filter(alternate=alternate)
        if not resource.exists():
            raise Exception("The resource requested does not exists")
        resource = resource.first()

        _exec = orchestrator.get_execution_object(exec_id)

        new_alternate = create_alternate(resource.title, exec_id)

        workspace = resource.alternate.split(':')[0]

        data_to_update = {
            "alternate": f'{workspace}:{new_alternate}', 
            'name': new_alternate
        }

        if _exec.input_params.get("title"):
            data_to_update['title'] = _exec.input_params.get("title")

        new_resource = custom_resource_manager.copy(
            resource,
            owner=resource.owner,
            defaults=data_to_update,
        )

        ResourceHandlerInfo.objects.create(
            resource=new_resource,
            handler_module_path=handler_module_path
        )

        assert f'{workspace}:{new_alternate}' == new_resource.alternate

        orchestrator.update_execution_request_status(
            execution_id=str(_exec.exec_id),
            input_params={**_exec.input_params, **{"instance": resource.pk}},
            output_params={
                "output": {"uuid": str(new_resource.uuid)}
            }
        )

        additional_kwargs = {
            "original_dataset_alternate": resource.alternate,
            "new_dataset_alternate": new_resource.alternate
        }

        task_params = ({}, exec_id, handler_module_path, actual_step, layer_name, new_alternate, action)

        import_orchestrator.apply_async(task_params, additional_kwargs)

    except Exception as e:
        raise CopyResourceException(detail=e)
    return exec_id, new_alternate


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.create_dynamic_structure",
    queue="importer.create_dynamic_structure",
    max_retries=1,
    acks_late=False,
    ignore_result=False,
    task_track_started=True
)
def create_dynamic_structure(execution_id: str, fields: dict, dynamic_model_schema_id: int, overwrite: bool, layer_name: str):
    def _create_field(dynamic_model_schema, field, _kwargs):
        # common method to define the Field Schema object
        return FieldSchema(
                    name=field['name'],
                    class_name=field['class_name'],
                    model_schema=dynamic_model_schema,
                    kwargs=_kwargs
                )
    '''
    Create the single dynamic model field for each layer. Is made by a batch of 30 field
    '''
    dynamic_model_schema = ModelSchema.objects.filter(id=dynamic_model_schema_id)
    if not dynamic_model_schema.exists():
        raise DynamicModelError(f"The model with id {dynamic_model_schema_id} does not exists.")

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
        # the build creation improves the overall permformance with the DB
        FieldSchema.objects.bulk_create(row_to_insert, 30)

    del row_to_insert
    return "dynamic_model", layer_name, execution_id


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.copy_dynamic_model",
    queue="importer.copy_dynamic_model",
    task_track_started=True
)
def copy_dynamic_model(exec_id, actual_step, layer_name, alternate, handlers_module_path, action, **kwargs):
    '''
    Once the base resource is copied, is time to copy also the dynamic model
    '''
    original_dataset_alternate = kwargs.get("kwargs").get("original_dataset_alternate")
    new_dataset_alternate = kwargs.get("kwargs").get("new_dataset_alternate")

    from importer.celery_tasks import import_orchestrator
    try:
        dynamic_schema = ModelSchema.objects.filter(name=original_dataset_alternate.split(':')[1])
        alternative_dynamic_schema = ModelSchema.objects.filter(name=new_dataset_alternate.split(':')[1])

        if dynamic_schema.exists() and not alternative_dynamic_schema.exists():
            # Creating the dynamic schema object
            new_schema = dynamic_schema.first()
            new_schema.name = new_dataset_alternate.split(':')[1]
            new_schema.pk = None
            new_schema.save()
            # create the field_schema object
            fields = []
            for field in dynamic_schema.first().fields.all():
                obj = field
                obj.model_schema=new_schema
                obj.pk = None
                fields.append(obj)

            FieldSchema.objects.bulk_create(fields)

        task_params = ({}, exec_id, handlers_module_path, actual_step, layer_name, alternate, action)
        # for some reason celery will always put the kwargs into a key kwargs
        # so we need to remove it
        kwargs = kwargs.get('kwargs') if "kwargs" in kwargs else kwargs

        import_orchestrator.apply_async(task_params, kwargs)
    except Exception as e:
        raise CopyResourceException(detail=e)
    return exec_id, kwargs


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.copy_geonode_data_table",
    queue="importer.copy_geonode_data_table",
    task_track_started=True
)
def copy_geonode_data_table(exec_id, actual_step, layer_name, alternate, handlers_module_path, action, **kwargs):
    '''
    Once the base resource is copied, is time to copy also the dynamic model
    '''
    original_dataset_alternate = kwargs.get("kwargs").get("original_dataset_alternate").split(':')[1]
    new_dataset_alternate = kwargs.get("kwargs").get("new_dataset_alternate").split(':')[1]

    from importer.celery_tasks import import_orchestrator
    try:
        
        db_name = ModelSchema.objects.filter(name=new_dataset_alternate).first().db_name
        with transaction.atomic():
            with connections[db_name].cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE {new_dataset_alternate} AS TABLE {original_dataset_alternate};"
                )

        task_params = ({}, exec_id, handlers_module_path, actual_step, layer_name, alternate, action)

        import_orchestrator.apply_async(task_params, kwargs)
    except Exception as e:
        raise CopyResourceException(detail=e)
    return exec_id, kwargs


@importer_app.task(name='dynamic_model_error_callback')
def dynamic_model_error_callback(*args, **kwargs):
    # revert eventually the import in ogr2ogr or the creation of the model in case of failing
    alternate = args[0].args[-1]
    schema_model = ModelSchema.objects.filter(name=alternate).first()

    drop_dynamic_model_schema(schema_model)

    return 'error'
