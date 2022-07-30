import logging
import os
from uuid import UUID

from celery import states
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.utils import timezone
from django_celery_results.models import TaskResult
from geonode.base.enumerations import (STATE_INVALID, STATE_PROCESSED,
                                       STATE_RUNNING)
from geonode.resource.models import ExecutionRequest
from geonode.upload.models import Upload
from typing import Optional
from django.utils.module_loading import import_string

from importer.api.exception import ImportException
from importer.celery_app import importer_app
from importer.handlers.base import BaseHandler
from importer.utils import error_handler

logger = logging.getLogger(__name__)


class ImportOrchestrator:
    ''''
    Main import object. Is responsible to handle all the execution steps
    Using the ExecutionRequest object, will extrapolate the information and
    it call the next step of the import pipeline
    Params: 
    
    enable_legacy_upload_status default=True: if true, will save the upload progress
        also in the legacy upload system
    '''
    def __init__(self, enable_legacy_upload_status=True) -> None:
        self.enable_legacy_upload_status = enable_legacy_upload_status

    def get_handler(self, _data) -> Optional[BaseHandler]:
        """
        If is part of the supported format, return the handler which can handle the import
        otherwise return None
        """
        for handler in BaseHandler.get_registry():
            if handler.can_handle(_data):
                return handler()
        logger.error("Handler not found, fallback on the legacy upload system")
        return None

    def load_handler(self, module_path):
        try:
            return import_string(module_path)
        except Exception as e:
            raise ImportException(detail=f"The handler is not available: {module_path}")

    def get_execution_object(self, exec_id):
        '''
        Returns the ExecutionRequest object with the detail about the 
        current execution
        '''
        req = ExecutionRequest.objects.filter(exec_id=exec_id)
        if not req.exists():
            raise ImportException("The selected UUID does not exists")
        return req.first()

    def perform_next_step(self, execution_id: str, action: str, handler_module_path: str, step: str = None, layer_name:str = None, alternate:str = None, **kwargs) -> None:
        '''
        It takes the executionRequest detail to extract which was the last step
        and take from the task_lists provided by the ResourceType handler
        which will be the following step. if empty a None is returned, otherwise
        in async the next step is called
        '''
        try:
            _exec_obj = self.get_execution_object(str(execution_id))
            if step is None:
                step = _exec_obj.step

            # retrieve the task list for the resource_type
            tasks = self.load_handler(handler_module_path).get_task_list(action=action)
            # getting the index
            _index = tasks.index(step) + 1
            if _index == 1:
                '''
                Means that the first task is available and we set the executions state as running
                So is updated only at the beginning keeping it in a consistent state
                '''
                self.update_execution_request_status(
                    execution_id=str(_exec_obj.exec_id),
                    status=ExecutionRequest.STATUS_RUNNING
                )
            # finding in the task_list the last step done
            remaining_tasks = tasks[_index:] if not _index >= len(tasks) else []
            if not remaining_tasks:
                # The list of task is empty, it means that the process is finished
                self.evaluate_execution_progress(execution_id)
                return
            # getting the next step to perform
            next_step = next(iter(remaining_tasks))
            # calling the next step for the resource

            # defining the tasks parameter for the step
            task_params = (str(execution_id), handler_module_path, action)
            logger.info(f"STARTING NEXT STEP {next_step}")

            if layer_name and alternate:
                # if the layer and alternate is provided, means that we are executing the step specifically for a layer
                # so we add this information to the task_parameters to be sent to the next step
                logger.info(f"STARTING NEXT STEP {next_step} for resource: {layer_name}, alternate {alternate}")

                '''
                If layer name and alternate are provided, are sent as an argument
                for the next task step
                '''
                task_params = (
                        str(execution_id),
                        next_step,
                        layer_name,
                        alternate,
                        handler_module_path,
                        action
                    )

            # continuing to the next step
            importer_app.tasks.get(next_step).apply_async(task_params, kwargs)
            return execution_id

        except StopIteration:
            # means that the expected list of steps is completed
            logger.info("The whole list of tasks has been processed")
            self.set_as_completed(execution_id)
            return
        except Exception as e:
            self.set_as_failed(execution_id, reason=error_handler(e, execution_id))
            raise e

    def set_as_failed(self, execution_id, reason=None):
        '''
        Utility method to set the ExecutionRequest object to fail
        '''
        self.update_execution_request_status(
                execution_id=str(execution_id),
                status=ExecutionRequest.STATUS_FAILED,
                finished=timezone.now(),
                last_updated=timezone.now(),
                log=reason,
                legacy_status=STATE_INVALID
            )

    def set_as_completed(self, execution_id):
        '''
        Utility method to set the ExecutionRequest object to fail
        '''
        self.update_execution_request_status(
                execution_id=str(execution_id),
                status=ExecutionRequest.STATUS_FINISHED,
                finished=timezone.now(),
                last_updated=timezone.now(),
                legacy_status=STATE_PROCESSED
            )

    def evaluate_execution_progress(self, execution_id):
        '''
        The execution id is a mandatory argument for the task
        We use that to filter out all the task execution that are still in progress.
        if any is failed, we raise it.
        '''
        execution_id = str(execution_id)  # force it as string to be sure
        lower_exec_id = execution_id.replace('-', '_').lower()
        exec_result = TaskResult.objects.filter(
            Q(task_args__icontains=lower_exec_id) | Q(task_kwargs__icontains=lower_exec_id) | Q(result__icontains=lower_exec_id)
            | Q(task_args__icontains=execution_id) | Q(task_kwargs__icontains=execution_id) | Q(result__icontains=execution_id)
        )
        # .all() is needed since we want to have the last status on the DB without take in consideration the cache
        if exec_result.all().exclude(Q(status=states.SUCCESS) | Q(status=states.FAILURE)).exists():
            logger.info(f"Execution progress with id {execution_id} is not finished yet, continuing")
            return
        elif exec_result.all().filter(status=states.FAILURE).exists():
            '''
            Should set it fail if all the execution are done and at least 1 is failed
            '''
            failed = [x.task_id for x in exec_result.filter(status=states.FAILURE)]
            _log_message = f"For the execution ID {execution_id} The following celery task are failed: {failed}"
            logger.error(_log_message)
            raise ImportException("One or more dataset raises an error during the import, please check the logs")
        else:
            logger.info(f"Execution with ID {execution_id} is completed. All tasks are done")
            self.set_as_completed(execution_id)


    def create_execution_request(
        self,
        user: get_user_model,
        func_name: str,
        step: str,
        input_params: dict = {},
        resource=None,
        legacy_upload_name="",
        action=None,
        name=None,
    ) -> UUID:
        """
        Create an execution request for the user. Return the UUID of the request
        """
        execution = ExecutionRequest.objects.create(
            user=user,
            geonode_resource=resource,
            func_name=func_name,
            step=step,
            input_params=input_params,
            action=action,
            name=name
        )
        if self.enable_legacy_upload_status:
            # getting the package name from the base_filename
            Upload.objects.create(
                name=legacy_upload_name or os.path.basename(input_params.get("files", {}).get("base_file")),
                state=STATE_RUNNING,
                user=user,
                metadata={
                    **{
                        "func_name": func_name,
                        "step": step,
                        "exec_id": str(execution.exec_id),
                    },
                    **input_params,
                },
            )
        return execution.exec_id

    def update_execution_request_status(self, execution_id, status=None, legacy_status=STATE_RUNNING, celery_task_request=None, **kwargs):
        '''
        Update the execution request status and also the legacy upload status if the
        feature toggle is enabled
        '''
        if status is not None:
            kwargs['status'] = status

        ExecutionRequest.objects.filter(exec_id=execution_id).update(**kwargs)

        if self.enable_legacy_upload_status:
            Upload.objects.filter(metadata__contains=execution_id).update(
                state=legacy_status, complete=True, metadata={**kwargs, **{"exec_id": execution_id}}
            )
        if celery_task_request:
            TaskResult.objects.filter(task_id=celery_task_request.id)\
                .update(task_args=celery_task_request.args)

orchestrator = ImportOrchestrator(enable_legacy_upload_status=False)
