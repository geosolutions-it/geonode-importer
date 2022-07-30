from abc import ABC
import logging
from typing import List
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.layers.models import Dataset
logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    '''
    Base abstract handler object
    define the required method needed to define an upload handler
    it must be:
    - provide the tasks list to complete the import
    - validation function
    - method to import the resource
    - create_error_log
    '''

    REGISTRY = []

    ACTIONS = {
        exa.IMPORT.value: (),
        exa.COPY.value: (),
        exa.DELETE.value: (),
        exa.UPDATE.value: (),
    }

    def __str__(self):
        return f"{self.__module__}.{self.__class__.__name__}"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def register(cls):
        BaseHandler.REGISTRY.append(cls)

    @classmethod
    def get_registry(cls):
        return BaseHandler.REGISTRY

    
    @classmethod
    def get_task_list(cls, action) -> tuple:
        if action not in cls.ACTIONS:
            raise Exception("The requested action is not implemented yet")
        return cls.ACTIONS.get(action)

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
    def extract_params_from_data(_data):
        '''
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        '''
        return []
    
    def fixup_name(self, name):
        return name.lower().replace('-', '_')

    def extract_resource_to_publish(self, files, layer_name, alternate):
        '''
        Function to extract the layer name and the CRS from needed in the 
        publishing phase
        [
            {'name': 'alternate or layer_name', 'crs': 'EPSG:25832'}
        ]
        '''
        return NotImplementedError

    @staticmethod
    def create_error_log(exc, task_name, *args):
        '''
        This function will handle the creation of the log error for each message.
        This is helpful and needed, so each handler can specify the log as needed
        '''
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}: {exc}"

    def import_resource(self, files: dict, execution_id: str, **kwargs):
        '''
        Define the step to perform the import of the data
        into the datastore db
        '''
        return NotImplementedError
    
    @staticmethod
    def publish_resources(resources: List[str], catalog, store, workspace):
        '''
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        '''
        return NotImplementedError

    def create_geonode_resource(self, layer_name, alternate, execution_id, resource_type: Dataset = Dataset):
        '''
        Base function to create the resource into geonode. Each handler can specify
        and handle the resource in a different way
        '''
        return NotImplementedError

    def get_ogr2ogr_task_group(self, execution_id, files, layer, should_be_overrided, alternate):
        '''
        implement custom ogr2ogr task group
        '''
        return NotImplementedError
