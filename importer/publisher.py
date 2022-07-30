import logging
import os
from typing import List

from geonode import settings
from geonode.geoserver.helpers import create_geoserver_db_featurestore
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace
from geoserver.catalog import Catalog
from geonode.utils import OGC_Servers_Handler
from django.utils.module_loading import import_string


logger = logging.getLogger(__name__)


class DataPublisher():
    '''
    Given a list of resources, will publish them on GeoServer
    '''
    def __init__(self, handler_module_path) -> None:
        ogc_server_settings = OGC_Servers_Handler(settings.OGC_SERVER)['default']

        _user, _password = ogc_server_settings.credentials

        self.cat = Catalog(
            service_url=ogc_server_settings.rest,
            username=_user,
            password=_password
        )
        self.workspace = get_geoserver_cascading_workspace(create=True)

        self.handler = import_string(handler_module_path)()

    def extract_resource_to_publish(self, files: dict, action: str, layer_name, alternate=None):
        '''
        Will try to extract the layers name from the original file
        this is needed since we have to publish the resources
        on geoserver by name:
        expected output:
        [
            {'name': 'layer_name', 'crs': 'EPSG:25832'}
        ]
        '''
        
        return self.handler.extract_resource_to_publish(files, action, layer_name, alternate)


    def publish_resources(self, resources: List[str]):
        '''
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        '''
        self.get_or_create_store()
        return self.handler.publish_resources(
            resources=resources,
            catalog=self.cat,
            store=self.store,
            workspace=self.workspace
        )
    
    def get_or_create_store(self):
        '''
        Evaluate if the store exists. if not is created
        '''
        geodatabase = os.environ.get('GEONODE_GEODATABASE', 'geonode_data')
        self.store = self.cat.get_store(
            name=geodatabase,
            workspace=self.workspace
        )
        if not self.store:
            logger.warning(f"The store does not exists: {geodatabase} creating...")
            self.store = create_geoserver_db_featurestore(store_name=geodatabase, workspace=self.workspace.name)
