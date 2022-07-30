from geonode.resource.manager import ResourceManager
from geonode.geoserver.manager import GeoServerResourceManager
from geonode.base.models import ResourceBase

def error_handler(exc, exec_id=None):
    return f'{str(exc.detail if hasattr(exc, "detail") else exc.args[0])}. Request: {exec_id}'


class ImporterConcreteManager(GeoServerResourceManager):
    '''
    The default GeoNode concrete manager, handle the communication with geoserver
    For this implementation the interaction with geoserver is not needed
    so we are going to override the concrete manager to avoid it
    '''
    def copy(self, instance, uuid, defaults):
        return ResourceBase.objects.get(uuid=uuid)

    def update(self, uuid, **kwargs) -> ResourceBase:
        return ResourceBase.objects.get(uuid=uuid)

custom_resource_manager = ResourceManager(concrete_manager=ImporterConcreteManager())
