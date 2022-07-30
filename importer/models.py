import logging

from django.db import models
from django.db.models.signals import post_delete
from django.dispatch import receiver
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.geoserver.signals import geoserver_delete
from geonode.layers.models import Dataset
from geonode.base.models import ResourceBase


logger = logging.getLogger(__name__)


@receiver(post_delete, sender=Dataset)
def delete_dynamic_model(instance, sender, **kwargs):
    '''
    Delete the dynamic relation and the geoserver layer
    '''
    try:
        geoserver_delete(instance.alternate)
        name = instance.alternate.split(":")[1]
        schema = ModelSchema.objects.filter(name=name)
        if schema.exists():
            for field in schema.first().fields.all():
                field.delete()
            ModelSchema.objects.filter(name=name).delete()
        # Removing Field Schema
    except Exception as e:
        logger.error(f"Error during deletion of Dynamic Model schema: {e.args[0]}")


class ResourceHandlerInfo(models.Model):

    """
    Here we save the relation between the geonode resource created and the handler that created that resource
    """
    resource = models.ForeignKey(ResourceBase, blank=False, null=False, on_delete=models.CASCADE)
    handler_module_path = models.CharField(max_length=250, blank=False, null=False)
