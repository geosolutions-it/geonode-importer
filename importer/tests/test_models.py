from django.test import TestCase
from dynamic_models.models import ModelSchema, FieldSchema
from geonode.base.populate_test_data import create_single_dataset


class TestModelSchemaSignal(TestCase):
    databases = ("default", "datastore")
        
    def setUp(self):
        self.resource = create_single_dataset(name="test_dataset")
        self.dynamic_model = ModelSchema.objects.create(
            name=self.resource.name,
            db_name="datastore"
        )
        self.dynamic_model_field = FieldSchema.objects.create(
            name="field",
            class_name="django.db.models.IntegerField",
            model_schema=self.dynamic_model,
        )
    
    def test_delete_dynamic_model(self):
        '''
        Ensure that the dynamic model is deleted
        '''
        self.resource.delete()
        self.assertFalse(ModelSchema.objects.filter(name="test_dataset").exists())
        self.assertFalse(FieldSchema.objects.filter(model_schema=self.dynamic_model, name="field").exists())
