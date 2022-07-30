from django.contrib.auth import get_user_model
from django.db import connections
from django.test import SimpleTestCase
from geonode.tests.base import GeoNodeBaseTestSupport
from unittest.mock import patch
from importer.api.exception import InvalidInputFileException

from importer.celery_tasks import copy_dynamic_model, copy_geonode_data_table, copy_geonode_resource, create_dynamic_structure, create_geonode_resource, import_orchestrator, import_resource, orchestrator, publish_resource
from geonode.resource.models import ExecutionRequest
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction
from geonode.base.models import ResourceBase
from geonode.base.populate_test_data import create_single_dataset
from dynamic_models.models import ModelSchema, FieldSchema
from dynamic_models.exceptions import DynamicModelError, InvalidFieldNameError

# Create your tests here.


class TestCeleryTasks(GeoNodeBaseTestSupport):

    def setUp(self):
        self.user = get_user_model().objects.first()
        self.exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=self.user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": {"base_file": "/filepath"},
                    #"override_existing_layer": True,
                    "store_spatial_files": True
                },
        )


    @patch("importer.celery_tasks.orchestrator.perform_next_step")
    def test_import_orchestrator_dont_create_exececution_request_if_not__none(
        self, importer
    ):
        user = get_user_model().objects.first()
        count = ExecutionRequest.objects.count()

        import_orchestrator(
            files={"base_file": "/tmp/file.gpkg"},
            store_spatial_files=True,
            user=user.username,
            execution_id="some value",
        )

        self.assertEqual(count, ExecutionRequest.objects.count())
        importer.assert_called_once()


    @patch("importer.celery_tasks.orchestrator.perform_next_step")
    @patch("importer.celery_tasks.DataStoreManager.input_is_valid")
    def test_import_resource_should_rase_exp_if_is_invalid(
        self, is_valid, importer,
    ):  
        user = get_user_model().objects.first()
    
        exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": "/filepath",
                    "store_spatial_files": True
                }
        )

        is_valid.side_effect = Exception(f"Invalid format type")

        with self.assertRaises(InvalidInputFileException) as _exc:
            import_resource(
                str(exec_id),
                action=ExecutionRequestAction.IMPORT.value,
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
            )
        expected_msg = f"Invalid format type {str(exec_id)}"
        self.assertEqual(expected_msg, str(_exc.exception.detail))
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()
    
    
    @patch("importer.celery_tasks.orchestrator.perform_next_step")
    @patch("importer.celery_tasks.DataStoreManager.input_is_valid")
    @patch("importer.celery_tasks.DataStoreManager.start_import")
    def test_import_resource_should_work(
        self, start_import, is_valid, importer,
    ):  
        is_valid.return_value = True
        user = get_user_model().objects.first()
    
        exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": "/filepath",
                    "store_spatial_files": True
                }
        )

        import_resource(
            str(exec_id),
            resource_type='gpkg',
            action=ExecutionRequestAction.IMPORT.value,
            handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
        )

        start_import.assert_called_once()
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_to_publish")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_should_work(
        self, publish_resources, extract_resource_to_publish, importer,
    ):
        try:
            publish_resources.return_value = True
            extract_resource_to_publish.return_value = [
                {"crs": 12345, "name": "dataset3"}
            ]

            publish_resource(
                str(self.exec_id),
                resource_type='gpkg',
                step_name="publish_resource",
                layer_name="dataset3",
                alternate="alternate_dataset3",
                action=ExecutionRequestAction.IMPORT.value,
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
            )

            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(self.exec_id))
            self.assertEqual(publish_resources.call_count , 1)
            self.assertEqual('importer.publish_resource', req.step)
            importer.assert_called_once()
        finally:
            #cleanup
            if self.exec_id:
                ExecutionRequest.objects.filter(exec_id=str(self.exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_to_publish")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_if_overwrite_should_not_call_the_publishing(
        self, publish_resources, extract_resource_to_publish, importer,
    ):
        try:
            publish_resources.return_value = True
            extract_resource_to_publish.return_value = [
                {"crs": 12345, "name": "dataset3"}
            ]
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=self.user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": {"base_file": "/filepath"},
                    "override_existing_layer": True,
                    "store_spatial_files": True
                },
            )
            publish_resource(
                str(exec_id),
                resource_type='gpkg',
                step_name="publish_resource",
                layer_name="dataset3",
                alternate="alternate_dataset3",
                action=ExecutionRequestAction.IMPORT.value,
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
            )

            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(exec_id))
            self.assertEqual('importer.publish_resource', req.step)            
            publish_resources.assert_not_called()
            importer.assert_called_once()

        finally:
            #cleanup
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    def test_create_geonode_resource(self, import_orchestrator):
        try:

            alternate = "geonode:alternate_foo_dataset"
            self.assertFalse(Dataset.objects.filter(alternate=alternate).exists())

            create_geonode_resource(
                str(self.exec_id),
                resource_type='gpkg',
                step_name="create_geonode_resource",
                layer_name="foo_dataset",
                alternate="alternate_foo_dataset",
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler',
                action='import'
            )


            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(self.exec_id))
            self.assertEqual('importer.create_geonode_resource', req.step)            

            self.assertTrue(Dataset.objects.filter(alternate=alternate).exists())

            import_orchestrator.assert_called_once()

        finally:
            #cleanup
            if Dataset.objects.filter(alternate=alternate).exists():
                Dataset.objects.filter(alternate=alternate).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    def test_copy_geonode_resource_should_raise_exeption_if_the_alternate_not_exists(self, async_call):

        with self.assertRaises(Exception) as e:
            copy_geonode_resource(
                str(self.exec_id),
                "importer.copy_geonode_resource",
                "cloning",
                "invalid_alternate",
                "importer.handlers.gpkg.handler.GPKGFileHandler",
                "copy"
            )
        async_call.assert_not_called()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    def test_copy_geonode_resource(self, async_call):
        alternate = "geonode:cloning"
        new_alternate = None
        try:
            rasource = create_single_dataset(name="cloning")

            exec_id, new_alternate = copy_geonode_resource(
                str(self.exec_id),
                "importer.copy_geonode_resource",
                "cloning",
                rasource.alternate,
                "importer.handlers.gpkg.handler.GPKGFileHandler",
                "copy"
            )

            self.assertTrue(ResourceBase.objects.filter(alternate__icontains=new_alternate).exists())
            async_call.assert_called_once()

        finally:
            #cleanup
            if Dataset.objects.filter(alternate=alternate).exists():
                Dataset.objects.filter(alternate=alternate).delete()
            if new_alternate:
                Dataset.objects.filter(alternate=new_alternate).delete()

class TestDynamicModelSchema(SimpleTestCase):
    databases = ("default", "datastore")

    def setUp(self):
        self.user = get_user_model().objects.first()
        self.exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=self.user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": {"base_file": "/filepath"},
                    #"override_existing_layer": True,
                    "store_spatial_files": True
                },
        )

    def test_create_dynamic_structure_should_raise_error_if_schema_is_not_available(self):
        with self.assertRaises(DynamicModelError) as _exc:
            create_dynamic_structure(
                execution_id=str(self.exec_id),
                fields=[],
                dynamic_model_schema_id=0,
                overwrite=False,
                layer_name="test_layer"
            )
            
        expected_msg = f"The model with id 0 does not exists."
        self.assertEqual(expected_msg, str(_exc.exception))

    def test_create_dynamic_structure_should_raise_error_if_field_class_is_none(self):
        try:
            name = str(self.exec_id)

            schema = ModelSchema.objects.create(name=f"schema_{name}", db_name="datastore")
            dynamic_fields = [
                {"name": "field1", "class_name": None, "null": True},
            ]
            with self.assertRaises(InvalidFieldNameError) as _exc:
                create_dynamic_structure(
                    execution_id=str(self.exec_id),
                    fields=dynamic_fields,
                    dynamic_model_schema_id=schema.pk,
                    overwrite=False,
                    layer_name="test_layer"
                )

            expected_msg = "Error during the field creation. The field or class_name is None {'name': 'field1', 'class_name': None, 'null': True}"
            self.assertEqual(expected_msg, str(_exc.exception))
        finally:
            ModelSchema.objects.filter(name=f"schema_{name}").delete()


    def test_create_dynamic_structure_should_work(self):
        try:
            name = str(self.exec_id)

            schema = ModelSchema.objects.create(name=f"schema_{name}", db_name="datastore")
            dynamic_fields = [
                {"name": "field1", "class_name": "django.contrib.gis.db.models.fields.LineStringField", "null": True},
            ]

            create_dynamic_structure(
                execution_id=str(self.exec_id),
                fields=dynamic_fields,
                dynamic_model_schema_id=schema.pk,
                overwrite=False,
                layer_name="test_layer"
            )

            self.assertTrue(
                FieldSchema.objects.filter(name="field1").exists()
            )
            
        finally:
            ModelSchema.objects.filter(name=f"schema_{name}").delete()
            FieldSchema.objects.filter(name="field1").delete()

    def test_copy_dynamic_model_should_work(self):
        try:
            name = str(self.exec_id)
            # setup model schema to be copied
            schema = ModelSchema.objects.create(name=f"schema_{name}", db_name="datastore")
            FieldSchema.objects.create(
                name=f"field_{name}",
                class_name="django.contrib.gis.db.models.fields.LineStringField",
                model_schema=schema
            )

            copy_dynamic_model(
                exec_id=str(self.exec_id),
                actual_step="copy",
                layer_name=f"schema_{name}",
                alternate=f"geonode:schema_{name}",
                handlers_module_path="importer.handlers.gpkg.handler.GPKGFileHandler",
                action=ExecutionRequestAction.COPY.value,
                kwargs={
                    "original_dataset_alternate": f"geonode:schema_{name}",
                    "new_dataset_alternate": f"geonode:schema_copy_{name}" # this alternate is generated dring the geonode resource copy
                }
            )

            self.assertTrue(
                ModelSchema.objects.filter(name=f"schema_{name}").exists()
            )
            self.assertTrue(
                ModelSchema.objects.filter(name=f"schema_copy_{name}").exists()
            )
            self.assertTrue(
                FieldSchema.objects.filter(model_schema=ModelSchema.objects.get(name=f"schema_copy_{name}")).exists()
            )
            
        finally:
            ModelSchema.objects.filter(name=f"schema_{name}").delete()
            ModelSchema.objects.filter(name=f"geonode:schema_copy_{name}").delete()
            FieldSchema.objects.filter(name=f"field_{name}").delete()

    @patch("importer.celery_tasks.connections")
    def test_copy_geonode_data_table_should_work(self, mock_connection):
        mock_cursor = mock_connection.__getitem__("datastore").cursor.return_value.__enter__.return_value
        ModelSchema.objects.create(name=f"schema_copy_{str(self.exec_id)}", db_name="datastore")

        copy_geonode_data_table(
            exec_id=str(self.exec_id),
            actual_step="copy",
            layer_name=f"schema_{str(self.exec_id)}",
            alternate=f"geonode:schema_{str(self.exec_id)}",
            handlers_module_path="importer.handlers.gpkg.handler.GPKGFileHandler",
            action=ExecutionRequestAction.COPY.value,
            kwargs={
                "original_dataset_alternate": f"geonode:schema_{str(self.exec_id)}",
                "new_dataset_alternate": f"geonode:schema_copy_{str(self.exec_id)}" # this alternate is generated dring the geonode resource copy
            }
        )
        mock_cursor.execute.assert_called_once()
        mock_cursor.execute.assert_called()