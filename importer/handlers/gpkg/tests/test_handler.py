
from celery import group
from django.test import TestCase
from mock import patch
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from django.contrib.auth import get_user_model
from importer.handlers.gpkg.handler import GPKGFileHandler
from importer import project_dir
from importer.orchestrator import orchestrator
from geonode.upload.models import UploadParallelismLimit
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.base.populate_test_data import create_single_dataset
from geonode.resource.models import ExecutionRequest
from dynamic_models.models import ModelSchema
from osgeo import ogr


class TestGPKGHandler(TestCase):
    databases = ("default", "datastore")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = GPKGFileHandler()
        cls.valid_gpkg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.invalid_gpkg = f"{project_dir}/tests/fixture/invalid.gpkg"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_gpkg}
        cls.valid_files = {"base_file": cls.valid_gpkg}
        cls.owner = get_user_model().objects.first()
        cls.layer = create_single_dataset(name='stazioni_metropolitana', owner=cls.owner)


    def test_task_list_is_the_expected_one(self):
        expected = (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource"
        )
        self.assertEqual(len(self.handler.ACTIONS['import']), 4)
        self.assertTupleEqual(expected, self.handler.ACTIONS['import'])

    def test_is_valid_should_raise_exception_if_the_gpkg_is_invalid(self):
        with self.assertRaises(InvalidGeopackageException) as _exc:
            self.handler.is_valid(files=self.invalid_files, user=self.user)
        
        self.assertIsNotNone(_exc)
        self.assertTrue(
            "Layer names must start with a letter, and valid characters are lowercase a-z, numbers or underscores" in str(_exc.exception.detail)
        )

    def test_is_valid_should_raise_exception_if_the_parallelism_is_met(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(slug="default_max_parallel_uploads")
        old_value = parallelism.max_number
        try:
            if not created:
                UploadParallelismLimit.objects.filter(slug="default_max_parallel_uploads").update(max_number=0)

            with self.assertRaises(UploadParallelismLimitException) as _exc:
                self.handler.is_valid(files=self.valid_files, user=self.user)
            
        finally:
            parallelism.max_number = old_value
            parallelism.save()


    def test_is_valid_should_raise_exception_if_layer_are_greater_than_max_parallel_upload(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(slug="default_max_parallel_uploads")
        old_value = parallelism.max_number
        try:
            if not created:
                UploadParallelismLimit.objects.filter(slug="default_max_parallel_uploads").update(max_number=1)

            with self.assertRaises(UploadParallelismLimitException) as _exc:
                self.handler.is_valid(files=self.valid_files, user=self.user)
            
        finally:
            parallelism.max_number = old_value
            parallelism.save()


    def test_is_valid_should_pass_with_valid_gpkg(self):
        self.handler.is_valid(files=self.valid_files, user=self.user)


    def test_create_error_log(self):
        '''
        Should return the formatted way for the log of the handler
        '''
        actual = self.handler.create_error_log(
            Exception("my exception"),
            "foo_task_name",
            *["exec_id", "layer_name", "alternate"]
        )
        expected = f"Task: foo_task_name raised an error during actions for layer: alternate: my exception"
        self.assertEqual(expected, actual)

    def test_create_dynamic_model_fields(self):
        try:
            # Prepare the test
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={
                    "files": self.valid_files,
                    "skip_existing_layer": True
                },
            )
            schema, _ = ModelSchema.objects.get_or_create(
                name="test_handler",
                db_name="datastore"
            )
            layers = ogr.Open(self.valid_gpkg)

            # starting the tests
            dynamic_model, celery_group = self.handler.create_dynamic_model_fields(
                layer=[x for x in layers][0],
                dynamic_model_schema=schema,
                overwrite=False,
                execution_id=str(exec_id),
                layer_name="stazioni_metropolitana"
            )

            self.assertIsNotNone(dynamic_model)
            self.assertIsInstance(celery_group, group)
            self.assertEqual(1, len(celery_group.tasks))
            self.assertEqual("importer.create_dynamic_structure", celery_group.tasks[0].name)
        finally:
            if schema:
                schema.delete()
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    def test_setup_dynamic_model_no_dataset_no_modelschema(self):
        self._assert_test_result()

    def test_setup_dynamic_model_no_dataset_no_modelschema_overwrite_true(self):
        self._assert_test_result(overwrite=True)

    def test_setup_dynamic_model_with_dataset_no_modelschema_overwrite_false(self):
        create_single_dataset(name='stazioni_metropolitana', owner=self.user)
        self._assert_test_result(overwrite=False)

    def test_setup_dynamic_model_with_dataset_no_modelschema_overwrite_True(self):
        create_single_dataset(name='stazioni_metropolitana', owner=self.user)
        self._assert_test_result(overwrite=True)

    def test_setup_dynamic_model_no_dataset_with_modelschema_overwrite_false(self):
        ModelSchema.objects.get_or_create(
            name="stazioni_metropolitana",
            db_name="datastore"
        )
        self._assert_test_result(overwrite=False)

    def test_setup_dynamic_model_with_dataset_with_modelschema_overwrite_false(self):
        create_single_dataset(name='stazioni_metropolitana', owner=self.user)
        ModelSchema.objects.create(
            name="stazioni_metropolitana",
            db_name="datastore",
            managed=True
        )
        self._assert_test_result(overwrite=False)
    
    def _assert_test_result(self, overwrite=False):
        try:
            # Prepare the test
            exec_id = orchestrator.create_execution_request(
                user=self.user,
                func_name="funct1",
                step="step",
                input_params={
                    "files": self.valid_files,
                    "skip_existing_layer": True
                },
            )
            
            layers = ogr.Open(self.valid_gpkg)

            # starting the tests
            dynamic_model, layer_name, celery_group = self.handler.setup_dynamic_model(
                layer=[x for x in layers][0],
                execution_id=str(exec_id),
                should_be_overrided=overwrite,
                username=self.user
            )

            self.assertIsNotNone(dynamic_model)

            #check if the uuid has been added to the model name
            self.assertIsNotNone(layer_name)

            self.assertIsInstance(celery_group, group)
            self.assertEqual(1, len(celery_group.tasks))
            self.assertEqual("importer.create_dynamic_structure", celery_group.tasks[0].name)
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    @patch("importer.handlers.common.vector.chord")
    def test_import_resource_should_not_be_imported(self, celery_chord):
        '''
        If the resource exists and should be skept, the celery task
        is not going to be called and the layer is skipped
        '''
        exec_id = None
        try:
            # create the executionId
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={
                    "files": self.valid_files,
                    "skip_existing_layer": True
                },
            )

            # start the resource import
            self.handler.import_resource(
                files=self.valid_files,
                execution_id=str(exec_id)
            )

            celery_chord.assert_not_called()
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    @patch("importer.handlers.common.vector.chord")
    def test_import_resource_should_work(self, celery_chord):
        try:
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={
                    "files": self.valid_files
                },
            )

            # start the resource import
            self.handler.import_resource(
                files=self.valid_files,
                execution_id=str(exec_id)
            )
        
            celery_chord.assert_called_once()
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()
