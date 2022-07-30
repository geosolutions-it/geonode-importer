
import os
from django.test import TestCase
from importer import project_dir
from importer.publisher import DataPublisher
from unittest.mock import patch


class TestDataPublisher(TestCase):
    '''
    Test to get the information and publish the resource in geoserver
    '''
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.publisher = DataPublisher(handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler')
        cls.gpkg_path = f"{project_dir}/tests/fixture/valid.gpkg"


    def test_extract_resource_name_and_crs(self):
        '''
        Given a layer and the original file, should extract the crs and the name
        to let it publish in Geoserver
        '''
        values_found = self.publisher.extract_resource_to_publish(
            files={"base_file": self.gpkg_path},
            action="import",
            layer_name="stazioni_metropolitana"
        )
        expected = {
            "crs": "EPSG:32632",
            "name": "stazioni_metropolitana"
        }
        self.assertDictEqual(expected, values_found[0])

    def test_extract_resource_name_and_crs_return_empty_if_the_file_does_not_exists(self):
        '''
        Given a layer and the original file, should extract the crs and the name
        to let it publish in Geoserver
        '''
        values_found = self.publisher.extract_resource_to_publish(
            files={"base_file": "/wrong/path/file.gpkg"},
            action="import",
            layer_name="stazioni_metropolitana"
        )
        self.assertListEqual([], values_found)

    @patch("importer.publisher.create_geoserver_db_featurestore")
    def test_get_or_create_store_creation_should_not_be_called(self, datastore):
        self.publisher.get_or_create_store()
        datastore.assert_not_called()

    @patch("importer.publisher.create_geoserver_db_featurestore")
    def test_get_or_create_store_creation_should_called(self, datastore):
        with patch.dict(os.environ, {"GEONODE_GEODATABASE": "not_existsing_db"}, clear=True):
            self.publisher.get_or_create_store()
            datastore.assert_called_once()

    @patch("importer.publisher.Catalog.publish_featuretype")
    def test_publish_resources_should_raise_exception_if_any_error_happen(self, publish_featuretype):
        publish_featuretype.side_effect = Exception("Exception")

        with self.assertRaises(Exception) as _exc:
            self.publisher.publish_resources(
                resources=[{
                    "crs": "EPSG:32632",
                    "name": "stazioni_metropolitana"
                }]
            )
        publish_featuretype.assert_called_once()

    @patch("importer.publisher.Catalog.publish_featuretype")
    def test_publish_resources_should_continue_in_case_the_resource_is_already_published(self, publish_featuretype):
        publish_featuretype.side_effect = Exception("Resource named stazioni_metropolitana already exists in store:")

        result = self.publisher.publish_resources(
            resources=[{
                "crs": "EPSG:32632",
                "name": "stazioni_metropolitana"
            }]
        )
        self.assertTrue(result)
        publish_featuretype.assert_called_once()

    @patch("importer.publisher.Catalog.publish_featuretype")
    def test_publish_resources_should_work(self, publish_featuretype):
        publish_featuretype.return_value = True

        result= self.publisher.publish_resources(
            resources=[{
                "crs": "EPSG:32632",
                "name": "stazioni_metropolitana"
            }]
        )

        self.assertTrue(result)
        publish_featuretype.assert_called_once()