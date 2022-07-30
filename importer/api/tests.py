from django.core.files.uploadedfile import SimpleUploadedFile
from django.http.response import HttpResponse
from geonode.tests.base import GeoNodeBaseTestSupport
from django.urls import reverse
from unittest.mock import MagicMock, patch
# Create your tests here.


class TestImporterViewSet(GeoNodeBaseTestSupport):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.url = reverse('importer_upload')
    
    def test_upload_method_not_allowed(self):
        self.client.login(username="admin", password="admin")
       
        response = self.client.get(self.url)
        self.assertTrue(405, response.status_code)

        response = self.client.put(self.url)
        self.assertTrue(405, response.status_code)

        response = self.client.patch(self.url)
        self.assertTrue(405, response.status_code)

    def test_anonymous_cannot_see_the_page(self):
        response = self.client.get(self.url)
        self.assertTrue(403, response.status_code)

    @patch('importer.api.views.UploadViewSet')
    def test_redirect_to_old_upload_if_file_is_not_a_gpkg(self, patch_upload):
        upload = MagicMock()
        upload.upload.return_value = HttpResponse()
        patch_upload.return_value = upload

        self.client.login(username="admin", password="admin")
        payload = {
            "base_file": SimpleUploadedFile(name="file.invalid", content=b"abc"),
        }        
        response = self.client.post(self.url, data=payload)
        self.assertTrue(200, response.status_code)
        upload.upload.assert_called_once()

    @patch('importer.api.views.UploadViewSet')
    def test_gpkg_raise_error_with_invalid_payload(self, patch_upload):
        upload = MagicMock()
        upload.upload.return_value = HttpResponse()
        patch_upload.return_value = upload

        self.client.login(username="admin", password="admin")
        payload = {
            "base_file": SimpleUploadedFile(name="test.gpkg", content=b"some-content"),
            "store_spatial_files": "invalid"
        }
        expected = {'success': False, 'errors': ['Must be a valid boolean.'], 'code': 'invalid'}
        
        response = self.client.post(self.url, data=payload)
        
        self.assertTrue(500, response.status_code)
        self.assertEqual(expected, response.json())

    @patch('importer.api.views.import_orchestrator')
    def test_gpkg_task_is_called(self, patch_upload):
        patch_upload.apply_async.side_effect = MagicMock()

        self.client.login(username="admin", password="admin")
        payload = {
            "base_file": SimpleUploadedFile(name="test.gpkg", content=b"some-content"),
            "store_spatial_files": True
        }
        
        response = self.client.post(self.url, data=payload)
        
        self.assertTrue(201, response.status_code)
