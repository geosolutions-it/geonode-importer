from django.apps import AppConfig


class HandlersConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'importer.handlers'

    def ready(self):
        """Finalize setup"""
        run_setup_hooks()
        super(HandlersConfig, self).ready()


def run_setup_hooks(*args, **kwargs):
    from .gpkg.handler import GPKGFileHandler
    from .geojson.handler import GeoJsonFileHandler
    GPKGFileHandler.register()
    GeoJsonFileHandler.register()