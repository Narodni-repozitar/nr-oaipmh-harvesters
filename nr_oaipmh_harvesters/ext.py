class NRDocsOAIHarvesterExt:
    def __init__(self, app=None):
        """Extension initialization."""
        if app:
            self.init_app(app)

    def init_app(self, app):
        """Flask application initialization."""
        self.app = app
        app.extensions["nrdocs_oaipmh_harvester"] = self
        self.load_config(app)

    def load_config(self, app):
        from . import config

        app.config.setdefault("DATASTREAMS_TRANSFORMERS", {}).update(
            config.DATASTREAMS_TRANSFORMERS
        )
