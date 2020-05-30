from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import Table

from .common import _fmt_log, LoadError
from .loaders import loader_for


class FileAlchemy:
    def __init__(self, app, db):
        self.app = app
        self.db = db

        self.data_dir = Path(self.app.config.get('FILEALCHEMY_DATA_DIR'))
        self.models = self._build_model_map(self.app.config.get('FILEALCHEMY_MODELS'))
        self.skip_no_model = self.app.config.get('FILEALCHEMY_SKIP_NO_MODEL')
        self.map_nested = self.app.config.get('FILEALCHEMY_MAP_NESTED')

        self.validate()

    def _build_model_map(self, models):
        model_map = {}
        for model in models:
            mapping = {}
            if isinstance(model, tuple):
                model, mapping = model
            model_map[model.__tablename__] = (model, mapping)
        return model_map

    def validate(self):
        if not self.models:
            raise LoadError(_fmt_log('no models found'))

        if not self.data_dir.exists() or not self.data_dir.is_dir():
            raise LoadError(
                _fmt_log('{} is not a directory'.format(self.data_dir))
            )

    def load_tables(self):
        self.db.create_all()

        with self.make_session() as session:
            for table in self.db.metadata.sorted_tables:
                model, mapping = self.model_for(table)

                if not model:
                    if self.skip_no_model:
                        continue
                    else:
                        raise LoadError(
                            _fmt_log('no model found for {}'.format(table.name))
                        )

                model_map = self.models if self.map_nested else {}
                loader = loader_for(self.data_dir, table, column_map=mapping, model_map=model_map)

                if not loader:
                    raise LoadError(
                        _fmt_log('no loader found for {}'.format(table.name))
                    )

                try:
                    for record in loader.extract_records(model):
                        session.add(record)

                    session.flush()
                except IntegrityError as e:
                    raise LoadError(e)

    @contextmanager
    def make_session(self):
        try:
            session = self.db.session

            yield session
        except LoadError:
            session.rollback()
            raise
        else:
            session.commit()
        finally:
            session.close()

    def directory_for(self, table: Table):
        return self.data_dir.joinpath(table.name)

    def model_for(self, table: Table):
        return self.models.get(table.name, (None, None))
