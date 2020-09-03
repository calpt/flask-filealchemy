import json
from pathlib import Path

from sqlalchemy.schema import Table

from .common import ColumnMapping, parse_yaml_file


class InvalidLoaderError(Exception):
    pass


class BaseLoader:
    """Base class for all Loader classes.
    """

    def __init__(self, data_dir: Path, table: Table, column_map: dict = {}, model_map: dict = {}):
        self.data_dir = data_dir
        self.table = table
        self.column_map = column_map
        self.model_map = model_map

        self.validate()

    def extract_records(self, model):
        raise NotImplementedError()

    def validate(self):
        raise NotImplementedError()


class YAMLSingleFileLoader(BaseLoader):
    """YAMLSingleFileLoader is used to load records from directories which
    contain a `_all.yml` file.

    Please note that while the existence of this file is a necessary
    requirement, this loader would still be chosen if the directory contains
    other files.
    """

    @property
    def data_path(self):
        return self.data_dir.joinpath(self.table.name).joinpath('_all.yml')

    def extract_records(self, model):
        values = parse_yaml_file(self.data_path)

        for value in values:
            kwargs = {
                column.name: value.get(column.name)
                for column in self.table.columns
            }

            yield model(**kwargs)

    def validate(self):
        all_ = self.data_path

        if not all_.exists() or not all_.is_file():
            raise InvalidLoaderError()


class YAMLDirectoryLoader(BaseLoader):
    """YAMLDirectoryLoader is used to load records from directories which
    contain only YAML-formatted files.
    """

    extensions = ('.yml', '.yaml', '.YML', '.YAML')

    @property
    def data_path(self):
        return self.data_dir.joinpath(self.table.name)

    def extract_records(self, model):
        for entry in self.data_path.glob('**/*'):
            if not entry.is_file():
                continue

            values = parse_yaml_file(entry)

            kwargs = {}
            # iterate default mappings
            for k, v in self.column_map.items():
                if v == ColumnMapping.FILE_NAME:
                    kwargs[k] = entry.stem
                elif v == ColumnMapping.FOLDER_NAME:
                    kwargs[k] = entry.parent.name
                else:
                    raise ValueError("Unknown column mapping '{}'".format(v))
            # iterate all mappings in file
            for k, value in values.items():
                if k in self.table.columns.keys():
                    if isinstance(value, list) or isinstance(value, dict):
                        value = json.dumps(value, indent=4)
                    kwargs[k] = value
                # check if we have a table in our schema with this key as name
                elif self.model_map and k in self.table.metadata.tables.keys() and isinstance(value, list):
                    sub_table = self.table.metadata.tables[k]
                    foreign_kwargs = {}
                    # if the found table has foreign keys referencing the current table, try to populate these
                    for const in sub_table.foreign_key_constraints:
                        for self_key, foreign_key in zip(const.column_keys, const.elements):
                            if foreign_key.references(self.table):
                                foreign_key = foreign_key.column.name
                                foreign_kwargs[self_key] = kwargs[foreign_key]
                    for sub_kwargs in value:
                        sub_kwargs.update(foreign_kwargs)
                        yield self.model_map[k][0](**sub_kwargs)

            yield model(**kwargs)

    def validate(self):
        for entry in self.data_path.glob('**/*'):
            if not entry.is_file():
                continue

            if not any(
                ext for ext in self.extensions if entry.name.endswith(ext)
            ):
                raise InvalidLoaderError()


def loader_for(data_dir: Path, table: Table, column_map: dict = {}, model_map: dict = {}):
    for cls in (YAMLSingleFileLoader, YAMLDirectoryLoader):
        try:
            loader = cls(data_dir, table, column_map=column_map, model_map=model_map)
        except InvalidLoaderError:
            pass
        else:
            return loader
