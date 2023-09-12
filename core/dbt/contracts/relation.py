from collections.abc import Mapping
from dataclasses import dataclass
from typing import (
    Optional,
    Dict,
)
from typing_extensions import Protocol

from dbt.dataclass_schema import dbtClassMixin, StrEnum

from dbt.contracts.util import Replaceable
from dbt.exceptions import CompilationError, DataclassNotDictError
from dbt.utils import deep_merge


class RelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materialized_view"
    External = "external"


class ComponentName(StrEnum):
    Database = "database"
    Schema = "schema"
    Identifier = "identifier"


class HasQuoting(Protocol):
    quoting: Dict[str, bool]


class FakeAPIObject(dbtClassMixin, Replaceable, Mapping):
    # override the mapping truthiness, len is always >1
    def __bool__(self):
        return True

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    def __iter__(self):
        raise DataclassNotDictError(self)

    def __len__(self):
        raise DataclassNotDictError(self)

    def incorporate(self, **kwargs):
        value = self.to_dict(omit_none=True)
        value = deep_merge(value, kwargs)
        return self.from_dict(value)


@dataclass
class Policy(FakeAPIObject):
    database: bool = True
    schema: bool = True
    identifier: bool = True

    def get_part(self, key: ComponentName) -> bool:
        if key == ComponentName.Database:
            return self.database
        elif key == ComponentName.Schema:
            return self.schema
        elif key == ComponentName.Identifier:
            return self.identifier
        else:
            raise ValueError(f"Got a key of {key}, expected one of {list(ComponentName)}")

    def replace_dict(self, dct: Dict[ComponentName, bool]):
        kwargs: Dict[str, bool] = {str(k): v for k, v in dct.items()}
        return self.replace(**kwargs)


@dataclass
class Path(FakeAPIObject):
    database: Optional[str] = None
    schema: Optional[str] = None
    identifier: Optional[str] = None

    def __post_init__(self):
        # handle pesky jinja2.Undefined sneaking in here and messing up rende
        if not isinstance(self.database, (type(None), str)):
            raise CompilationError(f"Got an invalid path database: {self.database}")
        if not isinstance(self.schema, (type(None), str)):
            raise CompilationError(f"Got an invalid path schema: {self.schema}")
        if not isinstance(self.identifier, (type(None), str)):
            raise CompilationError(f"Got an invalid path identifier: {self.identifier}")

    def get_lowered_part(self, key: ComponentName) -> Optional[str]:
        part = self.get_part(key)
        if part is not None:
            part = part.lower()
        return part

    def get_part(self, key: ComponentName) -> Optional[str]:
        if key == ComponentName.Database:
            return self.database
        elif key == ComponentName.Schema:
            return self.schema
        elif key == ComponentName.Identifier:
            return self.identifier
        else:
            raise ValueError(f"Got a key of {key}, expected one of {list(ComponentName)}")

    def replace_dict(self, dct: Dict[ComponentName, str]):
        kwargs: Dict[str, str] = {str(k): v for k, v in dct.items()}
        return self.replace(**kwargs)
