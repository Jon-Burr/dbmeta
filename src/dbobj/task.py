from .json_store import MutableJSONAssocStore
from .database import AssocDatabase, ColumnDesc
import json

class TaskDB(AssocDatabase):
    def __init__(self, db_file):
        super(TaskDB, self).__init__(
                store=MutableJSONAssocStore(db_file=db_file, db=self))

    taskID = ColumnDesc(is_index=True, store_type=str, type=int)
    taskName = ColumnDesc()

    def write(self, **kwargs):
        self._store.write(**kwargs)


if __name__ == "__main__":
    db = TaskDB("test.json")
