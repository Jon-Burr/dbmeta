from .json_store import MutableJSONAssocStore
from .database import AssocDatabase, IndexColumnDesc, ColumnDesc
import json

class TaskDB(AssocDatabase):
    def __init__(self, db_file):
        super(TaskDB, self).__init__(
                store=MutableJSONAssocStore(db_file=db_file, db=self))

    taskID = IndexColumnDesc(
            read_func=lambda x, source: int(x),
            write_func=lambda x, target: str(x) )
    taskName = ColumnDesc()

    def write(self, **kwargs):
        self._store.write(**kwargs)


if __name__ == "__main__":
    db = TaskDB("test.json")
