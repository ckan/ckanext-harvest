from logging import Handler, NOTSET

from ckanext.harvest.model import HarvestLog


class DBLogHandler(Handler):
    def __init__(self, level=NOTSET):
        super(DBLogHandler, self).__init__(level=level)

    def emit(self, record):
        try:
            level = record.levelname
            msg = self.format(record)
            obj = HarvestLog(level=level, content=msg)
            obj.save()
        except Exception:
            pass
