from logging import Handler

from ckanext.harvest.model import HarvestLog

class DBLogHandler(Handler, object):
    def __init__(self):
        super(DBLogHandler,self).__init__()

    def emit(self, record):
        try:
            level = record.levelname
            msg = self.format(record)
            obj = HarvestLog(level=level, content=msg)
            obj.save()
        except Exception as exc:
            pass