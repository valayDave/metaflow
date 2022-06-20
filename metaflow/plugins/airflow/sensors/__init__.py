from .external_task_sensor import ExternalTaskSensorDecorator
from .s3_sensor import S3KeySensorDecorator
from .sql_sensor import SQLSensorDecorator

# todo (savin-comments): add sql sensors back in .
SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator.name,
    S3KeySensorDecorator.name,
    SQLSensorDecorator.name,
]
