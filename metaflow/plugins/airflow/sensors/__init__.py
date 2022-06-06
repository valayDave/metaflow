from .external_task_sensor import ExternalTaskSensorDecorator
from .sql_sensor import SQLSensorDecorator
from .s3_sensor import S3KeySensorDecorator


SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator.name,
    S3KeySensorDecorator.name,
    SQLSensorDecorator.name,
]
