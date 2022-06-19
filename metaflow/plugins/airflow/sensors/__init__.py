from .external_task_sensor import ExternalTaskSensorDecorator
from .sql_sensor import SQLSensorDecorator
from .s3_sensor import S3KeySensorDecorator

# todo (savin-comments): We can consider sunsetting SQLSensor for now (pending confirmation from Asad)
SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator.name,
    S3KeySensorDecorator.name,
    SQLSensorDecorator.name,
]
