from typing import Union
from preprocessor.utils.preprocessor_utils import (
    PreProcessorLogger,
    load_config,
    PreProcessorException,
)
from preprocessor.dblogging.dblogger import AuditManager
from preprocessor.snowflake.schema import getSchema
import datetime
from preprocessor.file_utils import processFile


def processor(
    file_process_id: str,
    bucket: str,
    key: str,
    size: Union[str, int],
    flag: bool,
    configfile: str,
) -> int:
    try:
        logger = PreProcessorLogger(name="main")
        logger.info("Starting the process")
        config = load_config()
        adt = AuditManager(
            config.get("db").get("database"),
            config.get("db").get("user"),
            config.get("db").get("password"),
            config.get("db").get("host"),
            config.get("db").get("port"),
        )
        step_id = adt.insertIntoStepLog(
            {
                "file_process_id": file_process_id,
                "step_status": "IN-PROGRESS",
                "step_status_detail": "running",
                "step_start_ts": datetime.datetime.now(),
            }
        )
        logger.info("Getting schema from snowflake")
        targetConfig = getSchema()
        logger.info("processing the file {0}".format(key))
        processFile(bucket, key, targetConfig.get("Bucket"), config)
        adt.updateStepLog(
            {
                "step_id": step_id,
                "step_status": "DONE",
                "step_status_detail": "Completed Successfully",
            }
        )
        adt.closeConnection()
        logger.info("Pre-processor completed")
        return 0
    except Exception as e:
        logger.error(e)
        adt.updateStepLog(
            {"step_id": step_id, "step_status": "ERROR", "step_status_detail": str(e),}
        )
        adt.closeConnection()
        return 1
