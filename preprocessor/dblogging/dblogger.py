import psycopg2
import psycopg2.extras
import datetime
from typing import Dict
from preprocessor.utils.preprocessor_utils import (
    PreProcessorLogger,
    PreProcessorException,
)


class AuditManager:
    def __init__(
        self, db: str, user: str, password: str, host: str, port: int = 5432,
    ):
        """
        Class to access get and update information in the audit DB's
        """
        self.logger = PreProcessorLogger(name="dblogger")
        try:
            self.engine = psycopg2.connect(
                database=db, user=user, password=password, host=host, port=port
            )
            self.engine.autocommit = True
        except Exception as e:
            self.logger.error(str(e))
            raise PreProcessorException(
                "Unable to connect to postgres DB : {0}".format(e)
            )

    def closeConnection(self):
        """
        Close connection to DB
        """
        self.engine.close()

    def updateStepLog(self, data: Dict) -> None:
        """
        Updates  process step log , at the end of each
        file processing
        Args :
                Data -- Dict
                Dictionary containing all the required values
                *all values must be passed*
        Return:
                None
        """
        step_payload = {
            **data,
            **{
                "step_end_ts": str(datetime.datetime.now()),
                "upsert_by": "Pre-Processor",
                "upsert_ts": str(datetime.datetime.now()),
            },
        }
        UpdateQuery = """
        UPDATE file_process_step_log
        SET    step_status = '{step_status}',
               step_status_details = '{step_status_detail}',
               step_end_ts = timestamp '{step_end_ts}',
               upsert_by = '{upsert_by}',
               upsert_ts = timestamp '{upsert_ts}'
        WHERE  step_id = {step_id}
        """
        cursor = self.engine.cursor()
        try:
            cursor.execute(UpdateQuery.format(**step_payload))
        except Exception as e:
            raise PreProcessorException(
                "Failed while inserting data into audit table {0}".format(e)
            )
        finally:
            cursor.close()

    def insertIntoStepLog(self, data: Dict) -> int:
        """
        Inserts data into process step log , at the start of each
        file processing
        Args :
                Data -- Dict
                Dictionary containing all the required values
                *all values must be passed*
        Return:
                Step id inserted
        """
        step_payload = {
            **data,
            **{
                "step_name": "Pre-processor",
                "step_end_ts": str(datetime.datetime.now()),
                "upsert_by": "PP",
                "upsert_ts": str(datetime.datetime.now()),
            },
        }

        insertQuery = """
        INSERT INTO file_process_step_log
                    (file_process_id,
                    step_name,
                    step_status,
                    step_status_details,
                    step_start_ts,
                    step_end_ts,
                    upsert_by,
                    upsert_ts)
        VALUES    ( '{file_process_id}',
                    '{step_name}',
                    '{step_status}',
                    '{step_status_detail}',
                    timestamp '{step_start_ts}',
                    timestamp '{step_end_ts}',
                    '{upsert_by}',
                    timestamp '{upsert_ts}' ) 
        RETURNING step_id
        """
        cursor = self.engine.cursor()
        try:
            cursor.execute(insertQuery.format(**step_payload))
            step_id = cursor.fetchone()[0]
            return step_id
        except Exception as e:
            raise PreProcessorException(
                "Failed while inserting data into audit table {0}".format(e)
            )
        finally:
            cursor.close()
