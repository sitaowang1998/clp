from typing import Optional

import json
import mariadb
import msgpack
import uuid


_create_driver_query = "INSERT INTO `drivers` (`id`) VALUES (%s)"
_job_insert_query = "INSERT INTO `jobs` (`id`, `client_id`) VALUES (%s, %s)"
_task_insert_query = "INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `state`, `timeout`, `max_retry`) VALUES (%s, %s, %s, %s, %s, %s)"
_task_insert_input_value_query = "INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `value`) VALUES (%s, %s, %s, %s)"
_task_insert_output_query = "INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (%s, %s, %s)"
_task_insert_input_task_query = "INSERT INTO `input_tasks` (`job_id`, `task_id`, `position`) VALUES (%s, %s, %s)"
_task_insert_output_task_query = "INSERT INTO `output_tasks` (`job_id`, `task_id`, `position`) VALUES (%s, %s, %s)"
_job_status_query = "SELECT `state`  FROM `jobs` WHERE `id` = %s"
_job_output_tasks_query = "SELECT `task_id` FROM `output_tasks` WHERE `job_id` = %s ORDER BY `position`"
_task_output_values_query = "SELECT `value` FROM `task_outputs` WHERE `task_id` = %s ORDER BY `position`"
_int_typename = "i"
_int_list_typename = "St6vectorIiSaIiEE"
_string_typename = "NSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE"
_clp_compress_task_path = "/mnt/SSD-4T/sitao/clp-spider/build/clp-package/lib/python3/site-packages/job_orchestration/executor/compress/compression_task.py"
_spider_db_host = "127.0.0.1"
_spider_db_port = 3306
_spider_db_name = "spider-db"
_spider_db_user = "spider"
_spider_db_password = "spider-password"
_spider_client_id = uuid.uuid4().bytes

def create_db_connection():
    """
    Create a connection to the Spider database.
    :return: A tuple containing the database connection and cursor.
    """
    try:
        db_conn = mariadb.connect(
            host=_spider_db_host,
            port=_spider_db_port,
            user=_spider_db_user,
            password=_spider_db_password,
            database=_spider_db_name
        )
        db_cursor = db_conn.cursor()
        return db_conn, db_cursor
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return None, None


def submit_job(db_conn, db_cursor, task_params) -> Optional[uuid.UUID]:
    """
    Submit compression tasks to the Spider database.
    :param db_conn:
    :param db_cursor:
    :param task_params: List of dictionaries containing task parameters.
    :return: Job IDs for the submitted tasks. None if submission fails.
    """
    job_id = uuid.uuid4()
    task_ids = [uuid.uuid4().bytes for _ in range(len(task_params))]

    try:
        db_cursor.execute(_create_driver_query, (_spider_client_id,))
        db_cursor.execute(_job_insert_query, (job_id.bytes, _spider_client_id))
        db_cursor.executemany(_task_insert_query, [(task_ids[i], job_id.bytes, "clp_compress", "ready", 0, 0) for i in range(len(task_params))])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 0, _int_typename, msgpack.packb(task_param["job_id"])) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 1, _int_typename, msgpack.packb(task_param["task_id"])) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 2, _int_list_typename, msgpack.packb(task_param["tag_ids"])) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 3, _string_typename, msgpack.packb(task_param["clp_io_config_json"])) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 4, _string_typename, msgpack.packb(task_param["paths_to_compression_json"] if "paths_to_compression_json" in task_param else "")) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 5, _string_typename, msgpack.packb(task_param["clp_metadata_db_connection_config"])) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 6, _string_typename, msgpack.packb(_clp_compress_task_path)) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_output_query, [(task_ids[i], 0, _string_typename) for i in range(len(task_params))])
        db_cursor.executemany(_task_insert_input_task_query, [(job_id.bytes, task_ids[i], i) for i in range(len(task_params))])
        db_cursor.executemany(_task_insert_output_task_query, [(job_id.bytes, task_ids[i], i) for i in range(len(task_params))])
    except Exception as e:
        print(f"Error submitting job: {e}")
        db_conn.rollback()
        return None

    return job_id

def poll_result(db_conn, db_cursor, job_id: uuid.UUID):
    """
    Poll the result of a job by its ID.
    :param db_conn:
    :param db_cursor:
    :param job_id:
    :return: Job output values if the job is completed. If parsing values fails, return an empty
             list. None if the job is not found or not completed.
    """
    db_cursor.execute(_job_status_query, (job_id.bytes,))
    job_status = db_cursor.fetchone()

    if not job_status:
        db_conn.commit()
        return None

    if job_status[0] != "completed":
        db_conn.commit()
        return None

    try:
        db_cursor.execute(_job_output_tasks_query, (job_id.bytes,))
        output_tasks = db_cursor.fetchall()

        result = []
        for task in output_tasks:
            task_id = task[0]
            db_cursor.execute(_task_output_values_query, (task_id,))
            value = db_cursor.fetchone()
            result.append(json.loads(msgpack.unpackb(value[0])))
    except Exception as e:
        print(f"Error getting output values: {e}")
        db_conn.commit()
        return []

    db_conn.commit()
    return result

