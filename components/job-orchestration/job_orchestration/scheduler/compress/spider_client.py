import uuid

from typing import List, Union

_job_insert_query = "INSERT INTO `jobs` (`id`, `client_id`) VALUES (%s, %s)"
_task_insert_query = "INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `state`, `timeout`, `max_retry`) VALUES (%s, %s, %s, %s, %s, %s)"
_task_insert_input_value_query = "INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `value`) VALUES (%s, %s, %s, %s)"
_task_insert_output_query = "INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (%s, %s, %s)"
_task_insert_input_task_query = "INSERT INTO `input_tasks` (`job_id`, `task_id`, `position`) VALUES (%s, %s, %s)"
_task_insert_output_task_query = "INSERT INTO `output_tasks` (`job_id`, `task_id`, `position`) VALUES (%s, %s, %s)"
_job_status_query = "SELECT `state` FROM `jobs` WHERE `id` = %s"
_job_output_tasks_query = "SELECT `task_id` FROM `output_tasks` WHERE `job_id` = %s ORDER BY `position`"
_task_output_values_query = "SELECT `value` FROM `task_outputs` WHERE `task_id` = %s ORDER BY `position`"

def submit_jobs(db_conn, db_cursor, client_id, task_params) -> Union[List[uuid.UUID], None]:
    """
    Submit compression tasks to the Spider database.
    :param db_conn:
    :param db_cursor:
    :param client_id: The ID of the client submitting the jobs.
    :param task_params: List of dictionaries containing task parameters.
    :return: List of job IDs for the submitted tasks. None if submission fails.
    """
    job_ids = [uuid.uuid4() for _ in range(len(task_params))]
    task_ids = [uuid.uuid4() for _ in range(len(task_params))]

    try:
        db_cursor.executemany(_job_insert_query, [(job_id, client_id) for job_id in job_ids])
        db_cursor.executemany(_task_insert_query, [(task_ids[i], job_ids[i], "clp_compress", "ready", 0, 0) for i in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 0, "int", task_param["job_id"]) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 1, "int", task_param["task_id"]) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 2, "string", task_param["tag_ids"].join(';')) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 2, "string", task_param["clp_io_config_json"]) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 2, "string", task_param["paths_to_compression_json"]) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_input_value_query, [(task_ids[i], 2, "string", task_param["clp_metadata_db_connection_config"]) for i, task_param in enumerate(task_params)])
        db_cursor.executemany(_task_insert_output_query, [(task_ids[i], 0, "string") for i in range(len(task_params))])
        db_cursor.executemany(_task_insert_input_task_query, [(job_ids[i], task_ids[i], 0) for i in range(len(task_params))])
        db_cursor.executemany(_task_insert_output_task_query, [(job_ids[i], task_ids[i], 0) for i in range(len(task_params))])
    except Exception as e:
        db_conn.rollback()
        return None

    return job_ids

def poll_result(db_conn, db_cursor, job_id: uuid.UUID):
    """
    Poll the result of a job by its ID.
    :param db_conn:
    :param db_cursor:
    :param job_id:
    :return: Job output values if the job is completed, None otherwise.
    """
    db_cursor.execute(_job_status_query, (job_id,))
    job_status = db_cursor.fetchone()

    if not job_status:
        db_conn.commit()
        return None

    if job_status[0] != "completed":
        db_conn.commit()
        return None

    db_cursor.execute(_job_output_tasks_query, (job_id,))
    output_tasks = db_cursor.fetchall()

    output_values = []
    for task in output_tasks:
        task_id = task[0]
        db_cursor.execute(_task_output_values_query, (task_id,))
        values = [value[0] for value in db_cursor.fetchall()]
        output_values.extend(values)

    db_conn.commit()
    return output_values

