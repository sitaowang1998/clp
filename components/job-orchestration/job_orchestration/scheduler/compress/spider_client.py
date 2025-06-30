import uuid
from uuid import uuid4

from typing import List

_job_insert_query = "INSERT INTO `jobs` (`id`, `client_id`) VALUES (?, ?)"
_task_insert_query = "INSERT INTO `tasks` (`id`, `job_id`, `func_name`, `state`, `timeout`, `max_retry`) VALUES (?, ?, ?, ?, ?, ?)"
_task_input_value_query = "INSERT INTO `task_inputs` (`task_id`, `position`, `type`, `value`) VALUES (?, ?, ?, ?)"
_task_output_query = "INSERT INTO `task_outputs` (`task_id`, `position`, `type`) VALUES (?, ?, ?)"
_task_input_task_query = "INSERT INTO `input_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?)"
_task_output_task_query = "INSERT INTO `output_tasks` (`job_id`, `task_id`, `position`) VALUES (?, ?, ?)"

def submit_jobs(db_cursor, client_id, task_params) -> List[uuid.UUID]:
    """
    Submit compression tasks to the Spider database.
    :param db_cursor:
    :param client_id: The ID of the client submitting the jobs.
    :param task_params: List of dictionaries containing task parameters.
    :return:
    """
    job_ids = [uuid4() for _ in range(len(task_params))]
    task_ids = [uuid4() for _ in range(len(task_params))]

    db_cursor.executemany(_job_insert_query, [(job_id, client_id) for job_id in job_ids])
    db_cursor.executemany(_task_insert_query, [(task_ids[i], job_ids[i], "clp_compress", "ready", 0, 0) for i in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 0, "int", task_param[0]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 1, "int", task_param[1]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 2, "string", task_param[2]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 2, "string", task_param[3]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 2, "string", task_param[4]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_input_value_query, [(task_ids[i], 2, "string", task_param[5]) for i, task_param in enumerate(task_params)])
    db_cursor.executemany(_task_output_query, [(task_ids[i], 0, "string") for i in range(len(task_params))])
    db_cursor.executemany(_task_input_task_query, [(job_ids[i], task_ids[i], 0) for i in range(len(task_params))])
    db_cursor.executemany(_task_output_task_query, [(job_ids[i], task_ids[i], 0) for i in range(len(task_params))])

    return job_ids