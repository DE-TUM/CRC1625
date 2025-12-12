import logging
import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from multiprocessing import Process, Pipe
from typing import Any
from functools import partial

from handover_workflows_validation.handover_workflows_validation import is_workflow_instance_valid, WorkflowModel, WorkflowInstance

module_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(module_dir, '../.env'))

WORKFLOWS_VALIDATION_API_HOST = os.environ.get("WORKFLOWS_VALIDATION_API_HOST")
WORKFLOWS_VALIDATION_API_PORT = os.environ.get("WORKFLOWS_VALIDATION_API_PORT")

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_in_process(
        func,
        conn_pipe: Pipe,
        *args: Any,
        **kwargs: Any
):
    try:
        result = func(*args, **kwargs)
        conn_pipe.send({"success": True, "result": result})
    except Exception as e:
        error_msg = f"Process execution failed: {type(e).__name__}: {e}"
        logger.error(error_msg)
        conn_pipe.send({"success": False, "error": error_msg})
    finally:
        conn_pipe.close()


@app.post("/is_workflow_valid/", response_model=bool)
async def validate_workflow(
        workflow_model: WorkflowModel,
        workflow_instance: WorkflowInstance
):
    parent_conn, child_conn = Pipe()

    target_func = partial(
        is_workflow_instance_valid,
        workflow_model=workflow_model,
        workflow_instance=workflow_instance
    )

    process = Process(
        target=run_in_process,
        args=(target_func, child_conn)
    )

    logger.info("Starting validation process.")
    process.start()
    child_conn.close()

    try:
        if parent_conn.poll(timeout=30):
            result_data = parent_conn.recv()
        else:
            process.terminate()
            raise HTTPException(
                status_code=504,
                detail="Validation process timed out."
            )

        if result_data["success"]:
            return result_data["result"]
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Validation error: {result_data['error']}"
            )

    except Exception:
        if process.is_alive():
            process.terminate()
        raise

    finally:
        process.join()
        parent_conn.close()
        logger.info("Process finished and resources cleaned up.")


def run(debug: bool = False):
    log_level = 'warning'
    access_log = False
    if debug:
        log_level = 'debug'
        access_log = True

    uvicorn.run(app,
                host=WORKFLOWS_VALIDATION_API_HOST,
                port=int(WORKFLOWS_VALIDATION_API_PORT),
                workers=1,
                log_level=log_level,
                access_log=access_log)