from __future__ import annotations

import argparse
import os
import subprocess
import sys

def parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parse command line arguments. This function expects the following arguments:
    1. --python_path: Path to the Python libraries.
    2. --spider_worker_path: Path to the Spider worker executable.
    3. --num_workers: Number of Spider workers to start.
    4. --storage_url: URL of the spider storage backend.
    5. --host: Host address for the Spider worker.

    :param argv: A list of command line arguments.
    :return: Parsed arguments.
    """
    args_parser = argparse.ArgumentParser(description="Starts a Spider worker.")
    args_parser.add_argument(
        "--python_path",
        required=True,
        help="Path to the Python libraries.",
    )
    args_parser.add_argument(
        "--spider_worker_path",
        required=True,
        help="Path to the Spider worker executable.",
    )
    args_parser.add_argument(
        "--num_workers",
        "-n",
        type=int,
        required=True,
        help="Number of Spider workers to start.",
    )
    args_parser.add_argument(
        "--storage_url",
        required=True,
        help="URL of the spider storage backend.",
    )
    args_parser.add_argument(
        "--host",
        required=True,
        help="Host address for the Spider worker.",
    )
    return args_parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    """
    Main function to start multiple Spider workers.
    It parses command line arguments, sets up the environment, and starts the specified number of
    Spider worker processes.
    :param argv: A list of command line arguments.
    :return: Exit code of the script. Returns 0 if all workers started successfully,
    """
    args = parse_args(argv)
    python_path = args.python_path
    spider_worker_path = args.spider_worker_path
    num_workers = args.num_workers
    storage_url = args.storage_url
    host = args.host

    env = os.environ.copy()
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{python_path}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = python_path
    processes = []
    for _ in range(num_workers):
        p = subprocess.Popen([
            spider_worker_path,
            "--storage_url",
            storage_url,
            "--host",
            host,
        ],
            shell=True,
            env=env,
        )
        processes.append(p)

    return_codes = []
    for p in processes:
        p.wait()
        return_codes.append(p.returncode)

    for return_code in return_codes:
        if return_code != 0:
            return return_code
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))