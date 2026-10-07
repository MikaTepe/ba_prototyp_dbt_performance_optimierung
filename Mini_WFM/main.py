from __future__ import annotations

import argparse
import sys

from config import load_run_requests, load_workflow_config
from dbt_runner import DbtRunner
from orchestrator import WorkflowOrchestrator
from podman_client import PodmanClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal workflow manager v0.2: run multiple dbt commands inside Podman containers."
    )

    parser.add_argument(
        "--config",
        default="workflow_config.json",
        help="Path to workflow config JSON.",
    )

    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Override config and run requests sequentially.",
    )

    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Override config and run requests in parallel.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    workflow_config = load_workflow_config(args.config)
    run_requests = load_run_requests(args.config)

    if args.sequential and args.parallel:
        print("Cannot use --sequential and --parallel together.")
        return 2

    # CLI override without mutating the config file.
    if args.sequential or args.parallel:
        from dataclasses import replace

        workflow_config = replace(
            workflow_config,
            execution=replace(
                workflow_config.execution,
                parallel=args.parallel,
            ),
        )

    podman_client = PodmanClient(
        podman_binary=workflow_config.podman_binary,
    )

    dbt_runner = DbtRunner(
        config=workflow_config,
        podman_client=podman_client,
    )

    orchestrator = WorkflowOrchestrator(
        config=workflow_config,
        dbt_runner=dbt_runner,
    )

    print("Starting workflow manager v0.2")
    print(f"Configured runs: {len(run_requests)}")
    print(f"Parallel execution: {workflow_config.execution.parallel}")
    print(f"Max parallel jobs: {workflow_config.execution.max_parallel_jobs}")
    print("")

    batch_result = orchestrator.run_batch(run_requests)

    print("")
    print("=" * 100)
    print("Batch finished")
    print("=" * 100)

    for result in batch_result.results:
        status = "SUCCESS" if result.successful else "FAILED"
        print(
            f"{status} | run_id={result.run_id} | "
            f"returncode={result.returncode} | "
            f"duration={result.duration_seconds:.3f}s | "
            f"log={result.log_path}"
        )

    if batch_result.successful:
        print("")
        print("Batch status: SUCCESS")
        return 0

    print("")
    print("Batch status: FAILED")

    failed_ids = [result.run_id for result in batch_result.failed_results]
    print(f"Failed run_ids: {failed_ids}")

    return 1


if __name__ == "__main__":
    sys.exit(main())