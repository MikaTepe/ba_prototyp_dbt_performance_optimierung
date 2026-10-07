from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from domain import BatchResult, CommandResult, DbtRunRequest, WorkflowConfig
from dbt_runner import DbtRunner


class WorkflowOrchestrator:
    def __init__(self, config: WorkflowConfig, dbt_runner: DbtRunner) -> None:
        self.config = config
        self.dbt_runner = dbt_runner

    def run_batch(self, requests: list[DbtRunRequest]) -> BatchResult:
        if not requests:
            return BatchResult(results=[])

        if self.config.execution.parallel:
            return self._run_batch_parallel(requests)

        return self._run_batch_sequential(requests)

    def _run_batch_sequential(self, requests: list[DbtRunRequest]) -> BatchResult:
        results: list[CommandResult] = []

        for request in requests:
            print("=" * 100)
            print(f"Starting run_id={request.run_id}")
            print(f"Container={request.container_name}")
            print(f"dbt command={request.dbt_command}")
            print(f"model={request.model_name}")
            print("=" * 100)

            result = self.dbt_runner.run(request)
            results.append(result)

            print("")
            print(f"Finished run_id={result.run_id}")
            print(f"Return code={result.returncode}")
            print(f"Duration seconds={result.duration_seconds:.3f}")
            print(f"Log file={result.log_path}")

            if not result.successful:
                print("Sequential batch stopped because one run failed.")
                break

        return BatchResult(results=results)

    def _run_batch_parallel(self, requests: list[DbtRunRequest]) -> BatchResult:
        max_workers = min(
            self.config.execution.max_parallel_jobs,
            len(requests),
        )

        print("=" * 100)
        print(f"Starting parallel batch with {len(requests)} run(s)")
        print(f"max_workers={max_workers}")
        print("=" * 100)

        results: list[CommandResult] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_request = {
                executor.submit(self.dbt_runner.run, request): request
                for request in requests
            }

            for future in as_completed(future_to_request):
                request = future_to_request[future]

                try:
                    result = future.result()
                except Exception as exc:
                    print("")
                    print(f"Run failed with exception before CommandResult was created.")
                    print(f"run_id={request.run_id}")
                    print(f"container={request.container_name}")
                    print(f"error={exc}")

                    # Represent orchestration-level failure as a synthetic result.
                    # This keeps the batch result uniform.
                    result = CommandResult(
                        run_id=request.run_id,
                        command=[],
                        returncode=1,
                        duration_seconds=0.0,
                        log_path=self.config.log_dir / f"{request.run_id}_orchestration_error.log",
                    )

                    result.log_path.parent.mkdir(parents=True, exist_ok=True)
                    result.log_path.write_text(str(exc), encoding="utf-8")

                results.append(result)

                print("")
                print(f"Finished run_id={result.run_id}")
                print(f"Return code={result.returncode}")
                print(f"Duration seconds={result.duration_seconds:.3f}")
                print(f"Log file={result.log_path}")

        return BatchResult(results=results)