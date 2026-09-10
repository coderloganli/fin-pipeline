"""`python -m pipeline daily`, and `python -m pipeline backfill --periods FROM:TO`.

The whole pipeline in one process: one Spark session serves every step, which falls out
of the runner holding it for the process rather than being a second code path. Under a
DAG the same steps run one per task; see `dags/` and docs/adr/0046.
"""

import argparse
import sys

from pipeline import run as runner
from pipeline import steps as step_list


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="Run the pipeline: every step in order, stopping at the first "
                    "failure, and writing down what each one did.",
    )
    parser.add_argument("pipeline", choices=sorted(step_list.PIPELINES),
                        help="which sequence of steps to run")
    parser.add_argument("--source", default=str(runner.DEFAULT_SOURCE))
    parser.add_argument("--raw", default=str(runner.DEFAULT_RAW))
    parser.add_argument("--staging", default=str(runner.DEFAULT_STAGING))
    parser.add_argument("--periods", default=None,
                        help="the reporting range, as YYYY-MM:YYYY-MM. Required for a "
                             "backfill, which exists to rebuild one")
    parser.add_argument("--run-id", dest="run_id", default=None,
                        help="record this run under an identifier chosen by the "
                             "caller, rather than generating one")
    parser.add_argument("--orchestrator", default=None,
                        help="what started this run, if anything did")
    parser.add_argument("--orchestrator-run-id", dest="orchestrator_run_id",
                        default=None,
                        help="that orchestrator's own identifier for this run, so its "
                             "UI and this record each name the other")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Turn a run into an exit code. The only place that knows about exit codes."""
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as ended:
        return int(ended.code or 0)

    if args.pipeline == "backfill" and not args.periods:
        print("backfill needs --periods FROM:TO: it exists to rebuild a range the "
              "affected-period set does not name", file=sys.stderr)
        return 2

    context = runner.Context(
        source_dir=args.source,
        raw_dir=args.raw,
        staging_dir=args.staging,
        periods=args.periods,
        # A backfill rebuilds the range it was given whether or not anything is owed.
        force=args.pipeline == "backfill",
        # This is a fresh process this command controls, so a session built here is
        # this run's to stop. Nothing that borrows a process says this.
        owns_spark=True,
    )

    try:
        run_id = runner.run_pipeline(
            context,
            command=args.pipeline,
            steps=step_list.PIPELINES[args.pipeline],
            run_id=args.run_id,
            orchestrator=args.orchestrator,
            orchestrator_run_id=args.orchestrator_run_id,
        )
    except Exception as failure:
        # The record already names the step and carries the message; this is the line
        # the person who typed the command reads.
        print(f"{type(failure).__name__}: {failure}", file=sys.stderr)
        print("read the run with: python -m ingest.runs --raw "
              f"{args.raw}", file=sys.stderr)
        return 1

    print(f"run {run_id} succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
