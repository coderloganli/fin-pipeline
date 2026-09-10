"""What a run is: a sequence of named steps, and the record of what each one did.

Running this platform used to be seven commands in a remembered order, and only the
second of them wrote anything down. This package is the order, and the writing down.

`steps.py` declares the steps as data - a name and what it calls. `run.py` executes
them, opening the run record before the first and closing it after the last however it
ended. `dbt.py` is the one statement of how dbt is invoked here.

Everything that can be wrong about a run is in this package rather than in a DAG file:
the order, that a failure stops what follows, what each step records, and that
`clear-affected` runs last. Those are facts about this platform, not about the scheduler
that happens to be installed - so `pytest -q` exercises them directly and the files
under `dags/` declare a schedule and a task graph and call in here.

See docs/adr/0044, 0045 and 0046.
"""
