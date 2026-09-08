"""Landing the staging Parquet in Postgres, and the connection settings both the
loader and this suite resolve.

The loader is the one thing between figures Spark computed and figures dbt models, so
what it has to establish is that nothing changes on the way across: a decimal that
needs all its digits comes back with all of them, a second load leaves the table where
the first one left it, and a load that fails leaves the previous table alone.

The Postgres schema is called `landing`, not `staging`: docs/adr/0026 already owns the
name staging for the Parquet layer, and one name for two things is the failure this
repository argues against everywhere else. See docs/adr/0034.

Cases 1-12 of task.md.
"""

from decimal import Decimal
from pathlib import Path

import pytest

pytestmark = pytest.mark.db

VENDOR_COLUMNS = ["vendor_code", "name", "category"]


def column_types(db, schema: str, table: str) -> dict[str, str]:
    """Column name to the type Postgres actually gave it, precision included."""
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        rows = cursor.fetchall()
    types = {}
    for name, kind, precision, scale in rows:
        types[name] = f"numeric({precision},{scale})" if kind == "numeric" else kind
    return types


def table_checksum(db, schema: str, table: str, exclude=()) -> str:
    """The row-level checksum docs/adr/0017 defines, over a Postgres table.

    Rendered as length-prefixed text, columns sorted by name, rows sorted, hashed. Not
    over anything physical: a table is a set of rows, and two tables holding the same
    rows in different physical order have to check the same.
    """
    import hashlib

    from ingest import raw

    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        columns = sorted(name for (name,) in cursor.fetchall() if name not in exclude)
        quoted = ", ".join(f'"{name}"' for name in columns)
        cursor.execute(f'SELECT {quoted} FROM "{schema}"."{table}"')
        rows = cursor.fetchall()

    rendered = sorted(
        raw.UNIT_SEPARATOR.join(f"{len(str(value))}:{value}" for value in row)
        for row in rows
    )
    digest = hashlib.sha256()
    for line in rendered:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def row_count(db, schema: str, table: str) -> int:
    with db.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
        return cursor.fetchone()[0]


# --- case 1: the Arrow schema decides the Postgres types --------------------

def test_arrow_types_become_postgres_types(mart, clean_staging, db):
    """1. String, date, decimal and bigint land as text, date, numeric(p,s), bigint,
    with the precision and scale the Parquet declared."""
    build = mart(clean_staging)
    types = column_types(db, build.landing, "fct_gl_entry")

    assert types["entry_id"] == "text"
    assert types["accounting_date"] == "date"
    assert types["amount_dr"] == "numeric(18,2)"
    assert types["rate_to_base"] == "numeric(18,6)"
    assert types["amount_dr_base"] == "numeric(32,2)"

    # bigint has a home too: `rolling_periods` is a 32-bit count out of Spark, and the
    # mapping widens it rather than guessing int4 on the copy wire.
    assert column_types(db, build.landing, "agg_monthly_balance")["rolling_periods"] == (
        "bigint"
    )


def test_the_widest_spark_decimal_survives(mart, clean_staging, db):
    """2. `agg_monthly_balance` carries decimal(38,2) - the widest the Spark layer
    produces - and it lands as numeric(38,2). Checked rather than assumed: at Spark's
    maximum of 38 a further widening reduces the scale instead of raising."""
    build = mart(clean_staging)
    types = column_types(db, build.landing, "agg_monthly_balance")

    assert types["debit_total"] == "numeric(38,2)"
    assert types["credit_total"] == "numeric(38,2)"
    # All three balance columns, because the widening applies to each: the delta is a
    # sum of the same amounts and the restated figure is a sum of two of these. See
    # docs/adr/0043.
    assert types["balance_as_reported"] == "numeric(38,2)"
    assert types["restatement_delta"] == "numeric(38,2)"
    assert types["balance_as_restated"] == "numeric(38,2)"


def test_a_decimal_survives_the_round_trip_exactly(mart, clean_staging, db, spark):
    """3. Exactly, not within a tolerance. A conversion that went through a float
    would pass a tolerance and fail this."""
    from transform.spark import facts

    build = mart(clean_staging)
    from_parquet = {
        (row["entry_id"], row["version"]): row["amount_dr_base"]
        for row in facts.read(spark, clean_staging.staging)
    }
    with db.cursor() as cursor:
        cursor.execute(
            f'SELECT entry_id, version, amount_dr_base FROM "{build.landing}".fct_gl_entry'
        )
        landed = {(entry_id, version): amount for entry_id, version, amount in cursor}

    assert landed == pytest.approx(from_parquet, abs=0) or landed == from_parquet
    for key, value in landed.items():
        assert isinstance(value, Decimal)
        assert value == from_parquet[key]


def test_loading_twice_changes_nothing(mart, clean_staging, db):
    """4. The second load neither appends nor duplicates: same row count, same
    checksum. The loader drops and recreates, so this is the property that says it
    does."""
    build = mart(clean_staging)
    before_count = row_count(db, build.landing, "fct_gl_entry")
    before_sum = table_checksum(db, build.landing, "fct_gl_entry")

    from transform import load as mart_load

    mart_load.load_all(
        staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
        schema=build.landing,
    )

    assert row_count(db, build.landing, "fct_gl_entry") == before_count
    assert table_checksum(db, build.landing, "fct_gl_entry") == before_sum


def test_a_failed_copy_leaves_the_previous_table(mart, clean_staging, db, monkeypatch):
    """5. The DDL and the copy are one transaction, so a copy that raises after the
    table was recreated leaves the rows that were there before."""
    from transform import load as mart_load

    build = mart(clean_staging)
    before = table_checksum(db, build.landing, "fct_gl_entry")

    real_copy = mart_load.copy_rows

    def explode(cursor, schema, model, table):
        """Write the first batch, then die. A stub that raises before writing anything
        would test that the DDL rolls back on its own, which is not the scenario: what
        has to roll back is a table that was recreated and then half filled."""
        head = table.slice(0, max(table.num_rows // 2, 1))
        real_copy(cursor, schema, model, head)
        raise RuntimeError("the copy died halfway")

    monkeypatch.setattr(mart_load, "copy_rows", explode)
    with pytest.raises(RuntimeError):
        mart_load.load_all(
            staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
            schema=build.landing,
        )

    assert table_checksum(db, build.landing, "fct_gl_entry") == before


@pytest.mark.parametrize(
    "arrow_type, name",
    [
        (lambda pa: pa.timestamp("us"), "timestamp"),
        # A near miss: `is_decimal` also matches decimal256, so a mapping written by
        # family would accept it and the binary copy would then write it wrongly.
        (lambda pa: pa.decimal256(40, 2), "decimal256"),
        (lambda pa: pa.float64(), "double"),
    ],
)
def test_an_unmapped_arrow_type_stops_the_load(tmp_path, db, arrow_type, name):
    """6. Naming the column and the type, rather than guessing a Postgres type. A
    guess here would land a column whose values are silently wrong.

    Through the loader, not through the mapping function alone: what has to fail is a
    load, and a mapping that refused in isolation while the loader never called it
    would pass a test that proved nothing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from transform import load as mart_load

    kind = arrow_type(pa)
    staging = tmp_path / "staging"
    (staging / "odd_model").mkdir(parents=True)
    pq.write_table(
        pa.table({"when": pa.array([None, None], type=kind)}),
        staging / "odd_model" / "part-0000.parquet",
    )

    with pytest.raises(mart_load.UnmappedType) as failure:
        mart_load.load_model(db, staging, tmp_path / "raw", "public", "odd_model")

    assert "when" in str(failure.value)
    assert name in str(failure.value).lower()


def test_the_other_near_miss_is_refused_at_the_mapping(tmp_path):
    """6a. `date64` is the other type a family match would have swallowed - `is_date`
    matches it too - and it cannot be tested through a file, because Parquet has no
    date64 and normalises it to date32 on the way in. So it is asserted where it can be:
    against the mapping, which is the thing that would have been wrong."""
    import pyarrow as pa

    from transform import load as mart_load

    with pytest.raises(mart_load.UnmappedType) as failure:
        mart_load.postgres_schema(pa.schema([pa.field("when", pa.date64())]))

    assert "when" in str(failure.value)
    assert "date64" in str(failure.value).lower()


def test_dim_vendor_lands_only_its_contract_columns(mart, clean_staging, db):
    """7. Read from raw, not staging, and projected to the three columns the contract
    declares. Raw rows carry `_first_run_id` and `_last_run_id`; ingestion metadata is
    reached through the run record, not carried forward. See docs/adr/0020 and 0034."""
    build = mart(clean_staging)
    types = column_types(db, build.landing, "dim_vendor")

    assert sorted(types) == sorted(VENDOR_COLUMNS)
    assert set(types.values()) == {"text"}

    # And it could only have come from raw: transform/spark/scd2.py does not build a
    # staging form for a dimension that is not effective-dated, so there is none to read.
    assert not (clean_staging.staging / "dim_vendor").exists()


def test_a_staging_model_lands_whole(mart, clean_staging, db):
    """8. `transform/spark/` has already decided what belongs in a staging model, so
    the loader carries all of it - the two provenance columns included."""
    build = mart(clean_staging)
    types = column_types(db, build.landing, "fct_gl_entry")

    assert "source_first_run_id" in types
    assert "source_last_run_id" in types
    assert "_first_run_id" not in types


# --- cases 9-11: the settings both the loader and this suite resolve --------

def test_settings_resolve_environment_then_env_file_then_default(tmp_path):
    """9. Every setting, the two schema names included. Compose reads `.env` and pytest
    otherwise would not: editing it has to move the database and the tests together.

    The environment is passed in rather than patched. `settings` takes it as an argument
    for the same reason it takes the repository root — and it matters here rather than
    only being tidy: CI sets `POSTGRES_MART_SCHEMA` for the job, so a test reading the
    real environment would find the ambient value winning over the file it just wrote and
    fail on a machine where the setting works exactly as designed.
    """
    from transform import db as transform_db

    (tmp_path / ".env").write_text(
        "POSTGRES_DB=from_file\nPOSTGRES_MART_SCHEMA=mart_from_file\n", encoding="utf-8"
    )

    values = transform_db.settings(
        repo_root=tmp_path, env={"POSTGRES_DB": "from_environment"}
    )

    assert values["POSTGRES_DB"] == "from_environment"
    assert values["POSTGRES_MART_SCHEMA"] == "mart_from_file"
    assert values["POSTGRES_LANDING_SCHEMA"] == transform_db.DEFAULTS[
        "POSTGRES_LANDING_SCHEMA"
    ]

    # Every setting resolves, and every one of them can be moved from either place. A
    # key that only ever took its default would look identical to one nothing reads.
    assert set(values) == set(transform_db.DEFAULTS)
    for key in transform_db.DEFAULTS:
        (tmp_path / ".env").write_text(f"{key}=from_file\n", encoding="utf-8")
        from_file = transform_db.settings(repo_root=tmp_path, env={})
        assert from_file[key] == "from_file", key
        from_env = transform_db.settings(
            repo_root=tmp_path, env={key: "from_environment"}
        )
        assert from_env[key] == "from_environment", key


def test_settings_takes_its_root_as_an_argument(tmp_path):
    """10. Not as a module global. `tests/test_environment.py` used to monkeypatch the
    root to test `.env` precedence, and a resolver reading its own globals would quietly
    stop being affected by that."""
    from transform import db as transform_db

    (tmp_path / ".env").write_text("POSTGRES_USER=someone_else\n", encoding="utf-8")
    here = transform_db.settings(repo_root=tmp_path, env={})
    elsewhere = transform_db.settings(repo_root=tmp_path / "elsewhere", env={})

    assert here["POSTGRES_USER"] == "someone_else"
    assert elsewhere["POSTGRES_USER"] == transform_db.DEFAULTS["POSTGRES_USER"]


def test_conftest_re_exports_rather_than_copies():
    """11. The suite and the loader resolve settings through one function. Two copies
    would drift, and the one that drifted would be the one nothing tested."""
    from transform import db as transform_db

    import conftest

    assert conftest.settings is transform_db.settings
    assert conftest.connect is transform_db.connect
    assert conftest.DatabaseUnavailable is transform_db.DatabaseUnavailable
    assert conftest.DEFAULTS == transform_db.DEFAULTS


# --- case 12: the schema is configuration ----------------------------------

def test_load_all_closes_the_connection_it_opened(clean_staging, monkeypatch):
    """12a. And leaves alone the one it was handed. A loader that closed a caller's
    connection would break the next statement on it, at a distance."""
    from transform import db as transform_db, load as mart_load

    opened = []
    real_connection_from = transform_db.connection_from

    def watched(*args, **kwargs):
        connection = real_connection_from(*args, **kwargs)
        connection.autocommit = True
        opened.append(connection)
        return connection

    monkeypatch.setattr(mart_load.db, "connection_from", watched)
    mart_load.load_all(
        staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
        schema="landing_test_ownership", models=["dim_vendor"],
    )
    assert opened and all(connection.closed for connection in opened)

    borrowed = transform_db.connection_from()
    borrowed.autocommit = True
    try:
        mart_load.load_all(
            staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
            schema="landing_test_ownership", connection=borrowed,
            models=["dim_vendor"],
        )
        assert not borrowed.closed
    finally:
        with borrowed.cursor() as cursor:
            cursor.execute('DROP SCHEMA IF EXISTS "landing_test_ownership" CASCADE')
        borrowed.close()


def test_the_loader_writes_into_the_configured_schema(mart, clean_staging, db):
    """12. Creating it if absent, and leaving the default one alone. Running the suite
    must not overwrite the schema a developer has been looking at.

    Before and after, rather than asserting `landing` is empty. A developer who has run
    the pipeline by hand has a real `landing` schema full of real tables - which is the
    situation this setting exists for, so a test that only passes when it is absent is
    testing the developer's machine rather than the loader.
    """
    def landing_tables() -> set[str]:
        with db.cursor() as cursor:
            cursor.execute(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'landing'"""
            )
            return {name for (name,) in cursor.fetchall()}

    def landing_writes() -> dict[str, str]:
        """What is in the default schema, by content rather than by name."""
        return {
            table: table_checksum(db, "landing", table) for table in landing_tables()
        }

    before = landing_writes()
    build = mart(clean_staging)

    assert build.landing != "landing"
    assert row_count(db, build.landing, "fct_gl_entry") > 0
    assert landing_writes() == before


# --- a partitioned staging model across the boundary (ADR 0026, 0041) -------
#
# Case 38 of task.md. `fct_gl_entry` and `agg_monthly_balance` are partitioned by
# accounting period, so the column exists only in the directory names and pyarrow has
# to be told what type to give it back. `postgres_schema` matches types exactly and
# refuses what it has not seen - a dictionary-typed partition column would stop the
# load - and `copy_types` and the DDL both read the same Arrow schema, so the order
# they see has to be the order `copy_rows` writes.


def test_a_partitioned_model_crosses_the_boundary_whole(db, clean_staging):
    """Case 38. The whole boundary, not just the type: exactly one occurrence, values
    taken from the directory names, every partition on disk represented, one order for
    the DDL and the binary copy, and the row count unchanged."""
    import pyarrow as pa

    from transform import load as mart_load

    schema_name = "landing_case38"
    with db.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    db.commit()

    directory = mart_load.parquet_dir(clean_staging.staging, clean_staging.raw,
                                      "agg_monthly_balance")
    on_disk = {path.name.split("=", 1)[1]
               for path in directory.glob("accounting_period=*")}
    assert on_disk, "the model is not partitioned on disk"

    table = mart_load.read_table(directory, mart_load.columns_for("agg_monthly_balance"),
                                 partitioned=True)
    names = table.column_names

    assert names.count("accounting_period") == 1
    assert table.schema.field("accounting_period").type == pa.string()
    assert set(table.column("accounting_period").to_pylist()) == on_disk

    declared = [name for name, _ in mart_load.postgres_schema(table.schema)]
    assert declared == names
    assert len(mart_load.copy_types(table.schema)) == len(names)

    try:
        landed = mart_load.load_all(
            staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
            schema=schema_name, models=["agg_monthly_balance"],
        )
        assert landed["agg_monthly_balance"] == table.num_rows
        assert column_types(db, schema_name, "agg_monthly_balance")[
            "accounting_period"] == "text"
    finally:
        with db.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        db.commit()


def test_the_adjustment_fact_is_landed(db, clean_staging):
    """Case 39, first half. `gl_adjustment` was ingested and consumed by nothing; the
    loader is where it stops being."""
    from transform import load as mart_load

    assert "fct_gl_adjustment" in mart_load.MODELS

    schema_name = "landing_case39"
    try:
        landed = mart_load.load_all(
            staging_dir=clean_staging.staging, raw_dir=clean_staging.raw,
            schema=schema_name, models=["fct_gl_adjustment"],
        )
        assert landed["fct_gl_adjustment"] > 0
        types = column_types(db, schema_name, "fct_gl_adjustment")
        assert "adjusts_entry_id" in types
        assert "adjustment_type" in types
    finally:
        with db.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        db.commit()


def test_a_partitioned_model_with_no_partitions_says_so(tmp_path):
    """A partitioned model holding nothing has no schema to land, so the loader stops
    and says which directory - rather than falling through to Parquet schema inference
    and failing with a message about Arrow. Breaking is better than drifting."""
    from transform import load as mart_load

    empty = tmp_path / "fct_gl_entry"
    empty.mkdir()

    with pytest.raises(mart_load.EmptyStagingModel) as failure:
        mart_load.read_table(empty, None, partitioned=True)

    assert "fct_gl_entry" in str(failure.value)
    assert "staging build" in str(failure.value)
