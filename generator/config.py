"""What a generation run is configured with.

All switches default to off: that is the clean baseline every pipeline test starts
from, so it is what you get unless you ask for something else.
"""

from dataclasses import dataclass, field
from pathlib import Path

from . import schema

__all__ = ["Config", "SCHEMA_DRIFT_CHOICES"]

SCHEMA_DRIFT_CHOICES = ("none", "add_column", "drop_column")


@dataclass
class Config:
    seed: int = 42
    out_dir: Path = field(default_factory=lambda: Path("data/source"))
    periods: str = "2026-01:2026-12"
    entries_per_period: int = 1_000

    # Failure modes. All off is the clean baseline every pipeline test starts from.
    late_entries: bool = False
    restatements: bool = False
    cost_centre_move: bool = False
    schema_drift: str = "none"
    schema_drift_table: str = "gl_entry"
    unbalanced_vouchers: bool = False
    growing_account: bool = False
    amount_outliers: bool = False

    def __post_init__(self) -> None:
        self.out_dir = Path(self.out_dir)
        if self.schema_drift not in SCHEMA_DRIFT_CHOICES:
            raise ValueError(
                f"schema_drift must be one of {SCHEMA_DRIFT_CHOICES}, got {self.schema_drift!r}"
            )
        if self.schema_drift_table not in schema.COLUMNS:
            raise ValueError(
                f"schema_drift_table must be one of {sorted(schema.COLUMNS)}, "
                f"got {self.schema_drift_table!r}"
            )
