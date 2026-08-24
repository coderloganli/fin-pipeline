# contracts

One explicit contract per source table: columns, types, nullability, and semantic constraints.

The contract is what makes schema evolution a decision rather than an accident. Adding a column is compatible and passes with a warning. Dropping a column, changing a type, or changing the meaning of a field is incompatible and fails the run, and the dbt lineage graph names the downstream models that would have broken.
