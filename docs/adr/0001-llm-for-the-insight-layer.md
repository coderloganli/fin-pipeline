# DeepSeek V4 Flash is the model behind the insight layer

summary: The insight layer calls DeepSeek V4 Flash, pinned to an explicit build, because the investigation loop needs native tool calling and schema-constrained output.

## Context

The insight layer explains anomalies that the pipeline has already flagged. It does
not receive a pre-assembled slice of context; it investigates. Given one flagged
balance it forms a hypothesis, calls one of four parameterised query functions to
test it, and either confirms or rules it out before deciding what to look at next.
The loop is bounded from outside the model — a fixed tool registry, an eight-step
ceiling, trace de-duplication, and a citation check performed after the fact.

That shape constrains the choice of model in three ways:

- **Tool calling is not optional.** Without it there is no investigation loop, and
  the layer collapses back to handing the model a fixed top-N slice. That fallback
  is known to fail on long-tail anomalies, where the cause is spread across
  hundreds of small entries and never appears in the slice.
- **Every explanation must cite the entries it relied on**, and those citations are
  verified against the set of entry ids the investigation actually retrieved. Parsing
  ids out of free-form prose is an avoidable class of failure.
- **The golden-set score is a CI gate**, so the evaluation runs on every build. A
  per-call cost high enough to discourage running it would undermine the gate.

## Decision

The insight layer calls **DeepSeek V4 Flash** (`deepseek-v4-flash`).

Requests use schema-constrained output, so the model returns
`{explanation, cited_entry_ids[], no_single_driver}` rather than prose to be parsed.

The `insight` table records the **specific build** (for example
`deepseek-v4-flash-0731`), never the `deepseek-v4-flash` alias. The alias tracks the
latest build, so recording it would leave the reproducibility contract empty: a
stored explanation could not be tied to the model that produced it. Where the API
response does not carry the build identifier, it is declared in configuration and
updated together with this record.

## Reasoning

Three properties decided it, all verified against the official API documentation
rather than recalled:

- **Native function calling** (`tools`, `tool_choice`). This is the hard
  requirement. A model without it cannot drive the investigation loop at all.
- **Structured output against a JSON schema** (`response_format`). This moves
  citation handling from prose parsing to a typed field, removing parse failures
  as a failure mode and leaving only the semantic check — whether each cited id was
  genuinely retrieved during the investigation.
- **Cost low enough to be irrelevant to the design.** At off-peak rates a full
  golden-set run is on the order of a quarter of a dollar, so the CI gate can run
  without rationing. Cost is therefore not a constraint on how the layer is built.

The alternative considered was any model without tool calling. It was rejected for
what it would force rather than for what it costs: the layer would have to fall back
to a fixed context slice, which cannot reach the cause of a long-tail anomaly no
matter how capable the model is. Retrieval breadth, not model strength, is the
binding constraint on this layer.

The known cost of this decision is version drift. The 0731 build is in public beta
and the alias points at whatever is newest, so the golden-set score can move without
any change to this repository. Pinning the recorded build is what makes that drift
visible; the evaluation gate must be able to distinguish a score drop caused by a
prompt change from one caused by a model change.
