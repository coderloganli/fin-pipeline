# A vendor belongs to the voucher, and is drawn from its own stream

## Context

The insight layer's plan gives it four query functions, one of which is
`breakdown_by_vendor`, and the product's worked example turns on a sentence like "97%
of the increase came from vendor V-1042". The long-tail anomaly exists to be
*indistinguishable* from a concentrated one until somebody asks a second question, and
"is it concentrated in one vendor" is that second question.

The source tables carried no vendor and no description at all. Two questions had to be
settled to add them: which rows carry a vendor, and where the values come from.

## Decision

**A vendor belongs to the voucher, not to the line.** A voucher that involves a vendor
— debit an expense detail account, credit a payables detail account — carries the same
`vendor_code` on both of its lines. A voucher that does not involve one — debit
receivables, credit revenue — leaves `vendor_code` empty on both. `description` follows
the same rule and is never empty.

`vendor_code` is declared `nullable: true`; an empty CSV field is null, which the
contracts already say.

**The dimension comes from a stream of its own.** `generator/streams.py` gains
`VENDORS = "vendors"`, and building `dim_vendor` is the only thing that draws from it.

**The assignment itself draws nothing.** A voucher's supplier and wording are chosen by
position within its account's category - `codes[index % len(codes)]` - rather than
sampled. That is what makes the long tail spread evenly across thirty suppliers and a
concentrated anomaly land on one, and it means the two columns are a function of where a
voucher sits rather than of how many draws preceded it.

The rule "a vendor appears only on expense-side vouchers" is asserted by tests and
guaranteed by the generator. It is deliberately **not** a contract rule: the contract
validates a row at a time and would have to join `dim_account_src` to know an account's
type, which is a lineage the validator does not have and should not grow.

## Reasoning

**Per voucher rather than per line**, because that is what the document is. One
purchase invoice is from one vendor, and both halves of the entry it produces are about
that invoice. It also means `breakdown_by_vendor` aggregates correctly from either side
— summing debits or summing credits both reach the same vendor — where a debit-only
stamp would silently halve any aggregate taken from the credit side.

**Empty on revenue vouchers** rather than a vendor everywhere. Revenue is earned from
customers, not paid to suppliers, and stamping a supplier on 主营业务收入 is the kind of
detail that costs nothing to get right and is embarrassing to get wrong. It also gives
the pipeline a nullable column that is null for a business reason, which is a better
test of the null handling than `parent_code`'s single empty row.

**A stream of its own** is the constraint that shapes the implementation.
`docs/adr/0005-deterministic-generation.md` derives each stream from
`SHA-256(f"{seed}:{name}")` precisely so that a new concern can be added without moving
the numbers belonging to the old ones. Drawing a vendor from the existing `entries`
stream would shift every subsequent draw: amounts, dates and identifiers of entries
that have nothing to do with vendors would all change, and the isolation test that
compares switch configurations byte for byte would fail — correctly, because the
property it protects would genuinely be broken.

A test pins this directly rather than leaving it to the reviewer: regenerating with a
different seed for the `vendors` stream alone must leave the other ten columns of
`gl_entry` identical row for row.

**Descriptions are phrases chosen by account category, not free text.** The generator
is deterministic, and a description that varied between runs would break that for no
gain. Phrases are drawn from a per-category table, so a travel expense reads like a
travel expense and the same seed produces the same wording.

The vendor distribution across categories is a design input rather than an accident:
office supplies carries around thirty vendors so the long tail has somewhere to spread,
and marketing carries two, of which the growing account uses exactly one. The amount
outlier concentrates on one supplier too, but on one of its own account's category: an
outlier has to sit on an ordinary account, because what makes it an outlier is being
twenty times the median of the entries around it. A single vendor rather than a pair
because the test that separates the shapes measures the top vendor's share of the
increase, and two vendors splitting it evenly would put that share near a half, which is
neither concentrated nor spread.

Without that asymmetry the two anomaly shapes would look the same under
`breakdown_by_vendor`, and the A/B comparison the whole third step is built on would
have nothing to show.

The increase is measured as the difference between generating with the switch off and
with it on. The long-tail account carries its three hundred vouchers either way, by
`docs/adr/0007`, so grouping only the switched-on period by vendor would look spread out
even if the uplift never happened.
