# ADR writing guide

This guide codifies the style, tone, structure, and writing rules for ADRs in
the runkit project. Every rule here was established through iterative refinement
of ADR-0002. See `docs/adr/0002-rendezvous-hashing-for-workload-assignment.md`
as the reference exemplar.

## Format and structure

- Use the Nygard format strictly: Context, Decision, Consequences. No other
  top-level sections (no "Options", "Alternatives", etc.).
- The title states the decision, not the problem. For example, "Rendezvous
  hashing for workload assignment", not "How should we assign workloads?"
- File naming: `NNNN-title-with-dashes.md`.
- One decision per ADR. If the scope creeps, split into separate ADRs.

## Voice and tone

- First-person plural throughout: "We will...", "We need...", "We considered..."
- Conversational, not academic.
- Match the voice of dogma ADRs (see 0009, 0020, 0022, 0023, 0026 as
  exemplars in the dogma repository).

## Characters

- No non-ASCII characters in prose: no em dashes, en dashes, curly quotes, or
  other special characters.
- Standard ASCII punctuation only. Use hyphens, not dashes.
- Special characters are fine in code blocks and formulas where they add value.

## Terminology

- Introduce every term before using it. If "candidate" appears in the Decision
  section, it must first appear in Context.
- Pick one term and use it consistently throughout. Don't alternate between
  synonyms (e.g. "input" and "unit of work" for the same concept).
- No jargon without explanation. Write for average programmers, not domain
  experts. If a term like "avalanche properties" needs a sentence to explain,
  replace it with plain language instead.
- Avoid terms that collide with established meanings in other domains (e.g.
  "self-affine" has a specific meaning in fractal geometry).
- Terms well-defined in the Dogma ecosystem (command, aggregate, process, etc.)
  can be used without redefinition.

## Claims and evidence

- Quantify performance claims or hedge them. Say "in the order of nanoseconds",
  not "fast". Say "roughly 6 ns" only if you have a citable source.
- Don't cite specific figures without a reliable source.
- Be honest about tradeoffs. Don't misrepresent dismissed alternatives to make
  the chosen approach look better.

## References and links

- Link external concepts to Wikipedia on first use (e.g. consistent hashing,
  Kademlia, Voronoi partition).
- Link code identifiers to pkg.go.dev (e.g. `uuidpb.Validate()`).
- Link RFCs to rfc-editor.org.
- Use markdown reference-style links, collected at the bottom of the file inside
  a `<!-- references -->` comment.
- Keep the reference list alphabetized.

## Scope boundaries

- Don't reference undecided concepts or unfinished implementation details (e.g.
  don't mention "heartbeats" or "command backlog" if those haven't been decided).
- Use "we are free to..." not "future ADRs will..." when describing potential
  future applications.
- Consequences must describe inherent properties of the decision, not
  aspirational claims. Don't assert properties that aren't guaranteed by the
  decision itself.
- Don't base decisions on rejected or superseded ADRs. Verify that any
  referenced ADR is still in "Accepted" status.

## Dismissed alternatives

- Include a "Dismissed alternatives" subsection under Decision when relevant.
- Give honest, specific reasons for dismissal.
- Acknowledge genuine advantages of alternatives before explaining why they were
  rejected. Don't strawman them.

## Glossary integration

- Each ADR notes which glossary terms it introduces, in the Consequences
  section. Example: "This ADR introduces two terms to the glossary: rendezvous
  hashing and self-affinity."
- The glossary builds incrementally, ADR by ADR.

## Pseudocode

- Use the same terms as the surrounding prose. If the prose says "winner" and
  "workload", the pseudocode should too.
- Keep pseudocode minimal and readable.

## Pre-flight checks

Before finalizing an ADR:

- Verify the date is correct.
- Confirm all referenced ADRs are in "Accepted" status.
- Check that every term is introduced before first use.
- Check that no non-ASCII characters appear in prose.
- Check that reference links are alphabetized and resolve correctly.
