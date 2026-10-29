# Contract evolution

All schemas under `contracts/jsonschema/*-v1-draft.json` are **draft, not approved**. Reject unknown breaking changes. Preserve raw original payload immutably. Compatible additive nullable fields require validation updates and a documented minor version change. Identity or time semantics changes require a major version and migration ADR. Require R1+R2 review for transaction/CDC changes; R2+R3 for Gold KPI semantic changes; R4 for permissions/deployment.
