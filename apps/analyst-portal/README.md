# Optional analyst portal, not a Power BI replacement

- `web/`: React/TypeScript reference frontend, shows `DEMO_FIXTURE · NOT LIVE`; source written, not built in this environment because npm packages absent.
- `../analyst_portal/api/`: importable Python FastAPI reference backend, local SQLite allowlisted/read-only, disabled until local demo token explicitly set. Real Genie API, UC permission mapping, rate limits, authn/authz and production hosting not implemented.
- `apps/analyst-portal/api/README.md` explains path mapping. R3 owns the web UI, R4 the API, R2 metric/data contract reviews.
