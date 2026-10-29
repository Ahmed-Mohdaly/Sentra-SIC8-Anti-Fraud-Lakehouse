# GitHub Publication & Team Setup

**Status:** GitHub repo creation and remote publishing are pending. This document is not proof that anything is uploaded.

## Repository target

- Owner GitHub login (verified): `Ahmed-Mohdaly`
- Proposed repository: `SIC8-Anti-Fraud-Lakehouse`
- Privacy: **Private** until team completes security and licensing review.
- Default branch: `main`.
- Origin URL: `https://github.com/Ahmed-Mohdaly/SIC8-Anti-Fraud-Lakehouse.git` (planned; may 404 until created).

## One-time setup required by owner

1. On GitHub, create a **Private**, **empty** repository named `SIC8-Anti-Fraud-Lakehouse` under `Ahmed-Mohdaly`; do **not** add a generated README, `.gitignore`, or LICENSE.
2. Authorize the GitHub app/connector for this repository via GitHub's app installation settings so the assistant can access it. Avoid personal access tokens in chat.
3. Confirm the URL; the existing local source can then be published.

## Local publishing (fallback, only after repo exists)

```bash
cd sic8-anti-fraud-lakehouse
git status
python -m pytest -q
git add .
git diff --cached --stat
# Configure your own verified Git identity locally; do not invent a teammate's email.
git config user.name 'YOUR GITHUB DISPLAY NAME'
git config user.email 'YOUR VERIFIED GITHUB EMAIL'
git commit -m 'chore: initial SIC8 engineering baseline'
git branch -M main
git remote add origin https://github.com/Ahmed-Mohdaly/SIC8-Anti-Fraud-Lakehouse.git
git push -u origin main
```

If `origin` already exists, inspect `git remote -v` instead of adding it again. Use browser/OAuth or an authorized credential manager; do not paste tokens in a command history or commit them.

## Collaboration configuration once published

- Invite R1 Sayed, R3 Ahmed Abd El Razek, and R4 Ahmed Eslam **after collecting their actual GitHub usernames**.
- Add GitHub handles for all owners to `.github/CODEOWNERS`. The current file lists only verified R2.
- Enable branch protection/rulesets on `main`: require PR review, require the offline CI check, block force-push/deletion, and prefer linear history. Some controls depend on plan/repository visibility.
- Each contributor branches from `main`: `feature/r1-*`, `feature/r2-*`, `feature/r3-*`, `feature/r4-*`. Use PRs for merges.
- R4 tests the CI workflow and required checks on GitHub after first push. **Local pytest passing is not GitHub Actions evidence.**
- Do not upload S3 baseline CSVs, real AWS secrets, model secrets, or other teammates' missing binaries until reviewed. `.pbix/.pbit` are allowed by Git but require deliberate security and file-size checks; prefer Git LFS for large versions.

## Status boundaries

This source is a partial, offline-verified reference implementation. AWS/Databricks/Power BI and bonus features need owners' source files and integration evidence before they can be marked complete. See `STATUS.md` and `NEXT_HANDOFF.md`.
