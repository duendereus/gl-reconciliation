# F. — AI-Powered GL Reconciliation

> Detect transaction breaks in seconds. Understand why. Know what to do next.

A case study in productizing LLMs for fintech finance operations. Upload a CSV of transactions and the app detects breaks (FX rate mismatches, missing counterparties, duplicates, AML flags), explains the root cause of each in plain English, and recommends a specific action — backed by a configurable rule engine and a DB-cached AI layer so re-analyses are instant and cheap.

**Live demo:** `https://gl-reconciliation-production.up.railway.app` · login `demo` / `demo2026` (read-only)

---

## What it does

For each break detected, the AI returns:

- **Root cause** — plain English, COO-readable
- **Recommended action** — specific, operational
- **Confidence score** (0-100%)
- **Priority** (Low / Medium / High / Critical)
- **Step-by-step traceability** — fully auditable reasoning chain

Built-in detectors:

| Type | Rule |
|---|---|
| FX Rate | Applied rate vs reference rate (delta > 0.01) |
| Missing Counterparty | DISPATCHED transactions > 48h without bank confirmation |
| Duplicate | Same counterparty + amount + timestamp |
| Interest Mismatch | Savings accounts where credited MXN deviates from expected yield |
| AML Flag | Velocity-based heuristic for unregistered beneficiaries |
| Unauthorized Reversal | Negative amounts with no matching original |
| Settlement Timeout | Dispatched past the 15:00h cutoff |
| SPEI Duplicate | Network retry pattern in SPEI transactions |
| Fee Mismatch | Applied fee diverges from contracted rate |

On top of these, finance teams can add their own rules (CRUD UI in the Rules tab) without touching code.

---

## Engineering highlights

The AI is maybe 20% of the work. The rest is what makes it feel like a product:

- **DB-backed analysis cache** — every break analysis is hashed and stored; re-analyzing the same break is instant and free (skips the Claude API entirely).
- **Configurable rule engine** — declarative rules stored in DB, evaluated against any transaction CSV. Operators: `gt`, `lt`, `eq`, `neq`, `delta_gt`, `contains`, `not_contains`. Optional type/status filters.
- **CSV validation with COO-readable errors** — "Missing required columns: rate_applied, timestamp" instead of a 500 stacktrace.
- **Concurrent Claude calls** — bounded semaphore (3 parallel) + exponential backoff on 429s.
- **Real auth + read-only mode** — token-based sessions persisted across hard refresh, audit log of every login (IP + user agent), `READ_ONLY=true` env flag for public deployments that lets only admins make changes.
- **Lightweight schema migrations** — auto-detects missing columns on startup and `ALTER TABLE`s them in (works on SQLite + Postgres).
- **Branded PDF export** — multi-page report with executive summary, per-break analysis, and dynamically-derived next steps. Built client-side with jsPDF.
- **First-time onboarding tour** — sequenced tooltips, dismissible, persisted via localStorage.

---

## Stack

| Layer | Choice |
|---|---|
| Frontend | Single-file vanilla HTML/CSS/JS · Chart.js · jsPDF |
| Backend | FastAPI (Python 3.11) |
| AI | Claude Sonnet 4 (configurable via `CLAUDE_MODEL`) |
| Data | pandas |
| DB | SQLite (local) / Neon Postgres (prod) — via SQLAlchemy |
| Hosting | Railway |
| Tests | pytest · 100+ tests |

---

## Run locally

```bash
git clone https://github.com/duendereus/gl-reconciliation
cd gl-reconciliation
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY
docker compose up --build
```

Open `http://localhost:8000` and login with `demo` / `demo2026`.

To run without Docker:

```bash
pip install -r requirements.txt
uvicorn backend.main:app --reload --port 8000
```

To run the test suite:

```bash
pytest backend/tests/ -v
```

---

## Project structure

```
gl-reconciliation/
├── backend/
│   ├── main.py                    ← FastAPI entry, lifespan, user/rules seeding
│   ├── database.py                ← SQLAlchemy + auto column migrations
│   ├── models.py                  ← User, Session, BusinessRule, SavedDataset, AnalysisCache
│   ├── routes/
│   │   ├── auth.py                ← /auth/login, /me, /logout, /config + read-only middleware
│   │   ├── analyze.py             ← POST /analyze (CSV upload + persistence)
│   │   ├── datasets.py            ← GET /datasets, /datasets/{id}
│   │   ├── rules.py               ← Rule CRUD + seed/reset
│   │   └── admin.py               ← /admin/stats, /cache, /datasets (DB inspection)
│   ├── services/
│   │   ├── reconciliation.py      ← Built-in break detectors
│   │   ├── rule_engine.py         ← Dynamic rule evaluator
│   │   ├── claude_client.py       ← AsyncAnthropic + cache + concurrency control
│   │   └── csv_validator.py       ← Schema validation with user-friendly errors
│   └── tests/                     ← 100+ pytest tests
├── comp_files/
│   ├── r_reconciliation_v8_consistent.html  ← Self-contained UI
│   ├── dataset_1.csv              ← 200 synthetic transactions
│   └── dataset_2.csv              ← 500 synthetic transactions
├── Dockerfile
├── docker-compose.yml
├── railway.toml
├── requirements.txt
└── .env.example
```

---

## API

| Endpoint | Method | Purpose |
|---|---|---|
| `/` | GET | Serves the frontend |
| `/health` | GET | Healthcheck (Railway) |
| `/auth/config` | GET | Public flags (e.g. `read_only`) |
| `/auth/login` | POST | Token-based session |
| `/auth/me` | GET | Validate token |
| `/auth/logout` | POST | Invalidate session |
| `/auth/login-events` | GET | Audit log |
| `/datasets` | GET | List saved analyses |
| `/datasets/{id}` | GET | Load a saved analysis |
| `/analyze` | POST | Upload CSV → detect breaks → AI analysis (admin-only when read-only) |
| `/rules` | GET/POST | List + create rules |
| `/rules/{id}` | GET/PUT/DELETE | Rule CRUD |
| `/rules/seed` | POST | Seed default rules |
| `/rules/reset` | POST | Wipe and re-seed |
| `/admin/stats` | GET | DB stats + cache efficiency metrics |
| `/admin/cache` | GET | Inspect cached analyses |

---

## Configuration

All env vars (see `.env.example`):

```env
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql://...   # optional, falls back to SQLite
CLAUDE_MODEL=claude-sonnet-4-20250514
READ_ONLY=true                  # blocks writes for non-admin users
DEMO_USERNAME=demo
DEMO_PASSWORD=demo2026
ADMIN_USERNAME=admin
ADMIN_PASSWORD=changeme         # CHANGE THIS in production
```

---

## What's not in this repo

- Real transaction data (everything is synthetic)
- BANXICO API integration (FX reference rate is hardcoded)
- SWIFT gpi integration (recommendations are AI-generated, not executed)
- CNBV regulatory filing (outputs are advisory only)

---

## License

MIT

---

Built by [Fernando Céspedes](https://www.linkedin.com/in/fernandocespedesm/) · A portfolio project on productizing LLMs for finance ops · All data shown is synthetic.
