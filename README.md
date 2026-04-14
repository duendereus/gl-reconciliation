# R. — AI-Powered GL Reconciliation

> Detect transaction breaks in seconds. Understand why. Know what to do next.

Built to demonstrate how AI can automate Finance Operations reconciliation workflows in fintech — replacing hours of manual GL review with real-time, auditable AI analysis.

---

## What it does

R. analyzes transaction datasets and automatically detects:

- **FX Rate discrepancies** — applied rate vs BANXICO official closing
- **Missing counterparties** — SWIFT transfers with no bank confirmation after 48h
- **Duplicate transactions** — same entity, amount, and timestamp
- **Interest calculation mismatches** — yield variance across savings accounts
- **AML flags, unauthorized reversals, fee mismatches, SPEI duplicates** (Dataset 2)

For each break, the AI provides a plain-English root cause, a specific recommended action, a confidence score, and a full step-by-step reasoning trace.

---

## Demo

The interactive demo is in `comp_files/widget.html`. Open it directly in any browser — no build step required.

**Login credentials (pre-filled):**
```
Username: nicolas
Password: ••••••••••
```

**What to explore:**
1. **Breaks tab** — click any row to see the AI analysis panel
2. **Traceability tab** — expand each accordion to see how the model reached its decision
3. **Data Explorer tab** — scatter plot shows break outliers vs normal transactions visually
4. **Dataset selector** — switch between Dataset 1 (200 tx, 4 breaks) and Dataset 2 (500 tx, 9 breaks)

---

## Project structure

```
r-reconciliation/
├── comp_files/
│   └── widget.html        ← Self-contained interactive demo
├── backend/
│   ├── main.py            ← FastAPI entry point
│   ├── routes/
│   │   ├── analyze.py     ← POST /analyze
│   │   └── datasets.py    ← GET /datasets
│   ├── services/
│   │   ├── reconciliation.py
│   │   └── claude_client.py
│   └── data/
│       ├── dataset_1.csv  ← 200 synthetic transactions
│       └── dataset_2.csv  ← 500 synthetic transactions
├── requirements.txt
├── .env.example
├── Dockerfile
├── CLAUDE.md              ← Full architecture notes for AI agents
└── README.md              ← This file
```

---

## Stack

- **Frontend** — Vanilla HTML/CSS/JS (self-contained, zero build step)
- **Backend** — FastAPI + Python
- **AI** — Claude API (`claude-sonnet-4-20250514`) via Anthropic
- **Data** — pandas for CSV processing and break detection
- **Deploy** — Railway or Render (free tier)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/duendereus/r-reconciliation
cd r-reconciliation
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
ANTHROPIC_API_KEY=your_anthropic_key_here
PORT=8000
ENV=development
```

### 4. Run the backend

```bash
uvicorn backend.main:app --reload --port 8000
```

### 5. Open the demo

Open `comp_files/widget.html` in your browser, or serve it statically:

```bash
python -m http.server 3000
# then open http://localhost:3000/comp_files/widget.html
```

---

## API endpoints

```
POST /analyze
  Body: { file: CSV }
  Returns: { breaks: [...], summary: {...} }

GET /datasets
  Returns: list of available pre-loaded datasets
```

---

## Synthetic datasets

Both datasets contain realistic fintech transaction structures:

| Field | Description |
|---|---|
| `txn_id` | Unique transaction identifier |
| `type` | FX_TRANSFER, SWIFT_INTERNATIONAL, CORPORATE_CARD, SAVINGS_ACCOUNT, SPEI |
| `amount_usd` | Transaction amount in USD |
| `amount_mxn` | Equivalent in MXN |
| `rate_applied` | FX rate used at processing time |
| `timestamp` | ISO 8601 datetime |
| `status` | COMPLETED, DISPATCHED, PENDING |
| `counterparty` | Destination bank or entity |
| `client_tier` | RETAIL, CORPORATE_T1, CORPORATE_T2 |

Breaks are seeded at specific rows to guarantee consistent demo output.

---

## Deploy to Railway

```bash
railway login
railway init
railway up
```

Set `ANTHROPIC_API_KEY` in Railway environment variables. The app will be live at a public URL in under 2 minutes.

---

## Related projects

- [business-rag](https://github.com/duendereus/business-rag) — RAG microservice for unstructured business documents
- [data-rag](https://github.com/duendereus/data-rag) — Text-to-SQL microservice for natural language data queries

---

Built by [@duendereus](https://github.com/duendereus)
