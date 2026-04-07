---
title: sql-analytics-workspace
emoji: 🗄️
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 8000
pinned: false
---

# SQL Analytics Workspace

A multi-step OpenEnv environment where an AI agent acts as a data analyst. The agent explores a SQLite database — discovering tables, understanding schemas, spotting data quality issues — before writing SQL queries to answer business questions.

## Motivation

Data analysts don't write SQL from thin air. They explore the database first: checking what tables exist, understanding column types and relationships, sampling data to spot edge cases, and testing hypotheses before committing to a final query. This environment models that complete workflow, making it useful for training and evaluating LLM agents on structured data reasoning.

## How It Works

The agent goes through a multi-step exploration workflow:

1. **Explore** — discover what tables exist and how many rows each has
2. **Understand** — describe table columns, types, and foreign keys
3. **Inspect** — sample rows to spot data quality issues (NULLs, cancelled orders)
4. **Query** — run exploratory SQL to test hypotheses (read-only)
5. **Answer** — submit a final SQL query for grading

Each exploration step earns a small reward bonus. The final answer is graded against expected output. All rewards are strictly between 0 and 1 (exclusive).

## Action Space

| Field | Type | Description |
|-------|------|-------------|
| `action_type` | `str` | One of: `explore_tables`, `describe_table`, `sample_data`, `run_query`, `submit_answer` |
| `table_name` | `str` | Target table (for `describe_table` / `sample_data`) |
| `sql_query` | `str` | SQL query (for `run_query` / `submit_answer`) |
| `num_rows` | `int` | Sample rows to show, max 10 (for `sample_data`) |

### Action Details

| Action | What it does | Exploration Reward |
|--------|-------------|-------------------|
| `explore_tables` | Lists all tables with row counts | +0.05 (first time) |
| `describe_table` | Shows columns, types, FKs, and data quality warnings | +0.05 (first table) |
| `sample_data` | Shows first N rows of a table | +0.05 (first table) |
| `run_query` | Runs read-only exploratory SQL (DDL/DML blocked) | +0.02 (first 2 queries) |
| `submit_answer` | Submits final SQL for grading (up to 3 attempts) | Graded 0.01–0.99 |

## Observation Space

| Field | Type | Description |
|-------|------|-------------|
| `question` | `str` | Natural language business question |
| `tables_list` | `str` | Available tables with row counts (from `explore_tables`) |
| `table_description` | `str` | Column details, types, FKs (from `describe_table`) |
| `sample_rows` | `str` | Sample data rows (from `sample_data`) |
| `query_result` | `str` | Query output (from `run_query` / `submit_answer`) |
| `error_message` | `str` | Error from previous action, if any |
| `data_warnings` | `str` | Data quality warnings (NULLs, mixed order statuses) |
| `grader_explanation` | `str` | Feedback on submitted SQL answer |
| `phase` | `str` | Current phase: `exploration` or `submission` |
| `steps_remaining` | `int` | Steps left in episode |
| `submission_attempts_remaining` | `int` | `submit_answer` calls remaining |

## Tasks

### Task 1: Customer Lookup (Easy) — 12 steps, 3 submissions
- **Question:** Find premium customers who signed up after 2023, handle NULL emails with COALESCE
- **Skills:** WHERE, ORDER BY, COALESCE
- **Trap:** 2 customers have NULL emails — must show 'N/A' instead

### Task 2: Revenue Analysis (Medium) — 15 steps, 3 submissions
- **Question:** Total revenue per product category in Q4 2023
- **Skills:** 3-table JOIN, GROUP BY, HAVING, date filtering
- **Trap:** Cancelled/pending orders inflate revenue if not filtered by `status = 'completed'`

### Task 3: Customer Spending (Hard) — 15 steps, 3 submissions
- **Question:** Rank customers by total spend within their country
- **Skills:** CTE, HAVING, RANK() OVER PARTITION BY, percentage window function
- **Trap:** Cancelled orders corrupt spend totals; NULL emails should not exclude customers

### Task 4: Revenue Trends (Very Hard) — 20 steps, 3 submissions
- **Question:** Month-over-month revenue trends with growth classification
- **Skills:** Nested CTEs, LAG(), COALESCE, CASE WHEN (5 branches), strftime
- **Trap:** First month has no previous revenue — must handle NULL with COALESCE

## Reward Function

All rewards are strictly between 0 and 1 (never exactly 0.0 or 1.0).

**Two components:**

| Component | Range | Description |
|-----------|-------|-------------|
| Exploration bonus | 0 to 0.15 | Earned by exploring tables, sampling data, running queries |
| SQL grade | 0.01 to 0.84 | Grader score (0.01–0.99) scaled by 0.85 |

**Grader scoring ladder:**

| Grader Score | Meaning |
|:------------:|---------|
| 0.01 | SQL syntax error or runtime error |
| 0.10 | Valid SQL but empty result |
| 0.30 | Wrong number of columns |
| 0.50 | Correct columns but wrong values |
| 0.70 | Most rows correct, some missing or extra |
| 0.90 | Correct values but wrong row ordering |
| 0.99 | Exact match |

**Example combined rewards:**

| Scenario | Reward |
|----------|:------:|
| Explores + submits perfect SQL | 0.99 |
| Skips exploration + submits perfect SQL | 0.84 |
| Explores + submits partially correct SQL | ~0.58 |
| Submits SQL with syntax error (no exploration) | 0.01 |

## Data Quality Traps

The database contains intentional traps that test whether agents explore carefully:

| Trap | Impact if missed |
|------|-----------------|
| 2 customers have NULL emails | Wrong output if not handled with COALESCE |
| 4 orders are cancelled/pending | $8,700 in fake revenue inflates totals |
| `products.price` ≠ `order_items.unit_price` | Wrong revenue if using list price instead of sale price |

Agents that skip exploration and don't filter by `status = 'completed'` will get penalized.

## Database Schema

```
TABLE: customers (12 rows)
  - id INTEGER (PK)
  - name TEXT
  - email TEXT (nullable — 2 rows have NULL)
  - signup_date DATE
  - tier TEXT (basic / premium / enterprise)
  - country TEXT

TABLE: products (10 rows)
  - id INTEGER (PK)
  - name TEXT
  - category TEXT (Electronics / Furniture / Books)
  - price REAL (list price — may differ from sale price)

TABLE: orders (24 rows — includes 2 cancelled + 2 pending)
  - id INTEGER (PK)
  - customer_id INTEGER (FK → customers)
  - order_date DATE
  - status TEXT (completed / pending / cancelled)

TABLE: order_items (42 rows — includes items for non-completed orders)
  - id INTEGER (PK)
  - order_id INTEGER (FK → orders)
  - product_id INTEGER (FK → products)
  - quantity INTEGER
  - unit_price REAL (actual charged price)
```

## Setup and Usage

### Prerequisites
- Python 3.10, 3.11, or 3.12
- Docker (optional, for containerized testing)

### Install and run locally
```bash
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

### Run with Docker
```bash
docker build -f server/Dockerfile -t sql-analytics-workspace .
docker run -p 8000:8000 sql-analytics-workspace
```

### Run inference
```bash
export API_BASE_URL="https://router.huggingface.co/v1"
export MODEL_NAME="Qwen/Qwen2.5-72B-Instruct"
export HF_TOKEN="your-hf-token"
export ENV_URL="http://localhost:8000"
python inference.py
```

## Baseline Scores

Tested with three models. The agent explores the schema, then generates SQL (no data warnings or sample data given to the LLM — it must reason from schema alone).

| Task | Difficulty | Qwen 2.5 7B | Llama 3.1 70B | Qwen 2.5 72B |
|------|-----------|:----------:|:------------:|:-----------:|
| Customer Lookup | Easy | 0.95 | 0.95 | 0.95 |
| Revenue Analysis | Medium | 0.53 | 0.95 | 0.95 |
| Customer Spending | Hard | 0.35 | 0.95 | 0.95 |
| Revenue Trends | Very Hard | 0.35 | 0.69 | 0.35 |
| **Average** | | **0.55** | **0.89** | **0.80** |

**Observations:**
- Easy task: all models solve it — simple filtering with COALESCE
- Medium: 7B model falls for the dirty data trap (includes cancelled orders)
- Hard: 7B can't handle CTE + window functions; larger models succeed
- Very Hard: even Llama 70B only gets 0.69 — nested CTEs + LAG + CASE WHEN is genuinely difficult
- Clear progression: 7B (0.55) < 72B (0.80) < 70B (0.89)
