import os
from datetime import date, datetime

import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from psycopg2.extras import RealDictCursor


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Stock Signal</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4efe6;
        --panel: #fffdf8;
        --line: #d8cdbd;
        --text: #1d1a16;
        --muted: #73685b;
        --accent: #0f766e;
        --accent-soft: #d9f1ee;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        font-family: "Georgia", "Times New Roman", serif;
        background:
          radial-gradient(circle at top left, #efe3d1 0, transparent 28%),
          linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%);
        color: var(--text);
      }

      main {
        max-width: 920px;
        margin: 0 auto;
        padding: 48px 20px 80px;
      }

      .hero {
        margin-bottom: 28px;
      }

      .eyebrow {
        margin: 0 0 12px;
        font-size: 12px;
        letter-spacing: 0.24em;
        text-transform: uppercase;
        color: var(--muted);
      }

      h1 {
        margin: 0;
        font-size: clamp(36px, 7vw, 64px);
        line-height: 0.95;
      }

      .subtitle {
        margin: 14px 0 0;
        max-width: 560px;
        color: var(--muted);
        line-height: 1.6;
      }

      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 24px;
        box-shadow: 0 18px 50px rgba(50, 38, 18, 0.08);
      }

      .summary {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 16px;
        margin-bottom: 22px;
      }

      .card {
        padding: 18px;
        border-radius: 18px;
        background: #faf6ef;
        border: 1px solid #eadfce;
      }

      .label {
        margin: 0 0 8px;
        font-size: 12px;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: var(--muted);
      }

      .value {
        margin: 0;
        font-size: 28px;
      }

      .value.small {
        font-size: 18px;
      }

      .table-wrap {
        overflow-x: auto;
      }

      table {
        width: 100%;
        border-collapse: collapse;
      }

      th,
      td {
        padding: 14px 12px;
        border-bottom: 1px solid #ede4d7;
        text-align: left;
        white-space: nowrap;
      }

      th {
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
      }

      .status {
        margin: 0 0 16px;
        color: var(--muted);
      }

      .badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
      }

      .error {
        color: #b42318;
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <p class="eyebrow">Airflow Portfolio</p>
        <h1>Samsung Electronics<br />Daily Close</h1>
        <p class="subtitle">
          This page reads the mart table written by Airflow and shows the latest
          daily stock price records from web-postgres.
        </p>
      </section>

      <section class="panel">
        <p id="status" class="status">Loading latest mart rows...</p>

        <div class="summary">
          <article class="card">
            <p class="label">Stock</p>
            <p id="stock-name" class="value small">-</p>
          </article>
          <article class="card">
            <p class="label">Latest Close</p>
            <p id="close-price" class="value">-</p>
          </article>
          <article class="card">
            <p class="label">Trade Date</p>
            <p id="trade-date" class="value small">-</p>
          </article>
          <article class="card">
            <p class="label">Change Rate</p>
            <p id="change-rate" class="value small">-</p>
          </article>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Trade Date</th>
                <th>Close</th>
                <th>Open</th>
                <th>High</th>
                <th>Low</th>
                <th>Volume</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
      </section>
    </main>

    <script>
      const currency = new Intl.NumberFormat("ko-KR");

      function setText(id, value) {
        document.getElementById(id).textContent = value;
      }

      function renderRows(items) {
        const tbody = document.getElementById("rows");
        tbody.innerHTML = items.map((item) => `
          <tr>
            <td>${item.trade_date}</td>
            <td>${currency.format(item.close_price)}</td>
            <td>${currency.format(item.open_price)}</td>
            <td>${currency.format(item.high_price)}</td>
            <td>${currency.format(item.low_price)}</td>
            <td>${currency.format(item.volume)}</td>
            <td><span class="badge">${item.source}</span></td>
          </tr>
        `).join("");
      }

      async function loadRows() {
        const status = document.getElementById("status");

        try {
          const response = await fetch("/api/stock-prices");
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const payload = await response.json();
          if (!payload.items.length) {
            status.textContent = "No mart data has been loaded yet.";
            return;
          }

          const latest = payload.items[0];
          setText("stock-name", `${latest.stock_name} (${latest.stock_code})`);
          setText("close-price", currency.format(latest.close_price));
          setText("trade-date", latest.trade_date);
          setText("change-rate", latest.price_change_rate == null ? "-" : `${latest.price_change_rate.toFixed(2)}%`);
          renderRows(payload.items);
          status.textContent = `Showing ${payload.items.length} latest rows from web-postgres.`;
        } catch (error) {
          status.textContent = `Failed to load data: ${error.message}`;
          status.classList.add("error");
        }
      }

      loadRows();
    </script>
  </body>
</html>
"""


app = FastAPI()


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WEB_POSTGRES_HOST", "web-postgres"),
        port=os.environ.get("WEB_POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "stock_signal"),
        user=os.environ.get("POSTGRES_USER", "stock_signal"),
        password=os.environ.get("POSTGRES_PASSWORD", "stock_signal_local_pg_password"),
    )


def serialize_row(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def fetch_stock_prices():
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT
                    stock_code,
                    stock_name,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    price_change_rate,
                    source,
                    bronze_path,
                    silver_path,
                    collected_at,
                    loaded_at
                FROM stock_price_daily_mart
                ORDER BY trade_date DESC
                LIMIT 10
                """
            )
            return [serialize_row(row) for row in cursor.fetchall()]


@app.get("/", response_class=HTMLResponse)
def read_index():
    return HTMLResponse(content=HTML_PAGE)


@app.get("/api/stock-prices")
def read_stock_prices():
    return JSONResponse(content={"items": fetch_stock_prices()})


@app.get("/health")
def read_health():
    return {"status": "ok"}
