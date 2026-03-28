const currency = new Intl.NumberFormat("ko-KR");

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

function renderRows(items) {
  const tbody = document.getElementById("rows");
  tbody.innerHTML = items
    .map(
      (item) => `
        <tr>
          <td>${item.trade_date}</td>
          <td>${currency.format(item.close_price)}</td>
          <td>${currency.format(item.open_price)}</td>
          <td>${currency.format(item.high_price)}</td>
          <td>${currency.format(item.low_price)}</td>
          <td>${currency.format(item.volume)}</td>
          <td><span class="badge">${item.source}</span></td>
        </tr>
      `
    )
    .join("");
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
    setText(
      "change-rate",
      latest.price_change_rate == null
        ? "-"
        : `${latest.price_change_rate.toFixed(2)}%`
    );
    renderRows(payload.items);
    status.textContent = `Showing ${payload.items.length} latest rows from web-postgres.`;
  } catch (error) {
    status.textContent = `Failed to load data: ${error.message}`;
    status.classList.add("error");
  }
}

loadRows();
