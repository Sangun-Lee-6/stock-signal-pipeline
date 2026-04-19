document.addEventListener("DOMContentLoaded", async () => {
  const stockList = document.getElementById("stock-list");
  const stockTitle = document.getElementById("stock-title");
  const statPrice = document.getElementById("stat-price");
  const chart = document.getElementById("chart");
  const chartEmpty = document.getElementById("chart-empty");
  const eventList = document.getElementById("event-list");
  const numberFormatter = new Intl.NumberFormat("ko-KR");

  let payload = { items: [] };
  try {
    const response = await fetch("/api/stock-prices");
    payload = await response.json();
  } catch (error) {
    payload = { items: [] };
  }

  const items = Array.isArray(payload.items) ? payload.items : [];
  const fallbackItems = [
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-14", close_price: 68400 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-15", close_price: 71200 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-16", close_price: 70300 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-17", close_price: 74200 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-18", close_price: 77600 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-19", close_price: 69100 },
    { stock_code: "108320", stock_name: "LX 세미콘", trade_date: "2026-04-20", close_price: 73100 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-14", close_price: 173000 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-15", close_price: 176500 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-16", close_price: 171400 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-17", close_price: 182600 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-18", close_price: 178300 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-19", close_price: 168900 },
    { stock_code: "058470", stock_name: "리노공업", trade_date: "2026-04-20", close_price: 181100 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-14", close_price: 151400 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-15", close_price: 154200 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-16", close_price: 149700 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-17", close_price: 157100 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-18", close_price: 164300 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-19", close_price: 152600 },
    { stock_code: "039030", stock_name: "이오테크닉스", trade_date: "2026-04-20", close_price: 166400 }
  ];

  const sourceItems = items.length ? items : fallbackItems;
  const groupedStocks = {};

  for (const item of sourceItems) {
    const stockKey = item.stock_code || item.stock_name || "unknown";
    if (!groupedStocks[stockKey]) {
      groupedStocks[stockKey] = {
        stockCode: item.stock_code || stockKey,
        stockName: item.stock_name || stockKey,
        rows: []
      };
    }
    groupedStocks[stockKey].rows.push({
      tradeDate: String(item.trade_date).slice(0, 10),
      closePrice: Number(item.close_price || 0)
    });
  }

  const stockKeys = Object.keys(groupedStocks);
  let selectedKey = stockKeys[0] || null;

  const drawChart = (rows) => {
    if (!rows.length) {
      chart.innerHTML = "";
      chart.style.display = "none";
      chartEmpty.style.display = "flex";
      return [];
    }

    chart.style.display = "block";
    chartEmpty.style.display = "none";
    rows.sort((left, right) => left.tradeDate.localeCompare(right.tradeDate));
    const width = 920;
    const height = 520;
    const leftPad = 54;
    const rightPad = 32;
    const topPad = 24;
    const bottomPad = 64;
    const plotWidth = width - leftPad - rightPad;
    const plotHeight = height - topPad - bottomPad;
    let minValue = rows[0].closePrice;
    let maxValue = rows[0].closePrice;

    for (const row of rows) {
      minValue = Math.min(minValue, row.closePrice);
      maxValue = Math.max(maxValue, row.closePrice);
    }

    const valueGap = Math.max(maxValue - minValue, 1);
    const points = [];
    let pathData = "";

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const x = leftPad + (plotWidth * index) / Math.max(rows.length - 1, 1);
      const y = topPad + ((maxValue - row.closePrice) * plotHeight) / valueGap;
      points.push({ ...row, x, y });
      pathData += index === 0 ? `M ${x} ${y}` : ` L ${x} ${y}`;
    }

    const markerIndexes = [];
    if (points.length > 2) markerIndexes.push(1);
    if (points.length > 4) markerIndexes.push(Math.floor(points.length / 2));
    if (points.length > 3) markerIndexes.push(points.length - 2);

    let markerSvg = "";
    let labelSvg = "";
    const uniqueMarkers = Array.from(new Set(markerIndexes));

    for (const index of uniqueMarkers) {
      const point = points[index];
      markerSvg += `<line x1="${point.x}" y1="${point.y + 10}" x2="${point.x}" y2="${height - bottomPad + 2}" stroke="rgba(255,255,255,0.36)" stroke-dasharray="4 8" />`;
      markerSvg += `<circle cx="${point.x}" cy="${point.y}" r="8" fill="rgba(49,130,246,0.2)" stroke="#dce9ff" stroke-width="3" />`;
    }

    const labelIndexes = [0, Math.floor(points.length / 2), points.length - 1];
    for (const index of Array.from(new Set(labelIndexes))) {
      const point = points[index];
      const label = point.tradeDate.slice(5).replace("-", "/");
      labelSvg += `<text x="${point.x}" y="${height - 22}" fill="rgba(255,255,255,0.72)" font-size="18" text-anchor="middle">${label}</text>`;
    }

    chart.innerHTML = `
      <line x1="${leftPad}" y1="${height - bottomPad}" x2="${width - rightPad}" y2="${height - bottomPad}" stroke="rgba(255,255,255,0.44)" stroke-width="2" />
      <line x1="${leftPad}" y1="${height - bottomPad}" x2="${leftPad}" y2="${topPad}" stroke="rgba(255,255,255,0.44)" stroke-width="2" />
      <path d="${pathData}" fill="none" stroke="url(#price-gradient)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round" />
      ${markerSvg}
      ${labelSvg}
      <defs>
        <linearGradient id="price-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#d9e6ff" />
          <stop offset="100%" stop-color="#ffffff" />
        </linearGradient>
      </defs>
      <text x="${leftPad - 8}" y="${topPad + 8}" fill="rgba(255,255,255,0.86)" font-size="18" text-anchor="end">price</text>
      <text x="${width - rightPad}" y="${height - bottomPad + 40}" fill="rgba(255,255,255,0.86)" font-size="18" text-anchor="end">time</text>
    `;

    return uniqueMarkers.map((index, order) => ({
      title: `이벤트 ${order + 1}`,
      body: `${points[index].tradeDate} 가격 ${numberFormatter.format(points[index].closePrice)}원 구간`,
      note: order === 0 ? "뉴스/공시 연결 전 샘플 포인트" : "가격 반응 확인용 포인트"
    }));
  };

  const render = async () => {
    if (!selectedKey) {
      stockList.innerHTML = "";
      stockTitle.textContent = "표시할 종목이 없습니다";
      statPrice.textContent = "-";
      eventList.innerHTML = '<div class="event-placeholder">데이터가 아직 적재되지 않았습니다.</div>';
      drawChart([]);
      return;
    }

    stockList.innerHTML = "";
    for (const stockKey of stockKeys) {
      const button = document.createElement("button");
      button.className = `stock-button${stockKey === selectedKey ? " is-active" : ""}`;
      button.textContent = groupedStocks[stockKey].stockName;
      button.onclick = () => {
        selectedKey = stockKey;
        render();
      };
      stockList.appendChild(button);
    }

    const selectedStock = groupedStocks[selectedKey];
    const rows = selectedStock.rows.slice();
    const latestRow = rows[rows.length - 1];
    const fallbackEvents = drawChart(rows);
    const requestKey = selectedKey;

    stockTitle.textContent = selectedStock.stockName;
    statPrice.textContent = latestRow ? `${numberFormatter.format(latestRow.closePrice)}원` : "-";
    eventList.innerHTML = '<div class="event-placeholder">이벤트를 불러오는 중입니다.</div>';

    let events = [];
    try {
      const response = await fetch(`/api/stock-events?stock_code=${encodeURIComponent(selectedStock.stockCode)}`);
      const payload = await response.json();
      events = Array.isArray(payload.items) ? payload.items : [];
    } catch (error) {
      events = [];
    }

    if (requestKey !== selectedKey) {
      return;
    }

    eventList.innerHTML = "";

    if (!events.length && !fallbackEvents.length) {
      eventList.innerHTML = '<div class="event-placeholder">표시할 이벤트가 아직 없습니다.</div>';
      return;
    }

    const renderedEvents = events.length
      ? events.map((event, index) => ({
          kicker: `${event.event_source_name || event.source || "event"} · ${event.event_scope || "stock"}`,
          title: event.event_title || "제목 없음",
          meta: event.event_date || event.event_at || `이벤트 ${index + 1}`
        }))
      : fallbackEvents.map((event) => ({
          kicker: event.title,
          title: event.body,
          meta: event.note
        }));

    for (const event of renderedEvents) {
      const card = document.createElement("article");
      card.className = "event-card";
      const kicker = document.createElement("div");
      const title = document.createElement("div");
      const meta = document.createElement("div");

      kicker.className = "event-kicker";
      title.className = "event-title";
      meta.className = "event-meta";
      kicker.textContent = event.kicker;
      title.textContent = event.title;
      meta.textContent = event.meta;

      card.appendChild(kicker);
      card.appendChild(title);
      card.appendChild(meta);
      eventList.appendChild(card);
    }
  };

  await render();
});
