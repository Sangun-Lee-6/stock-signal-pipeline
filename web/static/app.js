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
    const leftPad = 96;
    const rightPad = 34;
    const topPad = 34;
    const bottomPad = 82;
    const plotWidth = width - leftPad - rightPad;
    const plotHeight = height - topPad - bottomPad;
    const baselineY = height - bottomPad;
    let minValue = rows[0].closePrice;
    let maxValue = rows[0].closePrice;

    for (const row of rows) {
      minValue = Math.min(minValue, row.closePrice);
      maxValue = Math.max(maxValue, row.closePrice);
    }

    const valueGap = Math.max(maxValue - minValue, 1);
    const valuePadding = Math.max(valueGap * 0.18, maxValue * 0.03, 1);
    const domainMin = Math.max(minValue - valuePadding, 0);
    const domainMax = maxValue + valuePadding;
    const domainGap = Math.max(domainMax - domainMin, 1);
    const points = [];
    let linePath = "";

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const x = leftPad + (plotWidth * index) / Math.max(rows.length - 1, 1);
      const y = topPad + ((domainMax - row.closePrice) * plotHeight) / domainGap;
      points.push({ ...row, x, y });
      linePath += index === 0 ? `M ${x} ${y}` : ` L ${x} ${y}`;
    }

    const areaPath = `${linePath} L ${points[points.length - 1].x} ${baselineY} L ${points[0].x} ${baselineY} Z`;
    const firstPoint = points[0];
    const latestPoint = points[points.length - 1];
    const changeValue = latestPoint.closePrice - firstPoint.closePrice;
    const trendColor = changeValue >= 0 ? "#ff8a8a" : "#7fb2ff";
    const trendFill = changeValue >= 0 ? "rgba(255,138,138,0.14)" : "rgba(127,178,255,0.16)";
    let maxIndex = 0;
    let minIndex = 0;

    for (let index = 1; index < points.length; index += 1) {
      if (points[index].closePrice > points[maxIndex].closePrice) {
        maxIndex = index;
      }
      if (points[index].closePrice < points[minIndex].closePrice) {
        minIndex = index;
      }
    }

    let gridSvg = "";
    for (let step = 0; step <= 4; step += 1) {
      const ratio = step / 4;
      const y = topPad + plotHeight * ratio;
      gridSvg += `<line x1="${leftPad}" y1="${y}" x2="${width - rightPad}" y2="${y}" stroke="rgba(255,255,255,${step === 4 ? "0.24" : "0.1"})" stroke-width="${step === 4 ? "1.6" : "1"}" />`;
    }

    let markerSvg = "";
    const uniqueMarkers = Array.from(new Set([minIndex, maxIndex, points.length - 1])).sort((left, right) => left - right);

    for (const index of uniqueMarkers) {
      const point = points[index];
      const isLatest = index === points.length - 1;
      markerSvg += `<line x1="${point.x}" y1="${point.y + 12}" x2="${point.x}" y2="${baselineY}" stroke="rgba(255,255,255,0.2)" stroke-dasharray="4 8" />`;
      markerSvg += `<circle cx="${point.x}" cy="${point.y}" r="${isLatest ? "9" : "7"}" fill="${isLatest ? trendFill : "rgba(49,130,246,0.2)"}" stroke="${isLatest ? trendColor : "#dce9ff"}" stroke-width="${isLatest ? "4" : "3"}" />`;
      markerSvg += `<circle cx="${point.x}" cy="${point.y}" r="3.5" fill="${isLatest ? trendColor : "#ffffff"}" />`;
    }

    chart.innerHTML = `
      <defs>
        <linearGradient id="chart-surface-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="#ffffff" stop-opacity="0.035" />
          <stop offset="100%" stop-color="#ffffff" stop-opacity="0" />
        </linearGradient>
        <linearGradient id="chart-area-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="#7fb2ff" stop-opacity="0.28" />
          <stop offset="100%" stop-color="#7fb2ff" stop-opacity="0.02" />
        </linearGradient>
        <linearGradient id="price-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#9cc4ff" />
          <stop offset="100%" stop-color="#ffffff" />
        </linearGradient>
        <filter id="line-glow" x="-10%" y="-10%" width="120%" height="120%">
          <feGaussianBlur stdDeviation="8" result="blurred" />
          <feMerge>
            <feMergeNode in="blurred" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>
      <rect x="${leftPad}" y="${topPad}" width="${plotWidth}" height="${plotHeight}" rx="28" fill="url(#chart-surface-gradient)" stroke="rgba(255,255,255,0.04)" />
      ${gridSvg}
      <line x1="${leftPad}" y1="${baselineY}" x2="${width - rightPad}" y2="${baselineY}" stroke="rgba(255,255,255,0.28)" stroke-width="1.6" />
      <path d="${areaPath}" fill="url(#chart-area-gradient)" />
      <line x1="${latestPoint.x}" y1="${latestPoint.y}" x2="${width - rightPad}" y2="${latestPoint.y}" stroke="rgba(255,255,255,0.2)" stroke-dasharray="4 8" />
      ${markerSvg}
      <path d="${linePath}" fill="none" stroke="rgba(140,185,255,0.28)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round" filter="url(#line-glow)" />
      <path d="${linePath}" fill="none" stroke="url(#price-gradient)" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" />
    `;

    return uniqueMarkers.map((index, order) => ({
      title: `이벤트 ${order + 1}`,
      body: `${points[index].tradeDate} 가격 ${numberFormatter.format(points[index].closePrice)}원 구간`,
      note: index === maxIndex ? "고점 확인 포인트" : index === minIndex ? "저점 확인 포인트" : "최근 가격 확인 포인트"
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
    const requestKey = selectedKey;

    stockTitle.textContent = selectedStock.stockName;
    statPrice.textContent = "-";
    eventList.innerHTML = '<div class="event-placeholder">이벤트를 불러오는 중입니다.</div>';

    let stockPricePayload = { stocks: [], items: [] };
    try {
      const response = await fetch(`/api/stock-prices?stock_code=${encodeURIComponent(selectedStock.stockCode)}`);
      stockPricePayload = await response.json();
    } catch (error) {
      stockPricePayload = { stocks: [], items: [] };
    }

    if (requestKey !== selectedKey) {
      return;
    }

    const stockItems = Array.isArray(stockPricePayload.items) ? stockPricePayload.items : [];
    const rows = stockItems.length
      ? stockItems.map((item) => ({
          tradeDate: String(item.trade_date).slice(0, 10),
          closePrice: Number(item.close_price || 0)
        }))
      : selectedStock.rows.slice();
    const stockMeta = Array.isArray(stockPricePayload.stocks)
      ? stockPricePayload.stocks.find((item) => item.stock_code === selectedStock.stockCode)
      : null;
    const fallbackEvents = drawChart(rows);
    const latestRow = rows[rows.length - 1];
    const latestClosePrice = stockMeta && stockMeta.last_close_price !== undefined && stockMeta.last_close_price !== null
      ? Number(stockMeta.last_close_price)
      : latestRow
        ? latestRow.closePrice
        : null;

    statPrice.textContent = latestClosePrice !== null && !Number.isNaN(latestClosePrice)
      ? `${numberFormatter.format(latestClosePrice)}원`
      : "-";

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
