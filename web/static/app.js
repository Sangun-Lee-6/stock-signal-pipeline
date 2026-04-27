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
  let selectedRange = payload.selected_range || "1m";
  let selectedEventScope = "시장전체";

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

    let selectedPointIndex = points.length - 1;
    const highlightColor = "#ffd166";
    const renderSelection = () => {
      const selectedPoint = points[selectedPointIndex];
      const labelWidth = 176;
      const labelHeight = 56;
      const labelX = Math.min(
        Math.max(selectedPoint.x - labelWidth / 2, leftPad + 14),
        width - rightPad - labelWidth - 14
      );
      const labelY = Math.max(topPad + 16, selectedPoint.y - 84);
      const selectablePointSvg = points.map((point, index) => `
        <circle
          data-point-index="${index}"
          cx="${point.x}"
          cy="${point.y}"
          r="14"
          fill="transparent"
          tabindex="0"
          role="button"
          aria-label="${point.tradeDate} 종가 ${numberFormatter.format(point.closePrice)}원"
          style="cursor:pointer"
        />
      `).join("");

      statPrice.textContent = `${numberFormatter.format(selectedPoint.closePrice)}원`;
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
        <line x1="${selectedPoint.x}" y1="${selectedPoint.y}" x2="${width - rightPad}" y2="${selectedPoint.y}" stroke="rgba(255,255,255,0.22)" stroke-dasharray="4 8" />
        <line x1="${selectedPoint.x}" y1="${selectedPoint.y + 14}" x2="${selectedPoint.x}" y2="${baselineY}" stroke="rgba(255,209,102,0.34)" stroke-dasharray="4 8" />
        ${markerSvg}
        <path d="${linePath}" fill="none" stroke="rgba(140,185,255,0.28)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round" filter="url(#line-glow)" />
        <path d="${linePath}" fill="none" stroke="url(#price-gradient)" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" />
        <circle cx="${selectedPoint.x}" cy="${selectedPoint.y}" r="15" fill="rgba(255,209,102,0.16)" stroke="${highlightColor}" stroke-width="2" />
        <circle cx="${selectedPoint.x}" cy="${selectedPoint.y}" r="6" fill="${highlightColor}" />
        <rect x="${labelX}" y="${labelY}" width="${labelWidth}" height="${labelHeight}" rx="16" fill="rgba(11,13,18,0.92)" stroke="rgba(255,209,102,0.36)" />
        <text x="${labelX + 16}" y="${labelY + 22}" fill="rgba(255,255,255,0.72)" font-size="12" font-weight="700">${selectedPoint.tradeDate}</text>
        <text x="${labelX + 16}" y="${labelY + 42}" fill="#f4f6fb" font-size="18" font-weight="800">${numberFormatter.format(selectedPoint.closePrice)}원</text>
        ${selectablePointSvg}
      `;

      for (const pointElement of chart.querySelectorAll("[data-point-index]")) {
        pointElement.addEventListener("click", () => {
          selectedPointIndex = Number(pointElement.getAttribute("data-point-index"));
          renderSelection();
        });
        pointElement.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            selectedPointIndex = Number(pointElement.getAttribute("data-point-index"));
            renderSelection();
          }
        });
      }
    };

    renderSelection();

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

    const chartCard = chart.closest(".chart-card");
    const chartWrap = chart.parentElement;
    let rangeControls = chartCard.querySelector('[data-role="price-range-controls"]');
    const rangeOptions = [
      { value: "1d", label: "최근 1일" },
      { value: "5d", label: "최근 5일" },
      { value: "1m", label: "최근 1개월" },
      { value: "6m", label: "최근 6개월" }
    ];

    if (!rangeControls) {
      rangeControls = document.createElement("div");
      rangeControls.dataset.role = "price-range-controls";
      rangeControls.style.display = "flex";
      rangeControls.style.flexWrap = "wrap";
      rangeControls.style.gap = "8px";
      rangeControls.style.margin = "0 0 16px";
      chartCard.insertBefore(rangeControls, chartWrap);
    }

    rangeControls.innerHTML = "";
    for (const option of rangeOptions) {
      const button = document.createElement("button");
      const isActive = option.value === selectedRange;
      button.type = "button";
      button.textContent = option.label;
      button.style.border = isActive ? "1px solid rgba(49, 130, 246, 0.45)" : "1px solid rgba(255, 255, 255, 0.08)";
      button.style.background = isActive ? "linear-gradient(180deg, rgba(49, 130, 246, 0.22), rgba(49, 130, 246, 0.08))" : "rgba(255, 255, 255, 0.03)";
      button.style.color = "#f4f6fb";
      button.style.padding = "10px 14px";
      button.style.borderRadius = "999px";
      button.style.font = "inherit";
      button.style.fontSize = "13px";
      button.style.fontWeight = "700";
      button.style.cursor = "pointer";
      button.style.transition = "160ms ease";
      button.onclick = () => {
        if (selectedRange === option.value) {
          return;
        }
        selectedRange = option.value;
        render();
      };
      rangeControls.appendChild(button);
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
    const requestKey = `${selectedKey}:${selectedRange}`;

    stockTitle.textContent = selectedStock.stockName;
    statPrice.textContent = "-";
    eventList.innerHTML = '<div class="event-placeholder">이벤트를 불러오는 중입니다.</div>';

    let stockPricePayload = { stocks: [], items: [] };
    try {
      const response = await fetch(`/api/stock-prices?stock_code=${encodeURIComponent(selectedStock.stockCode)}&range=${encodeURIComponent(selectedRange)}`);
      stockPricePayload = await response.json();
    } catch (error) {
      stockPricePayload = { stocks: [], items: [] };
    }

    if (requestKey !== `${selectedKey}:${selectedRange}`) {
      return;
    }

    const stockItems = Array.isArray(stockPricePayload.items) ? stockPricePayload.items : [];
    const fallbackRows = selectedRange === "1d"
      ? selectedStock.rows.slice(-1)
      : selectedRange === "5d"
        ? selectedStock.rows.slice(-5)
        : selectedStock.rows.slice();
    const rows = stockItems.length
      ? stockItems.map((item) => ({
          tradeDate: String(item.trade_date).slice(0, 10),
          closePrice: Number(item.close_price || 0)
        }))
      : fallbackRows;
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

    if (requestKey !== `${selectedKey}:${selectedRange}`) {
      return;
    }

    eventList.innerHTML = "";

    if (!events.length && !fallbackEvents.length) {
      eventList.innerHTML = '<div class="event-placeholder">표시할 이벤트가 아직 없습니다.</div>';
      return;
    }

    const renderedEvents = events.length
      ? events.map((event, index) => ({
          scope: event.impact_scope || "미분류",
          kicker: event.event_source_name || event.source || "event",
          title: event.standardized_title || event.event_title || "제목 없음",
          meta: event.event_date || event.event_at || `이벤트 ${index + 1}`,
          direction: event.impact_direction,
          driver: event.driver_category,
          entities: event.matched_entities,
          evidence: event.scope_evidence || event.driver_evidence || event.direction_evidence,
          url: event.event_url
        }))
      : fallbackEvents.map((event) => ({
          scope: "가격",
          kicker: event.title,
          title: event.body,
          meta: event.note,
          direction: null,
          driver: null,
          entities: null,
          evidence: null,
          url: null
        }));

    const scopeOrder = ["시장전체", "섹터", "기업", "미분류", "가격"];
    const scopeCopy = {
      "시장전체": "금리, 환율, 정책처럼 시장 전반을 움직일 수 있는 기사",
      "섹터": "특정 업종이나 테마에 집중된 기사",
      "기업": "개별 기업 영향이 큰 기사",
      "미분류": "분류 근거가 아직 충분하지 않은 기사",
      "가격": "차트에서 자동 생성한 가격 확인 포인트"
    };
    const eventPanel = eventList.closest(".events");
    let scopeControls = eventPanel.querySelector('[data-role="event-scope-controls"]');

    if (!scopeControls) {
      scopeControls = document.createElement("div");
      scopeControls.dataset.role = "event-scope-controls";
      scopeControls.style.display = "grid";
      scopeControls.style.gridTemplateColumns = "repeat(2, minmax(0, 1fr))";
      scopeControls.style.gap = "8px";
      scopeControls.style.margin = "0 0 16px";
      eventPanel.insertBefore(scopeControls, eventList);
    }

    scopeControls.innerHTML = "";
    for (const scope of scopeOrder.filter((value) => value !== "가격")) {
      const count = renderedEvents.filter((event) => event.scope === scope).length;
      const button = document.createElement("button");
      const isActive = scope === selectedEventScope;

      button.type = "button";
      button.textContent = `${scope} ${count}`;
      button.style.border = isActive ? "1px solid rgba(127, 178, 255, 0.45)" : "1px solid rgba(255, 255, 255, 0.08)";
      button.style.background = isActive ? "linear-gradient(180deg, rgba(127, 178, 255, 0.2), rgba(127, 178, 255, 0.08))" : "rgba(255, 255, 255, 0.03)";
      button.style.color = "#f4f6fb";
      button.style.padding = "10px 12px";
      button.style.borderRadius = "12px";
      button.style.font = "inherit";
      button.style.fontSize = "13px";
      button.style.fontWeight = "800";
      button.style.cursor = "pointer";
      button.onclick = () => {
        if (selectedEventScope === scope) {
          return;
        }
        selectedEventScope = scope;
        render();
      };
      scopeControls.appendChild(button);
    }

    for (const scope of scopeOrder) {
      if (scope !== selectedEventScope && !(scope === "가격" && !events.length)) {
        continue;
      }

      const scopedEvents = renderedEvents.filter((event) => event.scope === scope);

      if (!scopedEvents.length) {
        continue;
      }

      const section = document.createElement("section");
      const heading = document.createElement("div");
      const headingTitle = document.createElement("div");
      const headingMeta = document.createElement("div");

      section.style.display = "flex";
      section.style.flexDirection = "column";
      section.style.gap = "10px";
      heading.style.display = "flex";
      heading.style.justifyContent = "space-between";
      heading.style.gap = "12px";
      heading.style.alignItems = "baseline";
      heading.style.margin = "10px 2px 2px";
      headingTitle.className = "event-kicker";
      headingTitle.textContent = `${scope} ${scopedEvents.length}`;
      headingMeta.className = "event-meta";
      headingMeta.style.fontSize = "12px";
      headingMeta.style.textAlign = "right";
      headingMeta.textContent = scopeCopy[scope];

      heading.appendChild(headingTitle);
      heading.appendChild(headingMeta);
      section.appendChild(heading);

      for (const event of scopedEvents) {
        const card = document.createElement("article");
        card.className = "event-card";
        const topLine = document.createElement("div");
        const badge = document.createElement("span");
        const kicker = document.createElement("span");
        const title = event.url ? document.createElement("a") : document.createElement("div");
        const meta = document.createElement("div");
        const chips = document.createElement("div");
        const evidence = document.createElement("div");

        topLine.style.display = "flex";
        topLine.style.alignItems = "center";
        topLine.style.gap = "8px";
        topLine.style.flexWrap = "wrap";
        badge.textContent = event.scope;
        badge.style.padding = "4px 8px";
        badge.style.border = "1px solid rgba(127, 178, 255, 0.24)";
        badge.style.borderRadius = "999px";
        badge.style.background = "rgba(127, 178, 255, 0.1)";
        badge.style.color = "#dce9ff";
        badge.style.fontSize = "11px";
        badge.style.fontWeight = "800";
        kicker.className = "event-kicker";
        kicker.textContent = event.kicker;
        title.className = "event-title";
        title.textContent = event.title;
        title.style.display = "block";
        title.style.color = "#f4f6fb";
        title.style.textDecoration = "none";
        meta.className = "event-meta";
        meta.textContent = event.meta;
        chips.style.display = "flex";
        chips.style.flexWrap = "wrap";
        chips.style.gap = "6px";
        chips.style.marginTop = "10px";

        if (event.url) {
          title.href = event.url;
          title.target = "_blank";
          title.rel = "noreferrer";
        }

        for (const chipText of [event.direction, event.driver, event.entities].filter(Boolean)) {
          const chip = document.createElement("span");
          chip.textContent = chipText;
          chip.style.padding = "6px 8px";
          chip.style.borderRadius = "8px";
          chip.style.background = "rgba(255, 255, 255, 0.04)";
          chip.style.color = "#c7d0dd";
          chip.style.fontSize = "12px";
          chip.style.fontWeight = "700";
          chips.appendChild(chip);
        }

        topLine.appendChild(badge);
        topLine.appendChild(kicker);
        card.appendChild(topLine);
        card.appendChild(title);
        card.appendChild(meta);

        if (chips.children.length) {
          card.appendChild(chips);
        }

        if (event.evidence) {
          evidence.className = "event-meta";
          evidence.style.marginTop = "10px";
          evidence.style.lineHeight = "1.5";
          evidence.textContent = event.evidence;
          card.appendChild(evidence);
        }

        section.appendChild(card);
      }

      eventList.appendChild(section);
    }

    if (!eventList.children.length) {
      eventList.innerHTML = `<div class="event-placeholder">${selectedEventScope} 이벤트가 아직 없습니다.</div>`;
    }
  };

  await render();
});
