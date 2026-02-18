/* ============================================================
   POLYFOLIO — app.js
   Prediction Market Portfolio Forensics
   Pure vanilla JS, no build, no backend
   ============================================================ */

(function () {
  'use strict';

  /* ---------- CONSTANTS ---------- */
  const API_BASE = 'https://data-api.polymarket.com';
  const ADDR_RE = /^0x[a-fA-F0-9]{40}$/;

  /*
   * Per-endpoint pagination limits (from API docs + empirical testing):
   *   /positions         — accepts large limits (tested 500 → returned 305)
   *   /closed-positions  — hard cap at 50 per page (silently truncates)
   *   /trades            — max 10,000 per page, offset max 10,000
   *   /activity          — max 500 per page, offset max 10,000
   */
  const ENDPOINT_CONFIG = {
    positions:       { limit: 500,   maxOffset: 100000 },
    closedPositions: { limit: 50,    maxOffset: 100000 },
    trades:          { limit: 10000, maxOffset: 10000  },
    activity:        { limit: 500,   maxOffset: 10000  },
  };

  const CHART_COLORS = [
    '#C0503A', '#2D8A54', '#7B6250', '#4A7A8A', '#A06830',
    '#6A8A5B', '#8B5E6E', '#5A7060', '#B07848', '#5B7E90',
    '#9A7040', '#507A6A', '#8A6878', '#6A9070', '#C47050',
    '#3A7A5A', '#7A6A58', '#5890A0', '#A88050', '#688068',
  ];

  /* ---------- DOM REFS ---------- */
  const $ = (sel) => document.querySelector(sel);
  const entryScreen = $('#entry-screen');
  const loadingScreen = $('#loading-screen');
  const dashboard = $('#dashboard');
  const addressInput = $('#address-input');
  const loadBtn = $('#load-btn');
  const entryError = $('#entry-error');
  const loadingAddr = $('#loading-address');
  const dashAddr = $('#dash-address');
  const backBtn = $('#back-btn');

  /* ---------- STATE ---------- */
  let chartInstances = [];
  let currentSortKey = 'currentValue';
  let currentSortDir = 'desc';
  let activePositions = [];
  let closedSortKey = 'timestamp';
  let closedSortDir = 'desc';
  let closedPositionsData = [];
  let activePage = 1;
  let closedPage = 1;
  const PAGE_SIZE = 50;

  /* ---------- THEME ---------- */
  function getTheme() {
    return document.documentElement.getAttribute('data-theme') || 'light';
  }

  function setTheme(theme) {
    if (theme === 'dark') {
      document.documentElement.setAttribute('data-theme', 'dark');
    } else {
      document.documentElement.removeAttribute('data-theme');
    }
    try { localStorage.setItem('polyfolio-theme', theme); } catch (_) {}
  }

  function toggleTheme() {
    const newTheme = getTheme() === 'light' ? 'dark' : 'light';
    gtag('event', 'theme_toggle', { theme: newTheme });
    setTheme(newTheme);
    if (!dashboard.classList.contains('hidden') && lastData) {
      /* Suppress animations during theme switch */
      dashboard.classList.add('no-anim');
      renderAllCharts(lastData);
      requestAnimationFrame(() => dashboard.classList.remove('no-anim'));
    }
  }

  /* Restore saved theme (default: light) */
  (function initTheme() {
    try {
      const saved = localStorage.getItem('polyfolio-theme');
      if (saved) setTheme(saved);
    } catch (_) {}
  })();

  /* Wire both toggle buttons */
  $('#entry-theme-btn').addEventListener('click', toggleTheme);
  $('#dash-theme-btn').addEventListener('click', toggleTheme);

  /* ---------- HELPERS ---------- */
  function getCSSVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function formatUSD(n) {
    const num = Number(n);
    if (isNaN(num)) return '—';
    const abs = Math.abs(num);
    if (abs >= 1e6) return (num < 0 ? '-' : '') + '$' + (abs / 1e6).toFixed(2) + 'M';
    if (abs >= 1e3) return (num < 0 ? '-' : '') + '$' + (abs / 1e3).toFixed(2) + 'K';
    return '$' + num.toFixed(2);
  }

  function formatPct(n) {
    const num = Number(n);
    if (isNaN(num)) return '—';
    return (num >= 0 ? '+' : '') + (num * 100).toFixed(1) + '%';
  }

  function formatDate(d) {
    if (!d) return '—';
    const dt = new Date(d);
    if (isNaN(dt)) return '—';
    return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  }

  function pnlClass(n) {
    const num = Number(n);
    if (num > 0) return 'pnl-positive';
    if (num < 0) return 'pnl-negative';
    return '';
  }

  function metricClass(n) {
    const num = Number(n);
    if (num > 0) return 'positive';
    if (num < 0) return 'negative';
    return '';
  }

  function truncAddr(addr) {
    return addr.slice(0, 8) + '...' + addr.slice(-6);
  }

  function escapeHTML(str) {
    const el = document.createElement('span');
    el.textContent = str || '';
    return el.innerHTML;
  }

  /* ---------- API WITH PAGINATION ---------- */
  async function fetchJSON(path) {
    const res = await fetch(API_BASE + path);
    if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
    return res.json();
  }

  /**
   * Unwrap API response into an array regardless of shape.
   */
  function unwrapArray(data) {
    if (Array.isArray(data)) return data;
    if (data && Array.isArray(data.data)) return data.data;
    if (data && Array.isArray(data.results)) return data.results;
    if (data && Array.isArray(data.positions)) return data.positions;
    return null;
  }

  /**
   * Paginate through an endpoint using its documented limits.
   *
   * @param {string} basePath  — e.g. '/closed-positions?'
   * @param {string} address   — 0x-prefixed wallet
   * @param {object} config    — { limit, maxOffset } from ENDPOINT_CONFIG
   */
  async function fetchAllPages(basePath, address, config) {
    const { limit, maxOffset } = config;
    const sep = basePath.includes('?') ? '&' : '?';
    let all = [];
    let offset = 0;

    while (offset <= maxOffset) {
      const path = `${basePath}user=${address}${sep}limit=${limit}&offset=${offset}`;
      let raw;
      try {
        raw = await fetchJSON(path);
      } catch (_) {
        break;
      }

      const batch = unwrapArray(raw);
      if (!batch) break;

      all = all.concat(batch);

      /* Stop if this page returned fewer than the limit — no more data */
      if (batch.length < limit) break;

      offset += limit;
    }

    return all;
  }

  let lastData = null;

  /* ---------- METRICS ---------- */
  function computeMetrics(data) {
    const { positions, closedPositions, leaderboard } = data;

    const totalValue = positions.reduce((s, p) => s + Number(p.currentValue || 0), 0);
    const unrealizedPnl = positions.reduce((s, p) => s + Number(p.cashPnl || 0), 0);
    const realizedPnl = closedPositions.reduce((s, p) => s + Number(p.realizedPnl || 0), 0);

    const closedWithPnl = closedPositions.filter(p => Number(p.realizedPnl) !== 0);
    const wins = closedWithPnl.filter(p => Number(p.realizedPnl) > 0).length;
    const winRate = closedWithPnl.length > 0 ? wins / closedWithPnl.length : 0;

    const totalPnl = unrealizedPnl + realizedPnl;
    const costBasis = totalValue - unrealizedPnl;
    const returnPct = costBasis > 0 ? totalPnl / costBasis : 0;

    const rank = leaderboard && leaderboard[0] ? leaderboard[0].rank : null;

    return {
      rank,
      totalValue,
      unrealizedPnl,
      realizedPnl,
      returnPct,
      winRate,
      activeCount: positions.length,
      closedCount: closedPositions.length,
    };
  }

  function renderMetrics(m) {
    const rk = $('#m-rank');
    rk.textContent = m.rank ? '#' + Number(m.rank).toLocaleString() : '—';

    const tv = $('#m-total-value');
    const up = $('#m-unrealized-pnl');
    const rp = $('#m-realized-pnl');
    const ret = $('#m-return-pct');
    const wr = $('#m-win-rate');
    const ac = $('#m-active');
    const cl = $('#m-closed');

    tv.textContent = formatUSD(m.totalValue);
    up.textContent = formatUSD(m.unrealizedPnl);
    up.className = 'metric-value ' + metricClass(m.unrealizedPnl);
    rp.textContent = formatUSD(m.realizedPnl);
    rp.className = 'metric-value ' + metricClass(m.realizedPnl);
    const retVal = (m.returnPct * 100).toFixed(1) + '%';
    ret.textContent = (m.returnPct >= 0 ? '+' : '') + retVal;
    ret.className = 'metric-value ' + metricClass(m.returnPct);
    wr.textContent = (m.winRate * 100).toFixed(1) + '%';
    ac.textContent = m.activeCount;
    cl.textContent = m.closedCount;
  }

  /* ---------- PDF EXPORT ---------- */
  function generatePDF() {
    if (!lastData) return;
    if (!window.jspdf) { alert('PDF library failed to load. Please refresh and try again.'); return; }
    gtag('event', 'pdf_download');
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
    const pw = doc.internal.pageSize.getWidth();
    const margin = 14;
    const colW = pw - margin * 2;
    let y = 18;

    function checkPage(needed) {
      if (y + needed > 280) { doc.addPage(); y = 18; }
    }

    /* --- Title --- */
    doc.setFontSize(18);
    doc.setFont('helvetica', 'bold');
    doc.text('Polyfolio Report', margin, y);
    y += 7;
    doc.setFontSize(9);
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(120);
    const addr = dashAddr.textContent || '';
    const now = new Date();
    const dateStr = now.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
    const timeStr = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
    doc.text(addr + '  |  ' + dateStr + ' at ' + timeStr, margin, y);
    doc.setTextColor(0);
    y += 10;

    /* --- Metrics --- */
    const m = computeMetrics(lastData);
    const retVal = (m.returnPct >= 0 ? '+' : '') + (m.returnPct * 100).toFixed(1) + '%';
    const rankStr = m.rank ? '#' + Number(m.rank).toLocaleString() : '—';
    const metrics = [
      ['Portfolio Value', formatUSD(m.totalValue)],
      ['Return %', retVal],
      ['Win Rate', (m.winRate * 100).toFixed(1) + '%'],
      ['Realized PnL', formatUSD(m.realizedPnl)],
      ['Unrealized PnL', formatUSD(m.unrealizedPnl)],
      ['Leaderboard Rank', rankStr],
      ['Active Positions', String(m.activeCount)],
      ['Closed Positions', String(m.closedCount)],
    ];

    doc.setFontSize(12);
    doc.setFont('helvetica', 'bold');
    doc.text('Summary', margin, y);
    y += 6;

    doc.setFontSize(9);
    const mColW = colW / 4;
    metrics.forEach((pair, i) => {
      const col = i % 4;
      const x = margin + col * mColW;
      if (i > 0 && col === 0) y += 12;
      doc.setFont('helvetica', 'normal');
      doc.setTextColor(120);
      doc.text(pair[0], x, y);
      doc.setTextColor(0);
      doc.setFont('helvetica', 'bold');
      doc.text(pair[1], x, y + 4.5);
    });
    y += 18;

    /* --- Allocation Chart + Legend --- */
    const chartCanvas = $('#chart-alloc-position');
    const allocChart = chartInstances[0];
    if (chartCanvas && allocChart) {
      const allocLabels = allocChart.data.labels || [];
      const allocValues = allocChart.data.datasets[0].data || [];
      const allocColors = allocChart.data.datasets[0].backgroundColor || [];
      const allocTotal = allocValues.reduce((s, v) => s + v, 0);
      const legendLineH = 5;
      const chartSize = 60;
      const legendH = allocLabels.length * legendLineH;
      const sectionH = Math.max(chartSize, legendH) + 10;

      checkPage(sectionH + 10);
      doc.setFontSize(12);
      doc.setFont('helvetica', 'bold');
      doc.setTextColor(0);
      doc.text('Allocation by Position', margin, y);
      y += 4;

      const chartY = y;
      try {
        const imgData = chartCanvas.toDataURL('image/png');
        doc.addImage(imgData, 'PNG', margin, y, chartSize, chartSize);
      } catch (_) {}

      /* Legend to the right of chart */
      const legendX = margin + chartSize + 8;
      const legendW = colW - chartSize - 8;
      let ly = chartY + 2;
      doc.setFontSize(6.5);
      allocLabels.forEach((lbl, i) => {
        if (!allocChart.getDataVisibility(i)) return;
        const color = allocColors[i] || '#999';
        const r = parseInt(color.slice(1, 3), 16);
        const g = parseInt(color.slice(3, 5), 16);
        const b = parseInt(color.slice(5, 7), 16);
        doc.setFillColor(r, g, b);
        doc.rect(legendX, ly - 2.5, 3, 3, 'F');

        const pct = allocTotal ? ((allocValues[i] / allocTotal) * 100).toFixed(1) : '0.0';
        let txt = lbl + '  ' + formatUSD(allocValues[i]) + ' (' + pct + '%)';
        doc.setFont('helvetica', 'normal');
        doc.setTextColor(60);
        while (txt.length > 3 && doc.getTextWidth(txt) > legendW - 6) {
          txt = txt.slice(0, -4) + '...';
        }
        doc.text(txt, legendX + 5, ly);
        ly += legendLineH;
      });
      doc.setTextColor(0);
      y += Math.max(chartSize, ly - chartY) + 6;
    }

    /* --- Helper: draw table --- */
    function drawTable(title, headers, rows, widths) {
      checkPage(20);
      doc.setFontSize(12);
      doc.setFont('helvetica', 'bold');
      doc.setTextColor(0);
      doc.text(title, margin, y);
      y += 6;

      /* If no custom widths, give title col 40% and split the rest equally */
      const cellWidths = widths || (() => {
        const titleW = colW * 0.4;
        const restW = (colW - titleW) / (headers.length - 1);
        return headers.map((_, i) => i === 0 ? titleW : restW);
      })();
      const rowH = 5.5;

      /* Header row */
      doc.setFontSize(7);
      doc.setFont('helvetica', 'bold');
      doc.setFillColor(240, 237, 230);
      doc.rect(margin, y - 3.5, colW, rowH, 'F');
      headers.forEach((h, i) => {
        const x = margin + cellWidths.slice(0, i).reduce((s, w) => s + w, 0);
        doc.text(h, x + 1, y);
      });
      y += rowH;

      /* Data rows */
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(7);
      rows.forEach((row, ri) => {
        checkPage(rowH + 2);
        if (ri % 2 === 0) {
          doc.setFillColor(250, 248, 245);
          doc.rect(margin, y - 3.5, colW, rowH, 'F');
        }
        row.forEach((cell, i) => {
          const x = margin + cellWidths.slice(0, i).reduce((s, w) => s + w, 0);
          const maxW = cellWidths[i] - 2;
          let txt = String(cell || '');
          while (txt.length > 3 && doc.getTextWidth(txt) > maxW) {
            txt = txt.slice(0, -4) + '...';
          }
          doc.text(txt, x + 1, y);
        });
        y += rowH;
      });
      y += 4;
    }

    /* --- Active Positions Table --- */
    const positions = lastData.positions || [];
    if (positions.length) {
      const activeHeaders = ['Title', 'Outcome', 'Size', 'Avg Price', 'Cur Price', 'Value', 'PnL', '%'];
      const activeRows = positions.map(p => {
        const ppnl = Number(p.percentPnl || 0);
        return [
          p.title || 'Unknown',
          p.outcome || '',
          Number(p.size || 0).toFixed(2),
          Number(p.avgPrice || 0).toFixed(3),
          Number(p.curPrice || 0).toFixed(3),
          formatUSD(Number(p.currentValue || 0)),
          formatUSD(Number(p.cashPnl || 0)),
          (ppnl >= 0 ? '+' : '') + ppnl.toFixed(1) + '%',
        ];
      });
      drawTable('Active Positions (' + positions.length + ')', activeHeaders, activeRows);
    }

    /* --- Closed Positions Table --- */
    const closed = lastData.closedPositions || [];
    if (closed.length) {
      const closedHeaders = ['Title', 'Outcome', 'Realized PnL', '% Return', 'Closed'];
      const closedRows = closed.map(p => {
        const rpnl = Number(p.realizedPnl || 0);
        const costBasis = Number(p.totalBought || 0) * Number(p.avgPrice || 0);
        const rpct = costBasis > 0 ? (rpnl / costBasis) * 100 : 0;
        return [
          p.title || 'Unknown',
          p.outcome || '',
          formatUSD(rpnl),
          (rpct >= 0 ? '+' : '') + rpct.toFixed(1) + '%',
          formatDate(p.timestamp),
        ];
      });
      drawTable('Closed Positions (' + closed.length + ')', closedHeaders, closedRows);
    }

    /* --- Save --- */
    const filename = 'polyfolio-' + (addr.slice(0, 10) || 'report') + '.pdf';
    doc.save(filename);
  }

  /* ---------- DOWNLOAD DROPDOWN ---------- */
  const dlBtn = $('#download-btn');
  const dlDropdown = $('#download-dropdown');

  dlBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    dlDropdown.classList.toggle('visible');
  });

  document.addEventListener('click', (e) => {
    if (!dlDropdown.contains(e.target) && e.target !== dlBtn) {
      dlDropdown.classList.remove('visible');
    }
  });

  $('#dl-pdf').addEventListener('click', () => {
    dlDropdown.classList.remove('visible');
    generatePDF();
  });

  $('#dl-excel').addEventListener('click', () => {
    dlDropdown.classList.remove('visible');
    generateExcel();
  });

  /* ---------- EXCEL EXPORT ---------- */
  function generateExcel() {
    if (!lastData) return;
    if (!window.XLSX) { alert('Excel library failed to load. Please refresh and try again.'); return; }
    gtag('event', 'excel_download');

    const wb = XLSX.utils.book_new();
    const m = computeMetrics(lastData);
    const addr = dashAddr.textContent || '';
    const polymarketBase = 'https://polymarket.com/event/';

    /* --- Active Positions Sheet --- */
    const activeHeader = ['Title', 'Outcome', 'Size', 'Avg Price', 'Cur Price', 'Value', 'PnL', 'PnL %', 'End Date', 'Link'];
    const activeData = (lastData.positions || []).map(p => [
      p.title || 'Unknown',
      p.outcome || '',
      Number(p.size || 0),
      Number(p.avgPrice || 0),
      Number(p.curPrice || 0),
      Number(p.currentValue || 0),
      Number(p.cashPnl || 0),
      Number(p.percentPnl || 0),
      p.endDate ? formatDate(p.endDate) : '',
      p.slug ? polymarketBase + p.slug : '',
    ]);

    const summaryRows = [
      ['Polyfolio Report — ' + addr],
      ['Generated', new Date().toLocaleString()],
      [],
      ['Portfolio Value', m.totalValue, '', 'Return %', (m.returnPct * 100).toFixed(1) + '%'],
      ['Realized PnL', m.realizedPnl, '', 'Unrealized PnL', m.unrealizedPnl],
      ['Win Rate', (m.winRate * 100).toFixed(1) + '%', '', 'Rank', m.rank || '—'],
      ['Active Positions', m.activeCount, '', 'Closed Positions', m.closedCount],
      [],
    ];
    const activeSheet = XLSX.utils.aoa_to_sheet([...summaryRows, activeHeader, ...activeData]);
    XLSX.utils.book_append_sheet(wb, activeSheet, 'Active Positions');

    /* --- Closed Positions Sheet --- */
    const closedHeader = ['Title', 'Outcome', 'Realized PnL', '% Return', 'Closed', 'Link'];
    const closedData = (lastData.closedPositions || []).map(p => {
      const rpnl = Number(p.realizedPnl || 0);
      const costBasis = Number(p.totalBought || 0) * Number(p.avgPrice || 0);
      const rpct = costBasis > 0 ? (rpnl / costBasis) * 100 : 0;
      return [
        p.title || 'Unknown',
        p.outcome || '',
        rpnl,
        Number(rpct.toFixed(1)),
        formatDate(p.timestamp),
        p.slug ? polymarketBase + p.slug : '',
      ];
    });
    const closedSheet = XLSX.utils.aoa_to_sheet([closedHeader, ...closedData]);
    XLSX.utils.book_append_sheet(wb, closedSheet, 'Closed Positions');

    /* --- Winners & Losers Sheet --- */
    const pnlMap = {};
    for (const p of (lastData.positions || [])) {
      const key = p.title || 'Unknown';
      if (!pnlMap[key]) pnlMap[key] = { title: key, slug: p.slug || '', pnl: 0 };
      pnlMap[key].pnl += Number(p.cashPnl || 0);
      if (!pnlMap[key].slug && p.slug) pnlMap[key].slug = p.slug;
    }
    for (const p of (lastData.closedPositions || [])) {
      const key = p.title || 'Unknown';
      if (!pnlMap[key]) pnlMap[key] = { title: key, slug: p.slug || '', pnl: 0 };
      pnlMap[key].pnl += Number(p.realizedPnl || 0);
      if (!pnlMap[key].slug && p.slug) pnlMap[key].slug = p.slug;
    }
    const allPnl = Object.values(pnlMap).filter(p => p.pnl !== 0).sort((a, b) => b.pnl - a.pnl);
    const wlHeader = ['Market', 'Total PnL', 'Link'];
    const wlData = allPnl.map(p => [
      p.title,
      Number(p.pnl.toFixed(2)),
      p.slug ? polymarketBase + p.slug : '',
    ]);
    const wlSheet = XLSX.utils.aoa_to_sheet([wlHeader, ...wlData]);
    XLSX.utils.book_append_sheet(wb, wlSheet, 'Winners & Losers');

    /* --- Transaction History Sheet --- */
    const activity = lastData.activity || [];
    if (activity.length) {
      const actHeader = ['Date', 'Time', 'Type', 'Side', 'Title', 'Outcome', 'Size', 'Price', 'USDC Value', 'Link'];
      const actData = activity.map(ev => {
        const ts = ev.timestamp ? new Date(ev.timestamp * 1000) : null;
        const type = ev.type || 'TRADE';
        return [
          ts ? ts.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '',
          ts ? ts.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' }) : '',
          type.charAt(0) + type.slice(1).toLowerCase().replace('_', ' '),
          ev.side || '',
          ev.title || 'Unknown',
          ev.outcome || '',
          Number(ev.size || 0),
          Number(ev.price || 0),
          Number(ev.usdcSize || 0),
          (ev.slug || ev.eventSlug) ? polymarketBase + (ev.slug || ev.eventSlug) : '',
        ];
      });
      const actSheet = XLSX.utils.aoa_to_sheet([actHeader, ...actData]);
      XLSX.utils.book_append_sheet(wb, actSheet, 'Transaction History');
    }

    const filename = 'polyfolio-' + (addr.slice(0, 10) || 'report') + '.xlsx';
    XLSX.writeFile(wb, filename);
  }

  /* ---------- CHARTS ---------- */
  function destroyCharts() {
    chartInstances.forEach(c => c.destroy());
    chartInstances = [];
  }

  function chartDefaults() {
    Chart.defaults.color = getCSSVar('--chart-label');
    Chart.defaults.borderColor = getCSSVar('--border');
    Chart.defaults.font.family = "'IBM Plex Mono', 'Consolas', monospace";
    Chart.defaults.font.size = 11;
  }

  /**
   * Build chart data from positions with an "Other" bucket.
   * Shows top N individually, lumps the rest into "Other".
   */
  function buildPieData(items, maxSlices) {
    if (items.length <= maxSlices) {
      return { labels: items.map(i => i.label), fullLabels: items.map(i => i.fullLabel || i.label), slugs: items.map(i => i.slug || null), values: items.map(i => i.value) };
    }
    const top = items.slice(0, maxSlices - 1);
    const rest = items.slice(maxSlices - 1);
    const otherValue = rest.reduce((s, i) => s + i.value, 0);
    const otherNames = rest.map(i => i.fullLabel || i.label);
    return {
      labels: [...top.map(i => i.label), `Other (${rest.length})`],
      fullLabels: [...top.map(i => i.fullLabel || i.label), `Other - [${otherNames.join(', ')}]`],
      slugs: [...top.map(i => i.slug || null), null],
      values: [...top.map(i => i.value), otherValue],
    };
  }

  function renderAllocationByPosition(positions) {
    const ctx = $('#chart-alloc-position').getContext('2d');
    const chartBorder = getCSSVar('--chart-border');

    const sorted = [...positions]
      .map(p => {
        const full = (p.title || 'Unknown') + (p.outcome ? ` (${p.outcome})` : '');
        return {
          label: full.length > 65 ? full.slice(0, 65) + '…' : full,
          fullLabel: full,
          slug: p.slug || null,
          value: Number(p.currentValue || 0),
        };
      })
      .filter(p => p.value > 0)
      .sort((a, b) => b.value - a.value);

    const { labels, fullLabels, slugs, values } = buildPieData(sorted, 14);
    const colors = CHART_COLORS.slice(0, labels.length);

    const chart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels,
        datasets: [{
          data: values,
          backgroundColor: colors,
          borderColor: chartBorder,
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '55%',
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => fullLabels[items[0].dataIndex] || labels[items[0].dataIndex],
              label: (ctx) => {
                const total = ctx.dataset.data.reduce((s, v) => s + v, 0);
                const pct = total ? ((ctx.raw / total) * 100).toFixed(1) : 0;
                return ' ' + formatUSD(ctx.raw) + ` (${pct}%)`;
              },
            },
          },
        },
      },
    });
    chartInstances.push(chart);

    /* Build HTML legend */
    const legendEl = document.getElementById('alloc-legend');
    legendEl.innerHTML = '';
    labels.forEach((label, i) => {
      const item = document.createElement('div');
      item.className = 'alloc-legend-item';
      const slug = slugs[i];
      if (slug) item.dataset.slug = slug;

      const swatch = document.createElement('span');
      swatch.className = 'alloc-legend-swatch';
      swatch.style.backgroundColor = colors[i];

      const text = document.createElement('span');
      text.className = 'alloc-legend-text' + (slug ? ' alloc-legend-link' : '');
      const display = fullLabels[i] || label;
      text.textContent = display.length > 75 ? display.slice(0, 75) + '…' : display;
      text.title = display;

      /* Click swatch to toggle slice visibility */
      swatch.addEventListener('click', (e) => {
        e.stopPropagation();
        chart.toggleDataVisibility(i);
        chart.update();
        const hidden = !chart.getDataVisibility(i);
        item.style.opacity = hidden ? '0.4' : '';
        swatch.style.opacity = hidden ? '0.35' : '';
      });

      item.appendChild(swatch);
      item.appendChild(text);
      legendEl.appendChild(item);

      if (slug) {
        item.addEventListener('mouseenter', () => {
          clearTimeout(embedTimeout);
          showEmbedPopover(item, slug);
        });
        item.addEventListener('mouseleave', () => {
          hideEmbedPopover();
        });
      }
    });

    /* Download button — composites chart + legend into one PNG */
    $('#download-alloc').addEventListener('click', (e) => {
      e.preventDefault();
      const chartCanvas = $('#chart-alloc-position');
      const dpr = window.devicePixelRatio || 1;
      const textColor = getCSSVar('--text-primary');
      const bgColor = getCSSVar('--bg-card');

      const padding = 32;
      const swatchSize = 12;
      const lineHeight = 22;
      const legendFont = '13px sans-serif';
      const chartW = chartCanvas.width / dpr;
      const chartH = chartCanvas.height / dpr;
      const legendTop = chartH + padding * 2;
      const total = values.reduce((s, v) => s + v, 0);

      /* Measure legend text to size the canvas */
      const measure = document.createElement('canvas').getContext('2d');
      measure.font = legendFont;
      const legendLines = labels.map((lbl, i) => {
        const full = fullLabels[i] || lbl;
        const display = full.length > 100 ? full.slice(0, 100) + '…' : full;
        const pct = total ? ((values[i] / total) * 100).toFixed(1) : '0.0';
        return `${display}  ${formatUSD(values[i])} (${pct}%)`;
      });
      const maxTextW = Math.max(...legendLines.map(l => measure.measureText(l).width));
      const legendW = padding + swatchSize + 8 + maxTextW + padding;

      const legendH = labels.length * lineHeight + padding;
      const totalW = Math.max(chartW + padding * 2, legendW);
      const totalH = legendTop + legendH;

      const out = document.createElement('canvas');
      out.width = totalW * dpr;
      out.height = totalH * dpr;
      const cx = out.getContext('2d');
      cx.scale(dpr, dpr);

      /* Background */
      cx.fillStyle = bgColor;
      cx.fillRect(0, 0, totalW, totalH);

      /* Title */
      cx.fillStyle = textColor;
      cx.font = 'bold 16px sans-serif';
      cx.fillText('Allocation by Position', padding, padding - 8);

      /* Draw chart */
      cx.drawImage(chartCanvas, (totalW - chartW) / 2, padding, chartW, chartH);

      /* Draw legend */
      labels.forEach((lbl, i) => {
        if (!chart.getDataVisibility(i)) return;
        const y = legendTop + i * lineHeight;
        cx.fillStyle = colors[i];
        cx.fillRect(padding, y, swatchSize, swatchSize);
        cx.fillStyle = textColor;
        cx.font = legendFont;
        cx.fillText(legendLines[i], padding + swatchSize + 8, y + swatchSize - 1);
      });

      const link = document.createElement('a');
      link.download = 'allocation-by-position.png';
      link.href = out.toDataURL('image/png');
      link.click();
    });
  }

  function renderWinnersLosers(positions, closedPositions) {
    const greenColor = getCSSVar('--green');
    const redColor = getCSSVar('--red');
    const gridColor = getCSSVar('--chart-grid');

    /* Combine active (cashPnl) and closed (realizedPnl), aggregate by market title */
    const pnlMap = {};
    for (const p of positions) {
      const key = p.title || 'Unknown';
      if (!pnlMap[key]) pnlMap[key] = { title: key, slug: p.slug || null, pnl: 0 };
      pnlMap[key].pnl += Number(p.cashPnl || 0);
      if (!pnlMap[key].slug && p.slug) pnlMap[key].slug = p.slug;
    }
    for (const p of closedPositions) {
      const key = p.title || 'Unknown';
      if (!pnlMap[key]) pnlMap[key] = { title: key, slug: p.slug || null, pnl: 0 };
      pnlMap[key].pnl += Number(p.realizedPnl || 0);
      if (!pnlMap[key].slug && p.slug) pnlMap[key].slug = p.slug;
    }

    const allPnl = Object.values(pnlMap).filter(p => p.pnl !== 0);
    const sorted = allPnl.sort((a, b) => b.pnl - a.pnl);

    const winners = sorted.filter(p => p.pnl > 0).slice(0, 5);
    const losers = sorted.filter(p => p.pnl < 0).slice(-5).reverse();

    /* Plugin: draw underline beneath y-axis labels that have slugs */
    const labelUnderlinePlugin = {
      id: 'labelUnderline',
      afterDraw(chart) {
        const slugList = chart.options._slugs;
        if (!slugList) return;
        const yScale = chart.scales.y;
        const ctx = chart.ctx;
        const underlineColor = getCSSVar('--border');
        ctx.save();
        ctx.strokeStyle = underlineColor;
        ctx.lineWidth = 1;
        for (let i = 0; i < yScale.ticks.length; i++) {
          if (!slugList[i]) continue;
          const yPixel = yScale.getPixelForTick(i);
          const label = yScale.getLabelForValue(i);
          ctx.font = '10px ' + Chart.defaults.font.family;
          const textW = ctx.measureText(label).width;
          const x = yScale.right - textW - 2;
          ctx.beginPath();
          ctx.moveTo(x, yPixel + 7);
          ctx.lineTo(x + textW, yPixel + 7);
          ctx.stroke();
        }
        ctx.restore();
      }
    };

    const linkedLabelColor = getCSSVar('--text-primary');

    /* Helper: attach label-hover embed to a horizontal bar chart */
    function attachLabelHover(chart, slugs) {
      const canvas = chart.canvas;
      let hoveredIdx = -1;
      canvas.addEventListener('mousemove', (e) => {
        const yScale = chart.scales.y;
        const rect = canvas.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;
        /* Only trigger when cursor is in the y-axis label area */
        if (mx > yScale.right) {
          if (hoveredIdx !== -1) { hoveredIdx = -1; canvas.style.cursor = 'default'; hideEmbedPopover(); }
          return;
        }
        let found = -1;
        for (let i = 0; i < yScale.ticks.length; i++) {
          const yPixel = yScale.getPixelForTick(i);
          if (Math.abs(my - yPixel) < 14) { found = i; break; }
        }
        if (found !== hoveredIdx) {
          hoveredIdx = found;
          if (found >= 0 && slugs[found]) {
            canvas.style.cursor = 'pointer';
            clearTimeout(embedTimeout);
            const yPixel = yScale.getPixelForTick(found);
            showEmbedPopover({ x: rect.left + yScale.right, y: rect.top + yPixel }, slugs[found]);
          } else {
            canvas.style.cursor = 'default';
            hideEmbedPopover();
          }
        }
      });
      canvas.addEventListener('mouseleave', () => { hoveredIdx = -1; canvas.style.cursor = 'default'; hideEmbedPopover(); });
    }

    /* Winners */
    const winnerSlugs = winners.map(p => p.slug);
    const ctxW = $('#chart-winners').getContext('2d');
    const chartW = new Chart(ctxW, {
      type: 'bar',
      plugins: [labelUnderlinePlugin],
      data: {
        labels: winners.map(p => (p.title || '').slice(0, 22)),
        datasets: [{
          data: winners.map(p => p.pnl),
          backgroundColor: greenColor,
          borderRadius: 2,
          barThickness: 22,
        }],
      },
      options: {
        _slugs: winnerSlugs,
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (ctx) => formatUSD(ctx.raw) } },
        },
        scales: {
          x: {
            grid: { color: gridColor },
            ticks: { callback: (v) => formatUSD(v) },
          },
          y: {
            grid: { display: false },
            ticks: {
              font: { size: 10 },
              color: (ctx) => winnerSlugs[ctx.index] ? linkedLabelColor : getCSSVar('--chart-label'),
            },
          },
        },
      },
    });
    chartInstances.push(chartW);
    attachLabelHover(chartW, winnerSlugs);

    /* Losers */
    const loserSlugs = losers.map(p => p.slug);
    const ctxL = $('#chart-losers').getContext('2d');
    const chartL = new Chart(ctxL, {
      type: 'bar',
      plugins: [labelUnderlinePlugin],
      data: {
        labels: losers.map(p => (p.title || '').slice(0, 22)),
        datasets: [{
          data: losers.map(p => p.pnl),
          backgroundColor: redColor,
          borderRadius: 2,
          barThickness: 22,
        }],
      },
      options: {
        _slugs: loserSlugs,
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (ctx) => formatUSD(ctx.raw) } },
        },
        scales: {
          x: {
            grid: { color: gridColor },
            ticks: { callback: (v) => formatUSD(v) },
          },
          y: {
            grid: { display: false },
            ticks: {
              font: { size: 10 },
              color: (ctx) => loserSlugs[ctx.index] ? linkedLabelColor : getCSSVar('--chart-label'),
            },
          },
        },
      },
    });
    chartInstances.push(chartL);
    attachLabelHover(chartL, loserSlugs);
  }

  function renderTradeVolume(trades) {
    const ctx = $('#chart-volume').getContext('2d');
    const amberColor = getCSSVar('--amber');
    const amberMuted = getCSSVar('--amber-muted');
    const gridColor = getCSSVar('--chart-grid');
    const gridLightColor = getCSSVar('--chart-grid-light');

    if (!trades.length) {
      const chart = new Chart(ctx, {
        type: 'line',
        data: { labels: ['No data'], datasets: [{ data: [0] }] },
        options: { responsive: true, maintainAspectRatio: false },
      });
      chartInstances.push(chart);
      return;
    }

    /* Bucket trades by day — notional volume (size * price) */
    const dayMap = {};
    trades.forEach(t => {
      const d = new Date(t.timestamp || t.createdAt || 0);
      const key = d.toISOString().slice(0, 10);
      if (!dayMap[key]) dayMap[key] = 0;
      dayMap[key] += Math.abs(Number(t.size || 0) * Number(t.price || 0));
    });

    const sortedDays = Object.keys(dayMap).sort();
    const values = sortedDays.map(d => dayMap[d]);

    /* 7-day rolling average */
    const ma7 = values.map((_, i) => {
      const window = values.slice(Math.max(0, i - 6), i + 1);
      return window.reduce((s, v) => s + v, 0) / window.length;
    });

    const chart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: sortedDays.map(d => {
          const dt = new Date(d + 'T00:00:00');
          return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        }),
        datasets: [
          {
            label: 'Daily Volume',
            data: values,
            borderColor: amberColor,
            backgroundColor: amberColor + '14',
            fill: true,
            tension: 0.3,
            pointRadius: 0,
            pointHoverRadius: 4,
            borderWidth: 1.5,
          },
          {
            label: '7d Avg',
            data: ma7,
            borderColor: amberMuted,
            backgroundColor: 'transparent',
            fill: false,
            tension: 0.4,
            pointRadius: 0,
            pointHoverRadius: 3,
            borderWidth: 2,
            borderDash: [6, 3],
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: true,
            labels: {
              boxWidth: 12,
              padding: 16,
              font: { size: 10 },
              usePointStyle: true,
            },
          },
          tooltip: {
            callbacks: {
              label: (ctx) => ctx.dataset.label + ': ' + formatUSD(ctx.raw),
            },
          },
        },
        scales: {
          x: {
            grid: { color: gridLightColor },
            ticks: {
              maxTicksLimit: 12,
              font: { size: 10 },
            },
          },
          y: {
            grid: { color: gridColor },
            ticks: { callback: (v) => formatUSD(v) },
          },
        },
      },
    });
    chartInstances.push(chart);
  }

  /* ---------- ACTIVITY TIMELINE ---------- */
  let timelineActivity = [];
  let tlTypeFilters = new Set();
  let tlSideFilters = new Set(['BUY', 'SELL', '']); /* include events with no side */

  const TL_TYPE_LABELS = {
    TRADE: 'Trades',
    REDEEM: 'Redeems',
    SPLIT: 'Splits',
    MERGE: 'Merges',
    REWARD: 'Rewards',
    CONVERSION: 'Conversions',
    MAKER_REBATE: 'Rebates',
    YIELD: 'Yields',
  };

  const TL_TYPE_ICONS = {
    TRADE: '\u2194',       /* ↔ */
    REDEEM: '\u2714',      /* ✔ */
    SPLIT: '\u2702',       /* ✂ */
    MERGE: '\u2726',       /* ✦ */
    REWARD: '\u2605',      /* ★ */
    CONVERSION: '\u21C4',  /* ⇄ */
    MAKER_REBATE: '\u2668', /* ♨ */
    YIELD: '\u2234',        /* ∴ */
  };

  function buildTimelineFilters(activity) {
    const filterBar = $('#tl-filters');

    /* Discover types and sides present */
    const typeCounts = {};
    let buyCount = 0, sellCount = 0;
    activity.forEach(ev => {
      const t = ev.type || 'TRADE';
      typeCounts[t] = (typeCounts[t] || 0) + 1;
      if (ev.side === 'BUY') buyCount++;
      else if (ev.side === 'SELL') sellCount++;
    });

    const types = Object.keys(typeCounts).sort((a, b) => (typeCounts[b] - typeCounts[a]));
    tlTypeFilters = new Set(types);
    tlSideFilters = new Set(['BUY', 'SELL', '']);

    let html = '<span class="sort-label">Filter:</span>';

    /* Side filters */
    if (buyCount) html += '<button class="tl-filter-btn active" data-side="BUY">Buy</button>';
    if (sellCount) html += '<button class="tl-filter-btn active" data-side="SELL">Sell</button>';

    /* Separator if both sides and types exist */
    if ((buyCount || sellCount) && types.length) html += '<span class="tl-filter-sep"></span>';

    /* Type filters */
    types.forEach(type => {
      const label = TL_TYPE_LABELS[type] || type;
      html += '<button class="tl-filter-btn active" data-type="' + type + '">' + escapeHTML(label) + '</button>';
    });

    filterBar.innerHTML = html;

    filterBar.querySelectorAll('.tl-filter-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const type = btn.dataset.type;
        const side = btn.dataset.side;

        if (type) {
          if (tlTypeFilters.has(type)) { tlTypeFilters.delete(type); btn.classList.remove('active'); }
          else { tlTypeFilters.add(type); btn.classList.add('active'); }
        } else if (side) {
          if (tlSideFilters.has(side)) { tlSideFilters.delete(side); btn.classList.remove('active'); }
          else { tlSideFilters.add(side); btn.classList.add('active'); }
        }
        gtag('event', 'filter_change', { filter_type: type || side });
        renderTimelineEvents();
      });
    });
  }

  function renderTimelineEvents() {
    const container = $('#activity-timeline');
    const filtered = timelineActivity.filter(ev => {
      if (!tlTypeFilters.has(ev.type || 'TRADE')) return false;
      const side = ev.side || '';
      if (!tlSideFilters.has(side)) return false;
      if (ev.type === 'YIELD' && Math.abs(Number(ev.usdcSize || 0)) < 0.005) return false;
      return true;
    });

    if (!filtered.length) {
      container.innerHTML = '<p class="empty-state">No matching activity.</p>';
      return;
    }

    /* Sort by timestamp descending */
    const sorted = [...filtered].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));

    /* Group by date */
    const groups = {};
    sorted.forEach(ev => {
      const ts = ev.timestamp ? new Date(ev.timestamp * 1000) : null;
      const key = ts ? ts.toISOString().slice(0, 10) : 'Unknown';
      if (!groups[key]) groups[key] = [];
      groups[key].push(ev);
    });

    let html = '';
    const dateKeys = Object.keys(groups);

    dateKeys.forEach(dateKey => {
      const events = groups[dateKey];
      const dt = new Date(dateKey + 'T00:00:00');
      const dateLabel = isNaN(dt) ? dateKey : dt.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });

      html += '<div class="tl-day">';
      html += '<div class="tl-date">' + escapeHTML(dateLabel) + '</div>';
      html += '<div class="tl-events">';

      events.forEach(ev => {
        const type = ev.type || 'TRADE';
        const icon = TL_TYPE_ICONS[type] || '\u25CF';
        const side = ev.side || '';
        const title = ev.title || 'Unknown Market';
        const outcome = ev.outcome || '';
        const usdcSize = Number(ev.usdcSize || 0);
        const price = Number(ev.price || 0);
        const size = Number(ev.size || 0);
        const slug = ev.slug || ev.eventSlug || '';
        const ts = ev.timestamp ? new Date(ev.timestamp * 1000) : null;
        const time = ts ? ts.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' }) : '';

        const sideClass = side === 'BUY' ? 'tl-buy' : side === 'SELL' ? 'tl-sell' : '';
        const typeLabel = type.charAt(0) + type.slice(1).toLowerCase().replace('_', ' ');

        html += '<div class="tl-event">';
        html += '<div class="tl-icon ' + sideClass + '">' + icon + '</div>';
        html += '<div class="tl-body">';
        html += '<div class="tl-headline">';
        if (type === 'TRADE' && side) {
          html += '<span class="tl-side ' + sideClass + '">' + side + '</span> ';
        } else {
          html += '<span class="tl-type">' + escapeHTML(typeLabel) + '</span> ';
        }
        if (slug) {
          html += '<span class="tl-title tl-title-link" data-slug="' + escapeHTML(slug) + '">' + escapeHTML(title.length > 75 ? title.slice(0, 75) + '\u2026' : title) + '</span>';
        } else {
          html += '<span class="tl-title">' + escapeHTML(title.length > 75 ? title.slice(0, 75) + '\u2026' : title) + '</span>';
        }
        html += '</div>';

        html += '<div class="tl-details">';
        if (outcome) html += '<span class="tl-outcome">' + escapeHTML(outcome) + '</span>';
        if (size) html += '<span class="tl-size">' + size.toFixed(2) + ' shares</span>';
        if (price) html += '<span class="tl-price">@ ' + price.toFixed(2) + '\u00A2</span>';
        if (usdcSize) html += '<span class="tl-usdc">' + formatUSD(usdcSize) + '</span>';
        if (time) html += '<span class="tl-time">' + time + '</span>';
        html += '</div>';

        html += '</div></div>';
      });

      html += '</div></div>';
    });

    container.innerHTML = html;

    /* Embed hover on linked titles */
    container.querySelectorAll('.tl-title-link').forEach(el => {
      el.addEventListener('mouseenter', () => {
        const slug = el.dataset.slug;
        if (slug) {
          clearTimeout(embedTimeout);
          showEmbedPopover(el, slug);
        }
      });
      el.addEventListener('mouseleave', () => { hideEmbedPopover(); });
    });
  }

  function renderTimeline(activity) {
    const container = $('#activity-timeline');
    const filterBar = $('#tl-filters');
    if (!activity || !activity.length) {
      filterBar.innerHTML = '';
      container.innerHTML = '<p class="empty-state">No activity found.</p>';
      return;
    }
    timelineActivity = activity;
    buildTimelineFilters(activity);
    renderTimelineEvents();
  }

  function renderAllCharts(data) {
    chartDefaults();
    destroyCharts();
    if (data.positions.length) {
      renderAllocationByPosition(data.positions);
    }
    if (data.positions.length || data.closedPositions.length) {
      renderWinnersLosers(data.positions, data.closedPositions);
    }
    renderTimeline(data.activity);
    renderTradeVolume(data.trades);
  }

  /* ---------- TABLES ---------- */
  function renderActiveTable(positions, sortKey, sortDir) {
    const tbody = $('#active-tbody');
    const emptyEl = $('#active-empty');
    const countEl = $('#active-count');

    if (!positions.length) {
      tbody.innerHTML = '';
      emptyEl.classList.remove('hidden');
      countEl.textContent = '0 positions';
      return;
    }

    emptyEl.classList.add('hidden');
    countEl.textContent = positions.length + ' position' + (positions.length !== 1 ? 's' : '');

    const sorted = [...positions].sort((a, b) => {
      let va = a[sortKey];
      let vb = b[sortKey];

      if (sortKey === 'endDate') {
        va = va ? new Date(va).getTime() : 0;
        vb = vb ? new Date(vb).getTime() : 0;
      } else {
        va = Number(va) || 0;
        vb = Number(vb) || 0;
      }

      return sortDir === 'asc' ? va - vb : vb - va;
    });

    const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
    if (activePage > totalPages) activePage = totalPages;
    const start = (activePage - 1) * PAGE_SIZE;
    const page = sorted.slice(start, start + PAGE_SIZE);

    tbody.innerHTML = page.map(p => {
      const cv = Number(p.currentValue || 0);
      const cpnl = Number(p.cashPnl || 0);
      const ppnl = Number(p.percentPnl || 0);
      const outcomeClass = (p.outcome || '').toLowerCase() === 'yes' ? 'outcome-yes' :
                           (p.outcome || '').toLowerCase() === 'no' ? 'outcome-no' : '';
      const slugAttr = p.slug ? ` data-slug="${escapeHTML(p.slug)}"` : '';
      return `<tr>
        <td class="td-title"${slugAttr} title="${escapeHTML(p.title)}">${escapeHTML(p.title)}</td>
        <td class="td-outcome ${outcomeClass}">${escapeHTML(p.outcome)}</td>
        <td class="td-num">${Number(p.size || 0).toFixed(2)}</td>
        <td class="td-num">${Number(p.avgPrice || 0).toFixed(3)}</td>
        <td class="td-num">${Number(p.curPrice || 0).toFixed(3)}</td>
        <td class="td-num">${formatUSD(cv)}</td>
        <td class="td-num ${pnlClass(cpnl)}">${formatUSD(cpnl)}</td>
        <td class="td-num ${pnlClass(ppnl)}">${(ppnl >= 0 ? '+' : '') + ppnl.toFixed(1) + '%'}</td>
        <td>${formatDate(p.endDate)}</td>
      </tr>`;
    }).join('');

    renderPagination('active', activePage, totalPages, (p) => {
      activePage = p;
      renderActiveTable(positions, sortKey, sortDir);
    });
  }

  function renderClosedTable(closedPositions, sortKey, sortDir) {
    const tbody = $('#closed-tbody');
    const emptyEl = $('#closed-empty');
    const countEl = $('#closed-count');

    if (!closedPositions.length) {
      tbody.innerHTML = '';
      emptyEl.classList.remove('hidden');
      countEl.textContent = '0 positions';
      return;
    }

    emptyEl.classList.add('hidden');
    countEl.textContent = closedPositions.length + ' position' + (closedPositions.length !== 1 ? 's' : '');

    const sorted = [...closedPositions].sort((a, b) => {
      let va, vb;
      if (sortKey === 'timestamp') {
        va = a.timestamp ? new Date(a.timestamp).getTime() : 0;
        vb = b.timestamp ? new Date(b.timestamp).getTime() : 0;
      } else {
        va = Number(a[sortKey]) || 0;
        vb = Number(b[sortKey]) || 0;
      }
      return sortDir === 'asc' ? va - vb : vb - va;
    });

    const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
    if (closedPage > totalPages) closedPage = totalPages;
    const start = (closedPage - 1) * PAGE_SIZE;
    const page = sorted.slice(start, start + PAGE_SIZE);

    tbody.innerHTML = page.map(p => {
      const rpnl = Number(p.realizedPnl || 0);
      const costBasis = Number(p.totalBought || 0) * Number(p.avgPrice || 0);
      const rpct = costBasis > 0 ? (rpnl / costBasis) * 100 : 0;
      const outcomeClass = (p.outcome || '').toLowerCase() === 'yes' ? 'outcome-yes' :
                           (p.outcome || '').toLowerCase() === 'no' ? 'outcome-no' : '';
      const slugAttr = p.slug ? ` data-slug="${escapeHTML(p.slug)}"` : '';
      return `<tr>
        <td class="td-title"${slugAttr} title="${escapeHTML(p.title)}">${escapeHTML(p.title)}</td>
        <td class="td-outcome ${outcomeClass}">${escapeHTML(p.outcome)}</td>
        <td class="td-num ${pnlClass(rpnl)}">${formatUSD(rpnl)}</td>
        <td class="td-num ${pnlClass(rpnl)}">${(rpct >= 0 ? '+' : '') + rpct.toFixed(1) + '%'}</td>
        <td>${formatDate(p.timestamp)}</td>
      </tr>`;
    }).join('');

    renderPagination('closed', closedPage, totalPages, (p) => {
      closedPage = p;
      renderClosedTable(closedPositions, sortKey, sortDir);
    });
  }

  /* ---------- PAGINATION ---------- */
  function renderPagination(prefix, currentPage, totalPages, onPage) {
    const containerId = prefix + '-pagination';
    let container = document.getElementById(containerId);

    if (totalPages <= 1) {
      if (container) container.remove();
      return;
    }

    if (!container) {
      container = document.createElement('div');
      container.id = containerId;
      container.className = 'table-pagination';
      const tableSection = document.getElementById(prefix + '-table').closest('.table-section');
      tableSection.appendChild(container);
    }

    let html = '';
    html += '<button class="page-btn' + (currentPage <= 1 ? ' disabled' : '') + '" data-page="prev">&lsaquo; Prev</button>';

    const maxVisible = 7;
    let startP = 1, endP = totalPages;
    if (totalPages > maxVisible) {
      startP = Math.max(1, currentPage - 3);
      endP = Math.min(totalPages, startP + maxVisible - 1);
      if (endP - startP < maxVisible - 1) startP = Math.max(1, endP - maxVisible + 1);
    }

    if (startP > 1) {
      html += '<button class="page-btn" data-page="1">1</button>';
      if (startP > 2) html += '<span class="page-ellipsis">&hellip;</span>';
    }
    for (let i = startP; i <= endP; i++) {
      html += '<button class="page-btn' + (i === currentPage ? ' active' : '') + '" data-page="' + i + '">' + i + '</button>';
    }
    if (endP < totalPages) {
      if (endP < totalPages - 1) html += '<span class="page-ellipsis">&hellip;</span>';
      html += '<button class="page-btn" data-page="' + totalPages + '">' + totalPages + '</button>';
    }

    html += '<button class="page-btn' + (currentPage >= totalPages ? ' disabled' : '') + '" data-page="next">Next &rsaquo;</button>';

    container.innerHTML = html;

    container.querySelectorAll('.page-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        if (btn.classList.contains('disabled')) return;
        const val = btn.dataset.page;
        let newPage = currentPage;
        if (val === 'prev') newPage = currentPage - 1;
        else if (val === 'next') newPage = currentPage + 1;
        else newPage = parseInt(val, 10);
        if (newPage >= 1 && newPage <= totalPages && newPage !== currentPage) {
          onPage(newPage);
        }
      });
    });
  }

  /* ---------- EMBED POPOVER ---------- */
  let embedPopover = null;
  let embedTimeout = null;
  let embedShowTimeout = null;
  let activeEmbedCell = null;
  let embedLocked = false;

  function createEmbedPopover() {
    if (embedPopover) return embedPopover;
    embedPopover = document.createElement('div');
    embedPopover.className = 'embed-popover';
    embedPopover.innerHTML = '<iframe class="embed-iframe" frameborder="0"></iframe>';
    document.body.appendChild(embedPopover);

    embedPopover.addEventListener('mouseenter', () => {
      clearTimeout(embedTimeout);
      embedLocked = true;
    });
    embedPopover.addEventListener('mouseleave', () => {
      embedLocked = false;
      hideEmbedPopover();
    });

    return embedPopover;
  }

  function showEmbedPopover(anchor, slug) {
    if (!slug) return;
    if (embedLocked) return;
    clearTimeout(embedShowTimeout);

    embedShowTimeout = setTimeout(() => {
      _revealEmbed(anchor, slug);
    }, 1000);
  }

  function _revealEmbed(anchor, slug) {
    const pop = createEmbedPopover();
    const theme = getTheme();
    const iframe = pop.querySelector('.embed-iframe');
    const src = `https://embed.polymarket.com/market.html?market=${encodeURIComponent(slug)}&features=volume,chart&theme=${theme}`;

    if (iframe.src !== src) {
      iframe.src = src;
    }

    pop.classList.add('visible');
    embedLocked = true;
    activeEmbedCell = anchor;

    /* anchor can be a DOM element or a {x, y} point */
    const popW = 420;
    const popH = 400;
    let left, top;

    if (anchor instanceof HTMLElement) {
      const rect = anchor.getBoundingClientRect();
      left = rect.left + rect.width / 2 - popW / 2;
      top = rect.bottom + 8;
      if (top + popH > window.innerHeight - 8) {
        top = rect.top - popH - 8;
      }
    } else {
      /* Point-based positioning (for chart hover) */
      left = anchor.x - popW / 2;
      top = anchor.y + 16;
      if (top + popH > window.innerHeight - 8) {
        top = anchor.y - popH - 16;
      }
    }

    /* Keep within viewport horizontally */
    if (left < 8) left = 8;
    if (left + popW > window.innerWidth - 8) left = window.innerWidth - popW - 8;

    pop.style.left = left + 'px';
    pop.style.top = top + 'px';
  }

  function hideEmbedPopover() {
    if (embedLocked) return;
    clearTimeout(embedShowTimeout);
    embedTimeout = setTimeout(() => {
      if (embedPopover && !embedLocked) {
        embedPopover.classList.remove('visible');
        activeEmbedCell = null;
      }
    }, 200);
  }

  /* Hide embed on scroll */
  window.addEventListener('scroll', () => {
    if (embedPopover && embedPopover.classList.contains('visible')) {
      embedLocked = false;
      clearTimeout(embedShowTimeout);
      clearTimeout(embedTimeout);
      embedPopover.classList.remove('visible');
      activeEmbedCell = null;
    }
  }, true);

  /* Hide embed when mouse leaves any chart canvas */
  document.addEventListener('mouseleave', (e) => {
    if (e.target.tagName === 'CANVAS') {
      hideEmbedPopover();
    }
  }, true);

  /* Delegate hover events on title cells with data-slug */
  document.addEventListener('mouseover', (e) => {
    const cell = e.target.closest('.td-title[data-slug]');
    if (!cell) return;
    const slug = cell.dataset.slug;
    if (!slug) return;
    clearTimeout(embedTimeout);
    showEmbedPopover(cell, slug);
  });

  document.addEventListener('mouseout', (e) => {
    const cell = e.target.closest('.td-title[data-slug]');
    if (!cell) return;
    hideEmbedPopover();
  });

  /* ---------- SORT CONTROLS ---------- */
  function initSortControls() {
    const activeButtons = document.querySelectorAll('.sort-btn');
    activeButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        activeButtons.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentSortKey = btn.dataset.sort;
        currentSortDir = btn.dataset.dir;
        activePage = 1;
        gtag('event', 'table_sort', { table: 'active', sort_key: currentSortKey });
        renderActiveTable(activePositions, currentSortKey, currentSortDir);
      });
    });

    const closedButtons = document.querySelectorAll('.closed-sort-btn');
    closedButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        closedButtons.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        closedSortKey = btn.dataset.sort;
        closedSortDir = btn.dataset.dir;
        closedPage = 1;
        gtag('event', 'table_sort', { table: 'closed', sort_key: closedSortKey });
        renderClosedTable(closedPositionsData, closedSortKey, closedSortDir);
      });
    });
  }

  /* ---------- NAVIGATION ---------- */
  function showScreen(screen) {
    entryScreen.classList.add('hidden');
    loadingScreen.classList.add('hidden');
    dashboard.classList.add('hidden');

    screen.classList.remove('hidden');

    if (screen === dashboard) {
      dashboard.querySelectorAll('.anim-card').forEach(el => {
        el.style.animation = 'none';
        el.offsetHeight;
        el.style.animation = '';
      });
    }
  }

  /* ---------- SKELETON HELPERS ---------- */
  function showMetricSkeletons() {
    ['m-total-value', 'm-return-pct', 'm-win-rate', 'm-realized-pnl',
     'm-unrealized-pnl', 'm-rank', 'm-active', 'm-closed'].forEach(id => {
      const el = document.getElementById(id);
      el.textContent = '\u00A0';
      el.classList.add('skeleton-pulse');
      el.className = el.className.replace(/\b(positive|negative)\b/g, '').trim() + ' skeleton-pulse';
    });
  }

  function clearMetricSkeletons() {
    document.querySelectorAll('.metric-value.skeleton-pulse').forEach(el => {
      el.classList.remove('skeleton-pulse');
    });
  }

  function showChartSkeleton(canvasSelector) {
    const canvas = $(canvasSelector);
    if (!canvas) return;
    const wrap = canvas.closest('.chart-wrap') || canvas.parentElement;
    canvas.style.display = 'none';
    if (!wrap.querySelector('.chart-skeleton')) {
      const skel = document.createElement('div');
      skel.className = 'chart-skeleton';
      wrap.appendChild(skel);
    }
  }

  function clearChartSkeleton(canvasSelector) {
    const canvas = $(canvasSelector);
    if (!canvas) return;
    canvas.style.display = '';
    const wrap = canvas.closest('.chart-wrap') || canvas.parentElement;
    const skel = wrap.querySelector('.chart-skeleton');
    if (skel) skel.remove();
  }

  function showTableSkeleton(tbodySelector) {
    const tbody = $(tbodySelector);
    if (!tbody) return;
    const section = tbody.closest('.table-section');
    /* Hide empty state, table content, and controls */
    const emptyEl = section.querySelector('.empty-state');
    if (emptyEl) emptyEl.classList.add('hidden');
    const controls = section.querySelector('.table-controls');
    if (controls) controls.style.display = 'none';
    tbody.closest('.table-scroll').style.display = 'none';
    /* Remove old pagination if present */
    const pag = section.querySelector('.table-pagination');
    if (pag) pag.remove();
    /* Show skeleton */
    let skel = section.querySelector('.table-skeleton');
    if (!skel) {
      skel = document.createElement('div');
      skel.className = 'table-skeleton';
      for (let i = 0; i < 5; i++) {
        const row = document.createElement('div');
        row.className = 'table-skeleton-row';
        skel.appendChild(row);
      }
      section.appendChild(skel);
    }
  }

  function clearTableSkeleton(tbodySelector) {
    const tbody = $(tbodySelector);
    if (!tbody) return;
    const section = tbody.closest('.table-section');
    tbody.closest('.table-scroll').style.display = '';
    const controls = section.querySelector('.table-controls');
    if (controls) controls.style.display = '';
    const skel = section.querySelector('.table-skeleton');
    if (skel) skel.remove();
  }

  function showTimelineSkeleton() {
    const container = $('#activity-timeline');
    container.innerHTML = '';
    const skel = document.createElement('div');
    skel.className = 'table-skeleton';
    for (let i = 0; i < 5; i++) {
      const row = document.createElement('div');
      row.className = 'table-skeleton-row';
      skel.appendChild(row);
    }
    container.appendChild(skel);
    $('#tl-filters').innerHTML = '';
  }

  function showAllocSkeleton() {
    const canvas = $('#chart-alloc-position');
    if (!canvas) return;
    const col = canvas.closest('.alloc-chart-col');
    canvas.style.display = 'none';
    if (!col.querySelector('.chart-skeleton')) {
      const skel = document.createElement('div');
      skel.className = 'chart-skeleton';
      skel.style.borderRadius = '50%';
      col.appendChild(skel);
    }
    $('#alloc-legend').innerHTML = '';
  }

  function clearAllocSkeleton() {
    const canvas = $('#chart-alloc-position');
    if (!canvas) return;
    canvas.style.display = '';
    const col = canvas.closest('.alloc-chart-col');
    const skel = col.querySelector('.chart-skeleton');
    if (skel) skel.remove();
  }

  /* ---------- MAIN FLOW ---------- */
  async function analyze(address) {
    /* Show dashboard immediately with skeletons */
    dashAddr.textContent = address;
    dashAddr.href = `https://polymarket.com/@${address}`;

    destroyCharts();
    chartDefaults();

    lastData = { positions: [], closedPositions: [], trades: [], activity: [], quickValue: null, leaderboard: [] };
    activePositions = [];
    closedPositionsData = [];

    showMetricSkeletons();
    showAllocSkeleton();
    showChartSkeleton('#chart-winners');
    showChartSkeleton('#chart-losers');
    showChartSkeleton('#chart-volume');
    showTimelineSkeleton();
    showTableSkeleton('#active-tbody');
    showTableSkeleton('#closed-tbody');

    showScreen(dashboard);

    /* Track how many critical fetches have arrived for metrics */
    let positionsReady = false;
    let closedReady = false;
    let leaderboardReady = false;
    function tryRenderMetrics() {
      if (!positionsReady || !closedReady || !leaderboardReady) return;
      clearMetricSkeletons();
      const metrics = computeMetrics(lastData);
      renderMetrics(metrics);
    }

    function tryRenderWinnersLosers() {
      if (!positionsReady || !closedReady) return;
      clearChartSkeleton('#chart-winners');
      clearChartSkeleton('#chart-losers');
      if (lastData.positions.length || lastData.closedPositions.length) {
        renderWinnersLosers(lastData.positions, lastData.closedPositions);
      }
    }

    /* Fire all fetches independently */
    const positionsPromise = fetchAllPages('/positions?', address, ENDPOINT_CONFIG.positions).catch(() => []);
    const closedPromise = fetchAllPages('/closed-positions?', address, ENDPOINT_CONFIG.closedPositions).catch(() => []);
    const tradesPromise = fetchAllPages('/trades?', address, ENDPOINT_CONFIG.trades).catch(() => []);
    const activityPromise = fetchAllPages('/activity?', address, ENDPOINT_CONFIG.activity).catch(() => []);
    const leaderboardPromise = fetchJSON(`/v1/leaderboard?user=${address}&timePeriod=ALL`).catch(() => []);

    /* Positions → allocation chart + active table (also needed for metrics + winners/losers) */
    positionsPromise.then(positions => {
      lastData.positions = positions;
      positionsReady = true;

      /* Allocation chart */
      clearAllocSkeleton();
      if (positions.length) {
        renderAllocationByPosition(positions);
      }

      /* Active table */
      activePositions = positions;
      activePage = 1;
      clearTableSkeleton('#active-tbody');
      renderActiveTable(activePositions, currentSortKey, currentSortDir);

      tryRenderMetrics();
      tryRenderWinnersLosers();
    });

    /* Closed positions → closed table (also needed for metrics + winners/losers) */
    closedPromise.then(closedPositions => {
      lastData.closedPositions = closedPositions;
      closedReady = true;

      closedPositionsData = closedPositions;
      closedPage = 1;
      clearTableSkeleton('#closed-tbody');
      renderClosedTable(closedPositionsData, closedSortKey, closedSortDir);

      tryRenderMetrics();
      tryRenderWinnersLosers();
    });

    /* Trades → volume chart */
    tradesPromise.then(trades => {
      lastData.trades = trades;
      clearChartSkeleton('#chart-volume');
      renderTradeVolume(trades);
    });

    /* Activity → timeline */
    activityPromise.then(activity => {
      lastData.activity = activity;
      renderTimeline(activity);
    });

    /* Leaderboard → rank metric */
    leaderboardPromise.then(leaderboard => {
      lastData.leaderboard = leaderboard;
      leaderboardReady = true;
      tryRenderMetrics();
    });

    /* Fetch portfolio value (non-critical) */
    fetchJSON(`/value?user=${address}`).then(v => {
      lastData.quickValue = v;
    }).catch(() => {});

    /* Wait for all to settle; if ALL fail, show error */
    try {
      const results = await Promise.allSettled([
        positionsPromise, closedPromise, tradesPromise, activityPromise, leaderboardPromise
      ]);
      const allFailed = results.every(r => r.status === 'rejected');
      if (allFailed) {
        throw new Error('All API calls failed');
      }
      gtag('event', 'portfolio_load', { address: truncAddr(address) });
    } catch (err) {
      console.error(err);
      showScreen(entryScreen);
      entryError.textContent = 'Failed to load portfolio. Check the address and try again.';
    }
  }

  /* ---------- RECENT LOOKUPS ---------- */
  const RECENT_KEY = 'polyfolio-recent';
  const MAX_RECENT = 5;

  function getRecentLookups() {
    try {
      return JSON.parse(localStorage.getItem(RECENT_KEY)) || [];
    } catch (_) { return []; }
  }

  function saveRecentLookup(address) {
    const recent = getRecentLookups().filter(a => a !== address);
    recent.unshift(address);
    if (recent.length > MAX_RECENT) recent.length = MAX_RECENT;
    try { localStorage.setItem(RECENT_KEY, JSON.stringify(recent)); } catch (_) {}
    updateRecentDatalist();
  }

  const DEMO_ADDRESS = '0x0c9ebc4b65678b9db62fb0573887f04b00b0bba8';

  function updateRecentDatalist() {
    let dl = document.getElementById('recent-addresses');
    if (!dl) {
      dl = document.createElement('datalist');
      dl.id = 'recent-addresses';
      document.body.appendChild(dl);
      addressInput.setAttribute('list', 'recent-addresses');
    }
    const recent = getRecentLookups();
    if (!recent.includes(DEMO_ADDRESS)) recent.push(DEMO_ADDRESS);
    dl.innerHTML = recent
      .map(a => `<option value="${a.slice(2)}">`)
      .join('');
  }

  /* ---------- INPUT VALIDATION ---------- */
  function getFullAddress() {
    const val = addressInput.value.trim();
    if (val.startsWith('0x')) return val;
    return '0x' + val;
  }

  function validateInput() {
    const addr = getFullAddress();
    const valid = ADDR_RE.test(addr);
    loadBtn.disabled = !valid;
    return valid;
  }

  /* ---------- EVENT LISTENERS ---------- */
  addressInput.addEventListener('input', () => {
    entryError.textContent = '';
    validateInput();
  });

  addressInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !loadBtn.disabled) {
      loadBtn.click();
    }
  });

  loadBtn.addEventListener('click', () => {
    const addr = getFullAddress();
    if (!ADDR_RE.test(addr)) {
      entryError.textContent = 'Invalid Ethereum address format.';
      return;
    }
    entryError.textContent = '';
    saveRecentLookup(addr);
    updateURL(addr);
    analyze(addr);
  });

  backBtn.addEventListener('click', () => {
    gtag('event', 'new_lookup');
    destroyCharts();
    activePositions = [];
    closedPositionsData = [];
    lastData = null;
    currentSortKey = 'currentValue';
    currentSortDir = 'desc';
    closedSortKey = 'timestamp';
    closedSortDir = 'desc';

    document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
    const defaultSort = document.querySelector('.sort-btn[data-sort="currentValue"]');
    if (defaultSort) defaultSort.classList.add('active');

    document.querySelectorAll('.closed-sort-btn').forEach(b => b.classList.remove('active'));
    const defaultClosedSort = document.querySelector('.closed-sort-btn[data-sort="timestamp"]');
    if (defaultClosedSort) defaultClosedSort.classList.add('active');

    /* Push clean URL */
    window.location.hash = '';
    showScreen(entryScreen);
  });

  $('#top-brand-link').addEventListener('click', (e) => {
    e.preventDefault();
    backBtn.click();
  });

  /* ---------- URL ROUTING (hash-based) ---------- */

  function getAddressFromURL() {
    const hash = window.location.hash.slice(1);
    if (ADDR_RE.test(hash)) return hash;
    return null;
  }

  function updateURL(address) {
    window.location.hash = address;
  }

  window.addEventListener('hashchange', () => {
    const address = getAddressFromURL();
    if (address) {
      addressInput.value = address.slice(2);
      validateInput();
      analyze(address);
    } else {
      destroyCharts();
      activePositions = [];
      lastData = null;
      showScreen(entryScreen);
    }
  });

  /** On page load, check URL for address */
  function initFromURL() {
    const address = getAddressFromURL();
    if (address) {
      addressInput.value = address.slice(2);
      validateInput();
      analyze(address);
    }
  }

  /* ---------- INIT ---------- */
  initSortControls();
  updateRecentDatalist();
  initFromURL();

})();
