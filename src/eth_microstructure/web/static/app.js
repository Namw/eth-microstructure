const $ = (selector) => document.querySelector(selector);
let replaySource = null;
let replayEvents = 0;
let replayItems = [];
let replayIndex = -1;
let replayPlaying = false;
let replayComplete = false;
let replayRenderFrame = null;

const replayRange = {start: null, end: null};

function toast(message) {
  const element = $("#toast");
  element.textContent = message;
  element.classList.add("show");
  setTimeout(() => element.classList.remove("show"), 2600);
}

function bytes(value) {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const rank = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** rank).toFixed(rank ? 1 : 0)} ${units[rank]}`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {"Content-Type": "application/json"}, ...options});
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail || "请求失败");
  return data;
}

async function refresh() {
  const data = await api("/api/status");
  $("#statusDot").classList.toggle("live", data.collector_running);
  $("#statusText").textContent = data.collector_running ? `采集中 · 网页可管理 · PID ${data.collector_pid}` : "已停止";
  $("#startButton").disabled = data.collector_running;
  $("#restartButton").disabled = !data.collector_running;
  $("#stopButton").disabled = !data.collector_running;
  $("#parquetFiles").textContent = data.parquet_files;
  $("#tradeFiles").textContent = data.trade_files;
  $("#bookFiles").textContent = data.orderbook_files;
  $("#walBytes").textContent = bytes(data.wal_bytes);
  $("#filesTable").innerHTML = data.latest_files.length ? data.latest_files.map(file => `
    <tr><td>${file.stream}${file.active ? " (LIVE)" : ""}</td><td>${file.date} ${String(file.hour).padStart(2, "0")}:00</td><td>${file.rows ?? "ERR"}</td><td>${bytes(file.size)}</td></tr>
  `).join("") : '<tr><td colspan="4">尚无数据</td></tr>';
  $("#logOutput").textContent = data.logs.join("") || "尚无日志";
  $("#logOutput").scrollTop = $("#logOutput").scrollHeight;
}

document.querySelectorAll(".tab").forEach(tab => tab.addEventListener("click", () => {
  document.querySelectorAll(".tab, .panel").forEach(item => item.classList.remove("active"));
  tab.classList.add("active");
  $(`#${tab.dataset.panel}`).classList.add("active");
}));

$("#startButton").addEventListener("click", async () => {
  try { await api("/api/collector/start", {method: "POST"}); toast("Collector 已启动"); await refresh(); }
  catch (error) { toast(error.message); }
});
$("#stopButton").addEventListener("click", async () => {
  try { await api("/api/collector/stop", {method: "POST"}); toast("Collector 已停止，当前 WAL 已落盘"); await refresh(); }
  catch (error) { toast(error.message); }
});
$("#restartButton").addEventListener("click", async () => {
  try { await api("/api/collector/restart", {method: "POST"}); toast("Collector 已安全重启"); await refresh(); }
  catch (error) { toast(error.message); }
});
$("#refreshButton").addEventListener("click", refresh);

$("#inspectForm").addEventListener("submit", async event => {
  event.preventDefault();
  const body = Object.fromEntries(new FormData(event.target));
  body.hour = Number(body.hour); body.limit = Number(body.limit);
  $("#inspectOutput").textContent = "读取中…";
  try { $("#inspectOutput").textContent = JSON.stringify(await api("/api/inspect", {method: "POST", body: JSON.stringify(body)}), null, 2); }
  catch (error) { $("#inspectOutput").textContent = error.message; }
});

$("#validateForm").addEventListener("submit", async event => {
  event.preventDefault();
  const body = Object.fromEntries(new FormData(event.target));
  $("#validateOutput").textContent = "验证中…";
  try {
    const data = await api("/api/validate", {method: "POST", body: JSON.stringify(body)});
    const badge = $("#validationBadge");
    badge.textContent = data.status; badge.className = `badge ${data.status === "PASS" ? "pass" : data.status === "FAIL" ? "fail" : "warning"}`;
    $("#validateOutput").textContent = JSON.stringify(data, null, 2);
  } catch (error) { $("#validateOutput").textContent = error.message; }
});

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function compactPrice(value) {
  const parsed = number(value);
  if (parsed === null) return "—";
  return parsed.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 4});
}

function compactQuantity(value) {
  const parsed = number(value);
  if (parsed === null) return "—";
  return parsed.toLocaleString("en-US", {maximumFractionDigits: 3});
}

function timeLabel(value, seconds = true) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  const text = date.toISOString();
  return seconds ? text.slice(11, 23) : text.slice(0, 16).replace("T", " ");
}

function parseLevels(value) {
  try {
    const levels = typeof value === "string" ? JSON.parse(value) : value;
    if (!Array.isArray(levels)) return [];
    return levels.map(level => [number(level[0]), number(level[1])]).filter(level => level[0] !== null && level[1] !== null);
  } catch (_) {
    return [];
  }
}

function eventBookAt(index) {
  for (let cursor = index; cursor >= 0; cursor -= 1) {
    if (replayItems[cursor].stream === "orderbook") return replayItems[cursor];
  }
  return null;
}

function previousBookAt(index) {
  let foundCurrent = false;
  for (let cursor = index; cursor >= 0; cursor -= 1) {
    if (replayItems[cursor].stream !== "orderbook") continue;
    if (!foundCurrent) { foundCurrent = true; continue; }
    return replayItems[cursor];
  }
  return null;
}

function recentTradesAt(index, limit = 14) {
  const trades = [];
  for (let cursor = index; cursor >= 0 && trades.length < limit; cursor -= 1) {
    if (replayItems[cursor].stream === "trades") trades.push(replayItems[cursor]);
  }
  return trades.reverse();
}

function renderBookSide(selector, levels, side) {
  const visible = levels.slice(0, 10);
  let cumulative = 0;
  const rows = visible.map(([price, quantity]) => {
    cumulative += quantity;
    return {price, quantity, cumulative};
  });
  const maxDepth = Math.max(...rows.map(row => row.cumulative), 1);
  const displayRows = side === "ask" ? [...rows].reverse() : rows;
  $(selector).innerHTML = displayRows.length ? displayRows.map(row => `
    <div class="book-row ${side}" style="--depth:${Math.max(2, row.cumulative / maxDepth * 100)}%">
      <span>${compactPrice(row.price)}</span><span>${compactQuantity(row.quantity)}</span><span>${compactQuantity(row.cumulative)}</span>
    </div>
  `).join("") : '<div class="book-placeholder">当前时点尚无盘口</div>';
}

function renderBook(book) {
  if (!book) {
    renderBookSide("#asksBook", [], "ask"); renderBookSide("#bidsBook", [], "bid");
    ["#midPrice", "#spreadValue", "#imbalanceValue", "#bookSpread", "#bookMid"].forEach(id => $(id).textContent = "—");
    $("#bookUpdateId").textContent = "等待盘口快照";
    return;
  }
  const bids = parseLevels(book.payload.bids);
  const asks = parseLevels(book.payload.asks);
  renderBookSide("#asksBook", asks, "ask");
  renderBookSide("#bidsBook", bids, "bid");
  const bestBid = bids[0]?.[0]; const bestAsk = asks[0]?.[0];
  const mid = bestBid !== undefined && bestAsk !== undefined ? (bestBid + bestAsk) / 2 : null;
  const spread = bestBid !== undefined && bestAsk !== undefined ? bestAsk - bestBid : null;
  const bidDepth = bids.reduce((sum, level) => sum + level[1], 0);
  const askDepth = asks.reduce((sum, level) => sum + level[1], 0);
  const imbalance = bidDepth + askDepth ? (bidDepth - askDepth) / (bidDepth + askDepth) : null;
  $("#midPrice").textContent = compactPrice(mid);
  $("#spreadValue").textContent = spread === null ? "—" : spread.toFixed(2);
  $("#imbalanceValue").textContent = imbalance === null ? "—" : `${imbalance >= 0 ? "+" : ""}${(imbalance * 100).toFixed(1)}%`;
  $("#imbalanceValue").className = imbalance === null ? "" : imbalance >= 0 ? "positive" : "negative";
  $("#bookSpread").textContent = spread === null ? "—" : spread.toFixed(2);
  $("#bookMid").textContent = `MID ${compactPrice(mid)}`;
  $("#bookUpdateId").textContent = `UPDATE #${book.payload.last_update_id ?? "—"}`;
}

function drawPriceChart(trades) {
  const canvas = $("#priceChart");
  const empty = $("#chartEmpty");
  empty.hidden = trades.length > 0;
  const rect = canvas.getBoundingClientRect();
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, rect.width * scale); canvas.height = Math.max(1, rect.height * scale);
  const context = canvas.getContext("2d"); context.scale(scale, scale);
  const width = rect.width; const height = rect.height; const pad = {x: 44, y: 20};
  context.clearRect(0, 0, width, height);
  if (!trades.length) return;
  const prices = trades.map(trade => number(trade.payload.price)).filter(value => value !== null);
  if (!prices.length) return;
  let min = Math.min(...prices); let max = Math.max(...prices);
  const margin = Math.max((max - min) * .18, max * .00004, .01); min -= margin; max += margin;
  const x = index => pad.x + index / Math.max(trades.length - 1, 1) * (width - pad.x - 12);
  const y = price => pad.y + (max - price) / (max - min) * (height - pad.y * 2);
  context.strokeStyle = "rgba(130,144,153,.18)"; context.lineWidth = 1;
  for (let line = 0; line < 4; line += 1) {
    const lineY = pad.y + line / 3 * (height - pad.y * 2);
    context.beginPath(); context.moveTo(pad.x, lineY); context.lineTo(width - 12, lineY); context.stroke();
    context.fillStyle = "#71808a"; context.font = "10px ui-monospace";
    context.fillText((max - line / 3 * (max - min)).toFixed(2), 0, lineY + 3);
  }
  context.strokeStyle = "rgba(238,245,244,.55)"; context.lineWidth = 1.5; context.beginPath();
  trades.forEach((trade, index) => {
    const price = number(trade.payload.price); if (price === null) return;
    if (index === 0) context.moveTo(x(index), y(price)); else context.lineTo(x(index), y(price));
  });
  context.stroke();
  trades.forEach((trade, index) => {
    const price = number(trade.payload.price); if (price === null) return;
    context.beginPath(); context.fillStyle = trade.payload.is_buyer_maker ? "#ff7777" : "#62f2bd";
    context.arc(x(index), y(price), index === trades.length - 1 ? 4 : 2.5, 0, Math.PI * 2); context.fill();
  });
}

function renderTrades(trades) {
  const recent = trades.slice(-10).reverse();
  $("#tradeTape").innerHTML = recent.length ? recent.map(trade => {
    const sell = trade.payload.is_buyer_maker;
    return `<div class="tape-row ${sell ? "sell" : "buy"}"><strong>${sell ? "SELL" : "BUY"}</strong><span>${compactPrice(trade.payload.price)}</span><span>${compactQuantity(trade.payload.quantity)}</span><span>${timeLabel(trade.event_time)}</span></div>`;
  }).join("") : '<div class="tape-empty">当前时点尚无成交</div>';
  const last = trades.at(-1);
  $("#lastTrade").textContent = last ? `${last.payload.is_buyer_maker ? "卖" : "买"} ${compactPrice(last.payload.price)}` : "—";
  drawPriceChart(trades);
}

function changedLevels(current, previous) {
  if (!previous) return null;
  const countSide = side => {
    const before = new Map(parseLevels(previous.payload[side]).map(level => [level[0], level[1]]));
    const after = new Map(parseLevels(current.payload[side]).map(level => [level[0], level[1]]));
    return new Set([...before.keys(), ...after.keys()]).size
      ? [...new Set([...before.keys(), ...after.keys()])].filter(price => before.get(price) !== after.get(price)).length : 0;
  };
  return {bids: countSide("bids"), asks: countSide("asks")};
}

function renderEvent(item, index) {
  $("#eventSequence").textContent = `#${index + 1}`;
  $("#replayOutput").textContent = JSON.stringify(item.payload, null, 2);
  const facts = [];
  if (item.stream === "trades") {
    const side = item.payload.is_buyer_maker ? "主动卖出" : "主动买入";
    const quote = (number(item.payload.price) ?? 0) * (number(item.payload.quantity) ?? 0);
    $("#eventNarrative").innerHTML = `<span class="narrative-tag ${item.payload.is_buyer_maker ? "sell" : "buy"}">TRADE</span><strong>${side} ${compactQuantity(item.payload.quantity)} ETH</strong><p>成交价 ${compactPrice(item.payload.price)} USDT，名义金额约 ${compactQuantity(quote)} USDT。</p>`;
    facts.push(["成交 ID", item.payload.aggregate_trade_id ?? "—"], ["方向", side], ["成交时间", timeLabel(item.payload.trade_time)]);
  } else {
    const changes = changedLevels(item, previousBookAt(index));
    const bids = parseLevels(item.payload.bids); const asks = parseLevels(item.payload.asks);
    const bestBid = bids[0]?.[0]; const bestAsk = asks[0]?.[0];
    $("#eventNarrative").innerHTML = `<span class="narrative-tag book">BOOK</span><strong>盘口快照更新</strong><p>${changes ? `买盘 ${changes.bids} 档、卖盘 ${changes.asks} 档数量发生变化。` : "这是当前区间的首个盘口状态。"}</p>`;
    facts.push(["最优买价", compactPrice(bestBid)], ["最优卖价", compactPrice(bestAsk)], ["更新 ID", item.payload.last_update_id ?? "—"]);
  }
  $("#eventFacts").innerHTML = facts.map(([label, value]) => `<div><dt>${label}</dt><dd>${value}</dd></div>`).join("");
}

function updateReplayStatus(text, active = false) {
  $("#replayStatus").textContent = text;
  $("#replayStatusDot").classList.toggle("active", active);
}

function scheduleReplayRender() {
  if (replayRenderFrame !== null) return;
  replayRenderFrame = requestAnimationFrame(() => { replayRenderFrame = null; renderReplayAt(replayIndex); });
}

function renderReplayAt(index) {
  if (!replayItems.length) return;
  replayIndex = Math.max(0, Math.min(index, replayItems.length - 1));
  const item = replayItems[replayIndex];
  const book = eventBookAt(replayIndex); const trades = recentTradesAt(replayIndex, 120);
  renderBook(book); renderTrades(trades); renderEvent(item, replayIndex);
  $("#marketTime").textContent = new Date(item.event_time).toISOString().replace("T", " ").replace("Z", " UTC");
  $("#timelineCurrent").textContent = timeLabel(item.event_time, false);
  $("#replayTimeline").max = String(Math.max(replayItems.length - 1, 0));
  $("#replayTimeline").value = String(replayIndex);
  $("#eventPosition").textContent = `${replayIndex + 1} / ${replayItems.length}`;
  $("#replayCount").textContent = `${replayItems.length.toLocaleString()} 个事件`;
}

function revealReplayWorkspace() {
  $("#replayEmpty").hidden = true; $("#replayWorkspace").hidden = false; $("#replayTransport").hidden = false;
}

$("#replayForm").addEventListener("submit", event => {
  event.preventDefault();
  if (replaySource) replaySource.close();
  const values = Object.fromEntries(new FormData(event.target));
  replayRange.start = new Date(`${values.start}Z`); replayRange.end = new Date(`${values.end}Z`);
  values.start = `${values.start}Z`; values.end = `${values.end}Z`;
  replayEvents = 0; replayItems = []; replayIndex = -1; replayPlaying = true; replayComplete = false;
  $("#replayCount").textContent = "0 个事件"; $("#replayOutput").textContent = "{}";
  $("#replayEmpty").hidden = false; $("#replayWorkspace").hidden = true; $("#replayTransport").hidden = true;
  $("#toggleReplay").textContent = "暂停";
  $("#timelineStart").textContent = timeLabel(replayRange.start, false); $("#timelineEnd").textContent = timeLabel(replayRange.end, false);
  updateReplayStatus("正在载入事件", true);
  replaySource = new EventSource(`/api/replay?${new URLSearchParams(values)}`);
  replaySource.onmessage = message => {
    const eventData = JSON.parse(message.data); replayEvents += 1; replayItems.push(eventData);
    if (replayItems.length === 1) revealReplayWorkspace();
    if (replayPlaying) { replayIndex = replayItems.length - 1; scheduleReplayRender(); }
    else { $("#replayCount").textContent = `${replayItems.length.toLocaleString()} 个事件`; $("#eventPosition").textContent = `${replayIndex + 1} / ${replayItems.length}`; }
  };
  replaySource.addEventListener("complete", () => {
    replaySource.close(); replaySource = null; replayComplete = true;
    if (!replayItems.length) { updateReplayStatus("该时段没有事件"); toast("所选时段没有可回放数据"); return; }
    updateReplayStatus(replayPlaying ? "回放完成" : "载入完成 · 已暂停"); toast(`已载入 ${replayItems.length.toLocaleString()} 个事件`);
  });
  replaySource.onerror = () => { if (replaySource) { replaySource.close(); replaySource = null; updateReplayStatus("连接已中断"); } };
});
$("#stopReplay").addEventListener("click", () => {
  if (replaySource) replaySource.close(); replaySource = null; replayPlaying = false;
  $("#toggleReplay").textContent = "继续"; updateReplayStatus("已停止"); toast("Replay 已停止，已载入状态仍可检查");
});
$("#toggleReplay").addEventListener("click", () => {
  replayPlaying = !replayPlaying; $("#toggleReplay").textContent = replayPlaying ? "暂停" : "继续";
  if (replayPlaying && replayIndex < replayItems.length - 1) { replayIndex = replayItems.length - 1; scheduleReplayRender(); }
  updateReplayStatus(replayPlaying ? (replayComplete ? "回放完成" : "正在播放") : (replayComplete ? "载入完成 · 已暂停" : "已暂停 · 后台载入"), replayPlaying && !replayComplete);
});
$("#stepBack").addEventListener("click", () => { replayPlaying = false; $("#toggleReplay").textContent = "继续"; updateReplayStatus("单步检查"); renderReplayAt(replayIndex - 1); });
$("#stepForward").addEventListener("click", () => { replayPlaying = false; $("#toggleReplay").textContent = "继续"; updateReplayStatus("单步检查"); renderReplayAt(replayIndex + 1); });
$("#replayTimeline").addEventListener("input", event => { replayPlaying = false; $("#toggleReplay").textContent = "继续"; updateReplayStatus("时间轴定位"); renderReplayAt(Number(event.target.value)); });
window.addEventListener("resize", () => { if (replayIndex >= 0) scheduleReplayRender(); });

const utcNow = new Date();
const dateValue = utcNow.toISOString().slice(0, 10);
document.querySelectorAll('input[name="date"]').forEach(input => input.value = dateValue);
$("#inspectForm input[name='hour']").value = utcNow.getUTCHours();
setInterval(() => refresh().catch(() => {}), 5000);
refresh().catch(error => toast(error.message));
