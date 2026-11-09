"""Compact state and self-contained HTML for the daily portfolio widget."""

import json
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from financetracker.bot.intraday_daily_push import _pct, _rub, _stamp
from financetracker.bot.runtime import TZ, db_session
from financetracker.bot.today_report import build_today_change, load_today_inputs
from financetracker.database.models import IntradayPortfolioSnapshot


def build_today_widget_data() -> str:
    with db_session() as session:
        inputs = load_today_inputs(session)
        if isinstance(inputs, str):
            return _encode({"notice": inputs})
        day, events = inputs
        current = day.snapshots[-1]
        market_open = datetime.combine(day.observed_date, time(10), ZoneInfo("Europe/Moscow"))
        model = IntradayPortfolioSnapshot
        observations = (
            session.query(model.snapshot_at, model.total_value)
            .filter(
                model.account_id == day.account_id,
                model.snapshot_date == day.observed_date,
                model.snapshot_at >= market_open.astimezone(timezone.utc),
                model.snapshot_at <= current.observed_at,
                model.currency == current.currency,
                model.total_value.isnot(None),
            )
            .order_by(model.snapshot_at, model.id)
            .all()
        )

    line, stops = _chart(observations)
    stamp = _stamp(current.observed_at, TZ)
    current_time = stamp.split()[1]
    state = {
        "value": _rub(current.total_value, precision=2),
        "stamp": stamp,
        "line": line,
        "stops": [stop for stop in stops if stop[2] != current_time],
    }
    if day.baseline is None:
        return _encode({**state, "notice": "Нет предыдущей оценки для сравнения."})
    if day.baseline.currency.upper() != current.currency.upper():
        return _encode({**state, "notice": "Оценки портфеля в разных валютах."})

    report = build_today_change(day, events)
    delta = report["change"]
    state.update(
        change=f"{_rub(delta, signed=True, precision=2)} ({_pct(report['market_pct'])})",
        direction="up" if delta > 0 else "down" if delta < 0 else "flat",
        parts=[
            [key, _rub(amount, signed=True, precision=2)]
            for key, amount in report["reconciliation"]["components"].items() if amount
        ],
    )
    if any(event["kind"] == "unsupported_currency" for event in events):
        state["notice"] = "Есть операции в другой валюте. Подробности в /today."
    return _encode(state)


def _chart(observations) -> tuple[str, list]:
    if len(observations) < 2:
        return "", []
    start = observations[0][0]
    span = (observations[-1][0] - start).total_seconds()
    if span <= 0:
        return "", []
    low = min(value for _, value in observations)
    high = max(value for _, value in observations)
    count = len(observations)
    points = []
    stops = []
    stop_indices = {0, count // 3, 2 * count // 3, count - 1}
    for index, (stamp, value) in enumerate(observations):
        x = 6 + round(288 * (stamp - start).total_seconds() / span)
        y = 41 if low == high else 70 - round(58 * (value - low) / (high - low))
        points.append(f"{x},{y}")
        if index in stop_indices:
            label = _stamp(stamp, TZ).split()[1]
            if not stops or stops[-1][2] != label:
                stops.append([x, y, label, _rub(value, precision=2)])
    return " ".join(points), stops


def _encode(state) -> str:
    data = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    if len(data.encode("utf-8")) <= 1024:
        return data

    points = state.get("line", "").split()
    compact = {**state, "line": ""}
    base = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
    # A pair uses at most 6 ASCII bytes, plus a separator between pairs.
    if len(base.encode("utf-8")) + (13 if points else 0) > 1024:
        compact["stops"] = []
        base = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
    remaining = 1024 - len(base.encode("utf-8"))
    if remaining < 0:
        raise ValueError("Today widget data exceeds Telegram's limit")
    count = min((remaining + 1) // 7, len(points))
    if count < 2:
        return base
    endpoints = {0, len(points) - 1}
    anchors = endpoints | {
        points.index(f"{x},{y}") for x, y, *_ in compact.get("stops", [])
        if f"{x},{y}" in points
    }
    if len(anchors) > count:
        compact["stops"] = []
        anchors = endpoints
    available = [index for index in range(len(points)) if index not in anchors]
    slots = count - len(anchors)
    anchors.update(available[(2 * index + 1) * len(available) // (2 * slots)] for index in range(slots))
    compact["line"] = " ".join(points[index] for index in sorted(anchors))
    return json.dumps(compact, ensure_ascii=False, separators=(",", ":"))


TODAY_WIDGET_HTML = """<style>
  body { margin:0; font:14px system-ui,sans-serif; color:#fff; background:#173877; }
  .panel { padding:16px 16px 8px; background:linear-gradient(150deg,#172a67,#235daa 68%,#287d9d); }
  header { display:flex; justify-content:space-between; gap:6px; align-items:baseline; flex-wrap:wrap; }
  header span, #notice { color:#e0ebff; font-size:11px; }
  #value { margin:8px 0 0; font-size:28px; line-height:1.05; font-weight:700; letter-spacing:-.8px;
           font-variant-numeric:tabular-nums; overflow-wrap:anywhere; }
  #change { margin:5px 0 0; min-height:17px; color:#e0ebff; font-weight:600; }
  #change.up { color:#a6ffe1; }
  #change.down { color:#ffd2db; }
  #notice { margin:6px 0 0; }
  #plot, #parts { height:126px; margin:8px 0 0; }
  #chart { display:block; width:100%; height:82px; margin:0 0 2px; }
  #chart-note { margin:0; color:#e0ebff; font-size:12px; }
  #times { display:flex; gap:2px; background:#ffffff19; padding:3px; border-radius:12px; }
  button { padding:0; border:0; background:transparent; color:inherit; font:inherit; }
  #times button { flex:1; min-width:0; min-height:32px; font-size:11px; border-radius:9px; }
  #times button.active { background:#ffffff33; font-weight:700; }
  #details { display:inline-flex; align-items:center; gap:8px; min-height:36px; margin:3px 0 4px;
             padding:0 11px; border:1px solid #ffffff52; border-radius:10px;
             background:#ffffff19; font-size:12px; font-weight:600; }
  #details:active { background:#ffffff30; }
  #details-arrow { color:#cce9ff; font-size:17px; line-height:1; }
  #parts p { display:flex; justify-content:space-between; gap:8px; margin:0; padding:4px 0;
             border-top:1px solid #ffffff33; font-size:12px; }
  #parts span { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  #parts strong { text-align:right; white-space:nowrap; }
  #next { min-height:26px; width:100%; text-align:right; font-size:12px; }
  .is-hidden, #chart.is-hidden, #times.is-hidden, #details.is-hidden,
  #parts p.is-hidden { display:none; }
  @media (prefers-reduced-motion: no-preference) {
    .panel { animation:appear .26s ease-out; }
    #times button { transition:background-color .16s ease; }
    @keyframes appear { from { opacity:.72; } to { opacity:1; } }
  }
</style>
<div class="panel">
  <header><strong>Портфель · Сегодня</strong><span id="stamp"></span></header>
  <p id="value" class="is-hidden"></p>
  <p id="change" class="is-hidden"></p>
  <p id="notice">Загрузка данных…</p>
  <section id="plot" class="is-hidden">
  <p id="chart-note" class="is-hidden">Пока нет точек для графика</p>
  <svg id="chart" viewBox="0 0 300 82" preserveAspectRatio="none"
       role="img" aria-label="Стоимость портфеля за день" class="is-hidden">
    <defs><linearGradient id="area-ink" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop stop-color="#93ffe3" stop-opacity=".43"></stop>
      <stop offset="1" stop-color="#93ffe3" stop-opacity="0"></stop>
    </linearGradient></defs>
    <path d="M0 26H300 M0 52H300 M0 78H300" stroke="#ffffff" opacity=".19" fill="none"></path>
    <polygon id="area" fill="url(#area-ink)"></polygon>
    <polyline id="line" fill="none" stroke="#a4ffe8" stroke-width="2.5"
              stroke-linecap="round" stroke-linejoin="round"></polyline>
    <g id="marker" class="is-hidden">
      <line id="cursor" y1="8" y2="78" stroke="#ffffff" opacity=".5"></line>
      <circle id="dot" r="4" fill="#baffed" stroke="#245fa5" stroke-width="1.5"></circle>
    </g>
  </svg>
  <nav id="times" aria-label="Внутридневные снимки" class="is-hidden"></nav>
  </section>
  <section id="parts" class="is-hidden">
    <p id="row0" class="is-hidden"><span id="label0"></span><strong id="amount0"></strong></p>
    <p id="row1" class="is-hidden"><span id="label1"></span><strong id="amount1"></strong></p>
    <p id="row2" class="is-hidden"><span id="label2"></span><strong id="amount2"></strong></p>
    <p id="row3" class="is-hidden"><span id="label3"></span><strong id="amount3"></strong></p>
    <button id="next" type="button" class="is-hidden">Далее</button>
  </section>
  <button id="details" type="button" aria-controls="parts" aria-expanded="false" class="is-hidden">
    <span id="details-label">Из чего сложилось</span><span id="details-arrow" aria-hidden="true">›</span>
  </button>
</div>
<script>
  const widget = TelegramWidget;
  const byId = id => document.getElementById(id);
  const details = byId("details"), parts = byId("parts"), next = byId("next");
  const labels = {
    prices:"Цены бумаг", accrual:"Накопление НКД", coupon_reset:"Снижение НКД",
    coupon:"Купоны", dividend:"Дивиденды", deduction:"Вычет ИИС",
    commission:"Комиссии", tax:"Налоги", tax_refund:"Возврат налога", other:"Остаток"
  };
  const times = byId("times"), change = byId("change"), marker = byId("marker");
  const cursor = byId("cursor"), dot = byId("dot");
  let expanded = false, selected = null, page = 0, data = {};
  function setVisible(element, visible) {
    element.classList.toggle("is-hidden", !visible);
  }
  function showText(id, text) {
    const element = byId(id);
    element.textContent = text || "";
    setVisible(element, !!text);
  }
  function showDetails() {
    const rows = data.parts || [];
    const pages = Math.ceil(rows.length / 4);
    page = Math.min(page, Math.max(0, pages - 1));
    const visible = expanded && pages > 0;
    setVisible(byId("plot"), !visible && (!!data.line || pages > 0));
    setVisible(parts, visible);
    setVisible(details, pages > 0);
    details.setAttribute("aria-expanded", String(visible));
    byId("details-label").textContent = visible ? "К графику" : "Из чего сложилось";
    byId("details-arrow").textContent = visible ? "‹" : "›";
    for (let index = 0; index < 4; index++) {
      const row = rows[page * 4 + index];
      setVisible(byId("row" + index), !!row);
      byId("label" + index).textContent = row ? labels[row[0]] : "";
      byId("amount" + index).textContent = row ? row[1] : "";
    }
    setVisible(next, pages > 1);
    next.textContent = "Далее · " + (page + 1) + "/" + pages;
  }
  function selectPoint(data, point) {
    selected = point ? point[2] : null;
    showText("stamp", data.stamp);
    showText("value", point ? point[3] : data.value);
    showText("change", point ? "Снимок в " + point[2] : data.change && "Изменение " + data.change);
    change.classList.toggle("up", !point && data.direction === "up");
    change.classList.toggle("down", !point && data.direction === "down");
    setVisible(marker, !!point);
    if (point) {
      cursor.setAttribute("x1", point[0]);
      cursor.setAttribute("x2", point[0]);
      dot.setAttribute("cx", point[0]);
      dot.setAttribute("cy", point[1]);
    }
    for (const button of times.children) {
      const active = button.textContent === (selected || "Сейчас");
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    }
  }
  function render() {
    data = JSON.parse(widget.chatData || "{}");
    showText("notice", data.notice);
    setVisible(byId("chart-note"), !data.line);
    setVisible(byId("chart"), !!data.line);
    byId("line").setAttribute("points", data.line || "");
    if (data.line) byId("area").setAttribute("points", "6,82 " + data.line + " 294,82");
    const stops = data.stops || [];
    times.textContent = "";
    setVisible(times, stops.length > 0);
    if (stops.length) {
      for (const point of [...stops, null]) {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = point ? point[2] : "Сейчас";
        button.addEventListener("click", () => selectPoint(data, point));
        times.appendChild(button);
      }
    }
    selectPoint(data, stops.find(point => point[2] === selected));
    showDetails();
  }
  details.addEventListener("click", () => {
    expanded = !expanded;
    page = 0;
    if (expanded) selectPoint(data);
    showDetails();
  });
  next.addEventListener("click", () => {
    page = (page + 1) % Math.ceil(data.parts.length / 4);
    showDetails();
  });
  widget.onEvent("chatDataChanged", render);
  render();
  widget.ready();
</script>"""
