#!/usr/bin/env python3
"""
RUNNER IBKR TRIPLE SHADOW v1.2.4 — C TELEMETRY + FRESH-SIGNAL GUARD

A = legacy Helsinki 11:00-15:59 (preserved comparison)
B = legacy Helsinki 13:00-14:59 (preserved comparison)
C = confirmed Exness/UTC 11:00-15:59 decision time
    (Puerto Rico 07:00-11:59)

OBSERVER / DISARMED / NO ORDERS.
No broker orders. Signals use completed-bar MIDPOINT close. Virtual trades use
the observed spread when available and completed M15 OHLC for management.
"""
from __future__ import annotations

import csv
import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.wrapper import EWrapper

from telemetria_ibkr import Telemetria

# ---------------------------------------------------------------------------
# FROZEN STRATEGY CONSTANTS — RUNNER v4.6
# ---------------------------------------------------------------------------
TF_MINUTES = 15
HTF_HOURS = 4
HTF_EMA_PERIOD = 50
RSI_PERIOD = 14
ATR_PERIOD = 14
MIN_DIST_FROM_EMA_ATR = 0.5
BASELINE_TZ = ZoneInfo("Europe/Helsinki")
BASELINE_START_HOUR = 11
BASELINE_END_HOUR = 16
CANDIDATE_B_START_HOUR = 13
CANDIDATE_B_END_HOUR = 15
CONFIRMED_UTC_START_HOUR = 11
CONFIRMED_UTC_END_HOUR = 16
USE_SUNDAY_FILTER = True
MAX_TRADES_PER_DAY = 3
SL_ATR_MULT = 1.2
ACTIVATE_ATR = 3.0
LOCK_PROFIT_ATR = 2.0
TRAIL_ATR = 2.5
RISK_PERCENT = 1.0
MAX_ALLOWED_RISK_PERCENT = 4.0

# Telemetry is observational and is restricted to strategy C. One virtual
# XAUUSD unit represents one troy ounce, equivalent to 0.01 standard XAU lot.
TELEMETRY_ENABLED = os.getenv("RUNNER_C_TELEMETRY_ENABLED", "1") == "1"
VIRTUAL_XAU_OZ = float(os.getenv("RUNNER_C_VIRTUAL_XAU_OZ", "1.0"))
VIRTUAL_CAPITAL_RAW = os.getenv("RUNNER_C_VIRTUAL_CAPITAL", "").strip()
VIRTUAL_CAPITAL = float(VIRTUAL_CAPITAL_RAW) if VIRTUAL_CAPITAL_RAW else None

# ---------------------------------------------------------------------------
# IBKR / runtime
# ---------------------------------------------------------------------------
HOST = os.getenv("RUNNER_IBKR_HOST", "127.0.0.1")
PORT = int(os.getenv("RUNNER_IBKR_PORT", "7497"))
CLIENT_ID = int(os.getenv("RUNNER_IBKR_CLIENT_ID", "2461"))
MARKET_DATA_TYPE = int(os.getenv("RUNNER_IBKR_MARKET_DATA_TYPE", "1"))
LOOP_SECONDS = int(os.getenv("RUNNER_IBKR_LOOP_SECONDS", "10"))
HEARTBEAT_SECONDS = int(os.getenv("RUNNER_IBKR_HEARTBEAT_SECONDS", "300"))
RECONNECT_SECONDS = int(os.getenv("RUNNER_IBKR_RECONNECT_SECONDS", "30"))
RECONNECT_TIMEOUT_SECONDS = int(os.getenv("RUNNER_IBKR_RECONNECT_TIMEOUT", "15"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("RUNNER_IBKR_HISTORY_TIMEOUT", "30"))
ENTRY_MAX_AGE_SECONDS = int(os.getenv("RUNNER_IBKR_ENTRY_MAX_AGE_SECONDS", "180"))
MAX_ATTEMPTS = int(os.getenv("RUNNER_IBKR_HISTORY_ATTEMPTS", "3"))
BACKOFF_SECONDS = (2, 5)
M15_DURATION = os.getenv("RUNNER_IBKR_M15_DURATION", "5 D")
H4_DURATION = os.getenv("RUNNER_IBKR_H4_DURATION", "30 D")
OUTPUT_DIR = Path(os.getenv("RUNNER_IBKR_OUTPUT_DIR", str(Path.home() / "runner_ibkr_dual_observer_v1_logs")))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = OUTPUT_DIR / "runner_ibkr_dual_observer_state_v1_1.json"
EVENTS_CSV = OUTPUT_DIR / "runner_ibkr_dual_observer_events_v1_1.csv"
SIGNALS_CSV = OUTPUT_DIR / "runner_ibkr_triple_shadow_signals_v1_2.csv"
SHADOW_EVENTS_CSV = OUTPUT_DIR / "runner_ibkr_triple_shadow_events_v1_2.csv"
SHADOW_TRADES_CSV = OUTPUT_DIR / "runner_ibkr_triple_shadow_trades_v1_2.csv"
STATE_V12_PATH = OUTPUT_DIR / "runner_ibkr_triple_shadow_state_v1_2.json"

@dataclass(frozen=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float

@dataclass
class HistRequest:
    req_id: int
    timeframe: str
    event: threading.Event
    buffer: List[Bar]
    active: bool = True
    completed: bool = False


@dataclass
class VirtualTrade:
    strategy: str
    direction: str
    entry_time: str
    entry: float
    atr: float
    stop: float
    max_favorable: float
    activated: bool = False
    position_id: Optional[int] = None
    spread: Optional[float] = None
    telemetry_open_sent: bool = False


def xau_contract() -> Contract:
    c = Contract()
    c.symbol = "XAUUSD"
    c.secType = "CMDTY"
    c.exchange = "SMART"
    c.currency = "USD"
    c.conId = 69067924
    return c


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    out = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    for v in values[period:]:
        out = v * k + out * (1.0 - k)
    return out


def wilder_rsi(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if len(values) <= period:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag, al = sum(gains) / period, sum(losses) / period
    def calc(g, l):
        if l == 0:
            return 100.0 if g > 0 else 50.0
        rs = g / l
        return 100.0 - 100.0 / (1.0 + rs)
    out[period] = calc(ag, al)
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        ag = ((ag * (period - 1)) + max(d, 0.0)) / period
        al = ((al * (period - 1)) + max(-d, 0.0)) / period
        out[i] = calc(ag, al)
    return out


def wilder_atr(bars: List[Bar], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(bars)
    if len(bars) <= period:
        return out
    tr: List[Optional[float]] = [None]
    for i in range(1, len(bars)):
        h, l, pc = bars[i].high, bars[i].low, bars[i - 1].close
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    seed = sum(float(x) for x in tr[1:period + 1] if x is not None) / period
    out[period] = seed
    prev = seed
    for i in range(period + 1, len(bars)):
        prev = ((prev * (period - 1)) + float(tr[i])) / period
        out[i] = prev
    return out


def is_completed_bar(bar: Bar, now_utc: datetime, minutes: int) -> bool:
    return bar.ts + timedelta(minutes=minutes) <= now_utc


def baseline_session_pass(ts_utc: datetime) -> bool:
    local = ts_utc.astimezone(BASELINE_TZ)
    if USE_SUNDAY_FILTER and local.weekday() == 6:
        return False
    return BASELINE_START_HOUR <= local.hour < BASELINE_END_HOUR


def candidate_b_hour_pass(ts_utc: datetime) -> bool:
    local = ts_utc.astimezone(BASELINE_TZ)
    return CANDIDATE_B_START_HOUR <= local.hour < CANDIDATE_B_END_HOUR


def confirmed_utc_session_pass(ts_utc: datetime) -> bool:
    if USE_SUNDAY_FILTER and ts_utc.weekday() == 6:
        return False
    return CONFIRMED_UTC_START_HOUR <= ts_utc.hour < CONFIRMED_UTC_END_HOUR


def next_boundary(now_utc: datetime, minutes: int) -> datetime:
    step = minutes * 60
    epoch = int(now_utc.timestamp())
    return datetime.fromtimestamp(((epoch // step) + 1) * step, tz=timezone.utc)


class RunnerDualObserver(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.connected_event = threading.Event()
        self.managed_accounts_event = threading.Event()
        self.managed_accounts: List[str] = []
        self.contract_event = threading.Event()
        self.contract_details = []
        self.stop_requested = False
        self._connection_lock = threading.Lock()
        self._next_reconnect_monotonic = 0.0
        self._disconnect_announced = False
        self._has_connected_once = False

        self.m15_cache: List[Bar] = []
        self.h4_cache: List[Bar] = []
        self._req_lock = threading.Lock()
        self._requests: Dict[int, HistRequest] = {}
        self._next_req_id = 5000

        self.bid: Optional[float] = None
        self.ask: Optional[float] = None
        self.market_req_id = 4001
        self.market_data_started = False
        self.quote_event = threading.Event()
        self.quote_requests = 0
        self.quote_failures = 0

        self.last_processed_bar: Optional[str] = None
        self.processed_bar_keys: set[str] = set()
        self.forward_start_utc: Optional[str] = None
        self.signal_counts_a: Dict[str, int] = {}
        self.signal_counts_b: Dict[str, int] = {}
        self.signal_counts_c: Dict[str, int] = {}
        self.virtual_positions: Dict[str, VirtualTrade] = {}
        self.virtual_trade_counts: Dict[str, Dict[str, int]] = {"A": {}, "B": {}, "C": {}}
        self.parity_start_utc: Optional[str] = None
        self.telemetry: Optional[Telemetria] = None
        self.telemetry_realized_usd = 0.0
        self.telemetry_signal_counts_c: Dict[str, int] = {}
        # A new virtual position may only be created from a bar completed
        # after this process started and evaluated while still fresh. Historical
        # catch-up remains available for indicators and position management.
        self.live_entry_cutoff_utc: Optional[datetime] = None

        self.next_m15_refresh_due: Optional[datetime] = None
        self.next_h4_refresh_due: Optional[datetime] = None
        self.m15_refresh_failed = False
        self.h4_refresh_failed = False
        self.last_heartbeat_monotonic = 0.0

        self.stats = {
            "M15_REQUESTS": 0, "M15_TIMEOUTS": 0, "M15_RETRIES": 0, "M15_RECOVERIES": 0,
            "H4_REQUESTS": 0, "H4_TIMEOUTS": 0, "H4_RETRIES": 0, "H4_RECOVERIES": 0,
            "LATE_RESPONSES_IGNORED": 0, "DUPLICATE_BARS_BLOCKED": 0, "FARM_DEGRADED": 0,
        }
        self.load_state()

    # ---------------- IB callbacks ----------------
    def nextValidId(self, orderId: int):
        print(f"CONNECTED = True | NEXT_VALID_ID = {orderId}", flush=True)
        print("MODE = OBSERVER / DISARMED / NO ORDERS", flush=True)
        self.connected_event.set()
        self._disconnect_announced = False
        self._has_connected_once = True
        if self.telemetry is not None:
            self.telemetry.conexion(True)

    def managedAccounts(self, accountsList: str):
        self.managed_accounts = [x.strip() for x in accountsList.split(",") if x.strip()]
        self.managed_accounts_event.set()

    def connectionClosed(self):
        self._mark_disconnected("connectionClosed")

    def error(self, reqId, *args):
        error_time = None
        if len(args) >= 4:
            error_time, errorCode, errorString = args[0], args[1], args[2]
        elif len(args) >= 2:
            errorCode, errorString = args[0], args[1]
        else:
            print(f"IBKR ERROR CALLBACK (unparsed) reqId={reqId} args={args!r}", flush=True)
            return
        try:
            code = int(errorCode)
        except Exception:
            code = -999999
        suffix = f" (errorTime={error_time})" if error_time is not None else ""
        msg = f"ERROR {reqId} {errorCode}: {errorString}{suffix}"
        if code == 2103:
            self.stats["FARM_DEGRADED"] += 1
            print(f"FARM DEGRADED = {msg}", flush=True)
            self.write_event("FARM_DEGRADED", detail=msg)
            self.save_state()
        elif code in {2104, 2106, 2107, 2108, 2158}:
            print(msg, flush=True)
        else:
            print(msg, flush=True)
            self.write_event("IBKR_ERROR", detail=msg)
        if code in {504, 1100, 1300}:
            self._mark_disconnected(f"IBKR error {code}")
        elif code in {1101, 1102}:
            # IBKR restored connectivity. Force both caches to catch up.
            self.m15_refresh_failed = True
            self.h4_refresh_failed = True
            self.next_m15_refresh_due = datetime.now(timezone.utc)
            self.next_h4_refresh_due = datetime.now(timezone.utc)

    def contractDetails(self, reqId, details):
        self.contract_details.append(details)

    def contractDetailsEnd(self, reqId: int):
        self.contract_event.set()

    def historicalData(self, reqId, bar):
        with self._req_lock:
            req = self._requests.get(reqId)
            if req is None or not req.active:
                self.stats["LATE_RESPONSES_IGNORED"] += 1
                return
        try:
            raw = str(bar.date).strip()
            if raw.isdigit():
                ts = datetime.fromtimestamp(int(raw), tz=timezone.utc)
            else:
                parts = raw.split()
                ts = datetime.strptime(" ".join(parts[:2]), "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
            b = Bar(ts, float(bar.open), float(bar.high), float(bar.low), float(bar.close))
        except Exception as exc:
            print(f"BAR PARSE WARNING reqId={reqId}: {getattr(bar,'date',None)!r} | {exc}", flush=True)
            return
        with self._req_lock:
            req = self._requests.get(reqId)
            if req is None or not req.active:
                self.stats["LATE_RESPONSES_IGNORED"] += 1
                return
            req.buffer.append(b)

    def historicalDataEnd(self, reqId, start, end):
        with self._req_lock:
            req = self._requests.get(reqId)
            if req is None or not req.active:
                self.stats["LATE_RESPONSES_IGNORED"] += 1
                return
            req.completed = True
            req.event.set()

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId != self.market_req_id or price <= 0:
            return
        if tickType == TickTypeEnum.BID:
            self.bid = float(price)
            self.quote_event.set()
        elif tickType == TickTypeEnum.ASK:
            self.ask = float(price)
            self.quote_event.set()

    # ---------------- persistence ----------------
    def load_state(self):
        state_path = STATE_V12_PATH if STATE_V12_PATH.exists() else STATE_PATH
        if not state_path.exists():
            return
        try:
            d = json.loads(state_path.read_text(encoding="utf-8"))
            self.last_processed_bar = d.get("last_processed_bar")
            self.processed_bar_keys = set(d.get("processed_bar_keys", []))
            self.forward_start_utc = d.get("forward_start_utc")
            self.signal_counts_a = d.get("signal_counts_a", {})
            self.signal_counts_b = d.get("signal_counts_b", {})
            self.signal_counts_c = d.get("signal_counts_c", {})
            self.virtual_trade_counts = d.get("virtual_trade_counts", {"A": {}, "B": {}, "C": {}})
            self.parity_start_utc = d.get("parity_start_utc")
            self.telemetry_realized_usd = float(d.get("telemetry_realized_usd", 0.0))
            self.telemetry_signal_counts_c = d.get("telemetry_signal_counts_c", {})
            for name, raw in d.get("virtual_positions", {}).items():
                self.virtual_positions[name] = VirtualTrade(**raw)
            for k, v in d.get("stats", {}).items():
                if k in self.stats:
                    self.stats[k] = int(v)
        except Exception as exc:
            print(f"STATE LOAD WARNING = {exc}", flush=True)

    def save_state(self):
        payload = {
            "last_processed_bar": self.last_processed_bar,
            "processed_bar_keys": sorted(self.processed_bar_keys)[-2000:],
            "forward_start_utc": self.forward_start_utc,
            "signal_counts_a": self.signal_counts_a,
            "signal_counts_b": self.signal_counts_b,
            "signal_counts_c": self.signal_counts_c,
            "virtual_trade_counts": self.virtual_trade_counts,
            "virtual_positions": {k: vars(v) for k, v in self.virtual_positions.items()},
            "parity_start_utc": self.parity_start_utc,
            "telemetry_realized_usd": self.telemetry_realized_usd,
            "telemetry_signal_counts_c": self.telemetry_signal_counts_c,
            "stats": self.stats,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        tmp = STATE_V12_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(STATE_V12_PATH)

    # ---------------- logging ----------------
    def write_event(self, event: str, bar_time: str = "", detail: str = ""):
        new_file = not EVENTS_CSV.exists()
        row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "event": event,
               "bar_time_utc": bar_time, "detail": detail, "trading": "NO"}
        with EVENTS_CSV.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if new_file: w.writeheader()
            w.writerow(row)

    def write_signal(self, bar: Bar, direction: str, a_decision: str, b_decision: str,
                     h4_ema: float, atr1: float, dist_atr: float, rsi2: float, rsi1: float,
                     c_decision: str = ""):
        new_file = not SIGNALS_CSV.exists()
        local = bar.ts.astimezone(BASELINE_TZ)
        intended = self.ask if direction == "LONG" else self.bid
        sl_ref = ""
        if intended is not None:
            d = 1 if direction == "LONG" else -1
            sl_ref = f"{intended - d * atr1 * SL_ATR_MULT:.6f}"
        row = {
            "decision_time_utc": datetime.now(timezone.utc).isoformat(),
            "bar_time_utc": bar.ts.isoformat(), "bar_time_helsinki": local.isoformat(),
            "direction": direction, "a_decision": a_decision, "b_decision": b_decision,
            "c_decision": c_decision,
            "bid": "" if self.bid is None else f"{self.bid:.6f}",
            "ask": "" if self.ask is None else f"{self.ask:.6f}",
            "h4_ema50": f"{h4_ema:.6f}", "atr14": f"{atr1:.6f}",
            "distance_atr": f"{dist_atr:.6f}", "rsi_prev": f"{rsi2:.6f}",
            "rsi_last": f"{rsi1:.6f}", "sl_reference": sl_ref, "broker_order": "NO",
        }
        with SIGNALS_CSV.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if new_file: w.writeheader()
            w.writerow(row)

    def write_shadow_event(self, event: str, strategy: str, bar: Bar, trade: VirtualTrade,
                           exit_price: Optional[float] = None, result_r: Optional[float] = None):
        new_file = not SHADOW_EVENTS_CSV.exists()
        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(), "event": event,
            "strategy": strategy, "bar_time_utc": bar.ts.isoformat(),
            "direction": trade.direction, "entry_time_utc": trade.entry_time,
            "entry": f"{trade.entry:.6f}", "atr_entry": f"{trade.atr:.6f}",
            "stop": f"{trade.stop:.6f}", "activated": str(trade.activated).upper(),
            "exit": "" if exit_price is None else f"{exit_price:.6f}",
            "result_r": "" if result_r is None else f"{result_r:.6f}", "broker_order": "NO",
        }
        with SHADOW_EVENTS_CSV.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if new_file: w.writeheader()
            w.writerow(row)

    @staticmethod
    def virtual_position_id(trade: VirtualTrade) -> int:
        """Stable integer shared by C OPEN/CLOSE and preserved across restarts."""
        if trade.position_id is not None:
            return int(trade.position_id)
        entry_bar = datetime.fromisoformat(trade.entry_time)
        return int((entry_bar + timedelta(minutes=TF_MINUTES)).timestamp())

    def start_telemetry(self):
        if not TELEMETRY_ENABLED:
            print("TELEMETRY = DISABLED BY CONFIG", flush=True)
            return
        token = os.getenv("OPERACIONES_REALES_TOKEN", "").strip()
        if not token or token == "PEGA_AQUI_TU_TOKEN":
            print("TELEMETRY = DISABLED | OPERACIONES_REALES_TOKEN NOT SET", flush=True)
            return
        if not self.managed_accounts:
            try:
                self.reqManagedAccts()
            except Exception:
                pass
            self.managed_accounts_event.wait(10)
        if not self.managed_accounts:
            print("TELEMETRY = DISABLED | IBKR ACCOUNT NOT RECEIVED", flush=True)
            return
        account = self.managed_accounts[0]
        self.telemetry = Telemetria(
            cuenta_ib=account,
            client_id=CLIENT_ID,
            version="1.2.4-C-SHADOW",
            simbolo="XAUUSD",
            host=HOST,
            puerto=PORT,
            terminal="TWS",
            timeframe="15 mins",
            moneda="USD",
            solo_lectura=True,
            pendientes=str(OUTPUT_DIR / "runner_c_telemetry_pending.jsonl"),
        )
        self.telemetry.conexion(self.isConnected())
        if VIRTUAL_CAPITAL is not None:
            equity = VIRTUAL_CAPITAL + self.telemetry_realized_usd
            self.telemetry.cuenta(equity=equity, balance=equity)
        self.telemetry.iniciar()
        resent = self.telemetry.reenviar_pendientes()
        existing_c = self.virtual_positions.get("C")
        if existing_c is not None and not existing_c.telemetry_open_sent:
            self.telemetry_open_c(existing_c)
            self.save_state()
        print(f"TELEMETRY = ACTIVE | STRATEGY=C | RESENT={resent}", flush=True)

    def telemetry_open_c(self, trade: VirtualTrade):
        if self.telemetry is None:
            return
        risk_pct = None
        if VIRTUAL_CAPITAL is not None and VIRTUAL_CAPITAL > 0:
            risk_usd = SL_ATR_MULT * trade.atr * VIRTUAL_XAU_OZ
            risk_pct = risk_usd / VIRTUAL_CAPITAL * 100.0
        spread_pts = None if trade.spread is None else trade.spread / 0.01
        self.telemetry.abrir(
            direccion="BUY" if trade.direction == "LONG" else "SELL",
            lote=VIRTUAL_XAU_OZ,
            precio_pedido=trade.entry,
            precio_ejecutado=None,
            spread_pts=spread_pts,
            spread_dinero=None,
            riesgo_pct=risk_pct,
            atr_entrada=trade.atr,
            sl=trade.stop,
            tp=None,
            perm_id=self.virtual_position_id(trade),
            contexto={
                "shadow": "C",
                "execution_mode": "VIRTUAL",
                "session": "UTC_1100_1559_DECISION",
                "data_price": "M15_MIDPOINT_CLOSE",
                "broker_order": False,
                "xau_oz": VIRTUAL_XAU_OZ,
                "exness_lot_equiv": VIRTUAL_XAU_OZ / 100.0,
            },
        )
        trade.telemetry_open_sent = True

    def telemetry_close_c(self, bar: Bar, trade: VirtualTrade, exit_price: float):
        if self.telemetry is None:
            return
        signed = 1.0 if trade.direction == "LONG" else -1.0
        result_usd = signed * (exit_price - trade.entry) * VIRTUAL_XAU_OZ
        exit_atr = signed * (exit_price - trade.entry) / trade.atr
        mfe_atr = signed * (trade.max_favorable - trade.entry) / trade.atr
        entry_bar = datetime.fromisoformat(trade.entry_time)
        duration = max(0, int((bar.ts - entry_bar).total_seconds()))
        self.telemetry_realized_usd += result_usd
        if VIRTUAL_CAPITAL is not None:
            equity = VIRTUAL_CAPITAL + self.telemetry_realized_usd
            self.telemetry.cuenta(equity=equity, balance=equity)
        self.telemetry.cerrar(
            direccion="BUY" if trade.direction == "LONG" else "SELL",
            resultado=result_usd,
            comision=None,
            motivo="TRAIL" if trade.activated else "SL",
            precio_pedido=exit_price,
            precio_ejecutado=None,
            mfe_atr=mfe_atr,
            salida_atr=exit_atr,
            duracion_seg=duration,
            perm_id=self.virtual_position_id(trade),
        )

    def telemetry_daily_heartbeat(self):
        if self.telemetry is None:
            return
        day = datetime.now(timezone.utc).date().isoformat()
        open_c = self.virtual_positions.get("C")
        floating = None
        if open_c is not None and self.m15_cache:
            signed = 1.0 if open_c.direction == "LONG" else -1.0
            floating = signed * (self.m15_cache[-1].close - open_c.entry) * VIRTUAL_XAU_OZ
        self.telemetry.latido(
            senales=int(self.telemetry_signal_counts_c.get(day, 0)),
            entradas=int(self.virtual_trade_counts.get("C", {}).get(day, 0)),
            flotante=floating,
            abiertas=1 if open_c is not None else 0,
            contexto={
                "shadow": "C",
                "execution_mode": "VIRTUAL",
                "broker_orders": False,
                "result_unit": "USD_1_XAU_OZ",
            },
        )

    def manage_virtual_positions(self, bar: Bar):
        """Conservative completed-bar simulation; stop changes apply to future bars."""
        for strategy, trade in list(self.virtual_positions.items()):
            exit_price = None
            if trade.direction == "LONG":
                if bar.low <= trade.stop:
                    exit_price = trade.stop
                else:
                    trade.max_favorable = max(trade.max_favorable, bar.high)
                    if trade.max_favorable >= trade.entry + ACTIVATE_ATR * trade.atr:
                        trade.activated = True
                        trade.stop = max(trade.stop, trade.entry + LOCK_PROFIT_ATR * trade.atr,
                                         trade.max_favorable - TRAIL_ATR * trade.atr)
            else:
                if bar.high >= trade.stop:
                    exit_price = trade.stop
                else:
                    trade.max_favorable = min(trade.max_favorable, bar.low)
                    if trade.max_favorable <= trade.entry - ACTIVATE_ATR * trade.atr:
                        trade.activated = True
                        trade.stop = min(trade.stop, trade.entry - LOCK_PROFIT_ATR * trade.atr,
                                         trade.max_favorable + TRAIL_ATR * trade.atr)
            if exit_price is not None:
                signed = 1.0 if trade.direction == "LONG" else -1.0
                result_r = signed * (exit_price - trade.entry) / (SL_ATR_MULT * trade.atr)
                self.write_shadow_event("VIRTUAL_EXIT", strategy, bar, trade, exit_price, result_r)
                self.write_virtual_trade(strategy, bar, trade, exit_price, result_r)
                if strategy == "C":
                    self.telemetry_close_c(bar, trade, exit_price)
                del self.virtual_positions[strategy]

    def write_virtual_trade(self, strategy: str, bar: Bar, trade: VirtualTrade,
                            exit_price: float, result_r: float):
        new_file = not SHADOW_TRADES_CSV.exists()
        row = {"strategy": strategy, "direction": trade.direction,
               "entry_time_utc": trade.entry_time,
               "exit_bar_time_utc": bar.ts.isoformat(), "entry": f"{trade.entry:.6f}",
               "exit": f"{exit_price:.6f}", "atr_entry": f"{trade.atr:.6f}",
               "result_r": f"{result_r:.6f}", "broker_order": "NO"}
        with SHADOW_TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if new_file: w.writeheader()
            w.writerow(row)

    def open_virtual_trade(self, strategy: str, bar: Bar, direction: str, atr1: float):
        spread = (self.ask - self.bid) if self.ask is not None and self.bid is not None else 0.0
        entry = bar.close + spread / 2.0 if direction == "LONG" else bar.close - spread / 2.0
        stop = entry - SL_ATR_MULT * atr1 if direction == "LONG" else entry + SL_ATR_MULT * atr1
        position_id = int((bar.ts + timedelta(minutes=TF_MINUTES)).timestamp())
        trade = VirtualTrade(
            strategy, direction, bar.ts.isoformat(), entry, atr1, stop, entry,
            position_id=position_id, spread=spread,
        )
        self.virtual_positions[strategy] = trade
        self.write_shadow_event("VIRTUAL_ENTRY", strategy, bar, trade)
        if strategy == "C":
            self.telemetry_open_c(trade)

    # ---------------- historical request resilience ----------------
    def _new_req(self, timeframe: str) -> HistRequest:
        with self._req_lock:
            self._next_req_id += 1
            req = HistRequest(self._next_req_id, timeframe, threading.Event(), [])
            self._requests[req.req_id] = req
            return req

    def _retire_req(self, req: HistRequest):
        with self._req_lock:
            if req.req_id in self._requests:
                self._requests[req.req_id].active = False

    def _historical_refresh(self, timeframe: str) -> bool:
        duration, bar_size = (M15_DURATION, "15 mins") if timeframe == "M15" else (H4_DURATION, "4 hours")
        had_timeout = False
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if not (self.isConnected() and self.connected_event.is_set()):
                print(f"DATA REQUEST PAUSED {timeframe} = IBKR DISCONNECTED", flush=True)
                return False
            req = self._new_req(timeframe)
            self.stats[f"{timeframe}_REQUESTS"] += 1
            print(f"DATA REQUEST {timeframe} attempt={attempt} reqId={req.req_id}", flush=True)
            try:
                self.reqHistoricalData(req.req_id, xau_contract(), "", duration, bar_size,
                                       "MIDPOINT", 0, 2, False, [])
            except Exception as exc:
                self._retire_req(req)
                if attempt < MAX_ATTEMPTS:
                    self.stats[f"{timeframe}_RETRIES"] += 1
                    delay = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS)-1)]
                    print(f"DATA RETRY {timeframe} attempt={attempt+1} reason=request_exception wait={delay}s", flush=True)
                    time.sleep(delay)
                    continue
                self.write_event("DATA_REQUEST_EXCEPTION", detail=f"{timeframe}: {exc}")
                return False

            ok = req.event.wait(REQUEST_TIMEOUT_SECONDS)
            if not (self.isConnected() and self.connected_event.is_set()):
                try:
                    self.cancelHistoricalData(req.req_id)
                except Exception:
                    pass
                self._retire_req(req)
                print(f"DATA REQUEST ABORTED {timeframe} = IBKR DISCONNECTED", flush=True)
                return False
            if ok and req.completed and req.buffer:
                bars = sorted({b.ts: b for b in req.buffer}.values(), key=lambda b: b.ts)
                self._retire_req(req)
                if timeframe == "M15":
                    self.m15_cache = bars
                    self.m15_refresh_failed = False
                    self.next_m15_refresh_due = next_boundary(datetime.now(timezone.utc), TF_MINUTES) + timedelta(seconds=3)
                else:
                    self.h4_cache = bars
                    self.h4_refresh_failed = False
                    self.next_h4_refresh_due = next_boundary(datetime.now(timezone.utc), HTF_HOURS*60) + timedelta(seconds=3)
                if had_timeout:
                    self.stats[f"{timeframe}_RECOVERIES"] += 1
                    print(f"DATA RECOVERED {timeframe} attempts={attempt} last_good={bars[-1].ts.isoformat()}", flush=True)
                self.save_state()
                return True

            had_timeout = True
            self.stats[f"{timeframe}_TIMEOUTS"] += 1
            try: self.cancelHistoricalData(req.req_id)
            except Exception: pass
            self._retire_req(req)
            if attempt < MAX_ATTEMPTS:
                self.stats[f"{timeframe}_RETRIES"] += 1
                delay = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS)-1)]
                print(f"DATA RETRY {timeframe} attempt={attempt+1} reason=timeout wait={delay}s", flush=True)
                time.sleep(delay)
            else:
                print(f"DATA REFRESH FAILED {timeframe} attempts={MAX_ATTEMPTS} | CACHE PRESERVED", flush=True)
                self.write_event("DATA_REFRESH_FAILED", detail=f"{timeframe}; cache_preserved=YES")

        if timeframe == "M15": self.m15_refresh_failed = True
        else: self.h4_refresh_failed = True
        self.save_state()
        return False

    # ---------------- contract / market data ----------------
    def validate_contract(self):
        self.contract_event.clear(); self.contract_details = []
        self.reqContractDetails(3001, xau_contract())
        if not self.contract_event.wait(15):
            raise RuntimeError("Contract validation timed out.")
        if not self.contract_details:
            raise RuntimeError("IBKR did not resolve XAUUSD/CMDTY/SMART/USD.")
        d = self.contract_details[0]; c = d.contract
        print("CONTRACT PASS = "
              f"conId={c.conId} symbol={c.symbol} secType={c.secType} exchange={c.exchange} "
              f"currency={c.currency} minTick={getattr(d,'minTick','NA')}", flush=True)

    def request_quote(self, timeout_seconds: int = 8) -> bool:
        """Request a fresh BID/ASK only when a baseline-session bar needs evaluation.

        Mirrors the previously working observer behavior: clear old quote state,
        request market data, wait briefly for BID/ASK, then cancel the request.
        If no quote arrives, the M15 bar remains pending and is retried later.
        """
        self.bid = None
        self.ask = None
        self.quote_event.clear()
        self.quote_requests += 1

        self.reqMarketDataType(MARKET_DATA_TYPE)
        self.reqMktData(self.market_req_id, xau_contract(), "", False, False, [])

        deadline = time.monotonic() + timeout_seconds
        try:
            while time.monotonic() < deadline:
                remaining = max(0.0, deadline - time.monotonic())
                self.quote_event.wait(min(0.5, remaining))
                if self.bid is not None and self.ask is not None:
                    return True
                self.quote_event.clear()
        finally:
            try:
                self.cancelMktData(self.market_req_id)
            except Exception:
                pass

        self.quote_failures += 1
        print(
            f"QUOTE REFRESH FAILED | BID={self.bid} ASK={self.ask} "
            f"| pending bar will be retained",
            flush=True,
        )
        self.write_event(
            "QUOTE_REFRESH_FAILED",
            detail=f"bid={self.bid};ask={self.ask};timeout={timeout_seconds}s",
        )
        return False

    # ---------------- strategy evaluation ----------------
    def _mark_processed(self, bar: Bar):
        key = bar.ts.isoformat()
        self.processed_bar_keys.add(key)
        self.last_processed_bar = key
        self.save_state()

    def entry_decision_is_fresh(self, decision_time: datetime, now_utc: datetime) -> bool:
        """Block delayed entries reconstructed from startup/reconnect history."""
        if self.live_entry_cutoff_utc is None:
            return False
        age_seconds = (now_utc - decision_time).total_seconds()
        return (
            decision_time >= self.live_entry_cutoff_utc
            and 0.0 <= age_seconds <= ENTRY_MAX_AGE_SECONDS
        )

    def process_pending_bars(self):
        now = datetime.now(timezone.utc)
        m15 = sorted([b for b in self.m15_cache if is_completed_bar(b, now, TF_MINUTES)], key=lambda b: b.ts)
        h4 = sorted([b for b in self.h4_cache if is_completed_bar(b, now, HTF_HOURS*60)], key=lambda b: b.ts)
        if len(m15) < 40 or len(h4) < HTF_EMA_PERIOD + 10:
            return

        pending = [b for b in m15 if b.ts.isoformat() not in self.processed_bar_keys]
        if self.forward_start_utc:
            try:
                fs = datetime.fromisoformat(self.forward_start_utc)
                pending = [b for b in pending if b.ts + timedelta(minutes=TF_MINUTES) >= fs]
            except Exception: pass
        pending.sort(key=lambda b: b.ts)

        for bar in pending:
            key = bar.ts.isoformat()
            if key in self.processed_bar_keys:
                self.stats["DUPLICATE_BARS_BLOCKED"] += 1
                continue
            local = bar.ts.astimezone(BASELINE_TZ)
            # A/B remain frozen on the legacy bar-open clock. C mirrors the
            # Exness EA: a completed M15 bar is evaluated at the next boundary.
            decision_time = bar.ts + timedelta(minutes=TF_MINUTES)
            entry_fresh = self.entry_decision_is_fresh(decision_time, now)
            a_session = baseline_session_pass(bar.ts)
            b_session = a_session and candidate_b_hour_pass(bar.ts)
            c_session = confirmed_utc_session_pass(decision_time)

            # Open virtual positions are managed 24/5, including outside entry windows.
            if not (a_session or b_session or c_session):
                self.manage_virtual_positions(bar)
                print(f"M15={bar.ts.isoformat()} | NONE | A_FILTERED_SESSION / "
                      f"B_FILTERED_SESSION / C_FILTERED_SESSION | HELSINKI={local.isoformat()} | TRADING=NO", flush=True)
                self._mark_processed(bar)
                continue

            hist_m15 = [b for b in m15 if b.ts <= bar.ts]
            rsi_series = wilder_rsi([b.close for b in hist_m15], RSI_PERIOD)
            atr_series = wilder_atr(hist_m15, ATR_PERIOD)
            rsi1, rsi2, atr1 = rsi_series[-1], rsi_series[-2], atr_series[-1]

            h4_asof = [b for b in self.h4_cache if b.ts + timedelta(hours=HTF_HOURS) <= decision_time]
            h4_ema = ema([b.close for b in h4_asof], HTF_EMA_PERIOD)
            if any(v is None for v in (rsi1, rsi2, atr1, h4_ema)) or atr1 is None or atr1 <= 0:
                print(f"DATA BLOCK = indicator warmup at {bar.ts.isoformat()}", flush=True)
                break
            # Completed MIDPOINT close makes signal reconstruction deterministic.
            dist_atr = (bar.close - float(h4_ema)) / float(atr1)
            direction = "NONE"
            if dist_atr > MIN_DIST_FROM_EMA_ATR and float(rsi2) < 50.0 and float(rsi1) > 50.0:
                direction = "LONG"
            elif dist_atr < -MIN_DIST_FROM_EMA_ATR and float(rsi2) > 50.0 and float(rsi1) < 50.0:
                direction = "SHORT"

            # A quote is only needed when at least one strategy could open a
            # fresh position. Stale signals are logged but never converted into
            # entries at the current market price.
            if direction != "NONE" and entry_fresh and not self.request_quote():
                print(f"DATA BLOCK = fresh BID/ASK unavailable; pending bar retained {bar.ts.isoformat()}", flush=True)
                break

            if direction != "NONE" and c_session and entry_fresh:
                day = decision_time.date().isoformat()
                self.telemetry_signal_counts_c[day] = self.telemetry_signal_counts_c.get(day, 0) + 1

            self.manage_virtual_positions(bar)
            hel_day = local.date().isoformat()
            utc_day = decision_time.date().isoformat()

            def decide(strategy: str, session_ok: bool, day_key: str, counts: Dict[str, int], label: str) -> str:
                if not session_ok:
                    return f"{label}_FILTERED_SESSION"
                if direction == "NONE":
                    return "NO_SIGNAL"
                if not entry_fresh:
                    return f"{label}_FILTERED_STALE_SIGNAL"
                if strategy in self.virtual_positions:
                    return f"{label}_FILTERED_OPEN_POSITION"
                daily = self.virtual_trade_counts.setdefault(strategy, {}).get(day_key, 0)
                if daily >= MAX_TRADES_PER_DAY:
                    return f"{label}_FILTERED_MAX_DAILY"
                counts[day_key] = counts.get(day_key, 0) + 1
                self.virtual_trade_counts[strategy][day_key] = daily + 1
                self.open_virtual_trade(strategy, bar, direction, float(atr1))
                return f"{label}_SIGNAL"

            a_decision = decide("A", a_session, hel_day, self.signal_counts_a, "A")
            b_decision = decide("B", b_session, hel_day, self.signal_counts_b, "B")
            c_decision = decide("C", c_session, utc_day, self.signal_counts_c, "C")
            if direction != "NONE":
                self.write_signal(bar, direction, a_decision, b_decision, float(h4_ema), float(atr1),
                                  dist_atr, float(rsi2), float(rsi1), c_decision)

            print(f"M15={bar.ts.isoformat()} | {direction} | {a_decision} / {b_decision} / {c_decision} | "
                  f"HELSINKI={local.isoformat()} | TRADING=NO", flush=True)
            self.write_event("BAR_DECISION", bar.ts.isoformat(), f"{direction}|{a_decision}|{b_decision}|{c_decision}")
            self._mark_processed(bar)

    # ---------------- connection resilience ----------------
    def _mark_disconnected(self, reason: str):
        self.connected_event.clear()
        if self.telemetry is not None:
            self.telemetry.conexion(False)
        if not self._disconnect_announced:
            print(f"IBKR DISCONNECTED = {reason} | REQUESTS PAUSED", flush=True)
            self.write_event("IBKR_DISCONNECTED", detail=reason)
            self._disconnect_announced = True

    def ensure_connection(self) -> bool:
        """Reconnect TWS without issuing data requests over a dead socket."""
        if self.isConnected() and self.connected_event.is_set():
            return True
        self._mark_disconnected("socket unavailable")
        now_m = time.monotonic()
        if now_m < self._next_reconnect_monotonic:
            return False

        with self._connection_lock:
            if self.isConnected() and self.connected_event.is_set():
                return True
            self._next_reconnect_monotonic = time.monotonic() + RECONNECT_SECONDS
            print(f"IBKR RECONNECT ATTEMPT = {HOST}:{PORT} clientId={CLIENT_ID}", flush=True)
            try:
                try:
                    self.disconnect()
                except Exception:
                    pass
                self.connected_event.clear()
                self.managed_accounts_event.clear()
                self.connect(HOST, PORT, clientId=CLIENT_ID)
                threading.Thread(target=self.run, daemon=True).start()
            except Exception as exc:
                print(f"IBKR RECONNECT FAILED = {type(exc).__name__}: {exc}", flush=True)
                return False

            if not self.connected_event.wait(RECONNECT_TIMEOUT_SECONDS):
                print("IBKR RECONNECT FAILED = timeout | REQUESTS REMAIN PAUSED", flush=True)
                try:
                    self.disconnect()
                except Exception:
                    pass
                return False

            print("IBKR RECONNECTED = True | DATA CATCH-UP ARMED", flush=True)
            self.write_event("IBKR_RECONNECTED", detail="data_catch_up=YES")
            if self.telemetry is not None:
                self.telemetry.conexion(True)
                self.telemetry.reconectado()
                self.telemetry.reenviar_pendientes()
            self.m15_refresh_failed = True
            self.h4_refresh_failed = True
            self.next_m15_refresh_due = datetime.now(timezone.utc)
            self.next_h4_refresh_due = datetime.now(timezone.utc)
            return True

    # ---------------- scheduler ----------------
    def refresh_due_data(self):
        now = datetime.now(timezone.utc)
        need_m15 = (not self.m15_cache or self.m15_refresh_failed or self.next_m15_refresh_due is None
                    or now >= self.next_m15_refresh_due)
        if need_m15:
            if not self._historical_refresh("M15"):
                self.m15_refresh_failed = True
                self.next_m15_refresh_due = now + timedelta(seconds=60)
            time.sleep(0.5)

        need_h4 = (not self.h4_cache or self.h4_refresh_failed or self.next_h4_refresh_due is None
                   or now >= self.next_h4_refresh_due)
        if need_h4:
            if not self._historical_refresh("H4"):
                self.h4_refresh_failed = True
                self.next_h4_refresh_due = now + timedelta(seconds=120)

    def print_heartbeat(self):
        now = datetime.now(timezone.utc); hel = now.astimezone(BASELINE_TZ)
        print("HEARTBEAT", flush=True)
        print(f"UTC = {now.isoformat()}", flush=True)
        print(f"HELSINKI = {hel.isoformat()}", flush=True)
        print(f"IBKR_CONNECTED = {self.isConnected()}", flush=True)
        print(f"LAST_M15 = {self.m15_cache[-1].ts if self.m15_cache else None}", flush=True)
        print(f"LAST_H4 = {self.h4_cache[-1].ts if self.h4_cache else None}", flush=True)
        print(f"BID = {self.bid}", flush=True)
        print(f"ASK = {self.ask}", flush=True)
        print("MODE = OBSERVER / DISARMED", flush=True)
        self.telemetry_daily_heartbeat()

    def run_observer(self):
        self.live_entry_cutoff_utc = datetime.now(timezone.utc)
        self.start_telemetry()
        self.validate_contract()
        now = datetime.now(timezone.utc)
        if self.forward_start_utc is None:
            self.forward_start_utc = now.isoformat()
        if self.parity_start_utc is None:
            self.parity_start_utc = now.isoformat()
        self.save_state()
        print(f"FORWARD_START_UTC = {self.forward_start_utc}", flush=True)
        print(f"PARITY_START_UTC = {self.parity_start_utc}", flush=True)

        self._historical_refresh("M15")
        time.sleep(0.5)
        self._historical_refresh("H4")
        self.last_heartbeat_monotonic = 0.0

        while not self.stop_requested:
            try:
                if self.ensure_connection():
                    self.refresh_due_data()
                    self.process_pending_bars()
            except Exception as exc:
                print(f"CYCLE ERROR = {type(exc).__name__}: {exc}", flush=True)
                self.write_event("CYCLE_ERROR", detail=f"{type(exc).__name__}: {exc}")
            now_m = time.monotonic()
            if now_m - self.last_heartbeat_monotonic >= HEARTBEAT_SECONDS:
                self.print_heartbeat(); self.last_heartbeat_monotonic = now_m
            for _ in range(max(1, LOOP_SECONDS)):
                if self.stop_requested: break
                time.sleep(1)
        try:
            self.cancelMktData(self.market_req_id)
        except Exception:
            pass
        self.save_state()


def main():
    print("RUNNER IBKR TRIPLE SHADOW v1.2.4", flush=True)
    print("MODE = OBSERVER / DISARMED", flush=True)
    print("TRADING EXECUTED = NO", flush=True)
    print("CONTRACT = XAUUSD / CMDTY / SMART / USD", flush=True)
    print("A = LEGACY HELSINKI 11:00-15:59", flush=True)
    print("B = LEGACY HELSINKI 13:00-14:59", flush=True)
    print("C = CONFIRMED EXNESS CLOCK / UTC 11:00-15:59", flush=True)
    print("C SESSION CLOCK = DECISION TIME / COMPLETED BAR CLOSE", flush=True)
    print("C PUERTO RICO = 07:00-11:59", flush=True)
    print("ENTRY SIGNAL PRICE = COMPLETED M15 MIDPOINT CLOSE", flush=True)
    print("POSITION MANAGEMENT = VIRTUAL / COMPLETED M15 / 24x5", flush=True)
    print("QUOTE MODE = ON-DEMAND BID/ASK", flush=True)
    print("DATA RESILIENCE = v1.2.4 + SAFE TWS RECONNECT + FRESH-SIGNAL GUARD", flush=True)
    print(f"ENTRY FRESHNESS MAX AGE = {ENTRY_MAX_AGE_SECONDS} SECONDS", flush=True)
    print(f"TWS = {HOST}:{PORT}", flush=True)
    print(f"HOST = {HOST}", flush=True)
    print(f"PORT = {PORT}", flush=True)
    print(f"CLIENT_ID = {CLIENT_ID}", flush=True)

    app = RunnerDualObserver()
    def request_stop(signum=None, frame=None):
        print("STOP REQUESTED", flush=True); app.stop_requested = True
    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"): signal.signal(signal.SIGTERM, request_stop)

    app.connect(HOST, PORT, clientId=CLIENT_ID)
    thread = threading.Thread(target=app.run, daemon=True); thread.start()
    if not app.connected_event.wait(15):
        print("FATAL = TWS connection timed out.", flush=True)
        try: app.disconnect()
        except Exception: pass
        return 2
    try:
        app.run_observer()
    finally:
        if app.telemetry is not None:
            app.telemetry.detener("Runner C observer stopped")
        try: app.disconnect()
        except Exception: pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
