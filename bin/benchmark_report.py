#!/usr/bin/env python3
"""Convert Google Benchmark JSON outputs into a single HTML comparison report."""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import Any

STRATEGIES = {
    "0": "circular_bounded",
    "1": "circular_overwrite",
    "2": "deque_bounded",
    "3": "michael_scott",
    "4": "priority_queue",  
}

PROFILES = {
    "0": "balanced",
    "1": "fast_producer",
    "2": "fast_consumer",
    "3": "batch",
    "4": "burst",
    "5": "priority_mix", 
}

TOPOLOGIES = {
    "0": "SPSC (P1C1)",
    "1": "MPSC (P4C1)",
    "2": "SPMC (P1C4)",
    "3": "MPMC (P4C4)",
}

CASE_RE = re.compile(
    r"BM_QueueScenario/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)"
)

COUNTER_KEYS = [
    ("consume_per_s", "consume msg/s", "rate"),
    ("produce_per_s", "produce msg/s", "rate"),
    ("msg_per_s", "msg/s", "rate"),
    ("send_p50_ns", "send p50 ns", "latency"),
    ("send_p99_ns", "send p99 ns", "latency"),
    ("read_p50_ns", "read p50 ns", "latency"),
    ("read_p99_ns", "read p99 ns", "latency"),
    ("lost_messages", "lost", "count"),
    ("seq_gaps", "seq gaps", "count"),
    ("blocked_send_attempts", "blocked send", "count"),
    ("blocked_read_attempts", "blocked read", "count"),
    ("cas_retries", "CAS retries", "count"),
    ("producer_fairness", "producer fairness", "ratio"),
    ("consumer_fairness", "consumer fairness", "ratio"),
    ("fairness_ratio", "fairness", "ratio"),
    ("timed_out", "timed out", "ratio"),
    ("queue_size_at_end", "queue left", "count"),
    ("producer_threads", "producers", "count"),
    ("consumer_threads", "consumers", "count"),
    ("hw_threads", "hw threads", "count"),
    ("thread_ratio", "thread ratio", "ratio"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory with *.json files produced by run_benchmark_suite.bat",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: <input_dir>/report.html)",
    )
    return parser.parse_args()


def _try_repair_truncated_benchmark_json(text: str) -> dict[str, Any] | None:
    stripped = text.rstrip()
    if not stripped.startswith("{"):
        return None
    for suffix in ("\n  ]\n}", "\n]\n}"):
        try:
            payload = json.loads(stripped + suffix)
        except json.JSONDecodeError:
            continue
        benchmarks = payload.get("benchmarks")
        if isinstance(benchmarks, list) and benchmarks:
            return payload
    return None


def load_benchmark_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text), None
    except json.JSONDecodeError as exc:
        repaired = _try_repair_truncated_benchmark_json(text)
        if repaired is not None:
            count = len(repaired.get("benchmarks", []))
            return (
                repaired,
                f"{path.name}: truncated JSON repaired ({count} benchmark rows, {exc})",
            )
        return None, f"{path.name}: invalid JSON ({exc})"


def extract_counters(entry: dict[str, Any]) -> dict[str, float]:
    counters: dict[str, float] = {}
    user_counters = entry.get("user_counters")
    if isinstance(user_counters, dict):
        for key, value in user_counters.items():
            if isinstance(value, dict) and "value" in value:
                counters[key] = float(value["value"])
            elif isinstance(value, (int, float)):
                counters[key] = float(value)

    for key, _, _ in COUNTER_KEYS:
        if key in entry and key not in counters:
            counters[key] = float(entry[key])

    # Backward compatibility for reports generated before p99 migration.
    if "send_p99_ns" not in counters and "send_p95_ns" in counters:
        counters["send_p99_ns"] = counters["send_p95_ns"]
    if "read_p99_ns" not in counters and "read_p95_ns" in counters:
        counters["read_p99_ns"] = counters["read_p95_ns"]
    return counters


def parse_case_name(name: str) -> dict[str, str] | None:
    match = CASE_RE.search(name)
    if not match:
        return None
    strategy, profile, topology, capacity, messages = match.groups()
    return {
        "strategy_id": strategy,
        "profile_id": profile,
        "topology_id": topology,
        "capacity": capacity,
        "messages": messages,
        "strategy": STRATEGIES.get(strategy, strategy),
        "profile": PROFILES.get(profile, profile),
        "topology": TOPOLOGIES.get(topology, topology),
    }


def pick_report_rows(benchmarks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {}

    for entry in benchmarks:
        name = entry.get("name", "")

        case = parse_case_name(name)
        if case is None:
            continue

        key = "/".join(
            [
                case["strategy_id"],
                case["profile_id"],
                case["topology_id"],
                case["capacity"],
                case["messages"],
            ]
        )

        if name.endswith("_mean"):
            variant = "mean"
        elif name.endswith("_stddev"):
            variant = "stddev"
        elif name.endswith("_cv"):
            variant = "cv"
        elif name.endswith("_median"):
            variant = "median"
        else:
            variant = "run"
        grouped.setdefault(key, {})[variant] = entry

    rows: list[dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        variants = grouped[key]
        entry = variants.get("mean") or variants.get("run") or variants.get("median")
        if entry is None:
            continue
        stddev_entry = variants.get("stddev")

        case = parse_case_name(entry["name"])
        if case is None:
            continue

        counters = extract_counters(entry)
        stddev_counters = extract_counters(stddev_entry) if stddev_entry else {}
        real_time = float(entry.get("real_time", 0.0))
        time_unit = entry.get("time_unit", "ms")
        producers = int(round(counters.get("producer_threads", 0.0)))
        consumers = int(round(counters.get("consumer_threads", 0.0)))
        topology = case["topology"] if producers <= 0 or consumers <= 0 else f"P{producers}C{consumers}"

        rows.append(
            {
                **case,
                "topology": topology,
                "real_time": real_time,
                "time_unit": time_unit,
                "counters": counters,
                "stddev_counters": stddev_counters,
            }
        )
    return rows


def format_number(value: float | None, kind: str) -> str:
    if value is None:
        return "—"
    if kind == "rate":
        if value >= 1_000_000:
            return f"{value / 1_000_000:.3f}M"
        if value >= 1_000:
            return f"{value / 1_000:.2f}k"
        return f"{value:.2f}"
    if kind == "latency":
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}s"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}ms"
        if value >= 1_000:
            return f"{value / 1_000:.2f}µs"
        return f"{value:.0f}ns"
    if kind == "ratio":
        return f"{value:.3f}"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.2f}"


def best_worst_indices(rows: list[dict[str, Any]], key: str) -> tuple[int | None, int | None]:
    values = [row["counters"].get(key) for row in rows]
    numeric = [(idx, val) for idx, val in enumerate(values) if val is not None]
    if not numeric:
        return None, None
    best = max(numeric, key=lambda item: item[1])[0]
    worst = min(numeric, key=lambda item: item[1])[0]
    if best == worst:
        return best, None
    return best, worst


def cv_percent(row: dict[str, Any], key: str) -> float | None:
    value = row["counters"].get(key)
    stddev = row.get("stddev_counters", {}).get(key)
    if value is None or stddev is None or value == 0:
        return None
    return abs(stddev / value) * 100.0


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p class='empty'>No benchmark rows found in this section.</p>"

    best_idx, worst_idx = best_worst_indices(rows, "consume_per_s")

    header = [
        "Strategy",
        "Profile",
        "Topology",
        "Capacity",
        "Time",
        "consume CV",
        "produce CV",
        *[label for _, label, _ in COUNTER_KEYS],
    ]

    parts = ["<div class='table-wrap'><table>"]
    parts.append("<thead><tr>" + "".join(f"<th>{html.escape(col)}</th>" for col in header) + "</tr></thead>")
    parts.append("<tbody>")

    for idx, row in enumerate(rows):
        row_class = []
        if row["counters"].get("timed_out"):
            row_class.append("timeout")
        if idx == best_idx:
            row_class.append("best")
        if worst_idx is not None and idx == worst_idx:
            row_class.append("worst")
        class_attr = f" class='{' '.join(row_class)}'" if row_class else ""

        cells = [
            row["strategy"],
            row["profile"],
            row["topology"],
            row["capacity"],
            f"{row['real_time']:.2f} {row['time_unit']}",
            f"{cv_percent(row, 'consume_per_s'):.2f}%" if cv_percent(row, "consume_per_s") is not None else "—",
            f"{cv_percent(row, 'produce_per_s'):.2f}%" if cv_percent(row, "produce_per_s") is not None else "—",
        ]
        for key, _, kind in COUNTER_KEYS:
            cells.append(format_number(row["counters"].get(key), kind))

        parts.append(
            "<tr"
            + class_attr
            + ">"
            + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in cells)
            + "</tr>"
        )

    parts.append("</tbody></table></div>")
    return "".join(parts)


def render_section(title: str, description: str, rows: list[dict[str, Any]]) -> str:
    return (
        f"<section class='suite'>"
        f"<h2>{html.escape(title)}</h2>"
        f"<p class='desc'>{html.escape(description)}</p>"
        f"{render_table(rows)}"
        f"</section>"
    )


def suite_metadata(path: Path) -> tuple[str, str]:
    stem = path.stem
    mapping = {
        "01_throughput_spsc": (
            "Throughput · all strategies · SPSC · cap 1024",
            "Balanced throughput mode with backoff disabled.",
        ),
        "02_contention_mpsc": (
            "Contention · all strategies · MPSC · cap 1024",
            "Balanced MPSC contention with dynamic producer count.",
        ),
        "03_contention_mpmc": (
            "Contention · all strategies · MPMC · cap 1024",
            "Balanced MPMC contention with dynamic producer/consumer counts.",
        ),
        "04_overwrite_pressure": (
            "Overwrite pressure · circular_overwrite",
            "Small capacity overwrite stress across profiles and topologies.",
        ),
        "05_latency_profiles": (
            "Latency profiles · all strategies · SPSC",
            "Fast/batch/burst profiles with synthetic pauses enabled.",
        ),
        "06_capacity_sweep": (
            "Capacity sweep · all strategies · SPSC",
            "Balanced throughput for capacities 64/1024/4096.",
        ),
        "99_full_matrix": (
            "Full matrix",
            "Complete strategy × profile × topology × capacity grid, 100k messages, backoff enabled.",
        ),
    }
    return mapping.get(stem, (stem.replace("_", " "), f"Results from {path.name}"))


def report_context(
    sections: list[tuple[str, str, list[dict[str, Any]]]],
    json_payloads: list[dict[str, Any]],
) -> dict[str, str]:
    repetitions = "n/a"
    for payload in json_payloads:
        benchmarks = payload.get("benchmarks", [])
        if benchmarks:
            reps = benchmarks[0].get("repetitions")
            if reps is not None:
                repetitions = str(reps)
                break

    for _, _, rows in sections:
        for row in rows:
            counters = row.get("counters", {})
            hw_threads = counters.get("hw_threads")
            thread_ratio = counters.get("thread_ratio")
            if hw_threads is not None and thread_ratio is not None:
                return {
                    "hw_threads": str(int(round(hw_threads))),
                    "thread_ratio": f"{thread_ratio:.2f}",
                    "repetitions": repetitions,
                }
    return {"hw_threads": "n/a", "thread_ratio": "n/a", "repetitions": repetitions}


def build_report(
    sections: list[tuple[str, str, list[dict[str, Any]]]],
    output: Path,
    json_payloads: list[dict[str, Any]],
) -> None:
    generated = output.resolve()
    summary = report_context(sections, json_payloads)
    section_html = "".join(
        render_section(title, description, rows) for title, description, rows in sections
    )

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Message Queue Benchmark Report</title>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #171d25;
      --panel-2: #1f2731;
      --text: #e7ecf3;
      --muted: #9aa7b8;
      --line: #2d3847;
      --accent: #5b9dff;
      --best: #1f4d3a;
      --best-text: #7dffb2;
      --worst: #4d1f24;
      --worst-text: #ff9aa5;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Inter, system-ui, sans-serif;
      background: linear-gradient(180deg, #0b1015 0%, var(--bg) 100%);
      color: var(--text);
      line-height: 1.45;
    }}
    .page {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 32px 20px 64px;
    }}
    header {{
      margin-bottom: 28px;
      padding: 24px 28px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 10px 40px rgba(0, 0, 0, 0.25);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 1.8rem;
      letter-spacing: -0.02em;
    }}
    .subtitle {{
      margin: 0;
      color: var(--muted);
    }}
    .legend {{
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      margin-top: 16px;
      font-size: 0.92rem;
      color: var(--muted);
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--panel-2);
      border: 1px solid var(--line);
    }}
    .chip.best {{ color: var(--best-text); }}
    .chip.worst {{ color: var(--worst-text); }}
    .toc {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      margin: 0 0 28px;
    }}
    .toc a {{
      display: block;
      padding: 14px 16px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      color: var(--text);
      text-decoration: none;
      transition: border-color 0.15s ease, transform 0.15s ease;
    }}
    .toc a:hover {{
      border-color: var(--accent);
      transform: translateY(-1px);
    }}
    .suite {{
      margin-bottom: 28px;
      padding: 22px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
    }}
    .suite h2 {{
      margin: 0 0 8px;
      font-size: 1.25rem;
    }}
    .desc {{
      margin: 0 0 16px;
      color: var(--muted);
    }}
    .table-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 1200px;
      font-size: 0.92rem;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2),
    th:nth-child(3), td:nth-child(3),
    th:nth-child(4), td:nth-child(4) {{
      text-align: left;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #121820;
      color: #d7e0ec;
      z-index: 1;
    }}
    tr:hover td {{
      background: rgba(91, 157, 255, 0.06);
    }}
    tr.best td {{
      background: var(--best);
      color: var(--best-text);
    }}
    tr.worst td {{
      background: var(--worst);
      color: var(--worst-text);
    }}
    tr.timeout td {{
      background: #4d3a1f;
      color: #ffd18b;
    }}
    .empty {{
      color: var(--muted);
      font-style: italic;
    }}
  </style>
</head>
<body>
  <div class="page">
    <header>
      <h1>Message Queue Benchmark Report</h1>
      <p class="subtitle">Throughput + contention suites · green = highest consume msg/s in section</p>
      <p class="subtitle">Thread ratio: {summary['thread_ratio']} · HW threads: {summary['hw_threads']} · Repetitions: {summary['repetitions']}</p>
      <div class="legend">
        <span class="chip best">Best consume throughput in section</span>
        <span class="chip worst">Lowest consume throughput in section</span>
      </div>
    </header>
    <nav class="toc">
      {''.join(f"<a href='#section-{idx}'>{html.escape(title)}</a>" for idx, (title, _, _) in enumerate(sections, start=1))}
    </nav>
    {''.join(f"<div id='section-{idx}'>{render_section(title, description, rows)}</div>" for idx, (title, description, rows) in enumerate(sections, start=1))}
  </div>
</body>
</html>
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(doc, encoding="utf-8")
    print(f"Report written to {generated}")


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir
    output = args.output or (input_dir / "report.html")

    json_files = sorted(input_dir.glob("*.json"))
    if not json_files:
        raise SystemExit(f"No JSON files found in {input_dir}")

    sections: list[tuple[str, str, list[dict[str, Any]]]] = []
    payloads: list[dict[str, Any]] = []
    warnings: list[str] = []
    for path in json_files:
        payload, warning = load_benchmark_json(path)
        if warning:
            print(f"[WARN] {warning}", file=sys.stderr)
            warnings.append(warning)
        if payload is None:
            continue
        payloads.append(payload)
        benchmarks = payload.get("benchmarks", [])
        rows = pick_report_rows(benchmarks)
        title, description = suite_metadata(path)
        if warning and "truncated JSON repaired" in warning:
            description = f"{description} Partial run — JSON output was truncated and salvaged."
        sections.append((title, description, rows))

    if not sections:
        raise SystemExit(
            f"No valid benchmark JSON found in {input_dir}"
            + (f" ({len(warnings)} file(s) skipped)" if warnings else "")
        )

    build_report(sections, output, payloads)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
