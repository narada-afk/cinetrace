"""
Trend-awareness layer.

Google Trends → Cinetrace entity match → one LLM reasoning step → Insight.
See docs/trend-awareness.md for the full flow and failure/SKIP behaviour.

This package does NOT implement its own trend scoring, trend database, or
entity-resolution ML — it is a thin adapter from "what is trending right
now" (Google's own trending feed) onto the engine's existing Insight
contract, so the result flows through the unmodified Twitter generator,
validator, Telegram review and poster.
"""
