"""Refresh the independent Python oracle fixture consumed by Java harness tests."""
from adpulse.common import load_rules, write_json
from adpulse.generator import generate, receipts
from adpulse.oracle import calculate

data = generate(users=40, seed=23, scenario="mixed")
packets = receipts(data["transport"])
write_json("tests/fixtures/oracle-baseline.json", {"rules": load_rules(), "packets": packets, "expected": calculate(packets)})
