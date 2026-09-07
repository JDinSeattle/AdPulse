"""A scheduler-friendly immutable daily candidate release; promotion is explicit."""
from datetime import datetime, timezone

from adpulse.cli import main

if __name__ == "__main__":
    release = "replay-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    main(["replay", "--from-s3", "--release", release, "--output", f"artifacts/{release}", "--publish"])
