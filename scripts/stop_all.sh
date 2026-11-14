#!/bin/bash
# Stop all running experiment scripts, producers, and consumers.

echo "Stopping all XRPL processes..."

pkill -f "xrpl_to_kafka.py" && echo "  stopped xrpl_to_kafka" || echo "  xrpl_to_kafka not running"
pkill -f "xrpl_to_file.py"  && echo "  stopped xrpl_to_file"  || echo "  xrpl_to_file not running"
pkill -f "file_demo.py"     && echo "  stopped file_demo"     || echo "  file_demo not running"
pkill -f "consumers.validator.consumer" && echo "  stopped validator consumer" || echo "  validator consumer not running"

echo "Done."
