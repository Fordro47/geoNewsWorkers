#! /bin/bash
cd /opt/geoNewsWorkers
source workers/bin/activate
python sequential_runner.py >> logs/sequential_runner.log