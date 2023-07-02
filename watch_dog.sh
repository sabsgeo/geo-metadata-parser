#!/bin/bash
ST=$(sudo systemctl status mongod | grep "Active" | grep "running")
DD=$(date)
if [ -z "$ST" ]; then
        echo "${DD}:- Mongo DB need to be restarted"
        /home/ubuntu/geo-metadata-parser/stop_docker.sh
        sudo systemctl restart mongod | grep "running"
        sleep 5
        P=$(cd /home/ubuntu/geo-metadata-parser/ && ./run_docker.sh)
else
        echo "${DD}:- Everything is good"
fi
