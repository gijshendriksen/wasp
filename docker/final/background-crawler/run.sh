#!/bin/bash

elasticsearch_port=${ELASTICSEARCH_PORT:=9200}

case $1 in
  start)
    echo "Waiting for elasticsearch to start"
    until grep -q "started" /home/user/srv/elasticsearch/sysout.txt ; do
      sleep 1
    done
    echo "Should work now!"

    source env/bin/activate
    python crawler.py urls.json 1> sysout.txt 2> syserr.txt &
    echo $! > pid.txt
    ;;
  stop)
    pid=$(cat pid.txt)
    kill $pid
    ;;
  *)
    echo Unknown command: $1
esac
