#!/bin/sh
while [ $# -gt 0 ]; do case $1 in


  --test|-t)
    poetry run pytest --cov=dictdatabase --cov-report term-missing
    rm ./.coverage
    shift ;;


    --profiler|-p)
    poetry run python profiler.py
    shift ;;



  *|-*|--*)
    echo "Unknown option $1"
    echo "Usage: [ -t | --test ] [ -p | --profiler ]"
    exit 2
    exit 1 ;;


esac; done
