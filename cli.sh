#!/bin/sh
while [[ $# -gt 0 ]]; do case $1 in


  --test|-t)
    poetry run python testing/run_tests.py
    rm ./.coverage
    shift ;;



  *|-*|--*)
    echo "Unknown option $1"
    echo "Usage: [ -t | --test ] [ -p | --profiler ]"
    exit 2
    exit 1 ;;


esac; done
