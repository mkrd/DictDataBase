alias t := test
alias p := profiler
alias bp := benchmark_parallel
alias bt := benchmark_threaded
alias ba := benchmark_async

default:
    @just --list

test:
    poetry run pytest --cov=dictdatabase --cov-report term-missing
    rm ./.coverage

profiler:
    poetry run python profiler.py

benchmark_parallel:
    poetry run python tests/benchmark/run_parallel.py

benchmark_threaded:
    poetry run python tests/benchmark/run_threaded.py

benchmark_async:
    poetry run python tests/benchmark/run_async.py
