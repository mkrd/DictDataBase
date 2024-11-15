default:
    @just --list

alias t := test
test:
    poetry run pytest --cov=dictdatabase --cov-report term-missing
    rm ./.coverage

alias p := profiler
profiler:
    poetry run python profiler.py

alias bp := benchmark_parallel
benchmark_parallel:
    poetry run python tests/benchmark/run_parallel.py

alias bt := benchmark_threaded
benchmark_threaded:
    poetry run python tests/benchmark/run_threaded.py

alias ba := benchmark_async
benchmark_async:
    poetry run python tests/benchmark/run_async.py
