@echo off

start cmd /k python3 worker.py 0
start cmd /k python3 worker.py 1
start cmd /k python3 worker.py 2
start cmd /k python3 worker.py 3

timeout /t 5 /nobreak

start cmd /k python3 client.py