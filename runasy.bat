@echo off

start cmd /k python3 worker_asy.py 0
start cmd /k python3 worker_asy.py 1
start cmd /k python3 worker_asy.py 2
start cmd /k python3 worker_asy.py 3

timeout /t 5 /nobreak

start cmd /k 