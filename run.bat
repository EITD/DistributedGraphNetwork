@echo off

start cmd /k python worker.py 0
start cmd /k python worker.py 1
start cmd /k python worker.py 2
start cmd /k python worker.py 3

timeout /t 5 /nobreak

start cmd /k python client.py