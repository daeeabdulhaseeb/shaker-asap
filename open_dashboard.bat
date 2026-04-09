@echo off
echo Starting ASAP Sales Dashboard...
cd /d "%~dp0docs"
start "" "http://localhost:8000/asap.html"
python -m http.server 8000
