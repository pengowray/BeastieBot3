@echo off
setlocal ENABLEDELAYEDEXPANSION

rem Ensure we run from the repository root (this script's directory)
pushd "%~dp0" >nul

rem Pick Docker Compose command (v2 plugin preferred)
set "COMPOSE=docker compose"
docker compose version >nul2>&1
if errorlevel 1 (
 docker-compose version >nul2>&1
 if errorlevel 1 (
 echo ERROR: Docker Compose not found. Install Docker Desktop or docker-compose.
 popd >nul
 exit /b 1
 ) else (
 set "COMPOSE=docker-compose"
 )
)

rem Run the service and pass all CLI args to the app (after the service name)
%COMPOSE% run --rm app %*
set ERR=%ERRORLEVEL%

popd >nul
exit /b %ERR%
