@echo off
setlocal ENABLEDELAYEDEXPANSION

rem Ensure we run from the repository root (this script's directory)
pushd "%~dp0" >nul

echo Building .NET project...
dotnet build BeastieBot3.sln
if errorlevel 1 (
 popd >nul
 exit /b 1
)

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

set "ARGS="
if /I "%~1"=="rebuild" (
 echo Rebuilding image with no cache and pulling latest base layers...
 %COMPOSE% build --no-cache --pull
 set ERR=%ERRORLEVEL%
) else (
 echo Building image (cached)...
 %COMPOSE% build
 set ERR=%ERRORLEVEL%
)

popd >nul
exit /b %ERR%
