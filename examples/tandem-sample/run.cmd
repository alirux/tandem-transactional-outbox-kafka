@echo off
:: Runs the Tandem sample application (requires Docker).
::
:: Usage (from any directory):
::   examples\tandem-sample\run.cmd
::   run.cmd           (when inside examples\tandem-sample\)
::
:: The script navigates to the project root automatically so that Gradle and
:: Testcontainers can locate the schema files regardless of where you call it from.

setlocal
:: The project root is the nearest directory above this script that holds settings.gradle.kts.
for %%I in ("%~dp0.") do set "ROOT=%%~fI"
:find_root
if exist "%ROOT%\settings.gradle.kts" goto found_root
for %%I in ("%ROOT%\..") do set "PARENT=%%~fI"
if "%PARENT%"=="%ROOT%" (
    echo Cannot find settings.gradle.kts above this script 1>&2
    exit /b 1
)
set "ROOT=%PARENT%"
goto find_root
:found_root
cd /d "%ROOT%"

call gradlew.bat :tandem-sample:run --console=plain
