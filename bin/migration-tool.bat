@echo off
rem =============================================================================
rem Hadoop Cluster Migration Tool - Windows Launch Script
rem Migrates data from CDH 7.1.9 (Hive 2.1.1) to Huawei MRS 3.5.0 (Hive 3.1.0)
rem =============================================================================

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "CONF_DIR=%PROJECT_DIR%\conf"
set "LIB_DIR=%PROJECT_DIR%\lib"
set "STATE_DIR=%PROJECT_DIR%\state"
set "LOGS_DIR=%PROJECT_DIR%\logs"

rem Default configuration
set "CONFIG_FILE="
set "VERBOSE=0"

rem Parse arguments
:getArgs
if "%~1"=="" goto :checkEnv
if /i "%~1"=="--config" (
    set "CONFIG_FILE=%~2"
    shift
    shift
    goto :getArgs
)
if /i "%~1"=="--verbose" (
    set "VERBOSE=1"
    shift
    goto :getArgs
)
if /i "%~1"=="--help" (
    goto :showHelp
)
shift
goto :getArgs

:checkEnv
rem Check Java installation
where java >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Java is not installed or not in PATH.
    echo Please install Java 11 or higher.
    exit /b 1
)

rem Get Java version
java -version 2>&1 | findstr /i "version" >nul
if errorlevel 1 (
    echo [WARNING] Could not determine Java version.
)

rem Check if config file is provided, use default if not
if not defined CONFIG_FILE (
    if exist "%CONF_DIR%\config.yaml" (
        set "CONFIG_FILE=%CONF_DIR%\config.yaml"
    ) else (
        echo [ERROR] No config file specified and default not found.
        echo Please use: %0 --config ^<path-to-config^>
        exit /b 1
    )
)

rem Create necessary directories
if not exist "%STATE_DIR%" mkdir "%STATE_DIR%"
if not exist "%LOGS_DIR%" mkdir "%LOGS_DIR%"

rem Set classpath
set "CLASSPATH=%LIB_DIR%\*;%CONF_DIR%"
if exist "%CONF_DIR%\cdh-client-conf" (
    for %%f in ("%CONF_DIR%\cdh-client-conf\*.xml") do set "CLASSPATH=!CLASSPATH!;%CONF_DIR%\cdh-client-conf"
)
if exist "%CONF_DIR%\mrs-client-conf" (
    for %%f in ("%CONF_DIR%\mrs-client-conf\*.xml") do set "CLASSPATH=!CLASSPATH!;%CONF_DIR%\mrs-client-conf"
)

rem Build the command
set "MAIN_CLASS=com.hadoop.migration.Main"
set "CMD=java -cp "%CLASSPATH%" %MAIN_CLASS% --config "%CONFIG_FILE%""

if "%VERBOSE%"=="1" (
    echo [INFO] Project directory: %PROJECT_DIR%
    echo [INFO] Config file: %CONFIG_FILE%
    echo [INFO] Classpath: %CLASSPATH%
    echo [INFO] Command: %CMD%
    echo.
)

rem Execute
echo [INFO] Starting Hadoop Migration Tool...
echo [INFO] Log file: %LOGS_DIR%\migration_YYYYMMDD_HHMMSS.log
echo.

%CMD%

set "EXIT_CODE=%ERRORLEVEL%"

if %EXIT_CODE% equ 0 (
    echo.
    echo [SUCCESS] Migration completed successfully.
) else (
    echo.
    echo [ERROR] Migration failed with exit code: %EXIT_CODE%
)

exit /b %EXIT_CODE%

:showHelp
echo.
echo Hadoop Cluster Migration Tool
echo =================================
echo.
echo Usage: %0 [OPTIONS]
echo.
echo Options:
echo   --config ^<path^>    Path to configuration file (required if no default in conf/)
echo   --verbose           Enable verbose output
echo   --help              Show this help message
echo.
echo Environment Variables:
echo   HADOOP_HOME_CDH    CDH Hadoop installation directory
echo   HADOOP_HOME_MRS    MRS Hadoop installation directory
echo   HIVE_HOME_CDH      CDH Hive installation directory
echo   HIVE_HOME_MRS      MRS Hive installation directory
echo.
echo Example:
echo   %0 --config conf\config.yaml
echo.
exit /b 0
