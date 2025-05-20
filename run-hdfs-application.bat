@echo off
REM Script to run the US Accidents Analysis HDFS application

REM Set environment variables - update these paths to match your system
set JAVA_HOME=C:\JAVA\jdk-1.8
set HADOOP_HOME=C:\hadoop

REM Application jar
set APP_JAR=.\target\us-accidents-jar-with-dependencies.jar

REM Check if the application jar exists
if not exist "%APP_JAR%" (
    echo Error: Application jar not found at %APP_JAR%
    echo Please build the application first using: mvn clean package
    exit /b 1
)

REM Default HDFS directories
set HDFS_INPUT_DIR=/user/hadoop/us-accidents
set HDFS_OUTPUT_DIR=/tmp/usaccidents-output

REM Parse command line arguments
set USE_HIVE=false
set HIVE_JDBC=jdbc:hive2://localhost:10000/default

:parse_args
if "%~1"=="" goto :end_parse_args
if "%~1"=="--input" (
    set HDFS_INPUT_DIR=%~2
    shift
    goto :next_arg
)
if "%~1"=="--output" (
    set HDFS_OUTPUT_DIR=%~2
    shift
    goto :next_arg
)
if "%~1"=="--use-hive" (
    set USE_HIVE=true
    goto :next_arg
)
if "%~1"=="--hive-jdbc" (
    set HIVE_JDBC=%~2
    shift
    goto :next_arg
)

:next_arg
shift
goto :parse_args

:end_parse_args

echo Running US Accidents Analysis HDFS application...
echo HDFS Input directory: %HDFS_INPUT_DIR%
echo HDFS Output directory: %HDFS_OUTPUT_DIR%
echo Use Hive: %USE_HIVE%

REM Get Hadoop classpath using the wildcard-safe method
for /f "usebackq delims=" %%i in (`"%HADOOP_HOME%\bin\hadoop.cmd" classpath`) do set HADOOP_CLASSPATH=%%i

REM Set Hadoop native library path
set HADOOP_OPTS=-Djava.library.path="%HADOOP_HOME%\bin"

REM Check if Hadoop is running
"%HADOOP_HOME%\bin\hdfs.cmd" dfs -ls / > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Error: Cannot connect to HDFS. Please start Hadoop services first.
    echo Run 'start-all.cmd' to start Hadoop services.
    exit /b 1
)

REM Set command arguments
set CMD_ARGS=--input "%HDFS_INPUT_DIR%" --output "%HDFS_OUTPUT_DIR%"

if "%USE_HIVE%"=="true" (
    set CMD_ARGS=%CMD_ARGS% --use-hive --hive-jdbc "%HIVE_JDBC%"

    REM Check if Hive is running
    echo Testing Hive connection...
    "%HADOOP_HOME%\bin\beeline.cmd" -u "%HIVE_JDBC%" -e "show databases;" > nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo Error: Cannot connect to Hive. Please start Hive services first.
        echo Run 'hiveserver2' to start Hive server.
        exit /b 1
    )
    echo Hive connection successful.
)

REM Run the application with HDFS support
echo Starting application...
"%JAVA_HOME%\bin\java" -cp "%APP_JAR%;%HADOOP_CLASSPATH%" %HADOOP_OPTS% com.usaccidents.USAccidentsHDFSApp %CMD_ARGS%

if %ERRORLEVEL% EQU 0 (
    echo Application completed successfully!

    if "%USE_HIVE%"=="true" (
        echo Results are available in Hive tables: us_accidents, location_analysis, severity_analysis, time_analysis, and weather_analysis
        echo You can query them using Hive CLI or Beeline.

        echo.
        echo Example Hive queries:
        echo "%HADOOP_HOME%\bin\beeline.cmd" -u "%HIVE_JDBC%" -e "SELECT state, COUNT(*) as count FROM location_analysis GROUP BY state ORDER BY count DESC LIMIT 10;"
        echo "%HADOOP_HOME%\bin\beeline.cmd" -u "%HIVE_JDBC%" -e "SELECT weather_condition, accident_count FROM weather_analysis ORDER BY accident_count DESC LIMIT 10;"
    ) else (
        echo Results are available in HDFS: %HDFS_OUTPUT_DIR%

        REM Copy results from HDFS to local filesystem for convenience
        echo Copying results from HDFS to local directory...
        "%HADOOP_HOME%\bin\hdfs.cmd" dfs -copyToLocal %HDFS_OUTPUT_DIR%/* .\output\
        if %ERRORLEVEL% EQU 0 (
            echo Results copied to local directory: .\output\
        ) else (
            echo Failed to copy results to local directory. They are still available in HDFS.
        )
    )
) else (
    echo Application execution failed. Please check the error messages above.
)

exit /b %ERRORLEVEL%