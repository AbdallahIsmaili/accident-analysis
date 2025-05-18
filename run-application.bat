@echo off
REM Script to launch the US Accidents Analysis application on Apache Apex for Windows

REM Set environment variables - update these paths to match your system
set HADOOP_HOME=C:\hadoop
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_321
set APEX_HOME=C:\apex

REM Application jar
set APP_JAR=.\target\us-accidents-1.0-SNAPSHOT-apexapp.jar

REM Check if the application jar exists
if not exist "%APP_JAR%" (
    echo Error: Application jar not found at %APP_JAR%
    echo Please build the application first using: mvn clean package
    exit /b 1
)

REM Properties file for configuration
set PROPS_FILE=.\src\main\resources\application.properties

echo Launching US Accidents Analysis application...
echo Using properties file: %PROPS_FILE%

REM Use Apex CLI to launch the application
call %APEX_HOME%\bin\apex.bat launch -D%PROPS_FILE% ^
                              -Dapex.application.name=USAccidentsAnalysis ^
                              %APP_JAR%

if %ERRORLEVEL% EQU 0 (
    echo Application launched successfully!
    echo Check the log files for application progress.
) else (
    echo Application launch failed. Please check the error messages above.
)