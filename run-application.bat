@echo off
REM Script to run the US Accidents Analysis standalone application

REM Set environment variables - update these paths to match your system
set JAVA_HOME=C:\JAVA\jdk-1.8

REM Application jar
set APP_JAR=.\target\us-accidents-1.0-SNAPSHOT.jar

REM Check if the application jar exists
if not exist "%APP_JAR%" (
    echo Error: Application jar not found at %APP_JAR%
    echo Please build the application first using: mvn clean package
    exit /b 1
)

REM Default directories
set INPUT_DIR=E:\us_accidents_march23_clean
set OUTPUT_DIR=E:\us_accidents_march23_clean\output

REM Allow overriding directories from command line
if not "%~1"=="" set INPUT_DIR=%~1
if not "%~2"=="" set OUTPUT_DIR=%~2

echo Running US Accidents Analysis application...
echo Input directory: %INPUT_DIR%
echo Output directory: %OUTPUT_DIR%

REM Run the application
"%JAVA_HOME%\bin\java" -cp "%APP_JAR%" com.usaccidents.USAccidentsStandaloneApp "%INPUT_DIR%" "%OUTPUT_DIR%"

if %ERRORLEVEL% EQU 0 (
    echo Application completed successfully!
    echo Results are available in: %OUTPUT_DIR%
) else (
    echo Application execution failed. Please check the error messages above.
)