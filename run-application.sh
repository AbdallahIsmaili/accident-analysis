#!/bin/bash

# Script to launch the US Accidents Analysis application on Apache Apex

# Set environment variables
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export APEX_HOME=/usr/local/apex

# Application jar
APP_JAR="./target/us-accidents-1.0-SNAPSHOT-apexapp.jar"

# Check if the application jar exists
if [ ! -f "$APP_JAR" ]; then
    echo "Error: Application jar not found at $APP_JAR"
    echo "Please build the application first using: mvn clean package"
    exit 1
fi

# Ensure Hadoop is running
hadoop_status=$(ps -ef | grep -v grep | grep -c "hadoop")
if [ $hadoop_status -eq 0 ]; then
    echo "Error: Hadoop does not appear to be running."
    echo "Please start Hadoop before running this application."
    exit 1
fi

# Properties file for configuration
PROPS_FILE="./src/main/resources/application.properties"

echo "Launching US Accidents Analysis application..."
echo "Using properties file: $PROPS_FILE"

# Use Apex CLI to launch the application
$APEX_HOME/bin/apex launch -D$PROPS_FILE \
                          -Dapex.application.name=USAccidentsAnalysis \
                          $APP_JAR

if [ $? -eq 0 ]; then
    echo "Application launched successfully!"
    echo "Check the log files for application progress."
else
    echo "Application launch failed. Please check the error messages above."
fi