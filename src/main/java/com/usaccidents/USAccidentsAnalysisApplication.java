package com.usaccidents;

import com.usaccidents.model.Accident;
import com.usaccidents.operators.AccidentCSVParser;
import com.usaccidents.operators.AccidentAnalyzer;
import com.usaccidents.operators.AccidentOutputOperator;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "USAccidentsAnalysisApplication")
public class USAccidentsAnalysisApplication implements StreamingApplication
{
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        // Create and configure file input operator
        FSRecordReaderModule fileInput = dag.addModule("fileInput", new FSRecordReaderModule());
        fileInput.setDirectory("/user/hadoop/accidents-data");
        fileInput.setScanner(FSRecordReaderModule.LINE_SCANNER);

        // Create CSV parsing operator
        AccidentCSVParser csvParser = dag.addOperator("csvParser", new AccidentCSVParser());

        // Create accident analyzer operator
        AccidentAnalyzer analyzer = dag.addOperator("analyzer", new AccidentAnalyzer());

        // Create output operator
        AccidentOutputOperator output = dag.addOperator("output", new AccidentOutputOperator());

        // Create the stream connections
        dag.addStream("rawData", fileInput.records, csvParser.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("parsedAccidents", csvParser.output, analyzer.input);
        dag.addStream("analysisResults", analyzer.output, output.input);
    }
}