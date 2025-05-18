package com.usaccidents;

import com.usaccidents.model.Accident;
import com.usaccidents.operators.AccidentCSVParser;
import com.usaccidents.operators.AccidentAnalyzer;
import com.usaccidents.operators.AccidentOutputOperator;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
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
        LineByLineFileInputOperator fileInput = dag.addOperator("fileInput", new LineByLineFileInputOperator());
        fileInput.setDirectory("/user/hadoop/us-accidents");
        fileInput.setMatchPattern(".*\\.csv");

        // Create CSV parsing operator
        AccidentCSVParser csvParser = dag.addOperator("csvParser", new AccidentCSVParser());

        // Create accident analyzer operator
        AccidentAnalyzer analyzer = dag.addOperator("analyzer", new AccidentAnalyzer());

        // Create output operator
        AccidentOutputOperator output = dag.addOperator("output", new AccidentOutputOperator());

        // Create the stream connections
        dag.addStream("rawData", fileInput.output, csvParser.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("parsedAccidents", csvParser.output, analyzer.input);
        dag.addStream("analysisResults", analyzer.output, output.input);
    }
}