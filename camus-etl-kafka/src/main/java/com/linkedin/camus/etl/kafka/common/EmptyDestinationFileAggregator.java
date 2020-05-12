package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.DestinationFileAggregator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.Path;

public class EmptyDestinationFileAggregator implements DestinationFileAggregator {
    
    @Override
    public void addDestinationFile(Path path) {
        // do nothing
    }
    
    public void setJobContext(JobContext job) {
    }

    public JobContext getJobContext() {
        return null;
    }
}
