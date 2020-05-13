package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsRenameService {
    
    private Configuration conf;
    
    public HdfsRenameService(Configuration conf) {
        this.conf = conf;
    }
    
    public boolean rename(Path source, Path target) throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            if (!fs.exists(target)) {
                fs.rename(source, target);
            } else if (shouldOverride(fs, source, target)) {
                fs.delete(target, false);
                fs.rename(source, target);
            }
            return true;
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }

    private boolean shouldOverride(FileSystem fs, Path source, Path target) throws IOException {
        return isSourceLargerThanTarget(fs, source, target);
    }
    
    private boolean isSourceLargerThanTarget(FileSystem fs, Path source, Path target) throws IOException {
        return fs.getFileStatus(source).getLen() > fs.getFileStatus(target).getLen();
    }
}
