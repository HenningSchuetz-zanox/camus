package com.linkedin.camus.etl.kafka.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HdfsRenameServiceTest {
    
    private static final String TEMP_DIRECTORY_PREFIX = "hdfs-service-test";
    
    private static final String SOURCE_FILE_NAME = "source.txt";
    private static final String TARGET_FILE_NAME = "target.txt";

    private static final String SHORT_CONTENT = "short content";
    private static final String LONG_CONTENT = "long very long content that should never be overriden";
    
    private File baseTestDirectory;
    private HdfsRenameService hdfsRenameService;

    @Before
    public void setUp() throws Exception {
        baseTestDirectory = Files.createTempDirectory(TEMP_DIRECTORY_PREFIX).toFile();
        hdfsRenameService = new HdfsRenameService(new Configuration());
    }
    
    @After
    public void tearDown() throws Exception {
        baseTestDirectory.delete();
    }

    @Test
    public void shouldRenameIfTargetDoesNotExist() throws Exception {
        createFile(SOURCE_FILE_NAME, SHORT_CONTENT);
        Assert.assertTrue(hdfsRenameService.rename(new Path(baseTestDirectory.getAbsolutePath(), SOURCE_FILE_NAME),
                new Path(baseTestDirectory.getAbsolutePath(), TARGET_FILE_NAME)));
        Assert.assertEquals(SHORT_CONTENT, readFile(TARGET_FILE_NAME));
    }
    
    @Test
    public void shouldRenameIfTargetIsSmaller() throws Exception {
        createFile(SOURCE_FILE_NAME, LONG_CONTENT);
        createFile(TARGET_FILE_NAME, SHORT_CONTENT);
        Assert.assertTrue(hdfsRenameService.rename(new Path(baseTestDirectory.getAbsolutePath(), SOURCE_FILE_NAME),
                new Path(baseTestDirectory.getAbsolutePath(), TARGET_FILE_NAME)));
        Assert.assertEquals(LONG_CONTENT, readFile(TARGET_FILE_NAME));
    }
    
    @Test
    public void shouldRenameIfTargetIsEqual() throws Exception {
        createFile(SOURCE_FILE_NAME, SHORT_CONTENT);
        createFile(TARGET_FILE_NAME, SHORT_CONTENT);
        Assert.assertTrue(hdfsRenameService.rename(new Path(baseTestDirectory.getAbsolutePath(), SOURCE_FILE_NAME),
                new Path(baseTestDirectory.getAbsolutePath(), TARGET_FILE_NAME)));
        Assert.assertEquals(SHORT_CONTENT, readFile(TARGET_FILE_NAME));
    }
    
    @Test
    public void shouldNotRenameIfTargetIsLarger() throws Exception {
        createFile(SOURCE_FILE_NAME, SHORT_CONTENT);
        createFile(TARGET_FILE_NAME, LONG_CONTENT);
        Assert.assertTrue(hdfsRenameService.rename(new Path(baseTestDirectory.getAbsolutePath(), SOURCE_FILE_NAME),
                new Path(baseTestDirectory.getAbsolutePath(), TARGET_FILE_NAME)));
        Assert.assertEquals(LONG_CONTENT, readFile(TARGET_FILE_NAME));
    }

    private void createFile(String fileName, String content) throws Exception {
        
        PrintWriter writer = null;
        
        try {
            writer = new PrintWriter(new File(baseTestDirectory, fileName));
            writer.println(content);
        } finally {
            if(writer != null) {
                writer.close();
            }
        }
    }
    
    private String readFile(String fileName) throws Exception {
        
        BufferedReader reader = null;
        
        try  {
            reader = new BufferedReader(new FileReader(new File(baseTestDirectory, fileName)));
            return reader.readLine();
        } finally {
            if(reader != null) {
                reader.close();
            }
        }
        
    }
}
