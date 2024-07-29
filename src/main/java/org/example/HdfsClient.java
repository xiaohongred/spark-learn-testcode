package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    private FileSystem fs;

    @Before
    public void init() throws  IOException, URISyntaxException, InterruptedException {
        
        Configuration config = new Configuration();

        fs = FileSystem.get(new URI("hdfs://node1:8020"), config,"root");

    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        fs.mkdirs(new Path("/hdfsclient/test/"));
    }
    @Test
    public void testPut() throws IOException {
        Path  src = new Path("C:\\Users\\10279\\bigdata-software\\hadoop-3.3.5\\README.txt");
        Path  des = new Path("/hdfsclient/test/");
        fs.copyFromLocalFile(false, true, src, des);
    }

    @Test
    public void testGet() throws IOException {
        Path  des = new Path("C:\\Users\\10279\\bigdata-software\\hadoop-3.3.5\\testGet.txt");
        Path  src = new Path("/hdfsclient/test/README.txt");
        fs.copyToLocalFile(false, src, des);
    }

    @Test
    public void testRm() throws IOException {
        // 删除文件
        fs.delete(new Path("/hdfsclient/test/README.txt"), false);

        // 删除空目录
        fs.delete(new Path("/hdfsclient/test"), false);

        // 删除非空目录
        fs.delete(new Path("/hdfsclient"), true);
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        fs.rename(new Path("/hdfsclient/test/README.txt"), new Path("/hdfsclient/test/README_rename.txt"));
    }


    @Test
    public void testListFile() throws IOException, InterruptedException, URISyntaxException {
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }


}
