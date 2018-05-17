import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


/**
 * Created by spark on 5/13/18.
 */
public class HdfsClient {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs=FileSystem.get(new URI("hdfs://master1:9000"),conf,"spark");

        DatanodeInfo[] dataNodeStats=((DistributedFileSystem) fs).getDataNodeStats();
        for (DatanodeInfo dinfo:dataNodeStats){
            System.out.println(dinfo.getHostName());
        }
    }

    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles=fs.listFiles(new Path("/"),true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus=listFiles.next();

            System.out.println(fileStatus.getPath().getName());//filename
            System.out.println(fileStatus.getBlockSize());//block size
            System.out.println(fileStatus.getPermission());//file permission
            System.out.println(fileStatus.getLen());//bytes

            BlockLocation[] blockLocations=fileStatus.getBlockLocations();

            for (BlockLocation bl:blockLocations){
                System.out.println("block-length:"+bl.getLength()+"--"+"block-offset:"+bl.getOffset());
                String[] hosts=bl.getHosts();

                for (String host:hosts){
                    System.out.println(host);
                }
            }

            System.out.println("*********************************************");
        }
    }

    @Test
    public void testListAll() throws IOException {
        FileStatus[] listStatus=fs.listStatus(new Path("/"));

        String flag="d-- ";
        for (FileStatus fstatus:listStatus){
            if (fstatus.isFile())
                flag="f-- ";
            System.out.println(flag+fstatus.getPath().getName());
            System.out.println(fstatus.getPermission());
        }
    }


}
