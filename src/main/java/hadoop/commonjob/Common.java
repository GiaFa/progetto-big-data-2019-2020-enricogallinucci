package hadoop.commonjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Common {
    public static final int LOW_CLASS = 2;
    public static final int MEDIUM_CLASS = 4;
    public static final int HIGH_CLASS = 5;
    private static final Configuration conf = new Configuration();
        public static Configuration commonConf(){
            return conf;
        }
        public static Job commonJob(String jobName) throws IOException {
            return Job.getInstance(conf,jobName);
        }
        public static Path beerPath(){
            return new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
        }
        public static Path breweryPath(){
            return new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
        }
}
