package hadoop.commonjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Common {
    public static class SonCommon{
        private static final Configuration conf = new Configuration();
        public static Configuration commonConf(){
            return conf;
        }
        public static Job commonJob() throws IOException {
            return Job.getInstance(conf,"Beers and Breweries fusion");
        }
        public static Path beerPath(){
            return new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
        }
        public static Path breweryPath(){
            return new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
        }
    }
}
