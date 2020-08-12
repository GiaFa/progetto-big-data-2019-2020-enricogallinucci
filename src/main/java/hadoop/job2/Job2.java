package hadoop.job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class Job2 {
   public static void main(String[] args) throws IOException {
       Configuration conf = new Configuration();
       Job job = Job.getInstance(conf,"Hadoop Project BigData");
       Path beerPath = new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
       Path breweriesPath = new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
       Path reviewsPath = new Path("giovannim/dataset/input/datasetprogetto/reviews.csv");
       Path outputPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop");

       FileSystem fs = FileSystem.get(conf);

       if(fs.exists(outputPath)) {
           fs.delete(outputPath, true);
       }

       MultipleInputs.addInputPath(job,beerPath, TextInputFormat.class,BeersMapper.class);
       MultipleInputs.addInputPath(job,breweriesPath, TextInputFormat.class,BreweriesMapper.class);
       MultipleInputs.addInputPath(job,reviewsPath, TextInputFormat.class,ReviewsMapper.class);


    }
}
