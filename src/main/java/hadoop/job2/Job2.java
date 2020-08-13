package hadoop.job2;


import hadoop.BeerOrBrewery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Prima classificare le birre in base alla media voto (ad es: voti alti/medi/bassi, oppure ottima/buona/discreta/ecc.,
 * a seconda dei dati), quindi calcolare per ogni birreria la quantità di birre in ogni classe; eventualmente le si può
 * anche ordinare sulla base di uno score (ipotizzando di associare ad ogni classe un punteggio).
 */
public class Job2 {
   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       Configuration conf = new Configuration();
       Job jobAvg = Job.getInstance(conf,"AvgReviews");
       Job jobCreateClasses = Job.getInstance(conf,"Beer Classes");
       Job jobBeerAndBreweries = Job.getInstance(conf,"Beers and Breweries fusion");
       Path beerPath = new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
       Path breweriesPath = new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
       Path reviewsPath = new Path("giovannim/dataset/input/datasetprogetto/reviews.csv");
       Path avgTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/AvgTmp");
       Path beersAndBreweriesTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BeersAndBreweries");

       FileSystem fs = FileSystem.get(conf);

       if(fs.exists(avgTmpPath)) {
           fs.delete(avgTmpPath, true);
       }

       if(fs.exists(beersAndBreweriesTmpPath)) {
           fs.delete(beersAndBreweriesTmpPath, true);
       }

//       FileInputFormat.addInputPath(jobAvg,reviewsPath);
//       FileOutputFormat.setOutputPath(jobAvg,avgTmpPath);
//       jobAvg.setJarByClass(Job2.class);
//       jobAvg.setMapperClass(ReviewsMapper.class);
//       jobAvg.setReducerClass(ReviewsAvg.class);
//
//       jobAvg.setMapOutputKeyClass(IntWritable.class);
//       jobAvg.setMapOutputValueClass(DoubleWritable.class);
//
//       jobAvg.setOutputKeyClass(IntWritable.class);
//       jobAvg.setOutputValueClass(DoubleWritable.class);


       MultipleInputs.addInputPath(jobBeerAndBreweries,beerPath, TextInputFormat.class,BeersMapper.class);
       MultipleInputs.addInputPath(jobBeerAndBreweries,breweriesPath, TextInputFormat.class,BreweriesMapper.class);
       SequenceFileOutputFormat.setOutputPath(jobBeerAndBreweries, beersAndBreweriesTmpPath);
       jobBeerAndBreweries.setJarByClass(Job2.class);
       jobBeerAndBreweries.setReducerClass(BeersAndBreweriesReducer.class);
       jobBeerAndBreweries.setMapOutputKeyClass(IntWritable.class);
       jobBeerAndBreweries.setMapOutputValueClass(BeerOrBrewery.class);
       jobBeerAndBreweries.setOutputKeyClass(IntWritable.class);
       jobBeerAndBreweries.setOutputValueClass(BeerOrBrewery.class);
       jobBeerAndBreweries.setOutputFormatClass(SequenceFileOutputFormat.class);

       if (!jobBeerAndBreweries.waitForCompletion(true)) {
           System.exit(1);
       }

       SequenceFileInputFormat.addInputPath(jobCreateClasses,beersAndBreweriesTmpPath);
       jobCreateClasses.setInputFormatClass(SequenceFileInputFormat.class);
       FileOutputFormat.setOutputPath(jobCreateClasses,avgTmpPath);
       jobCreateClasses.setJarByClass(Job2.class);
      jobCreateClasses.setMapperClass(TestMapper.class);
      jobCreateClasses.setReducerClass(TestReducer.class);

      jobCreateClasses.setMapOutputKeyClass(IntWritable.class);
      jobCreateClasses.setMapOutputValueClass(Text.class);

      jobCreateClasses.setOutputKeyClass(IntWritable.class);
      jobCreateClasses.setOutputValueClass(Text.class);
       System.exit(jobCreateClasses.waitForCompletion(true) ? 0 : 1);

//       if (!jobAvg.waitForCompletion(true)) {
//           System.exit(1);
//       }

//



    }
}
