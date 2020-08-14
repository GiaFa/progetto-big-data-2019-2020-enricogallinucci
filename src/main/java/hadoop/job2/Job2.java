package hadoop.job2;

import hadoop.BeerOrBrewery;
import hadoop.Brewery;
import hadoop.commonjob.BeersMapper;
import hadoop.commonjob.BreweriesMapper;
import hadoop.commonjob.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


/**
 * Prima classificare le birre in base alla media voto (ad es: voti alti/medi/bassi, oppure ottima/buona/discreta/ecc.,
 * a seconda dei dati), quindi calcolare per ogni birreria la quantità di birre in ogni classe; eventualmente le si può
 * anche ordinare sulla base di uno score (ipotizzando di associare ad ogni classe un punteggio).
 */
public class Job2 {
   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       Configuration conf = Common.commonConf();
//       ControlledJob cont = new ControlledJob(conf);
//       ControlledJob cont1 = new ControlledJob(conf);
//       ControlledJob cont2 = new ControlledJob(conf);
//       cont2.addDependingJob(cont);
//       cont2.addDependingJob(cont1);
//       JobControl jc = new JobControl("aaa");
//       jc.addJob(cont);
//       jc.addJob(cont1);
//       jc.addJob(cont2);
//       jc.run();
       Job jobAvg = Common.commonJob("AvgReviews");
       Job jobBeerAndBreweries = Common.commonJob("Beers and Breweries fusion");
       Job jobBreweriesClasses = Common.commonJob("Beer Classes");
       Job jobFinal = Common.commonJob("Final Job");
       Path beerPath = new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
       Path breweriesPath = new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
       Path reviewsPath = new Path("giovannim/dataset/input/datasetprogetto/reviews.csv");
       Path avgTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/AvgTmp");
       Path beersAndBreweriesTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BeersAndBreweries");
       Path breweriesClassesPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BreweriesClasses");
       Path resultPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/job2");
       FileSystem fs = FileSystem.get(conf);

//       if(fs.exists(avgTmpPath)) {
//           fs.delete(avgTmpPath, true);
//       }
//
//       if(fs.exists(beersAndBreweriesTmpPath)) {
//           fs.delete(beersAndBreweriesTmpPath, true);
//       }
//
//       if(fs.exists(breweriesClassesPath)) {
//           fs.delete(breweriesClassesPath, true);
//       }


       if(fs.exists(resultPath)) {
           fs.delete(resultPath, true);
       }

//       FileInputFormat.addInputPath(jobAvg,reviewsPath);
//       SequenceFileOutputFormat.setOutputPath(jobAvg,avgTmpPath);
//       jobAvg.setJarByClass(Job2.class);
//       jobAvg.setMapperClass(ReviewsMapper.class);
//       jobAvg.setReducerClass(ReviewsAvg.class);
//       jobAvg.setMapOutputKeyClass(IntWritable.class);
//       jobAvg.setMapOutputValueClass(DoubleWritable.class);
//       jobAvg.setOutputKeyClass(IntWritable.class);
//       jobAvg.setOutputValueClass(IntWritable.class);
//       jobAvg.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//       MultipleInputs.addInputPath(jobBeerAndBreweries,beerPath, TextInputFormat.class, BeersMapper.class);
//       MultipleInputs.addInputPath(jobBeerAndBreweries,breweriesPath, TextInputFormat.class, BreweriesMapper.class);
//       SequenceFileOutputFormat.setOutputPath(jobBeerAndBreweries, beersAndBreweriesTmpPath);
//       jobBeerAndBreweries.setJarByClass(Job2.class);
//       jobBeerAndBreweries.setReducerClass(BeersAndBreweriesReducer.class);
//       jobBeerAndBreweries.setMapOutputKeyClass(IntWritable.class);
//       jobBeerAndBreweries.setMapOutputValueClass(BeerOrBrewery.class);
//       jobBeerAndBreweries.setOutputKeyClass(IntWritable.class);
//       jobBeerAndBreweries.setOutputValueClass(BeerOrBrewery.class);
//       jobBeerAndBreweries.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//       if (!jobAvg.waitForCompletion(true)) {
//           System.exit(1);
//       }
//       if (!jobBeerAndBreweries.waitForCompletion(true)) {
//           System.exit(1);
//       }
//
//       MultipleInputs.addInputPath(jobBreweriesClasses,avgTmpPath, SequenceFileInputFormat.class, AvgMapper.class);
//       MultipleInputs.addInputPath(jobBreweriesClasses,beersAndBreweriesTmpPath, SequenceFileInputFormat.class, BreweriesClassesMapper.class);
//       FileOutputFormat.setOutputPath(jobBreweriesClasses,breweriesClassesPath);
//       jobBreweriesClasses.setJarByClass(Job2.class);
//      jobBreweriesClasses.setReducerClass(BreweriesClassesReducer.class);
//
//      jobBreweriesClasses.setMapOutputKeyClass(IntWritable.class);
//      jobBreweriesClasses.setMapOutputValueClass(BreweriesAndClasses.class);
//
//      jobBreweriesClasses.setOutputKeyClass(IntWritable.class);
//      jobBreweriesClasses.setOutputValueClass(BreweriesAndClasses.class);
//      jobBreweriesClasses.setOutputFormatClass(SequenceFileOutputFormat.class);
//       if (!jobBreweriesClasses.waitForCompletion(true)) {
//           System.exit(1);
//       }

       jobFinal.setNumReduceTasks(1);
       SequenceFileInputFormat.addInputPath(jobFinal,breweriesClassesPath);
       jobFinal.setInputFormatClass(SequenceFileInputFormat.class);
       FileOutputFormat.setOutputPath(jobFinal,resultPath);
       jobFinal.setJarByClass(Job2.class);
       jobFinal.setMapperClass(ResultMapper.class);
       jobFinal.setReducerClass(ResultReducer.class);
       jobFinal.setMapOutputKeyClass(Pair.class);
       jobFinal.setMapOutputValueClass(Text.class);
       jobFinal.setOutputKeyClass(Text.class);
       jobFinal.setOutputValueClass(Text.class);
       System.exit(jobFinal.waitForCompletion(true) ? 0 : 1);

//       if (!jobAvg.waitForCompletion(true)) {
//           System.exit(1);
//       }

//



    }
}
