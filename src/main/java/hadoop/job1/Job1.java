package hadoop.job1;

import hadoop.BeerOrBrewery;
import hadoop.commonjob.Common;
import hadoop.job2.BeersAndBreweriesReducer;
import hadoop.commonjob.BeersMapper;
import hadoop.commonjob.BreweriesMapper;
import hadoop.job2.Job2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import static hadoop.commonjob.Common.*;
import java.io.IOException;
/**
 * Top 20 birrerie con almeno 5 birre diverse
 * con le medie di voti piu alta.(50 recensioni minima per ogni birra
 * (puo cambiare la quantita), vedremo la media di ricensioni per ogni birra).
 */
public class Job1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job jobAvg = Job.getInstance(SonCommon.commonConf(),"AvgReviews");
        Job jobCreateClasses = Job.getInstance(SonCommon.commonConf(),"Beer Classes");
        Job jobBeerAndBreweries = Job.getInstance(SonCommon.commonConf(),"Beers and Breweries fusion");
        Path beerPath = SonCommon.beerPath();
        Path breweriesPath = SonCommon.breweryPath();
        Path reviewsPath = new Path("giovannim/dataset/input/datasetprogetto/reviews.csv");
        Path avgTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/AvgTmp");
        Path beersAndBreweriesTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BeersAndBreweries");

        FileSystem fs = FileSystem.get(SonCommon.commonConf());
        if(fs.exists(avgTmpPath)) {
            fs.delete(avgTmpPath, true);
        }

        if(fs.exists(beersAndBreweriesTmpPath)) {
            fs.delete(beersAndBreweriesTmpPath, true);
        }
        MultipleInputs.addInputPath(jobBeerAndBreweries,beerPath, TextInputFormat.class, BeersMapper.class);
        MultipleInputs.addInputPath(jobBeerAndBreweries,breweriesPath, TextInputFormat.class, BreweriesMapper.class);
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
    }
}
