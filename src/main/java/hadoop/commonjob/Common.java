package hadoop.commonjob;

import hadoop.BeerOrBrewery;
import hadoop.job2.BeersAndBreweriesReducer;
import hadoop.job2.Job2;
import hadoop.job2.ReviewsAvg;
import hadoop.job2.ReviewsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Common {

    public static final int LOW_CLASS = 2;
    public static final int MEDIUM_CLASS = 4;
    public static final int HIGH_CLASS = 5;
    private static final Configuration conf = new Configuration();
    private static Path beerPath;
    private static Path breweriesPath;
    private static Path reviewsPath;
    private static Path avgTmpPath;
    private static Path beersAndBreweriesTmpPath;
    private static Path breweriesClassesPath;
    private static Path resultPath;
    private static FileSystem fs;
    private static final Object monitor = new Object();
    public static void allPath() throws IOException {
        beerPath = new Path("faspeeencina/datasets/input/project/file/beers.csv");
        breweriesPath = new Path("faspeeencina/datasets/input/project/file/breweries.csv");
        reviewsPath = new Path("faspeeencina/datasets/input/project/file/reviews.csv");
        avgTmpPath = new Path("faspeeencina/datasets/output/datasetprogetto/hadoop/AvgTmp");
        beersAndBreweriesTmpPath = new Path("faspeeencina/datasets/output/datasetprogetto/hadoop/BeersAndBreweries");
        breweriesClassesPath = new Path("faspeeencina/datasets/output/datasetprogetto/hadoop/BreweriesClasses");
        resultPath = new Path("faspeeencina/datasets/output/datasetprogetto/hadoop/job2");
        fs =  FileSystem.get(getConf());
    }

    public static void verifyDirectory() throws IOException {
        if(fs.exists(getAvgTmpPath())) {
            fs.delete(getAvgTmpPath(), true);
        }

        if(fs.exists(getBeersAndBreweriesTmpPath())) {
            fs.delete(getBeersAndBreweriesTmpPath(), true);
        }

        if(fs.exists(getBreweriesClassesPath())) {
            fs.delete(getBreweriesClassesPath(), true);
        }


        if(fs.exists(getResultPath())) {
            fs.delete(getResultPath(), true);
        }
    }

    public static ControlledJob controlledJob() throws IOException {
        return new ControlledJob(conf);
    }

    public static JobControl jobControl(String groupName){
       return new JobControl(groupName);
    }

    public static Configuration getConf() {return  conf;}

    public static void jobAvg(ControlledJob jobAvg,Path reviewsPath,Path avgTmpPath) throws IOException {

        FileInputFormat.addInputPath(jobAvg.getJob(),reviewsPath);
        SequenceFileOutputFormat.setOutputPath(jobAvg.getJob(),avgTmpPath);
        jobAvg.getJob().setJarByClass(Job2.class);
        jobAvg.getJob().setMapperClass(ReviewsMapper.class);
        jobAvg.getJob().setReducerClass(ReviewsAvg.class);
        jobAvg.getJob().setMapOutputKeyClass(IntWritable.class);
        jobAvg.getJob().setMapOutputValueClass(DoubleWritable.class);
        jobAvg.getJob().setOutputKeyClass(IntWritable.class);
        jobAvg.getJob().setOutputValueClass(IntWritable.class);
        jobAvg.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void jobBeerAndBreweries(ControlledJob jobBeerAndBreweries,Path beerPath,Path breweriesPath, Path beersAndBreweriesTmpPath){

        MultipleInputs.addInputPath(jobBeerAndBreweries.getJob(),beerPath, TextInputFormat.class, BeersMapper.class);
        MultipleInputs.addInputPath(jobBeerAndBreweries.getJob(),breweriesPath, TextInputFormat.class, BreweriesMapper.class);
        SequenceFileOutputFormat.setOutputPath(jobBeerAndBreweries.getJob(), beersAndBreweriesTmpPath);
        jobBeerAndBreweries.getJob().setJarByClass(Job2.class);
        jobBeerAndBreweries.getJob().setReducerClass(BeersAndBreweriesReducer.class);
        jobBeerAndBreweries.getJob().setMapOutputKeyClass(IntWritable.class);
        jobBeerAndBreweries.getJob().setMapOutputValueClass(BeerOrBrewery.class);
        jobBeerAndBreweries.getJob().setOutputKeyClass(IntWritable.class);
        jobBeerAndBreweries.getJob().setOutputValueClass(BeerOrBrewery.class);
        jobBeerAndBreweries.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static Path getBeerPath() {
        return beerPath;
    }

    public static Path getReviewsPath(){
        return reviewsPath;
    }

    public static Path getBreweriesPath() {
        return breweriesPath;
    }

    public static Path getAvgTmpPath() {
        return avgTmpPath;
    }

    public static Path getBeersAndBreweriesTmpPath() {
        return beersAndBreweriesTmpPath;
    }

    public static Path getBreweriesClassesPath() {
        return breweriesClassesPath;
    }

    public static Path getResultPath() {
        return resultPath;
    }


    public static void runJobControl(JobControl jc) throws InterruptedException {
        Thread start = new Thread(jc);
        start.start();
        synchronized (monitor){
            while(!jc.allFinished()){
                System.out.println(jc.getThreadState());
                monitor.wait(5000);
            }
        }
        jc.resume();
        jc.stop();
        start.join();
    }
}
