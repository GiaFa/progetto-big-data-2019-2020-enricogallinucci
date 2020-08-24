package hadoop.commonjob;

import hadoop.job1.BeersAndBreweriesReducer2;
import hadoop.job1.ReviewsReducerAvg2;
import hadoop.job2.BeersAndBreweriesReducer;
import hadoop.job2.Job2;
import hadoop.job2.ReviewsAvgReducer;
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
    private static Path resultJob1;
    private static Path resultJob2;
    private static FileSystem fs;
    private static final Object monitor = new Object();
    public static void allPath() throws IOException {
        beerPath = new Path("giovannim/dataset/input/datasetprogetto/beers.csv");
        breweriesPath = new Path("giovannim/dataset/input/datasetprogetto/breweries.csv");
        reviewsPath = new Path("giovannim/dataset/input/datasetprogetto/reviews.csv");
        avgTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/AvgTmp");
        beersAndBreweriesTmpPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BeersAndBreweries");
        breweriesClassesPath = new Path("giovannim/dataset/output/datasetprogetto/hadoop/BreweriesClasses");
        resultJob1 = new Path("giovannim/dataset/output/datasetprogetto/hadoop/job1");
        resultJob2 = new Path("giovannim/dataset/output/datasetprogetto/hadoop/job2");
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

        if(fs.exists(getResultPathJob1())) {
            fs.delete(getResultPathJob1(), true);
        }

        if(fs.exists(getResultPathJob2())) {
            fs.delete(getResultPathJob2(), true);
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
        jobAvg.getJob().setMapOutputKeyClass(IntWritable.class);
        jobAvg.getJob().setMapOutputValueClass(DoubleWritable.class);
        jobAvg.getJob().setOutputKeyClass(IntWritable.class);
        jobAvg.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
    }
    public static void setReducerJob2Avg(ControlledJob jobAvg){
        jobAvg.getJob().setReducerClass(ReviewsAvgReducer.class);
        jobAvg.getJob().setOutputValueClass(IntWritable.class);
    }
    public static void setReducerJob1Avg(ControlledJob jobAvg){
        jobAvg.getJob().setReducerClass(ReviewsReducerAvg2.class);
        jobAvg.getJob().setOutputValueClass(DoubleWritable.class);
    }

    public static void jobBeerAndBreweries(ControlledJob jobBeerAndBreweries,Path beerPath,Path breweriesPath, Path beersAndBreweriesTmpPath){

        MultipleInputs.addInputPath(jobBeerAndBreweries.getJob(),beerPath, TextInputFormat.class, BeersMapper.class);
        MultipleInputs.addInputPath(jobBeerAndBreweries.getJob(),breweriesPath, TextInputFormat.class, BreweriesMapper.class);
        SequenceFileOutputFormat.setOutputPath(jobBeerAndBreweries.getJob(), beersAndBreweriesTmpPath);
        jobBeerAndBreweries.getJob().setJarByClass(Job2.class);
        jobBeerAndBreweries.getJob().setMapOutputKeyClass(IntWritable.class);
        jobBeerAndBreweries.getJob().setMapOutputValueClass(BeerOrBrewery.class);
        jobBeerAndBreweries.getJob().setOutputKeyClass(IntWritable.class);
        jobBeerAndBreweries.getJob().setOutputValueClass(BeerOrBrewery.class);
        jobBeerAndBreweries.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void setReducerJob2JoinBeerBrewery(ControlledJob jobBeerAndBreweries){
        jobBeerAndBreweries.getJob().setReducerClass(BeersAndBreweriesReducer.class);
    }
    public static void setReducerJob1JoinBeerBrewery(ControlledJob jobBeerAndBreweries){
        jobBeerAndBreweries.getJob().setReducerClass(BeersAndBreweriesReducer2.class);
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

    public static Path getResultPathJob1() {
        return resultJob1;
    }

    public static Path getResultPathJob2() {
        return resultJob2;
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
