package hadoop.job1;

import hadoop.commonjob.Common;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/**
 * Top 20 birrerie con almeno 5 birre diverse
 * con le medie di voti piu alta.(50 recensioni minima per ogni birra
 * (puo cambiare la quantita), vedremo la media di ricensioni per ogni birra).
 */
public class Job1  extends Configured implements Tool {

    private static final JobControl jc=Common.jobControl("Job1");
    private static ControlledJob jobAvg;
    private static ControlledJob jobBeerAndBreweries;
    private static ControlledJob jobUnion;

    public static void main(String[] args)throws Exception {
        ToolRunner.run(new Job1(), args);

    }
    @Override
    public int run(String[] args) throws Exception {
        setControllerJobAndJobControl();
        Common.allPath();
        Common.verifyDirectory();
        setJobAvg();
        setJobBeerAndBreweries();
        setJobUnion();
        Common.runJobControl(jc);
        return 0;
    }
    private static void setControllerJobAndJobControl() throws IOException {
        jobAvg = Common.controlledJob();
        jobBeerAndBreweries = Common.controlledJob();
        jobUnion = Common.controlledJob();
        jobUnion.addDependingJob(jobAvg);
        jobUnion.addDependingJob(jobBeerAndBreweries);
        setJobControl();
    }

    private static void setJobControl(){
        jc.addJob(jobAvg);
        jc.addJob(jobBeerAndBreweries);
        jc.addJob(jobUnion);
    }

    private static void setJobAvg() throws IOException {
        Common.jobAvg(jobAvg,Common.getReviewsPath(),Common.getAvgTmpPath());
        Common.setReducerJob1Avg(jobAvg);
    }
    private static void setJobBeerAndBreweries(){
        Common.jobBeerAndBreweries(jobBeerAndBreweries,Common.getBeerPath(),Common.getBreweriesPath(),Common.getBeersAndBreweriesTmpPath());
        Common.setReducerJob1JoinBeerBrewery(jobBeerAndBreweries);
    }
    private static void setJobUnion(){
        jobUnion.getJob().setNumReduceTasks(1);
        MultipleInputs.addInputPath(jobUnion.getJob(),Common.getAvgTmpPath(), SequenceFileInputFormat.class, AvgMapper2.class);
        MultipleInputs.addInputPath(jobUnion.getJob(),Common.getBeersAndBreweriesTmpPath(), SequenceFileInputFormat.class, BreweriesAvgMapper.class);
        FileOutputFormat.setOutputPath(jobUnion.getJob(),Common.getBreweriesClassesPath());
        jobUnion.getJob().setJarByClass(Job1.class);
        jobUnion.getJob().setReducerClass(BreweriesAvgReducer.class);
        jobUnion.getJob().setMapOutputKeyClass(IntWritable.class);
        jobUnion.getJob().setMapOutputValueClass(BreweriesAndAvg.class);
        jobUnion.getJob().setOutputKeyClass(IntWritable.class);
        jobUnion.getJob().setOutputValueClass(Text.class);
    }

}
