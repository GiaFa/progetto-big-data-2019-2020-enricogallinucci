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
import java.util.regex.Pattern;

/**
 * Top N birrerie(N passabile come parametro, default 20) con almeno N birre diverse(N secondo parametro, default 5)
 * con le medie di voti piu alta.(N recensioni minime per ogni birra, terzo parametro, 50 default).
 */
public class Job1  extends Configured implements Tool {
    private static final JobControl jc=Common.jobControl("Job1");
    private static ControlledJob jobAvg;
    private static ControlledJob jobBeerAndBreweries;
    private static ControlledJob jobUnion;
    private static ControlledJob jobFinal;
    public static void main(String[] args)throws Exception {
        ToolRunner.run(new Job1(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        setControllerJobAndJobControl();
        Common.allPath();
        Common.verifyDirectory();
        setGlobalVariable(args);
        setJobAvg();
        setJobBeerAndBreweries();
        setJobUnion();
        setFinalJob();
        Common.runJobControl(jc);
        return 0;
    }

    private void setGlobalVariable(String[] args) {
        if(args.length>0 && Pattern.matches("([0-9]*)",args[0])){
            jobFinal.getJob().getConfiguration().setInt("nBirrerie",Integer.parseInt(args[0]));
        }
        if(args.length>1 && Pattern.matches("([0-9]*)",args[1])){
            jobBeerAndBreweries.getJob().getConfiguration().setInt("beersForBrewery",Integer.parseInt(args[1]));
        }
        if(args.length>2 && Pattern.matches("([0-9]*)",args[2])){
            jobAvg.getJob().getConfiguration().setInt("minRecensioni",Integer.parseInt(args[2]));
        }
    }

    private static void setControllerJobAndJobControl() throws IOException {
        jobAvg = Common.controlledJob();
        jobBeerAndBreweries = Common.controlledJob();
        jobUnion = Common.controlledJob();
        jobFinal = Common.controlledJob();
        jobUnion.addDependingJob(jobAvg);
        jobUnion.addDependingJob(jobBeerAndBreweries);
        jobFinal.addDependingJob(jobUnion);
        setJobControl();
    }

    private static void setJobControl(){
        jc.addJob(jobAvg);
        jc.addJob(jobBeerAndBreweries);
        jc.addJob(jobUnion);
        jc.addJob(jobFinal);
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
        MultipleInputs.addInputPath(jobUnion.getJob(),Common.getAvgTmpPath(), SequenceFileInputFormat.class, AvgMapper2.class);
        MultipleInputs.addInputPath(jobUnion.getJob(),Common.getBeersAndBreweriesTmpPath(), SequenceFileInputFormat.class, BreweriesAvgMapper.class);
        FileOutputFormat.setOutputPath(jobUnion.getJob(),Common.getBreweriesClassesPath());
        jobUnion.getJob().setJarByClass(Job1.class);
        jobUnion.getJob().setReducerClass(BreweriesAvgReducer.class);
        jobUnion.getJob().setMapOutputKeyClass(IntWritable.class);
        jobUnion.getJob().setMapOutputValueClass(BreweriesAndAvg.class);
        jobUnion.getJob().setOutputKeyClass(IntWritable.class);
        jobUnion.getJob().setOutputValueClass(BreweriesAndAvg.class);
        jobUnion.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);



    }
    private static void setFinalJob() throws IOException {
        jobFinal.getJob().setNumReduceTasks(1);
        SequenceFileInputFormat.addInputPath(jobFinal.getJob(),Common.getBreweriesClassesPath());
        jobFinal.getJob().setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(jobFinal.getJob(),Common.getResultPathJob1());
        jobFinal.getJob().setJarByClass(Job1.class);
        jobFinal.getJob().setMapperClass(FinalJobMapper.class);
        jobFinal.getJob().setReducerClass(FinalJobReducer.class);
        jobFinal.getJob().setMapOutputKeyClass(IntWritable.class);
        jobFinal.getJob().setMapOutputValueClass(BreweriesAndAvg.class);
        jobFinal.getJob().setOutputKeyClass(IntWritable.class);
        jobFinal.getJob().setOutputValueClass(Text.class);
    }

}
