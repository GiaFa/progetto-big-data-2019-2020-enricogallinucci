package hadoop.job2;

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
 * Inizialmente classifichiamo le birre in base alla media dei voti, poi le dividiamo in classi
 * di voto: media &lt;= 2 bassa qualita; 2&lt; media &lt;= 4 media qualita; media &gt; 4 alta qualità.
 * Per ogni birreria viene calcolata la quantità di birre in ogni classe; alla fine, le birrerie
 * vengono ordinate in base ad uno score, dove la percentuale di birre per ogni classe viene moltiplicata
 * per un punteggio di classe predefinito.
 */
public class Job2  extends Configured implements Tool {

    private static final JobControl jc=Common.jobControl("Job2");
    private static ControlledJob jobAvg;
    private static ControlledJob jobBeerAndBreweries;
    private static ControlledJob jobBreweriesClasses;
    private static ControlledJob jobFinal;
   public static void main(String[] args) throws Exception {
       ToolRunner.run(new Job2(), args);
   }

    @Override
    public int run(String[] args) throws Exception {
        setControllerJobAndJobControl();
        Common.allPath();
        Common.verifyDirectory();
        setGlobalVariable(args);
        setJobAvg();
        setJobBeerAndBreweries();
        setJobBreweriesClasses();
        setFinalJob();
        Common.runJobControl(jc);
        return 0;
    }

    private void setGlobalVariable(String[] args) {
        if(args.length>0 && Pattern.matches("([0-9]*)",args[0])){
            jobFinal.getJob().getConfiguration().setInt("nBirrerie", Integer.parseInt(args[0]));
        }
        if(args.length>1 && Pattern.matches("([0-9]*)",args[1])){
            jobAvg.getJob().getConfiguration().setInt("minRecensioni", Integer.parseInt(args[1]));
        }
    }

    private static void setControllerJobAndJobControl() throws IOException {
        jobAvg = Common.controlledJob();
        jobBeerAndBreweries = Common.controlledJob();
        jobBreweriesClasses = Common.controlledJob();
        jobFinal = Common.controlledJob();
        jobBreweriesClasses.addDependingJob(jobAvg);
        jobBreweriesClasses.addDependingJob(jobBeerAndBreweries);
        jobFinal.addDependingJob(jobBreweriesClasses);
        setJobControl();
    }

    private static void setJobControl(){
        jc.addJob(jobAvg);
        jc.addJob(jobBeerAndBreweries);
        jc.addJob(jobBreweriesClasses);
        jc.addJob(jobFinal);
    }

    private static void setJobAvg() throws IOException {
        Common.jobAvg(jobAvg,Common.getReviewsPath(),Common.getAvgTmpPath());
        Common.setReducerJob2Avg(jobAvg);
    }

    private static void setJobBeerAndBreweries(){
        Common.jobBeerAndBreweries(jobBeerAndBreweries,Common.getBeerPath(),Common.getBreweriesPath(),Common.getBeersAndBreweriesTmpPath());
        Common.setReducerJob2JoinBeerBrewery(jobBeerAndBreweries);
    }

    private static void setJobBreweriesClasses(){

        MultipleInputs.addInputPath(jobBreweriesClasses.getJob(),Common.getAvgTmpPath(), SequenceFileInputFormat.class, AvgMapper.class);
        MultipleInputs.addInputPath(jobBreweriesClasses.getJob(),Common.getBeersAndBreweriesTmpPath(), SequenceFileInputFormat.class, BreweriesClassesMapper.class);
        FileOutputFormat.setOutputPath(jobBreweriesClasses.getJob(),Common.getBreweriesClassesPath());
        jobBreweriesClasses.getJob().setJarByClass(Job2.class);
        jobBreweriesClasses.getJob().setReducerClass(BreweriesClassesReducer.class);

        jobBreweriesClasses.getJob().setMapOutputKeyClass(IntWritable.class);
        jobBreweriesClasses.getJob().setMapOutputValueClass(BreweriesAndClasses.class);

        jobBreweriesClasses.getJob().setOutputKeyClass(IntWritable.class);
        jobBreweriesClasses.getJob().setOutputValueClass(BreweriesAndClasses.class);
        jobBreweriesClasses.getJob().setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    private static void setFinalJob() throws IOException {
        jobFinal.getJob().setNumReduceTasks(1);
        SequenceFileInputFormat.addInputPath(jobFinal.getJob(),Common.getBreweriesClassesPath());
        jobFinal.getJob().setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(jobFinal.getJob(),Common.getResultPathJob2());
        jobFinal.getJob().setJarByClass(Job2.class);
        jobFinal.getJob().setMapperClass(ResultMapper.class);
        jobFinal.getJob().setReducerClass(ResultReducer.class);
        jobFinal.getJob().setMapOutputKeyClass(Pair.class);
        jobFinal.getJob().setMapOutputValueClass(Text.class);
        jobFinal.getJob().setOutputKeyClass(Text.class);
        jobFinal.getJob().setOutputValueClass(Text.class);
    }

}
