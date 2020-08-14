package hadoop.job1;

import hadoop.commonjob.Common;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
/**
 * Top 20 birrerie con almeno 5 birre diverse
 * con le medie di voti piu alta.(50 recensioni minima per ogni birra
 * (puo cambiare la quantita), vedremo la media di ricensioni per ogni birra).
 */
public class Job1 {

    private static final JobControl jc=Common.jobControl("Job1");
    private static ControlledJob jobAvg;
    private static ControlledJob jobBeerAndBreweries;

    public static void main(String[] args) throws IOException {
        setControllerJobAndJobControl();
        Common.allPath();
        Common.verifyDirectory();
        setJobAvg();
        setJobBeerAndBreweries();
        jc.run();
    }

    private static void setControllerJobAndJobControl() throws IOException {
        jobAvg = Common.controlledJob();
        jobBeerAndBreweries = Common.controlledJob();
        setJobControl();
    }

    private static void setJobControl(){
        jc.addJob(jobAvg);
        jc.addJob(jobBeerAndBreweries);
    }

    private static void setJobAvg() throws IOException {
        Common.jobAvg(jobAvg,Common.getReviewsPath(),Common.getAvgTmpPath());
    }
    private static void setJobBeerAndBreweries(){
        Common.jobBeerAndBreweries(jobBeerAndBreweries,Common.getBeerPath(),Common.getBreweriesPath(),Common.getBeersAndBreweriesTmpPath());
    }
}
