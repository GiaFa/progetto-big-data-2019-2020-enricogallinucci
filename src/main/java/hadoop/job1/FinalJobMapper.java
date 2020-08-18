package hadoop.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalJobMapper extends Mapper<IntWritable, BreweriesAndAvg, IntWritable, BreweriesAndAvg> {
    public void map(IntWritable keyBrewery, BreweriesAndAvg value, Context context) throws IOException, InterruptedException {
        context.write(keyBrewery,value);
    }
}
