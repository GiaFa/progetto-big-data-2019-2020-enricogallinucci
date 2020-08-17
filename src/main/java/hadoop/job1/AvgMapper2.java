package hadoop.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper2 extends Mapper<IntWritable, DoubleWritable, IntWritable, BreweriesAndAvg> {

    public void map(IntWritable keyBeer, DoubleWritable value, Context context) throws IOException, InterruptedException {
        BreweriesAndAvg res = new BreweriesAndAvg();
        res.setAvgBeer(value.get());
        context.write(keyBeer, res);
    }

}
