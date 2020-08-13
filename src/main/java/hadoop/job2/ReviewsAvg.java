package hadoop.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewsAvg extends Reducer<IntWritable, DoubleWritable,IntWritable,DoubleWritable> {
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double total = 0, count = 0;
        for (DoubleWritable doubleWritable : values) {
            count++;
            total += doubleWritable.get();
        }
        if(count > 50){
            context.write(key, new DoubleWritable( total / count));
        }
    }
}
