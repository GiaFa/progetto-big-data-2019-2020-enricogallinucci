package hadoop.job2;
import hadoop.commonjob.Common;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewsAvgReducer extends Reducer<IntWritable, DoubleWritable,IntWritable,IntWritable> {

    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double total = 0, count = 0;
        for (DoubleWritable doubleWritable : values) {
            count++;
            total += doubleWritable.get();
        }
        if(count > 50){
            //fare qualcosa con medie? buh
            double avg = total / count;
            if(avg < Common.LOW_CLASS){
                context.write(key, new IntWritable(Common.LOW_CLASS));
            } else if(avg < Common.MEDIUM_CLASS){
                context.write(key, new IntWritable(Common.MEDIUM_CLASS));
            } else {
                context.write(key, new IntWritable(Common.HIGH_CLASS));
            }
        }
    }
}
