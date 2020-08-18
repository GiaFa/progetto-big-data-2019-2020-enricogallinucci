package hadoop.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewsReducerAvg2 extends Reducer<IntWritable, DoubleWritable,IntWritable,DoubleWritable> {
    private static final int minRecensioni = 50;
    public void reduce(IntWritable keyBeer, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double total = 0, count = 0;
        for (DoubleWritable doubleWritable : values) {
            count++;
            total += doubleWritable.get();
        }
        final int minRecensione = context.getConfiguration().getInt("minRecensioni",minRecensioni);
        if(count >= minRecensione){
            double avg = total / count;
            context.write(keyBeer,new DoubleWritable(avg));
        }
    }
}
