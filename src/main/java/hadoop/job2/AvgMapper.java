package hadoop.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper extends Mapper<IntWritable, IntWritable, IntWritable, BreweriesAndClasses> {

    public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        BreweriesAndClasses res = new BreweriesAndClasses();
        res.setBeerClass(value.get());
        context.write(key, res);
    }

}
