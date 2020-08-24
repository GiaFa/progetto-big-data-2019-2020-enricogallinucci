package hadoop.job2;

import hadoop.commonjob.BeerOrBrewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BreweriesClassesMapper  extends Mapper<IntWritable, BeerOrBrewery, IntWritable, BreweriesAndClasses> {

    public void map(IntWritable key, BeerOrBrewery value, Context context) throws IOException, InterruptedException {
        BreweriesAndClasses res = new BreweriesAndClasses();
        res.setBeerOrBrewery(value);
        context.write(key, res);
    }
}