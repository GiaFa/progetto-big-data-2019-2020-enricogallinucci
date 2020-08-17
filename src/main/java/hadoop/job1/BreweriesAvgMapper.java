package hadoop.job1;

import hadoop.BeerOrBrewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BreweriesAvgMapper extends Mapper<IntWritable, BeerOrBrewery, IntWritable, BreweriesAndAvg> {

    public void map(IntWritable keyBeer, BeerOrBrewery value, Context context) throws IOException, InterruptedException {
        BreweriesAndAvg res = new BreweriesAndAvg();
        res.setBeerOrBrewery(value);
        context.write(keyBeer, res);
    }

}