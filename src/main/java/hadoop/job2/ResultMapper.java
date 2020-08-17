package hadoop.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ResultMapper extends Mapper<IntWritable, BreweriesAndClasses, Pair, Text> {
    public void map(IntWritable key, BreweriesAndClasses value, Context context) throws IOException, InterruptedException {
        Pair res = new Pair();
        res.setBrewery(value.getBeerOrBrewery().getBrewery().getId());
        res.setBeerclass(value.getBeerClass());
        context.write(res,new Text(value.getBeerOrBrewery().getBrewery().getName()));
    }
}
