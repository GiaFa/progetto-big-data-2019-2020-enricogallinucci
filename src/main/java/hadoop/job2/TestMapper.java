package hadoop.job2;

import hadoop.BeerOrBrewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<Object, BeerOrBrewery, IntWritable, Text> {
    public void map(Object key, BeerOrBrewery value, Context context)
            throws IOException, InterruptedException {
        Text result = new Text();
        result.set("Birra: ".concat(value.getBeer().getName()).concat(", Birreria: ").concat(value.getBrewery().getName()));
        context.write(new IntWritable(value.getBeer().getId()),result);
    }

}
