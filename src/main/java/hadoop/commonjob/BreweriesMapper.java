package hadoop.commonjob;

import hadoop.BeerOrBrewery;
import hadoop.Brewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BreweriesMapper extends Mapper<Object, Text, IntWritable, BeerOrBrewery> {

    private boolean first = true;

    private int getInt(String str) {
        if (str.isEmpty())
            return 0;
        else
            return Integer.parseInt(str);
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (!first) {
            List<String> columns = Arrays.asList(value.toString().split(","));
            int id = getInt(columns.get(0));
            String name = columns.get(1);
            BeerOrBrewery result = new BeerOrBrewery();
            result.setBrewery(new Brewery(id,name));
            context.write(new IntWritable(id),result);
        } else {
            first = false;
        }
    }
}
