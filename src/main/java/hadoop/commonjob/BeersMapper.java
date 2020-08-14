package hadoop.commonjob;

import hadoop.Beer;
import hadoop.BeerOrBrewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class BeersMapper extends Mapper<Object, Text, IntWritable, BeerOrBrewery> {
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
            int i = 2;
            String name = columns.get(1);
            while(!Pattern.matches("([0-9]*)", columns.get(i))){
                name = name.concat(columns.get(i));
                i = i+1;
            }
            int brewery = getInt(columns.get(i));
            BeerOrBrewery result = new BeerOrBrewery();
            result.setBeer(new Beer(id,name,brewery));
            context.write(new IntWritable(brewery),result);
        } else {
            first = false;
        }
    }
}
