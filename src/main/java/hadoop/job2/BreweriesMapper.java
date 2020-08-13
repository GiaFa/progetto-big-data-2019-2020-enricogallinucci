package hadoop.job2;

import hadoop.Brewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BreweriesMapper extends Mapper<Object, Text, IntWritable, Brewery> {

    private boolean first = true;

    private int getInt(String str) {
        if (str.isEmpty())
            return 0;
        else
            return Integer.parseInt(str);
    }

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        if (!first) {
            List<String> columns = Arrays.stream(value.toString().split(",")).map(x -> x.replaceAll("\"","")).collect(Collectors.toList());
            int id = getInt(columns.get(0));
            String name = columns.get(1);
            context.write(new IntWritable(id),new Brewery(id,name));
        } else {
            first = false;
        }
    }
}
