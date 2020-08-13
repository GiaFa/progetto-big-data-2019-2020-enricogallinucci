package hadoop.job2;

import hadoop.Beer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BeersMapper extends Mapper<Object, Text, IntWritable, Beer> {
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

            List<String> columns = Arrays.stream(value.toString().split(",")).map(x -> x.replaceAll("\"","")).collect(Collectors.toList());
            int id = getInt(columns.get(0));
            int i = 2;
            String name = columns.get(1);
            while(!Pattern.matches("([0-9]*)", columns.get(i))){
                name = name.concat(columns.get(i));
                i = i+1;
            }
            int brewery = getInt(columns.get(i));
            context.write(new IntWritable(id),new Beer(id,name,brewery));
        } else {
            first = false;
        }
    }
}
