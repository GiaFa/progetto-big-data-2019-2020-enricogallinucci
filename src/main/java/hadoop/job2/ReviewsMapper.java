package hadoop.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ReviewsMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

    private boolean first = true;

    private double getDouble(String str) {
        if (str.isEmpty())
            return 0;
        else
            return Double.parseDouble(str);
    }

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        if (!first) {
            List<String> columns = Arrays.asList(value.toString().split(","));
            int beer_id = Integer.parseInt(columns.get(0));
            double overall = getDouble(columns.get(columns.size() -1));
            context.write(new IntWritable(beer_id),new DoubleWritable(overall));
        } else {
            first = false;
        }
    }
}
