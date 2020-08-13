package hadoop.job2;

import hadoop.Review;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ReviewsMapper extends Mapper<Object, Text, IntWritable, Review>{

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
            List<String> columns = Arrays.stream(value.toString().split(",")).map(x -> x.replaceAll("\"","")).collect(Collectors.toList());
            int beer_id = Integer.parseInt(columns.get(0));
            double overall = getDouble(columns.get(columns.size() -1));
            context.write(new IntWritable(beer_id),new Review(beer_id,overall));
        } else {
            first = false;
        }
    }
}
