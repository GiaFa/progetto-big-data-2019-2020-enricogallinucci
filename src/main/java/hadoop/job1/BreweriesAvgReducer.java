package hadoop.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BreweriesAvgReducer extends Reducer<IntWritable, BreweriesAndAvg, IntWritable, BreweriesAndAvg> {
     public void reduce(IntWritable keyBeer, Iterable<BreweriesAndAvg> values, Context context) throws IOException, InterruptedException {
        BreweriesAndAvg result = new BreweriesAndAvg();
        int size = 0;
        for(BreweriesAndAvg value: values){
            size++;
            if(value.getIsAvg()){
                result.setAvgBeer(value.getAvgBeer());
            }else{
                result.setBeerOrBrewery(value.getBeerOrBrewery());
            }
        }
        if(size>1){
            context.write(new IntWritable(result.getBeerOrBrewery().getBrewery().getId()),result);
        }
    }
}