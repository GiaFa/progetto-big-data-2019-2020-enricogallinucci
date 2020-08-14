package hadoop.job2;

import hadoop.Beer;
import hadoop.BeerOrBrewery;
import hadoop.Brewery;
import hadoop.commonjob.Common;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ResultReducer extends Reducer<Pair, Text,Text,Text> {
    public static Map<String,ResultToPrint> map = new HashMap<>();
    public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String k = "";
        for(Text t: values){
            if(count == 0) {
                k = t.toString();
            }
            count++;
        }
        ResultToPrint update = map.get(k) == null ? new ResultToPrint(): map.get(k);

        if(key.getBeerclass() == Common.LOW_CLASS){
            update.lowClass = count;
        }else if(key.getBeerclass() == Common.MEDIUM_CLASS){
            update.middleClass = count;
        } else {
            update.highClass = count;
        }
        map.put(k,update);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        for(Map.Entry<String,ResultToPrint> brew: map.entrySet()){
            context.write(new Text(brew.getKey()), new Text(brew.getValue().toString()));
        }
    }

    private static class ResultToPrint{
        int lowClass;
        int middleClass;
        int highClass;

        @Override
        public String toString() {
            return " Quantità Birre Qualità Bassa : ".concat(Integer.toString(lowClass))
                    .concat(" Quantità Birre Qualità Media : ").concat(Integer.toString(middleClass)).concat(" Quantità Birre Qualità Alta : ")
                    .concat(Integer.toString(highClass));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResultToPrint that = (ResultToPrint) o;
            return lowClass == that.lowClass &&
                    middleClass == that.middleClass &&
                    highClass == that.highClass;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowClass, middleClass, highClass);
        }
    }
}