package hadoop.job2;

import hadoop.commonjob.Common;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ResultReducer extends Reducer<Pair, Text,Text,Text> {
    private static final int nBirrerie = 20;
    public static Map<String,ResultToPrint> map = new HashMap<>();
    public void reduce(Pair key, Iterable<Text> values, Context context) {
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
        } else if(key.getBeerclass() == Common.HIGH_CLASS) {
            update.highClass = count;
        }
        map.put(k,update);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<String,ResultToPrint>> result =sorted().subList(0,context.getConfiguration().getInt("nBirrerie",nBirrerie)); //first 20 element
        for(Map.Entry<String,ResultToPrint> brew: result){
            context.write(new Text(brew.getKey()), new Text(brew.getValue().toString()));
        }
    }

    private List<Map.Entry<String,ResultToPrint>> sorted(){
        List<Map.Entry<String,ResultToPrint>> element =
                new LinkedList<>(map.entrySet());
        Collections.sort(element,
                new Comparator<Map.Entry<String,ResultToPrint>>() {
                    @Override
                    public int compare(Map.Entry<String,ResultToPrint> es1,
                                       Map.Entry<String,ResultToPrint> es2) {
                        return Integer.compare(es2.getValue().highClass, es1.getValue().highClass);
                    }
                });
        return element;
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