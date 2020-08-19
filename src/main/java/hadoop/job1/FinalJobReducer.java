package hadoop.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FinalJobReducer extends Reducer<IntWritable, BreweriesAndAvg, IntWritable, Text> { 
    private final Map<Integer,Double> resultAvgMap = new HashMap<>();
    private final Map<Integer,String> breweriesAndAvgMap = new HashMap<>();
    private final static int INIT_VALUE = 0;
    private static final int nBirrerie = 20; 
    public void reduce(IntWritable keyBrewery, Iterable<BreweriesAndAvg> values, Context context) {
        double total = 0, count = 0;
        for (BreweriesAndAvg result : values) {
            if(count==0){ 
                breweriesAndAvgMap.put(keyBrewery.get(), result.getBeerOrBrewery().getBrewery().getName());
            }
            count++;
            total += result.getAvgBeer();
        }
        resultAvgMap.put(keyBrewery.get(),total/count);

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<Integer,Double>> result =sorted().subList(INIT_VALUE,context.getConfiguration().getInt("nBirrerie",nBirrerie)); //first 20 element
        int count = 1;
        for(Map.Entry<Integer,Double> breweryBeer : result){
            String name =breweriesAndAvgMap.get(breweryBeer.getKey());
            String finalResult = print(name,count,breweryBeer.getValue());
            context.write(new IntWritable(breweryBeer.getKey()),new Text(finalResult));
            count++;
        }
    }

    private List<Map.Entry<Integer,Double>> sorted(){
        List<Map.Entry<Integer,Double>> element =
                new LinkedList<>(resultAvgMap.entrySet());
        Collections.sort(element,
                new Comparator<Map.Entry<Integer,Double>>() {
                    @Override
                    public int compare(Map.Entry<Integer,Double> es1,
                                       Map.Entry<Integer,Double> es2) {
                        return Double.compare(es2.getValue(), es1.getValue());
                    }
                });
        return element;
    }


    private String print(String nameBirreria, int top, double score) {
        return " La Birreria : ".concat(nameBirreria)
                .concat(" e la Top : ").concat(Integer.toString(top)).concat(" Con un score di : ")
                .concat(Double.toString(score));
    }

}
