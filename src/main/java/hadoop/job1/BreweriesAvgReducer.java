package hadoop.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class BreweriesAvgReducer extends Reducer<IntWritable, BreweriesAndAvg, IntWritable, Text> {
    private final Map<Integer,BreweriesAndAvg> breweriesAndAvgMap = new HashMap<>();
    private final static int INIT_VALUE = 0;
    private static final int nBirrerie = 20;
    public void reduce(IntWritable keyBeer, Iterable<BreweriesAndAvg> values, Context context) {
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
            breweriesAndAvgMap.put(result.getBeerOrBrewery().getBrewery().getId(),result);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<Integer,BreweriesAndAvg>> result =sorted().subList(INIT_VALUE,context.getConfiguration().getInt("nBirrerie",nBirrerie)); //first 20 element
        int count = 1;
        for(Map.Entry<Integer,BreweriesAndAvg> breweryBeer : result){
            String name = breweryBeer.getValue().getBeerOrBrewery().getBrewery().getName();
            String finalResult = print(name,count,breweryBeer.getValue().getAvgBeer());
            context.write(new IntWritable(breweryBeer.getKey()),new Text(finalResult));
            count++;
        }
    }

    private List<Map.Entry<Integer,BreweriesAndAvg>> sorted(){
        List<Map.Entry<Integer,BreweriesAndAvg>> element =
                new LinkedList<>(breweriesAndAvgMap.entrySet());
        Collections.sort(element,
                new Comparator<Map.Entry<Integer,BreweriesAndAvg>>() {
                    @Override
                    public int compare(Map.Entry<Integer,BreweriesAndAvg> es1,
                                       Map.Entry<Integer,BreweriesAndAvg> es2) {
                        return Double.compare(es2.getValue().getAvgBeer(), es1.getValue().getAvgBeer());
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