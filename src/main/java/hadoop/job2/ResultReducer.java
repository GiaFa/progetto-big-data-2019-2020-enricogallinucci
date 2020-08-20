package hadoop.job2;

import hadoop.commonjob.Common;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ResultReducer extends Reducer<Pair, Text,Text,Text> {
    private static final int nBirrerie = 20;
    private static final double LOW_SCORE = 0.1;
    private static final double MEDIUM_SCORE = 0.3;
    private static final double HIGH_SCORE = 0.8;
    private double maxLow, maxMiddle,maxHigh = 0;
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
            maxLow = Math.max(count, maxLow);
        }else if(key.getBeerclass() == Common.MEDIUM_CLASS){
            update.middleClass = count;
            maxMiddle = Math.max(count,maxMiddle);
        } else if(key.getBeerclass() == Common.HIGH_CLASS) {
            update.highClass = count;
            maxHigh = Math.max(count,maxHigh);
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
                        int scoreCompare = Double.compare(es2.getValue().getScore(), es1.getValue().getScore());
                        if(scoreCompare!=0){
                            return scoreCompare;
                        }
                        int highCompare = Integer.compare(es2.getValue().highClass,es1.getValue().highClass);
                        if(highCompare !=0){
                            return highCompare;
                        }
                        int mediumCompare = Integer.compare(es2.getValue().middleClass,es1.getValue().middleClass);
                        if(mediumCompare !=0){
                            return highCompare;
                        }
                        return Integer.compare(es2.getValue().lowClass,es1.getValue().lowClass);
                    }
                });
        return element;
    }


    private class ResultToPrint{
        int lowClass;
        int middleClass;
        int highClass;

        public double getScore(){
            double normalizedLow = lowClass / maxLow;
            double normalizedMiddle = middleClass / maxMiddle;
            double normalizeHigh = highClass / maxHigh;
            return ((normalizedLow) * LOW_SCORE) + ((normalizedMiddle) * MEDIUM_SCORE) + ((normalizeHigh) * HIGH_SCORE);
        }

        @Override
        public String toString() {
            return " Quantità Birre Qualità Bassa : ".concat(Integer.toString(lowClass))
                    .concat(" Quantità Birre Qualità Media : ").concat(Integer.toString(middleClass)).concat(" Quantità Birre Qualità Alta : ")
                    .concat(Integer.toString(highClass)).concat(" Per uno score totale di: ").concat(Double.toString(getScore()));
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