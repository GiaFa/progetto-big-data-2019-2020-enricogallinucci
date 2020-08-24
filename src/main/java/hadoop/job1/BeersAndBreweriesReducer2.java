package hadoop.job1;

import hadoop.commonjob.Beer;
import hadoop.commonjob.BeerOrBrewery;
import hadoop.commonjob.Brewery;
import hadoop.commonjob.CommonMethodReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class BeersAndBreweriesReducer2 extends Reducer<IntWritable, BeerOrBrewery,IntWritable,BeerOrBrewery> {
    private static final int beersForBrewery = 5;
    public void reduce(IntWritable key, Iterable<BeerOrBrewery> values, Context context) throws IOException, InterruptedException {
        CommonMethodReducer methodReducer = new CommonMethodReducer();
        methodReducer.myReduce(values);
        List<Beer> beers = methodReducer.getBeers();
        Brewery brewery = methodReducer.getBrewery();
        final int minBeerForBrewery = context.getConfiguration().getInt("beersForBrewery",beersForBrewery);
        if(beers.size()>=minBeerForBrewery){
            for(Beer beer : beers){
                BeerOrBrewery result = new BeerOrBrewery();
                result.setBrewery(brewery);
                result.setBeer(beer);
                context.write(new IntWritable(beer.getId()), result);
            }
        }
    }

}
