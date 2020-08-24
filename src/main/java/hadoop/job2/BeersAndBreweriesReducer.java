package hadoop.job2;

import hadoop.commonjob.Beer;
import hadoop.commonjob.BeerOrBrewery;
import hadoop.commonjob.Brewery;
import hadoop.commonjob.CommonMethodReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class BeersAndBreweriesReducer extends Reducer<IntWritable, BeerOrBrewery,IntWritable,BeerOrBrewery> {
    public void reduce(IntWritable key, Iterable<BeerOrBrewery> values, Context context) throws IOException, InterruptedException {
        CommonMethodReducer methodReducer = new CommonMethodReducer();
        methodReducer.myReduce(values);
        List<Beer> beers = methodReducer.getBeers();
        Brewery brewery = methodReducer.getBrewery();
        for(Beer beerOrBrewery : beers){
            BeerOrBrewery result = new BeerOrBrewery();
            result.setBrewery(brewery);
            result.setBeer(beerOrBrewery);
            context.write(new IntWritable(beerOrBrewery.getId()), result);
        }
    }
}
