package hadoop.job1;

import hadoop.Beer;
import hadoop.BeerOrBrewery;
import hadoop.Brewery;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BeersAndBreweriesReducer extends Reducer<IntWritable, BeerOrBrewery,IntWritable,BeerOrBrewery> {
    public void reduce(IntWritable key, Iterable<BeerOrBrewery> values, Context context) throws IOException, InterruptedException {
        List<Beer> beers = new ArrayList<>();
        Brewery brewery = new Brewery();
        for(BeerOrBrewery beerOrBrewery : values){
            if(!beerOrBrewery.isBeer()){
                brewery = beerOrBrewery.getBrewery();
            } else {
                beers.add(beerOrBrewery.getBeer());
            }
        }
        for(Beer beerOrBrewery : beers){
            BeerOrBrewery result = new BeerOrBrewery();
            result.setBrewery(brewery);
            result.setBeer(beerOrBrewery);
            context.write(new IntWritable(beerOrBrewery.getId()), result);
        }
    }
}
