package hadoop.commonjob;

import hadoop.Beer;
import hadoop.BeerOrBrewery;
import hadoop.Brewery;

import java.util.ArrayList;
import java.util.List;

public class CommonMethodReducer {
    private List<Beer> beers;
    private  Brewery brewery;
    public void myReduce(Iterable<BeerOrBrewery> values) {
        beers = new ArrayList<>();
        brewery = new Brewery();
        for (
                BeerOrBrewery beerOrBrewery : values) {
            if (!beerOrBrewery.isBeer()) {
                brewery = beerOrBrewery.getBrewery();
            } else {
                beers.add(beerOrBrewery.getBeer());
            }
        }
    }
    public Brewery getBrewery() {
        return brewery;
    }
    public List<Beer> getBeers() {
        return beers;
    }
}
