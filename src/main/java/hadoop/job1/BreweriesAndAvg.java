package hadoop.job1;

import hadoop.BeerOrBrewery;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BreweriesAndAvg implements Writable {
    private BeerOrBrewery beerOrBrewery = new BeerOrBrewery();
    private double avgBeer;
    private boolean isBeer;
    public void setBeerOrBrewery(BeerOrBrewery beerOrBrewery) {
        this.beerOrBrewery = beerOrBrewery;
    }

    public BeerOrBrewery getBeerOrBrewery() {
        return beerOrBrewery;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        beerOrBrewery.write(out);
        out.writeDouble(avgBeer);
        out.writeBoolean(isBeer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.beerOrBrewery = BeerOrBrewery.read(in);
        this.avgBeer = in.readDouble();
        this.isBeer = in.readBoolean();
    }

    public void setAvgBeer(double avgBeer) {
        this.avgBeer = avgBeer;
        this.isBeer=true;
    }
    public boolean getIsBeer() {
        return isBeer;
    }
    public double getAvgBeer() {
        return avgBeer;
    }
}

