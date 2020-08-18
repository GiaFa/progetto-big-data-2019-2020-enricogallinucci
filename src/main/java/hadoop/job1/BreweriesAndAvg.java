package hadoop.job1;

import hadoop.BeerOrBrewery;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BreweriesAndAvg implements Writable {
    private BeerOrBrewery beerOrBrewery = new BeerOrBrewery();
    private double avgBeer;
    private boolean isAvg;
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
        out.writeBoolean(isAvg);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.beerOrBrewery = BeerOrBrewery.read(in);
        this.avgBeer = in.readDouble();
        this.isAvg = in.readBoolean();
    }

    public void setAvgBeer(double avgBeer) {
        this.avgBeer = avgBeer;
        this.isAvg=true;
    }
    public boolean getIsAvg() {
        return isAvg;
    }
    public double getAvgBeer() {
        return avgBeer;
    }
}

