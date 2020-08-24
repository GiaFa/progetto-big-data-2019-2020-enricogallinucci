package hadoop.job2;

import hadoop.commonjob.BeerOrBrewery;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BreweriesAndClasses  implements Writable {
    private BeerOrBrewery beerOrBrewery = new BeerOrBrewery();
    private boolean isClass;
    private int beerClass;

    public void setBeerClass(int beerClass) {
        this.isClass = true;
        this.beerClass = beerClass;
    }

    public void setBeerOrBrewery(BeerOrBrewery beerOrBrewery) {
        this.beerOrBrewery = beerOrBrewery;
    }

    public int getBeerClass() {
        return beerClass;
    }

    public BeerOrBrewery getBeerOrBrewery() {
        return beerOrBrewery;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        beerOrBrewery.write(out);
        out.writeInt(beerClass);
        out.writeBoolean(isClass);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.beerOrBrewery = BeerOrBrewery.read(in);
        this.beerClass = in.readInt();
        this.isClass = in.readBoolean();
    }

    public boolean isClass() {
        return isClass;
    }
}

