package hadoop.commonjob;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BeerOrBrewery implements Writable {

    private Beer beer = new Beer();
    private Brewery brewery = new Brewery();
    private boolean isBeer;

    public void setBeer(Beer beer) {
            this.beer = beer;
            this.isBeer = true;
    }

    public static BeerOrBrewery read(DataInput in) throws IOException {
        BeerOrBrewery w = new BeerOrBrewery();
        w.readFields(in);
        return w;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeerOrBrewery that = (BeerOrBrewery) o;
        return isBeer == that.isBeer &&
                Objects.equals(beer, that.beer) &&
                Objects.equals(brewery, that.brewery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(beer, brewery, isBeer);
    }

    public void setBrewery(Brewery brewery) {
        this.brewery = brewery;
    }

    public Beer getBeer() {
        return beer;
    }

    public Brewery getBrewery() {
        return brewery;
    }

    public boolean isBeer() {
        return isBeer;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.beer.write(out);
        this.brewery.write(out);
        out.writeBoolean(this.isBeer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.beer = Beer.read(in);
        this.brewery = Brewery.read(in);
        this.isBeer = in.readBoolean();
    }
}
