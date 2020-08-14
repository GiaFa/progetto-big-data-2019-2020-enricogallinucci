package hadoop.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Pair implements WritableComparable<Pair> {

    private IntWritable brewery;
    private IntWritable beerclass;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(brewery.get());
        out.writeInt(beerclass.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.brewery = new IntWritable(in.readInt());
        this.beerclass = new IntWritable(in.readInt());
    }

    public int getBeerclass() {
        return beerclass.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair pair = (Pair) o;
        return brewery == pair.brewery &&
                beerclass == pair.beerclass;
    }

    @Override
    public int hashCode() {
        return Objects.hash(brewery, beerclass);
    }

    public void setBeerclass(int beerclass) {
        this.beerclass = new IntWritable(beerclass);
    }

    public int getBrewery() {
        return brewery.get();
    }

    public void setBrewery(int brewery) {
        this.brewery = new IntWritable(brewery);
    }

    @Override
    public int compareTo(Pair o) {
        int cmp=beerclass.compareTo(o.beerclass);
        if(cmp!=0)
            return cmp;
        return brewery.compareTo(o.brewery);
    }
}
