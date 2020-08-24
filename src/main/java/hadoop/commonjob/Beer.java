package hadoop.commonjob;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Beer implements Writable {
    private int id;
    private String name = "";
    private int brewery;

    public static Beer read(DataInput in) throws IOException {
        Beer w = new Beer();
        w.readFields(in);
        return w;
    }

    public Beer(){}

    public Beer(int id, String name, int brewery){
        this.id = id;
        this.brewery = brewery;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Beer beer = (Beer) o;
        return id == beer.id &&
                brewery == beer.brewery &&
                Objects.equals(name, beer.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, brewery);
    }

    public String getName() {
        return name;
    }

    public int getBrewery() {
        return brewery;
    }

    public int getId() {
        return id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(brewery);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
        this.brewery = in.readInt();
    }
}
