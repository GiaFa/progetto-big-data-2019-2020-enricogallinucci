package hadoop;

public class Brewery {
    private final int id;
    private final String name;

    public Brewery(int id, String name){
        this.id = id;
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }
}
