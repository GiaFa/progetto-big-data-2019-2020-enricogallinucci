package hadoop;

public class Beer{
    private final int id;
    private final String name;
    private final int brewery;

    public Beer(int id, String name, int brewery){
        this.id = id;
        this.brewery = brewery;
        this.name = name;
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
}
