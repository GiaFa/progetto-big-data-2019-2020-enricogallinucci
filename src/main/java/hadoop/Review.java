package hadoop;

public class Review {

    private final int beer_id;
    private final double overall;

    public Review(int beer_id, double overall){
        this.beer_id = beer_id;
        this.overall = overall;
    }


    public double getOverall() {
        return overall;
    }

    public int getBeerId() {
        return beer_id;
    }
}
