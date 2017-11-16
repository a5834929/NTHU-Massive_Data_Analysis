package dgim;

public class MovieScorePair implements Comparable<MovieScorePair>{
	private String productId;
	private Double score;
	
	public MovieScorePair(String productId, double score) {
		setProductId(productId);
		setScore(score);
	}
	public String getProductId() {
		return productId;
	}
	public Double getScore() {
		return score;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	@Override
	public int compareTo(MovieScorePair o) {
		if(score.equals(o.score))
			return productId.compareTo(o.getProductId())*-1;
		return score.compareTo(o.getScore());
	}
	
}
