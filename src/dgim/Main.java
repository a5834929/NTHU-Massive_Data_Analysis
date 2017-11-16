package dgim;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Main {
	private static Date currentTime = null;
	private static DateFormat dateFormat = new SimpleDateFormat("y/M/d H");
	
	public static void main(String[] args) throws IOException {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Specify current time: (1999/2/9 8)");
		try {
			currentTime = dateFormat.parse(scanner.nextLine());
			DGIM.initialize(currentTime);
			DGIMAvg.initialize(currentTime);
		} catch (ParseException e) {
			System.out.println("Wrong time format!");
			System.exit(1);
		}
		
		System.out.println("Enter k (day): (500) or productId and k (day): (6302967538 500)");
		while(scanner.hasNextLine()){
			String[] token = scanner.nextLine().split(" ");
			if(token.length == 1){	// only k
				int k = Integer.valueOf(token[0]);
				System.out.println("Top 10 movies by avg. score:");
				ArrayList<MovieScorePair> topScoreList = DGIMAvg.topScoreMovies(k);
				for (int rank = 0; rank < 10; rank++) {
					MovieScorePair pair = topScoreList.get(rank);
					System.out.println("rank"+rank+": "+pair.getProductId()+"("+pair.getScore()+")");
				}
				System.out.println("Top 10 movies by the number of reviews");
				ArrayList<MovieScorePair> topReviewList = DGIM.topReviewMovies(k);
				for (int rank = 0; rank < 10; rank++) {
					MovieScorePair pair = topReviewList.get(rank);
					System.out.println("rank"+rank+": "+pair.getProductId()+"("+pair.getScore()+")");
				}
				
			}
			else if(token.length == 2){ // productId and k
				
				ArrayList<Integer> counts = DGIM.query(currentTime, token[0], Long.valueOf(token[1]));
				DGIMAvg.query(currentTime, token[0], Long.valueOf(token[1]), counts);	
				
			}
			else {
				System.err.println("Wrong Input.");
			}
			System.out.println("Enter k (day): (500) or productId and k (day): (6302967538 500)");
		};
		
		/* query for product id 
		System.out.println("Enter productId and k (day): (6302967538 500)");
		while(scanner.hasNextLine()){
			String[] token = scanner.nextLine().split(" ");
			ArrayList<Integer> counts = DGIM.query(currentTime, token[0], Long.valueOf(token[1]));
			DGIMAvg.query(currentTime, token[0], Long.valueOf(token[1]), counts);
			System.out.println("Enter productId and k (day): (6302967538 500)");
		};
		*/
		
		
		scanner.close();
		
	}
	
}
