package dgim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DGIMAvg {
	final static long SECONDS_PER_DAY = 60*60*24;
	private static HashMap<String, LinkedList<Bucket>> windows = null;
	private static int windowSize = 3000; 		// unit: days;
	private static FileSystem fs;
	private static Path inputPath;
	private static Date currentTime;
	
	public static void initialize(Date time) throws IOException{
		if(windows != null)
			return;
		currentTime = time;
		windows = new HashMap<String, LinkedList<Bucket>>();
		Configuration conf = new Configuration();
		fs = FileSystem.get(conf);
		inputPath = new Path("output/part*");
		
		// load files that reducer output
		FileStatus[] inputFileStatus = fs.globStatus(inputPath);
		for(int nowFile = 0; nowFile < inputFileStatus.length; nowFile++){
			System.out.println("DGIMAvg/ nowFile:"+nowFile);
			Path nowFilePath = inputFileStatus[nowFile].getPath();
			FSDataInputStream inStream = fs.open(nowFilePath);
			Scanner scanner = new Scanner(inStream);
			
			while(scanner.hasNextLine()){
				
				String line = scanner.nextLine();
//				System.out.println("DGIMAvg/ line:"+line);
				String[] pair = line.split("\\t");
				String[] attrs = pair[1].split("_");
				Date reviewTime = new Date(Long.valueOf(pair[0])*1000);
				String productId = attrs[0];
				double score = Double.valueOf(attrs[2]);
				
				if(reviewTime.compareTo(currentTime) > 0)
					break;
				addReview(reviewTime, productId, score);
			}
			scanner.close();
		}
		
		System.out.println("DGIMAvg/ input loaded to " + currentTime);
	}
	public static double[] query(Date time, String productId, long k, ArrayList<Integer> counts) throws IOException {
		double apprScore = getScore(productId, k)/counts.get(0);
		double trueScore = getTrueScore(productId, k)/counts.get(1);
		System.out.println("DGIMAvg/ approximate:"+apprScore+ " true: "+trueScore);
		
		double[] result = {apprScore, trueScore};
		return result;
	}
	
	public static double getScore(String productId, long k){
		double ans = 0.0;
		LinkedList<Bucket> movieWindow = windows.get(productId);
		ListIterator<Bucket> iter = movieWindow.listIterator();
		while(iter.hasNext()){
			Bucket bucket = iter.next();
			int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-bucket.getTime().getTime(), TimeUnit.MILLISECONDS);
			
			if(diffInDays < k){ // the bucket is still in window
				ans += bucket.getSum();
			}
			else{
				ans += bucket.getSum()/2;
				break;
			}
		}
		
		return ans;
	}
	public static double getTrueScore(String productId, long k) throws IOException{
		double ans = 0;
		int startFile = 0;
		FileStatus[] inputFileStatus = fs.globStatus(inputPath);
		
		// find the start file
		for(startFile = 0; startFile < inputFileStatus.length; startFile++){
			Path nowFilePath = inputFileStatus[startFile].getPath();
			FSDataInputStream inStream = fs.open(nowFilePath);
			Scanner scanner = new Scanner(inStream);
			if(scanner.hasNextLine()){ // read the first line of the file
				String line = scanner.nextLine();
				String[] pair = line.split("\\t");
				Date reviewTime = new Date(Long.valueOf(pair[0])*1000);
				if(reviewTime.compareTo(currentTime) > 0){
					break;
				}
			}
			scanner.close();
		}
		startFile--;
		
		
		// from the start file, scan each file to calculate the true count
		for(int nowFile = startFile; nowFile < inputFileStatus.length; nowFile++){
			Path nowFilePath = inputFileStatus[nowFile].getPath();
			FSDataInputStream inStream = fs.open(nowFilePath);
			Scanner scanner = new Scanner(inStream);
			
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
				String[] pair = line.split("\\t");
				String[] attrs = pair[1].split("_");
				Date reviewTime = new Date(Long.valueOf(pair[0])*1000);
				String product = attrs[0];
				double score = Double.valueOf(attrs[2]);
				int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-reviewTime.getTime(), TimeUnit.MILLISECONDS);
				
				if(diffInDays < k && reviewTime.compareTo(currentTime) <= 0){
					if(productId.equals(product)){
						ans += score;
					}
				}
				// if the record time already exceeds current time
				else if(reviewTime.compareTo(currentTime) > 0){ 
					break;
				}
			}
			scanner.close();
		}
		return ans;
	}
	 
	// return top 10 movies by avg. score 
	public static ArrayList<MovieScorePair> topScoreMovies(int k) throws IOException{
		ArrayList<MovieScorePair> list = new ArrayList<MovieScorePair>();
		TreeMap<MovieScorePair, Double> sortedList = new TreeMap<MovieScorePair, Double>();
		
		for (String productId : windows.keySet()) {
			int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-windows.get(productId).getFirst().getTime().getTime(), TimeUnit.MILLISECONDS);
			if(diffInDays < k) {
				double score = DGIMAvg.getScore(productId, k)/DGIM.getCount(productId, k);
				MovieScorePair pair = new MovieScorePair(productId, score);
				sortedList.put(pair, score);
			}
			
		}
		for(int rank = 0; rank < 10; rank++){
			list.add(sortedList.pollLastEntry().getKey());
		}
		return list;
	}
	
	public static void addReview(Date time, String productId, double score){
		Bucket bucket = new Bucket(time, 0, score);
		LinkedList<Bucket> movieWindow = windows.get(productId);
		
		if(movieWindow == null)		// the stream of this movie does not exist yet
			movieWindow = new LinkedList<Bucket>();
		
		movieWindow.addFirst(bucket);

		if(productId.equals("6302967538")){
			System.out.println(movieWindow);

		int diffInDays = (int)TimeUnit.DAYS.convert(time.getTime()-movieWindow.getLast().getTime().getTime(), TimeUnit.MILLISECONDS);
		if(diffInDays > windowSize){
			movieWindow.removeLast();
		}
		
		windows.put(productId, movieWindow);
		
		ListIterator<Bucket> iter = movieWindow.listIterator();
		
		if(needMerge(iter)){
			ListIterator<Bucket> iter1 = movieWindow.listIterator();
			merge(productId, iter1);
		}
	}
	}
	
	private static void merge(String productId, ListIterator<Bucket> iter){
		iter.next();  // skip the first bucket
		Bucket next = iter.next();
		Bucket nextToNext = iter.next();
		int size = next.getSize();
		
		if(size<=1){
			nextToNext.setSize(size+1);
			size = nextToNext.getSize();
		}else{
			Double sum = next.getSum()+nextToNext.getSum();
			Double bound = Math.pow(2,size+1);
			if(sum.compareTo(bound)<=0){
				next.setSize(size+1);
				next.updateSum(nextToNext.getSum());
				iter.remove();
				size = next.getSize();
			}
			else{
				nextToNext.setSize(size+1);
				size = nextToNext.getSize();
			}
		}
		if(productId.equals("6302967538")){
			Deque<Bucket> movieWindow = windows.get(productId);
			System.out.println("~~~~~~~After Merge");
			System.out.print("~~~~~~~");
			System.out.println(movieWindow);
					ListIterator<Bucket> iter1 = getIterator(productId, size);
		if(needMerge(iter1)){
			ListIterator<Bucket> iter2 = getIterator(productId, size);
			merge(productId, iter2);
		}
		}


	}

	private static ListIterator<Bucket> getIterator(String productId, int size){
		LinkedList<Bucket> movieWindow = windows.get(productId);
		if(movieWindow == null)	// the stream of this movie does not exist yet
			movieWindow = new LinkedList<Bucket>();
		ListIterator<Bucket> iter = movieWindow.listIterator();
		while(iter.hasNext()){
			Bucket bucket = iter.next();
			if(bucket.getSize() == size)
				iter.previous();
				return iter;
		}
		return null;
	}
	
	private static boolean needMerge(Iterator<Bucket> iter){
		Bucket head, next, nextToNext;		
		try {
			head = iter.next();
			System.out.println(head.getSize());
			next = iter.next();
			System.out.println(next.getSize());
			nextToNext = iter.next();
	
			
			System.out.println(nextToNext.getSize());
			if(head.getSize() != next.getSize()){
				System.out.println("false 1");
				return false;
			}
			if(next.getSize() == nextToNext.getSize()){
				//System.out.println("---------> Need Merge!");
				return true;
			}
		} catch (Exception e) {
			System.out.println("false 2");
			return false;
		}
		System.out.println("false 3");
		return false;
	}
	
	
}