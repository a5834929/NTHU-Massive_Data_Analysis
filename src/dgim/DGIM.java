package dgim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DGIM {
	final static long SECONDS_PER_DAY = 60*60*24;
	private static HashMap<String, Deque<Bucket>> windows = null;
	private static int windowSize = 720; 		// unit: days;
	private static FileSystem fs;
	private static Path inputPath;
	private static Date currentTime;
	
	public static void initialize(Date time) throws IOException{
		if(windows != null)
			return;
		currentTime = time;
		windows = new HashMap<String, Deque<Bucket>>();
		Configuration conf = new Configuration();
		fs = FileSystem.get(conf);
		inputPath = new Path("output/part*");
		
		// load files that reducer output
		FileStatus[] inputFileStatus = fs.globStatus(inputPath);
		for(int nowFile = 0; nowFile < inputFileStatus.length; nowFile++){
			Path nowFilePath = inputFileStatus[nowFile].getPath();
			FSDataInputStream inStream = fs.open(nowFilePath);
			Scanner scanner = new Scanner(inStream);
			
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
				String[] pair = line.split("\\t");
				String[] attrs = pair[1].split("_");
				Date reviewTime = new Date(Long.valueOf(pair[0])*1000);
				String productId = attrs[0];
				
				if(reviewTime.compareTo(currentTime) > 0)
					break;
				addReview(reviewTime, productId);
			}
			scanner.close();
		}
		
		System.out.println("DGIM/ input loaded to " + currentTime);
	}
	public static ArrayList<Integer> query(Date time, String productId, long k) throws IOException {
		ArrayList<Integer> ans = new ArrayList<Integer>(2);
		int count = getCount(productId, k);
		int trueCount = getTrueCount(productId, k);
		ans.add(count);
		ans.add(trueCount);
		System.out.println("DGIM/ approximate:"+ count + " true: "+ trueCount);
		return ans;
	}
	
	public static int getCount(String productId, long k){
		int ans = 0;
		Deque<Bucket> movieWindow = windows.get(productId);
		Iterator<Bucket> iter = movieWindow.iterator();
		while(iter.hasNext()){
			Bucket bucket = iter.next();
			int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-bucket.getTime().getTime(), TimeUnit.MILLISECONDS);
			
			if(diffInDays < k){ // the bucket is still in window
				ans += bucket.getSize();
			}
			else{
				ans += bucket.getSize()/2;
				break;
			}
		}
		return ans;
	}
	public static int getTrueCount(String productId, long k) throws IOException{
		int ans = 0;
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
		startFile = startFile-1 < 0 ? 0 : startFile-1;
		
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
				
				int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-reviewTime.getTime(), TimeUnit.MILLISECONDS);
				if(diffInDays < k && reviewTime.compareTo(currentTime) <= 0){
					if(productId.equals(product)){
						ans++;
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
	
	// return top 10 movies by the number of reviews 
	public static ArrayList<MovieScorePair> topReviewMovies(int k) throws IOException{
		ArrayList<MovieScorePair> list = new ArrayList<MovieScorePair>();
		TreeMap<MovieScorePair, Double> sortedList = new TreeMap<MovieScorePair, Double>();
		
		for (String productId : windows.keySet()) {
			int diffInDays = (int)TimeUnit.DAYS.convert(currentTime.getTime()-windows.get(productId).getFirst().getTime().getTime(), TimeUnit.MILLISECONDS);
			if(diffInDays < k) {
				double score = DGIM.getCount(productId, k);
				MovieScorePair pair = new MovieScorePair(productId, score);
				sortedList.put(pair, score);
			}
			
		}
		for(int rank = 0; rank < 10; rank++){
			list.add(sortedList.pollLastEntry().getKey());
		}
		return list;
	}
	
	public static void addReview(Date time, String productId){
		Bucket bucket = new Bucket(time, 1);
		Deque<Bucket> movieWindow = windows.get(productId);
		
		if(movieWindow == null)		// the stream of this movie does not exist yet
			movieWindow = new LinkedList<Bucket>();
		
		movieWindow.addFirst(bucket);
		int diffInDays = (int)TimeUnit.DAYS.convert(time.getTime()-movieWindow.getLast().getTime().getTime(), TimeUnit.MILLISECONDS);

		if(diffInDays > windowSize){
			movieWindow.removeLast();
		}
		
		windows.put(productId, movieWindow);
		
		Iterator<Bucket> iter = movieWindow.iterator();
		
		if(needMerge(iter)){
			Iterator<Bucket> iter1 = movieWindow.iterator();
			merge(productId, iter1);
		}
	}
	
	private static void merge(String productId, Iterator<Bucket> iter){
		iter.next();  // skip the first bucket
		Bucket next = iter.next();
		Bucket nextToNext = iter.next();
		int nextSize = next.getSize();
		int nextToNextSize = nextToNext.getSize();
		next.setSize(nextSize+nextToNextSize);
		iter.remove();
		nextSize = next.getSize();
		Iterator<Bucket> iter1 = getIterator(productId, nextSize);
		if(needMerge(iter1)){
			Iterator<Bucket> iter2 = getIterator(productId, nextSize);
			merge(productId, iter2);
		}
	}

	private static Iterator<Bucket> getIterator(String productId, int size){
		Deque<Bucket> movieWindow = windows.get(productId);
		if(movieWindow == null)	// the stream of this movie does not exist yet
			movieWindow = new LinkedList<Bucket>();
		
		Iterator<Bucket> iter = movieWindow.iterator();
		while(iter.hasNext()){
			Bucket bucket = iter.next();
			if(bucket.getSize() == size)
				return iter;
		}
		return null;
	}
	
	private static boolean needMerge(Iterator<Bucket> iter){
		Bucket head, next, nextToNext;		
		try {
			head = iter.next();
			next = iter.next();
			nextToNext = iter.next();
			if(head.getSize() != next.getSize())
				return false;
			if(next.getSize() == nextToNext.getSize())
				return true;
		} catch (Exception e) {
			return false;
		}
		return false;
	}
	
}
