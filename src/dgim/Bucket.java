package dgim;

import java.util.Date;

public class Bucket {
	private Date time;
	private int size;
	private double sum;
	
	public Bucket(Date time, int size) {
		this(time, size, 0);
	}
	
	public Bucket(long time, int size) {
		this(time, size, 0);
	}
	public Bucket(Date time, int size, double sum){
		setTime(time);
		this.size = size;
		this.sum = sum;
	}
	public Bucket(long time, int size, double sum){
		setTime(new Date(time));
		this.size = size;
		this.sum = sum;
	}
	
	public Date getTime() {
		return time;
	}
	public int getSize() {
		return size;
	}
	public double getSum(){
		return sum;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	public void updateSum(double sum){
		this.sum += sum;
	}
	@Override
	public String toString() {
		return "Bucket: time "+getTime()+", size: "+getSize()+", sum: "+getSum();
	}
	
	
	
}
