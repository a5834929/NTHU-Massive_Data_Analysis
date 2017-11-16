package preprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PreprocessKeyComparator extends WritableComparator {
	public PreprocessKeyComparator() {
		super(Text.class, true);
	}

	// TODO Order by A -> a -> B -> b .... 
	public int compare(WritableComparable o1, WritableComparable o2) {
		String key1 = o1.toString();
		String key2 = o2.toString();
		return key1.compareTo(key2);
	}
}

