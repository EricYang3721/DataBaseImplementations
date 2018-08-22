package concurrency;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code StorageManager} manages a collection of data items.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class StorageManager<V> {

	/**
	 * A map that associates each data ID with a value.
	 */
	HashMap<Integer, V> dID2value = new HashMap<Integer, V>();

	/**
	 * A {@code PrintStream}.
	 */
	PrintStream out;

	/**
	 * Constructs a {@code StorageManager}.
	 * 
	 * @param out
	 *            a {@code PrintStream}
	 */
	public StorageManager(PrintStream out) {
		this.out = out;
	}

	/**
	 * Returns the value of the specified data item.
	 * 
	 * @param dID
	 *            the ID of the data item
	 * @return the value of the data item
	 */
	public V read(int dID) {
		V value = dID2value.get(dID);
		out.println("  % d" + dID + ": " + value);
		return value;
	}

	/**
	 * Stores the specified data item.
	 * 
	 * @param dID
	 *            the ID of the data item
	 * @param dValue
	 *            the value of the data item
	 * @return the previous value of the data item
	 */
	public V write(int dID, V dValue) {
		V oValue = dID2value.put(dID, dValue);
		out.println("  % d" + dID + ": " + oValue + " -> " + dValue);
		return oValue;
	}

	/**
	 * Prints the data items managed by this {@code StorageManager}
	 * 
	 * @param out
	 *            a {@code PrintStream}
	 */
	public void print(PrintStream out) {
		for (Map.Entry<Integer, V> entry : dID2value.entrySet())
			out.println("d" + entry.getKey() + ": " + entry.getValue());
	}

	/**
	 * Removes the specified data item.
	 * 
	 * @param dID
	 *            the ID of the data item
	 */
	public void remove(int dID) {
		V oValue = dID2value.get(dID);
		out.println("  % d" + dID + ": " + oValue + " -> removed");
		dID2value.remove(dID);
	}
}
