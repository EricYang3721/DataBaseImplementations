package concurrency.command;

/**
 * A {@code WriteCommand} represents a command for updating a data item.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class WriteCommand<V> implements Command {

	/**
	 * The ID of the data item.
	 */
	int dID;

	/**
	 * The value of the data item.
	 */
	V dValue;

	/**
	 * Constructs a {@code WriteCommand}.
	 * 
	 * @param dID
	 *            the ID of the data item
	 * @param dValue
	 *            the value of the data item
	 */
	public WriteCommand(int dID, V dValue) {
		this.dID = dID;
		this.dValue = dValue;
	}

	/**
	 * Returns the ID of the data item.
	 * 
	 * @return the ID of the data item
	 */
	public int dID() {
		return dID;
	}

	/**
	 * Returns the value of the data item.
	 * 
	 * @return the value of the data item
	 */
	public V dValue() {
		return dValue;
	}

}
