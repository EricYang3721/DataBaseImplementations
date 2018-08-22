package concurrency.command;

/**
 * A {@code ReadCommand} represents a command for reading a data item.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class ReadCommand implements Command {

	/**
	 * The ID of the data item.
	 */
	int dID;

	/**
	 * Constructs a {@code ReadCommand}.
	 * 
	 * @param dID
	 *            the ID of the data item
	 */
	public ReadCommand(int dID) {
		this.dID = dID;
	}

	/**
	 * Returns the ID of the data item.
	 * 
	 * @return the ID of the data item
	 */
	public int dID() {
		return dID;
	}

}
