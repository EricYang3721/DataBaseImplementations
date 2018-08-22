package concurrency;

import java.io.PrintStream;

import concurrency.command.Command;
import concurrency.command.CommitCommand;
import concurrency.command.ReadCommand;
import concurrency.command.WriteCommand;
import concurrency.control.ConcurrencyController;
import concurrency.control.ConcurrencyController.AbortException;

/**
 * A {@code Transaction} represents a transaction.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class Transaction<V> extends Thread {

	/**
	 * An {@code TransactionUnreadyException} is thrown if a {@code Transaction} is not ready to take the next
	 * {@code Command}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class TransactionUnreadyException extends Exception {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 2740871001181936428L;

	}

	/**
	 * The ID of this {@code Transaction}.
	 */
	int tID;

	/**
	 * A {@code ConcurrencyController}.
	 */
	ConcurrencyController<V> controller;

	/**
	 * A {@code PrintStream}.
	 */
	PrintStream out;

	/**
	 * A flag indicating that this {@code Transaction} is ready for the next command.
	 */
	boolean readyForNextCommand = false;

	/**
	 * The next {@code Command} to execute.
	 */
	Command nextCommand;

	/**
	 * Constructs a {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 * @param controller
	 *            a {@code ConcurrencyController}
	 * @param out
	 *            a {@code PrintStream}
	 */
	public Transaction(int tID, ConcurrencyController<V> controller, PrintStream out) {
		this.tID = tID;
		this.controller = controller;
		this.out = out;
		controller.register(tID);
	}

	/**
	 * Runs a series of operations defined for this {@code Transaction}.
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		try {
			while (true) {
				Command command = nextCommand();
				if (command == null)
					return;
				else if (command instanceof ReadCommand)
					controller.read(tID, ((ReadCommand) command).dID());
				else if (command instanceof WriteCommand)
					controller.write(tID, ((WriteCommand<V>) command).dID(), ((WriteCommand<V>) command).dValue());
				else if (command instanceof CommitCommand)
					controller.commit(tID);
			}
		} catch (Exception e) {
			out.println("  % " + e.getClass().getSimpleName());
			if (e instanceof AbortException) {
				controller.rollback(tID);
			}
		}
	}

	/**
	 * Obtains the next {@code Command}.
	 * 
	 * @return the next {@code Command} to execute
	 */
	Command nextCommand() {
		synchronized (this) {
			try {
				readyForNextCommand = true;
				wait();
				readyForNextCommand = false;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return nextCommand;
	}

	/**
	 * Determines whether or not this {@code Transaction} is ready for the next command.
	 * 
	 * @return {@code true} if this {@code Transaction} is ready for the next command; {@code false} otherwise
	 */
	public boolean readyForNextCommand() {
		return readyForNextCommand;
	}

	/**
	 * Sets the next {@code Command} to execute.
	 * 
	 * @param nextCommand
	 *            the next {@code Command} to execute
	 * @throws TransactionUnreadyException
	 *             if a {@code Transaction} is not ready to take the next {@code Command}
	 */
	public void setNextCommand(Command nextCommand) throws TransactionUnreadyException {
		synchronized (this) {
			if (!readyForNextCommand) {
				this.nextCommand = null;
				notify();
				throw new TransactionUnreadyException();
			}
			this.nextCommand = nextCommand;
			notify();
		}
	}

}
