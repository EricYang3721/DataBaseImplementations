package concurrency.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import concurrency.Transaction;
import concurrency.Transaction.TransactionUnreadyException;
import concurrency.StorageManager;
import concurrency.command.Command;
import concurrency.command.CommitCommand;
import concurrency.command.ReadCommand;
import concurrency.command.WriteCommand;
import concurrency.control.ConcurrencyController;

/**
 * The {@code ConcurrencyControlTest} class tests {@code ConcurrencyController}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 */
public class ConcurrencyControlTest {

	/**
	 * An {@code UnsupportedCommandException} is thrown if a {@code ConcurrencyControlTest} is given an unknown command.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class UnsupportedCommandException extends Exception {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 6294597597949005502L;

	}

	/**
	 * The {@code ConcurrencyController} that this {@code ConcurrencyControlTest} uses.
	 */
	ConcurrencyController<Integer> controller;

	/**
	 * A map that associates {@code Transaction} IDs with {@code Transaction}s.
	 */
	Map<Integer, Transaction<Integer>> transactions = new HashMap<Integer, Transaction<Integer>>();

	/**
	 * A {@code PrintStream} that this {@code ConcurrencyControlTest} uses.
	 */
	PrintStream out;

	/**
	 * Tests the specified {@code ConcurrencyController} using a schedule defined in the specified file.
	 * 
	 * @param fileName
	 *            the name of the file defining a schedule
	 * @param ccImpl
	 *            an implementation of {@code ConcurrencyController}
	 * @param out
	 *            a {@code PrintStream}
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	public ConcurrencyControlTest(String fileName, Class<?> ccImpl, PrintStream out)
			throws NumberFormatException, IOException {
		this.out = out;
		out.println("currency controller: " + ccImpl.getSimpleName());
		out.println("schedule: " + fileName);
		StorageManager<Integer> storageManager = new StorageManager<Integer>(out);
		try {
			controller = (ConcurrencyController<Integer>) ccImpl.getConstructor(StorageManager.class)
					.newInstance(storageManager);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
			return;
		}
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				out.println(line);
				String[] tokens = line.split(" ");
				int tID = Integer.parseInt(tokens[0].substring(1));
				try {
					switch (tokens[1]) {
					case "start":
						startTransaction(tID);
						break;
					case "read":
						assignCommand(tID, new ReadCommand(Integer.parseInt(tokens[2].substring(1))));
						break;
					case "write":
						assignCommand(tID, new WriteCommand<Integer>(Integer.parseInt(tokens[2].substring(1)),
								Integer.parseInt(tokens[3])));
						break;
					case "commit":
						assignCommand(tID, new CommitCommand());
						break;
					default:
						throw new UnsupportedCommandException();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} finally {
			reader.close();
		}
		out.println("--------------------");
		out.println("final database state");
		storageManager.print(out);
		out.println();
		for (Transaction<Integer> t : transactions.values()) // terminating all ongoing transactions
			t.stop();
	}

	/**
	 * Assigns the specified {@code Command} to the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 * @param command
	 *            the {@code Command}
	 * @throws TransactionUnreadyException
	 *             if a {@code Transaction} is not ready to take the next {@code Command}.
	 */
	void assignCommand(int tID, Command command) throws TransactionUnreadyException {
		transactions.get(tID).setNextCommand(command);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Starts the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	void startTransaction(int tID) {
		Transaction<Integer> t = new Transaction<Integer>(tID, controller, out);
		t.start();
		transactions.put(tID, t);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * The main method of {@code ConcurrencyControlTest}. This method tests {@code ConcurrencyController}s.
	 * 
	 * @param args
	 *            the arguments of this main method
	 */
	public static void main(String[] args) throws Exception {
		new ConcurrencyControlTest("schedule1.txt", concurrency.control.TimestampConcurrencyController.class,
				System.out);
		new ConcurrencyControlTest("schedule2.txt", concurrency.control.TimestampConcurrencyController.class,
				System.out);
		new ConcurrencyControlTest("schedule3.txt", concurrency.control.TimestampConcurrencyController.class,
				System.out);
		new ConcurrencyControlTest("schedule3.txt", concurrency.control.TimestampConcurrencyControllerTWR.class,
				System.out);
		new ConcurrencyControlTest("schedule1.txt", concurrency.control.Strict2PLConcurrencyController.class,
				System.out);
		new ConcurrencyControlTest("schedule2.txt", concurrency.control.Strict2PLConcurrencyController.class,
				System.out);
		new ConcurrencyControlTest("schedule3.txt", concurrency.control.Strict2PLConcurrencyController.class,
				System.out);
	}

}
