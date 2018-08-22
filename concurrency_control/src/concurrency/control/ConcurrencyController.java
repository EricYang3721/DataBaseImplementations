package concurrency.control;

import java.util.HashMap;
import java.util.Map;

import concurrency.StorageManager;

/**
 * A {@code ConcurrencyController} controls concurrent execution of {@code Transaction}s in a manner that achieves
 * conflict-serializability.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class ConcurrencyController<V> {

	/**
	 * An {@code AbortException} is thrown if a {@code Transaction} needs to be aborted.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class AbortException extends Exception {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = -8094860179878777318L;

		/**
		 * Constructs an {@code AbortException}.
		 */
		public AbortException() {
		}

	}

	/**
	 * An {@code InvalidTransactionIDException} is thrown if an invalid {@code Transaction} ID is given to a
	 * {@code ConcurrencyController}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class InvalidTransactionIDException extends Exception {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 7967151005363447769L;

		/**
		 * Constructs an {@code InvalidTransactionIDException}.
		 */
		public InvalidTransactionIDException() {
		}

	}

	/**
	 * A {@code StorageManager}.
	 */
	private StorageManager<V> storageManager;

	/**
	 * A map that associates each {@code Transaction} ID with the data for rolling back the {@code Transaction}.
	 */
	private Map<Integer, Map<Integer, V>> tID2OrignalValues = new HashMap<Integer, Map<Integer, V>>();

	/**
	 * Constructs a {@code ConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public ConcurrencyController(StorageManager<V> storageManager) {
		this.storageManager = storageManager;
	}

	/**
	 * Registers a {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	public void register(int tID) {
	}

	/**
	 * Handles a read request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	public V read(int tID, int dID) throws InvalidTransactionIDException, AbortException {
		return storageManager.read(dID);
	}

	/**
	 * Handles a write request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @param dValue
	 *            the value of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	public void write(int tID, int dID, V dValue) throws InvalidTransactionIDException, AbortException {
		V pValue = storageManager.write(dID, dValue);
		Map<Integer, V> orignalValues = tID2OrignalValues.get(tID);
		if (orignalValues == null) {
			orignalValues = new HashMap<Integer, V>();
			tID2OrignalValues.put(tID, orignalValues);
		}
		if (orignalValues.get(dID) == null)
			orignalValues.put(dID, pValue);
	}

	/**
	 * Rolls back the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to roll back.
	 */
	public void rollback(int tID) {
		Map<Integer, V> orignalValues = tID2OrignalValues.get(tID);
		if (orignalValues != null)
			for (Map.Entry<Integer, V> entry : orignalValues.entrySet()) {
				if (entry.getValue() == null)
					storageManager.remove(entry.getKey());
				else
					storageManager.write(entry.getKey(), entry.getValue());
			}
	}

	/**
	 * Commits the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to commit.
	 */
	public void commit(int tID) {
		tID2OrignalValues.remove(tID);
	}
}
