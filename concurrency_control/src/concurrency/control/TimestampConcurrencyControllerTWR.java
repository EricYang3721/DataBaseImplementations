package concurrency.control;

import concurrency.StorageManager;
//import concurrency.control.ConcurrencyController.AbortException;

/**
 * The {@code TimestampConcurrencyControllerTWR} class implements timestamp-based concurrency control with Thomas' write rule.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyControllerTWR<V> extends TimestampConcurrencyController<V> {

	/**
	 * Constructs a {@code TimestampConcurrencyControllerTWR}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyControllerTWR(StorageManager<V> storageManager) {
		super(storageManager);
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
	@Override
	public void write(int tID, int dID, V dValue) throws InvalidTransactionIDException, AbortException {
//		Integer timestamp = tID2timestamp.get(tID); // the timestamp of the transaction specified by tID
		if(!tID2timestamp.containsKey(tID)) { //If transcation is not registered, register it
			register(tID);
		}
		Integer timestamp = tID2timestamp.get(tID); // the time stamp of the transaction specified by tID
		if(dID2Rtimestamp.containsKey(dID) && timestamp < dID2Rtimestamp.get(dID)) {  // if  R-timestamp(dID) exists and TS(tID) < R-timestamp(dID)
			throw new AbortException();  // rollback and throw AbortException
		} else if (dID2Wtimestamp.containsKey(dID) && timestamp < dID2Wtimestamp.get(dID)) { // if  W-timestamp(dID) exists and TS(tID) < W-timestamp(dID)
			//throw new AbortException();  // rollback and throw AbortException
		} else {
			//register(tID);
			dID2Wtimestamp.put(dID, timestamp);  //record W-timestamp(dID) = TS(tID)
			super.write(tID, dID, dValue); // write it into the disk
		}
		
		
	}

}
