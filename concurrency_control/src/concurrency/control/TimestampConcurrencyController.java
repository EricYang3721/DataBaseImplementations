package concurrency.control;
import java.lang.Math;
import java.util.HashMap;

import concurrency.StorageManager;

/**
 * The {@code TimestampConcurrencyController} class implements timestamp-based concurrency control.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates {@code Transaction} IDs with timestamps.
	 */
	HashMap<Integer, Integer> tID2timestamp = new HashMap<Integer, Integer>();
	
	
	/**
	 * Maps that associates {@code dataitem} IDs with Write-timestamps, and Read-timestamps.
	 */
	HashMap<Integer, Integer> dID2Wtimestamp = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> dID2Rtimestamp = new HashMap<Integer, Integer>();
	/**
	 * The number of {@code Transaction}s that have been registered so far.
	 */
	int count = 0;

	/**
	 * Constructs a {@code TimestampConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
	}

	/**
	 * Registers a {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	public void register(int tID) {
		tID2timestamp.put(tID, count++);
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
	@Override
	public V read(int tID, int dID) throws InvalidTransactionIDException, AbortException {
//		Integer timestamp = tID2timestamp.get(tID); // the timestamp of the transaction specified by tID
		Integer timestamp = tID2timestamp.get(tID); // the timestamp of the transaction specified by tID

		if(!dID2Wtimestamp.containsKey(dID)) {	// if W-timestamp(dID) not exist	
			 dID2Rtimestamp.put(dID, tID);			 // add R-timespace(dID) = tID;
			 return super.read(tID, dID);     // read dataitem dID from transaction tID
		 } else if (timestamp >= dID2Wtimestamp.get(dID)) { // if TS(tID) >= W-timestamp(dID)
			 if(!dID2Rtimestamp.containsKey(dID)) {   // if dID is not in hashmap <dID, R-timestamp>
				 dID2Rtimestamp.put(dID,  dID2Rtimestamp.get(dID));  // add it into the above hashmap
			 } else {
				 dID2Rtimestamp.put(dID, Math.max(timestamp, dID2Rtimestamp.get(dID)));	 // update R-timestamp(dID) as max(TS(tID), R-timestamp(dID)) 
			 }			 		
			 return super.read(tID, dID);   // read dataitem dID from transaction tID
		 }	else {			 
			 
			 throw new AbortException(); // rollback and throw Abort Exception
		 }		
		 //return null;
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
			throw new AbortException();  // rollback and throw AbortException
		} else {
			//register(tID);
			dID2Wtimestamp.put(dID, timestamp);  //record W-timestamp(dID) = TS(tID)
			super.write(tID, dID, dValue); // write it into the disk
		}
		
	}

}
