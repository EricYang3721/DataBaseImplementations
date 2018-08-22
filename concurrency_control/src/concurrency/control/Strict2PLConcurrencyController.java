package concurrency.control;

//import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import concurrency.StorageManager;

/**
 * The {@code Strict2PLConcurrencyController} class implements the strict 2 phase-locking protocol.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class Strict2PLConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates data IDs with locks.
	 */
	HashMap<Integer, ReentrantReadWriteLock> dID2lock = new HashMap<Integer, ReentrantReadWriteLock>();

	/**
	 * A map that associates transaction IDs with data IDs. Data IDs for each transaction ID is stored in a HashSet.
	 */
	HashMap<Integer, HashSet<Integer>> tID2dID = new HashMap<Integer, HashSet<Integer>>();
	/**
	 * Constructs a {@code Strict2PLConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public Strict2PLConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
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
//		Lock lock = getLock(dID).readLock();
//		lock.lock(); // wait until the read lock is acquired
//		remember the lock so that it can be released/unlocked when releaseAllRemainingLocks(int tID) is called
		ReentrantReadWriteLock.WriteLock rlock = getLock(dID).writeLock();   //get the lock respected to the data ID
		//ReentrantReadWriteLock.WriteLock wlock = getLock(dID).writeLock();  
		rlock.lock(); // wait until the read lock is acquired
		//wlock.lock();
		
		if(!tID2dID.containsKey(tID)) {    //if transaction ID is not in the tID2dID Hash Map
			HashSet<Integer> hset = new HashSet<>();   //create a new hashset and add the data IDs dID into the set
			hset.add(dID);
			tID2dID.put(tID, hset);  //put the new set into the transaction ID -- data ID HashMap
		} else {
			HashSet<Integer> hset = tID2dID.get(tID);  //if transaction ID is already in transaction ID -- data ID HashMap
			hset.add(dID);    //add new dID into the HashSet
			tID2dID.put(tID, hset);
		}
		return super.read(tID, dID);  //perform the read excution
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
//		Lock lock = getLock(dID).writeLock();
//		lock.lock(); // wait until the write lock is acquired
//		remember the lock so that it can be released/unlocked when releaseAllRemainingLocks(int tID) is called

		ReentrantReadWriteLock.WriteLock wlock = getLock(dID).writeLock(); // get write lock for data ID dID
		ReentrantReadWriteLock.ReadLock rlock = getLock(dID).readLock();
		wlock.lock();  //wait unitl the write lock is acquired
		rlock.lock();
		
		if(!tID2dID.containsKey(tID)) {        //if transcation ID is not in tID2dID hashmap,
			HashSet<Integer> hset = new HashSet<>();  // create the HashSet with current dID
			hset.add(dID);
			tID2dID.put(tID, hset);  // sore the (transcation ID, set(dID)) into the tID2dID hashmap
		} else {
			HashSet<Integer> hset = tID2dID.get(tID);  //transaction ID tID is already inside the tID2dID hashmap
			hset.add(dID);   //add the data ID dID into corresponding hashset in the tID2dID hashmap
			tID2dID.put(tID, hset);
		}
		super.write(tID, dID, dValue);  //perform write excution
	}

	/**
	 * Rolls back the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to roll back.
	 */
	@Override
	public void rollback(int tID) {
		super.rollback(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Commits the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to commit
	 */
	@Override
	public void commit(int tID) {
		super.commit(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Releases all remaining {@code Lock}s granted to the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	protected void releaseAllRemainingLocks(int tID) {
		// use a member variable you added
		HashSet<Integer> dIDset = tID2dID.remove(tID);
		for (Integer dID : dIDset) {
			
			getLock(dID).writeLock().unlock();
			getLock(dID).readLock().unlock();
		}
	}

	/**
	 * Returns the {@code Lock} associated with the specified data item.
	 * 
	 * @param dID
	 *            the ID of the data item
	 * @return the {@code Lock} associated with the specified data item
	 */
	protected ReentrantReadWriteLock getLock(int dID) {
		// use dID2lock
		if(dID2lock.containsKey(dID)) {  //if the dID is contained in the lock table dID2lock
			return dID2lock.get(dID);  // return the lock
		} else {   // if dID is not contained in the lock table
			ReentrantReadWriteLock RWlock = new ReentrantReadWriteLock(); //create a new lock
			dID2lock.put(dID, RWlock);
			return RWlock;
		}		
		 // was null
	}
}
