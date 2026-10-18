package didameetings.server;

import java.util.Enumeration;
import java.util.Hashtable;

public class RequestHistory {
  private Hashtable<Integer, RequestRecord> pending;
  private Hashtable<Integer, RequestRecord> executing;
  private Hashtable<Integer, RequestRecord> processed;

  public RequestHistory() {
    this.pending = new Hashtable<Integer, RequestRecord>();
    this.executing = new Hashtable<Integer, RequestRecord>();
    this.processed = new Hashtable<Integer, RequestRecord>();
  }

  public synchronized RequestRecord getIfPending(int requestid) {
    Integer id = new Integer(requestid);
    return this.pending.get(id);
  }

  public synchronized RequestRecord getFirstPending() {
    Enumeration<Integer> pendingids = this.pending.keys();
    if (pendingids.hasMoreElements()) return this.pending.get(pendingids.nextElement());
    else return null;
  }

  public synchronized RequestRecord getIfExecuting(int requestid) {
    Integer id = new Integer(requestid);
    return this.executing.get(id);
  }

  public synchronized RequestRecord getIfExists(int requestid) {
    RequestRecord record;
    Integer id = new Integer(requestid);

    record = this.pending.get(id);
    if (record == null) 
      record = this.executing.get(id);
    return this.processed.get(id);
  }
  
  public synchronized void addToPending(int requestid, RequestRecord record) {
    Integer id = new Integer(requestid);

    this.pending.put(id, record);
    this.notifyAll();
  }

  // If request is already in executing log, do not remove it from pending, as it is not there
  public synchronized RequestRecord moveToExecuting(int requestid) {
    Integer id = new Integer(requestid);
    RequestRecord record = this.getIfExecuting(requestid);
    
    if(record != null)
      return record;

    if(this.getIfPending(requestid) == null)  
      return null;
    record = this.pending.remove(id);
    this.executing.put(id, record);
    return record;
  }

  public synchronized RequestRecord moveToProcessed(int requestid) {
    Integer id = new Integer(requestid);
    RequestRecord record = this.executing.remove(id);
    this.processed.put(id, record);
    return record;
  }


  // Prints Pending, Executing and Processed hashtables status
  public synchronized void printStatus() {
    System.out.println("===== RequestHistory Status =====");

    System.out.println("\n\nPending:");
    for (Integer id : pending.keySet()) {
        System.out.println("  " + id + " -> " + pending.get(id).getId());
    }

    System.out.println("\n\nExecuting:");
    for (Integer id : executing.keySet()) {
        System.out.println("  " + id + " -> " + executing.get(id).getId());
    }

    System.out.println("\n\nProcessed:");
    for (Integer id : processed.keySet()) {
        System.out.println("  " + id + " -> " + processed.get(id).getId());
    }

    System.out.println("================================");
  }
}
