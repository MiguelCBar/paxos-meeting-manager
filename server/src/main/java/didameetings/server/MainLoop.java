package didameetings.server;

import java.util.ArrayList;
import java.util.List;

import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxosServiceGrpc;
import didameetings.util.CollectorStreamObserver;
import didameetings.util.GenericResponseCollector;
import io.grpc.ManagedChannel;

public class MainLoop implements Runnable {
  DidaMeetingsServerState server_state;
  Object applyProgressLock = new Object();  // LOCK to synchronize apply progress
  private volatile int nextToApply;
  private final Thread applyThread;
  private boolean has_work;
  private int next_log_entry;
  private List<Integer> all_participants;
  private int n_participants;
  private String[] targets;
  private ManagedChannel[] channels;
  private DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;
  private List<DidaMeetingsPaxos.PhaseOneInstance> phase_one_requests_to_execute;
  private volatile int phase_one_num_instances_to_accept;

  public MainLoop(DidaMeetingsServerState state) {
    this.server_state = state;
    this.has_work = false;
    this.next_log_entry = 0;
    this.nextToApply = 0;
    this.phase_one_num_instances_to_accept = -1;
    this.phase_one_requests_to_execute = new ArrayList<>();
    this.applyThread = new Thread(new ApplyRunnable()); // Start thread that handles the write in the Replicas of the requests
    this.applyThread.start();
  }

  // Wake up main loop waitForWork wait
  public synchronized void wakeup() {
    this.has_work = true;
    notify();
  }

  // Get the lock object used to synchronize application progress
  public Object getApplyProgressLock() {
    return this.applyProgressLock;
  }

  public void notifyApplyProgress() {        
    Object applyProgressLock = getApplyProgressLock();
    synchronized (applyProgressLock) {
        applyProgressLock.notifyAll(); // wakes up topic operations to check if can proceed
    }
  }

  // Get the number of instances needed to update the completed ballot in phase one
  public int getPhaseOneNumInstancesToAccept() {
    return this.phase_one_num_instances_to_accept;
  }

  // Get the next instance to apply
  public int getNextToApply() {
    return this.nextToApply;
  }

  public synchronized void incrementNextToApply() {
    this.nextToApply++;
  }

  public void run() {
    while(true) processEntry();
  }

  public synchronized void resetMainLoopLogCounter() {
    this.next_log_entry = 0;
  }

  public synchronized void incrementNextLogEntry() {
    this.next_log_entry++;
  }



  public synchronized void waitForWork() {
    // Either this proposer is not the leader or no request is pending
    this.has_work = false;
    while (!has_work) {
      System.out.println("[WAITING] No pending requests or work, waiting...");
      try { 
        wait(); 
      } catch (InterruptedException e) { 
        Thread.currentThread().interrupt();
      }
    }
  }

  public void checkPermissionForPhaseTwo() {
    Object activatedLock = server_state.getConsoleActivationLock();
    synchronized (activatedLock) {
      // check if console already activated
      while (!this.server_state.getActivated()) {
        try {
          System.out.println("Main loop waiting for console activation...");
          System.out.println("[NEW LEADER] Waiting for activation message from console...");
          server_state.getConsoleActivationLock().wait();  // waits until activated
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  public synchronized void processEntry() {
    
    while(true) {
      // Initialize configuration for Paxos Instance
      int ballot = this.server_state.getCurrentBallot();
      int completed_ballot = this.server_state.getCompletedBallot();
      List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
      int quorum = this.server_state.scheduler.quorum(ballot);
      int n_acceptors = acceptors.size();

      this.server_state.checkCrashDebug(this.server_state.getDebugMode());
      int phase_two_command_id = 0;

      // Check if I am the leader and if I need to do phase 1 
      if(ballot > -1 
          && this.server_state.scheduler.leader(ballot) == this.server_state.getServerId()
          && this.server_state.getHasPermissionForPhaseTwo() == false) {

        System.out.println("\n\n-------------------------------------------------------------------------------\n\n");
        System.out.println("[NEW LEADER]\t\t\tWill now execute PAXOS PHASE 1");
        System.out.println("\n\n-------------------------------------------------------------------------------\n\n");
            
        this.server_state.checkNonCrashDebugMode();
        
        // If current ballot is the first one, 0, then skip
        if(this.server_state.getCurrentBallot() == 0) {
          this.server_state.setHasPermissionForPhaseTwo(true);
          continue;
        }


        //Redefine acceptors to ask for logs of completed ballot
        List<Integer> completed_ballot_acceptors = this.server_state.scheduler.acceptors(completed_ballot);  
        int completed_ballot_n_acceptors = completed_ballot_acceptors.size();
        int completed_ballot_quorum = this.server_state.scheduler.quorum(completed_ballot);
        
        DidaMeetingsPaxos.PhaseOneRequest.Builder phase_one_request_builder =
                DidaMeetingsPaxos.PhaseOneRequest.newBuilder();
        phase_one_request_builder.setRequestballot(ballot);

        DidaMeetingsPaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();

        PhaseOneResponseProcessor phase_one_processor =
                new PhaseOneResponseProcessor(this.server_state.scheduler, completed_ballot_quorum);

                ArrayList<DidaMeetingsPaxos.PhaseOneReply> phase_one_responses = new ArrayList<>();
                GenericResponseCollector<DidaMeetingsPaxos.PhaseOneReply> phase_one_collector =
                new GenericResponseCollector<>(phase_one_responses, completed_ballot_n_acceptors, phase_one_processor);


        System.out.println("\n[NEW LEADER] Sending PAXOS PHASE 1 to acceptors.");
        for (int i = 0; i < completed_ballot_n_acceptors; i++) {
          CollectorStreamObserver<DidaMeetingsPaxos.PhaseOneReply> phase_one_observer =
                  new CollectorStreamObserver<>(phase_one_collector);
          this.server_state.async_stubs[completed_ballot_acceptors.get(i)].phaseone(
                  phase_one_request, phase_one_observer);
        }

        phase_one_collector.waitUntilDone();

        if(phase_one_processor.getAccepted() == false) { // Must abort, not accepted, higher ballot needed
          System.out.println("\n[NEW LEADER] Did not reach QUORUM, aborting...");
          int maxballot = phase_one_processor.getMaxBallot();
          if (maxballot > this.server_state.getCurrentBallot())
            this.server_state.setCurrentBallot(maxballot);
          this.server_state.abortCompletedBallot();    // wakeup WaitForCompletedBallot -> abort
        } else {
          System.out.println("\n[NEW LEADER] Reached QUORUM, continuing with execution.");
          this.server_state.resetNumPhaseOneInstancesAccepted();
          resetMainLoopLogCounter();
          this.phase_one_num_instances_to_accept = phase_one_processor.getPhaseOneNumInstancesToAccept();

          // If did not receive any logs to send to phase two from the completed ballot acceptors
          if(phase_one_num_instances_to_accept == 0) {
            System.out.println("[NEW LEADER] No requests to send to PAXOS PHASE TWO, sending update Completed Ballot message...");
            server_state.updateCompletedBallot(ballot);
          }
          else {
            System.out.println("[NEW LEADER] Will now send PAXOS PHASE ONE log updates to acceptors.");
            while(this.next_log_entry < phase_one_num_instances_to_accept) {
              createPhaseOneInstanceRequest(ballot, acceptors, quorum, phase_one_processor.getUpdatedInstance(this.next_log_entry));
              incrementNextLogEntry();
            }
          }
        }

        // must wait for console activation to know if has permission to continue or not
        checkPermissionForPhaseTwo();
        
        // Check if console gave permission to continue to phase two
        if (this.server_state.getHasPermissionForPhaseTwo() == false) 
          break;
      }

      //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

      //PHASE 2

      //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

      // Update entry number and create new entry for the Paxos Instance for every request
      int entry_number = this.next_log_entry;
      PaxosInstance next_entry = this.server_state.getPaxosLog().testAndSetEntry(entry_number);

      RequestHistory request_history = this.server_state.getRequestHistory();
      RequestRecord request_record;

      request_record = request_history.getFirstPending();


      if(request_record == null)
        break; // No pending requests

      //Only increment if a request is pending
      incrementNextLogEntry();

      // verifies if stopped being the leader in the meantime
      if(!(ballot > -1 && this.server_state.scheduler.leader(ballot) == this.server_state.getServerId()))
        break;

      this.server_state.checkNonCrashDebugMode();

      if(phase_two_command_id == 0)
        phase_two_command_id = request_record.getId();
      
      sendPhaseTwoRequest(entry_number, phase_two_command_id, ballot, acceptors, quorum);
    }
    // every time after processing entries, or getting out of the while because of a break, wait for more work
    waitForWork();
  }

  public synchronized void createPhaseOneInstanceRequest(int ballot, List<Integer> acceptors, int quorum, 
                                                          DidaMeetingsPaxos.PhaseOneInstance updated_instance) {
    int entry_number = this.next_log_entry;

    // Initiate instance, if it does not exist in the server
    this.server_state.getPaxosLog().testAndSetEntry(entry_number);

    sendPhaseTwoRequest(entry_number, updated_instance.getCommandid(), ballot, acceptors, quorum);
  }

  public synchronized void sendPhaseTwoRequest(int entry_number, int phase_two_command_id,
                                                int ballot, List<Integer> acceptors, int quorum) {                            
                                                  
    this.server_state.getRequestHistory().moveToExecuting(phase_two_command_id);

    System.out.println("[LEADER] Starting Phase 2 for entry " + entry_number +
                      " with command_id " + phase_two_command_id);

    new Thread(new PhaseTwoWorker(
            entry_number, ballot, phase_two_command_id, acceptors, quorum)).start();
  }

  // Worker Thread that runs a Paxos Instance Phase 2
  private class PhaseTwoWorker implements Runnable {
    private final int entryNumber;
    private final int ballot;
    private final int command_id;
    private final List<Integer> acceptors;
    private final int quorum;

    PhaseTwoWorker(int entryNumber, int ballot, int command_id, List<Integer> acceptors, int quorum) {
      this.entryNumber = entryNumber;
      this.ballot = ballot;
      this.command_id = command_id;
      this.acceptors = acceptors;
      this.quorum = quorum;
    }

    public void run() {
      try {
        System.out.println("[LEADER] Worker Thread running phase2 for ENTRY: " + entryNumber + " and COMMAND ID: " + command_id);
        DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request =
            DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
        phase_two_request.setInstance(entryNumber);
        phase_two_request.setRequestballot(ballot);
        phase_two_request.setCommandid(command_id);

        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum, server_state);
        ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses =
            new ArrayList<>();
        GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector =
            new GenericResponseCollector<>(phase_two_responses, acceptors.size(), phase_two_processor);

        for (int i = 0; i < acceptors.size(); i++) {
          CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer =
              new CollectorStreamObserver<>(phase_two_collector);
          server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
        }

        // Wait for responses (Quorum)
         phase_two_collector.waitUntilDone();
        if (!phase_two_processor.getAccepted()) {
          server_state.setCurrentBallot(phase_two_processor.getMaxballot());
          System.out.println("[LEADER] Phase2 aborted for entry " + entryNumber);
          return;
        } 
        System.out.println("[LEADER] Phase2 decided for entry " + entryNumber + " command_id " + command_id);
      } catch (Exception e) {
          Thread.currentThread().interrupt();
          return;
      }
    }
  }

  public void waitForDecision(PaxosInstance instance) {

    Object applyLock = server_state.getApplyLock();
  
    synchronized (applyLock) {
      while (instance.isDecided() == false) {
        System.out.println("[APPLY COMMAND] Waiting for entry " 
            + getNextToApply() + " to be decided .......");
        try {
          applyLock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        instance = server_state.getPaxosLog().testAndSetEntry(getNextToApply());
      }
    }
  }

  // Worker thread that applies requests to the replicas
  private class ApplyRunnable implements Runnable {

    public void run() {
      while (true) {
        PaxosInstance next_entry = server_state.getPaxosLog().testAndSetEntry(getNextToApply());

        System.out.println("[APPLY COMMAND] nextToApply: " + getNextToApply() + "; next_entry_decided: " + next_entry.getInstance() + "; is Decided? -> " + next_entry.isDecided());
        
        // To maintain complete order, the lowest entry still not written in the replica should be the first to be processed
        // Wait until the entry is decided
        if(next_entry.isDecided() == false) {
          waitForDecision(next_entry);
        }

        // If command id is 0, it is a NO OP, just go to the next entry
        if(next_entry.getCommandId() == 0) {
          System.out.print("[APPLY COMMAND] NO OP detected, skipping...");
          incrementNextToApply();
          continue;
        }
        RequestRecord request_record;
        // If receive the paxos decision before the request
        synchronized (server_state.getRequestHistory()) {

          request_record = server_state.getRequestHistory().moveToExecuting(next_entry.getCommandId());
          while (request_record == null) {
            try { 
              server_state.getRequestHistory().wait(); 
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            request_record = server_state.getRequestHistory().moveToExecuting(next_entry.getCommandId());
          }
        }

        // Execute request in entry
        DidaMeetingsCommand command = request_record.getRequest();
        boolean result = false;
        DidaMeetingsAction action = command.getAction();

        System.out.println("[APPLY COMMAND] Action  = " + action);
        switch (action) {
          case DidaMeetingsAction.OPEN:
            result = server_state.meeting_manager.open(command.getMeetingId(), server_state.max_participants);
            break;
          case DidaMeetingsAction.ADD:
            result = server_state.meeting_manager.addAndClose(command.getMeetingId(), command.getParticipantId());
            break;
          case DidaMeetingsAction.CLOSE:
            result = server_state.meeting_manager.close(command.getMeetingId());
            break;
          case DidaMeetingsAction.DUMP:
            server_state.meeting_manager.dump();
            result = true;
            break;
          default:
            result = false;
            System.err.println("*** Unknown command ****");
            break;
        }

        // Sending response to client and move request to processed
        System.out.println(
            "[APPLY COMMAND] Setting response for COMMAND with ID = "
                + next_entry.getCommandId()
                + " with RESULT = "
                + result);

        incrementNextToApply();
        request_record.setResponse(result);
        server_state.getRequestHistory().moveToProcessed(request_record.getId());
        // Notify topic operations that a request was applied so they can check if they can proceed
        notifyApplyProgress();
      }
    }
  }
}
