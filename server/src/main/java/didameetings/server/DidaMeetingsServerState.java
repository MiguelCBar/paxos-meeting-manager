package didameetings.server;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import didameetings.DidaMeetingsPaxosServiceGrpc;
import didameetings.configs.ConfigurationScheduler;
import didameetings.core.MeetingManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DidaMeetingsServerState {
  int max_participants;
  MeetingManager meeting_manager;
  ConfigurationScheduler scheduler;
  int base_port;
  int my_id;
  RequestHistory req_history;
  PaxosLog paxos_log;

  List<Integer> all_participants;
  int n_participants;
  String[] targets;
  ManagedChannel[] channels;
  DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;

  private final Object applyLock = new Object(); // Lock to notify the applyThread in the MainLoop when a new entry is decided
  private final Object consoleActivationLock = new Object(); // Lock to notify the main loop when the server is activated
  private int current_ballot;
  private int completed_ballot;
  private int debug_mode;
  private boolean freeze_mode;
  private boolean slow_mode;


  private boolean has_permission_for_phase_two;  // As leader, can execute new requests in phase 2
  private boolean activated; // Received the activation request from the console
  private boolean aborted;  // Leader aborted

  private int n_phase_one_instances_accepted;

  MainLoop main_loop;
  Thread main_loop_worker;

  public DidaMeetingsServerState(int port, int myself, char schedule, int max) {
    this.max_participants = max;
    this.meeting_manager = new MeetingManager();
    this.scheduler = new ConfigurationScheduler(schedule);
    this.base_port = port;
    this.my_id = myself;

    this.debug_mode = 0;
    this.freeze_mode = false;
    this.slow_mode = false;

    this.has_permission_for_phase_two = false;
    this.activated = false;
    this.aborted = false;

    this.current_ballot = 0;
    this.completed_ballot = -1;

    this.req_history = new RequestHistory();
    this.paxos_log = new PaxosLog();
    this.main_loop = new MainLoop(this);
    this.n_phase_one_instances_accepted = 0;

    // init comms
    this.all_participants = this.scheduler.allparticipants();
    this.n_participants = all_participants.size();

    this.targets = new String[this.n_participants];
    for (int i = 0; i < this.n_participants; i++) {
      int target_port = this.base_port + all_participants.get(i);
      this.targets[i] = new String();
      this.targets[i] = "localhost:" + target_port;
      System.out.printf("targets[%d] = %s%n", i, targets[i]);
    }

    this.channels = new ManagedChannel[this.n_participants];
    for (int i = 0; i < this.n_participants; i++)
      this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();

    this.async_stubs =
        new DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[this.n_participants];
    for (int i = 0; i < this.n_participants; i++)
      this.async_stubs[i] = DidaMeetingsPaxosServiceGrpc.newStub(this.channels[i]);

    // start worker
    this.main_loop_worker = new Thread(main_loop);
    this.main_loop_worker.start();
  }

  //------------------------------------------------------------------------

  public synchronized int getCurrentBallot() {
    return this.current_ballot;
  }

  public synchronized void setCurrentBallot(int ballot) {
    if (ballot > this.current_ballot) {
      this.current_ballot = ballot;
    }
  }

  public synchronized int getCompletedBallot() {
    return this.completed_ballot;
  }

  public synchronized void updateCompletedBallot(int ballot) {
    if (ballot > this.completed_ballot) 
      this.completed_ballot = ballot;
    this.notifyAll();
  }

  //Sets aborted to true and notifies thread waiting in waitForCompletedBallot, 
  // so that it sends response to console
  public synchronized void abortCompletedBallot() {
    this.aborted = true;
    this.notifyAll();
  }

  public synchronized void setCompletedBallot(int ballot) {
    if (ballot > this.completed_ballot) this.completed_ballot = ballot;
    this.notifyAll();
  }

  public synchronized int waitForCompletedBallot(int ballot) {
    // wake up if it was aborted or if completed was updated 
    while (this.aborted == false && this.completed_ballot < ballot) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return this.completed_ballot;
  }

  //------------------------------------------------------------------------

  public synchronized boolean getHasPermissionForPhaseTwo() {
    return this.has_permission_for_phase_two;
  }

  public synchronized void setHasPermissionForPhaseTwo(boolean value) {
    this.has_permission_for_phase_two = value;
  }

  public synchronized boolean getActivated() {
    return this.activated;
  }
  
  public synchronized void setActivated(boolean value) {
    this.activated = value;
  }

  public synchronized boolean getAborted() {
    return this.aborted;
  }

  public synchronized void setAborted(boolean value) {
    this.aborted = value;
  }

  //------------------------------------------------------------------------

  public synchronized MainLoop getMainLoop() {
    return this.main_loop;
  }

  public synchronized int getServerId() {
    return this.my_id;
  }

  public synchronized PaxosLog getPaxosLog() {
    return this.paxos_log;
  }

  public synchronized RequestHistory getRequestHistory() {
    return this.req_history;
  }

  public synchronized int getNumPhaseOneInstancesAccepted() {
    return this.n_phase_one_instances_accepted;
  }

  public synchronized void resetNumPhaseOneInstancesAccepted() {
    this.n_phase_one_instances_accepted = 0;
  }

  public synchronized void incrementNumPhaseOneInstancesAccepted() {
    this.n_phase_one_instances_accepted++;
  }

  //-------------------------------------------------------------------------


  public synchronized int getDebugMode() {
    return this.debug_mode;
  }

  public synchronized void setDebugMode(int mode) {
    this.debug_mode = mode;
  }

  //------------------------------------------------------------------------

  // Check if server should crash
  public synchronized void checkCrashDebug(int debug_mode) {
    if(debug_mode == 1) System.exit(0);
  }

  // Check server current debug mode and execute necessary actions
  public synchronized void checkNonCrashDebugMode() {
    if(this.freeze_mode == true) this.enterFreezeMode();

    if(this.slow_mode == true) this.enterSlowMode();
  }

  //------------------------------------------------------------------------

  public synchronized boolean getFreezeMode() {
    return this.freeze_mode;
  }

  public synchronized void setFreezeMode(boolean mode) {
    this.freeze_mode = mode;
  } 

  // Current server will remain in freeze mode until unfreeze command is called
  public synchronized void enterFreezeMode() {
    while(this.freeze_mode == true) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  //------------------------------------------------------------------------

  public synchronized boolean getSlowMode() {
    return this.slow_mode;
  }

  public synchronized void setSlowMode(boolean mode) {
    this.slow_mode = mode;
  }

  // Current server will remain in slow mode until unslow command is called
  public synchronized void enterSlowMode() {
    try {
      int miliseconds = ThreadLocalRandom.current().nextInt(10001);
      Thread.sleep(miliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  //------------------------------------------------------------------------
    
  public synchronized Object getApplyLock() {
    return this.applyLock;
  }

  public synchronized Object getConsoleActivationLock() {
    return this.consoleActivationLock;
  }
}
