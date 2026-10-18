package didameetings.server;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;
import didameetings.util.GenericResponseProcessor;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class PhaseOneResponseProcessor
    extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseOneReply> {
  private ConfigurationScheduler scheduler;
  private boolean accepted;
  private int responses;
  private int quorum;
  private int maxballot;
  private int num_instances_to_accept;

  private Hashtable<Integer, DidaMeetingsPaxos.PhaseOneInstance> updated_phase_one_logs;


  public PhaseOneResponseProcessor(ConfigurationScheduler scheduler,int quorum) {
    this.accepted = true;
    this.responses = 0;
    this.quorum = quorum;
    this.scheduler = scheduler;
    this.maxballot = -1;
    this.num_instances_to_accept = -1;
    this.updated_phase_one_logs = new Hashtable<>();
  }

  public boolean getAccepted() {
    return this.accepted;
  }

  public int getMaxBallot() {
    return this.maxballot;
  }

  public int getPhaseOneNumInstancesToAccept() {
    return this.num_instances_to_accept + 1;
  }

  public Hashtable<Integer, DidaMeetingsPaxos.PhaseOneInstance> getUpdatedLogs() {
    return this.updated_phase_one_logs;
  }

  public DidaMeetingsPaxos.PhaseOneInstance getUpdatedInstance(int entry_number) {
    return this.updated_phase_one_logs.get(entry_number);
  }

  public synchronized boolean onNext(
      ArrayList<DidaMeetingsPaxos.PhaseOneReply> all_responses,
      DidaMeetingsPaxos.PhaseOneReply last_response) {


    if (last_response.getAccepted() == false) {
      System.out.println("[ACCEPTOR] Failed to execute Paxos PHASE 1 -> Aborting...");
      this.accepted = false;
      if (last_response.getMaxballot() > this.maxballot)
        this.maxballot = last_response.getMaxballot();
      return true; // aborted
    }
    this.responses++;

    List<DidaMeetingsPaxos.PhaseOneInstance> acceptor_logs = last_response.getLogsList();

    for (DidaMeetingsPaxos.PhaseOneInstance instance : acceptor_logs) {
      int entry_number = instance.getInstance();
      DidaMeetingsPaxos.PhaseOneInstance existing_entry = updated_phase_one_logs.get(entry_number);

      // Only update if there's no entry yet, or the new ballot is higher
      if (existing_entry == null || existing_entry.getValballot() < instance.getValballot()) {
        updated_phase_one_logs.put(entry_number, instance);
      }
    }

    if (this.responses >= this.quorum) {
      this.num_instances_to_accept = acceptor_logs.size() - 2;
      System.out.println("[ACCEPTOR] Quorum achieved for Paxos PHASE 1 -> Accepting new ballot");
      this.accepted = true;
      return true;  // accepted
    }
    return false;
  }
}
