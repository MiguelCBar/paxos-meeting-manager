package didameetings.server;

import didameetings.DidaMeetingsPaxos;
import didameetings.util.GenericResponseProcessor;

import java.util.ArrayList;

public class PhaseTwoResponseProcessor
    extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseTwoReply> {
  private boolean accepted;
  private int maxballot;
  private int quorum;
  private int responses;

  private DidaMeetingsServerState server_state;

  public PhaseTwoResponseProcessor(int quorum, DidaMeetingsServerState server_state) {
    this.accepted = true;
    this.maxballot = 0;
    this.quorum = quorum;
    this.responses = 0;
    this.server_state = server_state;
  }

  public boolean getAccepted() {
    return this.accepted;
  }

  public int getMaxballot() {
    return this.maxballot;
  }

  public synchronized boolean onNext(
    ArrayList<DidaMeetingsPaxos.PhaseTwoReply> all_responses,
    DidaMeetingsPaxos.PhaseTwoReply last_response) {
      
    this.responses++;

    //Will abort, an acceptor has seen a higher ballot
    if (last_response.getAccepted() == false) {
      this.accepted = false;
      if (last_response.getMaxballot() > this.maxballot)
        this.maxballot = last_response.getMaxballot();
      // aborts completed
      if(this.server_state.getHasPermissionForPhaseTwo() == false)
        this.server_state.abortCompletedBallot();
      return true;
    } else if (responses >= quorum) {

      System.out.println("[NEW LEADER] received quorum for instance " 
                          + last_response.getInstance() 
                          + "; has permission for phase two?? ----> " 
                          + this.server_state.getHasPermissionForPhaseTwo());

      // Check if proposer is still executing the instances from the logs received in phase one
      if(this.server_state.getHasPermissionForPhaseTwo() == false) {
        this.server_state.incrementNumPhaseOneInstancesAccepted();

        System.out.println("[NEW LEADER] incremented phase one instances accepted to " 
                          + this.server_state.getNumPhaseOneInstancesAccepted()
                          + "instances needed to update server completed ballot: " 
                          + this.server_state.getMainLoop().getPhaseOneNumInstancesToAccept());

        // Check if we updated the ballot for every phase one received instance with the new ballot
        if(this.server_state.getNumPhaseOneInstancesAccepted() >= 
              this.server_state.getMainLoop().getPhaseOneNumInstancesToAccept()) {

          System.out.println("Will now update completed ballot with mine...");
          this.server_state.updateCompletedBallot(this.server_state.getCurrentBallot());
        }
      }
      return true;
    } else 
      return false;
  }
}
