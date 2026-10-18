package didameetings.server;

import didameetings.DidaMeetingsMaster;
import didameetings.DidaMeetingsMasterServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DidaMeetingsMasterServiceImpl
    extends DidaMeetingsMasterServiceGrpc.DidaMeetingsMasterServiceImplBase {
  DidaMeetingsServerState server_state;

  public DidaMeetingsMasterServiceImpl(DidaMeetingsServerState state) {
    this.server_state = state;
  }

  @Override
  public void newballot(
      DidaMeetingsMaster.NewBallotRequest request,
      StreamObserver<DidaMeetingsMaster.NewBallotReply> responseObserver) {

    System.out.println("[REPLICA " + this.server_state.my_id + "] New ballot request: " + request);

    int request_id = request.getReqid();
    int new_ballot = request.getNewballot();
    int completed_ballot = request.getCompletedballot();

    // for debug purposes
    System.out.println(
        "[REPLICA " + this.server_state.my_id + "] Current ballot = "
            + this.server_state.getCurrentBallot()
            + " new ballot = "
            + new_ballot
            + " completed ballot = "
            + completed_ballot);

    this.server_state.setCompletedBallot(completed_ballot);

    if (new_ballot > this.server_state.getCurrentBallot()) {
      this.server_state.setCurrentBallot(new_ballot);
      this.server_state.setHasPermissionForPhaseTwo(false);  // new leader = no permission for phase2 until completed and activated
      this.server_state.setActivated(false); // new leader = doesn´t have activation(permission T or F) from console yet
      this.server_state.setAborted(false); // new leader = did not abort yet
      this.server_state.main_loop.wakeup();

      completed_ballot = this.server_state.waitForCompletedBallot(new_ballot); 
    } else {
      completed_ballot = this.server_state.getCompletedBallot();
    }

    DidaMeetingsMaster.NewBallotReply.Builder response_builder =
        DidaMeetingsMaster.NewBallotReply.newBuilder();
    response_builder.setReqid(request_id);
    response_builder.setCompletedballot(completed_ballot);
    response_builder.setAccepted(!this.server_state.getAborted()); // if aborted, not accepted

    DidaMeetingsMaster.NewBallotReply response = response_builder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void activate(
      DidaMeetingsMaster.ActivateRequest request,
      StreamObserver<DidaMeetingsMaster.ActivatetReply> responseObserver) {

    System.out.println("[REPLICA " + this.server_state.getServerId() + "] Activate request: " + request);

    this.server_state.setHasPermissionForPhaseTwo(request.getHasPermissionForPhaseTwo());
    this.server_state.setActivated(true);
    synchronized (this.server_state.getConsoleActivationLock()) {
      System.out.println("[REPLICA " + this.server_state.getServerId() + "] Activation triggered.");
      this.server_state.getConsoleActivationLock().notify();
    }

    // Build and send an empty reply (console doesn't need anything else)
    DidaMeetingsMaster.ActivatetReply reply = DidaMeetingsMaster.ActivatetReply.newBuilder().build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }


  @Override
  public void setdebug(
      DidaMeetingsMaster.SetDebugRequest request,
      StreamObserver<DidaMeetingsMaster.SetDebugReply> responseObserver) {

    System.out.println("[REPLICA " + this.server_state.my_id + "] Debug mode" + request);

    boolean response_value = true;

    int request_id = request.getReqid();
    int request_mode = request.getMode();
    this.server_state.setDebugMode(request_mode);

    switch (request_mode) {
      case 1:
        this.server_state.getMainLoop().wakeup();    
        break;
      case 2:
        this.server_state.setFreezeMode(true);
        break;
      case 3:
        if (this.server_state.getFreezeMode()) {
            this.server_state.setFreezeMode(false);
            synchronized (this.server_state) {
                this.server_state.notifyAll();
            }
        }
        break;
      case 4:
        this.server_state.setSlowMode(true);
        break;
      case 5:
        if(this.server_state.getSlowMode() == true) this.server_state.setSlowMode(false);
        break;
    }

    String modeDescription;
    switch (this.server_state.getDebugMode()) {
      case 1:
        modeDescription = "crash";
        break;
      case 2:
        modeDescription = "freeze";
        break;
      case 3:
        modeDescription = "un-freeze";
        break;
      case 4:
        modeDescription = "slow-mode-on";
        break;
      case 5:
        modeDescription = "slow-mode-off";
        break;
      default:
        modeDescription = "unknown mode (" + this.server_state.getDebugMode() + ")";
        break;
    }
    System.out.println("[REPLICA " + this.server_state.my_id + "] Setting debug mode to = " + modeDescription);

    DidaMeetingsMaster.SetDebugReply.Builder response_builder =
        DidaMeetingsMaster.SetDebugReply.newBuilder();
    response_builder.setReqid(request_id);
    response_builder.setAck(response_value);

    DidaMeetingsMaster.SetDebugReply response = response_builder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
