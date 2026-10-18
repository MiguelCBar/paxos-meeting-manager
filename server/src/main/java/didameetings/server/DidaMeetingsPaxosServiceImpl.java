package didameetings.server;

import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxosServiceGrpc;
import didameetings.util.CollectorStreamObserver;
import didameetings.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.*;

public class DidaMeetingsPaxosServiceImpl
    extends DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceImplBase {
  DidaMeetingsServerState server_state;

  public DidaMeetingsPaxosServiceImpl(DidaMeetingsServerState state) {
    this.server_state = state;
  }

  @Override
  public void phaseone(
      DidaMeetingsPaxos.PhaseOneRequest request,
      StreamObserver<DidaMeetingsPaxos.PhaseOneReply> responseObserver) {

    this.server_state.checkNonCrashDebugMode();

    System.out.println("[ACCEPTOR] Receive PHASE ONE request: \n" + request);

    int ballot = request.getRequestballot();
    boolean accepted = false;
    
    DidaMeetingsPaxos.PhaseOneReply.Builder response_builder =
    DidaMeetingsPaxos.PhaseOneReply.newBuilder();
    
    int log_size = this.server_state.paxos_log.length();
    System.out.println("[ACCEPTOR] Current log size = " + log_size);
    
    if (ballot >= this.server_state.getCurrentBallot()) {
      accepted = true;
      this.server_state.setCurrentBallot(ballot);
      for (int entry_number = 0; entry_number < log_size; entry_number++) {
        PaxosInstance paxos_instance = this.server_state.getPaxosLog().testAndSetEntry(entry_number);
        paxos_instance.setReadBallot(ballot);
        
        DidaMeetingsPaxos.PhaseOneInstance.Builder builderForInstance =
        DidaMeetingsPaxos.PhaseOneInstance.newBuilder();
        builderForInstance.setInstance(entry_number);
        builderForInstance.setServerid(this.server_state.getServerId());
        builderForInstance.setRequestballot(ballot);
        builderForInstance.setCommandid(paxos_instance.getCommandId());
        builderForInstance.setValballot(paxos_instance.getWriteBallot());
        response_builder.addLogs(builderForInstance);
      }        
    }

    response_builder.setMaxballot(this.server_state.getCurrentBallot());
    response_builder.setAccepted(accepted);

    System.out.println("[ACCEPTOR] Sending PHASE ONE paxos logs response.");

    responseObserver.onNext(response_builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void phasetwo(
      DidaMeetingsPaxos.PhaseTwoRequest request,
      StreamObserver<DidaMeetingsPaxos.PhaseTwoReply> responseObserver) {

    this.server_state.checkNonCrashDebugMode();

    System.out.println ("[ACCEPTOR] Receive PHASE TWO request: \n" + request);

    int instance = request.getInstance();
    int ballot = request.getRequestballot();
    int command_id = request.getCommandid();
    PaxosInstance entry = this.server_state.getPaxosLog().testAndSetEntry(instance);
    boolean accepted = false;
    int maxballot = ballot;

    if (ballot >= this.server_state.getCurrentBallot()) {
      accepted = true;
      entry.setCommandId(command_id);
      entry.setWriteBallot(ballot);
      this.server_state.setCurrentBallot(ballot);
    } else maxballot = this.server_state.getCurrentBallot();

    DidaMeetingsPaxos.PhaseTwoReply.Builder response_builder =
        DidaMeetingsPaxos.PhaseTwoReply.newBuilder();
    response_builder.setAccepted(accepted);
    response_builder.setInstance(instance);
    response_builder.setServerid(this.server_state.getServerId());
    response_builder.setRequestballot(ballot);
    response_builder.setMaxballot(maxballot);

    DidaMeetingsPaxos.PhaseTwoReply response = response_builder.build();

    System.out.println("[ACCEPTOR] Sending PHASE TWO response: " + response);

    // Go to onNext on observer
    responseObserver.onNext(response);
    responseObserver.onCompleted();

    // Notify learners
    if (accepted == true) {

      Context ctx = Context.current().fork();
      ctx.run(
          () -> {
            List<Integer> learners = this.server_state.scheduler.learners(ballot);
            int n_targets = learners.size();

            DidaMeetingsPaxos.LearnRequest.Builder learn_request_builder =
                DidaMeetingsPaxos.LearnRequest.newBuilder();
            learn_request_builder.setInstance(instance);
            learn_request_builder.setCommandid(command_id);
            learn_request_builder.setBallot(ballot);

            DidaMeetingsPaxos.LearnRequest learn_request = learn_request_builder.build();

            System.out.println("[ACCEPTOR] Sending learn request: \n" + learn_request);

            ArrayList<DidaMeetingsPaxos.LearnReply> learn_responses =
                new ArrayList<DidaMeetingsPaxos.LearnReply>();
            GenericResponseCollector<DidaMeetingsPaxos.LearnReply> learn_collector =
                new GenericResponseCollector<DidaMeetingsPaxos.LearnReply>(
                    learn_responses, n_targets);
            ;
            for (int i = 0; i < n_targets; i++) {
              CollectorStreamObserver<DidaMeetingsPaxos.LearnReply> learn_observer =
                  new CollectorStreamObserver<DidaMeetingsPaxos.LearnReply>(learn_collector);
              this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
            }
            System.out.println("[ACCEPTOR] Learn request completed for instance = " + instance);
          });
    }
  }

  @Override
  public void learn(
      DidaMeetingsPaxos.LearnRequest request,
      StreamObserver<DidaMeetingsPaxos.LearnReply> responseObserver) {

    this.server_state.checkNonCrashDebugMode();

    System.out.println("[LEARNER] Receive LEARN request: \n" + request);

    int instance = request.getInstance();
    int ballot = request.getBallot();
    int command_id = request.getCommandid();
    

    //Move the request record associated with the incoming command ID to executing
    this.server_state.getRequestHistory().moveToExecuting(command_id);

    synchronized (this) {
      PaxosInstance entry = this.server_state.getPaxosLog().testAndSetEntry(instance);

      this.server_state.setCurrentBallot(ballot);

      if (ballot == entry.getAcceptBallot()) {
        entry.incrementNAccepts();
        if (entry.getNAccepts() >= this.server_state.scheduler.quorum(ballot)) {
          
          System.out.println("[LEARNER] Paxos learner for instance " + instance + " : number of accepts " + entry.getNAccepts());
            
          // Skip if entry already decided
          if(!entry.isDecided()) {
            entry.setAsDecided();
            
            // wakeup thread in the main loop to apply command
            Object applyLock = this.server_state.getApplyLock();
            synchronized (applyLock) { applyLock.notifyAll(); }
          }
        }
      } else if (ballot > entry.accept_ballot) {
        System.out.println("[LEARNER] Paxos learner for instance " + instance + " : resetting ");
        entry.setCommandId(command_id);
        entry.setAcceptBallot(ballot);
        entry.setNAccepts(1);
      }
    }
    DidaMeetingsPaxos.LearnReply.Builder response_builder =
        DidaMeetingsPaxos.LearnReply.newBuilder();
    response_builder.setInstance(instance);
    response_builder.setBallot(ballot);

    DidaMeetingsPaxos.LearnReply response = response_builder.build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
