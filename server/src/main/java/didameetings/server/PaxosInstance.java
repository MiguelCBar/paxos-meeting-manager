package didameetings.server;

public class PaxosInstance {
  int instance_nb;
  int command_id;
  int read_ballot;
  int write_ballot;
  int accept_ballot;
  int n_accepts;
  boolean decided;
  boolean value_is_locked;

  public PaxosInstance() {
    this.instance_nb = 0;
    this.command_id = 0;
    this.read_ballot = -1;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.n_accepts = 0;
    this.decided = false;
    this.value_is_locked = false;
  }

  public PaxosInstance(int id) {
    this.instance_nb = id;
    this.command_id = 0;
    this.read_ballot = -1;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.n_accepts = 0;
    this.decided = false;
    this.value_is_locked = false;
  }

  public PaxosInstance(int id, int ballot) {
    this.instance_nb = id;
    this.command_id = 0;
    this.read_ballot = ballot;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.n_accepts = 0;
    this.decided = false;
    this.value_is_locked = false;
  }

  public PaxosInstance(int id, int ballot, int command_id) {
    this.instance_nb = id;
    this.command_id = command_id;
    this.read_ballot = ballot;
    this.write_ballot = -1;
    this.accept_ballot = -1;
    this.n_accepts = 0;
    this.decided = false;
    this.value_is_locked = false;
  }


  public int getInstance() {
    return instance_nb;
  }

  public int getCommandId() {
    return command_id;
  }

  public void setCommandId(int command_id) {
    this.command_id = command_id;
  }

  public void setReadBallot(int ballot) {
    this.read_ballot = ballot;
  }

  public int getWriteBallot() {
    return write_ballot;
  }

  public void setWriteBallot(int ballot) {
    this.write_ballot = ballot;
  }

  public int getAcceptBallot() {
    return accept_ballot;
  }

  public void setAcceptBallot(int accept_ballot) {
    this.accept_ballot = accept_ballot;
  }

  public int getNAccepts() {
    return n_accepts;
  }
  
  public void setNAccepts(int n_accepts) {
    this.n_accepts = n_accepts;
  }

  public void incrementNAccepts() {
    this.n_accepts++;
  }
  
  public boolean isDecided() {
    return decided;
  }

  public void setAsDecided() {
    this.decided = true;
  }
}
