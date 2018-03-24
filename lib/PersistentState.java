package lib;

public class PersistentState {
  public Log log;
  public int currentTerm;
  public int votedFor;
  public boolean isVoting;

  public boolean isVoting() {
    return isVoting;
  }

  public void setVoting(boolean voting) {
    isVoting = voting;
  }

  public PersistentState() {
    this.log = new Log();
    this.currentTerm = 0;
    this.votedFor = -1; // assumption: id >= 0
    this.isVoting = false;
  }
}
