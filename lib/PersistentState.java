package lib;

public class PersistentState {
  public Log log;
  public int currentTerm;
  public int votedFor;

  public PersistentState() {
    this.log = new Log();
    this.currentTerm = 0;
    this.votedFor = -1; // assumption: id >= 0
  }
}
