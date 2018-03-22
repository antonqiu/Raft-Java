package lib;

public class PersistentState {
  private Log log;
  private int currentTerm;
  private int votedFor;

  public PersistentState() {
    this.log = new Log();
    this.currentTerm = 0;
    this.votedFor = -1; // assumption: id >= 0
  }
}
