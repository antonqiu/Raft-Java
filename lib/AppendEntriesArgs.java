package lib;

import java.io.Serializable;
import java.util.List;

public class AppendEntriesArgs implements Serializable {
  private static final long serialVersionUID = 1L;
  public int term;
  public int leaderId;
  public int prevLogIndex;
  public int prevLogTerm;
  public List<LogEntry> entries;
  public int leaderCommit;

  public AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm,
      List<LogEntry> entries, int leaderCommit) {
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
  }
}
