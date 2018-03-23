package lib;

import java.io.Serializable;

public class AppendEntriesArgs implements Serializable {
  public int term;
  public int leaderId;
  public int prevLogIndex;
  public int prevLogTerm;
  public LogEntry[] entries;
  public int leaderCommit;

  public AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm,
      LogEntry[] entries, int leaderCommit) {
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
  }
}
