package lib;

import java.io.Serializable;

public class AppendEntriesReply implements Serializable {
  int term;
  boolean success;

  public AppendEntriesReply(int term, boolean success) {
    this.term = term;
    this.success = success;
  }
}
