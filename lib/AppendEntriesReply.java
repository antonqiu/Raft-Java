package lib;

import java.io.Serializable;

public class AppendEntriesReply implements Serializable {
  private static final long serialVersionUID = 1L;
  public int term;
  public boolean success;

  public AppendEntriesReply(int term, boolean success) {
    this.term = term;
    this.success = success;
  }
}
