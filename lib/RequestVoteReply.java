package lib;

import java.io.Serializable;

/**
 * This class is a wrapper for packing all the result information that you might use in your own
 * implementation of the RequestVote call, and also should be serializable to return by remote
 * function call.
 */
public class RequestVoteReply implements Serializable {
  private static final long serialVersionUID = 1L;
  public int term;
  public boolean voteGranted;

  public RequestVoteReply(int term, boolean voteGranted) {
    this.term = term;
    this.voteGranted = voteGranted;
  }
}
