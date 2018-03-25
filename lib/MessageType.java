package lib;

import java.io.Serializable;

/**
 * This enum is for distinguishing different message type you might send, for example you may need a
 * unique type for each remote function call.
 */
public enum MessageType implements Serializable {
  RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply;

  private static final long serialVersionUID = 1L;
}
