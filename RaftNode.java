import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import lib.AppendEntriesReply;
import lib.AppendEntriesArgs;
import lib.ApplyMsg;
import lib.GetStateReply;
import lib.LogEntry;
import lib.Message;
import lib.MessageHandling;
import lib.MessageType;
import lib.PersistentState;
import lib.RequestVoteArgs;
import lib.RequestVoteReply;
import lib.Role;
import lib.StartReply;
import lib.TransportLib;

public class RaftNode implements MessageHandling {

  private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());

  private static final int ELECTION_TIMEOUT_MIN = 250;
  private static final int HEARTBEAT_TIMEOUT_MIN = 350;
  private static final int HEARTBEAT_EVERY_MILLI = 125; // 8 heartbeats per second
  private static final int RANDOM_TIMEOUT_PLUS_RANGE = 250;
  private static final int QUEUE_SIZE = 500;

  private int id;
  private static TransportLib lib;
  private int num_peers;

  /* STATES */
  private Role role;
  private PersistentState state;

  /* VOLATILE PARTICIPANTS STATES */
  private boolean heardHeartbeat;
  private int commitIndex;
  private int lastApplied;

  /* VOLATILE LEADER STATES */
  private int[] nextIndex;
  private int[] matchIndex;

  /* VOLATILE CANDIDATE STATES */
  private int votesRcvd;

  /* MESSAGE QUEUES */
  private volatile ArrayBlockingQueue<Integer> heartbeatQueue;

  public RaftNode(int port, int id, int num_peers) {
    LOGGER.setLevel(Level.ALL);
    ConsoleHandler ch = new ConsoleHandler();
    LOGGER.setUseParentHandlers(false);
    ch.setLevel(Level.OFF);
    LOGGER.addHandler(ch);

    this.id = id;
    this.num_peers = num_peers;
    lib = new TransportLib(port, id, this);

    // init states
    this.role = Role.FOLLOWER;  // initially as a follower
    this.state = new PersistentState(); // a persistent state where log resides
    this.heardHeartbeat = false;
    this.commitIndex = 0;
    this.lastApplied = 0;

    // init leader states
    this.nextIndex = new int[num_peers];
    Arrays.fill(this.nextIndex, 1);
    this.matchIndex = new int[num_peers];
    Arrays.fill(this.matchIndex, 0);

    // init candidate state
    this.votesRcvd = 0;

    this.heartbeatQueue = new ArrayBlockingQueue<>(1);

    this.startRoleChangeOperator();
    LOGGER.fine(String.format("Node %d initialized.", this.id));
  }

  private void startRoleChangeOperator() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      // LOGGER.info(Thread.currentThread().getName());
      while (true) {
        int actualHeartbeatTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(
            HEARTBEAT_TIMEOUT_MIN, RANDOM_TIMEOUT_PLUS_RANGE);
        int actualElectionTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(ELECTION_TIMEOUT_MIN,
            RANDOM_TIMEOUT_PLUS_RANGE);
        if (getRole() == Role.LEADER) {
          synchronized (this) {
              LOGGER.finer(String.format("Node %d: send heartbeat.", this.id));
              this.sendAppendEntry();
          }
          TimeUnit.MILLISECONDS.sleep(HEARTBEAT_EVERY_MILLI);
        } else {
          if (heartbeatQueue.poll(actualElectionTimeout, TimeUnit.MILLISECONDS) == null) {
            synchronized (this) {
              LOGGER.fine(String
                  .format("Node %d decides to elect itself after timeout %dms. nextTerm=%d", id,
                      actualElectionTimeout ,state.currentTerm + 1));
              this.electSelf();
            }
          }
        }
      }
    });
  }

  private void sendAppendEntry() {
    if (this.role != Role.LEADER) {
      return;
    }
    for (int i = 0; i < num_peers; i++) {
      final int peerId = i;
      if (peerId == this.id) {
        continue;  // do not send message to itself
      }
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(() -> {
        try {
          // build AppendEntriesArgs
          ArrayList<LogEntry> entries;
          AppendEntriesArgs args;
          entries = state.log.getLogEntriesStartingAt(nextIndex[peerId]);
          args = new AppendEntriesArgs(
              this.state.currentTerm,
              this.id,
              this.nextIndex[peerId] - 1,
              state.log.getTermForIndex(this.nextIndex[peerId] - 1),
              entries,
              this.commitIndex
          );
          byte[] argsBytes = RaftNode.objectToByteArray(args);
          Message msg = new Message(
              MessageType.AppendEntriesArgs,
              this.id,
              peerId,
              argsBytes
          );
          if (entries.size() == 0) {
            LOGGER.finest(String.format("Node %d %s: HB to %d: term %d (next %d match %d)", id,
                role, peerId, args.term, nextIndex[peerId], matchIndex[peerId]));
          } else {
            LOGGER.finer(String.format("Node %d %s: to node %d: idx %d length %d", id, role, msg
                .getDest(), args.prevLogIndex + 1, args.entries.size()));
          }
          Message reply = lib.sendMessage(msg);
          this.revAppendEntryReply(reply, args);
        } catch (RemoteException e) {
          LOGGER.info("RMI exception when sending AppendEntriesArgs.");
          // e.printStackTrace();
        } catch (Exception e) {
          LOGGER.info("Exception when sending AppendEntriesArgs:\n " + e.getLocalizedMessage());
          e.printStackTrace();
        }
      });
    }
  }

  private synchronized void revAppendEntryReply(Message reply, AppendEntriesArgs args) {
    if (reply == null || role != Role.LEADER) {
      return;
    }
    LOGGER.finest(
        String.format("Node %d %s: rcvd AppendEntryReply from %d.", id, role, reply.getSrc()));
    AppendEntriesReply aer = (AppendEntriesReply) byteArrayToObject(reply.getBody());
    if (aer == null) {
      return;
    }

    // all server #2, leader to follower
    if (aer.term > state.currentTerm) {
      if (role != Role.FOLLOWER) {
        LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Waiting for HB.\n"
            + "reqTerm=%s, thisTerm=%s", id, role, aer.term, state.currentTerm));
      }
      setRole(Role.FOLLOWER);
      this.state.currentTerm = aer.term;
      this.state.votedFor = -1;
      return;
    }

    if (aer.success) {
      this.nextIndex[reply.getSrc()] = aer.proposedNextIndex;
      this.matchIndex[reply.getSrc()] = this.nextIndex[reply.getSrc()] - 1;

      if (args.entries.size() != 0) {
        LOGGER.finer(
            String.format("Node %d %s: AER FROM %d SUCCESS: term %d (next %d match %d) ",
                id, role, reply.getSrc(), aer.term, nextIndex[reply.getSrc()], matchIndex[reply
                    .getSrc()]));
      } else {
        LOGGER.finest(
            String.format("Node %d %s: HB FROM %d SUCCESS: term %d (next %d match %d) ",
                id, role, reply.getSrc(), aer.term, nextIndex[reply.getSrc()], matchIndex[reply
                    .getSrc()]));
      }

      // if node has entries to be committed, and at least one entry in this term is pushed
      if (this.commitIndex < matchIndex[reply.getSrc()] &&
          state.log.getTermForIndex(matchIndex[reply.getSrc()]) == state.currentTerm) {
        int cnt = 0;
        for (int i = 0; i < num_peers; i++) {
          final int peerId = i;
          // count++ if this node has saved this entry
          if (matchIndex[peerId] >= matchIndex[reply.getSrc()] || peerId == this.id) {
            cnt++;
          }
        }

        if (cnt > num_peers / 2) {
          // ready to commit and apply
          commitIndex = matchIndex[reply.getSrc()];
          if (lastApplied < commitIndex) {
            for (int cIdx = lastApplied + 1; cIdx <= commitIndex; cIdx++) {
              try {
                lib.applyChannel(new ApplyMsg(this.id, cIdx, state.log.getLogEntryByIndex(cIdx)
                    .getCommand(), false, null));
                LOGGER.info(String.format("Node %d %s: APPLIED: idx%d command%d", id, role, cIdx,
                    state.log.getLogEntryByIndex(cIdx).getCommand()));
                lastApplied = cIdx;
              } catch (RemoteException e) {
                LOGGER.info(
                    "RMI exception when applying entry:\n " + state.log.getLogEntryByIndex(cIdx)
                        .toString());
                // e.printStackTrace();
              }
            }
          }
        }
      }
    } else {
      // rejected
      nextIndex[reply.getSrc()] = aer.proposedNextIndex;
      LOGGER.finer(
          String.format("Node %d %s: AER FROM %d REJECTED: %d %d",
              id, role, reply.getSrc(), nextIndex[reply.getSrc()], matchIndex[reply.getSrc()]));
    }
  }

  private synchronized AppendEntriesReply buildAppendEntryReply(AppendEntriesArgs request) {
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      if (role != Role.FOLLOWER) {
        LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Recognized node %d."
            + "term %d > %d", id, role, request.leaderId, request.term, state.currentTerm));
      }
      setRole(Role.FOLLOWER);
      this.state.currentTerm = request.term;
//      this.state.votedFor = request.leaderId;
    }


    // #1 AppendEntriesRPC
    if (request.term < state.currentTerm) {
      // reply false
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false, 1);
      return r;
    }

    // at this point, we're sure that this is the current leader
    if (state.votedFor == -1) {
      LOGGER.fine(String.format("Node %d %s: Recognized node %d.", id, role, request.leaderId));
      this.state.votedFor = request.leaderId;
    }

    if (request.prevLogIndex > state.log.getLatestIndex()) {
      LOGGER.fine(String.format("Node %d %s: AE rejected: req.prevIdx=%d prevIdx=%d", id, role,
          request.prevLogIndex, state.log.getLatestIndex()));
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false, state.log
          .getLatestIndex() + 1);
      return r;
    }

    // #2 RPC
    if (state.log.getTermForIndex(request.prevLogIndex) != request.prevLogTerm) {
      LOGGER.fine(String.format("Node %d %s: AE rejected: req.prevTerm=%d prevTerm=%d", id, role,
          request.prevLogTerm, state.log.getTermForIndex(request.prevLogIndex)));
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false, request.prevLogIndex);
      return r;
    }

    // #3 & #4
    for (int newEntriesIdx = 0; newEntriesIdx < request.entries.size(); newEntriesIdx++) {
      int logIdx = request.prevLogIndex + 1 + newEntriesIdx;
      LogEntry proposed = request.entries.get(newEntriesIdx);
      LogEntry saved = state.log.getLogEntryByIndex(logIdx);
      if (saved == null) {
        state.log.appendAllLogEntries(newEntriesIdx, request.entries);
        LOGGER.finer(String.format("Node %d %s: appendAllLogEntries: %s", id, role, request
            .entries.toString()));
        LOGGER.finer(String.format("Node %d %s: log: %s", id, role, state.log.toString()));
        break;
      } else if (!saved.equals(proposed)) {
        state.log.removeLogEntriesStartingAt(logIdx);
        state.log.appendAllLogEntries(newEntriesIdx, request.entries);
        LOGGER.finer(String.format("Node %d %s: remove&appendAllLogEntries: %s", id, role, request
            .entries.toString()));
        LOGGER.finer(String.format("Node %d %s: log: %s", id, role, state.log.toString()));
        break;
      }
    }

    // #5
    if (request.leaderCommit > commitIndex) {
      commitIndex = Math.min(request.leaderCommit, state.log.getLatestIndex());
      LOGGER.finer(String.format("Node %d %s: updateCommitIndex: %d", id, role, commitIndex));
    }

    // all server #1
    if (lastApplied < commitIndex) {
      for (int i = lastApplied; i < commitIndex; i++) {
        try {
          lib.applyChannel(
              new ApplyMsg(this.id, state.log.getLogEntryByIndex(i + 1).getIndex(), state.log
                  .getLogEntryByIndex(i + 1).getCommand(), false, null));
          LOGGER.info(String.format("Node %d %s: APPLIED: idx%d command: %d", id, role, i + 1,
              state.log.getLogEntryByIndex(i + 1).getCommand()));
          lastApplied = i + 1;
        } catch (RemoteException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, true, state.log
        .getLatestIndex() + 1);
    return r;
  }

  private void electSelf() {
    // update states
    setRole(Role.CANDIDATE);
    this.votesRcvd = 1; // vote for itself
    this.state.currentTerm++;
    this.state.votedFor = this.id;

    for (int i = 0; i < num_peers; i++) {
      final int peerId = i;
      if (peerId == this.id) {
        continue;
      }
      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          RequestVoteArgs args = new RequestVoteArgs(
              this.state.currentTerm,
              this.id,
              state.log.getLatestIndex(),
              this.state.log.getTermForIndex(this.state.log.getSize()));
          Message msg = new Message(
              MessageType.RequestVoteArgs,
              this.id,
              peerId,
              objectToByteArray(args)
          );
          this.revRequestVoteReply(lib.sendMessage(msg));
        } catch (RemoteException e) {
          LOGGER.info("RMI exception when sending RequestVoteArgs.");
          // e.printStackTrace();
        }
      });
    }
  }

  private synchronized void revRequestVoteReply(Message message) {
    if (message == null || getRole() != Role.CANDIDATE) {
      return;
    }
    LOGGER.finest(String.format("Node %d %s: recv RequestVoteReply from %d.", id, role, message
        .getSrc()));
    RequestVoteReply reply = (RequestVoteReply) byteArrayToObject(message.getBody());
    if (reply == null) {
      return;
    }

    if (state.currentTerm < reply.term) {
      LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER upon vote message from %d. term "
          + "%d > %d", id, role, message.getSrc(), reply.term, state.currentTerm));
      state.currentTerm = reply.term;
      setRole(Role.FOLLOWER);
      state.votedFor = -1;
      return;
    }

    if (state.currentTerm > reply.term) {
      LOGGER.fine(String.format("Node %d %s: recv RequestVotesReply from prev term.", id, role));
      return;
    }

    if (reply.voteGranted) {
      this.votesRcvd++;
    }

    if (votesRcvd > num_peers / 2) {
      setRole(Role.LEADER);
      Arrays.fill(this.nextIndex, this.state.log.getLatestIndex() + 1);
      Arrays.fill(this.matchIndex, 0);
      LOGGER.info(String.format("Node %d was elected as LEADER. last vote=%d. term=%d", id,
          message.getSrc(), state.currentTerm));
      heartbeatQueue.offer(1);
    }
  }

  private synchronized RequestVoteReply buildRequestVoteReply(RequestVoteArgs request) {
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      if (role != Role.FOLLOWER) {
        LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER for %d. Waiting for HB.\n"
            + "reqTerm=%s, thisTerm=%s", id, role, request.candidateId,request.term, state.currentTerm));
      }
      setRole(Role.FOLLOWER);
      this.state.currentTerm = request.term;
      this.state.votedFor = -1;
    }

    // #1
    if (request.term < this.state.currentTerm) {
      LOGGER.finest(String.format("Node %d %s: req term %d < cur term %d.", id, role, request.term,
          state.currentTerm));
      RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, false);
      return r;
    }

    // additional rule: break the tie
    if (request.term == this.state.currentTerm && role == Role.CANDIDATE) {
      // break the tie
      boolean voteGranted = request.candidateId < this.id;
      RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, voteGranted);
      if (voteGranted) {
        this.role = Role.FOLLOWER;
        this.state.votedFor = request.candidateId;
        this.votesRcvd--;
        LOGGER.finer(String.format("Node %d voted for %d via tiebreaker.", id, request
            .candidateId));
      }
      return r;
    }

    // #2
    if ((this.state.votedFor == -1 || this.state.votedFor == request.candidateId) &&
        this.state.log.isUpToDate(request.lastLogTerm, request.lastLogIndex)) {
      this.state.votedFor = request.candidateId;
      RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, true);
      LOGGER.finer(String.format("Node %d voted for %d.", id, request.candidateId));
      return r;
    }

    // #3
    RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, false);
    LOGGER.finest(String.format("Node %d %s: not voting for %d: req(%d %d) cur(%d %d).", id, role,
        request.candidateId, request.lastLogTerm, request.lastLogIndex, state.log.getLatestTerm(),
        state.log.getLatestIndex()));
    return r;
  }

  /*
   *call back.
   */

  @Override
  public synchronized StartReply start(int command) {
    if (this.role != Role.LEADER) {
      return new StartReply(0, 0, false);
    }

    this.state.log.appendLogEntry(
        new LogEntry(state.currentTerm,
            state.log.getLatestIndex() + 1,
            command));

    this.sendAppendEntry();
    LOGGER.info(String.format("Node %d %s: Command: %d, scheduled.", id, role, command));

    return new StartReply(state.log.getLatestIndex(), state.currentTerm, true);
  }

  @Override
  public synchronized GetStateReply getState() {
    return new GetStateReply(state.currentTerm, role == Role.LEADER);
  }

  @Override
  public Message deliverMessage(Message message) {
    LOGGER.finest(String.format("Node %d %s: rcvd %s from %d. Processing.", this.id, role, message
        .getType(), message.getSrc()));
    if (message.getType() == MessageType.AppendEntriesArgs) {
      AppendEntriesArgs request = (AppendEntriesArgs) byteArrayToObject(message.getBody());
      AppendEntriesReply reply = buildAppendEntryReply(request);
      if (request.entries.size() != 0) {
        LOGGER.finer(String
            .format("Node %d %s: rcvd %s from %d. Result: %s", this.id, role, message.getType(),
                message.getSrc(), reply.success));
      }
      this.heartbeatQueue.offer(1);
      return new Message(
          MessageType.AppendEntriesReply,
          message.getDest(),
          message.getSrc(),
          objectToByteArray(reply));
    } else if (message.getType() == MessageType.RequestVoteArgs) {
      RequestVoteArgs request = (RequestVoteArgs) byteArrayToObject(message.getBody());
      RequestVoteReply reply = buildRequestVoteReply(request);

      LOGGER.finest(String
          .format("Node %d %s: rcvd %s from %d. Result: %s", this.id, role, message.getType(),
              message.getSrc(), reply.voteGranted));
      this.heartbeatQueue.offer(1);
      return new Message(
          MessageType.RequestVoteReply,
          this.id,
          request.candidateId,
          objectToByteArray(reply)
      );
    } else {
      LOGGER.warning(String
          .format("Node %d %s: unknown message type %s. ", this.id, role, message.getType()));
    }
    return null;
  }


  /* GETTER & SETTER */
  public synchronized boolean isHeardHeartbeat() {
    return heardHeartbeat;
  }

  public synchronized void setHeardHeartbeat(boolean heardHeartbeat) {
    this.heardHeartbeat = heardHeartbeat;
  }

  public synchronized Role getRole() {
    return role;
  }

  public synchronized void setRole(Role role) {
    this.role = role;
  }

  /* UTILITIES */

  /**
   * Credits: https://goo.gl/EKa7iZ
   */
  private static byte[] objectToByteArray(Object obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(obj);
      out.flush();
      byte[] bytes = bos.toByteArray();
      return bytes;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bos.close();
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  /**
   * Credits: https://goo.gl/EKa7iZ
   */
  @SuppressWarnings("unchecked")
  private static Object byteArrayToObject(byte[] byteArray) {
    ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      return in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ignored) {
      }
    }
    return null;
  }


  private static int randomizeTimeoutPlusRangeMilli(int timeout, int plusRange) {
    Random rand = new Random();
    int plus = rand.nextInt(plusRange + 1); // inclusive: +1
    return timeout + plus;
  }

  //main function
  public static void main(String args[]) throws Exception {
    if (args.length != 3) {
      throw new Exception("Need 2 args: <port> <id> <num_peers>");
    }
    //new usernode
    RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
        Integer.parseInt(args[2]));
  }
}
