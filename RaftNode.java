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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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

  private static final int ELECTION_TIMEOUT_MIN = 600;
  private static final int HEARTBEAT_TIMEOUT_MIN = 300;
  private static final int HEARTBEAT_EVERY_MILLI = 120; // 8 heartbeats per second
  private static final int RANDOM_TIMEOUT_PLUS_RANGE = 300;
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

  public RaftNode(int port, int id, int num_peers) {
    LOGGER.setLevel(Level.ALL);
    ConsoleHandler ch = new ConsoleHandler();
    ch.setLevel(Level.FINER);
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

    this.startRoleChangeOperator();
    LOGGER.fine(String.format("Node %d initialized.", this.id));
  }

  private void startRoleChangeOperator() {
    int actualHeartbeatTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(
        HEARTBEAT_TIMEOUT_MIN, RANDOM_TIMEOUT_PLUS_RANGE);
    int actualElectionTimeout = ELECTION_TIMEOUT_MIN;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      // LOGGER.info(Thread.currentThread().getName());
      while (true) {
        if (this.role == Role.LEADER) {
          synchronized (this) {
            LOGGER.finest(String.format("Node %d: send heartbeat.", this.id));
            this.sendAppendEntry(true);
          }
          TimeUnit.MILLISECONDS.sleep(HEARTBEAT_EVERY_MILLI);
        } else if (this.role == Role.FOLLOWER) {
          TimeUnit.MILLISECONDS.sleep(actualHeartbeatTimeout);
          synchronized (this) {
            if (!isHeardHeartbeat() && !state.isVoting) {
              LOGGER.fine(String.format("Node %d %s: timeout: HB %s, voting %s.", id, role,
                  isHeardHeartbeat(), state.isVoting()));
              this.electSelf();
            }
            setHeardHeartbeat(false);
          }
        } else {
          TimeUnit.MILLISECONDS.sleep(actualElectionTimeout);
          synchronized (this) {
            if (this.role == Role.CANDIDATE && !isHeardHeartbeat()) {
              LOGGER.fine(String.format("Node %d: Election timed out. Role: %s", id, role));
              this.electSelf();
            }
            setHeardHeartbeat(false);
          }
        }
      }
    });
  }

  private void sendAppendEntry(boolean isHeartbeat) {
    if (this.role != Role.LEADER) {
      return;
    }
    for (int i = 0; i < num_peers; i++) {
      final int peerId = i;
      if (peerId == this.id) {
        continue;  // do not send message to itself
      }
      // build AppendEntriesArgs
      ArrayList<LogEntry> entries;
      if (isHeartbeat) {
        entries = new ArrayList<>();
      } else {
        entries = state.log.getLogEntriesInRange(nextIndex[peerId], state.log.getLatestIndex() + 1);
        LOGGER.finest(String.format("Node %d %s: sublist: %d %d -> length %d.", id, role,
            nextIndex[peerId], state.log.getSize(), entries.size()));
      }
      AppendEntriesArgs args = new AppendEntriesArgs(
          this.state.currentTerm,
          this.id,
          this.nextIndex[peerId] - 1,
          state.log.getTermForIndex(this.nextIndex[peerId] - 1),
          entries,
          this.commitIndex
      );
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(() -> {
        try {
          byte[] argsBytes = RaftNode.objectToByteArray(args);
          Message msg = new Message(
              MessageType.AppendEntriesArgs,
              this.id,
              peerId,
              argsBytes
          );
          if (isHeartbeat) {
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
      setRole(Role.FOLLOWER);
      this.state.currentTerm = aer.term;
      this.state.votedFor = -1;
      LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Waiting for HB.", id, role));
      return;
    }

    if (aer.success) {
      this.nextIndex[reply.getSrc()] += args.entries.size();
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
          if (matchIndex[peerId] >= matchIndex[reply.getSrc()]) {
            cnt++;
          }
          if (cnt > num_peers / 2) {
            // ready to commit and apply
            commitIndex = matchIndex[reply.getSrc()];
            for (int cIdx = lastApplied + 1; cIdx <= commitIndex; cIdx++) {
              try {
                lib.applyChannel(new ApplyMsg(this.id, cIdx, state.log.getLogEntryByIndex(cIdx)
                    .getCommand(), false, null));
                LOGGER.fine(String.format("Node %d %s: applied: idx%d command%d", id, role, cIdx,
                    state.log.getLogEntryByIndex(cIdx).getCommand()));
                lastApplied = cIdx;
              } catch (RemoteException e) {
                LOGGER.info(
                    "RMI exception when applying entry:\n " + state.log.getLogEntryByIndex(cIdx)
                        .toString());
                // e.printStackTrace();
              }
            }
            break;
          }
        }
      }
    } else {
      // rejected
      nextIndex[reply.getSrc()]--;

      LOGGER.finer(
          String.format("Node %d %s: AER FROM %d REJECTED: %d %d",
              id, role, reply.getSrc(), nextIndex[reply.getSrc()], matchIndex[reply.getSrc()]));
    }
  }

  private void electSelf() {
    LOGGER.finer(String.format("Node %d decides to elect itself.", id));

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
      RequestVoteArgs args = new RequestVoteArgs(
          this.state.currentTerm,
          this.id,
          state.log.getLatestIndex(),
          this.state.log.getTermForIndex(this.state.log.getSize()));
      CompletableFuture.supplyAsync(() -> {
        Message msg = new Message(
            MessageType.RequestVoteArgs,
            this.id,
            peerId,
            objectToByteArray(args)
        );
        try {
          return lib.sendMessage(msg);
        } catch (RemoteException e) {
          LOGGER.info("RMI exception when sending RequestVoteArgs:\n " + msg.toString());
          // e.printStackTrace();
          return null;
        }
      })
          .thenAcceptAsync(this::revRequestVoteReply);
    }
  }

  private synchronized void revRequestVoteReply(Message message) {
    if (message == null || role != Role.CANDIDATE) {
      return;
    }
    RequestVoteReply reply = (RequestVoteReply) byteArrayToObject(message.getBody());
    if (reply == null) {
      return;
    }

    if (state.currentTerm < reply.term) {
      state.currentTerm = reply.term;
      setRole(Role.FOLLOWER);
      state.votedFor = -1;
      LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER upon vote message.", id, role));
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
      LOGGER.fine(String.format("Node %d was elected as LEADER.", id));
      setRole(Role.LEADER);
      Arrays.fill(this.nextIndex, this.state.log.getLatestIndex() + 1);
      Arrays.fill(this.matchIndex, 0);
      state.setVoting(false);
    }
  }

  /*
   *call back.
   */

  @Override
  public StartReply start(int command) {
    if (this.role != Role.LEADER) {
      return new StartReply(0, 0, false);
    }

    this.state.log.appendLogEntry(
        new LogEntry(state.currentTerm,
            state.log.getLatestIndex() + 1,
            command));

    LOGGER.fine(String.format("Node %d %s: Command: %d", id, role, command));
    this.sendAppendEntry(false);
    LOGGER.fine(String.format("Node %d %s: Command: %d, scheduled.", id, role, command));

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

      return new Message(
          MessageType.AppendEntriesReply,
          message.getDest(),
          message.getSrc(),
          objectToByteArray(reply));
    } else if (message.getType() == MessageType.RequestVoteArgs) {
      this.state.setVoting(true);
      RequestVoteArgs request = (RequestVoteArgs) byteArrayToObject(message.getBody());
      RequestVoteReply reply = buildRequestVoteReply(request);
      // wait for election timeout
      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          TimeUnit.MILLISECONDS.sleep(ELECTION_TIMEOUT_MIN);
          if (state.isVoting) {
            LOGGER.finer(String.format("Node %d: reset votedFor.", this.id));
      this.state.votedFor = -1;
    }
    this.state.setVoting(false);
  } catch (InterruptedException e) {
    e.printStackTrace();
  }
});

    LOGGER.finest(String
    .format("Node %d %s: rcvd %s from %d. Result: %s", this.id, role, message.getType(),
    message.getSrc(), reply.voteGranted));
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

  private synchronized RequestVoteReply buildRequestVoteReply(RequestVoteArgs request) {
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      if (role != Role.FOLLOWER)
        LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Waiting for HB.", id, role));
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
        request.candidateId,request.lastLogTerm, request.lastLogIndex, state.log.getLatestTerm(),
        state.log.getLatestIndex()));
    return r;
  }

  private synchronized AppendEntriesReply buildAppendEntryReply(AppendEntriesArgs request) {
    setHeardHeartbeat(true);
    state.setVoting(false);
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      if (role != Role.FOLLOWER) {
        LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Recognized node %d.", id,
            role, request.leaderId));
      }
      setRole(Role.FOLLOWER);
      this.state.currentTerm = request.term;
      this.state.votedFor = request.leaderId;
    }

    // at this point, we're sure that this is the current leader
    if (state.votedFor == -1) {
      LOGGER.fine(String.format("Node %d %s: Recognized node %d.", id, role, request.leaderId));
      this.state.votedFor = request.leaderId;
    }

    // #1 AppendEntriesRPC
    if (request.term < state.currentTerm) {
      // reply false
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false);
      return r;
    }

    // #2 RPC
    if (state.log.getTermForIndex(request.prevLogIndex) != request.prevLogTerm) {
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false);
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
          LOGGER.fine(String.format("Node %d %s: about to apply: idx%d command: %d", id, role, i
                  + 1, state.log.getLogEntryByIndex(i + 1).getCommand()));
          lib.applyChannel(new ApplyMsg(this.id, state.log.getLogEntryByIndex(i + 1).getIndex(), state.log
              .getLogEntryByIndex(i + 1).getCommand(), false, null));
          LOGGER.fine(String.format("Node %d %s: applied: idx%d command: %d", id, role, i + 1,
              state.log.getLogEntryByIndex(i + 1).getCommand()));
          lastApplied = i + 1;
        } catch (RemoteException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, true);
    return r;
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
