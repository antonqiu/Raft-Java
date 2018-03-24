import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
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
import lib.Log;
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

  private static final int ELECTION_TIMEOUT_MIN = 800;
  private static final int HEARTBEAT_TIMEOUT_MIN = 250;
  private static final int HEARTBEAT_EVERY_MILLI = 120; // 8 heartbeats per second
  private static final int RANDOM_TIMEOUT_PLUS_RANGE = 200;
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
  private BlockingQueue<Object> outgoingReplyMsgQueue;

  public RaftNode(int port, int id, int num_peers) {
    LOGGER.setLevel(Level.ALL);
    ConsoleHandler ch = new ConsoleHandler();
    ch.setLevel(Level.FINE);
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

    // init queue
    this.outgoingReplyMsgQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

    this.startRoleChangeOperator();
    LOGGER.fine(String.format("Node %d initialized.", this.id));
    // System.out.println(String.format("Node %d initialized.", this.id));
  }

  private void startRoleChangeOperator() {
    int actualHeartbeatTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(
        HEARTBEAT_TIMEOUT_MIN, RANDOM_TIMEOUT_PLUS_RANGE);
    int actualElectionTimeout = ELECTION_TIMEOUT_MIN;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
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
              LOGGER.fine(String.format("Node %d: Heartbeat timed out.", id));
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
      LogEntry[] entries;
      if (isHeartbeat) {
        entries = null;
      } else {
        entries = state.log.getLogEntriesInRange(nextIndex[peerId], state.log.getSize());
      }
      state.log.getTermForIndex(this.nextIndex[peerId]);
      AppendEntriesArgs args = new AppendEntriesArgs(
          this.state.currentTerm,
          this.id,
          this.nextIndex[peerId] - 1,
          state.log.getTermForIndex(this.nextIndex[peerId] - 1),
          entries,
          this.commitIndex
      );
      Executor executor = Executors.newSingleThreadExecutor();
      CompletableFuture.supplyAsync(() -> {
        byte[] argsBytes = RaftNode.objectToByteArray(args);
        Message msg = new Message(
            MessageType.AppendEntriesArgs,
            this.id,
            peerId,
            argsBytes
        );
        try {
          return lib.sendMessage(msg);
        } catch (RemoteException e) {
          LOGGER.info("RMI exception when sending AppendEntriesArgs:\n " + msg.toString());
          // e.printStackTrace();
          return null;
        }
      }, executor)
          .thenAcceptAsync(this::revAppendEntryReply, executor);
    }
  }

  private synchronized void revAppendEntryReply(Message reply) {
    LOGGER.fine(String.format("Node %d %s: rcvd AppendEntryReply from %d.", id, role, reply.getSrc()));
    if (reply == null || role != Role.LEADER) {
      return;
    }
    AppendEntriesReply repliedBytes = byteArrayToObject(reply.getBody());
    if (repliedBytes == null) {
      return;
    }

    // all server #2, leader to follower
    if (role != Role.FOLLOWER && repliedBytes.term > state.currentTerm) {
      setRole(Role.FOLLOWER);
      this.state.currentTerm = repliedBytes.term;
      this.state.votedFor = -1;
      LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Waiting for HB.", id, role));
    }

    // TODO:
  }

  private void electSelf() {
    LOGGER.finer(String.format("Node %d decides to elect itself.", id));

    // update states
    setRole(Role.CANDIDATE);
    this.votesRcvd = 1; // vote for itself
    this.state.currentTerm++;
    this.state.votedFor = this.id;

    Executor executor = Executors.newSingleThreadExecutor();
    for (int i = 0; i < num_peers; i++) {
      final int peerId = i;
      if (peerId == this.id) {
        continue;
      }
      RequestVoteArgs args = new RequestVoteArgs(
          this.state.currentTerm,
          this.id,
          this.state.log.getSize() - 1,
          this.state.log.getTermForIndex(this.state.log.getSize() - 1));
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
      }, executor)
          .thenAcceptAsync(this::revRequestVoteReply, executor);
    }
  }

  private synchronized void revRequestVoteReply(Message message) {
    if (message == null || role != Role.CANDIDATE) {
      return;
    }
    RequestVoteReply reply = byteArrayToObject(message.getBody());
    if (reply == null) {
      return;
    }

    if (reply.voteGranted) {
      this.votesRcvd++;
    }

    if (votesRcvd > num_peers / 2) {
      LOGGER.fine(String.format("Node %d was elected as LEADER.", id));
      setRole(Role.LEADER);
      outgoingReplyMsgQueue.add(reply);
      Arrays.fill(this.nextIndex, this.state.log.getSize());
      Arrays.fill(this.matchIndex, 0);
    }
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
            state.log.getLastestIndex() + 1,
            command));
    this.sendAppendEntry(false);
    return new StartReply(state.log.getLastestIndex(), state.currentTerm, true);
  }

  @Override
  public synchronized GetStateReply getState() {
    return new GetStateReply(state.currentTerm, role == Role.LEADER);
  }

  @Override
  public Message deliverMessage(Message message) {
    LOGGER.finest(String.format("Node %d %s: rcvd %s from %d. Processing.", this.id, role, message
            .getType(),
        message.getSrc()));
    if (message.getType() == MessageType.AppendEntriesArgs) {
      AppendEntriesArgs request = byteArrayToObject(message.getBody());
      AppendEntriesReply reply = buildAppendEntryReply(request);
      LOGGER.finest(String
          .format("Node %d %s: rcvd %s from %d. Result: %s", this.id, role, message.getType(),
              message.getSrc(), reply.success));
      return new Message(
          MessageType.AppendEntriesReply,
          message.getDest(),
          message.getSrc(),
          objectToByteArray(reply));
    } else if (message.getType() == MessageType.RequestVoteArgs) {
      this.state.setVoting(true);
      RequestVoteArgs request = byteArrayToObject(message.getBody());
      RequestVoteReply reply = buildRequestVoteReply(request);
      // wait for election timeout
      CompletableFuture.runAsync(() -> {
        try {
          TimeUnit.MILLISECONDS.sleep(ELECTION_TIMEOUT_MIN);
          if (state.isVoting) {
            LOGGER.finest(String.format("Node %d: reset votedFor.", this.id));
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
    }
    return null;
  }

  private synchronized RequestVoteReply buildRequestVoteReply(RequestVoteArgs request) {
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      setRole(Role.FOLLOWER);
      this.state.currentTerm = request.term;
      this.state.votedFor = -1;
    }

    // #1
    if (request.term < this.state.currentTerm) {
      RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, false);
      outgoingReplyMsgQueue.add(r);
      return r;
    }

    // additional rule: break the tie
    if (request.term == this.state.currentTerm && role == Role.CANDIDATE) {
      // break the tie
      boolean voteGranted = request.candidateId > this.id;
      RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, voteGranted);
      outgoingReplyMsgQueue.add(r);
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
      outgoingReplyMsgQueue.add(r);
      LOGGER.finer(String.format("Node %d voted for %d.", id, request.candidateId));
      return r;
    }

    // #3
    RequestVoteReply r = new RequestVoteReply(this.state.currentTerm, false);
    outgoingReplyMsgQueue.add(r);
    return r;
  }

  private synchronized AppendEntriesReply buildAppendEntryReply(AppendEntriesArgs request) {
    setHeardHeartbeat(true);
    state.setVoting(false);
    // all server #2, leader to follower
    if (request.term > state.currentTerm) {
      setRole(Role.FOLLOWER);
      this.state.currentTerm = request.term;
      this.state.votedFor = request.leaderId;
      LOGGER.fine(String.format("Node %d %s: resigned to FOLLOWER. Recognized node %d.", id,
          role, request.leaderId));
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
      outgoingReplyMsgQueue.add(r);
      return r;
    }

    // #2 RPC
    if (state.log.getTermForIndex(request.prevLogIndex) != request.prevLogTerm) {
      AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, false);
      outgoingReplyMsgQueue.add(r);
      return r;
    }

    // #3 & #4
    for (int newEntriesIdx = 0; newEntriesIdx < request.entries.length; newEntriesIdx++) {
      int logIdx = request.prevLogIndex + 1 + newEntriesIdx;
      LogEntry proposed = request.entries[newEntriesIdx];
      LogEntry saved = state.log.getLogEntryByIndex(logIdx);
      if (saved == null) {
        state.log.appendAllLogEntries(newEntriesIdx, request.entries);
        break;
      } else if (!saved.equals(proposed)) {
        state.log.removeLogEntriesStartingAt(logIdx);
        state.log.appendAllLogEntries(newEntriesIdx, request.entries);
        break;
      }
    }

    // #5
    if (request.leaderCommit > commitIndex) {
      commitIndex = Math.min(request.leaderCommit, state.log.getLastestIndex());
    }

    // all server #1
    if (lastApplied < commitIndex) {
      for (int i = lastApplied; i < commitIndex; i++) {
        try {
          lib.applyChannel(new ApplyMsg(this.id, i + 1, state.log.getLogEntryByIndex(i + 1)
              .getCommand(), false, null));
          lastApplied += 1;
        } catch (RemoteException e) {
          e.printStackTrace();
        }
      }
    }

    AppendEntriesReply r = new AppendEntriesReply(state.currentTerm, true);
    outgoingReplyMsgQueue.add(r);
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
  private static <T extends Serializable> T byteArrayToObject(byte[] byteArray) {
    ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      return (T) in.readObject();
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
