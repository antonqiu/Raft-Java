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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lib.AppendEntriesReply;
import lib.AppendEntriesArgs;
import lib.GetStateReply;
import lib.Log;
import lib.LogEntry;
import lib.Message;
import lib.MessageHandling;
import lib.MessageType;
import lib.PersistentState;
import lib.Role;
import lib.StartReply;
import lib.TransportLib;

public class RaftNode implements MessageHandling {

  private final static Logger LOGGER = Logger.getLogger(RaftNode.class.getName());

  private static final int ELECTION_TIMEOUT_MIN = 250;
  private static final int HEARTBEAT_TIMEOUT_MIN = 300;
  private static final int HEARTBEAT_EVERY_MILLI = 125; // 8 heartbeats per second
  private static final int RANDOM_TIMEOUT_PLUS_RANGE = 150;

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

  public RaftNode(int port, int id, int num_peers) {
    LOGGER.setLevel(Level.INFO);

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
    Arrays.fill(this.nextIndex, -1);
    this.matchIndex = new int[num_peers];
    Arrays.fill(this.matchIndex, 0);

    this.startRoleChangeOperator();
  }

  private void startRoleChangeOperator() {
    int actualHeartbeatTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(
        HEARTBEAT_TIMEOUT_MIN, RANDOM_TIMEOUT_PLUS_RANGE);
    int actualElectionTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(
        ELECTION_TIMEOUT_MIN, RANDOM_TIMEOUT_PLUS_RANGE);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      while (true) {
        if (this.getRole() == Role.LEADER) {
          synchronized (this) {
            this.sendAppendEntry(true);
          }
        } else {
          if (this.role == Role.FOLLOWER) TimeUnit.MILLISECONDS.sleep(actualHeartbeatTimeout);
          else TimeUnit.MILLISECONDS.sleep(actualElectionTimeout);
          synchronized (this) {
            if (!isHeardHeartbeat()) {
              this.electSelf();
            }
          }
        }
      }
    });
  }

  private void sendAppendEntry(boolean isHeartbeat) {
    if (this.role != Role.LEADER) return;
    Log log = this.state.log;
    for (int i = 0; i < num_peers; i++) {
      final int peerId = i;
      if (peerId == this.id) return;  // do not send message to itself
      // build AppendEntriesArgs
      LogEntry[] entries;
      if (isHeartbeat) {
        entries = null;
      } else {
        entries = log.getLogEntriesInRange(nextIndex[peerId] - 1, log.getSize());
      }

      AppendEntriesArgs args = new AppendEntriesArgs(
          this.state.currentTerm,
          this.id,
          this.nextIndex[peerId] - 1,
          log.getTermForIndex(this.nextIndex[peerId] - 1),
          entries,
          this.commitIndex
      );
      CompletableFuture.supplyAsync(() -> {
        byte[] argsBytes = RaftNode.objectToByteArray(args);
        Message msg = new Message(
            MessageType.AppendEntriesArgs,
            this.id,
            peerId,
            argsBytes
        );
        try {
          return this.lib.sendMessage(msg);
        } catch (RemoteException e) {
          LOGGER.info("RMI exception when sending AppendEntriesArgs:\n " + msg.toString());
          // e.printStackTrace();
          return null;
        }
      })
      .thenAccept(this::revAppendEntryReply);
    }
  }

  private synchronized void revAppendEntryReply(Message reply) {
    if (reply == null) return;
    AppendEntriesReply repliedBytes = byteArrayToObject(reply.getBody());

    // TODO:
  }

  /**
   * Credits: https://goo.gl/EKa7iZ
   * @param obj
   * @return
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
      } catch (IOException ignored) {}
    }
    return null;
  }

  /**
   * Credits: https://goo.gl/EKa7iZ
   * @param byteArray
   * @param <T>
   * @return
   */
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
      } catch (IOException ignored) {}
    }
    return null;
  }

  private void electSelf() {
  }


  private static int randomizeTimeoutPlusRangeMilli(int timeout, int plusRange) {
    Random rand = new Random();
    int plus = rand.nextInt(plusRange + 1); // inclusive: +1
    return timeout + plus;
  }

  private void waitForRequestVoteReplyOrElectAgain() {}

  private void heartbeat() {

  }

  /*
   *call back.
   */
  @Override
  public StartReply start(int command) {
    return null;
  }

  @Override
  public GetStateReply getState() {
    return null;
  }

  @Override
  public Message deliverMessage(Message message) {
    return null;
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

  public synchronized Role getRole() {
    return role;
  }

  public synchronized void setRole(Role role) {
    this.role = role;
  }

  public synchronized boolean isHeardHeartbeat() {
    return heardHeartbeat;
  }

  public synchronized void setHeardHeartbeat(boolean heardHeartbeat) {
    this.heardHeartbeat = heardHeartbeat;
  }
}
