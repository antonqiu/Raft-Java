import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lib.*;

public class RaftNode implements MessageHandling {

  private static final int ELECTION_TIMEOUT = 450;
  private static final int HEARTBEAT_TIMEOUT = 300;
  private static final int HEARTBEAT_EVERY_MILLI = 150;
  private static final int RANDOM_TIMEOUT_PLUS_RANGE = 100;

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
  private ArrayList<Integer> nextIndex;
  private ArrayList<Integer> matchIndex;

  public RaftNode(int port, int id, int num_peers) {
    this.id = id;
    this.num_peers = num_peers;
    lib = new TransportLib(port, id, this);

    // init states
    this.role = Role.FOLLOWER;  // initially as a follower
    this.state = new PersistentState(); // a persistent state where log resides
    this.heardHeartbeat = false;
    this.commitIndex = 0;
    this.lastApplied = 0;

    this.listenForHeartbeat()
    .thenRun(this.electSelf());
  }

  private Runnable electSelf() {
    return new Runnable() {
      @Override
      public void run() {

      }
    };
  }

  private CompletableFuture listenForHeartbeat() {
    CompletableFuture promise = CompletableFuture
        .runAsync(() -> {
      int actualTimeout = RaftNode.randomizeTimeoutPlusRangeMilli(HEARTBEAT_TIMEOUT,
          RANDOM_TIMEOUT_PLUS_RANGE);
      while (true) {
        try {
          TimeUnit.MILLISECONDS.sleep(actualTimeout);
          if (!this.heardHeartbeat) return;
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    return promise;
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
}
