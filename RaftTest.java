import lib.*;
import java.util.ArrayList;
import java.util.List;

public class RaftTest {

    private static final int NUM_ARGS = 2;
    private static int controllerPort = 0;

    /*  The tester generously allows solutions to complete elections in one second
     *  (much more than the paper's range of timeouts).
     */
    private static int RAFT_ELECTION_TIMEOUT = 1000;

    private static void TestInitialElection() throws Exception {

        int term1, term2;
        int numServers = 3;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        System.out.println( "Testing initial election ..." );

        /* Check if there is only one leader. */
        cfg.checkOneLeader();

        term1 = cfg.checkTerms();
       
        Thread.sleep( 2 * RAFT_ELECTION_TIMEOUT );

        term2 = cfg.checkTerms();

        if( term1 != term2 ) {
            System.err.println( "Warning : Term changed without any failures. \n" );
        } else {
            System.out.println( " .... Passed ! " );
        }

        cfg.cleanup();

        System.out.println("####### Test done ##########");
    }

    private static void TestReElection() throws Exception {

        int leader1, leader2;
        int numServers = 3;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        System.out.println( "Testing election after network failure...\\n" );

        System.out.println( "Basic one leader ...\\n" );

        leader1 = cfg.checkOneLeader();

        /* if the leader disconnects, a new one should be elected. */
        System.out.println("1 disconnected leader\n");
        cfg.disconnect( leader1 );
        cfg.checkOneLeader();

        /* if the old leader rejoins, that shouldn't disturb the old leader. */
        System.out.println( "old connected 1 leader\n" );
        cfg.connect( leader1 );
        leader2 = cfg.checkOneLeader();

        /* if there's no quorum, no leader should be elected. */
        System.out.println( "2 disconnected no leader\n" );
        cfg.disconnect( leader2 );
        cfg.disconnect( (leader2 + 1) % numServers );
        Thread.sleep( 2 * RAFT_ELECTION_TIMEOUT );
        cfg.checkNoLeader();

        System.out.println( " reconnected 1 leader\n" );
        /* if a quorum arises, it should elect a leader. */
        cfg.connect( (leader2 + 1) % numServers);
        cfg.checkOneLeader();

        /* re-join of last node shouldn't prevent leader from existing. */
        System.out.println( "1 reconnected 1 leader\n" );
        cfg.connect( leader2);
        cfg.checkOneLeader();

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }

    private static void TestBasicAgree() throws Exception {


        int numServers = 5, numIters = 3, i = 0;

        Config.NCommitted ncommit;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        System.out.println( "Testing basic agreement...\\n" );

        for(i = 1; i < numIters + 1; i++) {

            ncommit = cfg.nCommitted(i);

            if( ncommit.nd > 0 ) {
                System.err.println("Some have committed before start!");
                cfg.cleanup();
            }

            ncommit.cmd = cfg.startCommit(i*100, numServers);
            if(ncommit.cmd != i) {
                System.err.println("Got index " + ncommit.cmd + " instead of " + i);
                cfg.cleanup();
            }
        }

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }

    private static void TestFailAgree() throws Exception {

        int numServers = 5, leader = 0;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        System.out.println( "Testing agreement despite follower disconnection\\n" );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        cfg.startCommit(101, numServers);

        // Follower disconnection
        System.out.println("Checking one leader\n");
        leader = cfg.checkOneLeader();
        cfg.disconnect( (leader + 1) % numServers );

        System.out.println("Checking agreement with one disconnected server");
        // agree when one server is down
        cfg.startCommit(102, numServers - 1);
        cfg.startCommit(103, numServers - 1);
        Thread.sleep( RAFT_ELECTION_TIMEOUT );
        cfg.startCommit(104, numServers - 1);
        cfg.startCommit(105, numServers - 1);

        //reconnect
        cfg.connect((leader + 1) % numServers);
        System.out.println("Checking with reconnected server");
        cfg.startCommit(106, numServers);
        Thread.sleep( RAFT_ELECTION_TIMEOUT );
        cfg.startCommit(107, numServers);

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }

    private static void TestFailNoAgree() throws Exception {

        int numServers = 5, leader = 0, leader2 = 0;

        StartReply startReply, startReply2;
        Config.NCommitted ncommit;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        System.out.println( "Testing no agreement if too many followers disconnect\\n" );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        cfg.startCommit(10, numServers);

        leader = cfg.checkOneLeader();
        cfg.disconnect( (leader + 1) % numServers );
        cfg.disconnect( (leader + 2) % numServers );
        cfg.disconnect( (leader + 3) % numServers );

        startReply = cfg.start(leader, 20);

        if( ! startReply.isLeader) {
            System.err.println("Leader rejected start");
            cfg.cleanup();
        }

        if( startReply.index != 2 ) {
            System.err.println("Got index " + startReply.index + " instead of 2");
            cfg.cleanup();
        }

        Thread.sleep( 2 * RAFT_ELECTION_TIMEOUT );

        ncommit = cfg.nCommitted(startReply.index);

        if(ncommit.nd > 0) {
            System.err.println(ncommit.nd + " committed but no majority");
            cfg.cleanup();
        }

        //repair
        cfg.connect( (leader + 1) % numServers );
        cfg.connect( (leader + 2) % numServers );
        cfg.connect( (leader + 3) % numServers );

        // the disconnected majority may have chosen a leader from
        // among their own ranks, forgetting index 2.
        // or perhaps
        leader2 = cfg.checkOneLeader();
        startReply2 = cfg.start(leader2, 20);
        if( ! startReply2.isLeader) {
            System.err.println("Leader 2 rejected start");
            cfg.cleanup();
        }

        if( (startReply2.index < 2) || (startReply2.index > 3) ) {
            System.err.println("Unexpected index " + startReply2.index);
            cfg.cleanup();
        }

        cfg.startCommit(100, numServers);

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }

    private static void TestRejoin() throws Exception {

        int numServers = 5, leader = 0, leader2 = 0;

        StartReply startReply, startReply2;
        Config.NCommitted ncommit;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        System.out.println( "Testing rejoin of partitioned leader\\n" );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        cfg.startCommit(101, numServers);

        // leader network failure
        leader = cfg.checkOneLeader();
        cfg.disconnect( leader );

        // make old leader try to agree on some entries
        cfg.start(leader, 102);
        cfg.start(leader, 103);
        cfg.start(leader, 104);

        // new leader commits for index 2
        cfg.startCommit(103, 2);

        // new leader network failure
        leader2 = cfg.checkOneLeader();
        cfg.disconnect( leader2 );

        // old leader connected again
        cfg.connect(leader);

        cfg.startCommit(104, 2);

        // all together now
        cfg.connect(leader2);

        cfg.startCommit(105, numServers);

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }

    private static void TestBackup() throws Exception {

        int numServers = 5, leader = 0, leader2 = 0, i = 0, other = 0;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        System.out.println( "Testing leader backs up quickly over incorrect follower logs ...\n" );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        cfg.startCommit(109, numServers);

        // put leader and one follower in a partition
        leader = cfg.checkOneLeader();
        cfg.disconnect( (leader + 2) % numServers );
        cfg.disconnect( (leader + 3) % numServers );
        cfg.disconnect( (leader + 4) % numServers );

        // submit lots of commands that won't commit
        for(i = 0; i < 50; i ++) {
            cfg.start(leader, 110 + i);
        }
    

        Thread.sleep( RAFT_ELECTION_TIMEOUT / 2);

        cfg.disconnect( (leader + 0) % numServers );
        cfg.disconnect( (leader + 1) % numServers );

        // allow other partition to recover
        cfg.connect( (leader + 2) % numServers );
        cfg.connect( (leader + 3) % numServers );
        cfg.connect( (leader + 4) % numServers );

        // lots of successful commands to new group.
        for(i = 0; i < 50; i ++) {
            cfg.startCommit(160 + i, 3);
        }

        // now another partitioned leader and one follower
        leader2 = cfg.checkOneLeader();
        other = (leader + 2) % numServers;
        if(leader2 == other) {
            other = (leader2 + 1) % numServers;
        }

        cfg.disconnect(other);

        // lots more commands that wont get committed
        for(i = 0; i < 50; i ++) {
            cfg.start(leader2, 210 + i);
        }

        Thread.sleep( RAFT_ELECTION_TIMEOUT / 2);

        // bring original leader back to life
        for(i = 0; i < numServers; i++) {
            cfg.disconnect(i);
        }

        cfg.connect((leader + 0) % numServers);
        cfg.connect((leader + 1) % numServers);
        cfg.connect(other);

        // lots of successful commands
        for(i = 0; i < 50; i ++) {
            cfg.startCommit(260 + i, 3);
        }

        // now everyone
        for(i = 0; i < numServers; i++) {
            cfg.connect(i);
        }

        cfg.startCommit(500, numServers);

        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }
    
    private static void TestCount() throws Exception {


        int numServers = 3, i = 0, j = 0, total1 = 0, total2 = 0, total3 = 0;
        int leader = 0, iters = 0;
        int randomCmd = 101;

        StartReply startReply1, startReply2;

        Config cfg = new Config( numServers, true /* is_reliable ? */, controllerPort );

        System.out.println( "Testing RPC counts are not too high\\n" );

        /* Waiting for all raft peers to start and register with Transport Layer controller. */
        cfg.waitUntilAllRegister();

        leader = cfg.checkOneLeader();

        for(i = 0; i < numServers; i++) {
            total1 += cfg.rpcCount(i);
        }

        if( (total1 > 30) || (total1 < 1)) {
            System.err.println("Too many or too few RPC to elect leader");
            cfg.cleanup();
        }

        boolean success = false;

        loop:

            for(j = 0; j < 5; j++) {

                total1 = 0;

                if( j > 0 ) {
                    // give solution some time to settle
                    Thread.sleep(3000);
                }

                leader = cfg.checkOneLeader();

                for(i = 0; i < numServers; i++) {
                    total1 += cfg.rpcCount(i);
                }

                iters = 10;

                startReply1 = cfg.start(leader, 1);

                if( !startReply1.isLeader ) {
                    // leader moved on too quickly
                    continue;
                }

                List<Integer> cmds = new ArrayList<Integer>();

                for(i = 1; i < (iters + 2); i++) {

                    cmds.add(randomCmd);

                    startReply2 = cfg.start(leader, randomCmd);

                    randomCmd++;

                    if( startReply2.term != startReply1.term) {
                        // Term changed while starting.
                        continue loop;
                    }

                    if( !startReply2.isLeader ) {
                        // No longer leader, so term has changed.
                        continue loop;
                    }

                    if( (startReply1.index + i) != startReply2.index ) {
                        System.err.println("Start() failed");
                        cfg.cleanup();
                    }
                }

                for( i = 1; i < (iters + 1); i++) {
                    int cmd = cfg.wait( (startReply1.index + i), numServers, startReply1.term);
                    if( cmd != cmds.get(i-1)) {

                        if( cmd == -1) {
                            // term changed -- try again
                            continue loop;
                        }

                        System.err.format("wrong value %d committed for index %d; expected %d\n", cmd, startReply1.index+i, cmds);
                        cfg.cleanup();
                    }
                }

                boolean failed = false;

                total2 = 0;

                for( int k = 0; k < numServers; k++ ) {

                    GetStateReply reply = cfg.getState(k);
                    if( reply != null ) {
                        if( reply.term != startReply1.term) {
                            // term changed -- can't expect low RPC counts
                            // need to keep going to update total2
                            failed = true;
                        }
                    }

                    total2 += cfg.rpcCount(k);
                }

                if( failed == true ) {
                    continue loop;
                }

                if( (total2 - total1) > ((iters + 4)*3)) {
                    System.err.println("Too many RPCs");
                    cfg.cleanup();
                }

                success = true;
                break;
            }

            if( !success ) {
                System.err.println("Term changed too often");
                cfg.cleanup();
            }

            Thread.sleep(RAFT_ELECTION_TIMEOUT);

            total3 = 0;

            for( int k = 0; k < numServers; k++) {
                total3 += cfg.rpcCount(k);
            }

            if( (total3 - total2) > (60)) {
                System.err.format("too many RPCs (%v) for 1 second of idleness\n", total3-total2);
                cfg.cleanup();
            }


        System.out.println( "  ... Passed\n" );

        cfg.cleanup();
    }


    public static void main( String[] args ) throws InterruptedException {

        if( args.length != NUM_ARGS ) {
            System.err.println( "Invalid number of arguments\n" );
            return;
        }

        String testCase = args[0];
        controllerPort = Integer.parseInt( args[1] );

        try {
            switch (testCase) {
                case "Initial-Election":
                    TestInitialElection();
                    break;

                case "Re-Election":
                    TestReElection();
                    break;

                case "Basic-Agree":
                    TestBasicAgree();
                    break;

                case "Fail-Agree":
                    TestFailAgree();
                    break;

                case "Fail-NoAgree":
                    TestFailNoAgree();
                    break;

                case "Rejoin":
                    TestRejoin();
                    break;

                case "Backup":
                    TestBackup();
                    break;

                case "Count":
                    TestCount();
                    break;
            }
        } catch (Exception e) {
                e.printStackTrace();
        }
    }
}