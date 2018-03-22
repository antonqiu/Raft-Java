import lib.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Config extends UnicastRemoteObject implements Remote {

    private static final long serialVersionUID = 1L;

    /* ##### Controller ##### */
    private int controllerPort = 14736;   /* Port on which the controller is listening. */
    private Controller transportLayerCtrl;  /* Transport layer controller for this project. */

    private MessagingLayer ms;

    private int numServers;         /* Number of servers in this network. */
    private Process raftPeers[];    /* Process for Raft server. */
    private boolean connected[];    /* Whether each server is connected to this network. */

    private List<Map<Integer, Integer>> logs; /* copy of each server's committed entries */

    private Map<Integer, BlockingQueue<ApplyMsg>> applyMsgMap;

    private Process spawnRaftPeer( int controllerPort, int id, int numServers ) {

        Process raftPeer = null;
        List<String> commands = new ArrayList<String>();
        commands.add("java");
        commands.add("RaftNode");
        commands.add(String.valueOf(controllerPort));
        commands.add(String.valueOf(id));
        commands.add(String.valueOf(numServers));

        ProcessBuilder builder = new ProcessBuilder(commands);
        builder.inheritIO();

        try {
            raftPeer = builder.start();
        } catch (Exception e) {
            e.printStackTrace();
            cleanup();
        }

        return raftPeer;
    }

    /* Nested class*/
    public class NCommitted {

        public int nd; /* how many servers think a log entry is committed? */
        public int cmd;
    }

    public NCommitted nCommitted( int index ) {

        NCommitted reply = new NCommitted();

        int count = 0;
        int cmd = -1;

        for( int i = 0; i < this.numServers; i++ ) {
            Map<Integer, Integer> serverLog = this.logs.get(i);
            if( serverLog.containsKey(index) ) {

                int cmd1 = serverLog.get(index);

                if( (count > 0) && (cmd != cmd1) ) {
                    System.out.println(" Committed values do not match: index " + index + " " + cmd + " " + cmd1);
                    this.cleanup();
                }
                count++;
                cmd = cmd1;
            }
        }

        reply.nd = count;
        reply.cmd = cmd;
        return reply;
    }

    /* Creates a configuration to be used by a tester or a service. */
    public Config( int numServers, boolean reliable, int ctrlPort ) throws RemoteException {

        /* Setup the transport layer controller. */
        controllerPort = ctrlPort;
        try {
            transportLayerCtrl = new Controller(controllerPort);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit( -1 );
        }

        this.logs = new ArrayList<Map<Integer, Integer>>();

        this.applyMsgMap = new HashMap<Integer, BlockingQueue<ApplyMsg>>();

        transportLayerCtrl.setApplyMsgMap(this.applyMsgMap);

        this.numServers = numServers;

        connected = new boolean[numServers];
        raftPeers = new Process[numServers];
        for( int i = 0; i < numServers; i++ ) {


           this.applyMsgMap.put(i, new LinkedBlockingDeque<>());
           BlockingQueue<ApplyMsg> applyMsgsQ = this.applyMsgMap.get(i);
           int nodeID = i;

           new Thread(new Runnable() {
                @Override
                public void run() {
                    listenRaftPeer( nodeID, applyMsgsQ );
                }
            }).start();

            /* Create a new Raft server. */
            raftPeers[i] = spawnRaftPeer(controllerPort, i, numServers);

            /* Connect this raft peer*/
            this.connect(i);

            this.logs.add(new HashMap<Integer, Integer>());
        }
    }

    /* Attach server "whichServer" to this config's network. */
    public void connect( int whichServer ) throws RemoteException{
        this.connected[whichServer] = true;
        transportLayerCtrl.re_connect(whichServer);
    }

    /* Detach server "whichServer" from this config's network. */
    public void disconnect( int whichServer ) throws RemoteException{
        System.out.println("disconnect this " + whichServer);
        this.connected[whichServer] = false;
        transportLayerCtrl.disconnect(whichServer);
    }

    /* Check that there's exactly one leader. */
    public int checkOneLeader() {
        System.out.println("Start to checkone leader");
        int iteration = 0;
        int lastTermWithLeader = 0;

        for( iteration = 0; iteration < 10; iteration++ ) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cleanup();
            }
            HashMap<Integer, List<Integer>> leaders = new HashMap<Integer, List<Integer>>();
            int i = 0;
            boolean skip = false;
            while(true) {
                if (i == numServers) {
                    break;
                }
                if (this.connected[i]) {
                    GetStateReply state = transportLayerCtrl.getState(i);
                    if( state == null ) {
                        /* Seems like some raft peer has not yet registered itself with the controller. */
                        continue;
                    }

                    if ( state.isLeader ) {
                        if (leaders.containsKey( state.term )) {

                            List<Integer> leadersPerTerm = leaders.get(state.term);
                            leadersPerTerm.add(i);

                        } else {
                            List<Integer> leadersPerTerm = new ArrayList<Integer>();
                            leadersPerTerm.add(i);
                            leaders.put(state.term, leadersPerTerm);

                        }
                    }
                }
                i++;
            }
            lastTermWithLeader = -1;

            for (Integer term : leaders.keySet() ) {

                List<Integer> leadersPerTerm = leaders.get( term );
                if( leadersPerTerm.size() > 1 ) {

                    System.err.println( "Fatal : Term " + term + " has " + leadersPerTerm.size() + " (>1) leaders");
                    cleanup();
                }

                if( term > lastTermWithLeader ) {
                    lastTermWithLeader = term;
                }
            }

            if( leaders.size() != 0 ) {
                List<Integer> leadersPerTerm = leaders.get( lastTermWithLeader );
                return leadersPerTerm.get(0);
            }
        }

        System.err.println( "Fatal : No leader found !" );
        cleanup();

        return -1;
    }

    /* Check that everyone agrees on the term. */
    public int checkTerms() {

        int term = -1;

        for( int i = 0; i < this.numServers; i++ ) {

            if( this.connected[i] ) {

                GetStateReply xterm = transportLayerCtrl.getState(i);
                if( term == -1 ) {
                    term = xterm.term;
                } else if( term != xterm.term ) {
                    System.err.println("Servers do not agree on term. Exiting!");
                    cleanup();
                }
            }
        }

        return term;
    }

    /* Check that there's no leader. */
    public void checkNoLeader() {

       for(int i = 0; i < numServers; i++) {
           if( this.connected[i] ) {
               GetStateReply xterm = transportLayerCtrl.getState(i);
               if( xterm.isLeader ) {
                   System.err.println( "Expected no leader, but "+ i + " claims to be leader" );
                   cleanup();
               }
           }
       }
    }

    public int startCommit( int cmd, int expectedServers ) {

        int starts = 0;
        long t0 = System.currentTimeMillis();

        while( (System.currentTimeMillis() - t0 ) < 10000 ) { /* Wait for 10 seconds before giving up. */

            int index = -1;

            for( int si = 0; si < this.numServers; si++ ) {

                starts = (starts + 1) % this.numServers;

                if( this.connected[si] ) {

                    StartReply reply = transportLayerCtrl.start(si, cmd);
                    if( reply == null ) {
                        System.err.println( "Error in executing start commit rpc on " + si );
                        cleanup();
                    }
                    if( reply.isLeader) {
                        index = reply.index;
                        break;
                    }
                }
            }

            if( index != -1) {

                long t1 = System.currentTimeMillis();
                while( (System.currentTimeMillis() - t1 ) < 2000 ) { /* Wait for 2 seconds before giving up. */

                    NCommitted reply = this.nCommitted(index);
                    if( (reply.nd > 0) && (reply.nd >= expectedServers) ) {
                        if( reply.cmd == cmd) {
                            /* It is the command that we submitted. */
                            return index;
                        }
                    }

                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        cleanup();
                    }
                }
            } else {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    cleanup();
                }
            }
        }

        System.err.println( "startCommit(" + cmd + ") failed to reach agreement");
        return -1;
    }

    private void listenRaftPeer( int me, BlockingQueue<ApplyMsg> applyMsgsQ ) {

        ApplyMsg applyMsg = null;
        String errString = null;
        boolean previousOk = false;

        while( true ) {

            /* Wait until you receive a apply message from a raft peer. */
            try {
               applyMsg  = applyMsgsQ.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println( "Listening on raft peer interrupted !");
                cleanup();
            }

            for( int j= 0; j < this.logs.size(); j++) {

                Map<Integer, Integer> logPerServer = this.logs.get(j);
                if (logPerServer.containsKey(applyMsg.index)) {

                    int old = logPerServer.get(applyMsg.index);

                    if (old != applyMsg.command) {
                        /* some server has already committed a different value for this entry! */
                        errString = String.format("Commit index : %d, server = %d - %d != server = %d - %d",
                                applyMsg.index, me, applyMsg.command, j, old);
                    }
                }
            }

            Map<Integer, Integer> logPerServer = this.logs.get(me);

            if( logPerServer.containsKey(applyMsg.index - 1)) {
                previousOk = true;
            } else {
                previousOk = false;
            }

            logPerServer.put( applyMsg.index, applyMsg.command);


            if( (applyMsg.index > 1) && (previousOk == false)) {
                errString =  String.format("server %d apply out of order %d", me, applyMsg.index);
            }



            if( errString != null ) {
                System.err.println(errString);
                cleanup();
            }
        }
    }

    public StartReply start( int nodeID, int cmd ) {
        return transportLayerCtrl.start(nodeID, cmd);
    }


    public int wait( int index, int n, int startTerm ) {

        long t0 = 10; /* 10 miliseconds. */

        for( int i = 0; i < 30; i++ ) {

            NCommitted reply = this.nCommitted(index);
            if( reply.nd >= n ) {
                break;
            }

            try {
                Thread.sleep(t0);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cleanup();
            }

            if( t0 < 1000 ) { /* Less than a second. */
                t0 = t0 * 2;
            }

            if( startTerm > -1 ) {
                for( int j = 0; j < this.numServers; j++ ) {

                    GetStateReply state = transportLayerCtrl.getState(j);
                    if( state != null ) {
                        if(state.term > startTerm) {
                            /*
                             * Someone has moved on
                             * can no longer guarantee that we'll "win"
                             */
                            return -1;
                        }
                    }
                }
            }
        }

        NCommitted reply = this.nCommitted(index);
        if( reply.nd < n ) {
            System.err.println( String.format("Fatal : only %d decided for index %d; wanted %d\n", reply.nd, index, n) );
            cleanup();
        }

        return reply.cmd;
    }

    public void cleanup() {

        for(int i = 0; i < numServers; i++) {

            if( raftPeers[i] != null ) {
                raftPeers[i].destroy();
            }
        }
        System.exit( 0 );
    }

    public void waitUntilAllRegister() {

        int numRegistered = 0;
        while( numRegistered != this.numServers ) {

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cleanup();
            }

            numRegistered = transportLayerCtrl.getNumRegistered();
        }
    }

    public int rpcCount(int node) {
        return transportLayerCtrl.getRPCCount(node);
    }

    public GetStateReply getState( int nodeID ) {
        return transportLayerCtrl.getState(nodeID);
    }
}