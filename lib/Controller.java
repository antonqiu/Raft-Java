package lib;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controller - This class is the implementation of the MessageLayer, which is
 * to simulate an underlayer unreliable network, all the message from each rafe
 * peer will be processed by this network, and the network decide whether there
 * is delay or drop the message packets.
 *
 * The Controller will be a RMI server executing on an input port and manage
 * the communicating of all the raf peers. The instance will be composed in the
 * Config.
 *
 * @author Sphoorti Joglekar, Priya Avhad, Yijia Cui, Zonglin Wang
 */
public class Controller extends UnicastRemoteObject implements MessagingLayer {
    /**
     * For byte serialization.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Registry for the RMI server.
     */
    private Registry reg;
    /**
     * Contains map from the node ID to the remote reference object of each
     * raft peer.
     */
    private Map<Integer, Node> nodes;
    /**
     * Network reliable flag, delay ,drop.
     */
    private boolean reliable;
    /**
     * Contains map from the node ID to each currently unconnected node.
     * raft peer.
     */
    private Map<Integer, Node> disconnected_nodes;

    private Map<Integer, BlockingQueue<ApplyMsg>> applyMsgMap;
    /**
     * rpc counters.
     */
    private Map<Integer, Integer> rpc_counters;
    /**
     * Lock for sync
     */
    private Object lock;
    /**
     * Controller - Construct a controller listening on a given port.
     * @param port the given port for controller
     * @throws Exception
     */
    public Controller(int port) throws Exception{
        reliable = true;
        try {
            reg = LocateRegistry.createRegistry(port);
        } catch (RemoteException e) {
            //already exist
        }
        Naming.rebind("rmi://localhost:" + port + "/MessageServer", this);
        nodes = new HashMap<>();
        disconnected_nodes = new HashMap<>();
        rpc_counters = new HashMap<>();
        lock = new Object();
    }

    /**
     * setReliable - Setter of the long reliability flag.
     *
     * @param reliable flag to set
     */
    public void setReliable(boolean reliable) {
        this.reliable = reliable;
    }

    /**
     * register - Register the node with the Message Server.
     * @param id the node id
     * @param remoteController the remoteControllerInterface of a raft peer.
     */
    public void register(int id, RemoteControllerIntf remoteController) {
        this.nodes.putIfAbsent(id, new Node(id, remoteController));
    }

    /**
     * send - Start a new thread to send out the message.
     * @param message the message to be sent.
     *
     * @throws RemoteException when the deliver RMI call fails.
     */
    public Message send(Message message) throws RemoteException {
        Message reply = null;
        //already disconnected.
        if (disconnected_nodes.containsKey(message.getSrc())) {
            return reply;
        }

        synchronized (lock) {
            int counter = rpc_counters.getOrDefault(message.getDest(), 0);
            rpc_counters.put(message.getDest(), counter + 1);
        }


        // try what to do with this message packet
        boolean successDeliver = makeDecision(message);
        if(successDeliver) {
            //we may also customize this into broadcast.
            Node n = nodes.get(message.getDest());
            if (n != null) {
                reply = n.rc.deliverMessage(message);
            }
        }
        return reply;
    }

    /**
     * disconnect - Set a node to be disconnected.
     * @param i the node ID.
     */
    public void disconnect(int i) {

        if( disconnected_nodes.containsKey(i) ) { /* Already disconnected. */
            return;
        }

        disconnected_nodes.put(i, nodes.get(i));
        nodes.remove(i);
    }

    /**
     * re_connect - Reconnect a node if it is disconnected, otherwise nothing happens.
     * @param i the node ID.
     */
    public void re_connect(int i) {
        if(disconnected_nodes.containsKey(i)) {
            nodes.put(i, disconnected_nodes.get(i));
            disconnected_nodes.remove(i);
        }
    }

    public int getRPCCount(int node) {
        synchronized (lock) {
            return rpc_counters.getOrDefault(node, 0);
        }
    }

    /**
     * makeDecision - Make decision about how to deal with a incoming packet,
     * which maybe send without delay, short delay or drop.
     *
     * @param message the incoming packet.
     */
    private boolean makeDecision(Message message){
        int dst = message.getDest(), src = message.getSrc();
        // if the end is crashed or there is no such dst end, simulate no
        // reply and eventual timeout.
        if ((!nodes.containsKey(dst)) || (nodes.get(dst) == null)) {
            return false;
        }

        Random rand = new Random();
        try {
            if(!reliable) {
                // short delay
                int ms = rand.nextInt() % 27;
                Thread.sleep(ms);

                int randInt = rand.nextInt() % 1000;
                // drop the request, return as if timeout
                if(randInt < 100) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * getState - This is the remote call for getting the node state.
     *
     * @param nodeID the node ID.
     * @return the state information packet
     */
    public GetStateReply getState( int nodeID ) {
        //System.out.println("Controller asks to the client " + nodeID + "for state");
        GetStateReply reply;

        try {
            Node n = nodes.get(nodeID);
            if (n != null) {
                reply = n.rc.getState();
                return reply;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit( -1 );
        }

        return null;
    }

    /**
     * start - Start agreement on a new log entry.
     *
     * @param nodeID the node ID.
     * @param cmd the command to append.
     * @return status information about this agreement.
     */
    public StartReply start( int nodeID, int cmd ) {
        StartReply reply;

        try {
            Node n = nodes.get(nodeID);
            if (n != null) {
                reply = n.rc.start(cmd);
                return reply;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit( -1 );
        }
        return null;
    }

    /**
     * applyChannel - add a ApplyMsg when a entry is committed for testing.
     *
     * @param msg the apply message.
     */
    public void applyChannel(ApplyMsg msg) {

        if( this.applyMsgMap.containsKey( msg.nodeID) ) {

            BlockingQueue<ApplyMsg> applyMsgsQ = this.applyMsgMap.get( msg.nodeID ) ;

            try {
                applyMsgsQ.put(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * setApplyMsgMap - setter of the controller channel map.
     *
     * @param applyMsgsMap the node to channel map.
     */
    public void setApplyMsgMap( Map<Integer, BlockingQueue<ApplyMsg>> applyMsgsMap ) {
        this.applyMsgMap = applyMsgsMap;
    }

    /**
     * getNumRegistered - return the currently registered nodes.
     *
     * @return the number of registered nodes.
     */
    public int getNumRegistered() {
        return this.nodes.size();
    }

    /**
     * Node - Class to represent a raft node from the serer point of view.
     */
    private static class Node {
        int id;
        RemoteControllerIntf rc;
        Node(int id, RemoteControllerIntf rc) {
            this.id = id;
            this.rc = rc;
        }
    }
}
