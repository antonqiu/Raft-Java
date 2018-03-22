package lib;

import java.rmi.Naming;
import java.rmi.RemoteException;

/**
 * This class is for a raft node to communicate with other nodes. All the
 * communication among raft nodes should use this class to complete.
 *
 * @author Sphoorti Joglekar, Priya Avhad, Yijia Cui, Zonglin Wang
 */
public class TransportLib {
    /**
     * The ID of this node.
     */
    private int id;
    /**
     * The underlayer network.
     */
    private MessagingLayer ms;
    /**
     * The remote interface to register callback for the node.
     */
    private RemoteControllerIntf remoteController;

    /**
     * Constructor the TransportLib for a node.
     *
     * @param port             the port of server
     * @param id               the id of this node
     * @param messagleHandling the instance of the node
     */
    public TransportLib(int port, int id, MessageHandling messagleHandling) {
        try {
            this.remoteController = new RemoteController(messagleHandling);
            ms = (MessagingLayer) Naming.lookup("rmi://localhost:" + port + "/MessageServer");
            ms.register(id, remoteController);
        } catch (Exception e) {
            System.out.println(port);
            e.printStackTrace();
            System.exit(-1);
        }
        this.id = id;
    }

    /**
     * Send message through message server(underlayer network), this function is
     * a synchronous call which means the thread will be blocked until the
     * response coming back, or timeout.
     * Always use this API to send a message.
     *
     * @param message the message to send.
     * @return On successfully sending, returns the respond. On sending failure
     * like target node crashed, return null, be sure sure sure to check the
     * result!
     *
     * @throws RemoteException when RMI failed
     */
    public Message sendMessage(Message message) throws RemoteException {
        return ms.send(message);
    }

    /**
     * Apply an ApplyMsg to the framework for testing, please call this function
     * whenever you finish an agreement. Refer to ApplyMsg class for more
     * details.
     *
     * @param msg the msg to apply.
     * @throws RemoteException when RMI failed
     */
    public void applyChannel(ApplyMsg msg) throws RemoteException {
        ms.applyChannel(msg);
    }
}