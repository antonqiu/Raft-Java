package lib;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
/**
 * This Interface is to define the remote callback functions for the raft node.
 * This is just a stub layer and you do not need to take care of this interface,
 * these APIs will eventually call your implementations of MessageHandling
 * interface in your raft node.
 *
 * @author Sphoorti Joglekar, Priya Avhad, Yijia Cui, Zonglin Wang
 */
public interface RemoteControllerIntf extends Remote {
    public Message deliverMessage(Message message) throws RemoteException;
    public GetStateReply getState() throws RemoteException;
    public StartReply start(int command) throws RemoteException;
}
