package lib;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
/**
 * This class is the implementation of the RemoteControllerIntf. Again, this is
 * just a stub layer and you do not need to take care of this class,
 * these APIs will eventually call your implementations of MessageHandling
 * interface in your raft node.
 *
 * @author Sphoorti Joglekar, Priya Avhad, Yijia Cui, Zonglin Wang
 */
public class RemoteController extends UnicastRemoteObject implements RemoteControllerIntf{
    public final MessageHandling message_callback;

    private static final long serialVersionUID = 1L;

    public RemoteController(MessageHandling mh) throws RemoteException {
        this.message_callback = mh;
    }

    public Message deliverMessage(Message message) throws RemoteException {
       return message_callback.deliverMessage(message);
    }

    public GetStateReply getState() throws RemoteException{
        return message_callback.getState();
    }


    public StartReply start(int command) throws RemoteException {
        return message_callback.start(command);
    }
}
