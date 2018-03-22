package lib;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MessagingLayer extends Remote {
    public void register(int id, RemoteControllerIntf remoteController) throws RemoteException;
    public Message send(Message message) throws RemoteException;
    public void applyChannel(ApplyMsg msg) throws RemoteException;
}
