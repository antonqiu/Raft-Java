package lib;

import java.io.Serializable;

public class ApplyMsg implements Serializable{

    private static final long serialVersionUID = 1L;

    public int nodeID;
    public int index;
    public int command;
    public boolean useSnapshot;
    public byte[] snapshot;

    public ApplyMsg(int nodeID, int index, int command,
                            boolean useSnapshot, byte[]snapshot) {

        this.nodeID = nodeID;
        this.index = index;
        this.command = command;
        this.useSnapshot = useSnapshot;
        this.snapshot = snapshot;
    }
}
