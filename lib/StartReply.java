package lib;

import java.io.Serializable;

public class StartReply implements Serializable{
    public int index;
    public int term;
    public boolean isLeader;
    private static final long serialVersionUID = 1L;
    public StartReply (int index, int term, boolean isLeader) {
        this.index = index;
        this.term = term;
        this.isLeader = isLeader;
    }
}
