package lib;

import java.io.Serializable;

/**
 * GetStateReply - This is a serializable packet wrapper for the getState
 * method of transportLayer.
 *
 * @author Sphoorti Joglekar, Priya Avhad, Yijia Cui, Zonglin Wang
 */
public class GetStateReply implements Serializable {
    /**
     * The current term number.
     */
    public int term;
    /**
     * The leader flag.
     */
    public boolean isLeader;

    private static final long serialVersionUID = 1L;

    /**
     * GetStateReply - Construct a get state packet with given term and leader.
     *
     * @param term the current term
     * @param isLeader The leader flag
     */
    public GetStateReply(int term, boolean isLeader) {
        this.term = term;
        this.isLeader = isLeader;
    }
}
