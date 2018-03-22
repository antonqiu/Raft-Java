package lib;

import java.io.Serializable;

public class LogEntry implements Serializable {

  private int term;
  private int index;
  private int command;

  public LogEntry(int term, int index, int command) {
    this.term = term;
    this.index = index;
    this.command = command;
  }

  public int getTerm() {
    return term;
  }

  public void setTerm(int term) {
    this.term = term;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public int getCommand() {
    return command;
  }

  public void setCommand(int command) {
    this.command = command;
  }
}
