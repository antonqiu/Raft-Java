package lib;

import java.util.ArrayList;
import java.util.List;

public class Log {
  private ArrayList<LogEntry> log;

  public Log() {
    this.log = new ArrayList<>();
  }

  public int getSize() {
    return log.size();
  }

  public LogEntry getLogEntryByIndex(int index) {
    try {
      return log.get(index);
    } catch (ArrayIndexOutOfBoundsException e) {
      return null;
    }
  }

  // not end inclusive
  public LogEntry[] getLogEntriesInRange(int startIdx, int endIdx) {
    return log.subList(startIdx, endIdx).toArray(new LogEntry[0]);
  }

  public int getTermForIndex (int index) {
    if (index < 0) {
      return 0;
    }
    return log.get(index).getTerm();
  }
}
