package lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Log {
  private ArrayList<LogEntry> log;

  public Log() {
    this.log = new ArrayList<>();
    this.log.add(null);
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
    if (index < 1) {
      return 0;
    }
    return log.get(index).getTerm();
  }

  public int getLatestTerm() {
    return this.getTermForIndex(getSize() - 1);
  }

  public int getLastestIndex() {
    return getSize() - 1;
  }

  public boolean appendAllLogEntries(int index, LogEntry[] entries) {
    return log.addAll(index, Arrays.asList(entries));
  }

  public boolean appendLogEntry(LogEntry entry) {
    return log.add(entry);
  }

  public void removeLogEntriesStartingAt(int startIdx) {
    log.subList(startIdx, getSize()).clear();
  }

  public boolean isUpToDate(int lastLogTerm, int lastLogIndex) {
    return lastLogTerm >= this.getLatestTerm() && lastLogIndex >= this.getLastestIndex();
  }
}
