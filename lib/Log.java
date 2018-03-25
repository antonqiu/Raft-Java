package lib;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

public class Log {
  private static final Logger LOGGER = Logger.getLogger(Log.class.getName());
  private CopyOnWriteArrayList<LogEntry> log;

  public Log() {
    this.log = new CopyOnWriteArrayList<LogEntry>();
  }

  public int getSize() {
    return log.size();
  }

  public LogEntry getLogEntryByIndex(int index) {
    if (index < 1) {
      return null;
    }
    try {
      return log.get(index - 1);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  // not end inclusive
  public ArrayList<LogEntry> getLogEntriesInRange(int startIdx, int endIdx) {
    return new ArrayList<>(log.subList(startIdx-1, endIdx-1));
  }

  public int getTermForIndex (int idx) {
    if (idx < 1 || idx > getSize()) {
      return 0;
    }
    return log.get(idx - 1).getTerm();
  }

  public int getLatestTerm() {
    return this.getTermForIndex(getSize());
  }

  public int getLatestIndex() {
    return getSize();
  }

  public boolean appendAllLogEntries(int index, List<LogEntry> entries) {
    return log.addAll(entries.subList(index, entries.size()));
  }

  public boolean appendLogEntry(LogEntry entry) {
    return log.add(entry);
  }

  public void removeLogEntriesStartingAt(int startIdx) {
    log.subList(startIdx - 1, getSize()).clear();
  }

  public boolean isUpToDate(int lastLogTerm, int lastLogIndex) {
    return lastLogTerm >= this.getLatestTerm() && lastLogIndex >= this.getLatestIndex();
  }

  @Override
  public String toString() {
    StringBuilder sb  = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < log.size(); i++) {
      LogEntry entry = log.get(i);
      if (entry == null) {
        sb.append("(NA, NA),");
      } else {
        sb.append(entry.toString()).append(',');
      }
    }
    return sb.append(']').toString();
  }
}
