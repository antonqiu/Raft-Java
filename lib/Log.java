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

  public synchronized int getSize() {
    return log.size();
  }

  public synchronized LogEntry getLogEntryByIndex(int index) {
    if (index < 1) {
      return null;
    }
    try {
      return log.get(index - 1);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  public synchronized ArrayList<LogEntry> getLogEntriesStartingAt(int startIdx) {
    if (startIdx > 0 && startIdx <= this.getSize()) {
      return new ArrayList<>(log.subList(startIdx - 1, this.getSize()));
    } else {
      return new ArrayList<>();
    }
  }

  public synchronized int getTermForIndex(int idx) {
    if (idx < 1 || idx > getSize()) {
      return 0;
    }
    return log.get(idx - 1).getTerm();
  }

  public synchronized int getLatestTerm() {
    return this.getTermForIndex(getSize());
  }

  public synchronized int getLatestIndex() {
    return getSize();
  }

  public synchronized boolean appendAllLogEntries(int index, List<LogEntry> entries) {
    return log.addAll(entries.subList(index, entries.size()));
  }

  public synchronized boolean appendLogEntry(LogEntry entry) {
    return log.add(entry);
  }

  public synchronized void removeLogEntriesStartingAt(int startIdx) {
    log.subList(startIdx - 1, getSize()).clear();
  }

  public synchronized boolean isUpToDate(int lastLogTerm, int lastLogIndex) {
    return lastLogTerm > this.getLatestTerm() || (lastLogTerm == this.getLatestTerm()
        && lastLogIndex >= this.getLatestIndex());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
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
