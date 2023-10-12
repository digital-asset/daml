// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging;

import org.slf4j.event.Level;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;
import scala.Function1;

import java.util.Queue;

import static org.slf4j.event.Level.*;

/**
 * Logger that formats log messages then saves in a buffer.
 * Trace messages are currently ignored.
 * Java because of awkwardness overriding and using Java varargs methods from Scala.
 */
class BufferingLogger extends MarkerIgnoringBase {
    private final Queue<LogEntry> messages;
    private final Function1<LogEntry, Boolean> skip;

    public BufferingLogger(Queue<LogEntry> messages, String name, Function1<LogEntry, Boolean> skip) {
        this.messages = messages;
        this.name = name;
        this.skip = skip;
    }

    @Override public boolean isTraceEnabled() { return false; }
    @Override public void trace(String msg) {}
    @Override public void trace(String format, Object arg) {}
    @Override public void trace(String format, Object arg1, Object arg2) {}
    @Override public void trace(String format, Object... arguments) {}
    @Override public void trace(String msg, Throwable t) {}

    @Override public boolean isDebugEnabled() { return true; }
    @Override public void debug(String msg) { skipOrAdd(toLogEntry(DEBUG, msg)); }
    @Override public void debug(String format, Object arg) { skipOrAdd(toLogEntry(DEBUG, format, arg)); }
    @Override public void debug(String format, Object arg1, Object arg2) { skipOrAdd(toLogEntry(DEBUG, format, arg1, arg2)); }
    @Override public void debug(String format, Object... arguments) { skipOrAdd(toLogEntry(DEBUG, format, arguments)); }
    @Override public void debug(String msg, Throwable t) { skipOrAdd(toLogEntry(DEBUG, msg, t)); }

    @Override public boolean isInfoEnabled() { return true; }
    @Override public void info(String msg) { skipOrAdd(toLogEntry(INFO, msg)); }
    @Override public void info(String format, Object arg) { skipOrAdd(toLogEntry(INFO, format, arg)); }
    @Override public void info(String format, Object arg1, Object arg2) { skipOrAdd(toLogEntry(INFO, format, arg1, arg2)); }
    @Override public void info(String format, Object... arguments) { skipOrAdd(toLogEntry(INFO, format, arguments)); }
    @Override public void info(String msg, Throwable t) { skipOrAdd(toLogEntry(INFO, msg, t)); }

    @Override public boolean isWarnEnabled() { return true; }
    @Override public void warn(String msg) { skipOrAdd(toLogEntry(WARN, msg)); }
    @Override public void warn(String msg, Throwable t) { skipOrAdd(toLogEntry(WARN, msg, t)); }
    @Override public void warn(String format, Object arg) { skipOrAdd(toLogEntry(WARN, format, arg)); }
    @Override public void warn(String format, Object arg1, Object arg2) { skipOrAdd(toLogEntry(WARN, format, arg1, arg2)); }
    @Override public void warn(String format, Object... arguments) { skipOrAdd(toLogEntry(WARN, format, arguments)); }

    @Override public boolean isErrorEnabled() { return true; }
    @Override public void error(String msg) { skipOrAdd(toLogEntry(ERROR, msg)); }
    @Override public void error(String msg, Throwable t) { skipOrAdd(toLogEntry(ERROR, msg, t)); }
    @Override public void error(String format, Object arg) { skipOrAdd(toLogEntry(ERROR, format, arg)); }
    @Override public void error(String format, Object arg1, Object arg2) { skipOrAdd(toLogEntry(ERROR, format, arg1, arg2)); }
    @Override public void error(String format, Object... arguments) { skipOrAdd(toLogEntry(ERROR, format, arguments)); }

    private LogEntry toLogEntry(Level level, String msg) {
        return LogEntry$.MODULE$.apply(level, name, msg, (Throwable) null);
    }

    private LogEntry toLogEntry(Level level, String msg, Throwable t) {
        return LogEntry$.MODULE$.apply(level, name, msg, t);
    }

    private LogEntry toLogEntry(Level level, String format, Object arg) {
        return LogEntry$.MODULE$.apply(level, name, MessageFormatter.format(format, arg));
    }

    private LogEntry toLogEntry(Level level, String format, Object arg1, Object arg2) {
        return LogEntry$.MODULE$.apply(level, name, MessageFormatter.format(format, arg1, arg2));
    }

    private LogEntry toLogEntry(Level level, String format, Object[] args) {
        return LogEntry$.MODULE$.apply(level, name, MessageFormatter.arrayFormat(format, args));
    }

    private void skipOrAdd(LogEntry entry) {
        if (!skip.apply(entry)) { messages.add(entry); }
    }
}
