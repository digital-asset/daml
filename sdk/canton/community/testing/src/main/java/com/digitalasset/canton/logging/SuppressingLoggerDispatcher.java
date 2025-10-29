// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.SubstituteLogger;
import scala.runtime.BoxedUnit;

import static org.slf4j.event.Level.*;

class SuppressingLoggerDispatcher extends SubstituteLogger {

  private final Logger suppressedMessageLogger;
  private final SuppressingLogger.ActiveState activeSuppressionState;
  private final String suppressionPrefix;

  SuppressingLoggerDispatcher(
      String name,
      Logger suppressedMessageLogger,
      SuppressingLogger.ActiveState activeSuppressionState,
      String suppressionPrefix) {
    // because we know we won't log anything before setting the delegate there's no need to set an
    // event queue hence the null
    super(name, null, true);
    this.suppressedMessageLogger = suppressedMessageLogger;
    this.activeSuppressionState = activeSuppressionState;
    this.suppressionPrefix = suppressionPrefix;
  }

  @Override
  public void error(String msg) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(withSuppressionHint(ERROR, msg));
          suppressedMessageLogger.error(msg);
        },
        () -> super.error(msg));
  }

  @Override
  public void error(String format, Object arg) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(withSuppressionHint(ERROR, format), arg);
          suppressedMessageLogger.error(format, arg);
        },
        () -> super.error(format, arg));
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(withSuppressionHint(ERROR, format), arg1, arg2);
          suppressedMessageLogger.error(format, arg1, arg2);
        },
        () -> super.error(format, arg1, arg2));
  }

  @Override
  public void error(String format, Object... arguments) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(withSuppressionHint(ERROR, format), arguments);
          suppressedMessageLogger.error(format, arguments);
        },
        () -> super.error(format, arguments));
  }

  @Override
  public void error(String msg, Throwable t) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(withSuppressionHint(ERROR, msg), t);
          suppressedMessageLogger.error(msg, t);
        },
        () -> super.error(msg, t));
  }

  @Override
  public void error(Marker marker, String msg) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(marker, withSuppressionHint(ERROR, msg));
          suppressedMessageLogger.error(marker, msg);
        },
        () -> super.error(marker, msg));
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(marker, withSuppressionHint(ERROR, format), arg);
          suppressedMessageLogger.error(marker, format, arg);
        },
        () -> super.error(marker, format, arg));
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(marker, withSuppressionHint(ERROR, format), arg1, arg2);
          suppressedMessageLogger.error(marker, format, arg1, arg2);
        },
        () -> super.error(marker, format, arg1, arg2));
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(marker, withSuppressionHint(ERROR, format), arguments);
          suppressedMessageLogger.error(marker, format, arguments);
        },
        () -> super.error(marker, format, arguments));
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    ifSuppressed(
        ERROR,
        () -> {
          super.info(marker, withSuppressionHint(ERROR, msg), t);
          suppressedMessageLogger.error(marker, msg, t);
        },
        () -> super.error(marker, msg, t));
  }

  @Override
  public void warn(String msg) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(withSuppressionHint(WARN, msg));
          suppressedMessageLogger.warn(msg);
        },
        () -> super.warn(msg));
  }

  @Override
  public void warn(String format, Object arg) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(withSuppressionHint(WARN, format), arg);
          suppressedMessageLogger.warn(format, arg);
        },
        () -> super.warn(format, arg));
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(withSuppressionHint(WARN, format), arg1, arg2);
          suppressedMessageLogger.warn(format, arg1, arg2);
        },
        () -> super.warn(format, arg1, arg2));
  }

  @Override
  public void warn(String format, Object... arguments) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(withSuppressionHint(WARN, format), arguments);
          suppressedMessageLogger.warn(format, arguments);
        },
        () -> super.warn(format, arguments));
  }

  @Override
  public void warn(String msg, Throwable t) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(withSuppressionHint(WARN, msg), t);
          suppressedMessageLogger.warn(msg, t);
        },
        () -> super.warn(msg, t));
  }

  @Override
  public void warn(Marker marker, String msg) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(marker, withSuppressionHint(WARN, msg));
          suppressedMessageLogger.warn(marker, msg);
        },
        () -> super.warn(marker, msg));
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(marker, withSuppressionHint(WARN, format), arg);
          suppressedMessageLogger.warn(marker, format, arg);
        },
        () -> super.warn(marker, format, arg));
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(marker, withSuppressionHint(WARN, format), arg1, arg2);
          suppressedMessageLogger.warn(marker, format, arg1, arg2);
        },
        () -> super.warn(marker, format, arg1, arg2));
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(marker, withSuppressionHint(WARN, format), arguments);
          suppressedMessageLogger.warn(marker, format, arguments);
        },
        () -> super.warn(marker, format, arguments));
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    ifSuppressed(
        WARN,
        () -> {
          super.info(marker, withSuppressionHint(WARN, msg), t);
          suppressedMessageLogger.warn(marker, msg, t);
        },
        () -> super.warn(marker, msg, t));
  }

  @Override
  public void info(String msg) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(withSuppressionHint(INFO, msg));
          suppressedMessageLogger.info(msg);
        },
        () -> super.info(msg));
  }

  @Override
  public void info(String format, Object arg) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(withSuppressionHint(INFO, format), arg);
          suppressedMessageLogger.info(format, arg);
        },
        () -> super.info(format, arg));
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(withSuppressionHint(INFO, format), arg1, arg2);
          suppressedMessageLogger.info(format, arg1, arg2);
        },
        () -> super.info(format, arg1, arg2));
  }

  @Override
  public void info(String format, Object... arguments) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(withSuppressionHint(INFO, format), arguments);
          suppressedMessageLogger.info(format, arguments);
        },
        () -> super.info(format, arguments));
  }

  @Override
  public void info(String msg, Throwable t) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(withSuppressionHint(INFO, msg), t);
          suppressedMessageLogger.info(msg, t);
        },
        () -> super.info(msg, t));
  }

  @Override
  public void info(Marker marker, String msg) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(marker, withSuppressionHint(INFO, msg));
          suppressedMessageLogger.info(marker, msg);
        },
        () -> super.info(marker, msg));
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(marker, withSuppressionHint(INFO, format), arg);
          suppressedMessageLogger.info(marker, format, arg);
        },
        () -> super.info(marker, format, arg));
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(marker, withSuppressionHint(INFO, format), arg1, arg2);
          suppressedMessageLogger.info(marker, format, arg1, arg2);
        },
        () -> super.info(marker, format, arg1, arg2));
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(marker, withSuppressionHint(INFO, format), arguments);
          suppressedMessageLogger.info(marker, format, arguments);
        },
        () -> super.info(marker, format, arguments));
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    ifSuppressed(
        INFO,
        () -> {
          super.info(marker, withSuppressionHint(DEBUG, msg), t);
          suppressedMessageLogger.info(marker, msg, t);
        },
        () -> super.info(marker, msg, t));
  }

  @Override
  public void debug(String msg) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(withSuppressionHint(DEBUG, msg));
          suppressedMessageLogger.debug(msg);
        },
        () -> super.debug(msg));
  }

  @Override
  public void debug(String format, Object arg) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(withSuppressionHint(DEBUG, format), arg);
          suppressedMessageLogger.debug(format, arg);
        },
        () -> super.debug(format, arg));
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(withSuppressionHint(DEBUG, format), arg1, arg2);
          suppressedMessageLogger.debug(format, arg1, arg2);
        },
        () -> super.debug(format, arg1, arg2));
  }

  @Override
  public void debug(String format, Object... arguments) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(withSuppressionHint(DEBUG, format), arguments);
          suppressedMessageLogger.debug(format, arguments);
        },
        () -> super.debug(format, arguments));
  }

  @Override
  public void debug(String msg, Throwable t) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(withSuppressionHint(DEBUG, msg), t);
          suppressedMessageLogger.debug(msg, t);
        },
        () -> super.debug(msg, t));
  }

  @Override
  public void debug(Marker marker, String msg) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(marker, withSuppressionHint(DEBUG, msg));
          suppressedMessageLogger.debug(marker, msg);
        },
        () -> super.debug(marker, msg));
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(marker, withSuppressionHint(DEBUG, format), arg);
          suppressedMessageLogger.debug(marker, format, arg);
        },
        () -> super.debug(marker, format, arg));
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(marker, withSuppressionHint(DEBUG, format), arg1, arg2);
          suppressedMessageLogger.debug(marker, format, arg1, arg2);
        },
        () -> super.debug(marker, format, arg1, arg2));
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(marker, withSuppressionHint(DEBUG, format), arguments);
          suppressedMessageLogger.debug(marker, format, arguments);
        },
        () -> super.debug(marker, format, arguments));
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    ifSuppressed(
        DEBUG,
        () -> {
          super.debug(marker, withSuppressionHint(DEBUG, msg), t);
          suppressedMessageLogger.debug(marker, msg, t);
        },
        () -> super.debug(marker, msg, t));
  }

  private void ifSuppressed(Level level, Runnable ifSuppressed, Runnable ifNotSuppressed) {
    activeSuppressionState.withSuppressionRule(
        rule -> {
          if (rule.isSuppressed(getName(), level)) {
            ifSuppressed.run();
          } else {
            ifNotSuppressed.run();
          }
          return BoxedUnit.UNIT;
        });
  }

  public String withSuppressionHint(Level level, String msg) {
    return String.format(suppressionPrefix, level, msg);
  }
}
