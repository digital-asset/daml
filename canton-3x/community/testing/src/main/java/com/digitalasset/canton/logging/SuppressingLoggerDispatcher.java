// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.SubstituteLogger;

import java.util.concurrent.atomic.AtomicReference;

import static org.slf4j.event.Level.*;

class SuppressingLoggerDispatcher extends SubstituteLogger {

  private Logger suppressedMessageLogger;
  private AtomicReference<SuppressionRule> activeSuppressionRule;
  private String suppressionPrefix;

  SuppressingLoggerDispatcher(
      String name,
      Logger suppressedMessageLogger,
      AtomicReference<SuppressionRule> activeSuppressionRule,
      String suppressionPrefix) {
    // because we know we won't log anything before setting the delegate there's no need to set an
    // event queue hence the null
    super(name, null, true);
    this.suppressedMessageLogger = suppressedMessageLogger;
    this.activeSuppressionRule = activeSuppressionRule;
    this.suppressionPrefix = suppressionPrefix;
  }

  @Override
  public void error(String msg) {
    if (isSuppressed(ERROR)) {
      super.info(withSuppressionHint(ERROR, msg));
      suppressedMessageLogger.error(msg);
    } else {
      super.error(msg);
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (isSuppressed(ERROR)) {
      super.info(withSuppressionHint(ERROR, format), arg);
      suppressedMessageLogger.error(format, arg);
    } else {
      super.error(format, arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (isSuppressed(ERROR)) {
      super.info(withSuppressionHint(ERROR, format), arg1, arg2);
      suppressedMessageLogger.error(format, arg1, arg2);
    } else {
      super.error(format, arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (isSuppressed(ERROR)) {
      super.info(withSuppressionHint(ERROR, format), arguments);
      suppressedMessageLogger.error(format, arguments);
    } else {
      super.error(format, arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (isSuppressed(ERROR)) {
      super.info(withSuppressionHint(ERROR, msg), t);
      suppressedMessageLogger.error(msg, t);
    } else {
      super.error(msg, t);
    }
  }

  @Override
  public void error(Marker marker, String msg) {
    if (isSuppressed(ERROR)) {
      super.info(marker, withSuppressionHint(ERROR, msg));
      suppressedMessageLogger.error(marker, msg);
    } else {
      super.error(marker, msg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (isSuppressed(ERROR)) {
      super.info(marker, withSuppressionHint(ERROR, format), arg);
      suppressedMessageLogger.error(marker, format, arg);
    } else {
      super.error(marker, format, arg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (isSuppressed(ERROR)) {
      super.info(marker, withSuppressionHint(ERROR, format), arg1, arg2);
      suppressedMessageLogger.error(marker, format, arg1, arg2);
    } else {
      super.error(marker, format, arg1, arg2);
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (isSuppressed(ERROR)) {
      super.info(marker, withSuppressionHint(ERROR, format), arguments);
      suppressedMessageLogger.error(marker, format, arguments);
    } else {
      super.error(marker, format, arguments);
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (isSuppressed(ERROR)) {
      super.info(marker, withSuppressionHint(ERROR, msg), t);
      suppressedMessageLogger.error(marker, msg, t);
    } else {
      super.error(marker, msg, t);
    }
  }

  @Override
  public void warn(String msg) {
    if (isSuppressed(WARN)) {
      super.info(withSuppressionHint(WARN, msg));
      suppressedMessageLogger.warn(msg);
    } else {
      super.warn(msg);
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (isSuppressed(WARN)) {
      super.info(withSuppressionHint(WARN, format), arg);
      suppressedMessageLogger.warn(format, arg);
    } else {
      super.warn(format, arg);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (isSuppressed(WARN)) {
      super.info(withSuppressionHint(WARN, format), arg1, arg2);
      suppressedMessageLogger.warn(format, arg1, arg2);
    } else {
      super.warn(format, arg1, arg2);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (isSuppressed(WARN)) {
      super.info(withSuppressionHint(WARN, format), arguments);
      suppressedMessageLogger.warn(format, arguments);
    } else {
      super.warn(format, arguments);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (isSuppressed(WARN)) {
      super.info(withSuppressionHint(WARN, msg), t);
      suppressedMessageLogger.warn(msg, t);
    } else {
      super.warn(msg, t);
    }
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (isSuppressed(WARN)) {
      super.info(marker, withSuppressionHint(WARN, msg));
      suppressedMessageLogger.warn(marker, msg);
    } else {
      super.warn(marker, msg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (isSuppressed(WARN)) {
      super.info(marker, withSuppressionHint(WARN, format), arg);
      suppressedMessageLogger.warn(marker, format, arg);
    } else {
      super.warn(marker, format, arg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (isSuppressed(WARN)) {
      super.info(marker, withSuppressionHint(WARN, format), arg1, arg2);
      suppressedMessageLogger.warn(marker, format, arg1, arg2);
    } else {
      super.warn(marker, format, arg1, arg2);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (isSuppressed(WARN)) {
      super.info(marker, withSuppressionHint(WARN, format), arguments);
      suppressedMessageLogger.warn(marker, format, arguments);
    } else {
      super.warn(marker, format, arguments);
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (isSuppressed(WARN)) {
      super.info(marker, withSuppressionHint(WARN, msg), t);
      suppressedMessageLogger.warn(marker, msg, t);
    } else {
      super.warn(marker, msg, t);
    }
  }

  @Override
  public void info(String msg) {
    if (isSuppressed(INFO)) {
      super.info(withSuppressionHint(INFO, msg));
      suppressedMessageLogger.info(msg);
    } else {
      super.info(msg);
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (isSuppressed(INFO)) {
      super.info(withSuppressionHint(INFO, format), arg);
      suppressedMessageLogger.info(format, arg);
    } else {
      super.info(format, arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (isSuppressed(INFO)) {
      super.info(withSuppressionHint(INFO, format), arg1, arg2);
      suppressedMessageLogger.info(format, arg1, arg2);
    } else {
      super.info(format, arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (isSuppressed(INFO)) {
      super.info(withSuppressionHint(INFO, format), arguments);
      suppressedMessageLogger.info(format, arguments);
    } else {
      super.info(format, arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (isSuppressed(INFO)) {
      super.info(withSuppressionHint(INFO, msg), t);
      suppressedMessageLogger.info(msg, t);
    } else {
      super.info(msg, t);
    }
  }

  @Override
  public void info(Marker marker, String msg) {
    if (isSuppressed(INFO)) {
      super.info(marker, withSuppressionHint(INFO, msg));
      suppressedMessageLogger.info(marker, msg);
    } else {
      super.info(marker, msg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (isSuppressed(INFO)) {
      super.info(marker, withSuppressionHint(INFO, format), arg);
      suppressedMessageLogger.info(marker, format, arg);
    } else {
      super.info(marker, format, arg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (isSuppressed(INFO)) {
      super.info(marker, withSuppressionHint(INFO, format), arg1, arg2);
      suppressedMessageLogger.info(marker, format, arg1, arg2);
    } else {
      super.info(marker, format, arg1, arg2);
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (isSuppressed(INFO)) {
      super.info(marker, withSuppressionHint(INFO, format), arguments);
      suppressedMessageLogger.info(marker, format, arguments);
    } else {
      super.info(marker, format, arguments);
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (isSuppressed(INFO)) {
      super.info(marker, withSuppressionHint(DEBUG, msg), t);
      suppressedMessageLogger.info(marker, msg, t);
    } else {
      super.info(marker, msg, t);
    }
  }

  @Override
  public void debug(String msg) {
    if (isSuppressed(DEBUG)) {
      super.debug(withSuppressionHint(DEBUG, msg));
      suppressedMessageLogger.debug(msg);
    } else {
      super.debug(msg);
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (isSuppressed(DEBUG)) {
      super.debug(withSuppressionHint(DEBUG, format), arg);
      suppressedMessageLogger.debug(format, arg);
    } else {
      super.debug(format, arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (isSuppressed(DEBUG)) {
      super.debug(withSuppressionHint(DEBUG, format), arg1, arg2);
      suppressedMessageLogger.debug(format, arg1, arg2);
    } else {
      super.debug(format, arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (isSuppressed(DEBUG)) {
      super.debug(withSuppressionHint(DEBUG, format), arguments);
      suppressedMessageLogger.debug(format, arguments);
    } else {
      super.debug(format, arguments);
    }
  }

  @Override
  public void debug(String msg, Throwable t) {
    if (isSuppressed(DEBUG)) {
      super.debug(withSuppressionHint(DEBUG, msg), t);
      suppressedMessageLogger.debug(msg, t);
    } else {
      super.debug(msg, t);
    }
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (isSuppressed(DEBUG)) {
      super.debug(marker, withSuppressionHint(DEBUG, msg));
      suppressedMessageLogger.debug(marker, msg);
    } else {
      super.debug(marker, msg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (isSuppressed(DEBUG)) {
      super.debug(marker, withSuppressionHint(DEBUG, format), arg);
      suppressedMessageLogger.debug(marker, format, arg);
    } else {
      super.debug(marker, format, arg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (isSuppressed(DEBUG)) {
      super.debug(marker, withSuppressionHint(DEBUG, format), arg1, arg2);
      suppressedMessageLogger.debug(marker, format, arg1, arg2);
    } else {
      super.debug(marker, format, arg1, arg2);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (isSuppressed(DEBUG)) {
      super.debug(marker, withSuppressionHint(DEBUG, format), arguments);
      suppressedMessageLogger.debug(marker, format, arguments);
    } else {
      super.debug(marker, format, arguments);
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (isSuppressed(DEBUG)) {
      super.debug(marker, withSuppressionHint(DEBUG, msg), t);
      suppressedMessageLogger.debug(marker, msg, t);
    } else {
      super.debug(marker, msg, t);
    }
  }

  private boolean isSuppressed(Level level) {
    return activeSuppressionRule.get().isSuppressed(getName(), level);
  }

  public String withSuppressionHint(Level level, String msg) {
    return String.format(suppressionPrefix, level, msg);
  }
}
