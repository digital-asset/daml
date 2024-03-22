// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import net.logstash.logback.encoder.LogstashEncoder;

/** canton specific json log encoding
  *
  * equivalent of:
  * <encoder class="net.logstash.logback.encoder.LogstashEncoder">
  *      <excludeMdcKeyName>err-context</excludeMdcKeyName>
  *      <shortenedLoggerNameLength>30</shortenedLoggerNameLength>
  * </encoder>
  */
class CantonJsonEncoder extends LogstashEncoder {
  addExcludeMdcKeyName("err-context")
  // Set by default here, as Canton logger names are quite long by default.
  setShortenedLoggerNameLength(30)
}
