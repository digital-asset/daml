// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common.logging

import org.slf4j.{Logger, LoggerFactory}

// Named Logger Factory implementation
private[logging] final class SimpleNamedLoggerFactory(val name: String) extends NamedLoggerFactory {
  override def append(subName: String): NamedLoggerFactory =
    if (name.isEmpty) new SimpleNamedLoggerFactory(subName)
    else new SimpleNamedLoggerFactory(s"$name/$subName")

  override protected def getLogger(fullName: String): Logger =
    LoggerFactory.getLogger(fullName)
}
