// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common.logging

import org.slf4j.{Logger, LoggerFactory}

private[logging] final class SimpleNamedLoggerFactory(val name: String) extends NamedLoggerFactory {
  override protected def getLogger(fullName: String): Logger =
    LoggerFactory.getLogger(fullName)
}
