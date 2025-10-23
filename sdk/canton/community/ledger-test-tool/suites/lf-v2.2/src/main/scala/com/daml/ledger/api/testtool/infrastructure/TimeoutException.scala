// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import scala.util.control.NoStackTrace

case object TimeoutException extends RuntimeException with NoStackTrace {
  override val getMessage: String = s"Future could not be completed before timeout"
}
