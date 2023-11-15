// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.util.control.NoStackTrace

case object TimeoutException extends RuntimeException with NoStackTrace {
  override val getMessage: String = s"Future could not be completed before timeout"
}
