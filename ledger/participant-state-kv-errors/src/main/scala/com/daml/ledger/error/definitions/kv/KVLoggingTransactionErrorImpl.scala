// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.error.definitions.kv

import com.daml.error.{ContextualizedErrorLogger, DamlErrorWithDefiniteAnswer, ErrorCode}
import com.google.rpc.status.Status

class KVLoggingTransactionErrorImpl(
    cause: String,
    throwable: Option[Throwable] = None,
    definiteAnswer: Boolean = false,
)(implicit
    code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends DamlErrorWithDefiniteAnswer(cause, throwable, definiteAnswer) {
  override def context: Map[String, String] = Map.empty

  final def asStatus: Status = rpcStatus()
}
