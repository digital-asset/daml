// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.errors

import com.daml.error.definitions.LoggingTransactionErrorImpl
import com.daml.error.{ContextualizedErrorLogger, ErrorCode}
import com.daml.grpc.GrpcStatus
import com.google.rpc.status.Status

class KVLoggingTransactionErrorImpl(
    cause: String,
    throwable: Option[Throwable] = None,
)(implicit
    code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends LoggingTransactionErrorImpl(cause, throwable) {
  override def context: Map[String, String] = Map.empty

  final def asStatus: Status = GrpcStatus.toProto(asGrpcStatusFromContext)
}
