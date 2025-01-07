// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.service

import com.google.rpc.Status
import io.grpc.protobuf.StatusProto
import scalaz.syntax.std.boolean.*

import scala.util.control.NonFatal

private[canton] object Grpc {
  object StatusEnvelope {
    def unapply(t: Throwable): Option[Status] = t match {
      case NonFatal(t) =>
        val status = StatusProto fromThrowable t
        if (status == null) None
        else {
          // fromThrowable uses UNKNOWN if it didn't find one
          val code = com.google.rpc.Code.forNumber(status.getCode)
          if (code == null) None
          else (code != com.google.rpc.Code.UNKNOWN) option status
        }
      case _ => None
    }
  }
}
