// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import com.google.rpc.Status
import io.grpc.protobuf.StatusProto
import scalaz.syntax.std.boolean._

import scala.util.control.NonFatal

private[daml] object Grpc {
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
