// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcRequestRefusedByServer

/** Synchronous error returned by a sequencer. */
sealed trait SendAsyncError extends PrettyPrinting {

  val message: String

  override protected def pretty: Pretty[SendAsyncError] = prettyOfClass(
    unnamedParam(_.message.unquoted)
  )

  /** The Sequencer is overloaded and declined to handle the request */
  def isOverload: Boolean
}

object SendAsyncError {

  /** Implementation of [[SendAsyncError]]s for gRPC transports */
  final case class SendAsyncErrorGrpc(error: GrpcError) extends SendAsyncError {
    override val message: String = error.toString

    override def isOverload: Boolean = error match {
      case _: GrpcRequestRefusedByServer =>
        error.decodedCantonError.exists { decoded =>
          decoded.code.id == SequencerErrors.Overloaded.id
        }
      case _ => false
    }
  }

  /** Implementation of [[SendAsyncError]]s for direct transports */
  final case class SendAsyncErrorDirect(override val message: String) extends SendAsyncError {
    override def isOverload: Boolean = false
  }
}
