// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

import com.google.rpc.Status
import io.grpc.StatusRuntimeException

trait RpcError {

  /** The error code, usually passed in as implicit where the error class is defined */
  def code: ErrorCode

  /** The context (declared fields) of this error */
  def context: Map[String, String]

  /** A human readable string indicating the error */
  def cause: String

  /** The resources related to this error */
  def resources: Seq[(ErrorResource, String)]

  /** The correlationId (e.g. submissionId) associated with the request that caused the error */
  def correlationId: Option[String]

  /** The traceId associated with the TraceContext at error creation */
  def traceId: Option[String]

  /** The gRPC status */
  def asGrpcStatus: Status

  /** The gRPC status encoded as a StatusRuntimeException */
  def asGrpcError: StatusRuntimeException

}

trait CantonRpcError extends RpcError
trait DamlRpcError extends RpcError
