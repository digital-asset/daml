// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.api.util

import io.grpc.{Status, StatusException, StatusRuntimeException}

object GrpcStatusError {

  def unapply(t: Throwable): Option[Status] = t match {
    case r: StatusRuntimeException => Some(r.getStatus)
    case e: StatusException => Some(e.getStatus)
    case _ => None
  }
}
