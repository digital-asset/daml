// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import io.grpc.{Metadata, Status, StatusException, StatusRuntimeException}

object GrpcException {

  def unapply(arg: Throwable): Option[(Status, Metadata)] =
    arg match {
      case e: StatusRuntimeException => Some((e.getStatus, e.getTrailers))
      case e: StatusException => Some((e.getStatus, e.getTrailers))
      case _ => None
    }

}
