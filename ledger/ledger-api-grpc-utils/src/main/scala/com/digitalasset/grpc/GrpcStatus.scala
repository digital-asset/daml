// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import io.grpc.Status

object GrpcStatus {

  def unapply(arg: Status): Option[(Status.Code, Option[String])] =
    Some((arg.getCode, Option(arg.getDescription)))

}
