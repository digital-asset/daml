// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.Port

object InProcessGrpcName {
  def forPort(port: Port): String = s"inprocess-grpc-${port.unwrap}"
}
