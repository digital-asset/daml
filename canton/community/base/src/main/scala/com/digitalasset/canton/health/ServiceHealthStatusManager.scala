// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import io.grpc.protobuf.services.HealthStatusManager

/** Combines a [[io.grpc.protobuf.services.HealthStatusManager]] (exposed as a gRPC health service)
  * with the set of [[HealthService]]s it needs to report on.
  */
final case class ServiceHealthStatusManager(
    name: String,
    manager: HealthStatusManager,
    services: Set[HealthService],
)
