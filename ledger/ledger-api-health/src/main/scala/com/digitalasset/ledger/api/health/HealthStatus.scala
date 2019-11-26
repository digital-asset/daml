// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

sealed trait HealthStatus

case object Healthy extends HealthStatus

case object Unhealthy extends HealthStatus
