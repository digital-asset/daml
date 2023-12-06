// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

/** Result of a health check */
sealed trait HealthCheckResult

/** Everything that the check checks is healthy */
object Healthy extends HealthCheckResult

/** The check deems something unhealthy
  * @param message User printable message describing why a unhealthy result was given
  */
final case class Unhealthy(message: String) extends HealthCheckResult
