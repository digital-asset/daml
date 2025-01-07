// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

sealed trait KmsDriverHealth extends Product with Serializable

object KmsDriverHealth {

  /** The driver is healthy. */
  case object Ok extends KmsDriverHealth

  /** The state of the KMS Driver is degraded but still functional. */
  final case class Degraded(reason: String) extends KmsDriverHealth

  /** The driver has failed and is not functional, but may recover. */
  final case class Failed(reason: String) extends KmsDriverHealth

  /** The driver is in a fatal state and will not recover. */
  final case class Fatal(reason: String) extends KmsDriverHealth
}
