// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

sealed trait KmsDriverHealth extends Product with Serializable

object KmsDriverHealth {

  /** The driver is healthy. */
  case object Ok extends KmsDriverHealth

  /** The driver's state is degraded but still functional (e.g., increased latency). The node's
    * crypto component will report as degraded, although the node's APIs will continue to serve
    * requests.
    */
  final case class Degraded(reason: String) extends KmsDriverHealth

  /** The driver has failed and is currently non-functional, but may recover without a restart.
    * While in this failing state, the node's APIs will be marked as not serving, but the liveness
    * probe will continue to succeed and will not trigger a restart.
    */
  final case class Failed(reason: String) extends KmsDriverHealth

  /** The driver is in a fatal and irrecoverable state, requiring the entire node to be restarted.
    * The [[Fatal]] health status will propagate to the node's liveness probe, causing it to fail
    * and trigger a restart via Kubernetes.
    */
  final case class Fatal(reason: String) extends KmsDriverHealth
}
