// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.DomainId

/** Used to bootstrap one or more domains at the start of a test.
  */
trait NetworkBootstrap {
  def bootstrap(): Unit

  /** bootstrap and manually start nodes */
  def bootstrapAndManuallyStartNodes(): Unit
}

/** A data container to hold useful information for initialized domains
  */
final case class InitializedDomain(
    domainId: DomainId,
    staticDomainParameters: StaticDomainParameters,
    domainOwners: Set[InstanceReference],
)
