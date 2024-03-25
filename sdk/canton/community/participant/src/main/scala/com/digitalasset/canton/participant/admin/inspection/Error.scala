// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.topology.DomainId

sealed abstract class Error extends Product with Serializable with HasDomainId {}

private[admin] object Error {

  final case class TimestampAfterPrehead(
      override val domainId: DomainId,
      requestedTimestamp: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
  ) extends Error

  final case class TimestampBeforePruning(
      override val domainId: DomainId,
      requestedTimestamp: CantonTimestamp,
      prunedTimestamp: CantonTimestamp,
  ) extends Error

  final case class InconsistentSnapshot(
      override val domainId: DomainId,
      missingContract: LfContractId,
  ) extends Error

  final case class InvariantIssue(
      override val domainId: DomainId,
      contract: LfContractId,
      errorMessage: String,
  ) extends Error

  final case class SerializationIssue(
      override val domainId: DomainId,
      contract: LfContractId,
      errorMessage: String,
  ) extends Error

  final case class OffboardingParty(domainId: DomainId, error: String) extends Error
}
