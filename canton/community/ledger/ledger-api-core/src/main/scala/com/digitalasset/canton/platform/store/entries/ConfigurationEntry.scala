// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.entries

import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.configuration.Configuration

private[platform] sealed abstract class ConfigurationEntry extends Product with Serializable {
  def toDomain: domain.ConfigurationEntry
}

private[platform] object ConfigurationEntry {

  final case class Accepted(submissionId: String, configuration: Configuration)
      extends ConfigurationEntry {
    override def toDomain: domain.ConfigurationEntry =
      domain.ConfigurationEntry.Accepted(submissionId, configuration)
  }

  final case class Rejected(
      submissionId: String,
      rejectionReason: String,
      proposedConfiguration: Configuration,
  ) extends ConfigurationEntry {
    override def toDomain: domain.ConfigurationEntry =
      domain.ConfigurationEntry.Rejected(submissionId, rejectionReason, proposedConfiguration)
  }

}
