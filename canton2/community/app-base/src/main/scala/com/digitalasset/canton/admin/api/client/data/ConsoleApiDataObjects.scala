// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.participant.admin.{v0 as participantAdminV0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*

final case class ListConnectedDomainsResult(
    domainAlias: DomainAlias,
    domainId: DomainId,
    healthy: Boolean,
)

object ListConnectedDomainsResult {
  def fromProtoV0(
      value: participantAdminV0.ListConnectedDomainsResponse.Result
  ): ParsingResult[ListConnectedDomainsResult] = {
    val participantAdminV0.ListConnectedDomainsResponse.Result(domainAlias, domainId, healthy) =
      value
    for {
      domainId <- DomainId.fromProtoPrimitive(domainId, "domainId")
      domainAlias <- DomainAlias.fromProtoPrimitive(domainAlias)

    } yield ListConnectedDomainsResult(
      domainAlias = domainAlias,
      domainId = domainId,
      healthy = healthy,
    )

  }
}

final case class DarMetadata(
    name: String,
    main: String,
    packages: Seq[String],
    dependencies: Seq[String],
)

object DarMetadata {
  def fromProtoV0(
      value: participantAdminV0.ListDarContentsResponse
  ): ParsingResult[DarMetadata] = {
    val participantAdminV0.ListDarContentsResponse(description, main, packages, dependencies) =
      value
    Right(DarMetadata(description, main, packages, dependencies))
  }

}
