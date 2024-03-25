// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.admin.participant.v30 as participantAdminV30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*

final case class ListConnectedDomainsResult(
    domainAlias: DomainAlias,
    domainId: DomainId,
    healthy: Boolean,
)

object ListConnectedDomainsResult {

  def fromProtoV30(
      value: participantAdminV30.ListConnectedDomainsResponse.Result
  ): ParsingResult[ListConnectedDomainsResult] = {
    val participantAdminV30.ListConnectedDomainsResponse.Result(domainAlias, domainId, healthy) =
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

  def fromProtoV30(
      value: participantAdminV30.ListDarContentsResponse
  ): ParsingResult[DarMetadata] = {
    val participantAdminV30.ListDarContentsResponse(description, main, packages, dependencies) =
      value
    Right(DarMetadata(description, main, packages, dependencies))
  }
}
