// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.TransactionVersion
import com.digitalasset.canton.participant.protocol.submission.UsableDomain.DomainNotUsedReason
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[submission] object DomainsFilter {

  def split(
      requiredPackagesPerParty: Map[Party, Set[PackageId]],
      domains: List[(DomainId, ProtocolVersion, TopologySnapshot)],
      transactionVersion: TransactionVersion,
  )(implicit ec: ExecutionContext): Future[(List[DomainNotUsedReason], List[DomainId])] = domains
    .parTraverse { case (domainId, protocolVersion, snapshot) =>
      UsableDomain
        .check(
          domainId,
          protocolVersion,
          snapshot,
          requiredPackagesPerParty,
          transactionVersion,
        )
        .map(_ => domainId)
        .value
    }
    .map(_.separate)

}
