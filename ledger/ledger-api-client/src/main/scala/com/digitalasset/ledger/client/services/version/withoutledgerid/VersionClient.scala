// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.version.withoutledgerid

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.experimental_features.ExperimentalFeatures
import com.daml.ledger.api.v1.version_service.{FeaturesDescriptor, GetLedgerApiVersionRequest}
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
private[daml] final class VersionClient(service: VersionServiceStub) {
  def getApiVersion(
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  )(implicit executionContext: ExecutionContext): Future[String] =
    LedgerClient
      .stub(service, token)
      .getLedgerApiVersion(
        new GetLedgerApiVersionRequest(ledgerIdToUse.unwrap)
      )
      .map(_.version)

  def getApiFeatures(
                     ledgerIdToUse: LedgerId,
                     token: Option[String] = None,
                   )(implicit executionContext: ExecutionContext): Future[Seq[VersionClient.Feature]] =
    LedgerClient
      .stub(service, token)
      .getLedgerApiVersion(
        new GetLedgerApiVersionRequest(ledgerIdToUse.unwrap)
      )
      .map(_.features.toSeq.flatMap(VersionClient.Feature.fromProto))
}

@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
private[daml] object VersionClient {
  sealed trait Feature
  object Feature {
    case object UserManagement extends Feature
    case object SelfServiceErrorCodes extends Feature

    def fromProto(featuresDescriptor: FeaturesDescriptor): Seq[Feature] =
      featuresDescriptor match {
        case FeaturesDescriptor(userManagement, experimentalFeatures) =>
          (userManagement.toSeq map (_ => UserManagement)) ++ (experimentalFeatures.toSeq flatMap fromProto)
      }

    def fromProto(experimentalFeatures: ExperimentalFeatures): Seq[Feature] =
      experimentalFeatures match {
        case ExperimentalFeatures(selfServiceErrorCodes) =>
          selfServiceErrorCodes.toSeq map (_ => SelfServiceErrorCodes)
      }

  }
}
