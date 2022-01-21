// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.version.withoutledgerid

import com.daml.ledger.api.domain.{Feature, LedgerId}
import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationPeriodSupport.{
  DurationSupport,
  OffsetSupport,
}
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalFeatures,
  ExperimentalStaticTime,
}
import com.daml.ledger.api.v1.version_service.{FeaturesDescriptor, GetLedgerApiVersionRequest}
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

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
  )(implicit executionContext: ExecutionContext): Future[Seq[Feature]] =
    LedgerClient
      .stub(service, token)
      .getLedgerApiVersion(
        new GetLedgerApiVersionRequest(ledgerIdToUse.unwrap)
      )
      .map(_.features.toList.flatMap(VersionClient.fromProto))
}

private[daml] object VersionClient {
  // see also com.daml.platform.apiserver.services.ApiVersionService.featuresDescriptor
  def fromProto(featuresDescriptor: FeaturesDescriptor): Seq[Feature] =
    featuresDescriptor match {
      case FeaturesDescriptor(userManagement, experimentalFeatures) =>
        (userManagement.toList map (_ => Feature.UserManagement)) ++
          (experimentalFeatures.toList flatMap fromProto)
    }

  def fromProto(experimentalFeatures: ExperimentalFeatures): Seq[Feature] =
    experimentalFeatures match {
      case ExperimentalFeatures(
            selfServiceErrorCodes,
            maybeStaticTime,
            maybeCommandDeduplicationFeatures,
            optionalLedgerId,
            _, // TODO
          ) =>
        (selfServiceErrorCodes.toList map (_ => Feature.SelfServiceErrorCodes)) ++
          (maybeStaticTime collect { case ExperimentalStaticTime(true) => Feature.StaticTime }) ++
          (maybeCommandDeduplicationFeatures.toList flatMap fromProto) ++
          (optionalLedgerId map { _ => Feature.ExperimentalOptionalLedgerId })
    }

  def fromProto(commandDeduplicationFeatures: CommandDeduplicationFeatures): Seq[Feature] = {
    commandDeduplicationFeatures match {
      case CommandDeduplicationFeatures(
            deduplicationPeriodSupport,
            deduplicationType,
            maxDeduplicationDurationEnforced,
          ) =>
        import Feature.CommandDeduplication.{OffsetSupport => OS, DurationSupport => DS, Type => DT}
        val (os, ds) = deduplicationPeriodSupport match {
          case Some(CommandDeduplicationPeriodSupport(offsetSupport, durationSupport)) =>
            (
              offsetSupport match {
                case OffsetSupport.OFFSET_NOT_SUPPORTED => Some(OS.OFFSET_NOT_SUPPORTED)
                case OffsetSupport.OFFSET_NATIVE_SUPPORT => Some(OS.OFFSET_NATIVE_SUPPORT)
                case OffsetSupport.OFFSET_CONVERT_TO_DURATION => Some(OS.OFFSET_CONVERT_TO_DURATION)
                case _ => None
              },
              durationSupport match {
                case DurationSupport.DURATION_NATIVE_SUPPORT => Some(DS.DURATION_NATIVE_SUPPORT)
                case DurationSupport.DURATION_CONVERT_TO_OFFSET =>
                  Some(DS.DURATION_CONVERT_TO_OFFSET)
                case _ => None
              },
            )
          case _ => (None, None)
        }

        val dt = deduplicationType match {
          case CommandDeduplicationType.ASYNC_ONLY => Some(DT.ASYNC_ONLY)
          case CommandDeduplicationType.ASYNC_AND_CONCURRENT_SYNC =>
            Some(DT.ASYNC_AND_CONCURRENT_SYNC)
          case CommandDeduplicationType.SYNC_ONLY => Some(DT.SYNC_ONLY)
          case _ => None
        }

        Seq(Feature.CommandDeduplication(os, ds, dt, maxDeduplicationDurationEnforced))
    }
  }

}
