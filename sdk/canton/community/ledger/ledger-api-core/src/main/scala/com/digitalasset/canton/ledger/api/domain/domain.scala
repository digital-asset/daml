// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.domain

import com.daml.lf.command.{ApiCommands as LfCommands, DisclosedContract as LfDisclosedContract}
import com.daml.lf.crypto
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.logging.*
import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value as Lf
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.DomainId
import scalaz.@@
import scalaz.syntax.tag.*

import scala.collection.immutable

final case class TransactionFilter(
    filtersByParty: immutable.Map[Ref.Party, CumulativeFilter],
    filtersForAnyParty: Option[CumulativeFilter] = None,
)

final case class InterfaceFilter(
    interfaceId: Ref.Identifier,
    includeView: Boolean,
    includeCreatedEventBlob: Boolean,
)

final case class TemplateFilter(
    templateTypeRef: Ref.TypeConRef,
    includeCreatedEventBlob: Boolean,
)

final case class TemplateWildcardFilter(
    includeCreatedEventBlob: Boolean
)

object TemplateFilter {
  def apply(templateId: Ref.Identifier, includeCreatedEventBlob: Boolean): TemplateFilter =
    TemplateFilter(
      Ref.TypeConRef(Ref.PackageRef.Id(templateId.packageId), templateId.qualifiedName),
      includeCreatedEventBlob,
    )
}

final case class CumulativeFilter(
    templateFilters: immutable.Set[TemplateFilter],
    interfaceFilters: immutable.Set[InterfaceFilter],
    templateWildcardFilter: Option[TemplateWildcardFilter],
)

object CumulativeFilter {
  def templateWildcardFilter(includeCreatedEventBlob: Boolean = false): CumulativeFilter =
    CumulativeFilter(
      templateFilters = Set.empty,
      interfaceFilters = Set.empty,
      templateWildcardFilter =
        Some(TemplateWildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)),
    )

}

sealed abstract class ParticipantOffset extends Product with Serializable

object ParticipantOffset {

  final case class Absolute(value: Ref.LedgerString) extends ParticipantOffset

  case object ParticipantBegin extends ParticipantOffset

  case object ParticipantEnd extends ParticipantOffset

  implicit val `Absolute Ordering`: Ordering[ParticipantOffset.Absolute] =
    Ordering.by[ParticipantOffset.Absolute, String](_.value)

  implicit val `ParticipantOffset to LoggingValue`: ToLoggingValue[ParticipantOffset] = value =>
    LoggingValue.OfString(value match {
      case ParticipantOffset.Absolute(absolute) => absolute
      case ParticipantOffset.ParticipantBegin => "%begin%"
      case ParticipantOffset.ParticipantEnd => "%end%"
    })
}

final case class Commands(
    workflowId: Option[WorkflowId],
    applicationId: Ref.ApplicationId,
    commandId: CommandId,
    submissionId: Option[SubmissionId],
    actAs: Set[Ref.Party],
    readAs: Set[Ref.Party],
    submittedAt: Timestamp,
    deduplicationPeriod: DeduplicationPeriod,
    commands: LfCommands,
    disclosedContracts: ImmArray[DisclosedContract],
    domainId: Option[DomainId] = None,
    packagePreferenceSet: Set[Ref.PackageId] = Set.empty,
    // Used to indicate the package map against which package resolution was performed.
    packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
) extends PrettyPrinting {

  override def pretty: Pretty[Commands] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("commandId", _.commandId.unwrap),
      paramIfDefined("submissionId", _.submissionId.map(_.unwrap)),
      param("applicationId", _.applicationId),
      param("actAs", _.actAs),
      paramIfNonEmpty("readAs", _.readAs),
      param("submittedAt", _.submittedAt),
      param("ledgerEffectiveTime", _.commands.ledgerEffectiveTime),
      param("deduplicationPeriod", _.deduplicationPeriod),
      paramIfDefined("workflowId", _.workflowId.filter(_ != commandId).map(_.unwrap)),
      paramIfDefined("domainId", _.domainId),
      indicateOmittedFields,
    )
  }
}

final case class DisclosedContract(
    templateId: Ref.TypeConName,
    packageName: Ref.PackageName,
    packageVersion: Option[Ref.PackageVersion],
    contractId: Lf.ContractId,
    argument: Value,
    createdAt: Timestamp,
    keyHash: Option[crypto.Hash],
    signatories: Set[Ref.Party],
    stakeholders: Set[Ref.Party],
    keyMaintainers: Option[Set[Ref.Party]],
    keyValue: Option[Value],
    driverMetadata: Bytes,
    transactionVersion: TransactionVersion,
) {
  def toLf: LfDisclosedContract =
    LfDisclosedContract(
      templateId,
      contractId,
      argument,
      keyHash,
    )
}

object Commands {

  import Logging.*

  implicit val `Timestamp to LoggingValue`: ToLoggingValue[Timestamp] =
    ToLoggingValue.ToStringToLoggingValue

  implicit val `Commands to LoggingValue`: ToLoggingValue[Commands] = commands => {
    LoggingValue.Nested.fromEntries(
      "workflowId" -> commands.workflowId,
      "applicationId" -> commands.applicationId,
      "submissionId" -> commands.submissionId,
      "commandId" -> commands.commandId,
      "actAs" -> commands.actAs,
      "readAs" -> commands.readAs,
      "submittedAt" -> commands.submittedAt,
      "deduplicationPeriod" -> commands.deduplicationPeriod,
    )
  }
}

object Logging {
  implicit def `tagged value to LoggingValue`[T: ToLoggingValue, Tag]: ToLoggingValue[T @@ Tag] =
    value => value.unwrap
}
