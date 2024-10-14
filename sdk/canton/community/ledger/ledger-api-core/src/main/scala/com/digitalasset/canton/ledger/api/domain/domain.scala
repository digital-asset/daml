// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.domain

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.daml.lf.command.ApiCommands as LfCommands
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.logging.*
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.FatContractInstance
import scalaz.@@
import scalaz.syntax.tag.*

import scala.collection.immutable

final case class TransactionFilter(
    filtersByParty: immutable.Map[Ref.Party, CumulativeFilter],
    filtersForAnyParty: Option[CumulativeFilter] = None,
)

final case class InterfaceFilter(
    interfaceTypeRef: Ref.TypeConRef,
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

object types {
  type ParticipantOffset = Ref.HexString
}

object ParticipantOffset {
  val ParticipantBegin: types.ParticipantOffset = Ref.HexString.assertFromString("")

  def fromString(str: String): types.ParticipantOffset =
    Ref.HexString.assertFromString(str)
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
    domainId: Option[DomainId],
    packagePreferenceSet: Set[Ref.PackageId] = Set.empty,
    // Used to indicate the package map against which package resolution was performed.
    packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
) extends PrettyPrinting {

  override protected def pretty: Pretty[Commands] = {
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

final case class DisclosedContract(
    fatContractInstance: FatContractInstance,
    domainIdO: Option[DomainId],
) extends PrettyPrinting {
  override protected def pretty: Pretty[DisclosedContract] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("contractId", _.fatContractInstance.contractId),
      param("templateId", _.fatContractInstance.templateId),
      paramIfDefined("domainId", _.domainIdO),
      indicateOmittedFields,
    )
  }
}

object Logging {
  implicit def `tagged value to LoggingValue`[T: ToLoggingValue, Tag]: ToLoggingValue[T @@ Tag] =
    value => value.unwrap
}
