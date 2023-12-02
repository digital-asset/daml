// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.domain

import com.daml.lf.command.{ApiCommands as LfCommands, DisclosedContract as LfDisclosedContract}
import com.daml.lf.crypto
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.logging.*
import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.value.Value as Lf
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.DomainId
import scalaz.syntax.tag.*
import scalaz.{@@, Tag}

import java.net.URL
import scala.collection.immutable
import scala.util.Try

final case class TransactionFilter(filtersByParty: immutable.Map[Ref.Party, Filters]) {
  def apply(party: Ref.Party, template: Ref.Identifier): Boolean =
    filtersByParty.get(party).fold(false)(_.apply(template))
}

final case class Filters(inclusive: Option[InclusiveFilters]) {
  def apply(identifier: Ref.Identifier): Boolean =
    inclusive.fold(true)(_.templateFilters.exists(_.templateId == identifier))
}

object Filters {
  val noFilter: Filters = Filters(None)

  def apply(inclusive: InclusiveFilters) = new Filters(Some(inclusive))
}

final case class InterfaceFilter(
    interfaceId: Ref.Identifier,
    includeView: Boolean,
    includeCreatedEventBlob: Boolean,
)

final case class TemplateFilter(
    templateId: Ref.Identifier,
    includeCreatedEventBlob: Boolean,
)

final case class InclusiveFilters(
    templateFilters: immutable.Set[TemplateFilter],
    interfaceFilters: immutable.Set[InterfaceFilter],
)

sealed abstract class LedgerOffset extends Product with Serializable

object LedgerOffset {

  final case class Absolute(value: Ref.LedgerString) extends LedgerOffset

  case object LedgerBegin extends LedgerOffset

  case object LedgerEnd extends LedgerOffset

  implicit val `Absolute Ordering`: Ordering[LedgerOffset.Absolute] =
    Ordering.by[LedgerOffset.Absolute, String](_.value)

  implicit val `LedgerOffset to LoggingValue`: ToLoggingValue[LedgerOffset] = value =>
    LoggingValue.OfString(value match {
      case LedgerOffset.Absolute(absolute) => absolute
      case LedgerOffset.LedgerBegin => "%begin%"
      case LedgerOffset.LedgerEnd => "%end%"
    })
}

final case class Commands(
    ledgerId: Option[LedgerId],
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

sealed trait DisclosedContract extends Product with Serializable {
  def templateId: Ref.TypeConName
  def contractId: Lf.ContractId
  def argument: Value
  def createdAt: Timestamp
  def keyHash: Option[crypto.Hash]
  def driverMetadata: Bytes

  def toLf: LfDisclosedContract =
    LfDisclosedContract(
      templateId,
      contractId,
      argument,
      keyHash,
    )
}

// TODO(#15058): Remove usages and logic associated with the old means of providing
//               the disclosed contract create argument payload in command submission
final case class NonUpgradableDisclosedContract(
    templateId: Ref.TypeConName,
    contractId: Lf.ContractId,
    argument: Value,
    createdAt: Timestamp,
    keyHash: Option[crypto.Hash],
    driverMetadata: Bytes,
) extends DisclosedContract

final case class UpgradableDisclosedContract(
    templateId: Ref.TypeConName,
    contractId: Lf.ContractId,
    argument: Value,
    createdAt: Timestamp,
    keyHash: Option[crypto.Hash],
    signatories: Set[Ref.Party],
    stakeholders: Set[Ref.Party],
    keyMaintainers: Option[Set[Ref.Party]],
    keyValue: Option[Value],
    driverMetadata: Bytes,
) extends DisclosedContract

object Commands {

  import Logging.*

  implicit val `Timestamp to LoggingValue`: ToLoggingValue[Timestamp] =
    ToLoggingValue.ToStringToLoggingValue

  implicit val `Commands to LoggingValue`: ToLoggingValue[Commands] = commands => {
    val maybeString: Option[String] = commands.ledgerId.map(Tag.unwrap)
    LoggingValue.Nested.fromEntries(
      "ledgerId" -> OfString(maybeString.getOrElse("<empty-ledger-id>")),
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

/** Configuration entry describes a change to the current configuration. */
sealed abstract class ConfigurationEntry extends Product with Serializable

object ConfigurationEntry {
  final case class Accepted(
      submissionId: String,
      configuration: Configuration,
  ) extends ConfigurationEntry

  final case class Rejected(
      submissionId: String,
      rejectionReason: String,
      proposedConfiguration: Configuration,
  ) extends ConfigurationEntry
}

sealed abstract class PackageEntry() extends Product with Serializable

object PackageEntry {
  final case class PackageUploadAccepted(
      submissionId: String,
      recordTime: Timestamp,
  ) extends PackageEntry

  final case class PackageUploadRejected(
      submissionId: String,
      recordTime: Timestamp,
      reason: String,
  ) extends PackageEntry
}

object Logging {
  implicit def `tagged value to LoggingValue`[T: ToLoggingValue, Tag]: ToLoggingValue[T @@ Tag] =
    value => value.unwrap
}

final case class ObjectMeta(
    resourceVersionO: Option[Long],
    annotations: Map[String, String],
)

object ObjectMeta {
  def empty: ObjectMeta = ObjectMeta(
    resourceVersionO = None,
    annotations = Map.empty,
  )
}

final case class User(
    id: Ref.UserId,
    primaryParty: Option[Ref.Party],
    isDeactivated: Boolean = false,
    metadata: ObjectMeta = ObjectMeta.empty,
    identityProviderId: IdentityProviderId = IdentityProviderId.Default,
)

final case class PartyDetails(
    party: Ref.Party,
    displayName: Option[String],
    isLocal: Boolean,
    metadata: ObjectMeta,
    identityProviderId: IdentityProviderId,
)

sealed abstract class UserRight extends Product with Serializable
object UserRight {
  final case object ParticipantAdmin extends UserRight
  final case object IdentityProviderAdmin extends UserRight
  final case class CanActAs(party: Ref.Party) extends UserRight
  final case class CanReadAs(party: Ref.Party) extends UserRight
}

sealed abstract class Feature extends Product with Serializable
object Feature {
  case object UserManagement extends Feature
}

final case class JwksUrl(value: String) extends AnyVal {
  def toURL = new URL(value)
}
object JwksUrl {
  def fromString(value: String): Either[String, JwksUrl] =
    Try(new URL(value)).toEither.left
      .map(_.getMessage)
      .map(_ => JwksUrl(value))

  def assertFromString(str: String): JwksUrl = fromString(str) match {
    case Right(value) => value
    case Left(err) => throw new IllegalArgumentException(err)
  }
}

sealed trait IdentityProviderId {
  def toRequestString: String

  def toDb: Option[IdentityProviderId.Id]
}

object IdentityProviderId {
  final case object Default extends IdentityProviderId {
    override def toRequestString: String = ""
    override def toDb: Option[Id] = None
  }

  final case class Id(value: Ref.LedgerString) extends IdentityProviderId {
    override def toRequestString: String = value

    override def toDb: Option[Id] = Some(this)
  }

  object Id {
    def fromString(id: String): Either[String, IdentityProviderId.Id] = {
      Ref.LedgerString.fromString(id).map(Id.apply)
    }

    def assertFromString(id: String): Id = {
      Id(Ref.LedgerString.assertFromString(id))
    }
  }

  def apply(identityProviderId: String): IdentityProviderId =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Id(Ref.LedgerString.assertFromString(id))
      case None => Default
    }

  def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Ref.LedgerString.fromString(id).map(Id.apply)
      case None => Right(Default)
    }

  def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
    identityProviderId match {
      case None => IdentityProviderId.Default
      case Some(id) => id
    }
}

final case class IdentityProviderConfig(
    identityProviderId: IdentityProviderId.Id,
    isDeactivated: Boolean = false,
    jwksUrl: JwksUrl,
    issuer: String,
    audience: Option[String],
)
