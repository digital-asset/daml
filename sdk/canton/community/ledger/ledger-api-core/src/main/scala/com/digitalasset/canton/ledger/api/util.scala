// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.package_management_service
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.{package_reference, package_service}
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.daml.nonempty.*
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  UnrecognizedEnum,
  ValueConversionError,
}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfFatContractInst
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion}
import com.digitalasset.daml.lf.command.{ApiCommands as LfCommands, ApiContractKey}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.logging.*
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import scalaz.@@
import scalaz.syntax.tag.*

import scala.annotation.nowarn
import scala.collection.immutable

final case class UpdateFormat(
    includeTransactions: Option[TransactionFormat],
    includeReassignments: Option[EventFormat],
    includeTopologyEvents: Option[TopologyFormat],
)

final case class TopologyFormat(
    participantAuthorizationFormat: Option[ParticipantAuthorizationFormat]
)

// The list of parties for which the topology transactions should be sent. If None then all the parties that the
// participant can read as are denoted (wildcard party).
final case class ParticipantAuthorizationFormat(parties: Option[Set[Ref.Party]])

final case class TransactionFormat(
    eventFormat: EventFormat,
    transactionShape: TransactionShape,
)

sealed trait TransactionShape
object TransactionShape {
  case object LedgerEffects extends TransactionShape
  case object AcsDelta extends TransactionShape

  def toProto(
      transactionShape: TransactionShape
  ): com.daml.ledger.api.v2.transaction_filter.TransactionShape =
    transactionShape match {
      case TransactionShape.LedgerEffects => TRANSACTION_SHAPE_LEDGER_EFFECTS
      case TransactionShape.AcsDelta => TRANSACTION_SHAPE_ACS_DELTA
    }
}

final case class EventFormat(
    filtersByParty: immutable.Map[Ref.Party, CumulativeFilter],
    filtersForAnyParty: Option[CumulativeFilter],
    verbose: Boolean,
)

final case class InterfaceFilter(
    interfaceTypeRef: Ref.TypeConRef, // TODO(#26879) use only NameTypeConRef and do not accept IdTypeConRef in 3.5
    includeView: Boolean,
    includeCreatedEventBlob: Boolean,
)

final case class TemplateFilter(
    templateTypeRef: Ref.TypeConRef, // TODO(#26879) use only NameTypeConRef and do not accept IdTypeConRef in 3.5
    includeCreatedEventBlob: Boolean,
)

final case class TemplateWildcardFilter(
    includeCreatedEventBlob: Boolean
)

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

final case class Commands(
    workflowId: Option[WorkflowId],
    userId: Ref.UserId,
    commandId: CommandId,
    submissionId: Option[SubmissionId],
    actAs: Set[Ref.Party],
    readAs: Set[Ref.Party],
    submittedAt: Timestamp,
    deduplicationPeriod: DeduplicationPeriod,
    commands: LfCommands,
    disclosedContracts: ImmArray[DisclosedContract],
    synchronizerId: Option[SynchronizerId],
    packagePreferenceSet: Set[Ref.PackageId] = Set.empty,
    // Used to indicate the package map against which package resolution was performed.
    packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
    prefetchKeys: Seq[ApiContractKey],
) extends PrettyPrinting {

  override protected def pretty: Pretty[Commands] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("commandId", _.commandId.unwrap),
      paramIfDefined("submissionId", _.submissionId.map(_.unwrap)),
      param("userId", _.userId),
      param("actAs", _.actAs),
      paramIfNonEmpty("readAs", _.readAs),
      param("submittedAt", _.submittedAt),
      param("ledgerEffectiveTime", _.commands.ledgerEffectiveTime),
      param("deduplicationPeriod", _.deduplicationPeriod),
      paramIfDefined("workflowId", _.workflowId.filter(_ != commandId).map(_.unwrap)),
      paramIfDefined("synchronizerId", _.synchronizerId),
      paramIfNonEmpty("prefetchKeys", _.prefetchKeys.map(_.toString.unquoted)),
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
      "userId" -> commands.userId,
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
    fatContractInstance: LfFatContractInst,
    synchronizerIdO: Option[SynchronizerId],
) extends PrettyPrinting {
  override protected def pretty: Pretty[DisclosedContract] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("contractId", _.fatContractInstance.contractId),
      param("templateId", _.fatContractInstance.templateId),
      paramIfDefined("synchronizerId", _.synchronizerIdO),
      indicateOmittedFields,
    )
  }
}

// TODO(#25385): Deduplicate with logic from TopologyAwareCommandExecutor
// Wrapper used for ordering package ids by version
final case class PackageReference(
    pkgId: LfPackageId,
    version: LfPackageVersion,
    packageName: LfPackageName,
) extends PrettyPrinting {

  override protected def pretty: Pretty[PackageReference] =
    prettyOfString(_ => show"pkg:$packageName:$version/$pkgId")
}

object PackageReference {
  implicit val packageReferenceOrdering: Ordering[PackageReference] =
    (x: PackageReference, y: PackageReference) =>
      if (x.packageName != y.packageName) {
        throw new RuntimeException(
          s"Cannot compare package-ids with different package names: $x and $y"
        )
      } else
        Ordering[(LfPackageVersion, LfPackageId)]
          .compare(x.version -> x.pkgId, y.version -> y.pkgId)

  implicit class PackageReferenceOps(val pkgId: LfPackageId) extends AnyVal {
    def toPackageReference(
        packageIdVersionMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
    ): Option[PackageReference] =
      packageIdVersionMap.get(pkgId).map { case (packageName, packageVersion) =>
        PackageReference(pkgId, packageVersion, packageName)
      }

    def unsafeToPackageReference(
        packageIdVersionMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
    ): PackageReference =
      toPackageReference(packageIdVersionMap).getOrElse {
        throw new NoSuchElementException(
          s"Package id $pkgId not found in packageIdVersionMap"
        )
      }
  }
}

object Logging {
  implicit def `tagged value to LoggingValue`[T: ToLoggingValue, Tag]: ToLoggingValue[T @@ Tag] =
    value => value.unwrap
}

final case class ListVettedPackagesOpts(
    packageFilter: Option[PackageMetadataFilter],
    topologyStateFilter: Option[TopologyStateFilter],
) {
  def toPredicate(metadata: PackageMetadata): Ref.PackageId => Boolean = { (pkgId: Ref.PackageId) =>
    val matchesMetadata = packageFilter.forall(_.toPredicate(metadata)(pkgId))

    val matchesTopologyState =
      topologyStateFilter.forall(_.toPredicate(metadata)(pkgId))

    matchesMetadata && matchesTopologyState
  }
}

object ListVettedPackagesOpts {
  def fromProto(
      req: package_service.ListVettedPackagesRequest
  ): ParsingResult[ListVettedPackagesOpts] =
    for {
      packageMetadataFilter <- req.packageMetadataFilter.traverse(PackageMetadataFilter.fromProto)
      topologyStateFilter <- req.topologyStateFilter.traverse(TopologyStateFilter.fromProto)
    } yield ListVettedPackagesOpts(packageMetadataFilter, topologyStateFilter)
}

final case class PackageMetadataFilter(
    packageIds: Seq[Ref.PackageId],
    packageNamePrefixes: Seq[String],
) {
  def toProtoLAPI: package_service.PackageMetadataFilter =
    package_service.PackageMetadataFilter(
      packageIds.map(_.toString),
      packageNamePrefixes,
    )

  def toPredicate(metadata: PackageMetadata): Ref.PackageId => Boolean = {
    lazy val noFilters = packageIds.isEmpty && packageNamePrefixes.isEmpty
    lazy val allPackageIds = packageIds.toSet
    lazy val allNames = (for {
      name <- metadata.packageNameMap.keys
      if packageNamePrefixes.exists(name.toString.startsWith(_))
    } yield name).toSet

    { (targetPkgId: Ref.PackageId) =>
      lazy val matchesPkgId = allPackageIds.contains(targetPkgId)
      lazy val matchesName = metadata.packageIdVersionMap.get(targetPkgId) match {
        case Some((name, _)) => allNames.contains(name)
        case None => false // package ID is not known on this participant
      }
      noFilters || matchesPkgId || matchesName
    }
  }
}

object PackageMetadataFilter {
  def fromProto(
      filter: package_service.PackageMetadataFilter
  ): ParsingResult[PackageMetadataFilter] =
    filter.packageIds
      .traverse(
        Ref.PackageId.fromString(_).leftMap(ValueConversionError("package_ids", _))
      )
      .map(PackageMetadataFilter(_, filter.packageNamePrefixes))
}

final case class TopologyStateFilter(
    participantIds: Seq[ParticipantId],
    synchronizerIds: Seq[SynchronizerId],
) {
  def toProtoLAPI: package_service.TopologyStateFilter =
    package_service.TopologyStateFilter(
      participantIds.map(_.toString),
      synchronizerIds.map(_.toString),
    )

  @nowarn
  def toPredicate(metadata: PackageMetadata): Ref.PackageId => Boolean =
    (_: Ref.PackageId) => true
}

object TopologyStateFilter {
  def fromProto(
      filter: package_service.TopologyStateFilter
  ): ParsingResult[TopologyStateFilter] =
    for {
      synchronizerIds <- filter.synchronizerIds.traverse(
        SynchronizerId.fromProtoPrimitive(_, "synchronizer_ids")
      )
      participantIds <- filter.participantIds.traverse(
        ParticipantId.fromProtoPrimitive(_, "participant_ids")
      )
    } yield TopologyStateFilter(
      participantIds = participantIds,
      synchronizerIds = synchronizerIds,
    )
}

final case class UpdateVettedPackagesOpts(
    changes: Seq[VettedPackagesChange],
    dryRun: Boolean,
    synchronizerIdO: Option[SynchronizerId],
) {
  def toTargetStates: Seq[SinglePackageTargetVetting[VettedPackagesRef]] =
    for {
      change <- changes
      ref <- change.packages
    } yield change match {
      case v: VettedPackagesChange.Vet =>
        SinglePackageTargetVetting(ref, Some((v.newValidFromInclusive, v.newValidUntilExclusive)))
      case v: VettedPackagesChange.Unvet => SinglePackageTargetVetting(ref, None)
    }
}

object UpdateVettedPackagesOpts {
  def fromProto(
      req: package_management_service.UpdateVettedPackagesRequest
  ): ParsingResult[UpdateVettedPackagesOpts] = for {
    vettingChanges <- req.changes
      .traverse(VettedPackagesChange.fromProto)
    synchronizerIdO <- OptionUtil
      .emptyStringAsNone(req.synchronizerId)
      .traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
  } yield UpdateVettedPackagesOpts(vettingChanges, req.dryRun, synchronizerIdO)
}

sealed trait VettedPackagesChange {
  def packages: Seq[VettedPackagesRef]
}

object VettedPackagesChange {
  final case class Vet(
      packages: Seq[VettedPackagesRef],
      newValidFromInclusive: Option[CantonTimestamp],
      newValidUntilExclusive: Option[CantonTimestamp],
  ) extends VettedPackagesChange

  object Vet {
    def fromProto(change: package_management_service.VettedPackagesChange.Vet): ParsingResult[Vet] =
      for {
        packages <- change.packages.traverse(VettedPackagesRef.fromProto)
        newValidFromInclusive <- change.newValidFromInclusive.traverse(
          CantonTimestamp.fromProtoTimestamp
        )
        newValidUntilExclusive <- change.newValidUntilExclusive.traverse(
          CantonTimestamp.fromProtoTimestamp
        )
      } yield Vet(packages, newValidFromInclusive, newValidUntilExclusive)
  }

  final case class Unvet(
      packages: Seq[VettedPackagesRef]
  ) extends VettedPackagesChange

  object Unvet {
    def fromProto(
        change: package_management_service.VettedPackagesChange.Unvet
    ): ParsingResult[Unvet] =
      change.packages
        .traverse(VettedPackagesRef.fromProto)
        .map(Unvet(_))
  }

  def fromProto(
      change: package_management_service.VettedPackagesChange
  ): ParsingResult[VettedPackagesChange] =
    change.operation match {
      case package_management_service.VettedPackagesChange.Operation.Vet(vet) =>
        Vet.fromProto(vet)
      case package_management_service.VettedPackagesChange.Operation.Unvet(unvet) =>
        Unvet.fromProto(unvet)
      case package_management_service.VettedPackagesChange.Operation.Empty =>
        Left(FieldNotSet("operation"))
    }
}

trait UploadDarVettingChange {
  def toProto: package_management_service.UploadDarFileRequest.VettingChange
}
object VetAllPackages extends UploadDarVettingChange {
  override def toProto =
    package_management_service.UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES
}
object DontVetAnyPackages extends UploadDarVettingChange {
  override def toProto =
    package_management_service.UploadDarFileRequest.VettingChange.VETTING_CHANGE_DONT_VET_ANY_PACKAGES
}

object UploadDarVettingChange {
  val default: UploadDarVettingChange = VetAllPackages

  def fromProto(
      fieldName: String,
      change: Option[package_management_service.UploadDarFileRequest.VettingChange],
  ): ParsingResult[UploadDarVettingChange] =
    change.map(fromProto(fieldName, _)).getOrElse(Right(VetAllPackages))

  def fromProto(
      fieldName: String,
      change: package_management_service.UploadDarFileRequest.VettingChange,
  ): ParsingResult[UploadDarVettingChange] =
    change match {
      case package_management_service.UploadDarFileRequest.VettingChange.VETTING_CHANGE_UNSPECIFIED =>
        Right(default)

      case package_management_service.UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES =>
        Right(VetAllPackages)
      case package_management_service.UploadDarFileRequest.VettingChange.VETTING_CHANGE_DONT_VET_ANY_PACKAGES =>
        Right(DontVetAnyPackages)
      case package_management_service.UploadDarFileRequest.VettingChange
            .Unrecognized(unrecognizedValue) =>
        Left(UnrecognizedEnum(fieldName, unrecognizedValue))
    }
}

sealed trait VettedPackagesRef extends PrettyPrinting {
  def toProtoLAPI: package_management_service.VettedPackagesRef
  def findMatchingPackages(metadata: PackageMetadata): Either[String, NonEmpty[Set[Ref.PackageId]]]
}

object VettedPackagesRef {
  final case class Id(
      id: Ref.PackageId
  ) extends VettedPackagesRef {
    def toProtoLAPI: package_management_service.VettedPackagesRef =
      package_management_service.VettedPackagesRef(id.toString, "", "")

    def findMatchingPackages(
        metadata: PackageMetadata
    ): Either[String, NonEmpty[Set[Ref.PackageId]]] =
      if (!metadata.packageIdVersionMap.contains(id)) {
        Left(s"No packages with package ID $id")
      } else {
        Right(NonEmpty(Set, id))
      }

    override protected def pretty: Pretty[Id] =
      prettyOfString(id => s"package-id: ${id.id.singleQuoted}")
  }

  final case class NameAndVersion(
      name: Ref.PackageName,
      version: Ref.PackageVersion,
  ) extends VettedPackagesRef {
    def toProtoLAPI: package_management_service.VettedPackagesRef =
      package_management_service.VettedPackagesRef(
        "",
        name.toString,
        version.toString,
      )

    def findMatchingPackages(
        metadata: PackageMetadata
    ): Either[String, NonEmpty[Set[Ref.PackageId]]] =
      metadata.packageNameMap.get(name) match {
        case None => Left(s"Name $name did not match any packages.")
        case Some(packageResolution) =>
          val matchingIds: Set[Ref.PackageId] =
            packageResolution.allPackageIdsForName.toSet
              .filter { matchingId =>
                val (_, matchingVersion) = metadata.packageIdVersionMap.getOrElse(
                  matchingId,
                  sys.error(
                    s"Unexpectedly missing package ID $matchingId from the package ID version map."
                  ),
                )
                version == matchingVersion
              }
          NonEmpty.from(matchingIds) match {
            case None => Left(s"No packages with name $name have version $version.")
            case Some(ne) => Right(ne)
          }
      }

    override protected def pretty: Pretty[NameAndVersion] = prettyOfClass(
      param("name", _.name),
      param("version", _.version),
    )
  }

  final case class All(
      id: Ref.PackageId,
      name: Ref.PackageName,
      version: Ref.PackageVersion,
  ) extends VettedPackagesRef {
    def toProtoLAPI: package_management_service.VettedPackagesRef =
      package_management_service.VettedPackagesRef(
        id.toString,
        name.toString,
        version.toString,
      )

    def findMatchingPackages(
        metadata: PackageMetadata
    ): Either[String, NonEmpty[Set[Ref.PackageId]]] =
      metadata.packageIdVersionMap.get(id) match {
        case None => Left(s"No packages with package ID $id")
        case Some((matchingName, matchingVersion)) =>
          if (name == matchingName && version == matchingVersion) {
            Right(NonEmpty(Set, id))
          } else {
            Left(
              s"Package with package ID $id has name $matchingName and version $matchingVersion, but filter specifies name $name and version $version"
            )
          }
      }

    override protected def pretty: Pretty[All] =
      prettyOfClass(
        param("id", _.id),
        param("name", _.name),
        param("version", _.version),
      )
  }

  final case class Name(
      name: Ref.PackageName
  ) extends VettedPackagesRef {
    def toProtoLAPI: package_management_service.VettedPackagesRef =
      package_management_service.VettedPackagesRef("", name.toString, "")

    def findMatchingPackages(
        metadata: PackageMetadata
    ): Either[String, NonEmpty[Set[Ref.PackageId]]] =
      metadata.packageNameMap.get(name) match {
        case None => Left(s"No packages with name $name")
        case Some(packageResolution) => Right(packageResolution.allPackageIdsForName)
      }

    override protected def pretty: Pretty[Name] =
      prettyOfString(name => s"package-name: ${name.name.singleQuoted}")
  }

  private def parseWith[A](
      name: String,
      value: String,
      f: String => Either[String, A],
  ): ParsingResult[Option[A]] =
    Some(value)
      .filter(_.nonEmpty)
      .traverse(f)
      .leftMap(ValueConversionError(name, _))

  private def process(
      mbPackageId: Option[Ref.PackageId],
      mbPackageName: Option[Ref.PackageName],
      mbPackageVersion: Option[Ref.PackageVersion],
  ): ParsingResult[VettedPackagesRef] =
    (mbPackageId, mbPackageName, mbPackageVersion) match {
      case (Some(id), Some(name), Some(version)) => Right(All(id, name, version))
      case (None, Some(name), Some(version)) => Right(NameAndVersion(name, version))
      case (Some(id), None, None) => Right(Id(id))
      case (None, Some(name), None) => Right(Name(name))
      case _ =>
        Left(
          InvariantViolation(
            "package_name",
            "Either package_id must be set, or package_name and package_version must be set, or all three must be set.",
          )
        )
    }

  def fromProto(
      raw: package_management_service.VettedPackagesRef
  ): ParsingResult[VettedPackagesRef] =
    for {
      mbPackageId <- parseWith("package_id", raw.packageId, Ref.PackageId.fromString)
      mbPackageName <- parseWith("package_name", raw.packageName, Ref.PackageName.fromString)
      mbPackageVersion <- parseWith(
        "package_version",
        raw.packageVersion,
        Ref.PackageVersion.fromString,
      )
      result <- process(mbPackageId, mbPackageName, mbPackageVersion)
    } yield result
}

final case class SinglePackageTargetVetting[R](
    ref: R,
    bounds: Option[(Option[CantonTimestamp], Option[CantonTimestamp])],
) {
  def isVetting: Boolean = !isUnvetting
  def isUnvetting: Boolean = bounds.isEmpty
}

final case class EnrichedVettedPackage(
    vetted: VettedPackage,
    name: Option[Ref.PackageName],
    version: Option[Ref.PackageVersion],
) {
  def toProtoLAPI: package_reference.VettedPackage = package_reference.VettedPackage(
    vetted.packageId,
    validFromInclusive = vetted.validFromInclusive.map(_.toProtoTimestamp),
    validUntilExclusive = vetted.validUntilExclusive.map(_.toProtoTimestamp),
    packageName = name.map(_.toString).getOrElse(""),
    packageVersion = version.map(_.toString).getOrElse(""),
  )
}

sealed trait PriorTopologySerial {
  def toProtoLAPI: package_reference.PriorTopologySerial
}

final case class PriorTopologySerialExists(serial: Int) extends PriorTopologySerial {
  override def toProtoLAPI =
    package_reference.PriorTopologySerial(
      package_reference.PriorTopologySerial.Serial.Prior(serial)
    )
}

final case object PriorTopologySerialNone extends PriorTopologySerial {
  override def toProtoLAPI =
    package_reference.PriorTopologySerial(
      package_reference.PriorTopologySerial.Serial.NoPrior(com.google.protobuf.empty.Empty())
    )
}
