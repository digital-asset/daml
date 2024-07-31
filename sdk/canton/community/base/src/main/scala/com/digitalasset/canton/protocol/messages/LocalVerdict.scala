// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.error.*
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.LocalReject.MalformedRejects.CreatesExistingContracts
import com.digitalasset.canton.protocol.messages.LocalVerdict.protocolVersionRepresentativeFor
import com.digitalasset.canton.protocol.{messages, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.empty
import org.slf4j.event.Level

/** Possible verdicts on a transaction view from the participant's perspective.
  * The verdict can be `LocalApprove`, `LocalReject` or `Malformed`.
  * The verdicts `LocalReject` and `Malformed` include a `reason` pointing out which checks in Phase 3 have failed.
  */
sealed trait LocalVerdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[LocalVerdict] {
  private[messages] def toProtoV0: v0.LocalVerdict

  private[messages] def toProtoV1: v1.LocalVerdict

  @transient override protected lazy val companionObj: LocalVerdict.type = LocalVerdict

  override def representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
}

object LocalVerdict extends HasProtocolVersionedCompanion[LocalVerdict] {

  override def name: String = getClass.getSimpleName

  override def supportedProtoVersions: messages.LocalVerdict.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.LocalVerdict)(
        supportedProtoVersion(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.LocalVerdict)(
        supportedProtoVersion(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
    )

  private[messages] def fromProtoV0(
      localVerdictP: v0.LocalVerdict
  ): ParsingResult[LocalVerdict] = {
    import v0.LocalVerdict.SomeLocalVerdict as Lv

    localVerdictP match {
      case v0.LocalVerdict(Lv.LocalApprove(empty.Empty(_))) =>
        protocolVersionRepresentativeFor(ProtoVersion(0)).map(LocalApprove()(_))

      case v0.LocalVerdict(Lv.LocalReject(value)) => LocalReject.fromProtoV0(value)
      case v0.LocalVerdict(Lv.Empty) =>
        Left(OtherError("Unable to deserialize LocalVerdict, as the content is empty"))
    }
  }

  private[messages] def fromProtoV1(localVerdictP: v1.LocalVerdict): ParsingResult[LocalVerdict] = {
    import v1.LocalVerdict.SomeLocalVerdict as Lv

    val v1.LocalVerdict(someLocalVerdictP) = localVerdictP

    someLocalVerdictP match {
      case Lv.LocalApprove(empty.Empty(_)) =>
        protocolVersionRepresentativeFor(ProtoVersion(1)).map(LocalApprove()(_))
      case Lv.LocalReject(localRejectP) => LocalReject.fromProtoV1(localRejectP)
      case Lv.Empty =>
        Left(OtherError("Unable to deserialize LocalVerdict, as the content is empty"))
    }
  }
}

final case class LocalApprove()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict {
  private[messages] def toProtoV0: v0.LocalVerdict =
    v0.LocalVerdict(v0.LocalVerdict.SomeLocalVerdict.LocalApprove(empty.Empty()))

  private[messages] def toProtoV1: v1.LocalVerdict =
    v1.LocalVerdict(v1.LocalVerdict.SomeLocalVerdict.LocalApprove(empty.Empty()))

  override def pretty: Pretty[this.type] = prettyOfClass()
}

object LocalApprove {
  def apply(protocolVersion: ProtocolVersion): LocalApprove =
    LocalApprove()(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}

/** Base type for error codes related to local reject.
  */
trait BaseLocalRejectErrorCode {

  /** The code of a LocalReject in proto format v0.
    * This is used to serialize rejections to v0.LocalReject.
    */
  def v0CodeP: v0.LocalReject.Code
}

/** Base type for ErrorCodes related to LocalReject, if the rejection does not (necessarily) occur due to malicious behavior.
  */
abstract class LocalRejectErrorCode(
    id: String,
    category: ErrorCategory,
    override val v0CodeP: v0.LocalReject.Code,
)(implicit parent: ErrorClass)
    extends ErrorCode(id, category)
    with BaseLocalRejectErrorCode {
  override implicit val code: LocalRejectErrorCode = this
}

/** Base type for ErrorCodes related to LocalReject, if the rejection is due to malicious behavior.
  */
abstract class MalformedErrorCode(id: String, override val v0CodeP: v0.LocalReject.Code)(implicit
    parent: ErrorClass
) extends AlarmErrorCode(id)
    with BaseLocalRejectErrorCode {
  implicit override val code: MalformedErrorCode = this
}

sealed trait LocalReject extends LocalVerdict with TransactionError with TransactionRejection {

  /** The first part of the cause. Typically the same for all instances of the particular type.
    */
  // The leading underscore will exclude the field from the error context, so that it doesn't get logged twice.
  def _causePrefix: String

  /** The second part of the cause. Typically a class parameter.
    */
  def _details: String = ""

  override def cause: String = _causePrefix + _details

  // Make sure the ErrorCode has a v0CodeP.
  override def code: ErrorCode with BaseLocalRejectErrorCode

  /** Make sure to define this, if _resources is non-empty.
    */
  def _resourcesType: Option[ErrorResource] = None

  /** The affected resources.
    * It is used as follows:
    * - It will be logged as part of the context information.
    * - It may be included into the resulting LocalReject.
    *   The computation of LocalReject performs truncation so this may or may not be included.
    * - The LocalReject is sent via the sequencer to the mediator. Therefore: do not include any confidential data!
    * - The LocalReject is also output through the ledger API.
    */
  def _resources: Seq[String] = Seq()

  override def resources: Seq[(ErrorResource, String)] =
    _resourcesType.fold(Seq.empty[(ErrorResource, String)])(rt => _resources.map(rs => (rt, rs)))

  override def context: Map[String, String] =
    _resourcesType.map(_.asString -> _resources.show).toList.toMap ++ super.context

  protected[messages] def toProtoV0: v0.LocalVerdict =
    v0.LocalVerdict(v0.LocalVerdict.SomeLocalVerdict.LocalReject(toLocalRejectProtoV0))

  protected[messages] def toLocalRejectProtoV0: v0.LocalReject =
    v0.LocalReject(code.v0CodeP, _details, _resources)

  protected[messages] def toProtoV1: v1.LocalVerdict =
    v1.LocalVerdict(v1.LocalVerdict.SomeLocalVerdict.LocalReject(toLocalRejectProtoV1))

  protected[messages] def toLocalRejectProtoV1: v1.LocalReject =
    v1.LocalReject(
      causePrefix = _causePrefix,
      details = _details,
      resource = _resources,
      errorCode = code.id,
      errorCategory = code.category.asInt,
    )

  override def pretty: Pretty[LocalReject] =
    prettyOfClass(
      param("code", _.code.id.unquoted),
      param("causePrefix", _._causePrefix.doubleQuoted),
      param("details", _._details.doubleQuoted, _._details.nonEmpty),
      param("resources", _._resources.map(_.singleQuoted)),
      paramIfDefined("throwable", _.throwableO),
    )
}

/** Base class for LocalReject errors, if the rejection does not (necessarily) occur due to malicious behavior.
  */
sealed abstract class LocalRejectImpl(
    override val _causePrefix: String,
    override val _details: String = "",
    override val throwableO: Option[Throwable] = None,
    override val _resourcesType: Option[ErrorResource] = None,
    override val _resources: Seq[String] = Seq.empty,
)(implicit override val code: LocalRejectErrorCode)
    extends LocalReject

/** Base class for LocalReject errors, if the rejection occurs due to malicious behavior.
  */
sealed abstract class Malformed(
    override val _causePrefix: String,
    override val _details: String = "",
    override val throwableO: Option[Throwable] = None,
    override val _resourcesType: Option[ErrorResource] = None,
    override val _resources: Seq[String] = Seq.empty,
)(implicit
    override val code: MalformedErrorCode
) extends BaseAlarm
    with LocalReject

object LocalReject extends LocalRejectionGroup {

  // list of local errors, used to map them during transport
  // if you add a new error below, you must add it to this list here as well

  private[messages] def fromProtoV0(v: v0.LocalReject): ParsingResult[LocalReject] = {
    import ConsistencyRejections.*
    import v0.LocalReject.Code
    protocolVersionRepresentativeFor(ProtoVersion(0)).flatMap { rpv =>
      v.code match {
        case Code.MissingCode => Left(FieldNotSet("LocalReject.code"))
        case Code.LockedContracts => Right(LockedContracts.Reject(v.resource)(rpv))
        case Code.LockedKeys => Right(LockedKeys.Reject(v.resource)(rpv))
        case Code.InactiveContracts => Right(InactiveContracts.Reject(v.resource)(rpv))
        case Code.DuplicateKey => Right(DuplicateKey.Reject(v.resource)(rpv))
        case Code.CreatesExistingContract =>
          Right(CreatesExistingContracts.Reject(v.resource)(rpv))
        case Code.LedgerTime => Right(TimeRejects.LedgerTime.Reject(v.reason)(rpv))
        case Code.SubmissionTime =>
          Right(TimeRejects.SubmissionTime.Reject(v.reason)(rpv))
        case Code.LocalTimeout => Right(TimeRejects.LocalTimeout.Reject()(rpv))
        case Code.MalformedPayloads =>
          Right(MalformedRejects.Payloads.Reject(v.reason)(rpv))
        case Code.MalformedModel =>
          Right(MalformedRejects.ModelConformance.Reject(v.reason)(rpv))
        case Code.MalformedConfirmationPolicy =>
          // MalformedConfirmationPolicy could only occur up to v2.6.x with malicious participants.
          // The error code has been removed since.
          Left(
            ValueDeserializationError(
              "reject",
              s"Unknown local rejection error code ${v.code} with ${v.reason}",
            )
          )
        case Code.BadRootHashMessage =>
          Right(MalformedRejects.BadRootHashMessages.Reject(v.reason)(rpv))
        case Code.TransferOutActivenessCheck =>
          Right(TransferOutRejects.ActivenessCheckFailed.Reject(v.reason)(rpv))
        case Code.TransferInAlreadyCompleted =>
          Right(TransferInRejects.AlreadyCompleted.Reject(v.reason)(rpv))
        case Code.TransferInAlreadyActive =>
          Right(TransferInRejects.ContractAlreadyActive.Reject(v.reason)(rpv))
        case Code.TransferInAlreadyArchived =>
          Right(TransferInRejects.ContractAlreadyArchived.Reject(v.reason)(rpv))
        case Code.TransferInLocked =>
          Right(TransferInRejects.ContractIsLocked.Reject(v.reason)(rpv))
        case Code.InconsistentKey => Right(InconsistentKey.Reject(v.resource)(rpv))
        case Code.Unrecognized(code) =>
          Left(
            ValueDeserializationError(
              "reject",
              s"Unknown local rejection error code $code with ${v.reason}",
            )
          )
      }
    }
  }

  private[messages] def fromProtoV1(localRejectP: v1.LocalReject): ParsingResult[LocalReject] = {
    import ConsistencyRejections.*
    val v1.LocalReject(causePrefix, details, resource, errorCodeP, errorCategoryP) = localRejectP

    protocolVersionRepresentativeFor(ProtoVersion(1)).flatMap { rpv =>
      errorCodeP match {
        case LockedContracts.id => Right(LockedContracts.Reject(resource)(rpv))
        case LockedKeys.id => Right(LockedKeys.Reject(resource)(rpv))
        case InactiveContracts.id => Right(InactiveContracts.Reject(resource)(rpv))
        case DuplicateKey.id => Right(DuplicateKey.Reject(resource)(rpv))
        case CreatesExistingContracts.id =>
          Right(CreatesExistingContracts.Reject(resource)(rpv))
        case TimeRejects.LedgerTime.id =>
          Right(TimeRejects.LedgerTime.Reject(details)(rpv))
        case TimeRejects.SubmissionTime.id =>
          Right(TimeRejects.SubmissionTime.Reject(details)(rpv))
        case TimeRejects.LocalTimeout.id => Right(TimeRejects.LocalTimeout.Reject()(rpv))
        case MalformedRejects.MalformedRequest.id =>
          Right(MalformedRejects.MalformedRequest.Reject(details)(rpv))
        case MalformedRejects.Payloads.id =>
          Right(MalformedRejects.Payloads.Reject(details)(rpv))
        case MalformedRejects.ModelConformance.id =>
          Right(MalformedRejects.ModelConformance.Reject(details)(rpv))
        case MalformedRejects.BadRootHashMessages.id =>
          Right(MalformedRejects.BadRootHashMessages.Reject(details)(rpv))
        case TransferOutRejects.ActivenessCheckFailed.id =>
          Right(TransferOutRejects.ActivenessCheckFailed.Reject(details)(rpv))
        case TransferInRejects.AlreadyCompleted.id =>
          Right(TransferInRejects.AlreadyCompleted.Reject(details)(rpv))
        case TransferInRejects.ContractAlreadyActive.id =>
          Right(TransferInRejects.ContractAlreadyActive.Reject(details)(rpv))
        case TransferInRejects.ContractAlreadyArchived.id =>
          Right(TransferInRejects.ContractAlreadyArchived.Reject(details)(rpv))
        case TransferInRejects.ContractIsLocked.id =>
          Right(TransferInRejects.ContractIsLocked.Reject(details)(rpv))
        case InconsistentKey.id => Right(InconsistentKey.Reject(resource)(rpv))
        case id =>
          val category = ErrorCategory
            .fromInt(errorCategoryP)
            .getOrElse(ErrorCategory.SystemInternalAssumptionViolated)
          Right(GenericReject(causePrefix, details, resource, id, category)(rpv))
      }
    }

  }

  object ConsistencyRejections extends ErrorGroup() {
    @Explanation(
      """The transaction is referring to locked contracts which are in the process of being
        created, transferred, or archived by another transaction. If the other transaction fails, this transaction could be successfully retried."""
    )
    @Resolution("Retry the transaction")
    object LockedContracts
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_LOCKED_CONTRACTS",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LockedContracts,
        ) {

      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = s"Rejected transaction is referring to locked contracts ",
            _resourcesType = Some(CantonErrorResource.ContractId),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """The transaction is referring to locked keys which are in the process of being
        modified by another transaction."""
    )
    @Resolution("Retry the transaction")
    object LockedKeys
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_LOCKED_KEYS",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LockedKeys,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transaction is referring to locked keys ",
            _resourcesType = Some(CantonErrorResource.ContractKey),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """The transaction is referring to contracts that have either been previously
                                archived, transferred to another domain, or do not exist."""
    )
    @Resolution("Inspect your contract state and try a different transaction.")
    object InactiveContracts
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_INACTIVE_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.InactiveContracts,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transaction is referring to inactive contracts ",
            _resourcesType = Some(CantonErrorResource.ContractId),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """If the participant provides unique contract key support,
         this error will indicate that a transaction would create a unique key which already exists."""
    )
    @Resolution(
      "It depends on your use case and application whether and when retrying makes sense or not."
    )
    object DuplicateKey
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_DUPLICATE_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.DuplicateKey,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      )
      // Error message contains the term: "Inconsistent" and "DuplicateKey" to avoid failing contract key ledger api conformance tests
          extends LocalRejectImpl(
            _causePrefix =
              "Inconsistent rejected transaction would create a key that already exists (DuplicateKey) ",
            _resourcesType = Some(CantonErrorResource.ContractKey),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """If the participant provides unique contract key support,
         this error will indicate that a transaction expected a key to be unallocated, but a contract for the key already exists."""
    )
    @Resolution(
      "It depends on your use case and application whether and when retrying makes sense or not."
    )
    object InconsistentKey
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_INCONSISTENT_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.InconsistentKey,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix =
              "Inconsistent rejected transaction expected unassigned key, which already exists ",
            _resourcesType = Some(CantonErrorResource.ContractKey),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }
  }

  object TimeRejects extends ErrorGroup() {

    @Explanation(
      """This error is thrown if the ledger time and the record time differ more than permitted.
        This can happen in an overloaded system due to high latencies or for transactions with long interpretation times."""
    )
    @Resolution(
      "For long-running transactions, specify a ledger time with the command submission or adjust the dynamic domain parameter ledgerTimeRecordTimeTolerance (and possibly the participant and mediator reaction timeout). For short-running transactions, simply retry."
    )
    object LedgerTime
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_LEDGER_TIME_OUT_OF_BOUND",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LedgerTime,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix =
              "Rejected transaction as delta of the ledger time and the record time exceed the time tolerance "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This error is thrown if the submission time and the record time differ more than permitted.
        This can happen in an overloaded system due to high latencies or for transactions with long interpretation times."""
    )
    @Resolution(
      "For long-running transactions, adjust the ledger time bounds used with the command submission. For short-running transactions, simply retry."
    )
    object SubmissionTime
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_SUBMISSION_TIME_OUT_OF_BOUND",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.SubmissionTime,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix =
              "Rejected transaction as delta of the submission time and the record time exceed the time tolerance "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is sent if the participant locally determined a timeout."""
    )
    @Resolution("""In the first instance, resubmit your transaction.
        | If the rejection still appears spuriously, consider increasing the `participantResponseTimeout` or
        | `mediatorReactionTimeout` values in the `DynamicDomainParameters`.
        | If the rejection appears unrelated to timeout settings, validate that the sequencer and mediator
        | function correctly.
        |""")
    object LocalTimeout
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_TIMEOUT",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.LocalTimeout,
        ) {
      override def logLevel: Level = Level.WARN
      final case class Reject()(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transaction due to a participant determined timeout "
          )

      object Reject {
        def apply(protocolVersion: ProtocolVersion): Reject =
          Reject()(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

  }

  object MalformedRejects extends ErrorGroup() {

    @Explanation(
      """This rejection is made by a participant if a request is malformed."""
    )
    @Resolution("Please contact support.")
    object MalformedRequest
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_MALFORMED_REQUEST",
          v0.LocalReject.Code.MalformedPayloads,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(_causePrefix = "")

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is made by a participant if a view of the transaction is malformed."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object Payloads
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_MALFORMED_PAYLOAD",
          v0.LocalReject.Code.MalformedPayloads,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(
            _causePrefix = "Rejected transaction due to malformed payload within views "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is made by a participant if a transaction fails a model conformance check."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object ModelConformance
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK",
          v0.LocalReject.Code.MalformedModel,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(
            _causePrefix = "Rejected transaction due to a failed model conformance check: "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is made by a participant if a transaction does not contain valid root hash messages."""
    )
    @Resolution(
      "This indicates a race condition due to a in-flight topology change, or malicious or faulty behaviour."
    )
    object BadRootHashMessages
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_BAD_ROOT_HASH_MESSAGES",
          v0.LocalReject.Code.BadRootHashMessage,
        ) {
      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(
            _causePrefix = "Rejected transaction due to bad root hash error messages. "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This error indicates that the transaction would create already existing contracts."""
    )
    @Resolution("This error indicates either faulty or malicious behaviour.")
    object CreatesExistingContracts
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_CREATES_EXISTING_CONTRACTS",
          v0.LocalReject.Code.CreatesExistingContract,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(
            _causePrefix = "Rejected transaction would create contract(s) that already exist ",
            _resourcesType = Some(CantonErrorResource.ContractId),
          )

      object Reject {
        def apply(resources: Seq[String], protocolVersion: ProtocolVersion): Reject =
          Reject(resources)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }
  }

  object TransferOutRejects extends ErrorGroup() {

    @Explanation(
      """Activeness check failed for transfer out submission. This rejection occurs if the contract to be
        |transferred has already been transferred or is currently locked (due to a competing transaction)
        |on  domain."""
    )
    @Resolution(
      "Depending on your use-case and your expectation, retry the transaction."
    )
    object ActivenessCheckFailed
        extends LocalRejectErrorCode(
          id = "TRANSFER_OUT_ACTIVENESS_CHECK_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.TransferOutActivenessCheck,
        ) {

      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(_causePrefix = "Activeness check failed.")

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

  }

  object TransferInRejects extends ErrorGroup() {
    @Explanation(
      """This rejection is emitted by a participant if a transfer would be invoked on an already archived contract."""
    )
    object ContractAlreadyArchived
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_CONTRACT_ALREADY_ARCHIVED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          v0.LocalReject.Code.TransferInAlreadyArchived,
        ) {

      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transfer as transferred contract is already archived. "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been made by another entity."""
    )
    object ContractAlreadyActive
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_CONTRACT_ALREADY_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.TransferInAlreadyActive,
        ) {

      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix =
              "Rejected transfer as the contract is already active on the target domain. "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in is referring to an already locked contract."""
    )
    object ContractIsLocked
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_CONTRACT_IS_LOCKED",
          ErrorCategory.ContentionOnSharedResources,
          v0.LocalReject.Code.TransferInLocked,
        ) {

      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transfer as the transferred contract is locked."
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been completed."""
    )
    object AlreadyCompleted
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_ALREADY_COMPLETED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
          v0.LocalReject.Code.TransferInAlreadyCompleted,
        ) {

      final case class Reject(override val _details: String)(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transfer as the transfer has already completed "
          )

      object Reject {
        def apply(details: String, protocolVersion: ProtocolVersion): Reject =
          Reject(details)(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
      }
    }
  }

  /** Fallback for deserializing local rejects that are not known to the current Canton version.
    * Should not be serialized.
    */
  final case class GenericReject(
      override val _causePrefix: String,
      override val _details: String,
      override val _resources: Seq[String],
      id: String,
      category: ErrorCategory,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
  ) extends LocalRejectImpl(
        _causePrefix = _causePrefix,
        // Append _resources to details, because we don't know _resourcesType and the _resources field is ignored if _resourcesType is None.
        _details = _details + _resources.mkString(", "),
      )(
        new LocalRejectErrorCode(
          id,
          category,
          v0.LocalReject.Code.LocalTimeout, // Using a dummy value, as this will not we used.
        ) {}
      )

  object GenericReject {
    def apply(
        causePrefix: String,
        details: String,
        resources: Seq[String],
        id: String,
        category: ErrorCategory,
        protocolVersion: ProtocolVersion,
    ): GenericReject =
      GenericReject(causePrefix, details, resources, id, category)(
        LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)
      )
  }
}
