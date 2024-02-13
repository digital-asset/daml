// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.error.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.LocalReject.MalformedRejects.CreatesExistingContracts
import com.digitalasset.canton.protocol.messages.LocalVerdict.protocolVersionRepresentativeFor
import com.digitalasset.canton.protocol.{messages, v30}
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
  private[messages] def toProtoV30: v30.LocalVerdict

  @transient override protected lazy val companionObj: LocalVerdict.type = LocalVerdict

  override def representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
}

object LocalVerdict extends HasProtocolVersionedCompanion[LocalVerdict] {

  override def name: String = getClass.getSimpleName

  override def supportedProtoVersions: messages.LocalVerdict.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.LocalVerdict)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  private[messages] def fromProtoV30(
      localVerdictP: v30.LocalVerdict
  ): ParsingResult[LocalVerdict] = {
    import v30.LocalVerdict.SomeLocalVerdict as Lv

    val protocolVersion = protocolVersionRepresentativeFor(ProtoVersion(30))
    val v30.LocalVerdict(someLocalVerdictP) = localVerdictP

    someLocalVerdictP match {
      case Lv.LocalApprove(empty.Empty(_)) => protocolVersion.map(LocalApprove()(_))
      case Lv.LocalReject(localRejectP) => LocalReject.fromProtoV30(localRejectP)
      case Lv.Empty =>
        Left(OtherError("Unable to deserialize LocalVerdict, as the content is empty"))
    }
  }
}

final case class LocalApprove()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LocalVerdict.type]
) extends LocalVerdict {
  private[messages] def toProtoV30: v30.LocalVerdict =
    v30.LocalVerdict(v30.LocalVerdict.SomeLocalVerdict.LocalApprove(empty.Empty()))

  override def pretty: Pretty[this.type] = prettyOfClass()
}

object LocalApprove {
  def apply(protocolVersion: ProtocolVersion): LocalApprove =
    LocalApprove()(LocalVerdict.protocolVersionRepresentativeFor(protocolVersion))
}

/** Base type for error codes related to local reject.
  */
trait BaseLocalRejectErrorCode {

  /** The code of a LocalReject
    * This is used to serialize rejections to LocalReject.
    */
  def v30CodeP: v30.LocalReject.Code
}

/** Base type for ErrorCodes related to LocalReject, if the rejection does not (necessarily) occur due to malicious behavior.
  */
abstract class LocalRejectErrorCode(
    id: String,
    category: ErrorCategory,
    override val v30CodeP: v30.LocalReject.Code,
)(implicit parent: ErrorClass)
    extends ErrorCode(id, category)
    with BaseLocalRejectErrorCode {
  override implicit val code: LocalRejectErrorCode = this
}

/** Base type for ErrorCodes related to LocalReject, if the rejection is due to malicious behavior.
  */
abstract class MalformedErrorCode(id: String, override val v30CodeP: v30.LocalReject.Code)(implicit
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
  override def code: ErrorCode & BaseLocalRejectErrorCode

  /** Make sure to define this, if _resources is non-empty.
    */
  def _resourcesType: Option[ErrorResource] = None

  /** The affected resources.
    * Will be logged as part of the context information.
    * If this error is converted to an rpc Status, this field is included as com.google.rpc.ResourceInfo.
    */
  def _resources: Seq[String] = Seq()

  override def resources: Seq[(ErrorResource, String)] =
    _resourcesType.fold(Seq.empty[(ErrorResource, String)])(rt => _resources.map(rs => (rt, rs)))

  protected[messages] def toProtoV30: v30.LocalVerdict =
    v30.LocalVerdict(v30.LocalVerdict.SomeLocalVerdict.LocalReject(toLocalRejectProtoV30))

  protected[messages] def toLocalRejectProtoV30: v30.LocalReject =
    v30.LocalReject(
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
  private[messages] def fromProtoV30(localRejectP: v30.LocalReject): ParsingResult[LocalReject] = {
    import ConsistencyRejections.*
    val v30.LocalReject(causePrefix, details, resource, errorCodeP, errorCategoryP) = localRejectP
    protocolVersionRepresentativeFor(ProtoVersion(30)).map { protocolVersion =>
      errorCodeP match {
        case LockedContracts.id => LockedContracts.Reject(resource)(protocolVersion)
        case InactiveContracts.id => InactiveContracts.Reject(resource)(protocolVersion)
        case CreatesExistingContracts.id =>
          CreatesExistingContracts.Reject(resource)(protocolVersion)
        case TimeRejects.LedgerTime.id =>
          TimeRejects.LedgerTime.Reject(details)(protocolVersion)
        case TimeRejects.SubmissionTime.id =>
          TimeRejects.SubmissionTime.Reject(details)(protocolVersion)
        case TimeRejects.LocalTimeout.id => TimeRejects.LocalTimeout.Reject()(protocolVersion)
        case MalformedRejects.MalformedRequest.id =>
          MalformedRejects.MalformedRequest.Reject(details)(protocolVersion)
        case MalformedRejects.Payloads.id =>
          MalformedRejects.Payloads.Reject(details)(protocolVersion)
        case MalformedRejects.ModelConformance.id =>
          MalformedRejects.ModelConformance.Reject(details)(protocolVersion)
        case MalformedRejects.BadRootHashMessages.id =>
          MalformedRejects.BadRootHashMessages.Reject(details)(protocolVersion)
        case TransferOutRejects.ActivenessCheckFailed.id =>
          TransferOutRejects.ActivenessCheckFailed.Reject(details)(protocolVersion)
        case TransferInRejects.AlreadyCompleted.id =>
          TransferInRejects.AlreadyCompleted.Reject(details)(protocolVersion)
        case TransferInRejects.ContractAlreadyActive.id =>
          TransferInRejects.ContractAlreadyActive.Reject(details)(protocolVersion)
        case TransferInRejects.ContractAlreadyArchived.id =>
          TransferInRejects.ContractAlreadyArchived.Reject(details)(protocolVersion)
        case TransferInRejects.ContractIsLocked.id =>
          TransferInRejects.ContractIsLocked.Reject(details)(protocolVersion)
        case id =>
          val category = ErrorCategory
            .fromInt(errorCategoryP)
            .getOrElse(ErrorCategory.SystemInternalAssumptionViolated)
          GenericReject(causePrefix, details, resource, id, category)(protocolVersion)
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
          v30.LocalReject.Code.CODE_LOCKED_CONTRACTS,
        ) {

      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = s"Rejected transaction is referring to locked contracts ",
            _resourcesType = Some(ErrorResource.ContractId),
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
          v30.LocalReject.Code.CODE_INACTIVE_CONTRACTS,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends LocalRejectImpl(
            _causePrefix = "Rejected transaction is referring to inactive contracts ",
            _resourcesType = Some(ErrorResource.ContractId),
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
          v30.LocalReject.Code.CODE_LEDGER_TIME,
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
          v30.LocalReject.Code.CODE_SUBMISSION_TIME,
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
          v30.LocalReject.Code.CODE_LEDGER_TIME,
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
          v30.LocalReject.Code.CODE_MALFORMED_PAYLOADS,
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
          v30.LocalReject.Code.CODE_MALFORMED_PAYLOADS,
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
          v30.LocalReject.Code.CODE_MALFORMED_MODEL,
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
          v30.LocalReject.Code.CODE_BAD_ROOT_HASH_MESSAGE,
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
          v30.LocalReject.Code.CODE_CREATES_EXISTING_CONTRACT,
        ) {
      final case class Reject(override val _resources: Seq[String])(
          override val representativeProtocolVersion: RepresentativeProtocolVersion[
            LocalVerdict.type
          ]
      ) extends Malformed(
            _causePrefix = "Rejected transaction would create contract(s) that already exist ",
            _resourcesType = Some(ErrorResource.ContractId),
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
          v30.LocalReject.Code.CODE_TRANSFER_OUT_ACTIVENESS_CHECK,
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
          v30.LocalReject.Code.CODE_TRANSFER_IN_ALREADY_ARCHIVED,
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
          v30.LocalReject.Code.CODE_TRANSFER_IN_ALREADY_ACTIVE,
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
          v30.LocalReject.Code.CODE_TRANSFER_IN_LOCKED,
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
          v30.LocalReject.Code.CODE_TRANSFER_IN_ALREADY_COMPLETED,
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
          v30.LocalReject.Code.CODE_LOCAL_TIMEOUT, // Using a dummy value, as this will not we used.
        ) {}
      )

  private object GenericReject {
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
