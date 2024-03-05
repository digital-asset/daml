// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.error.*
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error.{AlarmErrorCode, BaseAlarm, TransactionError}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{LocalReject, TransactionRejection}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.rpc.status.Status
import org.slf4j.event.Level

/** Base type for ErrorCodes related to LocalReject, if the rejection does not (necessarily) occur due to malicious behavior.
  */
abstract class LocalRejectErrorCode(
    id: String,
    category: ErrorCategory,
)(implicit parent: ErrorClass)
    extends ErrorCode(id, category) {
  override implicit val code: LocalRejectErrorCode = this
}

/** Base type for ErrorCodes related to LocalRejectError, if the rejection is due to malicious behavior.
  */
abstract class MalformedErrorCode(id: String)(implicit
    parent: ErrorClass
) extends AlarmErrorCode(id) {
  implicit override val code: MalformedErrorCode = this
}

sealed trait LocalRejectError
    extends TransactionError
    with TransactionRejection
    with PrettyPrinting
    with Product
    with Serializable {

  override def reason(): Status = rpcStatusWithoutLoggingContext()

  def toLocalReject(protocolVersion: ProtocolVersion): LocalReject =
    LocalReject.create(reason(), isMalformed = false, protocolVersion)

  /** The first part of the cause. Typically the same for all instances of the particular type.
    */
  // The leading underscore will exclude the field from the error context, so that it doesn't get logged twice.
  def _causePrefix: String

  /** The second part of the cause. Typically a class parameter.
    */
  def _details: String = ""

  override def cause: String = _causePrefix + _details

  override def code: ErrorCode

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

  override def pretty: Pretty[LocalRejectError] =
    prettyOfClass(
      param("code", _.code.id.unquoted),
      param("cause", _.cause.doubleQuoted),
      param("resources", _._resources.map(_.singleQuoted)),
      paramIfDefined("throwable", _.throwableO),
    )
}

/** Base class for LocalReject errors, if the rejection does not (necessarily) occur due to malicious behavior.
  */
sealed abstract class LocalRejectErrorImpl(
    override val _causePrefix: String,
    override val _details: String = "",
    override val throwableO: Option[Throwable] = None,
    override val _resourcesType: Option[ErrorResource] = None,
    override val _resources: Seq[String] = Seq.empty,
)(implicit override val code: LocalRejectErrorCode)
    extends LocalRejectError

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
    with LocalRejectError {
  override def toLocalReject(protocolVersion: ProtocolVersion): LocalReject =
    LocalReject.create(rpcStatusWithoutLoggingContext(), isMalformed = true, protocolVersion)

}

object LocalRejectError extends LocalRejectionGroup {

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
        ) {

      final case class Reject(override val _resources: Seq[String])
          extends LocalRejectErrorImpl(
            _causePrefix = s"Rejected transaction is referring to locked contracts ",
            _resourcesType = Some(ErrorResource.ContractId),
          )
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
        ) {
      final case class Reject(override val _resources: Seq[String])
          extends LocalRejectErrorImpl(
            _causePrefix = "Rejected transaction is referring to inactive contracts ",
            _resourcesType = Some(ErrorResource.ContractId),
          )
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
        ) {
      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix =
              "Rejected transaction as delta of the ledger time and the record time exceed the time tolerance "
          )
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
        ) {
      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix =
              "Rejected transaction as delta of the submission time and the record time exceed the time tolerance "
          )
    }

    @Explanation(
      """This rejection is sent if the participant locally determined a timeout."""
    )
    @Resolution("""In the first instance, resubmit your transaction.
                  | If the rejection still appears spuriously, consider increasing the `confirmationResponseTimeout` or
                  | `mediatorReactionTimeout` values in the `DynamicDomainParameters`.
                  | If the rejection appears unrelated to timeout settings, validate that the sequencer and mediator
                  | function correctly.
                  |""")
    object LocalTimeout
        extends LocalRejectErrorCode(
          id = "LOCAL_VERDICT_TIMEOUT",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      override def logLevel: Level = Level.WARN
      final case class Reject()
          extends LocalRejectErrorImpl(
            _causePrefix = "Rejected transaction due to a participant determined timeout "
          )

      val status: Status = Reject().rpcStatusWithoutLoggingContext()
    }

  }

  object MalformedRejects extends ErrorGroup() {

    @Explanation(
      """This rejection is made by a participant if a request is malformed."""
    )
    @Resolution("Please contact support.")
    object MalformedRequest
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_MALFORMED_REQUEST"
        ) {
      final case class Reject(override val _details: String) extends Malformed(_causePrefix = "")
    }

    @Explanation(
      """This rejection is made by a participant if a view of the transaction is malformed."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object Payloads
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_MALFORMED_PAYLOAD"
        ) {
      final case class Reject(override val _details: String)
          extends Malformed(
            _causePrefix = "Rejected transaction due to malformed payload within views "
          )
    }

    @Explanation(
      """This rejection is made by a participant if a transaction fails a model conformance check."""
    )
    @Resolution("This indicates either malicious or faulty behaviour.")
    object ModelConformance
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK"
        ) {
      final case class Reject(override val _details: String)
          extends Malformed(
            _causePrefix = "Rejected transaction due to a failed model conformance check: "
          )
    }

    @Explanation(
      """This rejection is made by a participant if a transaction does not contain valid root hash messages."""
    )
    @Resolution(
      "This indicates a race condition due to a in-flight topology change, or malicious or faulty behaviour."
    )
    object BadRootHashMessages
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_BAD_ROOT_HASH_MESSAGES"
        ) {
      final case class Reject(override val _details: String)
          extends Malformed(
            _causePrefix = "Rejected transaction due to bad root hash error messages. "
          )
    }

    @Explanation(
      """This error indicates that the transaction would create already existing contracts."""
    )
    @Resolution("This error indicates either faulty or malicious behaviour.")
    object CreatesExistingContracts
        extends MalformedErrorCode(
          id = "LOCAL_VERDICT_CREATES_EXISTING_CONTRACTS"
        ) {
      final case class Reject(override val _resources: Seq[String])
          extends Malformed(
            _causePrefix = "Rejected transaction would create contract(s) that already exist ",
            _resourcesType = Some(ErrorResource.ContractId),
          )
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
        ) {

      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(_causePrefix = "Activeness check failed.")
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
        ) {

      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix = "Rejected transfer as transferred contract is already archived. "
          )
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been made by another entity."""
    )
    object ContractAlreadyActive
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_CONTRACT_ALREADY_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
        ) {

      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix =
              "Rejected transfer as the contract is already active on the target domain. "
          )
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in is referring to an already locked contract."""
    )
    object ContractIsLocked
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_CONTRACT_IS_LOCKED",
          ErrorCategory.ContentionOnSharedResources,
        ) {

      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix = "Rejected transfer as the transferred contract is locked."
          )
    }

    @Explanation(
      """This rejection is emitted by a participant if a transfer-in has already been completed."""
    )
    object AlreadyCompleted
        extends LocalRejectErrorCode(
          id = "TRANSFER_IN_ALREADY_COMPLETED",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
        ) {

      final case class Reject(override val _details: String)
          extends LocalRejectErrorImpl(
            _causePrefix = "Rejected transfer as the transfer has already completed "
          )
    }
  }
}
