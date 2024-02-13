// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewParticipantData.RootAction
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransactionViewTree,
  TransactionView,
  ViewPosition,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.TransactionTreeConversionError
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.store.{
  ContractLookup,
  ContractLookupAndVerification,
  ExtendedContractLookup,
  StoredContract,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixes,
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfKeyResolver,
  LfPartyId,
  RequestCounter,
  checked,
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Allows for checking model conformance of a list of transaction view trees.
  * If successful, outputs the received transaction as LfVersionedTransaction along with TransactionMetadata.
  *
  * @param reinterpret reinterprets the lf command to a transaction.
  * @param transactionTreeFactory reconstructs a transaction view from the reinterpreted action description.
  */
class ModelConformanceChecker(
    val reinterpret: (
        ContractLookupAndVerification,
        Set[LfPartyId],
        LfCommand,
        CantonTimestamp,
        CantonTimestamp,
        Option[LfHash],
        Boolean,
        ViewHash,
        TraceContext,
    ) => EitherT[
      Future,
      DAMLeError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
    ],
    val validateContract: (
        SerializableContract,
        TraceContext,
    ) => EitherT[Future, ContractValidationFailure, Unit],
    val transactionTreeFactory: TransactionTreeFactory,
    participantId: ParticipantId,
    val serializableContractAuthenticator: SerializableContractAuthenticator,
    val enableContractUpgrading: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Reinterprets the transaction resulting from the received transaction view trees.
    *
    * @param rootViewTrees all received transaction view trees contained in a confirmation request that
    *                      have the same transaction id and represent a top-most view
    * @param keyResolverFor The key resolver to be used for re-interpreting root views
    * @param commonData the common data of all the (rootViewTree :  TransactionViewTree) trees in `rootViews`
    * @return the resulting LfTransaction with [[com.digitalasset.canton.protocol.LfContractId]]s only
    */
  def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      keyResolverFor: TransactionView => LfKeyResolver,
      requestCounter: RequestCounter,
      topologySnapshot: TopologySnapshot,
      commonData: CommonData,
  )(implicit traceContext: TraceContext): EitherT[Future, ErrorWithSubTransaction, Result] = {
    val CommonData(transactionId, ledgerTime, submissionTime, confirmationPolicy) = commonData

    // Previous checks in Phase 3 ensure that all the root views are sent to the same
    // mediator, and that they all have the same correct root hash, and therefore the
    // same CommonMetadata (which contains the UUID).
    val mediator = rootViewTrees.head1.mediator
    val transactionUuid = rootViewTrees.head1.transactionUuid

    def findValidSubtransactions(
        views: Seq[(TransactionView, ViewPosition, Option[ParticipantId])]
    ): Future[
      (
          Seq[Error],
          Seq[(TransactionView, WithRollbackScope[WellFormedTransaction[WithSuffixes]])],
      )
    ] = views
      .parTraverse { case (view, viewPos, submittingParticipantO) =>
        for {
          wfTxE <- checkView(
            view,
            viewPos,
            mediator,
            transactionUuid,
            keyResolverFor(view),
            requestCounter,
            ledgerTime,
            submissionTime,
            confirmationPolicy,
            submittingParticipantO,
            topologySnapshot,
          ).value

          errorsViewsTxs <- wfTxE match {
            case Right(wfTx) => Future.successful((Seq.empty, Seq((view, wfTx))))

            case Left(error) =>
              val subviewsWithInfo = view.subviews.unblindedElementsWithIndex.map {
                case (sv, svIndex) => (sv, svIndex +: viewPos, None)
              }

              findValidSubtransactions(subviewsWithInfo).map { case (subErrors, subViewsTxs) =>
                // If a view is not model conformant, all its ancestors are not either.
                // To avoid redundant errors, return this view's error only if the subviews are valid.
                val errors = if (subErrors.isEmpty) Seq(error) else subErrors

                (errors, subViewsTxs)
              }
          }
        } yield errorsViewsTxs
      }
      .map { aggregate =>
        val (errorsSeq, viewsTxsSeq) = aggregate.separate
        (errorsSeq.flatten, viewsTxsSeq.flatten)
      }

    val rootViewsWithInfo = rootViewTrees.map { viewTree =>
      val submittingParticipantO = viewTree.submitterMetadataO.map(_.submittingParticipant)
      (viewTree.view, viewTree.viewPosition, submittingParticipantO)
    }

    val resultFE = findValidSubtransactions(rootViewsWithInfo).map { case (errors, viewsTxs) =>
      val (views, txs) = viewsTxs.separate

      val wftxO = NonEmpty.from(txs) match {
        case Some(txsNE) =>
          WellFormedTransaction
            .merge(txsNE)
            .leftMap(mergeError =>
              // TODO(i14473): Handle failures to merge a transaction while preserving transparency
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Model conformance check failure when merging transaction $transactionId: $mergeError"
                )
              )
            )
            .toOption

        case None => None
      }

      NonEmpty.from(errors) match {
        case None =>
          wftxO match {
            case Some(wftx) => Right(Result(transactionId, wftx))
            case _ =>
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Model conformance check for transaction $transactionId completed successfully but without a valid transaction"
                )
              )
          }
        case Some(errorsNE) => Left(ErrorWithSubTransaction(errorsNE, wftxO, views))
      }
    }

    EitherT(resultFE)
  }

  private def validateInputContracts(
      view: TransactionView,
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Map[LfContractId, StoredContract]] = {
    view.tryFlattenToParticipantViews
      .flatMap(_.viewParticipantData.coreInputs)
      .parTraverse { case (cid, inputContract @ InputContract(contract, _)) =>
        validateContract(contract, traceContext)
          .leftMap {
            case DAMLeFailure(error) =>
              DAMLeError(error, view.viewHash): Error
            case ContractMismatch(actual, _) =>
              InvalidInputContract(cid, actual.templateId, view.viewHash): Error
          }
          .map(_ => cid -> StoredContract(contract, requestCounter, None))
      }
      .map(_.toMap)
  }

  private def checkView(
      view: TransactionView,
      viewPosition: ViewPosition,
      mediator: MediatorRef,
      transactionUuid: UUID,
      resolverFromView: LfKeyResolver,
      requestCounter: RequestCounter,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      confirmationPolicy: ConfirmationPolicy,
      submittingParticipantO: Option[ParticipantId],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, WithRollbackScope[WellFormedTransaction[WithSuffixes]]] = {
    val viewParticipantData = view.viewParticipantData.tryUnwrap
    val RootAction(cmd, authorizers, failed) =
      viewParticipantData.rootAction(enableContractUpgrading)
    val rbContext = viewParticipantData.rollbackContext
    val seed = viewParticipantData.actionDescription.seedOption
    for {
      viewInputContracts <- validateInputContracts(view, requestCounter)
      _ <- validatePackageVettings(view, topologySnapshot)
      contractLookupAndVerification =
        new ExtendedContractLookup(
          // all contracts and keys specified explicitly
          ContractLookup.noContracts(loggerFactory),
          viewInputContracts,
          resolverFromView,
          serializableContractAuthenticator,
        )
      lfTxAndMetadata <- reinterpret(
        contractLookupAndVerification,
        authorizers,
        cmd,
        ledgerTime,
        submissionTime,
        seed,
        failed,
        view.viewHash,
        traceContext,
      )
        .leftWiden[Error]
      (lfTx, metadata, resolverFromReinterpretation) = lfTxAndMetadata
      // For transaction views of protocol version 3 or higher,
      // the `resolverFromReinterpretation` is the same as the `resolverFromView`.
      // The `TransactionTreeFactoryImplV3` rebuilds the `resolverFromReinterpreation`
      // again by re-running the `ContractStateMachine` and checks consistency
      // with the reconstructed view's global key inputs,
      // which by the view equality check is the same as the `resolverFromView`.
      wfTx <- EitherT
        .fromEither[Future](
          WellFormedTransaction.normalizeAndCheck(lfTx, metadata, WithoutSuffixes)
        )
        .leftMap[Error](err => TransactionNotWellformed(err, view.viewHash))
      salts = transactionTreeFactory.saltsFromView(view)
      reconstructedViewAndTx <- checked(
        transactionTreeFactory.tryReconstruct(
          subaction = wfTx,
          rootPosition = viewPosition,
          rbContext = rbContext,
          confirmationPolicy = confirmationPolicy,
          mediator = mediator,
          submittingParticipantO = submittingParticipantO,
          salts = salts,
          transactionUuid = transactionUuid,
          topologySnapshot = topologySnapshot,
          contractOfId =
            TransactionTreeFactory.contractInstanceLookup(contractLookupAndVerification),
          keyResolver = resolverFromReinterpretation,
        )
      ).leftMap(err => TransactionTreeError(err, view.viewHash))
      (reconstructedView, suffixedTx) = reconstructedViewAndTx
      _ <- EitherT.cond[Future](
        view == reconstructedView,
        (),
        ViewReconstructionError(view, reconstructedView): Error,
      )

    } yield WithRollbackScope(rbContext.rollbackScope, suffixedTx)
  }

  private def validatePackageVettings(view: TransactionView, snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = {
    val referencedContracts =
      (view.inputContracts.fmap(_.contract) ++ view.createdContracts.fmap(_.contract)).values.toSet
    val packageIdsOfContracts =
      referencedContracts.map(_.contractInstance.unversioned.template.packageId)

    val packageIdsOfKeys = view.globalKeyInputs.keySet.flatMap(_.packageId)

    val packageIds = packageIdsOfContracts ++ packageIdsOfKeys

    val informees = view.viewCommonData.tryUnwrap.informees.map(_.party)

    EitherT(for {
      informeeParticipantsByParty <- snapshot.activeParticipantsOfParties(informees.toSeq)
      informeeParticipants = informeeParticipantsByParty.values.flatten.toSet
      unvettedResult <- informeeParticipants.toSeq
        .parTraverse(p => snapshot.findUnvettedPackagesOrDependencies(p, packageIds).map(p -> _))
        .value
      unvettedPackages = unvettedResult match {
        case Left(packageId) =>
          // The package is not in the store and thus the package is not vetted.
          // If the admin has tampered with the package store and the package is still vetted,
          // we consider this participant as malicious;
          // in that case, other participants may still commit the view.
          Seq(participantId -> Set(packageId))
        case Right(unvettedSeq) =>
          unvettedSeq.filter { case (_, packageIds) => packageIds.nonEmpty }
      }
    } yield {
      Either.cond(unvettedPackages.isEmpty, (), UnvettedPackages(unvettedPackages.toMap))
    })
  }

}

object ModelConformanceChecker {
  def apply(
      damle: DAMLe,
      transactionTreeFactory: TransactionTreeFactory,
      serializableContractAuthenticator: SerializableContractAuthenticator,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      enableContractUpgrading: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ModelConformanceChecker = {
    def reinterpret(
        contracts: ContractLookupAndVerification,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        submissionTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        expectFailure: Boolean,
        viewHash: ViewHash,
        traceContext: TraceContext,
    ): EitherT[Future, DAMLeError, (LfVersionedTransaction, TransactionMetadata, LfKeyResolver)] =
      damle
        .reinterpret(
          contracts,
          submitters,
          command,
          ledgerTime,
          submissionTime,
          rootSeed,
          expectFailure,
        )(traceContext)
        .leftMap(DAMLeError(_, viewHash))

    new ModelConformanceChecker(
      reinterpret,
      validateSerializedContract(damle),
      transactionTreeFactory,
      participantId,
      serializableContractAuthenticator,
      enableContractUpgrading,
      loggerFactory,
    )
  }

  private[validation] sealed trait ContractValidationFailure
  private[validation] final case class DAMLeFailure(error: engine.Error)
      extends ContractValidationFailure
  private[validation] final case class ContractMismatch(
      actual: LfNodeCreate,
      expected: LfNodeCreate,
  ) extends ContractValidationFailure

  private def validateSerializedContract(damle: DAMLe)(
      contract: SerializableContract,
      traceContext: TraceContext,
  )(implicit ec: ExecutionContext): EitherT[Future, ContractValidationFailure, Unit] = {

    val instance = contract.rawContractInstance
    val unversioned = instance.contractInstance.unversioned
    val metadata = contract.metadata

    for {
      actual <- damle
        .replayCreate(
          metadata.signatories,
          LfCreateCommand(unversioned.template, unversioned.arg),
          contract.ledgerCreateTime,
        )(traceContext)
        .leftMap(DAMLeFailure.apply)
      expected: LfNodeCreate = LfNodeCreate(
        // Do not validate the contract id. The validation would fail due to mismatching seed.
        // The contract id is already validated by SerializableContractAuthenticator,
        // as contract is an input contract of the underlying transaction.
        coid = actual.coid,
        packageName = unversioned.packageName,
        templateId = unversioned.template,
        arg = unversioned.arg,
        signatories = metadata.signatories,
        stakeholders = metadata.stakeholders,
        keyOpt = metadata.maybeKeyWithMaintainers,
        version = instance.contractInstance.version,
      )
      _ <- EitherT.cond[Future](
        actual == expected,
        (),
        ContractMismatch(actual, expected): ContractValidationFailure,
      )
    } yield ()

  }

  sealed trait Error extends PrettyPrinting

  /** Enriches a model conformance error with the valid subtransaction, if any.
    * If there is a valid subtransaction, the list of valid subview trees will not be empty.
    */
  final case class ErrorWithSubTransaction(
      errors: NonEmpty[Seq[Error]],
      validSubTransactionO: Option[WellFormedTransaction[WithSuffixesAndMerged]],
      validSubViews: Seq[TransactionView],
  ) extends PrettyPrinting {
    require(validSubTransactionO.isEmpty == validSubViews.isEmpty)

    override def pretty: Pretty[ErrorWithSubTransaction] = prettyOfClass(
      param("valid subtransaction", _.validSubTransactionO.toString.unquoted),
      param("valid subviews", _.validSubViews),
      param("errors", _.errors),
    )
  }

  /** Indicates that [[ModelConformanceChecker.reinterpret]] has failed. */
  final case class DAMLeError(cause: engine.Error, viewHash: ViewHash) extends Error {
    override def pretty: Pretty[DAMLeError] = adHocPrettyInstance
  }

  /** Indicates a different number of declared and reconstructed create nodes. */
  final case class CreatedContractsDeclaredIncorrectly(
      declaredCreateNodes: Seq[CreatedContract],
      reconstructedCreateNodes: Seq[LfNodeCreate],
      viewHash: ViewHash,
  ) extends Error {
    override def pretty: Pretty[CreatedContractsDeclaredIncorrectly] = prettyOfClass(
      param("declaredCreateNodes", _.declaredCreateNodes),
      param(
        "reconstructedCreateNodes",
        _.reconstructedCreateNodes.map(_.templateId),
      ),
      unnamedParam(_.viewHash),
    )
  }

  final case class TransactionNotWellformed(cause: String, viewHash: ViewHash) extends Error {
    override def pretty: Pretty[TransactionNotWellformed] = prettyOfClass(
      param("cause", _.cause.unquoted),
      unnamedParam(_.viewHash),
    )
  }

  final case class TransactionTreeError(
      details: TransactionTreeConversionError,
      viewHash: ViewHash,
  ) extends Error {

    def cause: String = "Failed to construct transaction tree."

    override def pretty: Pretty[TransactionTreeError] = prettyOfClass(
      param("cause", _.cause.unquoted),
      unnamedParam(_.details),
      unnamedParam(_.viewHash),
    )
  }

  final case class ViewReconstructionError(
      received: TransactionView,
      reconstructed: TransactionView,
  ) extends Error {

    def cause = "Reconstructed view differs from received view."

    override def pretty: Pretty[ViewReconstructionError] = prettyOfClass(
      param("cause", _.cause.unquoted),
      param("received", _.received),
      param("reconstructed", _.reconstructed),
    )
  }

  final case class JoinedTransactionNotWellFormed(
      cause: String
  ) extends Error {
    override def pretty: Pretty[JoinedTransactionNotWellFormed] = prettyOfClass(
      param("cause", _.cause.unquoted)
    )
  }

  final case class InvalidInputContract(
      contractId: LfContractId,
      templateId: Identifier,
      viewHash: ViewHash,
  ) extends Error {

    def cause =
      "Details of supplied contract to not match those that result from command reinterpretation"

    override def pretty: Pretty[InvalidInputContract] = prettyOfClass(
      param("cause", _.cause.unquoted),
      param("contractId", _.contractId),
      param("templateId", _.templateId),
      unnamedParam(_.viewHash),
    )
  }

  final case class UnvettedPackages(
      unvetted: Map[ParticipantId, Set[PackageId]]
  ) extends Error {
    override def pretty: Pretty[UnvettedPackages] = prettyOfClass(
      unnamedParam(
        _.unvetted
          .map { case (participant, packageIds) =>
            show"$participant has not vetted $packageIds".unquoted
          }
          .mkShow("\n")
      )
    )
  }

  final case class Result(
      transactionId: TransactionId,
      suffixedTransaction: WellFormedTransaction[WithSuffixesAndMerged],
  )

}
