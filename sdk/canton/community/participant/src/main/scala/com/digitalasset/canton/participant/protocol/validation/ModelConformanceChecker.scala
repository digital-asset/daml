// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Eval
import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.InteractiveSubmission.*
import com.digitalasset.canton.crypto.{Hash, InteractiveSubmission}
import com.digitalasset.canton.data.ViewParticipantData.RootAction
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransactionViewTree,
  SubmitterMetadata,
  TransactionView,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.TransactionTreeConversionError
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.{
  ConflictingNameBindings,
  PackageNotFound,
  *,
}
import com.digitalasset.canton.participant.store.{
  ContractLookup,
  ExtendedContractLookup,
  StoredContract,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.{
  HasReinterpret,
  PackageResolver,
  ReInterpretationResult,
  TransactionEnricher,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixes,
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.hash.HashTracer.NoOp
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{LfCreateCommand, LfKeyResolver, LfPartyId, RequestCounter, checked}
import com.digitalasset.daml.lf.data.Ref.{CommandId, Identifier, PackageId, PackageName}

import java.util.UUID
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** Allows for checking model conformance of a list of transaction view trees.
  * If successful, outputs the received transaction as LfVersionedTransaction along with TransactionMetadata.
  *
  * @param reinterpreter reinterprets the lf command to a transaction.
  * @param transactionTreeFactory reconstructs a transaction view from the reinterpreted action description.
  */
class ModelConformanceChecker(
    val reinterpreter: HasReinterpret,
    val validateContract: SerializableContractValidation,
    val transactionTreeFactory: TransactionTreeFactory,
    val participantId: ParticipantId,
    val serializableContractAuthenticator: SerializableContractAuthenticator,
    val packageResolver: PackageResolver,
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
  private[protocol] def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
      keyResolverFor: TransactionView => LfKeyResolver,
      requestCounter: RequestCounter,
      topologySnapshot: TopologySnapshot,
      commonData: CommonData,
      getEngineAbortStatus: GetEngineAbortStatus,
      reInterpretedTopLevelViews: LazyAsyncReInterpretation,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ErrorWithSubTransaction, Result] = {
    val CommonData(transactionId, ledgerTime, submissionTime) = commonData

    // Previous checks in Phase 3 ensure that all the root views are sent to the same
    // mediator, and that they all have the same correct root hash, and therefore the
    // same CommonMetadata (which contains the UUID).
    val mediator = rootViewTrees.head1.mediator
    val transactionUuid = rootViewTrees.head1.transactionUuid

    def findValidSubtransactions(
        views: Seq[(TransactionView, ViewPosition, Option[SubmitterMetadata])]
    ): FutureUnlessShutdown[
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
            submittingParticipantO,
            topologySnapshot,
            getEngineAbortStatus,
            reInterpretedTopLevelViews,
          ).value

          errorsViewsTxs <- wfTxE match {
            case Right(wfTx) => FutureUnlessShutdown.pure((Seq.empty, Seq((view, wfTx))))

            // There is no point in checking subviews if we have aborted
            case Left(error @ DAMLeError(DAMLe.EngineAborted(_), _)) =>
              FutureUnlessShutdown.pure((Seq(error), Seq.empty))

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
      (viewTree.view, viewTree.viewPosition, viewTree.submitterMetadataO)
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
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Map[LfContractId, StoredContract]] =
    view.tryFlattenToParticipantViews
      .flatMap(_.viewParticipantData.coreInputs)
      .parTraverse { case (cid, InputContract(contract, _)) =>
        validateContract(contract, getEngineAbortStatus, traceContext)
          .leftMap {
            case DAMLeFailure(error) =>
              DAMLeError(error, view.viewHash): Error
            case ContractMismatch(actual, _) =>
              InvalidInputContract(cid, actual.templateId, view.viewHash): Error
          }
          .map(_ => cid -> StoredContract(contract, requestCounter, isDivulged = true))
      }
      .map(_.toMap)
      .mapK(FutureUnlessShutdown.outcomeK)

  private def buildPackageNameMap(
      packageIds: Set[PackageId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Map[PackageName, PackageId]] =
    EitherT(for {
      resolvedE <- packageIds.toSeq.parTraverse(pId =>
        packageResolver(pId)(traceContext)
          .map {
            case None => Left(pId)
            case Some(ast) => Right((pId, ast.metadata.name))
          }
      )
    } yield {
      for {
        resolved <- resolvedE.separate match {
          case (Seq(), resolved) => Right(resolved)
          case (unresolved, _) =>
            Left(PackageNotFound(Map(participantId -> unresolved.toSet)): Error)
        }
        resolvedNameBindings = resolved.map { case (pId, name) => name -> pId }
        nameBindings <- MapsUtil.toNonConflictingMap(resolvedNameBindings) leftMap { conflicts =>
          ConflictingNameBindings(Map(participantId -> conflicts))
        }
      } yield nameBindings
    }).mapK(FutureUnlessShutdown.outcomeK)

  def reInterpret(
      view: TransactionView,
      resolverFromView: LfKeyResolver,
      requestCounter: RequestCounter,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, ConformanceReInterpretationResult] = {
    val viewParticipantData = view.viewParticipantData.tryUnwrap

    val RootAction(cmd, authorizers, failed, packageIdPreference) =
      viewParticipantData.rootAction

    val seed = viewParticipantData.actionDescription.seedOption
    for {
      viewInputContracts <- validateInputContracts(view, requestCounter, getEngineAbortStatus)

      contractLookupAndVerification =
        new ExtendedContractLookup(
          // all contracts and keys specified explicitly
          ContractLookup.noContracts(loggerFactory),
          viewInputContracts,
          resolverFromView,
          serializableContractAuthenticator,
        )

      packagePreference <- buildPackageNameMap(packageIdPreference)

      lfTxAndMetadata <- reinterpreter
        .reinterpret(
          contractLookupAndVerification,
          authorizers,
          cmd,
          ledgerTime,
          submissionTime,
          seed,
          packagePreference,
          failed,
          getEngineAbortStatus,
        )(traceContext)
        .leftMap(DAMLeError(_, view.viewHash))
        .leftWiden[Error]
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ConformanceReInterpretationResult(
      lfTxAndMetadata,
      contractLookupAndVerification,
      viewInputContracts,
    )
  }

  private def checkView(
      view: TransactionView,
      viewPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
      transactionUuid: UUID,
      resolverFromView: LfKeyResolver,
      requestCounter: RequestCounter,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      submitterMetadataO: Option[SubmitterMetadata],
      topologySnapshot: TopologySnapshot,
      getEngineAbortStatus: GetEngineAbortStatus,
      reInterpretedTopLevelViewsET: LazyAsyncReInterpretation,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, WithRollbackScope[
    WellFormedTransaction[WithSuffixes]
  ]] = {
    val submittingParticipantO = submitterMetadataO.map(_.submittingParticipant)
    val viewParticipantData = view.viewParticipantData.tryUnwrap

    val rbContext = viewParticipantData.rollbackContext
    for {
      // If we already have the re-interpreted view then re-use it
      lfTxAndMetadata <- reInterpretedTopLevelViewsET
        .get(view.viewHash)
        .map(_.value)
        .getOrElse(
          reInterpret(
            view,
            resolverFromView,
            requestCounter,
            ledgerTime,
            submissionTime,
            getEngineAbortStatus,
          )
        )

      ConformanceReInterpretationResult(
        ReInterpretationResult(
          lfTx,
          metadata,
          resolverFromReinterpretation,
          usedPackages,
          _,
        ),
        contractLookupAndVerification,
        _,
      ) = lfTxAndMetadata

      _ <- checkPackageVetting(view, topologySnapshot, usedPackages, metadata.ledgerTime)

      // For transaction views of protocol version 3 or higher,
      // the `resolverFromReinterpretation` is the same as the `resolverFromView`.
      // The `TransactionTreeFactoryImplV3` rebuilds the `resolverFromReinterpretation`
      // again by re-running the `ContractStateMachine` and checks consistency
      // with the reconstructed view's global key inputs,
      // which by the view equality check is the same as the `resolverFromView`.
      wfTxE = WellFormedTransaction
        .normalizeAndCheck(lfTx, metadata, WithoutSuffixes)
        .leftMap[Error](err => TransactionNotWellFormed(err, view.viewHash))

      wfTx <- EitherT(FutureUnlessShutdown.pure(wfTxE))

      salts = transactionTreeFactory.saltsFromView(view)

      reconstructedViewAndTx <- checked(
        transactionTreeFactory.tryReconstruct(
          subaction = wfTx,
          rootPosition = viewPosition,
          rbContext = rbContext,
          mediator = mediator,
          submittingParticipantO = submittingParticipantO,
          salts = salts,
          transactionUuid = transactionUuid,
          topologySnapshot = topologySnapshot,
          contractOfId =
            TransactionTreeFactory.contractInstanceLookup(contractLookupAndVerification),
          keyResolver = resolverFromReinterpretation,
        )
      ).leftMap(err => TransactionTreeError(err, view.viewHash)).mapK(FutureUnlessShutdown.outcomeK)

      (reconstructedView, suffixedTx) = reconstructedViewAndTx

      _ <- EitherT.cond[FutureUnlessShutdown](
        view == reconstructedView,
        (),
        ViewReconstructionError(view, reconstructedView): Error,
      )

    } yield WithRollbackScope(rbContext.rollbackScope, suffixedTx)
  }

  private def checkPackageVetting(
      view: TransactionView,
      snapshot: TopologySnapshot,
      packageIds: Set[PackageId],
      ledgerTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Error, Unit] = {

    val informees = view.viewCommonData.tryUnwrap.viewConfirmationParameters.informees

    EitherT(for {
      informeeParticipantsByParty <- FutureUnlessShutdown.outcomeF(
        snapshot.activeParticipantsOfParties(informees.toSeq)
      )
      informeeParticipants = informeeParticipantsByParty.values.flatten.toSet
      unvetted <- informeeParticipants.toSeq
        .parTraverse(p =>
          snapshot
            .findUnvettedPackagesOrDependencies(p, packageIds, ledgerTime)
            .map(p -> _)
        )

    } yield {
      val unvettedPackages = unvetted.filter { case (_, packageIds) => packageIds.nonEmpty }
      Either.cond(unvettedPackages.isEmpty, (), UnvettedPackages(unvettedPackages.toMap))
    })
  }

}

object ModelConformanceChecker {

  def apply(
      damlE: DAMLe,
      transactionTreeFactory: TransactionTreeFactory,
      serializableContractAuthenticator: SerializableContractAuthenticator,
      participantId: ParticipantId,
      packageResolver: PackageResolver,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ModelConformanceChecker =
    new ModelConformanceChecker(
      damlE,
      validateSerializedContract(damlE),
      transactionTreeFactory,
      participantId,
      serializableContractAuthenticator,
      packageResolver,
      loggerFactory,
    )

  // Type alias for a temporary caching of re-interpreted top views to be used both for authentication of externally
  // signed transaction and model conformance checker
  type LazyAsyncReInterpretation = Map[
    ViewHash,
    Eval[EitherT[
      FutureUnlessShutdown,
      ModelConformanceChecker.Error,
      ModelConformanceChecker.ConformanceReInterpretationResult,
    ]],
  ]
  private[protocol] final case class ConformanceReInterpretationResult(
      reInterpretationResult: ReInterpretationResult,
      contractLookup: ExtendedContractLookup,
      viewInputContracts: Map[LfContractId, StoredContract],
  ) {

    /** Compute the hash of a re-interpreted transaction to validate external signatures.
      * Note that we need to enrich the transaction to re-hydrate the record values with labels, since they're part of the hash
      */
    def computeHash(
        hashingSchemeVersion: HashingSchemeVersion,
        actAs: Set[LfPartyId],
        commandId: CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        domainId: DomainId,
        protocolVersion: ProtocolVersion,
        transactionEnricher: TransactionEnricher,
    )(implicit
        traceContext: TraceContext,
        ec: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, Hash] =
      for {
        enrichedTransaction <- transactionEnricher(
          reInterpretationResult.transaction
        )(traceContext)
          .leftMap(_.toString)
        hash <- EitherT.fromEither[FutureUnlessShutdown](
          InteractiveSubmission
            .computeVersionedHash(
              hashingSchemeVersion,
              enrichedTransaction,
              InteractiveSubmission.TransactionMetadataForHashing(
                actAs,
                commandId,
                transactionUUID,
                mediatorGroup,
                domainId,
                Option.when(reInterpretationResult.usesLedgerTime)(
                  reInterpretationResult.metadata.ledgerTime.toLf
                ),
                reInterpretationResult.metadata.submissionTime.toLf,
                SortedMap.from(
                  viewInputContracts.map { case (cid, storedContract) =>
                    cid -> storedContract.contract
                  }
                ),
              ),
              reInterpretationResult.metadata.seeds,
              protocolVersion,
              hashTracer = NoOp,
            )
            .leftMap(_.message)
        )
      } yield hash
  }

  private[validation] sealed trait ContractValidationFailure
  private[validation] final case class DAMLeFailure(error: DAMLe.ReinterpretationError)
      extends ContractValidationFailure
  private[validation] final case class ContractMismatch(
      actual: LfNodeCreate,
      expected: LfNodeCreate,
  ) extends ContractValidationFailure

  private type SerializableContractValidation =
    (
        SerializableContract,
        GetEngineAbortStatus,
        TraceContext,
    ) => EitherT[Future, ContractValidationFailure, Unit]

  private def validateSerializedContract(damlE: DAMLe)(
      contract: SerializableContract,
      getEngineAbortStatus: GetEngineAbortStatus,
      traceContext: TraceContext,
  )(implicit ec: ExecutionContext): EitherT[Future, ContractValidationFailure, Unit] = {

    val instance = contract.rawContractInstance
    val unversioned = instance.contractInstance.unversioned
    val metadata = contract.metadata

    for {
      actual <- damlE
        .replayCreate(
          metadata.signatories,
          LfCreateCommand(unversioned.template, unversioned.arg),
          contract.ledgerCreateTime,
          getEngineAbortStatus,
        )(traceContext)
        .leftMap(DAMLeFailure.apply)
      expected: LfNodeCreate = LfNodeCreate(
        // Do not validate the contract id. The validation would fail due to mismatching seed.
        // The contract id is already validated by SerializableContractAuthenticator,
        // as contract is an input contract of the underlying transaction.
        coid = actual.coid,
        packageName = unversioned.packageName,
        packageVersion = unversioned.packageVersion,
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

    override protected def pretty: Pretty[ErrorWithSubTransaction] = prettyOfClass(
      param("valid subtransaction", _.validSubTransactionO.toString.unquoted),
      param("valid subviews", _.validSubViews),
      param("errors", _.errors),
      param("engine abort status", _.engineAbortStatus),
    )

    // The request computation was aborted if any error is an abort
    lazy val (engineAbortStatus, nonAbortErrors) = {
      val (abortReasons, nonAbortErrors) = errors.partitionMap {
        case DAMLeError(DAMLe.EngineAborted(reason), _) => Left(reason)
        case error => Right(error)
      }

      val abortStatus = EngineAbortStatus(abortReasons.headOption) // Keep the first one as relevant

      (abortStatus, nonAbortErrors)
    }
  }

  /** Indicates that [[ModelConformanceChecker.reinterpreter]] has failed. */
  final case class DAMLeError(cause: DAMLe.ReinterpretationError, viewHash: ViewHash)
      extends Error {
    override protected def pretty: Pretty[DAMLeError] = prettyOfClass(
      param("cause", _.cause),
      param("view hash", _.viewHash),
    )
  }

  final case class TransactionNotWellFormed(cause: String, viewHash: ViewHash) extends Error {
    override protected def pretty: Pretty[TransactionNotWellFormed] = prettyOfClass(
      param("cause", _.cause.unquoted),
      unnamedParam(_.viewHash),
    )
  }

  final case class TransactionTreeError(
      details: TransactionTreeConversionError,
      viewHash: ViewHash,
  ) extends Error {

    def cause: String = "Failed to construct transaction tree."

    override protected def pretty: Pretty[TransactionTreeError] = prettyOfClass(
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

    override protected def pretty: Pretty[ViewReconstructionError] = prettyOfClass(
      param("cause", _.cause.unquoted),
      param("received", _.received),
      param("reconstructed", _.reconstructed),
    )
  }

  final case class InvalidInputContract(
      contractId: LfContractId,
      templateId: Identifier,
      viewHash: ViewHash,
  ) extends Error {

    def cause =
      "Details of supplied contract to not match those that result from command reinterpretation"

    override protected def pretty: Pretty[InvalidInputContract] = prettyOfClass(
      param("cause", _.cause.unquoted),
      param("contractId", _.contractId),
      param("templateId", _.templateId),
      unnamedParam(_.viewHash),
    )
  }

  final case class UnvettedPackages(
      unvetted: Map[ParticipantId, Set[PackageId]]
  ) extends Error {
    override protected def pretty: Pretty[UnvettedPackages] = prettyOfClass(
      unnamedParam(
        _.unvetted
          .map { case (participant, packageIds) =>
            show"$participant has not vetted $packageIds".unquoted
          }
          .mkShow("\n")
      )
    )
  }

  final case class PackageNotFound(
      missing: Map[ParticipantId, Set[PackageId]]
  ) extends Error {
    override protected def pretty: Pretty[PackageNotFound] = prettyOfClass(
      unnamedParam(
        _.missing
          .map { case (participant, packageIds) =>
            show"$participant can not find $packageIds".unquoted
          }
          .mkShow("\n")
      )
    )
  }

  final case class ConflictingNameBindings(
      conflicting: Map[ParticipantId, Map[PackageName, Set[PackageId]]]
  ) extends Error {
    override protected def pretty: Pretty[ConflictingNameBindings] = prettyOfClass(
      unnamedParam(
        _.conflicting
          .map { case (participant, conflicts) =>
            show"$participant has detected conflicting package name resolutions $conflicts".unquoted
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
