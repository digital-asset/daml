// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Eval
import cats.data.EitherT
import cats.implicits.{toFoldableOps, toFunctorOps}
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashOps, HmacOps, InteractiveSubmission}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewParticipantData.RootAction
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.TransactionTreeConversionError
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.store.ExtendedContractLookup
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.*
import com.digitalasset.canton.platform.store.dao.events.InputContractPackages
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.{
  ContractIdAbsolutizationDataV1,
  ContractIdAbsolutizationDataV2,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ContractValidator, ErrorUtil, RoseTree}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}
import com.digitalasset.daml.lf.data.Ref.{CommandId, PackageId, PackageName}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Allows for checking model conformance of a list of transaction view trees. If successful,
  * outputs the received transaction as LfVersionedTransaction along with TransactionMetadata.
  *
  * @param reinterpreter
  *   reinterprets the lf command to a transaction.
  * @param transactionTreeFactory
  *   reconstructs a transaction view from the reinterpreted action description.
  */
class ModelConformanceChecker(
    reinterpreter: HasReinterpret,
    transactionTreeFactory: TransactionTreeFactory,
    participantId: ParticipantId,
    contractValidator: ContractValidator,
    packageResolver: PackageResolver,
    hashOps: HashOps & HmacOps,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Reinterprets the transaction resulting from the received transaction view trees.
    *
    * @param rootViewTrees
    *   all received transaction view trees contained in a confirmation request that have the same
    *   transaction id and represent a top-most view
    * @param keyResolverFor
    *   The key resolver to be used for re-interpreting root views
    * @param commonData
    *   the common data of all the (rootViewTree : TransactionViewTree) trees in `rootViews`
    * @return
    *   the resulting LfTransaction with [[com.digitalasset.canton.protocol.LfContractId]]s only
    */
  private[protocol] def check[ViewEffect](
      rootViewTrees: NonEmpty[Seq[(FullTransactionViewTree, RoseTree[ViewEffect])]],
      keyResolverFor: TransactionView => LfKeyResolver,
      topologySnapshot: TopologySnapshot,
      commonData: CommonData,
      getEngineAbortStatus: GetEngineAbortStatus,
      reInterpretedTopLevelViews: LazyAsyncReInterpretationMap,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ErrorWithSubTransaction[ViewEffect], Result] = {
    val CommonData(updateId, ledgerTime, preparationTime) = commonData

    // Previous checks in Phase 3 ensure that all the root views are sent to the same
    // mediator, and that they all have the same correct root hash, and therefore the
    // same CommonMetadata (which contains the UUID).
    val (headViewTree, _) = rootViewTrees.head1
    val mediator = headViewTree.mediator
    val transactionUuid = headViewTree.transactionUuid

    def findValidSubtransactions(
        views: Seq[
          (
              TransactionView,
              RoseTree[ViewEffect],
              ViewPosition,
              Option[SubmitterMetadata],
          )
        ]
    ): FutureUnlessShutdown[
      (
          Seq[Error],
          Seq[
            (
                TransactionView,
                RoseTree[ViewEffect],
                WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]],
            )
          ],
      )
    ] = views
      .parTraverse { case (view, effects, viewPos, submittingParticipantO) =>
        for {
          wfTxE <- checkView(
            updateId,
            view,
            viewPos,
            mediator,
            transactionUuid,
            keyResolverFor(view),
            ledgerTime,
            preparationTime,
            submittingParticipantO,
            topologySnapshot,
            getEngineAbortStatus,
            reInterpretedTopLevelViews,
          ).value

          errorsViewsTxs <- wfTxE match {
            case Right(wfTx) => FutureUnlessShutdown.pure((Seq.empty, Seq((view, effects, wfTx))))

            // There is no point in checking subviews if we have aborted
            case Left(error @ DAMLeError(DAMLe.EngineAborted(_), _)) =>
              FutureUnlessShutdown.pure((Seq(error), Seq.empty))

            case Left(error) =>
              val subviewsWithIndex = view.subviews.unblindedElementsWithIndex
              val childEffects = effects.children
              ErrorUtil.requireArgument(
                subviewsWithIndex.sizeCompare(childEffects) == 0,
                s"Number of subviews (${subviewsWithIndex.size}) and child effects (${childEffects.size}) do not match for view at position $viewPos",
              )
              val subviewsWithInfo =
                subviewsWithIndex.zip(childEffects).map { case ((sv, svIndex), svEffects) =>
                  (sv, svEffects, svIndex +: viewPos, None)
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

    val rootViewsWithInfo = rootViewTrees.map { case (viewTree, effects) =>
      (viewTree.view, effects, viewTree.viewPosition, viewTree.submitterMetadataO)
    }

    val resultFE = findValidSubtransactions(rootViewsWithInfo).map { case (errors, viewsTxs) =>
      val (_views, effects, txs) = viewsTxs.unzip3

      val (wftxO, mergeErrorOO) = NonEmpty.from(txs).map(WellFormedTransaction.merge(_)).separate
      val mergeErrorO = mergeErrorOO.flatten.map(MergeError.apply)

      NonEmpty.from(errors ++ mergeErrorO) match {
        case None =>
          wftxO match {
            case Some(wftx) => Right(Result(updateId, wftx))
            case _ =>
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Model conformance check for transaction $updateId completed successfully but without a valid transaction"
                )
              )
          }
        case Some(errorsNE) =>
          val flatEffects = effects.flatMap(_.preorder)
          Left(ErrorWithSubTransaction(errorsNE, wftxO, flatEffects))
      }
    }

    EitherT(resultFE)
  }

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
    })

  def reInterpret(
      view: TransactionView,
      resolverFromView: LfKeyResolver,
      ledgerTime: CantonTimestamp,
      preparationTime: CantonTimestamp,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, ConformanceReInterpretationResult] = {
    val viewParticipantData = view.viewParticipantData.tryUnwrap

    val RootAction(cmd, authorizers, failed, packageIdPreference) =
      viewParticipantData.rootAction

    val seed = viewParticipantData.actionDescription.seedOption

    val inputContracts = view.inputContracts.fmap(_.contract)

    val contractAndKeyLookup = new ExtendedContractLookup(inputContracts, resolverFromView)

    for {

      packagePreference <- buildPackageNameMap(packageIdPreference)

      lfTxAndMetadata <- reinterpreter
        .reinterpret(
          contractAndKeyLookup,
          contractValidator.authenticateHash,
          authorizers,
          cmd,
          ledgerTime,
          preparationTime,
          seed,
          packagePreference,
          failed,
          getEngineAbortStatus,
        )(traceContext)
        .leftMap(DAMLeError(_, view.viewHash))
        .leftWiden[Error]
    } yield ConformanceReInterpretationResult(
      lfTxAndMetadata,
      contractAndKeyLookup,
      inputContracts,
    )
  }

  private def checkView(
      updateId: UpdateId,
      view: TransactionView,
      viewPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
      transactionUuid: UUID,
      resolverFromView: LfKeyResolver,
      ledgerTime: CantonTimestamp,
      preparationTime: CantonTimestamp,
      submitterMetadataO: Option[SubmitterMetadata],
      topologySnapshot: TopologySnapshot,
      getEngineAbortStatus: GetEngineAbortStatus,
      reInterpretedTopLevelViewsET: LazyAsyncReInterpretationMap,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, WithRollbackScope[
    WellFormedTransaction[WithAbsoluteSuffixes]
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
            ledgerTime,
            preparationTime,
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
        contractAndKeyLookup,
        _,
      ) = lfTxAndMetadata

      _ <- checkPackageVetting(view, topologySnapshot, usedPackages, metadata.ledgerTime)

      // For transaction views of protocol version 3 or higher,
      // the `resolverFromReinterpretation` is the same as the `resolverFromView`.
      // The `TransactionTreeFactoryImplV3` rebuilds the `resolverFromReinterpretation`
      // again by re-running the `ContractStateMachine` and checks consistency
      // with the reconstructed view's global key inputs,
      // which by the view equality check is the same as the `resolverFromView`.
      wfTx <- EitherT.fromEither[FutureUnlessShutdown](
        WellFormedTransaction
          .check(lfTx, metadata, WithoutSuffixes)
          .leftMap[Error](err => TransactionNotWellFormed(err, view.viewHash))
      )

      salts = transactionTreeFactory.saltsFromView(view)
      absolutizationData = transactionTreeFactory.cantonContractIdVersion match {
        case _: CantonContractIdV1Version => ContractIdAbsolutizationDataV1
        case _: CantonContractIdV2Version =>
          ContractIdAbsolutizationDataV2(updateId, metadata.ledgerTime)
      }
      absolutizer = new ContractIdAbsolutizer(hashOps, absolutizationData)

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
          contractOfId = TransactionTreeFactory.contractInstanceLookup(contractAndKeyLookup),
          keyResolver = resolverFromReinterpretation,
          absolutizer = absolutizer,
        )
      ).leftMap(err => TransactionTreeError(err, view.viewHash))

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
      informeeParticipantsByParty <-
        snapshot.activeParticipantsOfParties(informees.toSeq)

      informeeParticipants = informeeParticipantsByParty.values.flatten.toSet
      unvetted <- informeeParticipants.toSeq
        .parTraverse(p => snapshot.loadUnvettedPackagesOrDependencies(p, packageIds, ledgerTime))

    } yield {
      val combined = unvetted.combineAll.unknownOrUnvetted
      Either.cond(combined.isEmpty, (), UnvettedPackages(combined))
    })
  }

}

object ModelConformanceChecker {

  def apply(
      damlE: DAMLe,
      transactionTreeFactory: TransactionTreeFactory,
      contractValidator: ContractValidator,
      participantId: ParticipantId,
      packageResolver: PackageResolver,
      hashOps: HashOps & HmacOps,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ModelConformanceChecker =
    new ModelConformanceChecker(
      damlE,
      transactionTreeFactory,
      participantId,
      contractValidator,
      packageResolver,
      hashOps,
      loggerFactory,
    )

  // Type alias for a temporary caching of re-interpreted top views to be used both for authentication of externally
  // signed transaction and model conformance checker
  type LazyAsyncReInterpretation = Eval[EitherT[
    FutureUnlessShutdown,
    ModelConformanceChecker.Error,
    ModelConformanceChecker.ConformanceReInterpretationResult,
  ]]
  type LazyAsyncReInterpretationMap = Map[ViewHash, LazyAsyncReInterpretation]
  private[protocol] final case class ConformanceReInterpretationResult(
      reInterpretationResult: ReInterpretationResult,
      contractLookup: ExtendedContractLookup,
      viewInputContracts: Map[LfContractId, GenContractInstance],
  ) {

    /** Compute the hash of a re-interpreted transaction to validate external signatures. Note that
      * we need to enrich the transaction to re-hydrate the record values with labels, since they're
      * part of the hash
      */
    def computeHash(
        hashingSchemeVersion: HashingSchemeVersion,
        actAs: Set[LfPartyId],
        commandId: CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        synchronizerId: SynchronizerId,
        protocolVersion: ProtocolVersion,
        transactionEnricher: TransactionEnricher,
        contractEnricher: ContractEnricher,
        hashTracer: HashTracer,
    )(implicit
        traceContext: TraceContext,
        ec: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, Hash] =
      for {
        // Enrich the transaction...
        enrichedTransaction <- transactionEnricher(reInterpretationResult.transaction)(traceContext)
          .leftMap(_.toString)

        // ... and the input contracts so that labels and template identifiers are set and can be included in the hash
        inputContracts <- EitherT.fromEither[FutureUnlessShutdown](
          InputContractPackages
            .forTransactionWithContracts(enrichedTransaction.transaction, viewInputContracts)
            .leftMap(mismatch =>
              s"The following input contract IDs were not found in both the transaction and the provided contracts: $mismatch"
            )
        )

        enrichedInputContracts <- inputContracts.toList
          .parTraverse { case (cid, (inst, targetPackageIds)) =>
            contractEnricher((inst, targetPackageIds))(traceContext).map(cid -> _)
          }
          .map(_.toMap)
          .leftMap(_.toString)
        hash <- EitherT.fromEither[FutureUnlessShutdown](
          InteractiveSubmission
            .computeVersionedHash(
              hashingSchemeVersion,
              enrichedTransaction,
              InteractiveSubmission.TransactionMetadataForHashing.create(
                actAs,
                commandId,
                transactionUUID,
                mediatorGroup,
                synchronizerId,
                reInterpretationResult.timeBoundaries,
                reInterpretationResult.metadata.preparationTime.toLf,
                enrichedInputContracts,
              ),
              reInterpretationResult.metadata.seeds,
              protocolVersion,
              hashTracer = hashTracer,
            )
            .leftMap(_.message)
        )
      } yield hash
  }

  sealed trait Error extends PrettyPrinting

  /** Enriches a model conformance error with the valid subtransaction, if any. If there is a valid
    * subtransaction, the list of valid subview trees will not be empty.
    */
  final case class ErrorWithSubTransaction[+ViewEffect](
      errors: NonEmpty[Seq[Error]],
      validSubTransactionO: Option[WellFormedTransaction[WithSuffixesAndMerged]],
      validSubViewEffects: Seq[ViewEffect],
  ) {
    require(validSubTransactionO.isEmpty == validSubViewEffects.isEmpty)

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

  object ErrorWithSubTransaction {
    implicit def prettyErrorWithSubTransaction[ViewEffect: Pretty]
        : Pretty[ErrorWithSubTransaction[ViewEffect]] = {
      import com.digitalasset.canton.logging.pretty.PrettyUtil.*
      import com.digitalasset.canton.util.ShowUtil.*
      Pretty.prettyOfClass[ErrorWithSubTransaction[ViewEffect]](
        param("errors", _.errors),
        param("engine abort status", _.engineAbortStatus),
        paramIfDefined("valid subtransaction", _.validSubTransactionO.map(_.toString.unquoted)),
        paramIfNonEmpty("valid subview effects", _.validSubViewEffects),
      )
    }
  }

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
            val ref =
              conflicts
                .map { case (name, packageIds) => s"$name -> $packageIds" }
                .mkString("[", ", ", "]")
            show"$participant has detected conflicting package name resolutions: $ref".unquoted
          }
          .mkShow("\n")
      )
    )
  }

  final case class MergeError(cause: String) extends Error {
    override protected def pretty: Pretty[MergeError] = prettyOfParam(_.cause.unquoted)
  }

  final case class Result(
      updateId: UpdateId,
      suffixedTransaction: WellFormedTransaction[WithSuffixesAndMerged],
  )

}
