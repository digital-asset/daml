// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.daml.logging.LoggingContext
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.{Engine, Result, ResultDone, Enricher => LfEnricher}
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy._
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.Value

import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}

private[lf] object IdeLedgerRunner {

  private def crash(reason: String) =
    throw Error.Internal(reason)

  private def handleUnsafe[T](unsafe: => T): Either[Error, T] = {
    Try(unsafe) match {
      case Failure(err: Error) => Left(err: Error)
      case Failure(other) => throw other
      case Success(t) => Right(t)
    }
  }

  sealed abstract class SubmissionResult[+R] {
    @tailrec
    private[lf] final def resolve(): Either[SubmissionError, Commit[R]] = {
      this match {
        case commit: Commit[R] => Right(commit)
        case error: SubmissionError => Left(error)
        case Interruption(continue) => continue().resolve()
      }
    }
  }

  final case class Commit[+R](
      result: R,
      tx: IncompleteTransaction,
  ) extends SubmissionResult[R]

  final case class SubmissionError(error: Error, tx: IncompleteTransaction)
      extends SubmissionResult[Nothing]

  final case class Interruption[R](continue: () => SubmissionResult[R]) extends SubmissionResult[R]

  // The interface we need from a ledger during submission. We allow abstracting over this so we can play
  // tricks like caching all responses in some benchmarks.
  private[lf] abstract class LedgerApi[R] {
    def lookupContract(
        coid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        cbPresent: FatContractInstance => Unit,
    ): Either[Error, Unit]
    def lookupKey(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        canContinue: Option[ContractId] => Boolean,
    ): Either[Error, Unit]
    def currentTime: Time.Timestamp
    def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: CommittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, R]
  }

  private[lf] case class ScriptLedgerApi(ledger: IdeLedger)
      extends LedgerApi[IdeLedger.CommitResult] {

    override def lookupContract(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: FatContractInstance => Unit,
    ): Either[Error, Unit] =
      handleUnsafe(lookupContractUnsafe(acoid, actAs, readAs, callback))

    private def lookupContractUnsafe(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: FatContractInstance => Unit,
    ) = {

      val effectiveAt = ledger.currentTime

      ledger.lookupGlobalContract(
        actAs,
        readAs,
        effectiveAt = effectiveAt,
        acoid,
      ) match {
        case IdeLedger.LookupOk(coinst) =>
          callback(coinst)

        case IdeLedger.LookupContractNotFound(coid) =>
          // This should never happen, hence we don't have a specific
          // error for this.
          throw Error.Internal(s"contract ${coid.coid} not found")

        case IdeLedger.LookupContractNotEffective(coid, tid, effectiveAt) =>
          throw Error.ContractNotEffective(coid, tid, effectiveAt)

        case IdeLedger.LookupContractNotActive(coid, tid, consumedBy) =>
          throw Error.ContractNotActive(coid, tid, consumedBy)

        case IdeLedger.LookupContractNotVisible(coid, tid, observers, stakeholders @ _) =>
          throw Error.ContractNotVisible(coid, tid, actAs, readAs, observers)
      }
    }

    override def lookupKey(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Either[Error, Unit] =
      handleUnsafe(lookupKeyUnsafe(gk, actAs, readAs, callback))

    private def lookupKeyUnsafe(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Unit = {

      val effectiveAt = ledger.currentTime
      val readers = actAs union readAs

      def missingWith(err: Error) =
        if (!callback(None)) {
          throw err
        }

      ledger.ledgerData.activeKeys.get(gk) match {
        case None =>
          missingWith(
            Error.RunnerException(
              SError.SErrorDamlException(interpretation.Error.ContractKeyNotFound(gk))
            )
          )
        case Some(acoid) =>
          ledger.lookupGlobalContract(
            actAs,
            readAs,
            effectiveAt = effectiveAt,
            acoid,
          ) match {
            case IdeLedger.LookupOk(contract) =>
              if (!readers.intersect(contract.stakeholders).isEmpty)
                // Note that even with a successful global lookup
                // the callback can return false. This happens for a fetch-by-key
                // if the contract got archived in the meantime.
                // We discard the result here and rely on fetch-by-key
                // setting up the state such that continuing interpretation fails.
                discard(callback(Some(acoid)))
              else
                throw Error.ContractKeyNotVisible(acoid, gk, actAs, readAs, contract.stakeholders)
            case IdeLedger.LookupContractNotFound(coid) =>
              missingWith(
                Error.Internal(s"contract ${coid.coid} not found, but we found its key!")
              )
            case IdeLedger.LookupContractNotEffective(_, _, _) =>
              missingWith(
                Error.Internal(
                  s"contract ${acoid.coid} not effective, but we found its key!"
                )
              )
            case IdeLedger.LookupContractNotActive(_, _, _) =>
              missingWith(
                Error.Internal(s"contract ${acoid.coid} not active, but we found its key!")
              )
            case IdeLedger.LookupContractNotVisible(
                  coid,
                  tid @ _,
                  observers @ _,
                  stakeholders,
                ) =>
              throw Error.ContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders)
          }
      }
    }

    override def currentTime = ledger.currentTime

    override def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: CommittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, IdeLedger.CommitResult] =
      IdeLedger.commitTransaction(
        actAs = committers,
        readAs = readAs,
        effectiveAt = ledger.currentTime,
        optLocation = location,
        tx = tx,
        locationInfo = locationInfo,
        l = ledger,
      ) match {
        case Left(fas) =>
          Left(Error.CommitError(fas))
        case Right(result) =>
          Right(result)
      }
  }

  private[this] abstract class Enricher {
    def enrich(tx: VersionedTransaction): VersionedTransaction
    def enrich(tx: IncompleteTransaction): IncompleteTransaction
  }

  private[this] object NoEnricher extends Enricher {
    override def enrich(tx: VersionedTransaction): VersionedTransaction = tx
    override def enrich(tx: IncompleteTransaction): IncompleteTransaction = tx
  }

  private[this] class EnricherImpl(compiledPackages: CompiledPackages) extends Enricher {
    val config = Engine.DevEngine.config
    def loadPackage(pkgId: PackageId, context: language.Reference): Result[Unit] = {
      crash(LookupError.MissingPackage.pretty(pkgId, context))
    }
    val strictEnricher = new LfEnricher(
      compiledPackages = compiledPackages,
      loadPackage = loadPackage,
      addTypeInfo = true,
      addFieldNames = true,
      addTrailingNoneFields = true,
      forbidLocalContractIds = true,
    )
    val lenientEnricher = new LfEnricher(
      compiledPackages = compiledPackages,
      loadPackage = loadPackage,
      addTypeInfo = true,
      addFieldNames = true,
      addTrailingNoneFields = true,
      forbidLocalContractIds = false,
    )
    def consume[V](res: Result[V]): V =
      res match {
        case ResultDone(x) => x
        case x => crash(s"unexpected Result when enriching value: $x")
      }
    override def enrich(tx: VersionedTransaction): VersionedTransaction =
      consume(strictEnricher.enrichVersionedTransaction(tx))
    override def enrich(tx: IncompleteTransaction): IncompleteTransaction =
      consume(lenientEnricher.enrichIncompleteTransaction(tx))
  }

  /** A class for suffixing all the local contract IDs of a transaction with the TypedNormalForm hash of their Create
    * argument. Assumes that the creation package of these contract and its transitive dependencies are all present in
    * [compiledPackages].
    */
  private[this] class CidSuffixer(compiledPackages: CompiledPackages) {
    private[this] val valueTranslator = new speedy.ValueTranslator(
      compiledPackages.pkgInterface,
      forbidLocalContractIds = false,
      forbidTrailingNones = false,
    )

    private[this] def hashCreateNode(createNode: Node.Create): crypto.Hash = {
      val sValue = valueTranslator
        .translateValue(Ast.TTyCon(createNode.templateId), createNode.arg)
        .fold(
          e => crash(s"unexpected error when enriching a Create node produced by the engine: $e"),
          identity,
        )
      SValueHash
        .hashContractInstance(createNode.packageName, createNode.templateId.qualifiedName, sValue)
        .fold(
          e => crash(s"unexpected error when hashing a Create node produced by the engine: $e"),
          identity,
        )
    }

    def suffixCids(tx: VersionedTransaction): VersionedTransaction = {
      val cidMapping = tx
        .foldInExecutionOrder(Map.empty[ContractId, ContractId].withDefault(identity))(
          exerciseBegin = (mapping, _, _) => (mapping, ChildrenRecursion.DoRecurse),
          rollbackBegin = (mapping, _, _) => (mapping, ChildrenRecursion.DoRecurse),
          leaf = (mapping, _, leaf) => {
            leaf match {
              case create: Node.Create =>
                val suffix = hashCreateNode(create.mapCid(mapping)).bytes
                mapping + (create.coid -> data.assertRight(
                  create.coid.suffixCid(_ => suffix, _ => suffix)
                ))
              case _ =>
                mapping
            }
          },
          exerciseEnd = (mapping, _, _) => mapping,
          rollbackEnd = (mapping, _, _) => mapping,
        )
      tx.mapCid(cidMapping)
    }
  }

  def submit[R](
      compiledPackages: CompiledPackages,
      disclosures: Iterable[FatContractInstance],
      ledger: LedgerApi[R],
      committers: Set[Party],
      readAs: Set[Party],
      commands: SExpr,
      location: Option[Location],
      seed: crypto.Hash,
      packageResolution: Map[PackageName, PackageId] = Map.empty,
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      doEnrichment: Boolean = true,
  )(implicit loggingContext: LoggingContext): SubmissionResult[R] = {

    val disclosuresByCoid = disclosures.view.map(fci => fci.contractId -> fci).toMap
    val disclosuresByKey = disclosures.view.collect {
      case fci if fci.contractKeyWithMaintainers.isDefined =>
        fci.contractKeyWithMaintainers.get.globalKey -> fci.contractId
    }.toMap

    val ledgerMachine = Speedy.UpdateMachine(
      packageResolution = packageResolution,
      compiledPackages = compiledPackages,
      preparationTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SEApp(commands, ArraySeq(SValue.SToken)),
      committers = committers,
      readAs = readAs,
      traceLog = traceLog,
      warningLog = warningLog,
      commitLocation = location,
      limits = interpretation.Limits.Lenient,
    )
    // TODO (drsk) validate and propagate errors back to submitter
    // https://github.com/digital-asset/daml/issues/14108
    val enricher = if (doEnrichment) new EnricherImpl(compiledPackages) else NoEnricher
    import enricher._
    val suffixer = new CidSuffixer(compiledPackages)

    def continue = () => go()

    @tailrec
    def go(): SubmissionResult[R] = {
      ledgerMachine.run() match {
        case SResultQuestion(question) =>
          question match {
            case Question.Update.NeedContract(coid, committers, callback) =>
              disclosuresByCoid.get(coid) match {
                case Some(fcoinst) =>
                  callback(
                    fcoinst.nonVerboseWithoutTrailingNones,
                    Hash.HashingMethod.TypedNormalForm,
                    h =>
                      fcoinst.contractId match {
                        case ContractId.V1(_, suffix) => h.bytes == suffix
                        case ContractId.V2(_, suffix) => h.bytes == suffix
                      },
                  )
                  go()
                case None =>
                  ledger.lookupContract(
                    coid,
                    committers,
                    readAs,
                    (fcoinst: FatContractInstance) =>
                      callback(
                        fcoinst.nonVerboseWithoutTrailingNones,
                        Hash.HashingMethod.TypedNormalForm,
                        h =>
                          fcoinst.contractId match {
                            case ContractId.V1(_, suffix) => h.bytes == suffix
                            case ContractId.V2(_, suffix) => h.bytes == suffix
                          },
                      ),
                  ) match {
                    case Left(err) =>
                      SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                    case Right(_) => go()
                  }
              }
            case Question.Update.NeedKey(keyWithMaintainers, committers, callback) =>
              disclosuresByKey.get(keyWithMaintainers.globalKey) match {
                case Some(fcoinst) =>
                  discard[Boolean](callback(Some(fcoinst)))
                  go()
                case None =>
                  ledger.lookupKey(
                    keyWithMaintainers.globalKey,
                    committers,
                    readAs,
                    callback,
                  ) match {
                    case Left(err) =>
                      SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                    case Right(_) => go()
                  }
              }
            case Question.Update.NeedTime(callback) =>
              callback(ledger.currentTime)
              go()
            case res: Question.Update.NeedPackage =>
              throw Error.Internal(s"unexpected $res")
          }
        case SResultInterruption =>
          Interruption(continue)
        case SResult.SResultFinal(_) =>
          ledgerMachine.finish match {
            case Right(Speedy.UpdateMachine.Result(tx, locationInfo, _, _)) =>
              val committedTx = CommittedTransaction(enrich(suffixer.suffixCids(tx)))
              ledger.commit(committers, readAs, location, committedTx, locationInfo) match {
                case Left(err) =>
                  SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                case Right(r) =>
                  Commit(r, enrich(ledgerMachine.incompleteTransaction))
              }
            case Left(err) =>
              throw err
          }
        case SResultError(err) =>
          SubmissionError(Error.RunnerException(err), enrich(ledgerMachine.incompleteTransaction))
      }
    }
    go()
  }

  private[lf] def nextSeed(submissionSeed: crypto.Hash): crypto.Hash =
    crypto.Hash.deriveTransactionSeed(
      submissionSeed,
      Ref.ParticipantId.assertFromString("script-service"),
      // MinValue makes no sense here but this is what we did before so
      // to avoid breaking all tests we keep it for now at least.
      Time.Timestamp.MinValue,
    )

  sealed abstract class ScriptResult extends Product with Serializable {
    def ledger: IdeLedger
    def traceLog: TraceLog
    def warningLog: WarningLog
  }

  final case class CurrentSubmission(
      commitLocation: Option[Location],
      ptx: IncompleteTransaction,
  )

  final case class ScriptSuccess(
      ledger: IdeLedger,
      traceLog: TraceLog,
      warningLog: WarningLog,
      duration: Double,
      steps: Int,
      resultValue: Value,
  ) extends ScriptResult

  final case class ScriptError(
      ledger: IdeLedger,
      traceLog: TraceLog,
      warningLog: WarningLog,
      currentSubmission: Option[CurrentSubmission],
      stackTrace: ImmArray[Location],
      error: Error,
  ) extends ScriptResult
}
