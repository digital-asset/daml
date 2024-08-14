// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package wasm

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.command.{ApiCommands, DisclosedContract, ReplayCommand}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result
import com.digitalasset.daml.lf.speedy.{InitialSeeding, SError}
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.WasmExpr
import com.digitalasset.daml.lf.transaction.{
  Node,
  SubmittedTransaction,
  VersionedTransaction,
  Transaction => Tx,
}
import com.google.protobuf.ByteString

import scala.annotation.unused

// TODO: this file is very much WIP!!
class WasmEngine(config: EngineConfig) {

  def submit(
      submitters: Set[Ref.Party],
      readAs: Set[Ref.Party],
      cmds: ApiCommands,
      @unused disclosures: ImmArray[DisclosedContract] = ImmArray.empty,
      participantId: Ref.ParticipantId,
      submissionSeed: crypto.Hash,
      @unused packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      @unused packagePreference: Set[Ref.PackageId] = Set.empty,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val submissionTime = cmds.ledgerEffectiveTime
    val initialSeeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime)
    val wasm = new WasmRunner(submitters, readAs, initialSeeding, config.authorizationChecker)
    // FIXME: for the moment, we always load a universe Wasm module
    val wexpr = WasmExpr(WasmEngine.universe, "main")

    mapToResult(
      submissionTime,
      wasm.evaluateWasmExpression(
        wexpr,
        ledgerTime = cmds.ledgerEffectiveTime,
        // FIXME: for the moment, we perform no package resolution
        packageResolution = Map.empty,
      ),
    )
  }

  def reinterpret(
      submitters: Set[Ref.Party],
      @unused command: ReplayCommand,
      nodeSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
      @unused packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      @unused packagePreference: Set[Ref.PackageId] = Set.empty,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val readAs = Set.empty[Ref.Party]
    val initialSeeding = InitialSeeding.RootNodeSeeds(ImmArray(nodeSeed))
    val wasm = new WasmRunner(submitters, readAs, initialSeeding, config.authorizationChecker)
    // FIXME: for the moment, we always load a universe Wasm module and ignore the replay command
    val wexpr = WasmExpr(WasmEngine.universe, "main")

    mapToResult(
      submissionTime,
      wasm.evaluateWasmExpression(
        wexpr,
        ledgerTime = ledgerEffectiveTime,
        // FIXME: for the moment, we perform no package resolution
        packageResolution = Map.empty,
      ),
    )
  }

  def replay(
      submitters: Set[Ref.Party],
      @unused tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      @unused packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      @unused packagePreference: Set[Ref.PackageId] = Set.empty,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val readAs = Set.empty[Ref.Party]
    val initialSeeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime)
    val wasm = new WasmRunner(submitters, readAs, initialSeeding, config.authorizationChecker)
    // FIXME: for the moment, we always load a universe Wasm module and ignore calculating commands to run from the given tx
    val wexpr = WasmExpr(WasmEngine.universe, "main")

    mapToResult(
      submissionTime,
      wasm.evaluateWasmExpression(
        wexpr,
        ledgerTime = ledgerEffectiveTime,
        // FIXME: for the moment, we perform no package resolution
        packageResolution = Map.empty,
      ),
    )
  }

  def validate(
      submitters: Set[Ref.Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
  )(implicit loggingContext: LoggingContext): Result[Unit] = {
    // reinterpret
    for {
      result <- replay(
        submitters,
        tx,
        ledgerEffectiveTime,
        participantId,
        submissionTime,
        submissionSeed,
      )
      (rtx, _) = result
      validationResult <-
        transaction.Validation
          .isReplayedBy(tx, rtx)
          .fold(
            e => ResultError(Error.Validation.ReplayMismatch(e)),
            _ => ResultDone.Unit,
          )
    } yield validationResult
  }

//  def computeInterfaceView(
//                            templateId: Ref.Identifier,
//                            argument: WasmValue,
//                            interfaceId: Ref.Identifier,
//                          )(implicit loggingContext: LoggingContext): Result[Versioned[WasmValue]] = {
//    // TODO:
//    ???
//  }

  private def mapToResult(
      submissionTime: Time.Timestamp,
      result: Either[SErrorCrash, UpdateMachine.Result],
  ): Result[(SubmittedTransaction, Tx.Metadata)] = result match {
    case Right(
          UpdateMachine.Result(tx, _, nodeSeeds, globalKeyMapping, disclosedCreateEvents)
        ) =>
      deps(tx).flatMap { deps =>
        val meta = Tx.Metadata(
          submissionSeed = None,
          submissionTime = submissionTime,
          usedPackages = deps,
          // FIXME: for the moment, just fake this parameter
          dependsOnTime = false,
          nodeSeeds = nodeSeeds,
          globalKeyMapping = globalKeyMapping,
          disclosedEvents = disclosedCreateEvents,
        )
        ResultDone((tx, meta))
      }

    case Left(err) =>
      handleError(err)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def deps(tx: VersionedTransaction): Result[Set[PackageId]] = {
    val nodePkgIds =
      tx.nodes.values.collect { case node: Node.Action => node.packageIds }.flatten.toSet
    val deps = nodePkgIds.foldLeft(nodePkgIds)((acc, _) =>
      // FIXME: for the moment, we do not worry about the Wasm version of compiledPackages
      acc
    )

    ResultDone(deps)
  }

  private def handleError(err: SError.SError, detailMsg: Option[String] = None): ResultError = {
    err match {
      case SError.SErrorDamlException(error) =>
        ResultError(Error.Interpretation.DamlException(error), detailMsg)
      case err @ SError.SErrorCrash(where, reason) =>
        ResultError(Error.Interpretation.Internal(where, reason, Some(err)))
    }
  }
}

object WasmEngine {
  // FIXME: temporary hack to model our Wasm package/module universe - avoids need for dynamic loading etc. of Wasm modules
  val universe: ByteString = ByteString.empty()
}
