// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

package wasm

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.{PureCompiledPackages, interpretation}
import com.digitalasset.daml.lf.speedy.SResult.{
  SResultError,
  SResultFinal,
  SResultInterruption,
  SResultQuestion,
}
import com.digitalasset.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.toSValue
import com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode
import com.digitalasset.daml.lf.value.{Value => LfValue}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

final class SpeedyRunner(
    submitters: Set[Party],
    readAs: Set[Party],
    submissionTime: Time.Timestamp,
    logger: ContextualizedLogger = ContextualizedLogger.get(classOf[SpeedyRunner]),
    compiledPackages: PureCompiledPackages,
    activeContractStore: ParticipantContractStore,
    initialLocalContractStore: Map[LfValue.ContractId, ContractInfo],
    initialPtx: PartialTransaction,
)(implicit loggingContext: LoggingContext)
    extends SpeedyUtils(compiledPackages.pkgInterface)
    with host.WasmHostFunctions {

  private[this] var localContractStore: Map[LfValue.ContractId, ContractInfo] =
    initialLocalContractStore
  private[this] var ptx: PartialTransaction = initialPtx

  override def logInfo(msg: String): Unit = {
    logger.info(msg)
  }

  override def createContract(templateId: Ref.TypeConRef, argsV: LfValue): LfValue.ContractId = {
    // TODO: manage get failure
    val contractDef =
      compiledPackages.getDefinition(SExpr.CreateDefRef(templateId.assertToTypeConName)).get.body
    val sexpr = SExpr.SEApp(contractDef, Array(toSValue(argsV), SValue.SToken))

    evaluateSpeedyExpression(sexpr) match {
      case LfValue.ValueContractId(contractId) =>
        contractId

      case _ =>
        // TODO: handle fall through case
        ???
    }
  }

  override def fetchContractArg(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      timeout: Duration,
  ): LfValue = {
    // TODO: manage get failure
    val templateDef = compiledPackages
      .getDefinition(SExpr.FetchTemplateDefRef(templateId.assertToTypeConName))
      .get
      .body
    val sexpr = SExpr.SEApp(templateDef, Array(SValue.SContractId(contractId), SValue.SToken))

    Await.result(Future(evaluateSpeedyExpression(sexpr))(ExecutionContext.global), timeout)
  }

  override def exerciseChoice(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      choiceName: Ref.ChoiceName,
      choiceArg: LfValue,
  ): LfValue = {
    // TODO: manage get failure
    val templateChoiceDef = compiledPackages
      .getDefinition(SExpr.TemplateChoiceDefRef(templateId.assertToTypeConName, choiceName))
      .get
      .body
    val sexpr = SExpr.SEApp(
      templateChoiceDef,
      Array(SValue.SContractId(contractId), toSValue(choiceArg), SValue.SToken),
    )

    evaluateSpeedyExpression(sexpr)
  }

  private[wasm] def incompleteTransaction
      : (Map[LfValue.ContractId, ContractInfo], PartialTransaction) = {
    (localContractStore, ptx)
  }

  private def evaluateSpeedyExpression(sexpr: SExpr.SExpr): LfValue = {
    val machine = new UpdateMachine(
      sexpr = sexpr,
      committers = submitters,
      readAs = readAs,
      submissionTime = submissionTime,
      traceLog = new TraceLog {
        def add(message: String, optLocation: Option[Ref.Location])(implicit
            loggingContext: LoggingContext
        ): Unit = {
          logger.info(message)
        }

        def iterator: Iterator[(String, Option[Ref.Location])] = ???
      },
      warningLog = new WarningLog(logger),
      profile = new Profile(),
      iterationsBetweenInterruptions = 10000,
      compiledPackages = compiledPackages,
      packageResolution =
        Map.empty, // TODO: used by SBSoftFetchInterface - so we ignore this for now
      validating = false,
      contractKeyUniqueness = ContractKeyUniquenessMode.Strict,
      commitLocation = None,
      limits = interpretation.Limits.Lenient,
      ptx = ptx,
    )

    machine.localContractStore = localContractStore.map { case (contractId, contractInfo) =>
      contractId -> (contractInfo.templateId, toSValue(contractInfo.arg))
    }

    val result = evaluateSExpr(machine)

    localContractStore = machine.localContractStore.map { case (contractId, (templateId, _)) =>
      if (localContractStore.contains(contractId)) {
        contractId -> localContractStore(contractId)
      } else {
        contractId -> machine.contractInfoCache((contractId, templateId.packageId))
      }
    }
    ptx = machine.ptx

    result.toUnnormalizedValue
  }

  private def evaluateSExpr(machine: UpdateMachine): SValue = {
    @scala.annotation.tailrec
    def loop(): SValue = {
      machine.run() match {
        case SResultQuestion(question) =>
          question match {
            case _: Question.Update.NeedAuthority =>
              // TODO: implement this case
              ???

            case Question.Update.NeedPackageId(_, pid0, callback) =>
              // TODO: https://github.com/digital-asset/daml/issues/16154 (dynamic-exercise)
              // For now this just continues with the input package id
              callback(pid0)
              loop()

            case Question.Update.NeedTime(callback) =>
              callback(submissionTime)
              loop()

            case _: Question.Update.NeedPackage =>
              // TODO: implement this case
              ???

            case Question.Update.NeedContract(coid, _, callback) =>
              // FIXME: manage contract not found!
              val coinst = Await
                .result(activeContractStore.lookupActiveContract(readAs, coid), Duration.Inf)
                .get
              callback(coinst.unversioned)
              loop()

            case _: Question.Update.NeedUpgradeVerification =>
              // TODO: implement this case
              ???

            case _: Question.Update.NeedKey =>
              // TODO: implement this case
              ???
          }

        case SResultInterruption =>
          // TODO: implement this case
          ???

        case SResultFinal(result) =>
          result

        case SResultError(err) =>
          // TODO: improve error reporting
          throw new RuntimeException(err.toString)
      }
    }

    loop()
  }
}
