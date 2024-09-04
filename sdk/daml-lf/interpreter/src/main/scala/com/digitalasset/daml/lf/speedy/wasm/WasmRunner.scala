// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.speedy.SResult.SVisibleToStakeholders
import com.digitalasset.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, Node}
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.digitalasset.daml.lf.speedy.wasm.exports.{
  WasmChoiceExportFunctions,
  WasmTemplateExportFunctions,
}
import com.digitalasset.daml.lf.speedy.wasm.host.{PureWasmHostFunctions, UpdateWasmHostFunctions}
import com.digitalasset.daml.lf.speedy.wasm.host.WasmHostFunctions
import com.dylibso.chicory.runtime.{
  HostFunction => WasmHostFunction,
  HostImports => WasmHostImports,
  Instance => WasmInstance,
  Module => WasmModule,
}
import com.google.protobuf.ByteString

import scala.concurrent.Await
import scala.concurrent.duration.Duration

final class WasmRunner(
    submitters: Set[Party],
    readAs: Set[Party],
    submissionTime: Time.Timestamp,
    logger: ContextualizedLogger = ContextualizedLogger.get(classOf[WasmRunner]),
    pkgInterface: language.PackageInterface = PackageInterface.Empty,
    wasmExpr: WasmRunner.WasmExpr,
    activeContractStore: ParticipantContractStore,
    initialLocalContractStore: Map[LfValue.ContractId, ContractInfo],
    initialPtx: PartialTransaction,
)(implicit loggingContext: LoggingContext)
    extends UpdateWasmHostFunctions(pkgInterface)
    with WasmHostFunctions {

  import internal.WasmUtils._
  import WasmTemplateExportFunctions._
  import WasmChoiceExportFunctions._
  import host.internal.WasmRunnerHostFunctions._
  import WasmRunner._

  // TODO: do we care about Speedy limit enforcement?
  // TODO: what about being able to run in validation mode (c.f. Speedy)?

  // Holds the template constructor and argument for contracts created in this transaction
  private[this] var localContractStore: Map[LfValue.ContractId, ContractInfo] =
    initialLocalContractStore
  private[this] var ptx: PartialTransaction = initialPtx

  override def logInfo(msg: String): Unit = {
    logger.info(msg)
  }

  override def createContract(templateId: Ref.TypeConRef, argsV: LfValue): LfValue.ContractId = {
    val templateTypeCon = templateId.assertToTypeConName
    val (pkgName, pkgVersion) = tmplId2PackageNameVersion(templateTypeCon)
    val argsSV = toSValue(argsV)
    val txVersion = tmplId2TxVersion(templateTypeCon)
    val templateName = templateId.qName.name.segments.head
    val precond = wasmTemplatePrecondFunction(templateName, txVersion)(argsV)(PureWasmInstance())

    if (precond) {
      val signatories =
        wasmTemplateSignatoriesFunction(templateName, txVersion)(argsV)(PureWasmInstance())
      val observers =
        wasmTemplateObserversFunction(templateName, txVersion)(argsV)(PureWasmInstance())
      val contractInfo = ContractInfo(
        txVersion,
        pkgName,
        pkgVersion,
        templateTypeCon,
        argsSV,
        signatories,
        observers,
        None,
      )
      val (contractId, updatedPtx) = ptx
        .insertCreate(submissionTime, contractInfo, None)
        .fold(
          { case (_, err) =>
            // TODO: manage Left case
            throw new RuntimeException(err.toString)
          },
          identity,
        )

      localContractStore = localContractStore + (contractId -> contractInfo)
      ptx = updatedPtx

      contractId
    } else {
      // TODO: manage precond failure case
      ???
    }
  }

  override def fetchContractArg(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      timeout: Duration,
  ): LfValue = {
    val templateTypeCon = templateId.assertToTypeConName
    val txVersion = tmplId2TxVersion(templateTypeCon)

    localContractStore.get(contractId) match {
      case Some(contractInfo) if contractInfo.templateId == templateTypeCon =>
        contractInfo.arg

      case Some(_) =>
        // TODO: manage wrongly typed contract case
        ???

      case _ =>
        Await.result(
          activeContractStore.lookupActiveContract(submitters ++ readAs, contractId),
          timeout,
        ) match {
          case Some(contract) =>
            if (contract.unversioned.template.toRef == templateId) {
              val templateName = templateId.qName.name.segments.head
              val precond =
                wasmTemplatePrecondFunction(templateName, txVersion)(contract.unversioned.arg)(
                  PureWasmInstance()
                )

              if (precond) {
                val signatories =
                  wasmTemplateSignatoriesFunction(templateName, txVersion)(
                    contract.unversioned.arg
                  )(PureWasmInstance())
                val observers =
                  wasmTemplateObserversFunction(templateName, txVersion)(contract.unversioned.arg)(
                    PureWasmInstance()
                  )
                val contractInfo = ContractInfo(
                  txVersion,
                  contract.unversioned.packageName,
                  contract.unversioned.packageVersion,
                  templateTypeCon,
                  toSValue(contract.unversioned.arg),
                  signatories,
                  observers,
                  None,
                )

                // TODO: what about being able to run in validation mode (c.f. Speedy)?
                SVisibleToStakeholders.fromSubmitters(submitters, readAs)(
                  contractInfo.stakeholders
                ) match {
                  case SVisibleToStakeholders.Visible =>
                    ()

                  case SVisibleToStakeholders.NotVisible(actAs, readAs) =>
                    val readers = (actAs union readAs).mkString(",")
                    val stakeholders = contractInfo.stakeholders.mkString(",")
                    logger.warn(
                      s"""Tried to fetch or exercise $templateId on contract $contractId
                         | but none of the reading parties [$readers] are contract stakeholders [$stakeholders].
                         | Use of divulged contracts is deprecated and incompatible with pruning.
                         | To remedy, add one of the readers [$readers] as an observer to the contract.
                         |""".stripMargin.replaceAll("\r|\n", "")
                    )
                }

                val updatedPtx =
                  ptx
                    .insertFetch(contractId, contractInfo, None, byKey = false, txVersion)
                    .fold(err => throw new RuntimeException(err.toString), identity)

                ptx = updatedPtx

                contract.unversioned.arg
              } else {
                // TODO: manage precond failure case
                ???
              }
            } else {
              // TODO: manage wrongly typed contract case
              ???
            }

          case None =>
            // TODO: manage contract lookup failure
            ???
        }
    }
  }

  override def exerciseChoice(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      choiceName: Ref.ChoiceName,
      choiceArg: LfValue,
      consuming: Boolean,
  ): LfValue = {
    val templateTypeCon = templateId.assertToTypeConName
    val txVersion = tmplId2TxVersion(templateTypeCon)
    val (pkgName, _) = tmplId2PackageNameVersion(templateTypeCon)
    val contractInfo = localContractStore.get(contractId) match {
      case Some(contractInfo) if contractInfo.templateId == templateTypeCon =>
        contractInfo

      case Some(_) =>
        // TODO: manage wrongly typed contract case
        ???

      case None =>
        // TODO: manage contract not being locally known (e.g. global contract not fetched)
        ???
    }
    val controllers =
      wasmChoiceControllersFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)(
        PureWasmInstance()
      )
    val observers = wasmChoiceObserversFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)(
      PureWasmInstance()
    )
    val authorizers =
      wasmChoiceAuthorizersFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)(
        PureWasmInstance()
      )

    val startPtx = ptx
      .beginExercises(
        packageName = pkgName,
        templateId = templateTypeCon,
        targetId = contractId,
        contract = contractInfo,
        interfaceId = None,
        choiceId = choiceName,
        optLocation = None,
        consuming = consuming,
        actingParties = controllers,
        choiceObservers = observers,
        choiceAuthorizers = authorizers,
        byKey = false,
        chosenValue = choiceArg,
        version = txVersion,
      )
      .fold(err => throw new RuntimeException(err.toString), identity)

    ptx = startPtx

    try {
      val result = wasmChoiceExerciseFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)(
        UpdateWasmInstance()
      )

      val endPtx = ptx.endExercises(txVer => toSValue(result).toNormalizedValue(txVer))

      ptx = endPtx

      result
    } catch {
      case exn: Throwable =>
        val abortPtx = ptx.abortExercises

        ptx = abortPtx

        throw exn
    }
  }

  def evaluateWasmExpression(): Either[SErrorCrash, UpdateMachine.Result] = {
    implicit val instance: WasmInstance = UpdateWasmInstance()

    val exprEvaluator = instance.export(wasmExpr.name)

    // Calling imported host functions applies a series of (transactional) side effects to ptx
    val _ = exprEvaluator.apply(wasmExpr.args.map(copyByteArray).flatten: _*)

    finish
  }

  private def PureWasmInstance(): WasmInstance = {
    val imports = new WasmHostImports(
      Array[WasmHostFunction](
        logFunc,
        PureWasmHostFunctions.createContractFunc,
        PureWasmHostFunctions.fetchContractArgFunc,
        PureWasmHostFunctions.exerciseChoiceFunc,
      )
    )

    WasmModule.builder(wasmExpr.module.toByteArray).withHostImports(imports).build().instantiate()
  }

  private def UpdateWasmInstance(): WasmInstance = {
    val imports = new WasmHostImports(
      Array[WasmHostFunction](logFunc, createContractFunc, fetchContractArgFunc, exerciseChoiceFunc)
    )

    WasmModule.builder(wasmExpr.module.toByteArray).withHostImports(imports).build().instantiate()
  }

  private def finish: Either[SErrorCrash, UpdateMachine.Result] = ptx.finish.map {
    case (tx, seeds) =>
      UpdateMachine.Result(
        tx,
        ptx.locationInfo(),
        zipSameLength(seeds, ptx.actionNodeSeeds.toImmArray),
        ptx.contractState.globalKeyInputs.transform((_, v) => v.toKeyMapping),
        // TODO: for the moment, we ignore disclosed contracts completely
        ImmArray.empty[Node.Create],
      )
  }

  private[wasm] def incompleteTransaction
      : (Map[LfValue.ContractId, ContractInfo], PartialTransaction) = {
    (localContractStore, ptx)
  }

  private val logFunc: WasmHostFunction = wasmFunction("logInfo", 1, WasmUnitResultType) { param =>
    logInfo(param(0).toStringUtf8)

    ByteString.empty()
  }
}

object WasmRunner {

  final case class WasmExpr(module: ByteString, name: String, args: Array[Byte]*)

  def apply(
      submitters: Set[Party],
      readAs: Set[Party],
      seeding: speedy.InitialSeeding,
      submissionTime: Time.Timestamp,
      authorizationChecker: AuthorizationChecker,
      contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
      logger: ContextualizedLogger = ContextualizedLogger.get(classOf[WasmRunner]),
      pkgInterface: language.PackageInterface = PackageInterface.Empty,
      wasmExpr: WasmRunner.WasmExpr,
      activeContractStore: ParticipantContractStore,
      initialLocalContractStore: Map[LfValue.ContractId, ContractInfo] = Map.empty,
  )(implicit loggingContext: LoggingContext): WasmRunner = {
    new WasmRunner(
      submitters = submitters,
      readAs = readAs,
      submissionTime = submissionTime,
      logger = logger,
      pkgInterface = pkgInterface,
      wasmExpr = wasmExpr,
      activeContractStore = activeContractStore,
      initialLocalContractStore = initialLocalContractStore,
      initialPtx = PartialTransaction
        .initial(
          contractKeyUniqueness,
          seeding,
          submitters,
          authorizationChecker,
        ),
    )
  }

  private[wasm] def toSValue(value: LfValue): SValue = value match {
    case LfValue.ValueUnit =>
      SValue.SUnit
    case LfValue.ValueBool(b) =>
      SValue.SBool(b)
    case LfValue.ValueInt64(i) =>
      SValue.SInt64(i)
    case LfValue.ValueDate(date) =>
      SValue.SDate(date)
    case LfValue.ValueTimestamp(ts) =>
      SValue.STimestamp(ts)
    case LfValue.ValueNumeric(n) =>
      SValue.SNumeric(n)
    case LfValue.ValueParty(party) =>
      SValue.SParty(party)
    case LfValue.ValueText(txt) =>
      SValue.SText(txt)
    case LfValue.ValueContractId(cid) =>
      SValue.SContractId(cid)
    case LfValue.ValueOptional(optValue) =>
      SValue.SOptional(optValue.map(toSValue))
    case LfValue.ValueList(values) =>
      SValue.SList(values.map(toSValue))
    case LfValue.ValueTextMap(values) =>
      SValue.SMap(
        isTextMap = true,
        values.iterator.map { case (k, v) => (SValue.SText(k), toSValue(v)) }.toSeq: _*
      )
    case LfValue.ValueGenMap(values) =>
      SValue.SMap(
        isTextMap = false,
        values.iterator.map { case (k, v) => (toSValue(k), toSValue(v)) }.toSeq: _*
      )
    case LfValue.ValueRecord(Some(tyCon), fields) =>
      SValue.SRecord(
        tyCon,
        fields.map(_._1.get),
        ArrayList.from(fields.map(kv => toSValue(kv._2)).toArray[SValue]),
      )
    case LfValue.ValueRecord(None, fields) =>
      // As this is not really used anywhere else, use a fake type constructor
      val tyCon = Ref.Identifier.assertFromString("package:module:record")
      val fieldNames =
        ImmArray.from(fields.indices.map(index => Ref.Name.assertFromString(s"_$index")))
      SValue.SRecord(
        tyCon,
        fieldNames,
        ArrayList.from(fields.map(kv => toSValue(kv._2)).toArray[SValue]),
      )
    case LfValue.ValueVariant(Some(tyCon), variant, value) =>
      // No applications, so rank is always 0
      SValue.SVariant(tyCon, variant, 0, toSValue(value))
    case LfValue.ValueVariant(None, variant, value) =>
      // As this is not really used anywhere else, use a fake type constructor
      val tyCon = Ref.Identifier.assertFromString("package:module:variant")
      // No applications, so rank is always 0
      SValue.SVariant(tyCon, variant, 0, toSValue(value))
    case LfValue.ValueEnum(Some(tyCon), value) =>
      // No applications, so rank is always 0
      SValue.SEnum(tyCon, value, 0)
    case LfValue.ValueEnum(None, value) =>
      // As this is not really used anywhere else, use a fake type constructor
      val tyCon = Ref.Identifier.assertFromString("package:module:enum")
      // No applications, so rank is always 0
      SValue.SEnum(tyCon, value, 0)
  }
}
