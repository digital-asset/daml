// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.speedy.SResult.SVisibleToStakeholders
import com.digitalasset.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, Node, TransactionVersion}
import com.digitalasset.daml.lf.value.{
  Value => LfValue,
  ValueCoder => LfValueCoder,
  ValueOuterClass => proto,
}
import com.dylibso.chicory.runtime.{
  HostFunction => WasmHostFunction,
  HostImports => WasmHostImports,
  Instance => WasmInstance,
  Module => WasmModule,
}
import com.dylibso.chicory.wasm.types.{Value => WasmValue, ValueType => WasmValueType}
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._
import scala.annotation.unused
import scala.concurrent.Await
import scala.concurrent.duration.Duration

final class WasmRunner(
    submitters: Set[Party],
    readAs: Set[Party],
    seeding: speedy.InitialSeeding,
    submissionTime: Time.Timestamp,
    authorizationChecker: AuthorizationChecker,
    contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
    logger: ContextualizedLogger = ContextualizedLogger.get(classOf[WasmRunner]),
    pkgInterface: language.PackageInterface = PackageInterface.Empty,
    activeContractStore: ParticipantContractStore,
)(implicit loggingContext: LoggingContext)
    extends WasmRunnerHostFunctions {

  import WasmRunner._

  // TODO: do we care about Speedy limit enforcement?
  // TODO: what about being able to run in validation mode (c.f. Speedy)?

  // Holds the template constructor and argument for contracts created in this transaction
  private[this] var localContractStore = Map.empty[LfValue.ContractId, ContractInfo]
  private[this] var ptx: PartialTransaction = PartialTransaction
    .initial(
      contractKeyUniqueness,
      seeding,
      submitters,
      authorizationChecker,
    )

  override def logInfo(msg: String): Unit = {
    logger.info(msg)
  }

  override def createContract(templateId: Ref.TypeConRef, argsV: LfValue)(implicit
      instance: WasmInstance
  ): LfValue.ContractId = {
    val templateTypeCon = templateId.assertToTypeConName
    val (pkgName, pkgVersion) = tmplId2PackageNameVersion(templateTypeCon)
    val argsSV = toSValue(argsV)
    val txVersion = tmplId2TxVersion(templateTypeCon)
    val templateName = templateId.qName.name.segments.head
    // TODO: ideally, we should spawn and use a new *pure* WasmInstance here
    val precond = wasmTemplatePrecondFunction(templateName, txVersion)(argsV)

    if (precond) {
      // TODO: ideally, we should spawn and use a new *pure* WasmInstance here
      val signatories = wasmTemplateSignatoriesFunction(templateName, txVersion)(argsV)
      val observers = wasmTemplateObserversFunction(templateName, txVersion)(argsV)
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
  )(implicit instance: WasmInstance): LfValue = {
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
              // TODO: ideally, we should spawn and use a new *pure* WasmInstance here
              val precond =
                wasmTemplatePrecondFunction(templateName, txVersion)(contract.unversioned.arg)

              if (precond) {
                // TODO: ideally, we should spawn and use a new *pure* WasmInstance here
                val signatories =
                  wasmTemplateSignatoriesFunction(templateName, txVersion)(contract.unversioned.arg)
                val observers =
                  wasmTemplateObserversFunction(templateName, txVersion)(contract.unversioned.arg)
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
  )(implicit instance: WasmInstance): LfValue = {
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
    // TODO: ideally, we should spawn and use a new *pure* WasmInstance here
    val controllers =
      wasmChoiceControllersFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)
    val observers = wasmChoiceObserversFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)
    val authorizers =
      wasmChoiceAuthorizersFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)

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
      // TODO: ideally, we should spawn and use a new WasmInstance here
      val result = wasmChoiceExerciseFunction(choiceName, txVersion)(contractInfo.arg, choiceArg)

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

  def evaluateWasmExpression(
      wasmExpr: WasmExpr,
      // TODO: speedy uses this param for NeedTime callbacks
      @unused ledgerTime: Time.Timestamp,
  ): Either[SErrorCrash, UpdateMachine.Result] = {
    val logFunc = wasmFunction("logInfo", 1, WasmUnitResultType) { param => _ =>
      logInfo(param(0).toStringUtf8)

      ByteString.empty()
    }
    val createContractFunc = wasmFunction("createContract", 2, WasmValueResultType) {
      param => instance =>
        // NB. as we do not need to compute the contract instance, we do not need the funcPtr to the template constructor
        val templateId =
          LfValueCoder
            .decodeIdentifier(proto.Identifier.parseFrom(param(0)))
            .fold(err => throw new RuntimeException(err.toString), identity)
            .toRef
        val txVersion = tmplId2TxVersion(templateId.assertToTypeConName)
        val arg = LfValueCoder
          .decodeValue(txVersion, param(1))
          .fold(err => throw new RuntimeException(err.toString), identity)
        val contractId = createContract(templateId, arg)(instance)

        LfValueCoder
          .encodeValue(txVersion, LfValue.ValueContractId(contractId))
          .fold(err => throw new RuntimeException(err.toString), identity)
    }
    val fetchContractArgFunc = wasmFunction("fetchContractArg", 3, WasmValueResultType) {
      param => instance =>
        val templateId =
          LfValueCoder
            .decodeIdentifier(proto.Identifier.parseFrom(param(0)))
            .fold(err => throw new RuntimeException(err.toString), identity)
            .toRef
        val txVersion = tmplId2TxVersion(templateId.assertToTypeConName)
        val optContractId = LfValueCoder
          .decodeValue(txVersion, param(1))
          .fold(err => throw new RuntimeException(err.toString), identity) match {
          case LfValue.ValueContractId(contractId) =>
            Some(contractId)

          case _ =>
            None
        }
        val timeout = Duration(param(2).toStringUtf8)
        val arg = fetchContractArg(templateId, optContractId.get, timeout)(instance)

        LfValueCoder
          .encodeValue(txVersion, arg)
          .fold(err => throw new RuntimeException(err.toString), identity)
    }
    val exerciseChoiceFunc = wasmFunction("exerciseChoice", 5, WasmValueResultType) {
      param => instance =>
        val templateId =
          LfValueCoder
            .decodeIdentifier(proto.Identifier.parseFrom(param(0)))
            .fold(err => throw new RuntimeException(err.toString), identity)
            .toRef
        val txVersion = tmplId2TxVersion(templateId.assertToTypeConName)
        val optContractId = LfValueCoder
          .decodeValue(txVersion, param(1))
          .fold(err => throw new RuntimeException(err.toString), identity) match {
          case LfValue.ValueContractId(contractId) =>
            Some(contractId)

          case _ =>
            None
        }
        val choiceName = Ref.ChoiceName.assertFromString(param(2).toStringUtf8)
        val choiceArg = LfValueCoder
          .decodeValue(txVersion, param(3))
          .fold(err => throw new RuntimeException(err.toString), identity)
        assert(
          param(4).toByteArray.length == 1,
          s"exerciseChoice(_, _, _, _, consuming: bool): invalid byte encoding ${param(4).toByteArray.map("%02x".format(_)).mkString}",
        )
        val consuming = param(4).toByteArray.head match {
          case 0 => false
          case 1 => true
          case _ => ??? // TODO: manage invalid bool value case
        }

        val result =
          exerciseChoice(templateId, optContractId.get, choiceName, choiceArg, consuming)(instance)

        LfValueCoder
          .encodeValue(txVersion, result)
          .fold(err => throw new RuntimeException(err.toString), identity)
    }
    val imports = new WasmHostImports(
      Array[WasmHostFunction](logFunc, createContractFunc, fetchContractArgFunc, exerciseChoiceFunc)
    )

    implicit val instance: WasmInstance =
      WasmModule.builder(wasmExpr.module.toByteArray).withHostImports(imports).build().instantiate()

    val exprEvaluator = instance.export(wasmExpr.name)

    // Calling imported host functions applies a series of (transactional) side effects to ptx
    val _ = exprEvaluator.apply(wasmExpr.args.map(copyByteArray).flatten: _*)

    finish
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

  @throws[IllegalArgumentException]
  private def zipSameLength[X, Y](xs: ImmArray[X], ys: ImmArray[Y]): ImmArray[(X, Y)] = {
    val n1 = xs.length
    val n2 = ys.length
    if (n1 != n2) {
      throw new IllegalArgumentException(s"sameLengthZip, $n1 /= $n2")
    }
    xs.zip(ys)
  }

  private def tmplId2TxVersion(tmplId: Ref.TypeConName): TransactionVersion = {
    TransactionVersion.assignNodeVersion(
      pkgInterface.packageLanguageVersion(tmplId.packageId)
    )
  }

  private def tmplId2PackageNameVersion(
      tmplId: Ref.TypeConName
  ): (Ref.PackageName, Option[Ref.PackageVersion]) = {
    pkgInterface.signatures(tmplId.packageId).pkgNameVersion
  }
}

object WasmRunner {
  final case class WasmExpr(module: ByteString, name: String, args: Array[Byte]*)

  private val WasmValueParameterType = List(WasmValueType.I32)
  private val WasmUnitResultType = None
  private val WasmValueResultType = Some(WasmValueType.I32)
  private val i32Size = WasmValueType.I32.size()

  private def wasmFunction(name: String, numOfParams: Int, returnType: Option[WasmValueType])(
      lambda: Array[ByteString] => WasmInstance => ByteString
  ): WasmHostFunction = {
    new WasmHostFunction(
      (instance: WasmInstance, args: Array[WasmValue]) => {
        require(args.length == numOfParams)

        copyByteString(
          lambda((0 until numOfParams).map(copyWasmValues(args, _)(instance)).toArray)(instance)
        )(instance)
      },
      "env",
      name,
      (0 until numOfParams).flatMap(_ => WasmValueParameterType).asJava,
      returnType.toList.asJava,
    )
  }

  private def wasmChoiceExerciseFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    wasmChoiceFunction(s"${choiceName}_choice_exercise", txVersion)(contractArg, choiceArg)
  }

  private def wasmChoiceControllersFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): Set[Party] = {
    wasmChoiceFunction(s"${choiceName}_choice_controllers", txVersion)(
      contractArg,
      choiceArg,
    ) match {
      case LfValue.ValueList(values) =>
        values
          .map {
            case LfValue.ValueParty(party) =>
              party
            case _ =>
              // TODO: manage fall through case
              ???
          }
          .iterator
          .toSet

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmChoiceObserversFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): Set[Party] = {
    wasmChoiceFunction(s"${choiceName}_choice_observers", txVersion)(
      contractArg,
      choiceArg,
    ) match {
      case LfValue.ValueList(values) =>
        values
          .map {
            case LfValue.ValueParty(party) =>
              party
            case _ =>
              // TODO: manage fall through case
              ???
          }
          .iterator
          .toSet

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmChoiceAuthorizersFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit
      instance: WasmInstance
  ): Option[Set[Party]] = {
    wasmChoiceFunction(s"${choiceName}_choice_authorizers", txVersion)(
      contractArg,
      choiceArg,
    ) match {
      case LfValue.ValueOptional(Some(LfValue.ValueList(values))) =>
        val authorizers = values
          .map {
            case LfValue.ValueParty(party) =>
              party
            case _ =>
              // TODO: manage fall through case
              ???
          }
          .iterator
          .toSet
        Some(authorizers)

      case LfValue.ValueOptional(None) =>
        None

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmTemplatePrecondFunction(templateName: String, txVersion: TransactionVersion)(
      contractArg: LfValue
  )(implicit instance: WasmInstance): Boolean = {
    wasmTemplateFunction(s"${templateName}_precond", txVersion)(contractArg) match {
      case LfValue.ValueBool(result) =>
        result

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmTemplateSignatoriesFunction(templateName: String, txVersion: TransactionVersion)(
      contractArg: LfValue
  )(implicit instance: WasmInstance): Set[Party] = {
    wasmTemplateFunction(s"${templateName}_signatories", txVersion)(contractArg) match {
      case LfValue.ValueList(values) =>
        values
          .map {
            case LfValue.ValueParty(party) =>
              party
            case _ =>
              // TODO: manage fall through case
              ???
          }
          .iterator
          .toSet

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmTemplateObserversFunction(templateName: String, txVersion: TransactionVersion)(
      contractArg: LfValue
  )(implicit instance: WasmInstance): Set[Party] = {
    wasmTemplateFunction(s"${templateName}_observers", txVersion)(contractArg) match {
      case LfValue.ValueList(values) =>
        values
          .map {
            case LfValue.ValueParty(party) =>
              party
            case _ =>
              // TODO: manage fall through case
              ???
          }
          .iterator
          .toSet

      case _ =>
        // TODO: manage fall through case
        ???
    }
  }

  private def wasmChoiceFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    val choice = instance.export(choiceName)
    val contractArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, contractArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val choiceArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, choiceArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val choiceResultPtr = choice.apply(contractArgPtr.head, choiceArgPtr.head)
    try {
      if (choiceResultPtr.nonEmpty) {
        LfValueCoder
          .decodeValue(txVersion, copyWasmValue(choiceResultPtr))
          .fold(err => throw new RuntimeException(err.toString), identity)
      } else {
        LfValue.ValueUnit
      }
    } finally {
      deallocByteString(contractArgPtr.head)
      deallocByteString(choiceArgPtr.head)
      deallocByteString(choiceResultPtr.head)
    }
  }

  private def wasmTemplateFunction(
      functionName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    val function = instance.export(functionName)
    val contractArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, contractArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val resultPtr = function.apply(contractArgPtr.head)
    try {
      if (resultPtr.nonEmpty) {
        LfValueCoder
          .decodeValue(txVersion, copyWasmValue(resultPtr))
          .fold(err => throw new RuntimeException(err.toString), identity)
      } else {
        LfValue.ValueUnit
      }
    } finally {
      deallocByteString(contractArgPtr.head)
      deallocByteString(resultPtr.head)
    }
  }

  private def copyWasmValue(values: Array[WasmValue])(implicit
      instance: WasmInstance
  ): ByteString = {
    copyWasmValues(values, 0)
  }

  private def copyWasmValues(values: Array[WasmValue], index: Int)(implicit
      instance: WasmInstance
  ): ByteString = {
    require(0 <= index && index < values.length)

    val byteStringPtr = values(index).asInt()
    val ptr = instance.memory().readI32(byteStringPtr)
    val size = instance.memory().readI32(byteStringPtr + i32Size)

    ByteString.copyFrom(
      instance.memory().readBytes(ptr.asInt(), size.asInt())
    )
  }

  private def copyByteString(
      value: ByteString
  )(implicit instance: WasmInstance): Array[WasmValue] = {
    copyByteArray(value.toByteArray)
  }

  private def copyByteArray(
      value: Array[Byte]
  )(implicit instance: WasmInstance): Array[WasmValue] = {
    if (value.isEmpty) {
      Array.empty
    } else {
      val alloc = instance.export("alloc")
      val valuePtr = alloc.apply(WasmValue.i32(value.length))(0).asInt
      val byteStringPtr = alloc.apply(WasmValue.i32(2 * i32Size))(0).asInt

      instance.memory().write(valuePtr, value)
      instance.memory().writeI32(byteStringPtr, valuePtr)
      instance.memory().writeI32(byteStringPtr + i32Size, value.length)

      Array(WasmValue.i32(byteStringPtr))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private def deallocByteString(byteStringPtr: WasmValue)(implicit instance: WasmInstance): Unit = {
    val dealloc = instance.export("dealloc")
    val valuePtr = instance.memory().readI32(byteStringPtr.asInt())
    val size = instance.memory().readI32(byteStringPtr.asInt() + i32Size)

    discard {
      dealloc.apply(valuePtr, size)
      dealloc.apply(byteStringPtr, WasmValue.i32(2 * i32Size))
    }
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
