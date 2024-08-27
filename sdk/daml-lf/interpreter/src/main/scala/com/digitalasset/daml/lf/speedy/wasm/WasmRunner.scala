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

  override def createContract(templateId: Ref.TypeConRef, argsV: LfValue): LfValue.ContractId = {
    val templateTypeCon = templateId.assertToTypeConName
    val (pkgName, pkgVersion) = tmplId2PackageNameVersion(templateTypeCon)
    val argsSV = toSValue(argsV)
    val txVersion = tmplId2TxVersion(templateTypeCon)
    // TODO: cache contractInfo against the contractId (c.f. Speedy)?
    val contractInfo = ContractInfo(
      txVersion,
      pkgName,
      pkgVersion,
      templateTypeCon,
      argsSV,
      submitters,
      readAs,
      None,
    )
    // TODO: global vs local contract IDs??
    val (contractId, updatedPtx) = ptx.insertCreate(submissionTime, contractInfo, None).toOption.get

    ptx = updatedPtx

    contractId
  }

  override def fetchContractArg(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
  ): LfValue = {
    val templateTypeCon = templateId.assertToTypeConName
    val txVersion = tmplId2TxVersion(templateTypeCon)

    // We rely on the caller to timeout this WASM computation
    Await.result(
      activeContractStore.lookupActiveContract(submitters ++ readAs, contractId),
      Duration.Inf,
    ) match {
      case Some(contract) => {
        val contractInfo = ContractInfo(
          txVersion,
          contract.unversioned.packageName,
          contract.unversioned.packageVersion,
          templateTypeCon,
          toSValue(contract.unversioned.arg),
          submitters,
          readAs,
          None,
        )
        val updatedPtx =
          ptx.insertFetch(contractId, contractInfo, None, byKey = false, txVersion).toOption.get

        ptx = updatedPtx

        contract.unversioned.arg
      }

      case None =>
        ???
    }
  }

  def evaluateWasmExpression(
      wasmExpr: WasmExpr,
      // TODO: speedy uses this param for NeedTime callbacks
      @unused ledgerTime: Time.Timestamp,
  ): Either[SErrorCrash, UpdateMachine.Result] = {
    val logFunc = wasmFunction("logInfo", 1, WasmUnitResultType) { param =>
      logInfo(param(0).toStringUtf8)

      ByteString.empty()
    }
    val createContractFunc = wasmFunction("createContract", 2, WasmValueResultType) { param =>
      // NB. as we do not need to compute the contract instance, we do not need the funcPtr to the template constructor
      val templateId =
        LfValueCoder.decodeIdentifier(proto.Identifier.parseFrom(param(0))).toOption.get.toRef
      val txVersion = tmplId2TxVersion(templateId.assertToTypeConName)
      val arg = LfValueCoder.decodeValue(txVersion, param(1)).toOption.get
      val contractId = createContract(templateId, arg)

      LfValueCoder.encodeValue(txVersion, LfValue.ValueContractId(contractId)).toOption.get
    }
    val fetchContractArgFunc = wasmFunction("fetchContractArg", 2, WasmValueResultType) { param =>
      val templateId =
        LfValueCoder.decodeIdentifier(proto.Identifier.parseFrom(param(0))).toOption.get.toRef
      val txVersion = tmplId2TxVersion(templateId.assertToTypeConName)
      val optContractId = LfValueCoder.decodeValue(txVersion, param(1)).toOption.get match {
        case LfValue.ValueContractId(contractId) =>
          Some(contractId)

        case _ =>
          None
      }
      val arg = fetchContractArg(templateId, optContractId.get)

      LfValueCoder.encodeValue(txVersion, arg).toOption.get
    }
    val imports = new WasmHostImports(
      Array[WasmHostFunction](logFunc, createContractFunc, fetchContractArgFunc)
    )
    implicit val instance: WasmInstance =
      WasmModule.builder(wasmExpr.module.toByteArray).withHostImports(imports).build().instantiate()
    val machine = instance.export(wasmExpr.name)

    // Calling imported host functions applies a series of (transactional) side effects to ptx
    val _ = machine.apply(wasmExpr.args.map(copyByteArray).flatten: _*)

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

  private def toSValue(value: LfValue): SValue = value match {
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

object WasmRunner {
  final case class WasmExpr(module: ByteString, name: String, args: Array[Byte]*)

  private val WasmValueParameterType = List(WasmValueType.I32)
  private val WasmUnitResultType = None
  private val WasmValueResultType = Some(WasmValueType.I32)
  private val i32Size = WasmValueType.I32.size()

  private def wasmFunction(name: String, numOfParams: Int, returnType: Option[WasmValueType])(
      lambda: Array[ByteString] => ByteString
  ): WasmHostFunction = {
    new WasmHostFunction(
      (instance: WasmInstance, args: Array[WasmValue]) => {
        require(args.length == numOfParams)

        copyByteString(
          lambda((0 until numOfParams).map(copyWasmValues(args, _)(instance)).toArray)
        )(instance)
      },
      "env",
      name,
      (0 until numOfParams).flatMap(_ => WasmValueParameterType).asJava,
      returnType.toList.asJava,
    )
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
}
