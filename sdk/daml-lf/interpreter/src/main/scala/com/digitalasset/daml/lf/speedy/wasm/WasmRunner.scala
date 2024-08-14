// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, Node}
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

final class WasmRunner(
    submitters: Set[Party],
    @unused readAs: Set[Party],
    seeding: speedy.InitialSeeding,
    authorizationChecker: AuthorizationChecker,
    contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
    logger: ContextualizedLogger = ContextualizedLogger.get(classOf[WasmRunner]),
)(implicit loggingContext: LoggingContext)
    extends WasmRunnerHostFunctions {

  import WasmRunner._

  // TODO: make this a var once we have create host functions
  private[this] val ptx: PartialTransaction = PartialTransaction
    .initial(
      contractKeyUniqueness,
      seeding,
      submitters,
      authorizationChecker,
    )

  def logInfo(msg: String): Unit = {
    logger.info(msg)
  }

  def evaluateWasmExpression(
      wasmExpr: WasmExpr,
      // FIXME: speedy uses this param for NeedTime callbacks
      @unused ledgerTime: Time.Timestamp,
      @unused packageResolution: Map[Ref.PackageName, Ref.PackageId],
  ): Either[SErrorCrash, UpdateMachine.Result] = {
    val logFunc = wasmFunction("logInfo", 1, List.empty) { param =>
      logInfo(param(0).toStringUtf8)

      ByteString.empty()
    }
    val imports = new WasmHostImports(Array[WasmHostFunction](logFunc))
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
        // FIXME: for the moment, we ignore disclosed contracts completely
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
}

object WasmRunner {
  final case class WasmExpr(module: ByteString, name: String, args: Array[Byte]*)

  private val WasmParameterType = List(WasmValueType.I32, WasmValueType.I32)
//  private val WasmResultType = List(WasmValueType.I32, WasmValueType.I32)

  private def wasmFunction(name: String, numOfParams: Int, returnType: List[WasmValueType])(
      lambda: Array[ByteString] => ByteString
  ): WasmHostFunction = {
    new WasmHostFunction(
      (instance: WasmInstance, args: Array[WasmValue]) => {
        require(args.length == 2 * numOfParams)

        copyByteString(
          lambda((0 until numOfParams).map(copyWasmValues(args, _)(instance)).toArray)
        )(instance)
      },
      "env",
      name,
      (0 until numOfParams).flatMap(_ => WasmParameterType).asJava,
      returnType.asJava,
    )
  }

  private def copyWasmValues(values: Array[WasmValue], index: Int)(implicit
      instance: WasmInstance
  ): ByteString = {
    require(0 <= index && 2 * index + 1 < values.length)

    ByteString.copyFrom(
      instance.memory().readBytes(values(2 * index).asInt(), values(2 * index + 1).asInt())
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
    val alloc = instance.export("alloc")
    val valuePtr = alloc.apply(WasmValue.i32(value.length))(0).asInt

    // TODO: do we need to also store the length here? Probably OK?
    instance.memory().write(valuePtr, value)

    Array(WasmValue.i32(valuePtr), WasmValue.i32(value.length))
  }
}
