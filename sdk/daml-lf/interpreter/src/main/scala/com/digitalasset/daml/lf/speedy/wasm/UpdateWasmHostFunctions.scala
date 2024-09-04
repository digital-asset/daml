// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.value.{
  Value => LfValue,
  ValueCoder => LfValueCoder,
  ValueOuterClass => proto,
}
import com.dylibso.chicory.runtime.{HostFunction => WasmHostFunction}

import scala.concurrent.duration.Duration

abstract class UpdateWasmHostFunctions(pkgInterface: PackageInterface)
    extends SpeedyUtils(pkgInterface) { self: WasmRunnerHostFunctions =>
  import WasmUtils._
  import WasmRunnerHostFunctions._

  val createContractFunc: WasmHostFunction =
    wasmFunction("createContract", 2, WasmValueResultType) { param =>
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
      val contractId = createContract(templateId, arg)

      LfValueCoder
        .encodeValue(txVersion, LfValue.ValueContractId(contractId))
        .fold(err => throw new RuntimeException(err.toString), identity)
    }

  val fetchContractArgFunc: WasmHostFunction =
    wasmFunction("fetchContractArg", 3, WasmValueResultType) { param =>
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
      val arg = fetchContractArg(templateId, optContractId.get, timeout)

      LfValueCoder
        .encodeValue(txVersion, arg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    }

  val exerciseChoiceFunc: WasmHostFunction =
    wasmFunction("exerciseChoice", 5, WasmValueResultType) { param =>
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

      val result = exerciseChoice(templateId, optContractId.get, choiceName, choiceArg, consuming)

      LfValueCoder
        .encodeValue(txVersion, result)
        .fold(err => throw new RuntimeException(err.toString), identity)
    }
}
