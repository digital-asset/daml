// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

package exports

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.dylibso.chicory.runtime.{Instance => WasmInstance}

object WasmChoiceExportFunctions {
  import internal.WasmRunnerExportFunctions._

  private[wasm] def wasmChoiceExerciseFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    wasmChoiceFunction(s"${choiceName}_choice_exercise", txVersion)(contractArg, choiceArg)
  }

  private[wasm] def wasmChoiceControllersFunction(
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

  private[wasm] def wasmChoiceObserversFunction(
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

  private[wasm] def wasmChoiceAuthorizersFunction(
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
}
