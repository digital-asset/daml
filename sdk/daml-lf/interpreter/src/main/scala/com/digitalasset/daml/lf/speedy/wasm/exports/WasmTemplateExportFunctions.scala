// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

package exports

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.dylibso.chicory.runtime.{Instance => WasmInstance}

object WasmTemplateExportFunctions {
  import internal.WasmRunnerExportFunctions._

  private[wasm] def wasmTemplatePrecondFunction(
      templateName: String,
      txVersion: TransactionVersion,
  )(
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

  private[wasm] def wasmTemplateSignatoriesFunction(
      templateName: String,
      txVersion: TransactionVersion,
  )(
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

  private[wasm] def wasmTemplateObserversFunction(
      templateName: String,
      txVersion: TransactionVersion,
  )(
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
}
