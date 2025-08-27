// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.interpretation
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

private[lf] final class ValueTranslator(
    pkgInterface: language.PackageInterface,
    requireContractIdSuffix: Boolean,
    shouldCheckDataSerializable: Boolean = true,
) {

  private[this] val speedyValueTranslator =
    new speedy.ValueTranslator(pkgInterface, requireContractIdSuffix, shouldCheckDataSerializable)

  private[this] def translateError(
      error: interpretation.Error.Dev.TranslationError.Error
  ): Error.Preprocessing.Error = {
    import interpretation.Error.Dev.TranslationError._
    error match {
      case LookupError(lookupError) => Error.Preprocessing.Lookup(lookupError)
      case TypeMismatch(expectedType, actualValue, message) =>
        Error.Preprocessing.TypeMismatch(expectedType, actualValue, message)
      case ValueNesting(value) =>
        Error.Preprocessing.ValueNesting(value)
      case NonSuffixedV1ContractId(cid) =>
        Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(cid)
      case NonSuffixedV2ContractId(cid) =>
        Error.Preprocessing.IllegalContractId.NonSuffixV2ContractId(cid)
    }
  }

  /** Executes an action and rethrows any [[interpretation.Error.Dev.TranslationError.Error]]
    * it may throw as an [[Error.Preprocessing.Error]]
    */
  @throws[Error.Preprocessing.Error]
  private[this] def translateException[A](f: => A): A = {
    try {
      f
    } catch {
      case e: interpretation.Error.Dev.TranslationError.Error =>
        throw translateError(e)
    }
  }

  @throws[Error.Preprocessing.Error]
  def validateCid(cid: Value.ContractId): Unit =
    translateException(speedyValueTranslator.validateCid(cid))

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateCid(cid: ContractId): SValue.SContractId =
    translateException(speedyValueTranslator.unsafeTranslateCid(cid))

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateValue(ty: Type, value: Value): SValue =
    translateException(speedyValueTranslator.unsafeTranslateValue(ty, value))

  def translateValue(
      ty: Type,
      value: Value,
  ): Either[Error.Preprocessing.Error, SValue] =
    speedyValueTranslator.translateValue(ty, value).left.map(translateError)
}
