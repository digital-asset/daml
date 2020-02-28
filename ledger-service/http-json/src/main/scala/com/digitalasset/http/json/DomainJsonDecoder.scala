// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.http.ErrorMessages.{
  cannotResolveChoiceArgType,
  cannotResolvePayloadType,
  cannotResolveTemplateId
}
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.json.JsValueToApiValueConverter.mustBeApiRecord
import com.digitalasset.http.{PackageService, domain}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/, \/-}
import spray.json.{JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: PackageService.ResolveTemplateId,
    resolveTemplateRecordType: PackageService.ResolveTemplateRecordType,
    resolveChoiceRecordType: PackageService.ResolveChoiceRecordType,
    resolveKey: PackageService.ResolveKeyType,
    jsValueToApiValue: (domain.LfType, JsValue) => JsonError \/ lav1.value.Value,
    jsValueToLfValue: (domain.LfType, JsValue) => JsonError \/ domain.LfValue
) {

  import com.digitalasset.http.util.ErrorOps._

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeCreateCommand(a: JsValue)(implicit ev1: JsonReader[domain.CreateCommand[JsValue]])
    : JsonError \/ domain.CreateCommand[lav1.value.Record] = {
    val err = "DomainJsonDecoder_decodeCreateCommand"
    for {
      fj <- SprayJson
        .decode[domain.CreateCommand[JsValue]](a)
        .liftErrS(err)(JsonError)

      tId <- resolveTemplateId(fj.templateId)
        .toRightDisjunction(JsonError(s"$err ${cannotResolveTemplateId(fj.templateId)}"))

      payloadT <- resolveTemplateRecordType(tId)
        .liftErrS(err + " " + cannotResolvePayloadType(tId))(JsonError)

      fv <- fj.traverse(x => jsValueToApiValue(payloadT, x).flatMap(mustBeApiRecord))
    } yield fv
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeUnderlyingValues[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsValue]): JsonError \/ F[lav1.value.Value] = {
    for {
      damlLfId <- lookupLfType(fa)
      apiValue <- fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue))
    } yield apiValue
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeUnderlyingValuesToLf[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsValue]): JsonError \/ F[domain.LfValue] = {
    for {
      lfType <- lookupLfType(fa)
      lfValue <- fa.traverse(jsValue => jsValueToLfValue(lfType, jsValue))
    } yield lfValue
  }

  private def lookupLfType[F[_]: domain.HasTemplateId](fa: F[_]): JsonError \/ domain.LfType = {
    val H: HasTemplateId[F] = implicitly
    val templateId: domain.TemplateId.OptionalPkg = H.templateId(fa)
    for {
      tId <- resolveTemplateId(templateId).toRightDisjunction(
        JsonError(s"DomainJsonDecoder_lookupLfType ${cannotResolveTemplateId(templateId)}"))
      lfType <- H
        .lfType(fa, tId, resolveTemplateRecordType, resolveChoiceRecordType, resolveKey)
        .liftErrS("DomainJsonDecoder_lookupLfType")(JsonError)
    } yield lfType
  }

  def decodeContractLocator(a: JsValue)(implicit ev: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ContractLocator[domain.LfValue] =
    SprayJson
      .decode[domain.ContractLocator[JsValue]](a)
      .liftErrS("DomainJsonDecoder_decodeContractLocator")(JsonError)
      .flatMap(decodeContractLocatorUnderlyingValue)

  private def decodeContractLocatorUnderlyingValue(
      a: domain.ContractLocator[JsValue]): JsonError \/ domain.ContractLocator[domain.LfValue] =
    a match {
      case k: domain.EnrichedContractKey[JsValue] =>
        decodeUnderlyingValuesToLf[domain.EnrichedContractKey](k)
      case c: domain.EnrichedContractId =>
        \/-(c)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeExerciseCommand(a: JsValue)(
      implicit ev1: JsonReader[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]])
    : JsonError \/ domain.ExerciseCommand[domain.LfValue, domain.ContractLocator[domain.LfValue]] =
    for {
      cmd0 <- SprayJson
        .decode[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]](a)
        .liftErrS("DomainJsonDecoder_decodeExerciseCommand")(JsonError)

      lfType <- lookupLfType[domain.ExerciseCommand[+?, domain.ContractLocator[_]]](cmd0)(
        domain.ExerciseCommand.hasTemplateId)

      cmd1 <- cmd0.bitraverse(
        arg => jsValueToLfValue(lfType, arg),
        ref => decodeContractLocatorUnderlyingValue(ref)
      ): JsonError \/ domain.ExerciseCommand[domain.LfValue, domain.ContractLocator[domain.LfValue]]

    } yield cmd1

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeCreateAndExerciseCommand(a: JsValue)(
      implicit ev1: JsonReader[domain.CreateAndExerciseCommand[JsValue, JsValue]])
    : JsonError \/ domain.CreateAndExerciseCommand[lav1.value.Record, lav1.value.Value] = {
    val err = "DomainJsonDecoder_decodeCreateAndExerciseCommand"
    for {
      fjj <- SprayJson
        .decode[domain.CreateAndExerciseCommand[JsValue, JsValue]](a)
        .liftErrS(err)(JsonError)

      tId <- resolveTemplateId(fjj.templateId)
        .toRightDisjunction(JsonError(s"$err ${cannotResolveTemplateId(fjj.templateId)}"))

      payloadT <- resolveTemplateRecordType(tId)
        .liftErrS(err + " " + cannotResolvePayloadType(tId))(JsonError)

      argT <- resolveChoiceRecordType(tId, fjj.choice)
        .liftErrS(err + " " + cannotResolveChoiceArgType(tId, fjj.choice))(JsonError)

      fvv <- fjj.bitraverse(
        x => jsValueToApiValue(payloadT, x).flatMap(mustBeApiRecord),
        x => jsValueToApiValue(argT, x)
      )

    } yield fvv
  }

  // TODO(Leo) see if you can get get rid of the above boilerplate and rely on the JsonReaders defined below

  def ApiValueJsonReader(lfType: domain.LfType): JsonReader[lav1.value.Value] =
    (json: JsValue) =>
      jsValueToApiValue(lfType, json).valueOr(e => spray.json.deserializationError(e.shows))

  def ApiRecordJsonReader(lfType: domain.LfType): JsonReader[lav1.value.Record] =
    (json: JsValue) =>
      SprayJson
        .mustBeJsObject(json)
        .flatMap(jsObj => jsValueToApiValue(lfType, jsObj).flatMap(mustBeApiRecord))
        .valueOr(e => spray.json.deserializationError(e.shows))
}
