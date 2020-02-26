// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.http.ErrorMessages.{
  cannotResolveChoiceArgType,
  cannotResolvePayloadType,
  cannotResolveTemplateId
}
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.{PackageService, domain}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.bitraverse._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/, \/-}
import spray.json.{JsObject, JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: PackageService.ResolveTemplateId,
    resolveTemplateRecordType: PackageService.ResolveTemplateRecordType,
    resolveChoiceRecordType: PackageService.ResolveChoiceRecordType,
    resolveKey: PackageService.ResolveKeyType,
    jsObjectToApiRecord: (domain.LfType, JsObject) => JsonError \/ lav1.value.Record,
    jsValueToApiValue: (domain.LfType, JsValue) => JsonError \/ lav1.value.Value,
    jsValueToLfValue: (domain.LfType, JsValue) => JsonError \/ domain.LfValue) {

  import com.digitalasset.http.util.ErrorOps._

  def decodeR[F[_]](a: String)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.parse(a).liftErr(JsonError)
      c <- SprayJson.mustBeJsObject(b)
      d <- decodeR(c)
    } yield d

  def decodeR[F[_]](a: JsObject)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.decode[F[JsObject]](a)(ev1).liftErr(JsonError)
      c <- decodeUnderlyingRecords(b)
    } yield c

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeUnderlyingRecords[F[_]: Traverse: domain.HasTemplateId](
      fa: F[JsObject]): JsonError \/ F[lav1.value.Record] = {
    for {
      lfType <- lookupLfType(fa)
      apiValue <- fa.traverse(jsObject => jsObjectToApiRecord(lfType, jsObject))
    } yield apiValue
  }

  def decodeV[F[_]](a: String)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.parse(a).liftErr(JsonError)
      d <- decodeV(b)
    } yield d

  def decodeV[F[_]](a: JsValue)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.decode[F[JsValue]](a)(ev1).liftErr(JsonError)
      c <- decodeUnderlyingValues(b)
    } yield c

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

  def decodeContractLocator(a: String)(implicit ev: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ContractLocator[domain.LfValue] =
    for {
      b <- SprayJson.parse(a).liftErrS("DomainJsonDecoder_decodeContractLocator")(JsonError)
      c <- decodeContractLocator(b)
    } yield c

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

  def decodeExerciseCommand(a: String)(
      implicit ev1: JsonReader[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]])
    : JsonError \/ domain.ExerciseCommand[domain.LfValue, domain.ContractLocator[domain.LfValue]] =
    for {
      b <- SprayJson.parse(a).liftErrS("DomainJsonDecoder_decodeExerciseCommand")(JsonError)
      c <- decodeExerciseCommand(b)
    } yield c

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

  def decodeCreateAndExerciseCommand(a: String)(
      implicit ev1: JsonReader[domain.CreateAndExerciseCommand[JsValue, JsValue]])
    : JsonError \/ domain.CreateAndExerciseCommand[lav1.value.Value, lav1.value.Value] =
    for {
      b <- SprayJson
        .parse(a)
        .liftErrS("DomainJsonDecoder_decodeCreateAndExerciseCommand")(JsonError)
      c <- decodeCreateAndExerciseCommand(b)
    } yield c

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeCreateAndExerciseCommand(a: JsValue)(
      implicit ev1: JsonReader[domain.CreateAndExerciseCommand[JsValue, JsValue]])
    : JsonError \/ domain.CreateAndExerciseCommand[lav1.value.Value, lav1.value.Value] = {
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

      fvv <- fjj.bitraverse(x => jsValueToApiValue(payloadT, x), x => jsValueToApiValue(argT, x))

    } yield fvv
  }
}
