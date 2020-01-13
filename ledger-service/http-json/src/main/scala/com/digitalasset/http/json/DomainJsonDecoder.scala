// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.http.ErrorMessages.cannotResolveTemplateId
import com.digitalasset.http.domain.HasTemplateId
import com.digitalasset.http.{PackageService, domain}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/, \/-}
import spray.json.{JsObject, JsValue, JsonReader}

import scala.language.higherKinds

class DomainJsonDecoder(
    resolveTemplateId: PackageService.ResolveTemplateId,
    resolveTemplateRecordType: PackageService.ResolveTemplateRecordType,
    resolveRecordType: PackageService.ResolveChoiceRecordType,
    resolveKey: PackageService.ResolveKeyType,
    jsObjectToApiRecord: (domain.LfType, JsObject) => JsonError \/ lav1.value.Record,
    jsValueToApiValue: (domain.LfType, JsValue) => JsonError \/ lav1.value.Value,
    jsValueToLfValue: (domain.LfType, JsValue) => JsonError \/ domain.LfValue) {

  def decodeR[F[_]](a: String)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.parse(a).leftMap(e => JsonError(e.shows))
      c <- SprayJson.mustBeJsObject(b)
      d <- decodeR(c)
    } yield d

  def decodeR[F[_]](a: JsObject)(
      implicit ev1: JsonReader[F[JsObject]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Record] =
    for {
      b <- SprayJson.decode[F[JsObject]](a)(ev1).leftMap(e => JsonError(e.shows))
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
      b <- SprayJson.parse(a).leftMap(e => JsonError(e.shows))
      d <- decodeV(b)
    } yield d

  def decodeV[F[_]](a: JsValue)(
      implicit ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: domain.HasTemplateId[F]): JsonError \/ F[lav1.value.Value] =
    for {
      b <- SprayJson.decode[F[JsValue]](a)(ev1).leftMap(e => JsonError(e.shows))
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
        JsonError(s"DomainJsonDecoder_lookupLfType: ${cannotResolveTemplateId(templateId)}"))
      lfType <- H
        .lfType(fa, tId, resolveTemplateRecordType, resolveRecordType, resolveKey)
        .leftMap(e => JsonError("DomainJsonDecoder_lookupLfType " + e.shows))
    } yield lfType
  }

  def decodeContractLocator(a: String)(implicit ev: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ContractLocator[domain.LfValue] =
    for {
      b <- SprayJson
        .parse(a)
        .leftMap(e => JsonError("DomainJsonDecoder_decodeContractLocator " + e.shows))
      c <- decodeContractLocator(b)
    } yield c

  def decodeContractLocator(a: JsValue)(implicit ev: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ContractLocator[domain.LfValue] =
    SprayJson
      .decode[domain.ContractLocator[JsValue]](a)
      .leftMap(e => JsonError("DomainJsonDecoder_decodeContractLocator " + e.shows))
      .flatMap {
        case k: domain.EnrichedContractKey[JsValue] =>
          decodeUnderlyingValuesToLf[domain.EnrichedContractKey](k)
        case c: domain.EnrichedContractId =>
          \/-(c)
      }

  def decodeExerciseCommand(a: String)(
      implicit ev1: JsonReader[domain.ExerciseCommand[JsValue, JsValue]],
      ev2: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ExerciseCommand[domain.LfValue, domain.ContractLocator[domain.LfValue]] =
    for {
      b <- SprayJson
        .parse(a)
        .leftMap(e => JsonError("DomainJsonDecoder_decodeExerciseCommand " + e.shows))
      c <- decodeExerciseCommand(b)
    } yield c

  def decodeExerciseCommand(a: JsValue)(
      implicit ev1: JsonReader[domain.ExerciseCommand[JsValue, JsValue]],
      ev2: JsonReader[domain.ContractLocator[JsValue]])
    : JsonError \/ domain.ExerciseCommand[domain.LfValue, domain.ContractLocator[domain.LfValue]] =
    for {
      cmd0 <- SprayJson
        .decode[domain.ExerciseCommand[JsValue, JsValue]](a)
        .leftMap(e => JsonError("DomainJsonDecoder_decodeExerciseCommand " + e.shows))

      ref <- decodeContractLocator(cmd0.reference)

      cmd1 = cmd0.copy(reference = ref): domain.ExerciseCommand[
        JsValue,
        domain.ContractLocator[domain.LfValue]]

      lfType <- lookupLfType[domain.ExerciseCommand[+?, domain.ContractLocator[_]]](cmd1)(
        domain.ExerciseCommand.hasTemplateId)

      arg <- jsValueToLfValue(lfType, cmd1.argument)

    } yield cmd1.copy(argument = arg)
}
