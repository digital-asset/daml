// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import com.daml.http.ErrorMessages.cannotResolveTemplateId
import com.daml.http.domain.{ContractTypeId, HasTemplateId}
import com.daml.http.json.JsValueToApiValueConverter.mustBeApiRecord
import com.daml.http.util.FutureUtil.either
import com.daml.http.util.Logging.InstanceUUID
import com.daml.http.{PackageService, domain}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.LoggingContextOf
import scalaz.std.option._
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.applicative.{ToFunctorOps => _, _}
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, Bifoldable, EitherT, Traverse}
import scalaz.EitherT.eitherT
import spray.json.{JsValue, JsonReader}

import scala.concurrent.{ExecutionContext, Future}
import scalaz.std.scalaFuture._
import com.daml.ledger.api.{domain => LedgerApiDomain}

class DomainJsonDecoder(
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    resolveTemplateRecordType: PackageService.ResolveTemplateRecordType,
    resolveChoiceArgType: PackageService.ResolveChoiceArgType,
    resolveKeyType: PackageService.ResolveKeyType,
    jsValueToApiValue: (domain.LfType, JsValue) => JsonError \/ lav1.value.Value,
    jsValueToLfValue: (domain.LfType, JsValue) => JsonError \/ domain.LfValue,
) {

  import com.daml.http.util.ErrorOps._
  type ET[A] = EitherT[Future, JsonError, A]

  def decodeCreateCommand(a: JsValue, jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      ev1: JsonReader[
        domain.CreateCommand[JsValue, ContractTypeId.Template.OptionalPkg]
      ],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.CreateCommand[lav1.value.Record, ContractTypeId.Template.RequiredPkg]] = {
    val err = "DomainJsonDecoder_decodeCreateCommand"
    for {
      fj <- either(
        SprayJson
          .decode[domain.CreateCommand[JsValue, ContractTypeId.Template.OptionalPkg]](a)
          .liftErrS(err)(JsonError)
      )

      tmplId <- templateId_(fj.templateId, jwt, ledgerId)
      payloadT <- either(templateRecordType(tmplId))

      fv <- either(
        fj
          .copy(templateId = tmplId)
          .traversePayload(x => jsValueToApiValue(payloadT, x).flatMap(mustBeApiRecord))
      )
    } yield fv
  }

  def decodeUnderlyingValues[F[_]: Traverse: domain.HasTemplateId.Compat](
      fa: F[JsValue],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[F[lav1.value.Value]] = {
    for {
      damlLfId <- lookupLfType(fa, jwt, ledgerId)
      apiValue <- either(fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue)))
    } yield apiValue
  }

  def decodeUnderlyingValuesToLf[F[_]: Traverse: domain.HasTemplateId.Compat](
      fa: F[JsValue],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[F[domain.LfValue]] = {
    for {
      lfType <- lookupLfType(fa, jwt, ledgerId)
      lfValue <- either(fa.traverse(jsValue => jsValueToLfValue(lfType, jsValue)))
    } yield lfValue
  }

  private def lookupLfType[F[_]](fa: F[_], jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      H: HasTemplateId[F],
  ): ET[H.TypeFromCtId] =
    for {
      tId <- templateId_(H.templateId(fa), jwt, ledgerId)
      lfType <- either(
        H
          .lfType(fa, tId, resolveTemplateRecordType, resolveChoiceArgType, resolveKeyType)
          .liftErrS("DomainJsonDecoder_lookupLfType")(JsonError)
      )
    } yield lfType

  private[http] def decodeContractLocatorKey(
      a: domain.ContractLocator[JsValue],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.ContractLocator[domain.LfValue]] =
    a match {
      case k: domain.EnrichedContractKey[JsValue] =>
        decodeUnderlyingValuesToLf[domain.EnrichedContractKey](k, jwt, ledgerId).map(_.widen)
      case c: domain.EnrichedContractId =>
        (c: domain.ContractLocator[domain.LfValue]).pure[ET]
    }

  def decodeExerciseCommand(a: JsValue, jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      ev1: JsonReader[domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[
    domain.ExerciseCommand.RequiredPkg[domain.LfValue, domain.ContractLocator[domain.LfValue]]
  ] =
    for {
      cmd0 <- either(
        SprayJson
          .decode[domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]](a)
          .liftErrS("DomainJsonDecoder_decodeExerciseCommand")(JsonError)
      )

      ifIdlfType <- lookupLfType[
        domain.ExerciseCommand.OptionalPkg[+*, domain.ContractLocator[_]]
      ](
        cmd0,
        jwt,
        ledgerId,
      )
      (oIfaceId, argLfType) = ifIdlfType
      // treat an inferred iface ID as a user-specified one
      choiceIfaceOverride <-
        if (oIfaceId.isDefined)
          (oIfaceId: Option[domain.ContractTypeId.Interface.RequiredPkg]).pure[ET]
        else cmd0.choiceInterfaceId.traverse(templateId_(_, jwt, ledgerId))

      lfArgument <- either(jsValueToLfValue(argLfType, cmd0.argument))
      lfMeta <- cmd0.meta traverse (decodeMeta(_, jwt, ledgerId))

      cmd1 <-
        cmd0
          .copy(argument = lfArgument, choiceInterfaceId = choiceIfaceOverride, meta = lfMeta)
          .bitraverse(
            _.point[ET],
            ref => decodeContractLocatorKey(ref, jwt, ledgerId),
          ): ET[domain.ExerciseCommand.RequiredPkg[domain.LfValue, domain.ContractLocator[
          domain.LfValue
        ]]]

    } yield cmd1

  def decodeCreateAndExerciseCommand(a: JsValue, jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(
      implicit
      ev1: JsonReader[
        domain.CreateAndExerciseCommand[
          JsValue,
          JsValue,
          ContractTypeId.Template.OptionalPkg,
          ContractTypeId.OptionalPkg,
        ]
      ],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): EitherT[Future, JsonError, domain.CreateAndExerciseCommand.LAVResolved] = {
    val err = "DomainJsonDecoder_decodeCreateAndExerciseCommand"
    for {
      fjj <- either(
        SprayJson
          .decode[domain.CreateAndExerciseCommand[
            JsValue,
            JsValue,
            ContractTypeId.Template.OptionalPkg,
            ContractTypeId.OptionalPkg,
          ]](a)
          .liftErrS(err)(JsonError)
      ).flatMap(_.bitraverse(templateId_(_, jwt, ledgerId), templateId_(_, jwt, ledgerId)))

      tId = fjj.templateId
      ciId = fjj.choiceInterfaceId

      payloadT <- either(resolveTemplateRecordType(tId).liftErr(JsonError))

      oIfIdArgT <- either(resolveChoiceArgType(ciId getOrElse tId, fjj.choice).liftErr(JsonError))
      (oIfaceId, argT) = oIfIdArgT

      payload <- either(jsValueToApiRecord(payloadT, fjj.payload))
      argument <- either(jsValueToApiValue(argT, fjj.argument))
      meta <- fjj.meta traverse (decodeMetaPayloads(_, jsValueToApiRecord))
    } yield fjj.copy(
      payload = payload,
      argument = argument,
      choiceInterfaceId = oIfaceId orElse fjj.choiceInterfaceId,
      meta = meta,
    )
  }

  private[this] def jsValueToApiRecord(t: domain.LfType, v: JsValue) =
    jsValueToApiValue(t, v) flatMap mustBeApiRecord

  private[this] def decodeMeta(
      meta: domain.CommandMeta[ContractTypeId.Template.OptionalPkg, JsValue],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.CommandMeta[ContractTypeId.Template.Resolved, domain.LfValue]] = for {
    resolved <- resolveMetaTemplateIds(meta, jwt, ledgerId)
    // then use all the resolved template IDs for the disclosed contracts
    decoded <- decodeMetaPayloads(resolved)
  } yield decoded

  private[this] def resolveMetaTemplateIds[U, R, LfV](
      meta: domain.CommandMeta[U with ContractTypeId.OptionalPkg, LfV],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      resolveOverload: PackageService.ResolveContractTypeId.Overload[U, R],
  ): ET[domain.CommandMeta[R, LfV]] = for {
    // resolve as few template IDs as possible
    tpidToResolved <- {
      import scalaz.std.vector._
      val inputTpids = Bifoldable[domain.CommandMeta].leftFoldable[Any].toSet(meta)
      inputTpids.toVector
        .traverse { ot => templateId_(ot, jwt, ledgerId) strengthL ot }
        .map(_.toMap)
    }
  } yield meta leftMap tpidToResolved

  private[this] def decodeMetaPayloads[R](
      meta: domain.CommandMeta[ContractTypeId.Template.Resolved, JsValue],
      decode: (domain.LfType, JsValue) => JsonError \/ R = jsValueToLfValue,
  )(implicit
      ec: ExecutionContext
  ): ET[domain.CommandMeta[ContractTypeId.Template.Resolved, R]] = for {
    disclosedContracts <- either {
      import scalaz.std.list._
      meta.disclosedContracts traverse (_ traverse { dc =>
        val tpid = dc.templateId
        dc.traverse(jsrec =>
          resolveTemplateRecordType(tpid) liftErr JsonError
            flatMap (decode(_, jsrec))
        )
      })
    }
  } yield meta.copy(disclosedContracts = disclosedContracts)

  private def templateId_[U, R](
      id: U with domain.ContractTypeId.OptionalPkg,
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      resolveOverload: PackageService.ResolveContractTypeId.Overload[U, R],
  ): ET[R] =
    eitherT(
      resolveContractTypeId(jwt, ledgerId)(id)
        .map(_.toOption.flatten.toRightDisjunction(JsonError(cannotResolveTemplateId(id))))
    )

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

  def templateRecordType(
      id: domain.ContractTypeId.Template.RequiredPkg
  ): JsonError \/ domain.LfType =
    resolveTemplateRecordType(id).liftErr(JsonError)

  def keyType(id: domain.ContractTypeId.Template.OptionalPkg)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  ): ET[domain.LfType] =
    templateId_(id, jwt, ledgerId).flatMap {
      case it: domain.ContractTypeId.Template.Resolved =>
        either(resolveKeyType(it: ContractTypeId.Template.Resolved).liftErr(JsonError))
      case other =>
        either(-\/(JsonError(s"Expect contract type Id to be template Id, got otherwise: $other")))
    }
}
