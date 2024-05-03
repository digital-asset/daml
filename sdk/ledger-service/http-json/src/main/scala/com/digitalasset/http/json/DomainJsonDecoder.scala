// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import scalaz.syntax.applicative.{ToFunctorOps => _, _}
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Foldable, Traverse, \/}
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
        domain.CreateCommand[JsValue, ContractTypeId.Template.RequiredPkg]
      ],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.CreateCommand[lav1.value.Record, ContractTypeId.Template.RequiredPkg]] = {
    val err = "DomainJsonDecoder_decodeCreateCommand"
    for {
      fj <- either(
        SprayJson
          .decode[domain.CreateCommand[JsValue, ContractTypeId.Template.RequiredPkg]](a)
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
      ev1: JsonReader[domain.ExerciseCommand.RequiredPkg[JsValue, domain.ContractLocator[JsValue]]],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[
    domain.ExerciseCommand.RequiredPkg[domain.LfValue, domain.ContractLocator[domain.LfValue]]
  ] =
    for {
      cmd0 <- either(
        SprayJson
          .decode[domain.ExerciseCommand.RequiredPkg[JsValue, domain.ContractLocator[JsValue]]](a)
          .liftErrS("DomainJsonDecoder_decodeExerciseCommand")(JsonError)
      )

      ifIdlfType <- lookupLfType[
        domain.ExerciseCommand.RequiredPkg[+*, domain.ContractLocator[_]]
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
        else cmd0.choiceInterfaceId.traverse(id => templateId_(id, jwt, ledgerId))

      lfArgument <- either(jsValueToLfValue(argLfType, cmd0.argument))
      metaWithResolvedIds <- cmd0.meta.traverse(resolveMetaTemplateIds(_, jwt, ledgerId))

      cmd1 <-
        cmd0
          .copy(
            argument = lfArgument,
            choiceInterfaceId = choiceIfaceOverride,
            meta = metaWithResolvedIds,
          )
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
          ContractTypeId.Template.RequiredPkg,
          ContractTypeId.RequiredPkg,
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
            ContractTypeId.Template.RequiredPkg,
            ContractTypeId.RequiredPkg,
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
    } yield fjj.copy(
      payload = payload,
      argument = argument,
      choiceInterfaceId = oIfaceId orElse fjj.choiceInterfaceId,
      meta = fjj.meta,
    )
  }

  private[this] def jsValueToApiRecord(t: domain.LfType, v: JsValue) =
    jsValueToApiValue(t, v) flatMap mustBeApiRecord

  private[this] def resolveMetaTemplateIds[U, R <: ContractTypeId.Resolved, LfV](
      meta: domain.CommandMeta[U with ContractTypeId.RequiredPkg],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.CommandMeta[R]] = for {
    // resolve as few template IDs as possible
    tpidToResolved <- {
      import scalaz.std.vector._
      val inputTpids = Foldable[domain.CommandMeta].toSet(meta)
      inputTpids.toVector
        .traverse { ot => templateId_(ot, jwt, ledgerId) strengthL ot }
        .map(_.toMap)
    }
  } yield meta map tpidToResolved map (_.asInstanceOf[R])

  private def templateId_[CtId[T] <: domain.ContractTypeId[T] with domain.ContractTypeId.Ops[CtId, T]](
      id: CtId[String],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.ContractTypeId.ResolvedOf[CtId]] =
    eitherT(
      resolveContractTypeId(jwt, ledgerId)(id)
        .map(
          _.toOption.flatten
            .map(_.original.asInstanceOf[ContractTypeId.ResolvedOf[CtId]])
            .toRightDisjunction(JsonError(cannotResolveTemplateId(id)))
        )
    )

  // TODO(Leo) see if you can get get rid of the above boilerplate and rely on the JsonReaders defined below

  def templateRecordType(
      id: domain.ContractTypeId.Template.RequiredPkg
  ): JsonError \/ domain.LfType =
    resolveTemplateRecordType(id).liftErr(JsonError)

  def keyType(id: domain.ContractTypeId.Template.RequiredPkg)(implicit
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
