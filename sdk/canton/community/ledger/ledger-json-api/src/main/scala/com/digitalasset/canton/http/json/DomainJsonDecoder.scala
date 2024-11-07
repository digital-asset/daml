// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2 as lav2
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.ErrorMessages.cannotResolveTemplateId
import com.digitalasset.canton.http.domain.{ContractTypeId, HasTemplateId}
import com.digitalasset.canton.http.util.FutureUtil.either
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.http.{PackageService, domain}
import com.digitalasset.daml.lf.data.Ref
import scalaz.EitherT.eitherT
import scalaz.std.option.*
import scalaz.std.scalaFuture.*
import scalaz.syntax.applicative.{ToFunctorOps as _, *}
import scalaz.syntax.bitraverse.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, Foldable, Traverse, \/}
import spray.json.{JsValue, JsonReader}

import scala.concurrent.{ExecutionContext, Future}

import JsValueToApiValueConverter.mustBeApiRecord

class DomainJsonDecoder(
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    resolveTemplateRecordType: PackageService.ResolveTemplateRecordType,
    resolveChoiceArgType: PackageService.ResolveChoiceArgType,
    resolveKeyType: PackageService.ResolveKeyType,
    jsValueToApiValue: (domain.LfType, JsValue) => JsonError \/ lav2.value.Value,
    jsValueToLfValue: (domain.LfType, JsValue) => JsonError \/ domain.LfValue,
) {

  import com.digitalasset.canton.http.util.ErrorOps.*
  type ET[A] = EitherT[Future, JsonError, A]

  def decodeCreateCommand(a: JsValue, jwt: Jwt)(implicit
      ev1: JsonReader[
        domain.CreateCommand[JsValue, ContractTypeId.Template.RequiredPkg]
      ],
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.CreateCommand[lav2.value.Record, ContractTypeId.Template.RequiredPkg]] = {
    val err = "DomainJsonDecoder_decodeCreateCommand"
    for {
      fj <- either(
        SprayJson
          .decode[domain.CreateCommand[JsValue, ContractTypeId.Template.RequiredPkg]](a)
          .liftErrS(err)(JsonError)
      )

      tmplId <- templateId_(fj.templateId, jwt)
      payloadT <- either(templateRecordType(tmplId.latestPkgId))

      fv <- either(
        fj
          .copy(templateId = tmplId.original)
          .traversePayload(x => jsValueToApiValue(payloadT, x).flatMap(mustBeApiRecord))
      )
    } yield fv
  }

  def decodeUnderlyingValues[F[_]: Traverse: domain.HasTemplateId.Compat](
      fa: F[JsValue],
      jwt: Jwt,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[F[lav2.value.Value]] =
    for {
      damlLfId <- lookupLfType(fa, jwt)
      apiValue <- either(fa.traverse(jsValue => jsValueToApiValue(damlLfId, jsValue)))
    } yield apiValue

  def decodeUnderlyingValuesToLf[F[_]: Traverse: domain.HasTemplateId.Compat](
      fa: F[JsValue],
      jwt: Jwt,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[F[domain.LfValue]] =
    for {
      lfType <- lookupLfType(fa, jwt)
      lfValue <- either(fa.traverse(jsValue => jsValueToLfValue(lfType, jsValue)))
    } yield lfValue

  private def lookupLfType[F[_]](fa: F[_], jwt: Jwt)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      H: HasTemplateId[F],
  ): ET[H.TypeFromCtId] =
    for {
      tId <- templateId_(H.templateId(fa), jwt)
      lfType <- either(
        H
          .lfType(
            fa,
            tId.latestPkgId,
            resolveTemplateRecordType,
            resolveChoiceArgType,
            resolveKeyType,
          )
          .liftErrS("DomainJsonDecoder_lookupLfType")(JsonError)
      )
    } yield lfType

  def decodeContractLocatorKey(
      a: domain.ContractLocator[JsValue],
      jwt: Jwt,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): ET[domain.ContractLocator[domain.LfValue]] =
    a match {
      case k: domain.EnrichedContractKey[JsValue] =>
        decodeUnderlyingValuesToLf[domain.EnrichedContractKey](k, jwt).map(_.widen)
      case c: domain.EnrichedContractId =>
        (c: domain.ContractLocator[domain.LfValue]).pure[ET]
    }

  def decodeExerciseCommand(a: JsValue, jwt: Jwt)(implicit
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
      )
      (oIfaceId, argLfType) = ifIdlfType
      // treat an inferred iface ID as a user-specified one
      choiceIfaceOverride <-
        if (oIfaceId.isDefined)
          oIfaceId.map(i => i.map(p => Ref.PackageRef.Id(p): Ref.PackageRef)).pure[ET]
        else cmd0.choiceInterfaceId.traverse(templateId_(_, jwt).map(_.original))

      lfArgument <- either(jsValueToLfValue(argLfType, cmd0.argument))
      metaWithResolvedIds <- cmd0.meta.traverse(resolveMetaTemplateIds(_, jwt))

      cmd1 <-
        cmd0
          .copy(
            argument = lfArgument,
            choiceInterfaceId = choiceIfaceOverride,
            meta = metaWithResolvedIds,
          )
          .bitraverse(
            _.point[ET],
            ref => decodeContractLocatorKey(ref, jwt),
          ): ET[domain.ExerciseCommand.RequiredPkg[domain.LfValue, domain.ContractLocator[
          domain.LfValue
        ]]]

    } yield cmd1

  def decodeCreateAndExerciseCommand(a: JsValue, jwt: Jwt)(implicit
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
      ).flatMap(_.bitraverse(templateId_(_, jwt), templateId_(_, jwt)))

      tId = fjj.templateId.latestPkgId
      ciId = fjj.choiceInterfaceId.map(_.latestPkgId)

      payloadT <- either(resolveTemplateRecordType(tId).liftErr(JsonError))
      oIfIdArgT <- either(resolveChoiceArgType(ciId getOrElse tId, fjj.choice).liftErr(JsonError))
      (oIfaceId, argT) = oIfIdArgT

      payload <- either(jsValueToApiRecord(payloadT, fjj.payload))
      argument <- either(jsValueToApiValue(argT, fjj.argument))

      choiceIfaceOverride =
        if (oIfaceId.isDefined) oIfaceId.map(i => i.map(p => Ref.PackageRef.Id(p): Ref.PackageRef))
        else fjj.choiceInterfaceId.map(_.original)

      cmd <- fjj.bitraverse(_.original.pure[ET], _.pure[ET])
    } yield cmd.copy(
      payload = payload,
      argument = argument,
      choiceInterfaceId = choiceIfaceOverride,
    )
  }

  private[this] def jsValueToApiRecord(t: domain.LfType, v: JsValue) =
    jsValueToApiValue(t, v) flatMap mustBeApiRecord

  private[this] def resolveMetaTemplateIds[
      U,
      CtId[T] <: ContractTypeId[T] with ContractTypeId.Ops[CtId, T],
  ](
      meta: domain.CommandMeta[U with ContractTypeId.RequiredPkg],
      jwt: Jwt,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      resolveOverload: PackageService.ResolveContractTypeId.Overload[U, CtId],
  ): ET[domain.CommandMeta[CtId[Ref.PackageRef]]] = for {
    // resolve as few template IDs as possible
    tpidToResolved <- {
      import scalaz.std.vector.*
      val inputTpids = Foldable[domain.CommandMeta].toSet(meta)
      inputTpids.toVector
        .traverse(ot => templateId_(ot, jwt).map(_.original) strengthL ot)
        .map(_.toMap)
    }
  } yield meta map tpidToResolved

  private def templateId_[U, CtId[T] <: ContractTypeId[T] with ContractTypeId.Ops[CtId, T]](
      id: U with ContractTypeId.RequiredPkg,
      jwt: Jwt,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      resolveOverload: PackageService.ResolveContractTypeId.Overload[U, CtId],
  ): ET[domain.ContractTypeRef[CtId]] =
    eitherT(
      resolveContractTypeId(jwt)(id)
        .map(_.toOption.flatten.toRightDisjunction(JsonError(cannotResolveTemplateId(id))))
    )

  def templateRecordType(
      id: domain.ContractTypeId.Template.RequiredPkgId
  ): JsonError \/ domain.LfType =
    resolveTemplateRecordType(id).liftErr(JsonError)

  def keyType(id: domain.ContractTypeId.Template.RequiredPkg)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      jwt: Jwt,
  ): ET[domain.LfType] =
    templateId_(id, jwt).map(_.latestPkgId).flatMap {
      case it: domain.ContractTypeId.Template.ResolvedPkgId =>
        either(resolveKeyType(it: ContractTypeId.Template.ResolvedPkgId).liftErr(JsonError))
      case other =>
        either(-\/(JsonError(s"Expect contract type Id to be template Id, got otherwise: $other")))
    }
}
