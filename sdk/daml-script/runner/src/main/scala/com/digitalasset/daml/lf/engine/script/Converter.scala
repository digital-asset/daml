// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script

import com.daml.ledger.api.v2.value
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.typesig.EnvironmentSignature
import com.digitalasset.daml.lf.typesig.reader.SignatureReader
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  ExtendedValue,
  ExtendedValueAny,
  ExtendedValueTypeRep,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.script.converter.ConverterException
import com.digitalasset.canton.ledger.api.{PartyDetails, User, UserRight}
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, \/-}
import spray.json._

import scala.concurrent.Future
import scala.util.Random

// Helper to create identifiers pointing to the Daml.Script module
case class ScriptIds(val scriptPackageId: PackageId, isLegacy: Boolean) {
  def damlScriptModule(module: String, s: String) =
    Identifier(
      scriptPackageId,
      QualifiedName(ModuleName.assertFromString(module), DottedName.assertFromString(s)),
    )
}

object ScriptIds {
  // Constructs ScriptIds if the given type has the form Daml.Script.Script a (or Daml.Script.Internal.LowLevel.Script a).
  def fromType(ty: Type): Either[String, ScriptIds] = {
    ty match {
      case TApp(TTyCon(tyCon), _) => {
        val scriptIds = ScriptIds(tyCon.packageId, false)
        if (tyCon == scriptIds.damlScriptModule("Daml.Script.Internal.LowLevel", "Script")) {
          Right(scriptIds)
        } else if (tyCon == scriptIds.damlScriptModule("Daml.Script", "Script")) {
          // In legacy daml-script, the Script type was stored in the top level Daml.Script module
          // We still give back a scriptIds, so the runner can throw the error more formally
          Right(scriptIds.copy(isLegacy = true))
        } else {
          Left(s"Expected type 'Daml.Script.Script a' but got $ty")
        }
      }
      case _ => Left(s"Expected type 'Daml.Script.Script a' but got $ty")
    }
  }
}

final case class AnyTemplate(ty: Identifier, arg: ExtendedValue)
final case class AnyChoice(name: ChoiceName, arg: ExtendedValue)
final case class AnyContractKey(templateId: Identifier, ty: Type, key: ExtendedValue)
// frames ordered from most-recent to least-recent

final case class Disclosure(templatedId: TypeConId, contractId: ContractId, blob: Bytes)

object Converter {
  def apply(majorLanguageVersion: LanguageVersion.Major): ConverterMethods = {
    majorLanguageVersion match {
      case LanguageVersion.Major.V2 => com.digitalasset.daml.lf.engine.script.v2.Converter
      case _ => throw new IllegalArgumentException(s"${majorLanguageVersion.pretty} not supported")
    }
  }
}

abstract class ConverterMethods(stablePackages: language.StablePackages) {
  import com.digitalasset.daml.lf.script.converter.Converter._

  private def toNonEmptySet[A](as: OneAnd[FrontStack, A]): OneAnd[Set, A] = {
    import scalaz.syntax.foldable._
    OneAnd(as.head, as.tail.toSet - as.head)
  }

  private def fromIdentifier(id: value.Identifier): ExtendedValue = {
    ExtendedValueTypeRep(
      TTyCon(
        TypeConId(
          PackageId.assertFromString(id.packageId),
          QualifiedName(
            DottedName.assertFromString(id.moduleName),
            DottedName.assertFromString(id.entityName),
          ),
        )
      )
    )
  }

  private[lf] def fromTemplateTypeRep(templateId: ExtendedValue): ExtendedValue =
    record(stablePackages.TemplateTypeRep, ("getTemplateTypeRep", templateId))

  private[lf] def fromTemplateTypeRep(templateId: value.Identifier): ExtendedValue =
    fromTemplateTypeRep(fromIdentifier(templateId))

  private[lf] def fromTemplateTypeRep(templateId: Identifier): ExtendedValue =
    fromTemplateTypeRep(ExtendedValueTypeRep(TTyCon(templateId)))

  private[lf] def fromAnyContractId(
      scriptIds: ScriptIds,
      templateId: value.Identifier,
      contractId: ContractId,
  ): ExtendedValue = {
    val contractIdTy =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Util", "AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromTemplateTypeRep(templateId)),
      ("contractId", ValueContractId(contractId)),
    )
  }

  private[lf] def fromAnyException(
      tycon: Identifier,
      v: ExtendedValue,
  ): ExtendedValue = ExtendedValueAny(TTyCon(tycon), v)

  def toFuture[T](s: Either[String, T]): Future[T] = s match {
    case Left(err) => Future.failed(new ConverterException(err))
    case Right(s) => Future.successful(s)
  }

  private[lf] def fromAnyTemplate(
      templateId: Identifier,
      argument: ExtendedValue,
  ): ExtendedValue =
    record(
      stablePackages.AnyTemplate,
      ("getAnyTemplate", ExtendedValueAny(TTyCon(templateId), argument)),
    )

  def toAnyTemplate(v: ExtendedValue): Either[String, AnyTemplate] = {
    v match {
      case ValueRecord(_, ImmArray((_, ExtendedValueAny(TTyCon(ty), templateVal)))) =>
        Right(AnyTemplate(ty, templateVal))
      case _ => Left(s"Expected AnyTemplate but got $v")
    }
  }

  private[lf] def fromAnyChoice(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      argument: ExtendedValue,
  ): Either[String, ExtendedValue] =
    for {
      choice <- lookupChoice(templateId, interfaceId, choiceName)
    } yield record(
      stablePackages.AnyChoice,
      ("getAnyChoice", ExtendedValueAny(choice.argBinder._2, argument)),
      (
        "getAnyChoiceTemplateTypeRep",
        fromTemplateTypeRep(toApiIdentifier(interfaceId.getOrElse(templateId))),
      ),
    )

  private[this] def choiceArgTypeToChoiceName(choiceCons: TypeConId) = {
    // This exploits the fact that in Daml, choice argument type names
    // and choice names match up.
    assert(choiceCons.qualifiedName.name.segments.length == 1)
    choiceCons.qualifiedName.name.segments.head
  }

  private[lf] def toAnyChoice(v: ExtendedValue): Either[String, AnyChoice] =
    v match {
      case ValueRecord(_, ImmArray((_, ExtendedValueAny(TTyCon(choiceCons), choiceVal)), _)) =>
        Right(AnyChoice(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case _ =>
        Left(s"Expected AnyChoice but got $v")
    }

  def toAnyContractKey(v: ExtendedValue): Either[String, AnyContractKey] = {
    v match {
      case ValueRecord(_, ImmArray((_, ExtendedValueAny(ty, key)), (_, templateRep))) =>
        typeRepToIdentifier(templateRep).map(templateId => AnyContractKey(templateId, ty, key))
      case _ => Left(s"Expected AnyContractKey but got $v")
    }
  }

  def typeRepToIdentifier(v: ExtendedValue): Either[String, Identifier] = {
    v match {
      case ValueRecord(_, ImmArray((_, ExtendedValueTypeRep(TTyCon(ty))))) => Right(ty)
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  def fromAnyContractKey(key: AnyContractKey): ExtendedValue =
    record(
      stablePackages.AnyContractKey,
      ("getAnyContractKey", ExtendedValueAny(key.ty, key.key)),
      (
        "getAnyContractKeyTemplateTypeRep",
        fromTemplateTypeRep(key.templateId),
      ),
    )

  def toParty(v: ExtendedValue): Either[String, Party] =
    v match {
      case ValueParty(p) => Right(p)
      case _ => Left(s"Expected ValueParty but got $v")
    }

  def toParties(v: ExtendedValue): Either[String, OneAnd[Set, Party]] =
    v match {
      case ValueList(FrontStackCons(x, xs)) =>
        OneAnd(x, xs).traverse(toParty(_)).map(toNonEmptySet(_))
      case _ => Left(s"Expected non-empty ValueList but got $v")
    }

  def toTimestamp(v: ExtendedValue): Either[String, Time.Timestamp] =
    v match {
      case ValueTimestamp(t) => Right(t)
      case _ => Left(s"Expected ValueTimestamp but got $v")
    }

  def toInt(v: ExtendedValue): Either[String, Int] = v match {
    case ValueInt64(n) => Right(n.toInt)
    case v => Left(s"Expected ValueInt64 but got $v")
  }

  def toList[A](
      v: ExtendedValue,
      convertElem: ExtendedValue => Either[String, A],
  ): Either[String, List[A]] =
    v match {
      case ValueList(xs) =>
        xs.toImmArray.toList.traverse(convertElem)
      case _ => Left(s"Expected ValueList but got $v")
    }

  def toOptional[A](
      v: ExtendedValue,
      convertElem: ExtendedValue => Either[String, A],
  ): Either[String, Option[A]] =
    v match {
      case ValueOptional(v) => v.traverse(convertElem)
      case _ => Left(s"Expected ValueOptional but got $v")
    }

  private case class SrcLoc(
      pkgId: PackageId,
      module: ModuleName,
      start: (Int, Int),
      end: (Int, Int),
  )

  private def toSrcLoc(
      knownPackages: Map[String, PackageId],
      v: ExtendedValue,
  ): Either[String, SrcLoc] =
    v match {
      case ValueRecord(
            _,
            ImmArray(
              (_, unitId),
              (_, module),
              _,
              (_, startLine),
              (_, startCol),
              (_, endLine),
              (_, endCol),
            ),
          ) =>
        for {
          unitId <- toText(unitId)
          packageId <- unitId match {
            // GHC uses unit-id "main" for the current package,
            // but the scenario context expects "-homePackageId-".
            case "main" => PackageId.fromString("-homePackageId-")
            case id => knownPackages.get(id).toRight(s"Unknown package $id")
          }
          module <- toText(module).flatMap(ModuleName.fromString(_))
          startLine <- toInt(startLine)
          startCol <- toInt(startCol)
          endLine <- toInt(endLine)
          endCol <- toInt(endCol)
        } yield SrcLoc(packageId, module, (startLine, startCol), (endLine, endCol))
      case _ => Left(s"Expected SrcLoc but got $v")
    }

  def toLocation(
      knownPackages: Map[String, PackageId],
      v: ExtendedValue,
  ): Either[String, Location] =
    v match {
      case ValueRecord(_, ImmArray((_, definition), (_, loc))) =>
        for {
          // TODO[AH] This should be the outer definition. E.g. `main` in `main = do submit ...`.
          //   However, the call-stack only gives us access to the inner definition, `submit` in this case.
          //   The definition is not used when pretty printing locations. So, we can ignore this for now.
          definition <- toText(definition)
          loc <- toSrcLoc(knownPackages, loc)
        } yield Location(loc.pkgId, loc.module, definition, loc.start, loc.end)
      case _ => Left(s"Expected (Text, SrcLoc) but got $v")
    }

  def toStackTrace(
      knownPackages: Map[String, PackageId],
      v: ExtendedValue,
  ): Either[String, StackTrace] =
    v match {
      case ValueList(frames) =>
        frames.toVector.traverse(toLocation(knownPackages, _)).map(StackTrace(_))
      case _ =>
        new Throwable().printStackTrace();
        Left(s"Expected ValueList but got $v")
    }

  def toParticipantNames(v: ExtendedValue): Either[String, List[Participant]] = v match {
    case ValueList(vs) => vs.toList.traverse(toParticipantName)
    case _ => Left(s"Expected a list of participants but got $v")
  }

  def toOptionalParticipantName(v: ExtendedValue): Either[String, Option[Participant]] = v match {
    case ValueOptional(Some(ValueText(t))) => Right(Some(Participant(t)))
    case ValueOptional(None) => Right(None)
    case _ => Left(s"Expected optional participant name but got $v")
  }

  def toParticipantName(v: ExtendedValue): Either[String, Participant] = v match {
    case ValueText(t) => Right(Participant(t))
    case _ => Left(s"Expected participant name but got $v")
  }

  private def randomHex(rand: Random, length: Int) =
    s"%0${length}x".format(rand.nextLong()).takeRight(length)

  // If the givenIdHint is provided, we just use that as is.
  def toPartyIdHint(
      givenIdHint: String,
      requestedDisplayName: String,
      random: Random,
  ): Either[String, String] = {
    lazy val uniqueSuffix = randomHex(random, 8)
    (givenIdHint.nonEmpty, requestedDisplayName.nonEmpty) match {
      case (false, false) =>
        // Caller doesn't care so come up with something unique
        Right("party-" + uniqueSuffix)
      case (false, true) =>
        // Make unique id hint from name
        Right(requestedDisplayName + "-" + uniqueSuffix)
      case (true, false) =>
        // The caller is responsible for ensuring hints are unique
        Right(givenIdHint)
      case (true, true) =>
        if (givenIdHint != requestedDisplayName)
          Left(
            s"Requested name '$requestedDisplayName' cannot be different from id hint '$givenIdHint'"
          )
        else Right(givenIdHint)
    }
  }

  def fromApiIdentifier(id: value.Identifier): Either[String, Identifier] =
    for {
      packageId <- PackageId.fromString(id.packageId)
      moduleName <- DottedName.fromString(id.moduleName)
      entityName <- DottedName.fromString(id.entityName)
    } yield Identifier(packageId, QualifiedName(moduleName, entityName))

  def fromPartyDetails(
      scriptIds: ScriptIds,
      details: PartyDetails,
  ): ExtendedValue = {
    record(
      scriptIds
        .damlScriptModule("Daml.Script.Internal.Questions.PartyManagement", "PartyDetails"),
      ("party", ValueParty(details.party)),
      ("isLocal", ValueBool(details.isLocal)),
    )
  }

  def fromOptional[A](
      x: Option[A],
      f: A => Either[String, ExtendedValue],
  ): Either[String, ExtendedValue] =
    x.traverse(f).map(ValueOptional(_))

  def fromUser(scriptIds: ScriptIds, user: User): Either[String, ExtendedValue] =
    Right(
      record(
        scriptIds.damlScriptModule("Daml.Script.Internal.Questions.UserManagement", "User"),
        ("userId", fromUserId(scriptIds, user.id)),
        ("primaryParty", ValueOptional(user.primaryParty.map(ValueParty(_)))),
      )
    )

  def fromUserId(scriptIds: ScriptIds, userId: UserId): ExtendedValue =
    record(
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.UserManagement", "UserId"),
      ("unpack", ValueText(userId)),
    )

  def toUser(v: ExtendedValue): Either[String, User] =
    v match {
      case ValueRecord(_, ImmArray((_, idValue), (_, primaryPartyValue))) =>
        for {
          id <- toUserId(idValue)
          primaryParty <- toOptional(primaryPartyValue, toParty)
        } yield User(id, primaryParty)
      case _ => Left(s"Expected User but got $v")
    }

  def toUserId(v: ExtendedValue): Either[String, UserId] =
    v match {
      case ValueRecord(_, ImmArray((_, userNameValue))) =>
        for {
          userName <- toText(userNameValue)
          userId <- UserId.fromString(userName)
        } yield userId
      case _ => Left(s"Expected UserId but got $v")
    }

  def fromUserRight(
      scriptIds: ScriptIds,
      right: UserRight,
  ): Either[String, ExtendedValue] = {
    def toRight(constructor: String, value: ExtendedValue): ExtendedValue =
      ValueVariant(
        Some(
          scriptIds.damlScriptModule("Daml.Script.Internal.Questions.UserManagement", "UserRight")
        ),
        Name.assertFromString(constructor),
        value,
      )
    Right(right match {
      case UserRight.IdentityProviderAdmin =>
        // TODO #15857
        // Add support for the `IdentityProviderAdmin` in the Daml Script
        sys.error("IdentityProviderAdmin user right has not been supported yet")
      case UserRight.ParticipantAdmin => toRight("ParticipantAdmin", ValueUnit)
      case UserRight.CanActAs(p) => toRight("CanActAs", ValueParty(p))
      case UserRight.CanReadAs(p) => toRight("CanReadAs", ValueParty(p))
      case UserRight.CanReadAsAnyParty => toRight("CanReadAsAnyParty", ValueUnit)
      case UserRight.CanExecuteAs(p) => toRight("CanExecuteAs", ValueParty(p))
      case UserRight.CanExecuteAsAnyParty => toRight("CanExecuteAsAnyParty", ValueUnit)
    })
  }

  def toUserRight(v: ExtendedValue): Either[String, UserRight] =
    v match {
      case ValueVariant(_, "ParticipantAdmin", ValueUnit) =>
        Right(UserRight.ParticipantAdmin)
      case ValueVariant(_, "IdentityProviderAdmin", ValueUnit) =>
        // TODO #15857
        // Add support for the `IdentityProviderAdmin` in the Daml Script
        sys.error("IdentityProviderAdmin user right has not been supported yet")
      case ValueVariant(_, "CanReadAs", v) =>
        toParty(v).map(UserRight.CanReadAs(_))
      case ValueVariant(_, "CanActAs", v) =>
        toParty(v).map(UserRight.CanActAs(_))
      case ValueVariant(_, "CanReadAsAnyParty", ValueUnit) =>
        Right(UserRight.CanReadAsAnyParty)
      case ValueVariant(_, "CanExecuteAs", v) =>
        toParty(v).map(UserRight.CanExecuteAs(_))
      case ValueVariant(_, "CanExecuteAsAnyParty", ValueUnit) =>
        Right(UserRight.CanExecuteAsAnyParty)
      case _ => Left(s"Expected ParticipantAdmin, CanReadAs or CanActAs but got $v")
    }

  def toIfaceType(
      ctx: QualifiedName,
      astTy: Type,
  ): Either[String, typesig.Type] =
    SignatureReader.toIfaceType(ctx, astTy) match {
      case -\/(e) => Left(e.toString)
      case \/-(ty) => Right(ty)
    }

  private[lf] def fromJsonValue(
      ctx: QualifiedName,
      environmentSignature: EnvironmentSignature,
      ty: Type,
      jsValue: JsValue,
  ): Either[String, Value] = {
    def damlLfTypeLookup(id: Identifier): Option[typesig.DefDataType.FWT] =
      environmentSignature.typeDecls.get(id).map(_.`type`)
    for {
      paramIface <-
        toIfaceType(ctx, ty).left
          .map(s => s"Failed to convert $ty: $s")
      lfValue <-
        try {
          Right(
            jsValue.convertTo[Value](
              LfValueCodec.apiValueJsonReader(paramIface, damlLfTypeLookup(_))
            )
          )
        } catch {
          case e: Exception => Left(s"LF conversion failed: ${e.toString}")
        }
    } yield lfValue
  }

  def toDisclosure(v: ExtendedValue): Either[String, Disclosure] =
    v match {
      case ValueRecord(_, ImmArray((_, tplId), (_, cid), (_, blob))) =>
        for {
          tplId <- typeRepToIdentifier(tplId)
          cid <- toContractId(cid)
          blob <- toText(blob)
          blob <- Bytes.fromString(blob)
        } yield Disclosure(tplId, cid, blob)
      case _ =>
        Left(s"Expected Disclosure but got $v")
    }

  def noDisclosures(disclosures: List[Disclosure]) = toFuture(
    Either.cond(
      disclosures.isEmpty,
      (),
      "Explicit disclosures not supported",
    )
  )
}
