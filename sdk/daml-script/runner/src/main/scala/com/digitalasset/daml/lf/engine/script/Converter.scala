// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import com.daml.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.ledger.api.v1.value
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageMajorVersion, StablePackages}
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, \/-}
import spray.json._

import scala.concurrent.Future

// Helper to create identifiers pointing to the Daml.Script module
case class ScriptIds(val scriptPackageId: PackageId) {
  def damlScript(s: String) = damlScriptModule("Daml.Script", s)

  def damlScriptModule(module: String, s: String) =
    Identifier(
      scriptPackageId,
      QualifiedName(ModuleName.assertFromString(module), DottedName.assertFromString(s)),
    )
}

object ScriptIds {
  // Constructs ScriptIds if the given type has the form Daml.Script.Script a (or Daml.Script.Internal.LowLevel.Script a).
  def fromType(ty: Type): Option[ScriptIds] = {
    ty match {
      case TApp(TTyCon(tyCon), _) => {
        val scriptIds = ScriptIds(tyCon.packageId)
        // First is v1, second is v2 where Script was moved to Daml.Script.Internal.LowLevel
        if (
          tyCon == scriptIds.damlScript("Script")
          || tyCon == scriptIds.damlScriptModule("Daml.Script.Internal.LowLevel", "Script")
        ) {
          Some(scriptIds)
        } else {
          None
        }
      }
      case _ => None
    }
  }
}

final case class AnyTemplate(ty: Identifier, arg: SValue)
final case class AnyChoice(name: ChoiceName, arg: SValue)
final case class AnyContractKey(templateId: Identifier, ty: Type, key: SValue)
// frames ordered from most-recent to least-recent

final case class Disclosure(templatedId: TypeConName, contractId: ContractId, blob: Bytes)

object Converter {
  private val converters = LanguageMajorVersion.All.map(v => v -> new Converter(v)).toMap

  def apply(majorLanguageVersion: LanguageMajorVersion): Converter =
    converters(majorLanguageVersion)
}

class Converter(majorLanguageVersion: LanguageMajorVersion) {
  import com.daml.script.converter.Converter._

  val stablePackages = StablePackages(majorLanguageVersion)

  private def toNonEmptySet[A](as: OneAnd[FrontStack, A]): OneAnd[Set, A] = {
    import scalaz.syntax.foldable._
    OneAnd(as.head, as.tail.toSet - as.head)
  }

  private def fromIdentifier(id: value.Identifier): SValue = {
    STypeRep(
      TTyCon(
        TypeConName(
          PackageId.assertFromString(id.packageId),
          QualifiedName(
            DottedName.assertFromString(id.moduleName),
            DottedName.assertFromString(id.entityName),
          ),
        )
      )
    )
  }

  private[lf] def fromTemplateTypeRep(templateId: SValue): SValue =
    record(stablePackages.TemplateTypeRep, ("getTemplateTypeRep", templateId))

  private[lf] def fromTemplateTypeRep(templateId: value.Identifier): SValue =
    fromTemplateTypeRep(fromIdentifier(templateId))

  private[lf] def fromTemplateTypeRep(templateId: Identifier): SValue =
    fromTemplateTypeRep(STypeRep(TTyCon(templateId)))

  private[lf] def fromAnyContractId(
      scriptIds: ScriptIds,
      templateId: value.Identifier,
      contractId: ContractId,
  ): SValue = {
    val contractIdTy = scriptIds.damlScript("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromTemplateTypeRep(templateId)),
      ("contractId", SContractId(contractId)),
    )
  }

  def toFuture[T](s: Either[String, T]): Future[T] = s match {
    case Left(err) => Future.failed(new ConverterException(err))
    case Right(s) => Future.successful(s)
  }

  private[lf] def fromAnyTemplate(
      translator: preprocessing.ValueTranslator,
      templateId: Identifier,
      argument: Value,
      enableContractUpgrading: Boolean = false,
  ): Either[String, SValue] = {
    for {
      translated <- translator
        .translateValue(
          TTyCon(templateId),
          argument,
          if (enableContractUpgrading) preprocessing.ValueTranslator.Config.Upgradeable
          else preprocessing.ValueTranslator.Config.Strict,
        )
        .left
        .map(err => s"Failed to translate create argument: $err")
    } yield record(
      stablePackages.AnyTemplate,
      ("getAnyTemplate", SAny(TTyCon(templateId), translated)),
    )
  }

  def toAnyTemplate(v: SValue): Either[String, AnyTemplate] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case SAny(TTyCon(ty), templateVal) => Right(AnyTemplate(ty, templateVal))
          case v => Left(s"Expected SAny but got $v")
        }
      }
      case _ => Left(s"Expected AnyTemplate but got $v")
    }
  }

  private[lf] def fromAnyChoice(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      argument: Value,
      enableContractUpgrading: Boolean = false,
  ): Either[String, SValue] = {
    for {
      choice <- lookupChoice(templateId, interfaceId, choiceName)
      translated <- translator
        .translateValue(
          choice.argBinder._2,
          argument,
          if (enableContractUpgrading) preprocessing.ValueTranslator.Config.Upgradeable
          else preprocessing.ValueTranslator.Config.Strict,
        )
        .left
        .map(err => s"Failed to translate exercise argument: $err")
    } yield record(
      stablePackages.AnyChoice,
      ("getAnyChoice", SAny(choice.argBinder._2, translated)),
      (
        "getAnyChoiceTemplateTypeRep",
        fromTemplateTypeRep(toApiIdentifier(interfaceId.getOrElse(templateId))),
      ),
    )
  }

  private[this] def choiceArgTypeToChoiceName(choiceCons: TypeConName) = {
    // This exploits the fact that in Daml, choice argument type names
    // and choice names match up.
    assert(choiceCons.qualifiedName.name.segments.length == 1)
    choiceCons.qualifiedName.name.segments.head
  }

  private[lf] def toAnyChoice(v: SValue): Either[String, AnyChoice] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(choiceCons), choiceVal), _)) =>
        Right(AnyChoice(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case SRecord(
            _,
            _,
            ArrayList(
              SAny(
                TStruct(Struct((_, TTyCon(choiceCons)), _)),
                SStruct(_, ArrayList(choiceVal, _)),
              ),
              _,
            ),
          ) =>
        Right(AnyChoice(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case _ =>
        Left(s"Expected AnyChoice but got $v")
    }

  def toAnyContractKey(v: SValue): Either[String, AnyContractKey] = {
    v match {
      case SRecord(_, _, ArrayList(SAny(ty, key), templateRep)) =>
        typeRepToIdentifier(templateRep).map(templateId => AnyContractKey(templateId, ty, key))
      case _ => Left(s"Expected AnyContractKey but got $v")
    }
  }

  def typeRepToIdentifier(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case STypeRep(TTyCon(ty)) => Right(ty)
          case x => Left(s"Expected STypeRep but got $x")
        }
      }
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  def fromAnyContractKey(key: AnyContractKey): SValue =
    record(
      stablePackages.AnyContractKey,
      ("getAnyContractKey", SAny(key.ty, key.key)),
      (
        "getAnyContractKeyTemplateTypeRep",
        fromTemplateTypeRep(key.templateId),
      ),
    )

  private val fstName = Name.assertFromString("fst")
  private val sndName = Name.assertFromString("snd")
  private[lf] val tupleFieldInputOrder =
    Struct.assertFromSeq(List(fstName, sndName).zipWithIndex)
  private[lf] val fstOutputIdx = tupleFieldInputOrder.indexOf(fstName)
  private[lf] val sndOutputIdx = tupleFieldInputOrder.indexOf(sndName)

  private[lf] val extractToTuple = SEMakeClo(
    Array(),
    2,
    SEAppAtomic(SEBuiltin(SBStructCon(tupleFieldInputOrder)), Array(SELocA(0), SELocA(1))),
  )

  def toParty(v: SValue): Either[String, Party] =
    v match {
      case SParty(p) => Right(p)
      case _ => Left(s"Expected SParty but got $v")
    }

  def toParties(v: SValue): Either[String, OneAnd[Set, Party]] =
    v match {
      case SList(FrontStackCons(x, xs)) =>
        OneAnd(x, xs).traverse(toParty(_)).map(toNonEmptySet(_))
      case SParty(p) =>
        Right(
          OneAnd(p, Set())
        ) // For backwards compatibility, we support a single part here as well.
      case _ => Left(s"Expected non-empty SList but got $v")
    }

  def toTimestamp(v: SValue): Either[String, Time.Timestamp] =
    v match {
      case STimestamp(t) => Right(t)
      case _ => Left(s"Expected STimestamp but got $v")
    }

  def toInt(v: SValue): Either[String, Int] = v match {
    case SInt64(n) => Right(n.toInt)
    case v => Left(s"Expected SInt64 but got $v")
  }

  def toList[A](v: SValue, convertElem: SValue => Either[String, A]): Either[String, List[A]] =
    v match {
      case SList(xs) =>
        xs.toImmArray.toList.traverse(convertElem)
      case _ => Left(s"Expected SList but got $v")
    }

  def toOptional[A](
      v: SValue,
      convertElem: SValue => Either[String, A],
  ): Either[String, Option[A]] =
    v match {
      case SOptional(v) => v.traverse(convertElem)
      case _ => Left(s"Expected SOptional but got $v")
    }

  private case class SrcLoc(
      pkgId: PackageId,
      module: ModuleName,
      start: (Int, Int),
      end: (Int, Int),
  )

  private def toSrcLoc(knownPackages: Map[String, PackageId], v: SValue): Either[String, SrcLoc] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(unitId, module, file @ _, startLine, startCol, endLine, endCol),
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

  def toLocation(knownPackages: Map[String, PackageId], v: SValue): Either[String, Location] =
    v match {
      case SRecord(_, _, ArrayList(definition, loc)) =>
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
      v: SValue,
  ): Either[String, StackTrace] =
    v match {
      case SList(frames) =>
        frames.toVector.traverse(toLocation(knownPackages, _)).map(StackTrace(_))
      case _ =>
        new Throwable().printStackTrace();
        Left(s"Expected SList but got $v")
    }

  def toParticipantName(v: SValue): Either[String, Option[Participant]] = v match {
    case SOptional(Some(SText(t))) => Right(Some(Participant(t)))
    case SOptional(None) => Right(None)
    case _ => Left(s"Expected optional participant name but got $v")
  }

  def fromApiIdentifier(id: value.Identifier): Either[String, Identifier] =
    for {
      packageId <- PackageId.fromString(id.packageId)
      moduleName <- DottedName.fromString(id.moduleName)
      entityName <- DottedName.fromString(id.entityName)
    } yield Identifier(packageId, QualifiedName(moduleName, entityName))

  private[lf] def fromInterfaceView(
      translator: preprocessing.ValueTranslator,
      viewType: Type,
      value: Value,
  ): Either[String, SValue] = {
    for {
      translated <- translator
        .strictTranslateValue(viewType, value)
        .left
        .map(err => s"Failed to translate value of interface view: $err")
    } yield translated
  }

  def fromStatusException(
      scriptIds: ScriptIds,
      ex: StatusRuntimeException,
  ): Either[String, SValue] = {
    val status = ex.getStatus
    Right(
      record(
        scriptIds.damlScript("SubmitFailure"),
        ("status", SInt64(status.getCode.value.asInstanceOf[Long])),
        ("description", SText(status.getDescription)),
      )
    )
  }

  def fromPartyDetails(scriptIds: ScriptIds, details: PartyDetails): Either[String, SValue] = {
    Right(
      record(
        scriptIds.damlScript("PartyDetails"),
        ("party", SParty(details.party)),
        ("displayName", SOptional(details.displayName.map(SText))),
        ("isLocal", SBool(details.isLocal)),
      )
    )
  }

  def fromOptional[A](x: Option[A], f: A => Either[String, SValue]): Either[String, SOptional] =
    x.traverse(f).map(SOptional(_))

  def fromUser(scriptIds: ScriptIds, user: User): Either[String, SValue] =
    Right(
      record(
        scriptIds.damlScript("User"),
        ("userId", fromUserId(scriptIds, user.id)),
        ("primaryParty", SOptional(user.primaryParty.map(SParty(_)))),
      )
    )

  def fromUserId(scriptIds: ScriptIds, userId: UserId): SValue =
    record(
      scriptIds.damlScript("UserId"),
      ("unpack", SText(userId)),
    )

  def toUser(v: SValue): Either[String, User] =
    v match {
      case SRecord(_, _, vals) if vals.size >= 2 =>
        for {
          id <- toUserId(vals.get(0))
          primaryParty <- toOptional(vals.get(1), toParty)
        } yield User(id, primaryParty)
      case _ => Left(s"Expected User but got $v")
    }

  def toUserId(v: SValue): Either[String, UserId] =
    v match {
      case SRecord(_, _, vals) if vals.size == 1 =>
        for {
          userName <- toText(vals.get(0))
          userId <- UserId.fromString(userName)
        } yield userId
      case _ => Left(s"Expected UserId but got $v")
    }

  def fromUserRight(
      scriptIds: ScriptIds,
      right: UserRight,
  ): Either[String, SValue] = {
    def toRight(constructor: String, rank: Int, value: SValue): SValue =
      SVariant(scriptIds.damlScript("UserRight"), Name.assertFromString(constructor), rank, value)
    Right(right match {
      case UserRight.IdentityProviderAdmin =>
        // TODO #15857
        // Add support for the `IdentityProviderAdmin` in the Daml Script
        sys.error("IdentityProviderAdmin user right has not been supported yet")
      case UserRight.ParticipantAdmin => toRight("ParticipantAdmin", 0, SUnit)
      case UserRight.CanActAs(p) => toRight("CanActAs", 1, SParty(p))
      case UserRight.CanReadAs(p) => toRight("CanReadAs", 2, SParty(p))
    })
  }

  def toUserRight(v: SValue): Either[String, UserRight] =
    v match {
      case SVariant(_, "ParticipantAdmin", _, SUnit) =>
        Right(UserRight.ParticipantAdmin)
      case SVariant(_, "IdentityProviderAdmin", _, SUnit) =>
        // TODO #15857
        // Add support for the `IdentityProviderAdmin` in the Daml Script
        sys.error("IdentityProviderAdmin user right has not been supported yet")
      case SVariant(_, "CanReadAs", _, v) =>
        toParty(v).map(UserRight.CanReadAs(_))
      case SVariant(_, "CanActAs", _, v) =>
        toParty(v).map(UserRight.CanActAs(_))
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
      compiledPackages: CompiledPackages,
      ty: Type,
      jsValue: JsValue,
  ): Either[String, SValue] = {
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
      valueTranslator =
        new preprocessing.ValueTranslator(
          compiledPackages.pkgInterface,
          requireV1ContractIdSuffix = false,
        )
      sValue <- valueTranslator
        .strictTranslateValue(ty, lfValue)
        .left
        .map(_.message)
    } yield sValue
  }

  def toDisclosure(v: SValue): Either[String, Disclosure] =
    v match {
      case SRecord(_, _, ArrayList(tplId, cid, blob)) =>
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

  def makeTuple(v1: SValue, v2: SValue): SValue =
    record(stablePackages.Tuple2, ("_1", v1), ("_2", v2))

  def makeTuple(v1: SValue, v2: SValue, v3: SValue): SValue =
    record(stablePackages.Tuple3, ("_1", v1), ("_2", v2), ("_3", v3))
}
