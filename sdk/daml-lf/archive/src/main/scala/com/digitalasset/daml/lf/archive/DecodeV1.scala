// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.util
import com.daml.daml_lf_dev.{DamlLf1 => PLF}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Decimal, ImmArray, Numeric, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import LV.Features

private[archive] class DecodeCommon(languageVersion: LV) {

  import DecodeCommon._
  import Work.Ret

  private val minor: LV.Minor = languageVersion.minor

  def decodePackage( // entry point
      packageId: PackageId,
      lfPackage: PLF.Package,
      onlySerializableDataDefs: Boolean,
  ): Either[Error, Package] = attempt(NameOf.qualifiedNameOfCurrentFunc) {

    val internedStrings = lfPackage.getInternedStringsList.asScala.to(ImmArraySeq)

    val internedDottedNames =
      decodeInternedDottedNames(
        lfPackage.getInternedDottedNamesList.asScala,
        internedStrings,
      )

    val dependencyTracker = new PackageDependencyTracker(packageId)

    val metadata: Option[PackageMetadata] =
      if (lfPackage.hasMetadata) {
        assertSince(Features.packageMetadata, "Package.metadata")
        Some(decodePackageMetadata(lfPackage.getMetadata, internedStrings))
      } else {
        if (!versionIsOlderThan(Features.packageMetadata)) {
          throw Error.Parsing(s"Package.metadata is required in Daml-LF 1.$minor")
        }
        None
      }

    val env0 = Env(
      packageId,
      internedStrings,
      internedDottedNames,
      IndexedSeq.empty,
      Some(dependencyTracker),
      None,
      onlySerializableDataDefs,
    )

    val internedTypes = Work.run(decodeInternedTypes(env0, lfPackage))
    val env = env0.copy(internedTypes = internedTypes)

    Package.build(
      modules = lfPackage.getModulesList.asScala.map(env.decodeModule(_)),
      directDeps = dependencyTracker.getDependencies,
      languageVersion = languageVersion,
      metadata = metadata,
    )

  }

  private[archive] def decodePackageMetadata(
      metadata: PLF.PackageMetadata,
      internedStrings: ImmArraySeq[String],
  ): PackageMetadata = {
    def getInternedStr(id: Int) =
      internedStrings.lift(id).getOrElse {
        throw Error.Parsing(s"invalid internedString table index $id")
      }
    def getInternedPackageId(id: Int): PackageId =
      eitherToParseError(PackageId.fromString(getInternedStr(id)))
    PackageMetadata(
      toPackageName(getInternedStr(metadata.getNameInternedStr), "PackageMetadata.name"),
      toPackageVersion(getInternedStr(metadata.getVersionInternedStr), "PackageMetadata.version22"),
      if (metadata.hasUpgradedPackageId) {
        assertSince(Features.packageUpgrades, "Package.metadata.upgradedPackageId")
        Some(
          getInternedPackageId(metadata.getUpgradedPackageId.getUpgradedPackageIdInternedStr)
        )
      } else None,
    )
  }

  // each LF scenario module is wrapped in a distinct proto package
  type ProtoScenarioModule = PLF.Package

  def decodeScenarioModule( // entry point
      packageId: PackageId,
      lfScenarioModule: ProtoScenarioModule,
  ): Either[Error, Module] = attempt(NameOf.qualifiedNameOfCurrentFunc) {
    val internedStrings =
      lfScenarioModule.getInternedStringsList.asScala.to(ImmArraySeq)
    val internedDottedNames =
      decodeInternedDottedNames(
        lfScenarioModule.getInternedDottedNamesList.asScala,
        internedStrings,
      )

    if (lfScenarioModule.getModulesCount != 1)
      throw Error.Parsing(
        s"expected exactly one module in proto package, found ${lfScenarioModule.getModulesCount} modules"
      )

    val env0 = new Env(
      packageId,
      internedStrings,
      internedDottedNames,
      IndexedSeq.empty,
      None,
      None,
      onlySerializableDataDefs = false,
    )
    val internedTypes = Work.run(decodeInternedTypes(env0, lfScenarioModule))
    val env = env0.copy(internedTypes = internedTypes)
    env.decodeModule(lfScenarioModule.getModules(0))

  }

  private[this] def decodeInternedDottedNames(
      internedList: collection.Seq[PLF.InternedDottedName],
      internedStrings: ImmArraySeq[String],
  ): ImmArraySeq[DottedName] = {

    if (internedList.nonEmpty)
      assertSince(Features.internedDottedNames, "interned dotted names table")

    internedList.view
      .map(idn =>
        decodeSegments(
          idn.getSegmentsInternedStrList.asScala
            .map(id =>
              internedStrings
                .lift(id)
                .getOrElse(throw Error.Parsing(s"invalid string table index $id"))
            )
        )
      )
      .to(ImmArraySeq)
  }

  private[this] def decodeSegments(segments: collection.Seq[String]): DottedName =
    DottedName.fromSegments(segments) match {
      case Left(err) => throw Error.Parsing(err)
      case Right(x) => x
    }

  private[archive] def decodeInternedTypesForTest( // test entry point
      env: Env,
      lfPackage: PLF.Package,
  ): IndexedSeq[Type] = {
    Work.run(decodeInternedTypes(env, lfPackage))
  }

  private def decodeInternedTypes(
      env: Env,
      lfPackage: PLF.Package,
  ): Work[IndexedSeq[Type]] = Ret {
    val lfTypes = lfPackage.getInternedTypesList
    if (!lfTypes.isEmpty)
      assertSince(Features.internedTypes, "interned types table")
    lfTypes.iterator.asScala
      .foldLeft(new mutable.ArrayBuffer[Type](lfTypes.size)) { (buf, typ) =>
        buf += env.copy(internedTypes = buf).uncheckedDecodeTypeForTest(typ)
      }
      .toIndexedSeq
  }
  private[archive] class PackageDependencyTracker(self: PackageId) {
    private val deps = mutable.Set.empty[PackageId]
    def markDependency(pkgId: PackageId): Unit =
      if (pkgId != self)
        discard(deps += pkgId)
    def getDependencies: Set[PackageId] = deps.toSet
  }

  private[archive] case class Env(
      packageId: PackageId,
      internedStrings: ImmArraySeq[String],
      internedDottedNames: ImmArraySeq[DottedName],
      internedTypes: collection.IndexedSeq[Type],
      optDependencyTracker: Option[PackageDependencyTracker],
      optModuleName: Option[ModuleName],
      onlySerializableDataDefs: Boolean,
  ) {

    // decode*ForTest -- test entry points

    private[archive] def decodeChoiceForTest(
        tpl: DottedName,
        lfChoice: PLF.TemplateChoice,
    ): TemplateChoice = {
      Work.run(decodeChoice(tpl, lfChoice))
    }

    private[archive] def decodeDefInterfaceForTest(
        id: DottedName,
        lfInterface: PLF.DefInterface,
    ): DefInterface = {
      Work.run(decodeDefInterface(id, lfInterface))
    }

    private[archive] def decodeKindForTest(lfKind: PLF.Kind): Kind = {
      Work.run(decodeKind(lfKind))
    }

    private[archive] def decodeTypeForTest(lfType: PLF.Type): Type = {
      Work.run(decodeType(lfType)(Ret(_)))
    }

    private[archive] def uncheckedDecodeTypeForTest(lfType: PLF.Type): Type = {
      Work.run(uncheckedDecodeType(lfType))
    }

    private[archive] def decodeExprForTest(lfExpr: PLF.Expr, definition: String): Expr = {
      Work.run(decodeExpr(lfExpr, definition)(Ret(_)))
    }

    private var currentDefinitionRef: Option[DefinitionRef] = None

    def decodeModule(lfModule: PLF.Module): Module = {
      val moduleName = handleDottedName(
        lfModule.getNameCase,
        PLF.Module.NameCase.NAME_DNAME,
        lfModule.getNameDname,
        PLF.Module.NameCase.NAME_INTERNED_DNAME,
        lfModule.getNameInternedDname,
        "Module.name.name",
      )
      copy(optModuleName = Some(moduleName)).decodeModuleWithName(lfModule, moduleName)
    }

    private def decodeModuleWithName(lfModule: PLF.Module, moduleName: ModuleName): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()
      val exceptions = mutable.ArrayBuffer[(DottedName, DefException)]()
      val interfaces = mutable.ArrayBuffer[(DottedName, DefInterface)]()

      if (versionIsOlderThan(Features.typeSynonyms)) {
        assertEmpty(lfModule.getSynonymsList, "Module.synonyms")
      } else if (!onlySerializableDataDefs) {
        // collect type synonyms
        lfModule.getSynonymsList.asScala
          .foreach { defn =>
            val defName = handleDottedName(
              defn.getNameCase,
              PLF.DefTypeSyn.NameCase.NAME_DNAME,
              defn.getNameDname,
              PLF.DefTypeSyn.NameCase.NAME_INTERNED_DNAME,
              defn.getNameInternedDname,
              "DefTypeSyn.name.name",
            )
            currentDefinitionRef =
              Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
            val d = Work.run(decodeDefTypeSyn(defn))
            defs += (defName -> d)
          }
      }

      // collect data types
      lfModule.getDataTypesList.asScala
        .filter(!onlySerializableDataDefs || _.getSerializable)
        .foreach { defn =>
          val defName = handleDottedName(
            defn.getNameCase,
            PLF.DefDataType.NameCase.NAME_DNAME,
            defn.getNameDname,
            PLF.DefDataType.NameCase.NAME_INTERNED_DNAME,
            defn.getNameInternedDname,
            "DefDataType.name.name",
          )
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = Work.run(decodeDefDataType(defn))
          defs += (defName -> d)
        }

      if (!onlySerializableDataDefs) {
        // collect values
        lfModule.getValuesList.asScala.foreach { defn =>
          val nameWithType = defn.getNameWithType
          val defName = handleDottedName(
            nameWithType.getNameDnameList.asScala,
            nameWithType.getNameInternedDname,
            "NameWithType.name",
          )

          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = Work.run(decodeDefValue(defn))
          defs += (defName -> d)
        }
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName = handleDottedName(
          defn.getTyconCase,
          PLF.DefTemplate.TyconCase.TYCON_DNAME,
          defn.getTyconDname,
          PLF.DefTemplate.TyconCase.TYCON_INTERNED_DNAME,
          defn.getTyconInternedDname,
          "DefTemplate.tycon.tycon",
        )
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += ((defName, Work.run(decodeTemplate(defName, defn))))
      }

      if (versionIsOlderThan(Features.exceptions)) {
        assertEmpty(lfModule.getExceptionsList, "Module.exceptions")
      } else if (!onlySerializableDataDefs) {
        lfModule.getExceptionsList.asScala
          .foreach { defn =>
            val defName = getInternedDottedName(defn.getNameInternedDname)
            exceptions += (defName -> Work.run(decodeException(defName, defn)))
          }
      }

      if (versionIsOlderThan(Features.basicInterfaces)) {
        assertEmpty(lfModule.getInterfacesList, "Module.interfaces")
      } else {
        lfModule.getInterfacesList.asScala.foreach { defn =>
          val defName = getInternedDottedName(defn.getTyconInternedDname)
          interfaces += (defName -> Work.run(decodeDefInterface(defName, defn)))
        }
      }

      Module.build(
        moduleName,
        defs,
        templates,
        exceptions,
        interfaces,
        decodeFeatureFlags(lfModule.getFlags),
      )
    }

    private[this] def getInternedStr(id: Int) =
      internedStrings.lift(id).getOrElse {
        throw Error.Parsing(s"invalid internedString table index $id")
      }

    private[this] def getInternedPackageId(id: Int): PackageId =
      eitherToParseError(PackageId.fromString(getInternedStr(id)))

    private[this] def getInternedName(id: Int, description: => String): Name = {
      assertSince(Features.internedStrings, description)
      eitherToParseError(Name.fromString(getInternedStr(id)))
    }

    private[this] def getInternedDottedName(id: Int) =
      internedDottedNames.lift(id).getOrElse {
        throw Error.Parsing(s"invalid dotted name table index $id")
      }

    private[this] def handleDottedName(
        segments: collection.Seq[String],
        interned_id: Int,
        description: => String,
    ): DottedName =
      if (versionIsOlderThan(Features.internedDottedNames)) {
        assertUndefined(interned_id, s"${description}_interned_id")
        decodeSegments(segments)
      } else {
        assertUndefined(segments, description)
        getInternedDottedName(interned_id)
      }

    private[this] def handleDottedName[Case](
        actualCase: Case,
        dNameCase: Case,
        dName: => PLF.DottedName,
        internedDNameCase: Case,
        internedDName: => Int,
        description: => String,
    ): DottedName =
      if (versionIsOlderThan(Features.internedDottedNames)) {
        if (actualCase != dNameCase)
          throw Error.Parsing(s"${description}_dname is required by Daml-LF 1.$minor")
        decodeSegments(dName.getSegmentsList.asScala)
      } else {
        if (actualCase != internedDNameCase)
          throw Error.Parsing(s"${description}_interned_dname is required by Daml-LF 1.$minor")
        getInternedDottedName(internedDName)
      }

    private[archive] def decodeFeatureFlags(flags: PLF.FeatureFlags): FeatureFlags = {
      // NOTE(JM, #157): We disallow loading packages with these flags because they impact the Ledger API in
      // ways that would currently make it quite complicated to support them.
      if (
        !flags.getDontDivulgeContractIdsInCreateArguments || !flags.getDontDiscloseNonConsumingChoicesToObservers || !flags.getForbidPartyLiterals
      ) {
        throw Error.Parsing(
          "Deprecated feature flag settings detected, refusing to parse package"
        )
      }
      FeatureFlags.default
    }

    private[this] def decodeDefDataType(lfDataType: PLF.DefDataType): Work[DDataType] = {
      val params = lfDataType.getParamsList.asScala
      Work.sequence(params.view.map(decodeTypeVarWithKind(_))) { binders =>
        Work.bind(lfDataType.getDataConsCase match {
          case PLF.DefDataType.DataConsCase.RECORD =>
            Work.bind(decodeFields(lfDataType.getRecord.getFieldsList.asScala)) { fields =>
              Ret(DataRecord(fields))
            }
          case PLF.DefDataType.DataConsCase.VARIANT =>
            Work.bind(decodeFields(lfDataType.getVariant.getFieldsList.asScala)) { fields =>
              Ret(DataVariant(fields))
            }
          case PLF.DefDataType.DataConsCase.ENUM =>
            assertEmpty(params, "params")
            Ret(DataEnum(decodeEnumCon(lfDataType.getEnum)))
          case PLF.DefDataType.DataConsCase.DATACONS_NOT_SET =>
            throw Error.Parsing("DefDataType.DATACONS_NOT_SET")
          case PLF.DefDataType.DataConsCase.INTERFACE =>
            Ret(DataInterface)

        }) { dataCons =>
          Ret(DDataType(lfDataType.getSerializable, binders.to(ImmArray), dataCons))
        }
      }
    }

    private[this] def decodeDefTypeSyn(lfTypeSyn: PLF.DefTypeSyn): Work[DTypeSyn] =
      decodeType(lfTypeSyn.getType) { expr =>
        val params = lfTypeSyn.getParamsList.asScala
        Work.sequence(params.view.map(decodeTypeVarWithKind)) { binders =>
          Ret(DTypeSyn(binders.to(ImmArray), expr))
        }
      }

    private[this] def handleInternedName[Case](
        actualCase: Case,
        stringCase: Case,
        string: => String,
        internedStringCase: Case,
        internedString: => Int,
        description: => String,
    ) = {
      val str = if (versionIsOlderThan(Features.internedStrings)) {
        if (actualCase != stringCase)
          throw Error.Parsing(s"${description}_str is required by Daml-LF 1.$minor")
        string
      } else {
        if (actualCase != internedStringCase)
          throw Error.Parsing(s"${description}_interned_str is required by Daml-LF 1.$minor")
        internedStrings(internedString)
      }
      toName(str)
    }

    private[this] def handleInternedNames(
        strings: util.List[String],
        stringIds: util.List[Integer],
        description: => String,
    ): ImmArray[Name] =
      if (versionIsOlderThan(Features.internedStrings)) {
        assertEmpty(stringIds, description + "_interned_string")
        strings.asScala.view.map(toName).to(ImmArray)
      } else {
        assertEmpty(strings, description)
        stringIds.asScala.view.map(id => toName(internedStrings(id))).to(ImmArray)
      }

    private[this] def decodeFieldName(lfFieldWithType: PLF.FieldWithType): Name =
      handleInternedName(
        lfFieldWithType.getFieldCase,
        PLF.FieldWithType.FieldCase.FIELD_STR,
        lfFieldWithType.getFieldStr,
        PLF.FieldWithType.FieldCase.FIELD_INTERNED_STR,
        lfFieldWithType.getFieldInternedStr,
        "FieldWithType.field.field",
      )

    private[this] def decodeFields(
        lfFields: collection.Seq[PLF.FieldWithType]
    ): Work[ImmArray[(Name, Type)]] = {
      Work.sequence(lfFields.view.map { lfFieldWithType =>
        decodeType(lfFieldWithType.getType) { typ =>
          Ret(decodeFieldName(lfFieldWithType) -> typ)
        }
      }) { xs =>
        Ret(xs.to(ImmArray))
      }
    }

    private[this] def decodeEnumCon(
        enumCon: PLF.DefDataType.EnumConstructors
    ): ImmArray[EnumConName] =
      handleInternedNames(
        enumCon.getConstructorsStrList,
        enumCon.getConstructorsInternedStrList,
        "EnumConstructors.constructors",
      )

    private[archive] def decodeDefValueForTest(lfValue: PLF.DefValue): DValue = {
      Work.run(decodeDefValue(lfValue))
    }

    private def decodeDefValue(lfValue: PLF.DefValue): Work[DValue] = {
      if (!lfValue.getNoPartyLiterals) {
        throw Error.Parsing("DefValue must have no_party_literals set to true")
      }
      val name = handleDottedName(
        lfValue.getNameWithType.getNameDnameList.asScala,
        lfValue.getNameWithType.getNameInternedDname,
        "DefValue.NameWithType.name",
      )
      decodeType(lfValue.getNameWithType.getType) { typ =>
        decodeExpr(lfValue.getExpr, name.toString) { body =>
          Ret(
            DValue(
              typ,
              body,
              isTest = lfValue.getIsTest,
            )
          )
        }
      }
    }

    private[this] def decodeTemplateKey(
        tpl: DottedName,
        key: PLF.DefTemplate.DefKey,
        tplVar: ExprVarName,
    ): Work[TemplateKey] = {
      Work.bind(decodeTemplateKeyExpr(tpl, key, tplVar)) { keyExpr =>
        decodeType(key.getType) { typ =>
          decodeExpr(key.getMaintainers, s"${tpl}:maintainer") { maintainers =>
            Ret(
              TemplateKey(
                typ,
                keyExpr,
                maintainers,
              )
            )
          }
        }
      }
    }

    private[this] def decodeTemplate(tpl: DottedName, lfTempl: PLF.DefTemplate): Work[Template] = {
      val lfImplements = lfTempl.getImplementsList.asScala
      if (versionIsOlderThan(Features.basicInterfaces))
        assertEmpty(lfImplements, "DefTemplate.implements")
      val paramName = handleInternedName(
        lfTempl.getParamCase,
        PLF.DefTemplate.ParamCase.PARAM_STR,
        lfTempl.getParamStr,
        PLF.DefTemplate.ParamCase.PARAM_INTERNED_STR,
        lfTempl.getParamInternedStr,
        "DefTemplate.param.param",
      )
      Work.bind(
        if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond, s"$tpl:ensure")(Ret(_))
        else Ret(ETrue)
      ) { precond =>
        decodeExpr(lfTempl.getSignatories, s"$tpl.signatory") { signatories =>
          decodeExpr(lfTempl.getAgreement, s"$tpl:agreement") { agreementText =>
            Work.sequence(lfTempl.getChoicesList.asScala.view.map(decodeChoice(tpl, _))) {
              choices =>
                decodeExpr(lfTempl.getObservers, s"$tpl:observer") { observers =>
                  Work.sequence(lfImplements.view.map(decodeTemplateImplements(_))) { implements =>
                    Work.bind(
                      if (lfTempl.hasKey) {
                        Work.bind(decodeTemplateKey(tpl, lfTempl.getKey, paramName)) { tk =>
                          Ret(Some(tk))
                        }
                      } else Ret(None)
                    ) { key =>
                      Ret(
                        Template.build(
                          param = paramName,
                          precond = precond,
                          signatories = signatories,
                          agreementText = agreementText,
                          choices = choices,
                          observers = observers,
                          implements = implements,
                          key = key,
                        )
                      )
                    }
                  }
                }
            }
          }
        }
      }
    }

    private[this] def decodeTemplateImplements(
        lfImpl: PLF.DefTemplate.Implements
    ): Work[TemplateImplements] = {
      Work.bind(decodeInterfaceInstanceBody(lfImpl.getBody)) { body =>
        Ret(
          TemplateImplements.build(
            interfaceId = decodeTypeConName(lfImpl.getInterface),
            body,
          )
        )
      }
    }

    private[this] def decodeInterfaceInstanceBody(
        lfBody: PLF.InterfaceInstanceBody
    ): Work[InterfaceInstanceBody] = {
      decodeExpr(lfBody.getView, "InterfaceInstanceBody.view") { view =>
        Work.sequence(lfBody.getMethodsList.asScala.view.map(decodeInterfaceInstanceMethod(_))) {
          methods =>
            Ret(InterfaceInstanceBody.build(methods, view))
        }
      }
    }

    private[this] def decodeInterfaceInstanceMethod(
        lfMethod: PLF.InterfaceInstanceBody.InterfaceInstanceMethod
    ): Work[InterfaceInstanceMethod] = {
      decodeExpr(lfMethod.getValue, "InterfaceInstanceMethod.value") { value =>
        Ret(
          InterfaceInstanceMethod(
            methodName =
              getInternedName(lfMethod.getMethodInternedName, "InterfaceInstanceMethod.name"),
            value,
          )
        )
      }
    }

    private def decodeChoice(
        tpl: DottedName,
        lfChoice: PLF.TemplateChoice,
    ): Work[TemplateChoice] = {
      Work.bind(decodeBinder(lfChoice.getArgBinder)) { case (v, t) =>
        val chName = handleInternedName(
          lfChoice.getNameCase,
          PLF.TemplateChoice.NameCase.NAME_STR,
          lfChoice.getNameStr,
          PLF.TemplateChoice.NameCase.NAME_INTERNED_STR,
          lfChoice.getNameInternedStr,
          "TemplateChoice.name.name",
        )
        val selfBinder = handleInternedName(
          lfChoice.getSelfBinderCase,
          PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_STR,
          lfChoice.getSelfBinderStr,
          PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_INTERNED_STR,
          lfChoice.getSelfBinderInternedStr,
          "TemplateChoice.self_binder.self_binder",
        )
        decodeExpr(lfChoice.getControllers, s"$tpl:$chName:controller") { controllers =>
          Work.bind(
            if (lfChoice.hasObservers) {
              assertSince(Features.choiceObservers, "TemplateChoice.observers")
              decodeExpr(lfChoice.getObservers, s"$tpl:$chName:observers") { observers =>
                Ret(Some(observers))
              }
            } else {
              assertUntil(Features.choiceObservers, "missing TemplateChoice.observers")
              Ret(None)
            }
          ) { choiceObservers =>
            Work.bind(
              if (lfChoice.hasAuthorizers) {
                assertSince(Features.choiceAuthority, "TemplateChoice.authorizers")
                decodeExpr(lfChoice.getAuthorizers, s"$tpl:$chName:authorizers") { authorizers =>
                  Ret(Some(authorizers))
                }
              } else {
                // authorizers are optional post Features.choiceAuthority
                Ret(None)
              }
            ) { choiceAuthorizers =>
              decodeType(lfChoice.getRetType) { returnType =>
                decodeExpr(lfChoice.getUpdate, s"$tpl:$chName:choice") { update =>
                  Ret(
                    TemplateChoice(
                      name = chName,
                      consuming = lfChoice.getConsuming,
                      controllers,
                      choiceObservers,
                      choiceAuthorizers,
                      selfBinder = selfBinder,
                      argBinder = v -> t,
                      returnType,
                      update,
                    )
                  )
                }
              }
            }
          }
        }
      }
    }

    private def decodeException(
        exceptionName: DottedName,
        lfException: PLF.DefException,
    ): Work[DefException] =
      decodeExpr(lfException.getMessage, s"$exceptionName:message") { expr =>
        Ret(DefException(expr))
      }

    private def decodeDefInterface(
        id: DottedName,
        lfInterface: PLF.DefInterface,
    ): Work[DefInterface] = {
      Work.sequence(lfInterface.getMethodsList.asScala.view.map(decodeInterfaceMethod(_))) {
        methods =>
          Work.sequence(lfInterface.getChoicesList.asScala.view.map(decodeChoice(id, _))) {
            choices =>
              Work.sequence(
                lfInterface.getCoImplementsList.asScala.view.map(decodeInterfaceCoImplements(_))
              ) { coImplements =>
                decodeType(lfInterface.getView) { view =>
                  Ret(
                    DefInterface.build(
                      requires =
                        if (lfInterface.getRequiresCount != 0) {
                          assertSince(Features.basicInterfaces, "DefInterface.requires")
                          lfInterface.getRequiresList.asScala.view.map(decodeTypeConName)
                        } else
                          List.empty,
                      param =
                        getInternedName(lfInterface.getParamInternedStr, "DefInterface.param"),
                      choices = choices,
                      methods = methods,
                      view = view,
                      coImplements = coImplements,
                    )
                  )
                }
              }
          }
      }
    }

    private[this] def decodeInterfaceMethod(
        lfMethod: PLF.InterfaceMethod
    ): Work[InterfaceMethod] = {
      decodeType(lfMethod.getType) { returnType =>
        Ret(
          InterfaceMethod(
            name = getInternedName(lfMethod.getMethodInternedName, "InterfaceMethod.name"),
            returnType,
          )
        )
      }
    }

    private[this] def decodeInterfaceCoImplements(
        lfCoImpl: PLF.DefInterface.CoImplements
    ): Work[InterfaceCoImplements] = {
      Work.bind(decodeInterfaceInstanceBody(lfCoImpl.getBody)) { body =>
        Ret(
          InterfaceCoImplements.build(
            templateId = decodeTypeConName(lfCoImpl.getTemplate),
            body,
          )
        )
      }
    }

    private def decodeKind(lfKind: PLF.Kind): Work[Kind] = {
      Work.Delay { () =>
        lfKind.getSumCase match {
          case PLF.Kind.SumCase.STAR => Ret(KStar)
          case PLF.Kind.SumCase.NAT =>
            assertSince(Features.numeric, "Kind.NAT")
            Ret(KNat)
          case PLF.Kind.SumCase.ARROW =>
            val kArrow = lfKind.getArrow
            val params = kArrow.getParamsList.asScala
            assertNonEmpty(params, "params")
            Work.bind(decodeKind(kArrow.getResult)) { base =>
              Work.sequence(params.view.map(decodeKind)) { kinds =>
                Ret((kinds foldRight base)(KArrow))
              }
            }
          case PLF.Kind.SumCase.SUM_NOT_SET =>
            throw Error.Parsing("Kind.SUM_NOT_SET")
        }
      }
    }

    private def decodeType[T](lfType: PLF.Type)(k: Type => Work[T]): Work[T] = {
      Work.Bind(
        Work.Delay { () =>
          if (versionIsOlderThan(Features.internedTypes)) {
            uncheckedDecodeType(lfType)
          } else {
            lfType.getSumCase match {
              case PLF.Type.SumCase.INTERNED =>
                Ret(
                  internedTypes.applyOrElse(
                    lfType.getInterned,
                    (index: Int) => throw Error.Parsing(s"invalid internedTypes table index $index"),
                  )
                )
              case otherwise =>
                throw Error.Parsing(s"$otherwise is not supported outside type interning table")
            }
          }
        },
        k,
      )
    }

    private def uncheckedDecodeType(lfType: PLF.Type): Work[Type] = {
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          val varName = handleInternedName(
            tvar.getVarCase,
            PLF.Type.Var.VarCase.VAR_STR,
            tvar.getVarStr,
            PLF.Type.Var.VarCase.VAR_INTERNED_STR,
            tvar.getVarInternedStr,
            "Type.var.var",
          )
          Work.sequence(tvar.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(types.foldLeft[Type](TVar(varName))(TApp))
          }
        case PLF.Type.SumCase.NAT =>
          assertSince(Features.numeric, "Type.NAT")
          Ret(
            Numeric.Scale
              .fromLong(lfType.getNat)
              .fold[TNat](
                _ =>
                  throw Error.Parsing(
                    s"TNat must be between ${Numeric.Scale.MinValue} and ${Numeric.Scale.MaxValue}, found ${lfType.getNat}"
                  ),
                TNat(_),
              )
          )
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          Work.sequence(tcon.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(types.foldLeft[Type](TTyCon(decodeTypeConName(tcon.getTycon)))(TApp))
          }
        case PLF.Type.SumCase.SYN =>
          val tsyn = lfType.getSyn
          Work.sequence(tsyn.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(TSynApp(decodeTypeSynName(tsyn.getTysyn), types.to(ImmArray)))
          }
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val baseType =
            if (prim.getPrim == PLF.PrimType.DECIMAL) {
              assertUntil(Features.numeric, "PrimType.DECIMAL")
              TDecimal
            } else {
              val info = builtinTypeInfoMap(prim.getPrim)
              assertSince(info.minVersion, prim.getPrim.getValueDescriptor.getFullName)
              info.typ
            }
          Work.sequence(prim.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(types.foldLeft(baseType)(TApp))
          }
        case PLF.Type.SumCase.FORALL =>
          val tForall = lfType.getForall
          val vars = tForall.getVarsList.asScala
          assertNonEmpty(vars, "vars")
          Work.bind(uncheckedDecodeType(tForall.getBody)) { base =>
            Work.sequence(vars.view.map(decodeTypeVarWithKind)) { binders =>
              Ret((binders foldRight base)(TForall))
            }
          }
        case PLF.Type.SumCase.STRUCT =>
          val struct = lfType.getStruct
          val fields = struct.getFieldsList.asScala
          assertNonEmpty(fields, "fields")
          Work.sequence(fields.view.map { lfFieldWithType =>
            Work.bind(uncheckedDecodeType(lfFieldWithType.getType)) { typ =>
              Ret(decodeFieldName(lfFieldWithType) -> typ)
            }
          }) { elems =>
            Ret(
              TStruct(
                Struct
                  .fromSeq(elems)
                  .fold(name => throw Error.Parsing(s"TStruct: duplicate field $name"), identity)
              )
            )
          }
        case PLF.Type.SumCase.INTERNED =>
          Ret(
            internedTypes.applyOrElse(
              lfType.getInterned,
              (index: Int) => throw Error.Parsing(s"invalid internedTypes table index $index"),
            )
          )
        case PLF.Type.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("Type.SUM_NOT_SET")
      }
    }

    private[this] def decodeModuleRef(lfRef: PLF.ModuleRef): (PackageId, ModuleName) = {
      val modName = handleDottedName(
        lfRef.getModuleNameCase,
        PLF.ModuleRef.ModuleNameCase.MODULE_NAME_DNAME,
        lfRef.getModuleNameDname,
        PLF.ModuleRef.ModuleNameCase.MODULE_NAME_INTERNED_DNAME,
        lfRef.getModuleNameInternedDname,
        "ModuleRef.module_name.module_name",
      )
      import PLF.PackageRef.{SumCase => SC}

      val pkgId = lfRef.getPackageRef.getSumCase match {
        case SC.SELF =>
          this.packageId
        case SC.PACKAGE_ID_STR =>
          toPackageId(lfRef.getPackageRef.getPackageIdStr, "PackageRef.packageId")
        case SC.PACKAGE_ID_INTERNED_STR =>
          getInternedPackageId(lfRef.getPackageRef.getPackageIdInternedStr)
        case SC.SUM_NOT_SET =>
          throw Error.Parsing("PackageRef.SUM_NOT_SET")
      }
      optDependencyTracker.foreach(_.markDependency(pkgId))
      (pkgId, modName)
    }

    private[this] def decodeTypeConName(lfTyConName: PLF.TypeConName): TypeConName = {
      val (packageId, module) = decodeModuleRef(lfTyConName.getModule)
      val name = handleDottedName(
        lfTyConName.getNameCase,
        PLF.TypeConName.NameCase.NAME_DNAME,
        lfTyConName.getNameDname,
        PLF.TypeConName.NameCase.NAME_INTERNED_DNAME,
        lfTyConName.getNameInternedDname,
        "TypeConName.name.name",
      )
      Identifier(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeSynName(lfTySynName: PLF.TypeSynName): TypeSynName = {
      val (packageId, module) = decodeModuleRef(lfTySynName.getModule)
      val name = handleDottedName(
        lfTySynName.getNameCase,
        PLF.TypeSynName.NameCase.NAME_DNAME,
        lfTySynName.getNameDname,
        PLF.TypeSynName.NameCase.NAME_INTERNED_DNAME,
        lfTySynName.getNameInternedDname,
        "TypeSynName.name.name",
      )
      Identifier(packageId, QualifiedName(module, name))
    }

    private def decodeExpr[T](lfExpr: PLF.Expr, definition: String)(k: Expr => Work[T]): Work[T] = {
      Work.Bind(Work.Delay(() => decodeExpr1(lfExpr, definition)), k)
    }

    private[this] def decodeTypeVarWithKind(
        lfTypeVarWithKind: PLF.TypeVarWithKind
    ): Work[(TypeVarName, Kind)] = {
      val name =
        handleInternedName(
          lfTypeVarWithKind.getVarCase,
          PLF.TypeVarWithKind.VarCase.VAR_STR,
          lfTypeVarWithKind.getVarStr,
          PLF.TypeVarWithKind.VarCase.VAR_INTERNED_STR,
          lfTypeVarWithKind.getVarInternedStr,
          "TypeVarWithKind.var.var",
        )
      Work.bind(decodeKind(lfTypeVarWithKind.getKind)) { kind =>
        Ret { name -> kind }
      }
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): Work[(ExprVarName, Type)] = {
      decodeType(lfBinder.getType) { typ =>
        Ret(
          handleInternedName(
            lfBinder.getVarCase,
            PLF.VarWithType.VarCase.VAR_STR,
            lfBinder.getVarStr,
            PLF.VarWithType.VarCase.VAR_INTERNED_STR,
            lfBinder.getVarInternedStr,
            "VarWithType.var.var",
          ) -> typ
        )
      }
    }

    /* Begin Decoding Expression */

    private[this] def handleInternedName(
        internedString: => Int
    ): Name =
      toName(internedStrings(internedString))

    private def decodeExpr1(lfExpr: PLF.Expr, definition: String): Work[Expr] = {
      Work.bind(lfExpr.getSumCase match {
        case PLF.Expr.SumCase.VAR_STR =>
          assertUntil(Features.internedStrings, "Expr.var_str")
          Ret(EVar(toName(lfExpr.getVarStr)))

        case PLF.Expr.SumCase.VAR_INTERNED_STR =>
          Ret(EVar(getInternedName(lfExpr.getVarInternedStr, "Expr.var_interned_id")))

        case PLF.Expr.SumCase.VAL =>
          Ret(EVal(decodeValName(lfExpr.getVal)))

        case PLF.Expr.SumCase.PRIM_LIT =>
          Ret(EPrimLit(decodePrimLit(lfExpr.getPrimLit)))

        case PLF.Expr.SumCase.PRIM_CON =>
          Ret(lfExpr.getPrimCon match {
            case PLF.PrimCon.CON_UNIT => EUnit
            case PLF.PrimCon.CON_FALSE => EFalse
            case PLF.PrimCon.CON_TRUE => ETrue
            case PLF.PrimCon.UNRECOGNIZED =>
              throw Error.Parsing("PrimCon.UNRECOGNIZED")
          })

        case PLF.Expr.SumCase.BUILTIN =>
          val info = builtinInfoMap(lfExpr.getBuiltin)
          assertSince(info.minVersion, lfExpr.getBuiltin.getValueDescriptor.getFullName)
          info.maxVersion.foreach(assertUntil(_, lfExpr.getBuiltin.getValueDescriptor.getFullName))
          Ret(info.expr)

        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          Work.bind(decodeTypeConApp(recCon.getTycon)) { tycon =>
            Work.sequence(
              recCon.getFieldsList.asScala.view.map(x => decodeFieldWithExpr(x, definition))
            ) { fields =>
              Ret(ERecCon(tycon, fields = fields.to(ImmArray)))
            }
          }

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          Work.bind(decodeTypeConApp(recProj.getTycon)) { tycon =>
            val field = handleInternedName(
              recProj.getFieldCase,
              PLF.Expr.RecProj.FieldCase.FIELD_STR,
              recProj.getFieldStr,
              PLF.Expr.RecProj.FieldCase.FIELD_INTERNED_STR,
              recProj.getFieldInternedStr,
              "Expr.RecProj.field.field",
            )
            decodeExpr(recProj.getRecord, definition) { record =>
              Ret(ERecProj(tycon, field, record))
            }
          }

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          Work.bind(decodeTypeConApp(recUpd.getTycon)) { tycon =>
            val field = handleInternedName(
              recUpd.getFieldCase,
              PLF.Expr.RecUpd.FieldCase.FIELD_STR,
              recUpd.getFieldStr,
              PLF.Expr.RecUpd.FieldCase.FIELD_INTERNED_STR,
              recUpd.getFieldInternedStr,
              "Expr.RecUpd.field.field",
            )
            decodeExpr(recUpd.getRecord, definition) { record =>
              decodeExpr(recUpd.getUpdate, definition) { update =>
                Ret(ERecUpd(tycon, field, record, update))
              }
            }
          }

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          Work.bind(decodeTypeConApp(varCon.getTycon)) { tycon =>
            val name = handleInternedName(
              varCon.getVariantConCase,
              PLF.Expr.VariantCon.VariantConCase.VARIANT_CON_STR,
              varCon.getVariantConStr,
              PLF.Expr.VariantCon.VariantConCase.VARIANT_CON_INTERNED_STR,
              varCon.getVariantConInternedStr,
              "Expr.VariantCon.variant_con.variant_con",
            )
            decodeExpr(varCon.getVariantArg, definition) { expr =>
              Ret(EVariantCon(tycon, name, expr))
            }
          }

        case PLF.Expr.SumCase.ENUM_CON =>
          val enumCon = lfExpr.getEnumCon
          Ret(
            EEnumCon(
              decodeTypeConName(enumCon.getTycon),
              handleInternedName(
                enumCon.getEnumConCase,
                PLF.Expr.EnumCon.EnumConCase.ENUM_CON_STR,
                enumCon.getEnumConStr,
                PLF.Expr.EnumCon.EnumConCase.ENUM_CON_INTERNED_STR,
                enumCon.getEnumConInternedStr,
                "Expr.EnumCon.enum_con.enum_con",
              ),
            )
          )

        case PLF.Expr.SumCase.STRUCT_CON =>
          val structCon = lfExpr.getStructCon
          Work.sequence(
            structCon.getFieldsList.asScala.view.map(x => decodeFieldWithExpr(x, definition))
          ) { xs => Ret(EStructCon(xs.to(ImmArray))) }

        case PLF.Expr.SumCase.STRUCT_PROJ =>
          val structProj = lfExpr.getStructProj
          val field = handleInternedName(
            structProj.getFieldCase,
            PLF.Expr.StructProj.FieldCase.FIELD_STR,
            structProj.getFieldStr,
            PLF.Expr.StructProj.FieldCase.FIELD_INTERNED_STR,
            structProj.getFieldInternedStr,
            "Expr.StructProj.field.field",
          )
          decodeExpr(structProj.getStruct, definition) { struct =>
            Ret(EStructProj(field, struct))
          }

        case PLF.Expr.SumCase.STRUCT_UPD =>
          val structUpd = lfExpr.getStructUpd
          val field = handleInternedName(
            structUpd.getFieldCase,
            PLF.Expr.StructUpd.FieldCase.FIELD_STR,
            structUpd.getFieldStr,
            PLF.Expr.StructUpd.FieldCase.FIELD_INTERNED_STR,
            structUpd.getFieldInternedStr,
            "Expr.StructUpd.field.field",
          )
          decodeExpr(structUpd.getStruct, definition) { struct =>
            decodeExpr(structUpd.getUpdate, definition) { update =>
              Ret(EStructUpd(field, struct, update))
            }
          }

        case PLF.Expr.SumCase.APP =>
          val app = lfExpr.getApp
          val args = app.getArgsList.asScala
          assertNonEmpty(args, "args")
          decodeExpr(app.getFun, definition) { base =>
            Work.sequence(args.view.map(decodeExpr(_, definition)(Ret(_)))) { args =>
              Ret((args foldLeft base)(EApp))
            }
          }

        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          decodeExpr(lfAbs.getBody, definition) { base =>
            Work.sequence(params.view.map(decodeBinder)) { binders =>
              Ret((binders foldRight base)((binder, e) => EAbs(binder, e, currentDefinitionRef)))
            }
          }

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          assertNonEmpty(args, "args")
          decodeExpr(tyapp.getExpr, definition) { base =>
            Work.sequence(args.view.map(decodeType(_)(Ret(_)))) { types =>
              Ret((types foldLeft base)(ETyApp))
            }
          }

        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          decodeExpr(lfTyAbs.getBody, definition) { base =>
            Work.sequence(params.view.map(decodeTypeVarWithKind)) { binders =>
              Ret((binders foldRight base)(ETyAbs))
            }
          }

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val bindings = lfLet.getBindingsList.asScala
          assertNonEmpty(bindings, "bindings")
          decodeExpr(lfLet.getBody, definition) { base =>
            Work.sequence(bindings.view.map { binding =>
              Work.bind(decodeBinder(binding.getBinder)) { case (v, t) =>
                decodeExpr(binding.getBound, definition) { bound =>
                  Ret((v, t, bound))
                }
              }
            }) { bindings =>
              Ret((bindings foldRight base) { case ((v, t, bound), e) =>
                ELet(Binding(Some(v), t, bound), e)
              })
            }
          }

        case PLF.Expr.SumCase.NIL =>
          decodeType(lfExpr.getNil.getType) { typ =>
            Ret(ENil(typ))
          }

        case PLF.Expr.SumCase.CONS =>
          val cons = lfExpr.getCons
          val front = cons.getFrontList.asScala
          assertNonEmpty(front, "front")
          decodeType(cons.getType) { typ =>
            Work.sequence(front.view.map(decodeExpr(_, definition)(Ret(_)))) { elems =>
              decodeExpr(cons.getTail, definition) { tail =>
                Ret(ECons(typ, elems.to(ImmArray), tail))
              }
            }
          }

        case PLF.Expr.SumCase.CASE =>
          val case_ = lfExpr.getCase
          decodeExpr(case_.getScrut, definition) { scrut =>
            Work.sequence(case_.getAltsList.asScala.view.map(decodeCaseAlt(_, definition))) {
              alts =>
                Ret(ECase(scrut, alts.to(ImmArray)))
            }
          }

        case PLF.Expr.SumCase.UPDATE =>
          Work.bind(decodeUpdate(lfExpr.getUpdate, definition)) { update =>
            Ret(EUpdate(update))
          }

        case PLF.Expr.SumCase.SCENARIO =>
          Work.bind(decodeScenario(lfExpr.getScenario, definition)) { scenario =>
            Ret(EScenario(scenario))
          }

        case PLF.Expr.SumCase.OPTIONAL_NONE =>
          decodeType(lfExpr.getOptionalNone.getType) { typ =>
            Ret(ENone(typ))
          }

        case PLF.Expr.SumCase.OPTIONAL_SOME =>
          val some = lfExpr.getOptionalSome
          decodeType(some.getType) { typ =>
            decodeExpr(some.getBody, definition) { expr =>
              Ret(ESome(typ, expr))
            }
          }

        case PLF.Expr.SumCase.TO_ANY =>
          assertSince(Features.anyType, "Expr.ToAny")
          decodeType(lfExpr.getToAny.getType) { typ =>
            decodeExpr(lfExpr.getToAny.getExpr, definition) { expr =>
              Ret(EToAny(typ, expr))
            }
          }

        case PLF.Expr.SumCase.FROM_ANY =>
          assertSince(Features.anyType, "Expr.FromAny")
          decodeType(lfExpr.getFromAny.getType) { typ =>
            decodeExpr(lfExpr.getFromAny.getExpr, definition) { expr =>
              Ret(EFromAny(typ, expr))
            }
          }

        case PLF.Expr.SumCase.TYPE_REP =>
          assertSince(Features.typeRep, "Expr.type_rep")
          decodeType(lfExpr.getTypeRep) { typ =>
            Ret(ETypeRep(typ))
          }

        case PLF.Expr.SumCase.THROW =>
          assertSince(Features.exceptions, "Expr.from_any_exception")
          val eThrow = lfExpr.getThrow
          decodeType(eThrow.getReturnType) { returnType =>
            decodeType(eThrow.getExceptionType) { exceptionType =>
              decodeExpr(eThrow.getExceptionExpr, definition) { exception =>
                Ret(EThrow(returnType, exceptionType, exception))
              }
            }
          }

        case PLF.Expr.SumCase.TO_ANY_EXCEPTION =>
          assertSince(Features.exceptions, "Expr.to_any_exception")
          val toAnyException = lfExpr.getToAnyException
          decodeType(toAnyException.getType) { typ =>
            decodeExpr(toAnyException.getExpr, definition) { value =>
              Ret(EToAnyException(typ, value))
            }
          }

        case PLF.Expr.SumCase.FROM_ANY_EXCEPTION =>
          assertSince(Features.exceptions, "Expr.from_any_exception")
          val fromAnyException = lfExpr.getFromAnyException
          decodeType(fromAnyException.getType) { typ =>
            decodeExpr(fromAnyException.getExpr, definition) { value =>
              Ret(EFromAnyException(typ, value))
            }
          }

        case PLF.Expr.SumCase.TO_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.to_interface")
          val toInterface = lfExpr.getToInterface
          val interfaceId = decodeTypeConName(toInterface.getInterfaceType)
          val templateId = decodeTypeConName(toInterface.getTemplateType)
          decodeExpr(toInterface.getTemplateExpr, definition) { value =>
            Ret(EToInterface(interfaceId, templateId, value))
          }

        case PLF.Expr.SumCase.FROM_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.from_interface")
          val fromInterface = lfExpr.getFromInterface
          val interfaceId = decodeTypeConName(fromInterface.getInterfaceType)
          val templateId = decodeTypeConName(fromInterface.getTemplateType)
          decodeExpr(fromInterface.getInterfaceExpr, definition) { value =>
            Ret(EFromInterface(interfaceId, templateId, value))
          }

        case PLF.Expr.SumCase.CALL_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.call_interface")
          val callInterface = lfExpr.getCallInterface
          val interfaceId = decodeTypeConName(callInterface.getInterfaceType)
          val methodName =
            getInternedName(callInterface.getMethodInternedName, "ECallInterface.method")
          decodeExpr(callInterface.getInterfaceExpr, definition) { value =>
            Ret(ECallInterface(interfaceId, methodName, value))
          }

        case PLF.Expr.SumCase.SIGNATORY_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.signatory_interface")
          val signatoryInterface = lfExpr.getSignatoryInterface
          val ifaceId = decodeTypeConName(signatoryInterface.getInterface)
          decodeExpr(signatoryInterface.getExpr, definition) { body =>
            Ret(ESignatoryInterface(ifaceId, body))
          }

        case PLF.Expr.SumCase.OBSERVER_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.observer_interface")
          val observerInterface = lfExpr.getObserverInterface
          decodeExpr(observerInterface.getExpr, definition) { body =>
            Ret(
              EObserverInterface(ifaceId = decodeTypeConName(observerInterface.getInterface), body)
            )
          }

        case PLF.Expr.SumCase.UNSAFE_FROM_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.unsafe_from_interface")
          val unsafeFromInterface = lfExpr.getUnsafeFromInterface
          val interfaceId = decodeTypeConName(unsafeFromInterface.getInterfaceType)
          val templateId = decodeTypeConName(unsafeFromInterface.getTemplateType)
          decodeExpr(unsafeFromInterface.getContractIdExpr, definition) { contractIdExpr =>
            decodeExpr(unsafeFromInterface.getInterfaceExpr, definition) { ifaceExpr =>
              Ret(EUnsafeFromInterface(interfaceId, templateId, contractIdExpr, ifaceExpr))
            }
          }

        case PLF.Expr.SumCase.TO_REQUIRED_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.to_required_interface")
          val toRequiredInterface = lfExpr.getToRequiredInterface
          val requiredIfaceId = decodeTypeConName(toRequiredInterface.getRequiredInterface)
          val requiringIfaceId = decodeTypeConName(toRequiredInterface.getRequiringInterface)
          decodeExpr(toRequiredInterface.getExpr, definition) { body =>
            Ret(EToRequiredInterface(requiredIfaceId, requiringIfaceId, body))
          }

        case PLF.Expr.SumCase.FROM_REQUIRED_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.from_required_interface")
          val fromRequiredInterface = lfExpr.getFromRequiredInterface
          decodeExpr(fromRequiredInterface.getExpr, definition) { body =>
            Ret(
              EFromRequiredInterface(
                requiredIfaceId = decodeTypeConName(fromRequiredInterface.getRequiredInterface),
                requiringIfaceId = decodeTypeConName(fromRequiredInterface.getRequiringInterface),
                body,
              )
            )
          }

        case PLF.Expr.SumCase.UNSAFE_FROM_REQUIRED_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.from_required_interface")
          val unsafeFromRequiredInterface = lfExpr.getUnsafeFromRequiredInterface
          val requiredIfaceId = decodeTypeConName(unsafeFromRequiredInterface.getRequiredInterface)
          val requiringIfaceId =
            decodeTypeConName(unsafeFromRequiredInterface.getRequiringInterface)
          decodeExpr(unsafeFromRequiredInterface.getContractIdExpr, definition) { contractIdExpr =>
            decodeExpr(unsafeFromRequiredInterface.getInterfaceExpr, definition) { ifaceExpr =>
              Ret(
                EUnsafeFromRequiredInterface(
                  requiredIfaceId,
                  requiringIfaceId,
                  contractIdExpr,
                  ifaceExpr,
                )
              )
            }
          }

        case PLF.Expr.SumCase.INTERFACE_TEMPLATE_TYPE_REP =>
          assertSince(Features.basicInterfaces, "Expr.interface_template_type_rep")
          val interfaceTemplateTypeRep = lfExpr.getInterfaceTemplateTypeRep
          val ifaceId = decodeTypeConName(interfaceTemplateTypeRep.getInterface)
          decodeExpr(interfaceTemplateTypeRep.getExpr, definition) { body =>
            Ret(EInterfaceTemplateTypeRep(ifaceId, body))
          }

        case PLF.Expr.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("Expr.SUM_NOT_SET")

        case PLF.Expr.SumCase.VIEW_INTERFACE =>
          assertSince(Features.basicInterfaces, "Expr.view_interface")
          val viewInterface = lfExpr.getViewInterface
          val ifaceId = decodeTypeConName(viewInterface.getInterface)
          decodeExpr(viewInterface.getExpr, definition) { expr =>
            Ret(EViewInterface(ifaceId, expr))
          }

        case PLF.Expr.SumCase.CHOICE_CONTROLLER =>
          assertSince(Features.choiceFuncs, "Expr.choice_controller")
          val choiceController = lfExpr.getChoiceController
          val tplCon = decodeTypeConName(choiceController.getTemplate)
          val choiceName = handleInternedName(choiceController.getChoiceInternedStr)
          decodeExpr(choiceController.getContractExpr, definition) { contractExpr =>
            decodeExpr(choiceController.getChoiceArgExpr, definition) { choiceArgExpr =>
              Ret(EChoiceController(tplCon, choiceName, contractExpr, choiceArgExpr))
            }
          }

        case PLF.Expr.SumCase.CHOICE_OBSERVER =>
          assertSince(Features.choiceFuncs, "Expr.choice_observer")
          val choiceObserver = lfExpr.getChoiceObserver
          val tplCon = decodeTypeConName(choiceObserver.getTemplate)
          val choiceName = handleInternedName(choiceObserver.getChoiceInternedStr)
          decodeExpr(choiceObserver.getContractExpr, definition) { contractExpr =>
            decodeExpr(choiceObserver.getChoiceArgExpr, definition) { choiceArgExpr =>
              Ret(EChoiceObserver(tplCon, choiceName, contractExpr, choiceArgExpr))
            }
          }

        case PLF.Expr.SumCase.EXPERIMENTAL =>
          assertSince(Features.unstable, "Expr.experimental")
          val experimental = lfExpr.getExperimental
          decodeType(experimental.getType) { typ =>
            Ret(EExperimental(experimental.getName, typ))
          }

      }) { expr =>
        decodeLocation(lfExpr, definition) match {
          case None => Ret(expr)
          case Some(loc) => Ret(ELocation(loc, expr))
        }
      }
    }

    private[this] def decodeCaseAlt(lfCaseAlt: PLF.CaseAlt, definition: String): Work[CaseAlt] = {
      val pat: CasePat = lfCaseAlt.getSumCase match {
        case PLF.CaseAlt.SumCase.DEFAULT =>
          CPDefault
        case PLF.CaseAlt.SumCase.VARIANT =>
          val variant = lfCaseAlt.getVariant
          CPVariant(
            tycon = decodeTypeConName(variant.getCon),
            variant = handleInternedName(
              variant.getVariantCase,
              PLF.CaseAlt.Variant.VariantCase.VARIANT_STR,
              variant.getVariantStr,
              PLF.CaseAlt.Variant.VariantCase.VARIANT_INTERNED_STR,
              variant.getVariantInternedStr,
              "CaseAlt.Variant.variant.variant",
            ),
            binder = handleInternedName(
              variant.getBinderCase,
              PLF.CaseAlt.Variant.BinderCase.BINDER_STR,
              variant.getBinderStr,
              PLF.CaseAlt.Variant.BinderCase.BINDER_INTERNED_STR,
              variant.getBinderInternedStr,
              "CaseAlt.Variant.binder.binder",
            ),
          )
        case PLF.CaseAlt.SumCase.ENUM =>
          val enumeration = lfCaseAlt.getEnum
          CPEnum(
            tycon = decodeTypeConName(enumeration.getCon),
            constructor = handleInternedName(
              enumeration.getConstructorCase,
              PLF.CaseAlt.Enum.ConstructorCase.CONSTRUCTOR_STR,
              enumeration.getConstructorStr,
              PLF.CaseAlt.Enum.ConstructorCase.CONSTRUCTOR_INTERNED_STR,
              enumeration.getConstructorInternedStr,
              "CaseAlt.Enum.constructor.constructor",
            ),
          )
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          decodePrimCon(lfCaseAlt.getPrimCon)
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(
            head = handleInternedName(
              cons.getVarHeadCase,
              PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_STR,
              cons.getVarHeadStr,
              PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_INTERNED_STR,
              cons.getVarHeadInternedStr,
              "CaseAlt.Cons.var_head.var_head",
            ),
            tail = handleInternedName(
              cons.getVarTailCase,
              PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_STR,
              cons.getVarTailStr,
              PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_INTERNED_STR,
              cons.getVarTailInternedStr,
              "CaseAlt.Cons.var_tail.var_tail",
            ),
          )

        case PLF.CaseAlt.SumCase.OPTIONAL_NONE =>
          CPNone

        case PLF.CaseAlt.SumCase.OPTIONAL_SOME =>
          val some = lfCaseAlt.getOptionalSome
          CPSome(
            handleInternedName(
              some.getVarBodyCase,
              PLF.CaseAlt.OptionalSome.VarBodyCase.VAR_BODY_STR,
              some.getVarBodyStr,
              PLF.CaseAlt.OptionalSome.VarBodyCase.VAR_BODY_INTERNED_STR,
              some.getVarBodyInternedStr,
              "CaseAlt.OptionalSom.var_body.var_body",
            )
          )

        case PLF.CaseAlt.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("CaseAlt.SUM_NOT_SET")
      }
      decodeExpr(lfCaseAlt.getBody, definition) { rhs =>
        Ret(CaseAlt(pat, rhs))
      }
    }

    private[this] def decodeRetrieveByKey(
        value: PLF.Update.RetrieveByKey,
        definition: String,
    ): Work[RetrieveByKey] = {
      decodeExpr(value.getKey, definition) { keyE =>
        Ret(RetrieveByKey(decodeTypeConName(value.getTemplate), keyE))
      }
    }

    private[this] def decodeUpdate(lfUpdate: PLF.Update, definition: String): Work[Update] = {
      lfUpdate.getSumCase match {

        case PLF.Update.SumCase.PURE =>
          val pure = lfUpdate.getPure
          decodeType(pure.getType) { typ =>
            decodeExpr(pure.getExpr, definition) { expr =>
              Ret(UpdatePure(typ, expr))
            }
          }

        case PLF.Update.SumCase.BLOCK =>
          val block = lfUpdate.getBlock
          decodeExpr(block.getBody, definition) { body =>
            Work.sequence(
              block.getBindingsList.asScala.view.map(x => decodeBinding(x, definition))
            ) { bindings =>
              Ret(UpdateBlock(bindings = bindings.to(ImmArray), body))
            }
          }

        case PLF.Update.SumCase.CREATE =>
          val create = lfUpdate.getCreate
          decodeExpr(create.getExpr, definition) { arg =>
            Ret(UpdateCreate(templateId = decodeTypeConName(create.getTemplate), arg))
          }

        case PLF.Update.SumCase.CREATE_INTERFACE =>
          val create = lfUpdate.getCreateInterface
          decodeExpr(create.getExpr, definition) { arg =>
            Ret(UpdateCreateInterface(interfaceId = decodeTypeConName(create.getInterface), arg))
          }

        case PLF.Update.SumCase.EXERCISE =>
          val exercise = lfUpdate.getExercise
          val templateId = decodeTypeConName(exercise.getTemplate)
          val choice = handleInternedName(
            exercise.getChoiceCase,
            PLF.Update.Exercise.ChoiceCase.CHOICE_STR,
            exercise.getChoiceStr,
            PLF.Update.Exercise.ChoiceCase.CHOICE_INTERNED_STR,
            exercise.getChoiceInternedStr,
            "Update.Exercise.choice.choice",
          )
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(UpdateExercise(templateId, choice, cidE, argE))
            }
          }

        case PLF.Update.SumCase.SOFT_EXERCISE =>
          assertSince(Features.packageUpgrades, "softExercise")
          val exercise = lfUpdate.getSoftExercise
          val templateId = decodeTypeConName(exercise.getTemplate)
          val choice = handleInternedName(
            exercise.getChoiceCase,
            PLF.Update.SoftExercise.ChoiceCase.CHOICE_STR,
            exercise.getChoiceStr,
            PLF.Update.SoftExercise.ChoiceCase.CHOICE_INTERNED_STR,
            exercise.getChoiceInternedStr,
            "Update.SoftExercise.choice.choice",
          )
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(
                UpdateSoftExercise(templateId, choice, cidE, argE)
              )
            }
          }

        case PLF.Update.SumCase.DYNAMIC_EXERCISE =>
          val exercise = lfUpdate.getDynamicExercise
          val templateId = decodeTypeConName(exercise.getTemplate)
          val choice = handleInternedName(exercise.getChoiceInternedStr)
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(UpdateDynamicExercise(templateId, choice, cidE, argE))
            }
          }

        case PLF.Update.SumCase.EXERCISE_INTERFACE =>
          assertSince(Features.basicInterfaces, "exerciseInterface")
          val exercise = lfUpdate.getExerciseInterface
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Work.bind(
                if (exercise.hasGuard) {
                  assertSince(Features.extendedInterfaces, "exerciseInterface.guard")
                  decodeExpr(exercise.getGuard, definition) { e =>
                    Ret(Some(e))
                  }
                } else
                  Ret(None)
              ) { guardE =>
                val interfaceId = decodeTypeConName(exercise.getInterface)
                val choice = handleInternedName(exercise.getChoiceInternedStr)
                Ret(UpdateExerciseInterface(interfaceId, choice, cidE, argE, guardE))
              }
            }
          }

        case PLF.Update.SumCase.EXERCISE_BY_KEY =>
          assertSince(Features.exerciseByKey, "exerciseByKey")
          val exerciseByKey = lfUpdate.getExerciseByKey
          val templateId = decodeTypeConName(exerciseByKey.getTemplate)
          val choice = getInternedName(
            exerciseByKey.getChoiceInternedStr,
            "Update.ExerciseByKey.choice.choice",
          )
          decodeExpr(exerciseByKey.getKey, definition) { keyE =>
            decodeExpr(exerciseByKey.getArg, definition) { argE =>
              Ret(UpdateExerciseByKey(templateId, choice, keyE, argE))
            }
          }

        case PLF.Update.SumCase.GET_TIME =>
          Ret(UpdateGetTime)

        case PLF.Update.SumCase.FETCH =>
          val fetch = lfUpdate.getFetch
          decodeExpr(fetch.getCid, definition) { contractId =>
            Ret(UpdateFetchTemplate(templateId = decodeTypeConName(fetch.getTemplate), contractId))
          }

        case PLF.Update.SumCase.SOFT_FETCH =>
          assertSince(Features.packageUpgrades, "softFetch")
          val softFetch = lfUpdate.getSoftFetch
          decodeExpr(softFetch.getCid, definition) { contractId =>
            Ret(
              UpdateSoftFetchTemplate(
                templateId = decodeTypeConName(softFetch.getTemplate),
                contractId,
              )
            )
          }

        case PLF.Update.SumCase.FETCH_INTERFACE =>
          assertSince(Features.basicInterfaces, "fetchInterface")
          val fetch = lfUpdate.getFetchInterface
          decodeExpr(fetch.getCid, definition) { contractId =>
            Ret(
              UpdateFetchInterface(interfaceId = decodeTypeConName(fetch.getInterface), contractId)
            )
          }

        case PLF.Update.SumCase.FETCH_BY_KEY =>
          Work.bind(decodeRetrieveByKey(lfUpdate.getFetchByKey, definition)) { rbk =>
            Ret(UpdateFetchByKey(rbk))
          }

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          Work.bind(decodeRetrieveByKey(lfUpdate.getLookupByKey, definition)) { rbk =>
            Ret(UpdateLookupByKey(rbk))
          }

        case PLF.Update.SumCase.EMBED_EXPR =>
          val embedExpr = lfUpdate.getEmbedExpr
          decodeType(embedExpr.getType) { typ =>
            decodeExpr(embedExpr.getBody, definition) { expr =>
              Ret(UpdateEmbedExpr(typ, expr))
            }
          }

        case PLF.Update.SumCase.TRY_CATCH =>
          assertSince(Features.exceptions, "Update.try_catch")
          val tryCatch = lfUpdate.getTryCatch
          decodeType(tryCatch.getReturnType) { typ =>
            decodeExpr(tryCatch.getTryExpr, definition) { body =>
              val binder = toName(internedStrings(tryCatch.getVarInternedStr))
              decodeExpr(tryCatch.getCatchExpr, definition) { handler =>
                Ret(UpdateTryCatch(typ, body, binder, handler))
              }
            }
          }

        case PLF.Update.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("Update.SUM_NOT_SET")
      }
    }

    private[this] def decodeScenario(
        lfScenario: PLF.Scenario,
        definition: String,
    ): Work[Scenario] = {
      lfScenario.getSumCase match {
        case PLF.Scenario.SumCase.PURE =>
          val pure = lfScenario.getPure
          decodeType(pure.getType) { typ =>
            decodeExpr(pure.getExpr, definition) { expr =>
              Ret(ScenarioPure(typ, expr))
            }
          }

        case PLF.Scenario.SumCase.COMMIT =>
          val commit = lfScenario.getCommit
          decodeExpr(commit.getParty, definition) { party =>
            decodeExpr(commit.getExpr, definition) { expr =>
              decodeType(commit.getRetType) { typ =>
                Ret(ScenarioCommit(party, expr, typ))
              }
            }
          }

        case PLF.Scenario.SumCase.MUSTFAILAT =>
          val commit = lfScenario.getMustFailAt
          decodeExpr(commit.getParty, definition) { party =>
            decodeExpr(commit.getExpr, definition) { expr =>
              decodeType(commit.getRetType) { typ =>
                Ret(ScenarioMustFailAt(party, expr, typ))
              }
            }
          }

        case PLF.Scenario.SumCase.BLOCK =>
          val block = lfScenario.getBlock
          decodeExpr(block.getBody, definition) { body =>
            Work.sequence(
              block.getBindingsList.asScala.view.map(x => decodeBinding(x, definition))
            ) { bindings =>
              Ret(ScenarioBlock(bindings = bindings.to(ImmArray), body))
            }
          }

        case PLF.Scenario.SumCase.GET_TIME =>
          Ret(ScenarioGetTime)

        case PLF.Scenario.SumCase.PASS =>
          decodeExpr(lfScenario.getPass, definition) { pass =>
            Ret(ScenarioPass(pass))
          }

        case PLF.Scenario.SumCase.GET_PARTY =>
          decodeExpr(lfScenario.getGetParty, definition) { party =>
            Ret(ScenarioGetParty(party))
          }

        case PLF.Scenario.SumCase.EMBED_EXPR =>
          val embedExpr = lfScenario.getEmbedExpr
          decodeType(embedExpr.getType) { typ =>
            decodeExpr(embedExpr.getBody, definition) { expr =>
              Ret(ScenarioEmbedExpr(typ, expr))
            }
          }

        case PLF.Scenario.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("Scenario.SUM_NOT_SET")
      }
    }

    private[this] def decodeBinding(lfBinding: PLF.Binding, definition: String): Work[Binding] = {
      Work.bind(decodeBinder(lfBinding.getBinder)) { case (binder, typ) =>
        decodeExpr(lfBinding.getBound, definition) { expr =>
          Ret(Binding(Some(binder), typ, expr))
        }
      }
    }

    private[this] def decodePrimCon(lfPrimCon: PLF.PrimCon): CPPrimCon =
      lfPrimCon match {
        case PLF.PrimCon.CON_UNIT =>
          CPUnit
        case PLF.PrimCon.CON_FALSE =>
          CPFalse
        case PLF.PrimCon.CON_TRUE =>
          CPTrue
        case _ => throw Error.Parsing("Unknown PrimCon: " + lfPrimCon.toString)
      }

    private[this] def decodePrimLit(lfPrimLit: PLF.PrimLit): PrimLit =
      lfPrimLit.getSumCase match {
        case PLF.PrimLit.SumCase.INT64 =>
          PLInt64(lfPrimLit.getInt64)
        case PLF.PrimLit.SumCase.DECIMAL_STR =>
          assertUntil(Features.numeric, "PrimLit.decimal")
          assertUntil(Features.internedStrings, "PrimLit.decimal_str")
          toPLDecimal(lfPrimLit.getDecimalStr)
        case PLF.PrimLit.SumCase.TEXT_STR =>
          assertUntil(Features.internedStrings, "PrimLit.text_str")
          PLText(lfPrimLit.getTextStr)
        case PLF.PrimLit.SumCase.TIMESTAMP =>
          val t = Time.Timestamp.fromLong(lfPrimLit.getTimestamp)
          t.fold(e => throw Error.Parsing("error decoding timestamp: " + e), PLTimestamp)
        case PLF.PrimLit.SumCase.DATE =>
          val d = Time.Date.fromDaysSinceEpoch(lfPrimLit.getDate)
          d.fold(e => throw Error.Parsing("error decoding date: " + e), PLDate)
        case PLF.PrimLit.SumCase.TEXT_INTERNED_STR =>
          assertSince(Features.internedStrings, "PrimLit.text_interned_str")
          PLText(getInternedStr(lfPrimLit.getTextInternedStr))
        case PLF.PrimLit.SumCase.NUMERIC_INTERNED_STR =>
          assertSince(Features.numeric, "PrimLit.numeric")
          toPLNumeric(getInternedStr(lfPrimLit.getNumericInternedStr))
        case PLF.PrimLit.SumCase.ROUNDING_MODE =>
          assertSince(Features.bigNumeric, "Expr.rounding_mode")
          PLRoundingMode(java.math.RoundingMode.valueOf(lfPrimLit.getRoundingModeValue))
        case PLF.PrimLit.SumCase.PARTY_STR | PLF.PrimLit.SumCase.PARTY_INTERNED_STR =>
          throw Error.Parsing("Party literals are not supported")
        case PLF.PrimLit.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("PrimLit.SUM_NOT_SET")
      }

    private[this] def decodeFieldWithExpr(
        lfFieldWithExpr: PLF.FieldWithExpr,
        definition: String,
    ): Work[(Name, Expr)] =
      decodeExpr(lfFieldWithExpr.getExpr, definition) { expr =>
        Ret(
          handleInternedName(
            lfFieldWithExpr.getFieldCase,
            PLF.FieldWithExpr.FieldCase.FIELD_STR,
            lfFieldWithExpr.getFieldStr,
            PLF.FieldWithExpr.FieldCase.FIELD_INTERNED_STR,
            lfFieldWithExpr.getFieldInternedStr,
            "FieldWithType.name",
          ) -> expr
        )
      }

    private[this] def decodeValName(lfVal: PLF.ValName): ValueRef = {
      val (packageId, module) = decodeModuleRef(lfVal.getModule)
      val name =
        handleDottedName(lfVal.getNameDnameList.asScala, lfVal.getNameInternedDname, "ValName.name")
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConApp(lfTyConApp: PLF.Type.Con): Work[TypeConApp] = {
      Work.sequence(lfTyConApp.getArgsList.asScala.view.map(decodeType(_)(Ret(_)))) { types =>
        Ret(TypeConApp(decodeTypeConName(lfTyConApp.getTycon), types.to(ImmArray)))
      }
    }

    private def decodeLocation(lfExpr: PLF.Expr, definition: String): Option[Location] =
      if (lfExpr.hasLocation && lfExpr.getLocation.hasRange) {
        val loc = lfExpr.getLocation
        val optModuleRef =
          if (loc.hasModule)
            Some(decodeModuleRef(loc.getModule))
          else
            optModuleName.map((packageId, _))
        optModuleRef.map { case (pkgId, moduleName) =>
          val range = loc.getRange
          Location(
            pkgId,
            moduleName,
            definition,
            (range.getStartLine, range.getStartCol),
            (range.getEndLine, range.getEndCol),
          )
        }
      } else {
        None
      }

    private[this] def decodeKeyExpr(expr: PLF.KeyExpr, tplVar: ExprVarName): Work[Expr] = {
      Work.Delay { () =>
        expr.getSumCase match {

          case PLF.KeyExpr.SumCase.RECORD =>
            val recCon = expr.getRecord
            Work.bind(decodeTypeConApp(recCon.getTycon)) { tycon =>
              Work.sequence(recCon.getFieldsList.asScala.view.map { field =>
                val name =
                  handleInternedName(
                    field.getFieldCase,
                    PLF.KeyExpr.RecordField.FieldCase.FIELD_STR,
                    field.getFieldStr,
                    PLF.KeyExpr.RecordField.FieldCase.FIELD_INTERNED_STR,
                    field.getFieldInternedStr,
                    "KeyExpr.field",
                  )
                Work.bind(decodeKeyExpr(field.getExpr, tplVar)) { expr =>
                  Ret(name -> expr)
                }
              }) { fields => Ret(ERecCon(tycon, fields = fields.to(ImmArray))) }
            }

          case PLF.KeyExpr.SumCase.PROJECTIONS =>
            val lfProjs = expr.getProjections.getProjectionsList.asScala
            Work.sequence(lfProjs.view.map { lfProj =>
              Work.bind(decodeTypeConApp(lfProj.getTycon)) { tycon =>
                val name =
                  handleInternedName(
                    lfProj.getFieldCase,
                    PLF.KeyExpr.Projection.FieldCase.FIELD_STR,
                    lfProj.getFieldStr,
                    PLF.KeyExpr.Projection.FieldCase.FIELD_INTERNED_STR,
                    lfProj.getFieldInternedStr,
                    "KeyExpr.Projection.field",
                  )
                Ret((tycon, name))
              }
            }) { projs =>
              Ret(projs.foldLeft(EVar(tplVar): Expr) { case (acc, (tycon, name)) =>
                ERecProj(tycon, name, acc)
              })
            }

          case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
            throw Error.Parsing("KeyExpr.SUM_NOT_SET")
        }
      }
    }

    private[this] def decodeTemplateKeyExpr(
        tpl: DottedName,
        key: PLF.DefTemplate.DefKey,
        tplVar: ExprVarName,
    ): Work[Expr] = key.getKeyExprCase match {
      case PLF.DefTemplate.DefKey.KeyExprCase.KEY =>
        decodeKeyExpr(key.getKey, tplVar)
      case PLF.DefTemplate.DefKey.KeyExprCase.COMPLEX_KEY => {
        decodeExpr(key.getComplexKey, s"${tpl}:key") { Ret(_) }
      }
      case PLF.DefTemplate.DefKey.KeyExprCase.KEYEXPR_NOT_SET =>
        throw Error.Parsing("DefKey.KEYEXPR_NOT_SET")
    }

    /* End Decoding Expression */

  }

  private def versionIsOlderThan(minVersion: LV): Boolean =
    languageVersion < minVersion

  private def toPackageId(s: String, description: => String): PackageId = {
    assertUntil(Features.internedStrings, description)
    eitherToParseError(PackageId.fromString(s))
  }

  private[this] def toName(s: String): Name =
    eitherToParseError(Name.fromString(s))

  private[this] def toPackageName(s: String, description: => String): PackageName = {
    assertSince(Features.packageMetadata, description)
    eitherToParseError(PackageName.fromString(s))
  }

  private[this] def toPackageVersion(s: String, description: => String): PackageVersion = {
    assertSince(Features.packageMetadata, description)
    eitherToParseError(PackageVersion.fromString(s))
  }

  // maxVersion excluded
  private[this] def assertUntil(maxVersion: LV, description: => String): Unit =
    if (!versionIsOlderThan(maxVersion))
      throw Error.Parsing(s"$description is not supported by Daml-LF 1.$minor")

  // minVersion included
  private[this] def assertSince(minVersion: LV, description: => String): Unit =
    if (versionIsOlderThan(minVersion))
      throw Error.Parsing(s"$description is not supported by Daml-LF 1.$minor")

  private def assertUndefined(i: Int, description: => String): Unit =
    if (i != 0)
      throw Error.Parsing(s"$description is not supported by Daml-LF 1.$minor")

  private def assertUndefined(s: collection.Seq[_], description: => String): Unit =
    if (s.nonEmpty)
      throw Error.Parsing(s"$description is not supported by Daml-LF 1.$minor")

  private def assertNonEmpty(s: collection.Seq[_], description: => String): Unit =
    if (s.isEmpty) throw Error.Parsing(s"Unexpected empty $description")

  private[this] def assertEmpty(s: collection.Seq[_], description: => String): Unit =
    if (s.nonEmpty) throw Error.Parsing(s"Unexpected non-empty $description")

  private[this] def assertEmpty(s: util.List[_], description: => String): Unit =
    if (!s.isEmpty) throw Error.Parsing(s"Unexpected non-empty $description")

}

private[lf] object DecodeCommon {

  private def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw Error.Parsing(err), identity)

  case class BuiltinTypeInfo(
      proto: PLF.PrimType,
      bTyp: BuiltinType,
      minVersion: LV = Features.default,
  ) {
    val typ = TBuiltin(bTyp)
  }

  val builtinTypeInfos: List[BuiltinTypeInfo] = {
    import PLF.PrimType._
    // DECIMAL is not there and should be handled in an ad-hoc way.
    List(
      BuiltinTypeInfo(UNIT, BTUnit),
      BuiltinTypeInfo(BOOL, BTBool),
      BuiltinTypeInfo(TEXT, BTText),
      BuiltinTypeInfo(INT64, BTInt64),
      BuiltinTypeInfo(TIMESTAMP, BTTimestamp),
      BuiltinTypeInfo(PARTY, BTParty),
      BuiltinTypeInfo(LIST, BTList),
      BuiltinTypeInfo(UPDATE, BTUpdate),
      BuiltinTypeInfo(SCENARIO, BTScenario),
      BuiltinTypeInfo(CONTRACT_ID, BTContractId),
      BuiltinTypeInfo(DATE, BTDate),
      BuiltinTypeInfo(OPTIONAL, BTOptional),
      BuiltinTypeInfo(TEXTMAP, BTTextMap),
      BuiltinTypeInfo(GENMAP, BTGenMap, minVersion = Features.genMap),
      BuiltinTypeInfo(ARROW, BTArrow),
      BuiltinTypeInfo(NUMERIC, BTNumeric, minVersion = Features.numeric),
      BuiltinTypeInfo(ANY, BTAny, minVersion = Features.anyType),
      BuiltinTypeInfo(TYPE_REP, BTTypeRep, minVersion = Features.typeRep),
      BuiltinTypeInfo(BIGNUMERIC, BTBigNumeric, minVersion = Features.bigNumeric),
      BuiltinTypeInfo(ROUNDING_MODE, BTRoundingMode, minVersion = Features.bigNumeric),
      BuiltinTypeInfo(ANY_EXCEPTION, BTAnyException, minVersion = Features.exceptions),
    )
  }

  private val builtinTypeInfoMap =
    builtinTypeInfos
      .map(info => info.proto -> info)
      .toMap

  /* Begin Decoding Expression */

  private def toPLNumeric(s: String) =
    PLNumeric(eitherToParseError(Numeric.fromString(s)))

  private def toPLDecimal(s: String) =
    PLNumeric(eitherToParseError(Decimal.fromString(s)))

  case class BuiltinFunctionInfo(
      proto: PLF.BuiltinFunction,
      builtin: BuiltinFunction,
      minVersion: LV = Features.default, // first version that does support the builtin
      maxVersion: Option[LV] = None, // first version that does not support the builtin
      implicitParameters: List[Type] = List.empty,
  ) {
    val expr: Expr = implicitParameters.foldLeft[Expr](EBuiltin(builtin))(ETyApp)
  }

  val builtinFunctionInfos: List[BuiltinFunctionInfo] = {
    import Features._
    import PLF.BuiltinFunction._
    List(
      BuiltinFunctionInfo(
        ADD_DECIMAL,
        BAddNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(
        SUB_DECIMAL,
        BSubNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(
        MUL_DECIMAL,
        BMulNumericLegacy,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal, TNat.Decimal, TNat.Decimal),
      ),
      BuiltinFunctionInfo(
        DIV_DECIMAL,
        BDivNumericLegacy,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal, TNat.Decimal, TNat.Decimal),
      ),
      BuiltinFunctionInfo(
        ROUND_DECIMAL,
        BRoundNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(ADD_NUMERIC, BAddNumeric, minVersion = numeric),
      BuiltinFunctionInfo(SUB_NUMERIC, BSubNumeric, minVersion = numeric),
      BuiltinFunctionInfo(MUL_NUMERIC_LEGACY, BMulNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(MUL_NUMERIC, BMulNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(DIV_NUMERIC_LEGACY, BDivNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(DIV_NUMERIC, BDivNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(ROUND_NUMERIC, BRoundNumeric, minVersion = numeric),
      BuiltinFunctionInfo(CAST_NUMERIC_LEGACY, BCastNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(CAST_NUMERIC, BCastNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(SHIFT_NUMERIC_LEGACY, BShiftNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(SHIFT_NUMERIC, BShiftNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(ADD_INT64, BAddInt64),
      BuiltinFunctionInfo(SUB_INT64, BSubInt64),
      BuiltinFunctionInfo(MUL_INT64, BMulInt64),
      BuiltinFunctionInfo(DIV_INT64, BDivInt64),
      BuiltinFunctionInfo(MOD_INT64, BModInt64),
      BuiltinFunctionInfo(EXP_INT64, BExpInt64),
      BuiltinFunctionInfo(
        INT64_TO_DECIMAL,
        BInt64ToNumericLegacy,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(
        DECIMAL_TO_INT64,
        BNumericToInt64,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(INT64_TO_NUMERIC_LEGACY, BInt64ToNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(INT64_TO_NUMERIC, BInt64ToNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(NUMERIC_TO_INT64, BNumericToInt64, minVersion = numeric),
      BuiltinFunctionInfo(FOLDL, BFoldl),
      BuiltinFunctionInfo(FOLDR, BFoldr),
      BuiltinFunctionInfo(TEXTMAP_EMPTY, BTextMapEmpty),
      BuiltinFunctionInfo(TEXTMAP_INSERT, BTextMapInsert),
      BuiltinFunctionInfo(TEXTMAP_LOOKUP, BTextMapLookup),
      BuiltinFunctionInfo(TEXTMAP_DELETE, BTextMapDelete),
      BuiltinFunctionInfo(TEXTMAP_TO_LIST, BTextMapToList),
      BuiltinFunctionInfo(TEXTMAP_SIZE, BTextMapSize),
      BuiltinFunctionInfo(GENMAP_EMPTY, BGenMapEmpty, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_INSERT, BGenMapInsert, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_LOOKUP, BGenMapLookup, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_DELETE, BGenMapDelete, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_KEYS, BGenMapKeys, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_VALUES, BGenMapValues, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_SIZE, BGenMapSize, minVersion = genMap),
      BuiltinFunctionInfo(APPEND_TEXT, BAppendText),
      BuiltinFunctionInfo(ERROR, BError),
      BuiltinFunctionInfo(
        LEQ_INT64,
        BLessEq,
        implicitParameters = List(TInt64),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LEQ_DECIMAL,
        BLessEq,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal),
      ),
      BuiltinFunctionInfo(
        LEQ_NUMERIC,
        BLessEqNumeric,
        maxVersion = Some(genComparison),
        minVersion = numeric,
      ),
      BuiltinFunctionInfo(
        LEQ_TEXT,
        BLessEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText),
      ),
      BuiltinFunctionInfo(
        LEQ_TIMESTAMP,
        BLessEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp),
      ),
      BuiltinFunctionInfo(
        LEQ_PARTY,
        BLessEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(
        GEQ_INT64,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64),
      ),
      BuiltinFunctionInfo(
        GEQ_DECIMAL,
        BGreaterEq,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal),
      ),
      BuiltinFunctionInfo(
        GEQ_NUMERIC,
        BGreaterEqNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        GEQ_TEXT,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText),
      ),
      BuiltinFunctionInfo(
        GEQ_TIMESTAMP,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp),
      ),
      BuiltinFunctionInfo(
        GEQ_PARTY,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(
        LESS_INT64,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64),
      ),
      BuiltinFunctionInfo(
        LESS_DECIMAL,
        BLess,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal),
      ),
      BuiltinFunctionInfo(
        LESS_NUMERIC,
        BLessNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LESS_TEXT,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText),
      ),
      BuiltinFunctionInfo(
        LESS_TIMESTAMP,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp),
      ),
      BuiltinFunctionInfo(
        LESS_PARTY,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(
        GREATER_INT64,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64),
      ),
      BuiltinFunctionInfo(
        GREATER_DECIMAL,
        BGreater,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal),
      ),
      BuiltinFunctionInfo(
        GREATER_NUMERIC,
        BGreaterNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        GREATER_TEXT,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText),
      ),
      BuiltinFunctionInfo(
        GREATER_TIMESTAMP,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp),
      ),
      BuiltinFunctionInfo(
        GREATER_PARTY,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(INT64_TO_TEXT, BInt64ToText),
      BuiltinFunctionInfo(
        DECIMAL_TO_TEXT,
        BNumericToText,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal),
      ),
      BuiltinFunctionInfo(NUMERIC_TO_TEXT, BNumericToText, minVersion = numeric),
      BuiltinFunctionInfo(TIMESTAMP_TO_TEXT, BTimestampToText),
      BuiltinFunctionInfo(PARTY_TO_TEXT, BPartyToText),
      BuiltinFunctionInfo(TEXT_TO_TEXT, BTextToText),
      BuiltinFunctionInfo(
        CONTRACT_ID_TO_TEXT,
        BContractIdToText,
        minVersion = contractIdTextConversions,
      ),
      BuiltinFunctionInfo(PARTY_TO_QUOTED_TEXT, BPartyToQuotedText, maxVersion = Some(exceptions)),
      BuiltinFunctionInfo(CODE_POINTS_TO_TEXT, BCodePointsToText),
      BuiltinFunctionInfo(TEXT_TO_PARTY, BTextToParty),
      BuiltinFunctionInfo(TEXT_TO_INT64, BTextToInt64),
      BuiltinFunctionInfo(
        TEXT_TO_DECIMAL,
        BTextToNumericLegacy,
        implicitParameters = List(TNat.Decimal),
        maxVersion = Some(numeric),
      ),
      BuiltinFunctionInfo(TEXT_TO_NUMERIC_LEGACY, BTextToNumericLegacy, minVersion = numeric),
      BuiltinFunctionInfo(TEXT_TO_NUMERIC, BTextToNumeric, minVersion = natTypeErasure),
      BuiltinFunctionInfo(TEXT_TO_CODE_POINTS, BTextToCodePoints),
      BuiltinFunctionInfo(SHA256_TEXT, BSHA256Text),
      BuiltinFunctionInfo(DATE_TO_UNIX_DAYS, BDateToUnixDays),
      BuiltinFunctionInfo(EXPLODE_TEXT, BExplodeText),
      BuiltinFunctionInfo(IMPLODE_TEXT, BImplodeText),
      BuiltinFunctionInfo(
        GEQ_DATE,
        BGreaterEq,
        implicitParameters = List(TDate),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LEQ_DATE,
        BLessEq,
        implicitParameters = List(TDate),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LESS_DATE,
        BLess,
        implicitParameters = List(TDate),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(TIMESTAMP_TO_UNIX_MICROSECONDS, BTimestampToUnixMicroseconds),
      BuiltinFunctionInfo(DATE_TO_TEXT, BDateToText),
      BuiltinFunctionInfo(UNIX_DAYS_TO_DATE, BUnixDaysToDate),
      BuiltinFunctionInfo(UNIX_MICROSECONDS_TO_TIMESTAMP, BUnixMicrosecondsToTimestamp),
      BuiltinFunctionInfo(
        GREATER_DATE,
        BGreater,
        implicitParameters = List(TDate),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(EQUAL, BEqual, minVersion = genComparison),
      BuiltinFunctionInfo(LESS, BLess, minVersion = genComparison),
      BuiltinFunctionInfo(LESS_EQ, BLessEq, minVersion = genComparison),
      BuiltinFunctionInfo(GREATER, BGreater, minVersion = genComparison),
      BuiltinFunctionInfo(GREATER_EQ, BGreaterEq, minVersion = genComparison),
      BuiltinFunctionInfo(EQUAL_LIST, BEqualList),
      BuiltinFunctionInfo(
        EQUAL_INT64,
        BEqual,
        implicitParameters = List(TInt64),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        EQUAL_DECIMAL,
        BEqual,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal),
      ),
      BuiltinFunctionInfo(
        EQUAL_NUMERIC,
        BEqualNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        EQUAL_TEXT,
        BEqual,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText),
      ),
      BuiltinFunctionInfo(
        EQUAL_TIMESTAMP,
        BEqual,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp),
      ),
      BuiltinFunctionInfo(
        EQUAL_DATE,
        BEqual,
        maxVersion = Some(genComparison),
        implicitParameters = List(TDate),
      ),
      BuiltinFunctionInfo(
        EQUAL_PARTY,
        BEqual,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(
        EQUAL_BOOL,
        BEqual,
        maxVersion = Some(genComparison),
        implicitParameters = List(TBool),
      ),
      BuiltinFunctionInfo(
        EQUAL_TYPE_REP,
        BEqual,
        minVersion = typeRep,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTypeRep),
      ),
      BuiltinFunctionInfo(EQUAL_CONTRACT_ID, BEqualContractId, maxVersion = Some(genComparison)),
      BuiltinFunctionInfo(TRACE, BTrace),
      BuiltinFunctionInfo(COERCE_CONTRACT_ID, BCoerceContractId),
      BuiltinFunctionInfo(SCALE_BIGNUMERIC, BScaleBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(PRECISION_BIGNUMERIC, BPrecisionBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(ADD_BIGNUMERIC, BAddBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(SUB_BIGNUMERIC, BSubBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(MUL_BIGNUMERIC, BMulBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(DIV_BIGNUMERIC, BDivBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(SHIFT_RIGHT_BIGNUMERIC, BShiftRightBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(
        BIGNUMERIC_TO_NUMERIC_LEGACY,
        BBigNumericToNumericLegacy,
        minVersion = bigNumeric,
      ),
      BuiltinFunctionInfo(
        BIGNUMERIC_TO_NUMERIC,
        BBigNumericToNumeric,
        minVersion = natTypeErasure,
      ),
      BuiltinFunctionInfo(NUMERIC_TO_BIGNUMERIC, BNumericToBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(BIGNUMERIC_TO_TEXT, BBigNumericToText, minVersion = bigNumeric),
      BuiltinFunctionInfo(ANY_EXCEPTION_MESSAGE, BAnyExceptionMessage, minVersion = exceptions),
      BuiltinFunctionInfo(TYPE_REP_TYCON_NAME, BTypeRepTyConName, minVersion = unstable),
    )
  }

  private val builtinInfoMap =
    builtinFunctionInfos
      .map(info => info.proto -> info)
      .toMap
      .withDefault(_ => throw Error.Parsing("BuiltinFunction.UNRECOGNIZED"))

  /* End Decoding Expresion */

}
