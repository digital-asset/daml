// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.util
import com.daml.daml_lf_dev.{DamlLf1 => PLF}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Struct}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.mutable
import scala.jdk.CollectionConverters._

// taken from main-2.x as of April 4th.
// https://github.com/digital-asset/daml/commit/5f6bd1d0fe2696453c537a0a6db7054b5d274f23
// remove part decoding expression.

private[archive] class DecodeV1(minor: LV.Minor) {

  import DecodeV1._
  import Work.Ret

  private val languageVersion = LV(LV.Major.V1, minor)

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
      metadata = metadata.getOrElse(NoPackageMetadata),
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

    @scala.annotation.nowarn("cat=unused")
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

    @scala.annotation.nowarn("cat=unused")
    private[this] def decodeTemplateKey(
        tpl: DottedName,
        key: PLF.DefTemplate.DefKey,
        tplVar: ExprVarName,
    ): Work[TemplateKey] = {
      decodeType(key.getType) { typ =>
        decodeExpr(key.getMaintainers, s"${tpl}:maintainer") { maintainers =>
          Ret(
            TemplateKey(
              typ,
              EUnit,
              maintainers,
            )
          )
        }
      }
    }

    @scala.annotation.nowarn("cat=unused")
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

    @scala.annotation.nowarn("cat=unused")
    private def decodeExpr[T](lfExpr: PLF.Expr, definition: String)(k: Expr => Work[T]): Work[T] = {
      Work.Delay(() => k(EUnit))
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

private[lf] object DecodeV1 {

  object Features {
    import LV._
    val default = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val bigNumeric = v1_13
    val exceptions = v1_14
    val basicInterfaces = v1_15
    val choiceFuncs = v1_dev
    val choiceAuthority = v1_dev
    val natTypeErasure = v1_dev
    val packageUpgrades = v1_dev
    val dynamicExercise = v1_dev
    val sharedKeys = v1_dev

    /** TYPE_REP_TYCON_NAME builtin */
    val templateTypeRepToText = v1_dev

    /** Guards in interfaces */
    val extendedInterfaces = v1_dev

    /** Unstable, experimental features. This should stay in x.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v1_dev

  }

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
    builtinTypeInfos.map(info => info.proto -> info).toMap

}
