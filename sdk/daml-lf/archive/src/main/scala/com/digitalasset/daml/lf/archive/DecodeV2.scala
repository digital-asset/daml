// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import java.util
import com.digitalasset.daml.lf.archive.{DamlLf2 => PLF}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Numeric, Struct, Time}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion => LV}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.math.Ordering.Implicits.infixOrderingOps

/** Decodes LF2 packages and modules. */
private[archive] class DecodeV2(minor: LV.Minor) {

  import DecodeV2._
  import Work.Ret

  private val languageVersion: LV = LV(LanguageMajorVersion.V2, minor)

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

    val metadata: PackageMetadata = {
      if (!lfPackage.hasMetadata)
        throw Error.Parsing(s"Package.metadata is required in Daml-LF 2.$minor")
      decodePackageMetadata(lfPackage.getMetadata, internedStrings)
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

    val modules = lfPackage.getModulesList.asScala.map(env.decodeModule(_))
    Package.build(
      modules = modules,
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
      toPackageName(getInternedStr(metadata.getNameInternedStr)),
      toPackageVersion(getInternedStr(metadata.getVersionInternedStr)),
      if (metadata.hasUpgradedPackageId) {
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
      val moduleName = getInternedDottedName(lfModule.getNameInternedDname)
      copy(optModuleName = Some(moduleName)).decodeModuleWithName(lfModule, moduleName)
    }

    private def decodeModuleWithName(lfModule: PLF.Module, moduleName: ModuleName): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()
      val exceptions = mutable.ArrayBuffer[(DottedName, DefException)]()
      val interfaces = mutable.ArrayBuffer[(DottedName, DefInterface)]()

      if (!onlySerializableDataDefs) {
        // collect type synonyms
        lfModule.getSynonymsList.asScala
          .foreach { defn =>
            val defName = getInternedDottedName(defn.getNameInternedDname)
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
          val defName = getInternedDottedName(defn.getNameInternedDname)
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = Work.run(decodeDefDataType(defn))
          defs += (defName -> d)
        }

      if (!onlySerializableDataDefs) {
        // collect values
        lfModule.getValuesList.asScala.foreach { defn =>
          val nameWithType = defn.getNameWithType
          val defName = getInternedDottedName(nameWithType.getNameInternedDname)

          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = Work.run(decodeDefValue(defn))
          defs += (defName -> d)
        }
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName = getInternedDottedName(defn.getTyconInternedDname)
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += ((defName, Work.run(decodeTemplate(defName, defn))))
      }

      if (versionIsOlderThan(LV.Features.exceptions)) {
        assertEmpty(lfModule.getExceptionsList, "Module.exceptions")
      } else if (!onlySerializableDataDefs) {
        lfModule.getExceptionsList.asScala
          .foreach { defn =>
            val defName = getInternedDottedName(defn.getNameInternedDname)
            exceptions += (defName -> Work.run(decodeException(defName, defn)))
          }
      }

      lfModule.getInterfacesList.asScala.foreach { defn =>
        val defName = getInternedDottedName(defn.getTyconInternedDname)
        interfaces += (defName -> Work.run(decodeDefInterface(defName, defn)))
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

    private def getInternedName(id: Int) = {
      eitherToParseError(Name.fromString(getInternedStr(id)))
    }

    private[this] def getInternedDottedName(id: Int) =
      internedDottedNames.lift(id).getOrElse {
        throw Error.Parsing(s"invalid dotted name table index $id")
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

    private[this] def internedName(internedString: Int): Name =
      toName(internedStrings(internedString))

    private def internedNames(stringIds: util.List[Integer]) =
      stringIds.asScala.view.map(id => toName(internedStrings(id))).to(ImmArray)

    private[this] def decodeFieldName(lfFieldWithType: PLF.FieldWithType): Name =
      internedName(lfFieldWithType.getFieldInternedStr)

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

    private[this] def decodeFieldWithExpr(
        lfFieldWithExpr: PLF.FieldWithExpr,
        definition: String,
    ): Work[(Name, Expr)] =
      decodeExpr(lfFieldWithExpr.getExpr, definition) { expr =>
        Ret(
          internedName(lfFieldWithExpr.getFieldInternedStr) -> expr
        )
      }

    private[this] def decodeEnumCon(
        enumCon: PLF.DefDataType.EnumConstructors
    ): ImmArray[EnumConName] =
      internedNames(enumCon.getConstructorsInternedStrList)

    private[archive] def decodeDefValueForTest(lfValue: PLF.DefValue): DValue = {
      Work.run(decodeDefValue(lfValue))
    }

    private def decodeDefValue(lfValue: PLF.DefValue): Work[DValue] = {
      if (lfValue.getIsTest) {
        assertSince(LV.Features.scenarios, "is_test")
      }
      val name = getInternedDottedName(lfValue.getNameWithType.getNameInternedDname)
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

    private[this] def decodeTemplateKey(
        tpl: DottedName,
        key: PLF.DefTemplate.DefKey,
    ): Work[TemplateKey] = {
      assertSince(LV.Features.contractKeys, "key")
      decodeExpr(key.getKeyExpr, s"${tpl}:key") { keyExpr =>
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
      val paramName = internedName(lfTempl.getParamInternedStr)
      Work.bind(
        if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond, s"$tpl:ensure")(Ret(_))
        else Ret(ETrue)
      ) { precond =>
        decodeExpr(lfTempl.getSignatories, s"$tpl.signatory") { signatories =>
          Work.sequence(lfTempl.getChoicesList.asScala.view.map(decodeChoice(tpl, _))) { choices =>
            decodeExpr(lfTempl.getObservers, s"$tpl:observer") { observers =>
              Work.sequence(lfImplements.view.map(decodeTemplateImplements(_))) { implements =>
                Work.bind(
                  if (lfTempl.hasKey) {
                    assertSince(LV.Features.contractKeys, "key")
                    Work.bind(decodeTemplateKey(tpl, lfTempl.getKey)) { tk =>
                      Ret(Some(tk))
                    }
                  } else Ret(None)
                ) { key =>
                  Ret(
                    Template.build(
                      param = paramName,
                      precond,
                      signatories,
                      choices,
                      observers,
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

    private[this] def decodeTemplateImplements(
        lfImpl: PLF.DefTemplate.Implements
    ): Work[TemplateImplements] = {
      Work.bind(decodeInterfaceInstanceBody(lfImpl.getBody)) { body =>
        Ret(
          TemplateImplements.build(
            interfaceId = decodeTypeConId(lfImpl.getInterface),
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
            methodName = getInternedName(lfMethod.getMethodInternedName),
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
        val chName = internedName(lfChoice.getNameInternedStr)
        val selfBinder = internedName(lfChoice.getSelfBinderInternedStr)
        decodeExpr(lfChoice.getControllers, s"$tpl:$chName:controller") { controllers =>
          Work.bind(
            decodeExpr(lfChoice.getObservers, s"$tpl:$chName:observers") { observers =>
              Ret(Some(observers))
            }
          ) { choiceObservers =>
            Work.bind(
              if (lfChoice.hasAuthorizers) {
                assertSince(LV.Features.choiceAuthority, "TemplateChoice.authorizers")
                decodeExpr(lfChoice.getAuthorizers, s"$tpl:$chName:authorizers") { authorizers =>
                  Ret(Some(authorizers))
                }
              } else {
                // authorizers are optional post LV.Features.choiceAuthority
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
              decodeType(lfInterface.getView) { view =>
                Ret(
                  DefInterface.build(
                    requires =
                      if (lfInterface.getRequiresCount != 0) {
                        lfInterface.getRequiresList.asScala.view.map(decodeTypeConId)
                      } else
                        List.empty,
                    param = getInternedName(lfInterface.getParamInternedStr),
                    choices,
                    methods,
                    view,
                  )
                )
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
            name = getInternedName(lfMethod.getMethodInternedName),
            returnType,
          )
        )
      }
    }

    private def decodeKind(lfKind: PLF.Kind): Work[Kind] = {
      Work.Delay { () =>
        lfKind.getSumCase match {
          case PLF.Kind.SumCase.STAR => Ret(KStar)
          case PLF.Kind.SumCase.NAT =>
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
          {
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
          val varName = internedName(tvar.getVarInternedStr)
          Work.sequence(tvar.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(types.foldLeft[Type](TVar(varName))(TApp))
          }
        case PLF.Type.SumCase.NAT =>
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
            Ret(types.foldLeft[Type](TTyCon(decodeTypeConId(tcon.getTycon)))(TApp))
          }
        case PLF.Type.SumCase.SYN =>
          val tsyn = lfType.getSyn
          Work.sequence(tsyn.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
            Ret(TSynApp(decodeTypeSynId(tsyn.getTysyn), types.to(ImmArray)))
          }
        case PLF.Type.SumCase.BUILTIN =>
          val builtin = lfType.getBuiltin
          val info = builtinTypeInfoMap(builtin.getBuiltin)
          assertSince(info.minVersion, builtin.getBuiltin.getValueDescriptor.getFullName)
          val baseType: Type = info.typ
          Work.sequence(builtin.getArgsList.asScala.view.map(uncheckedDecodeType)) { types =>
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

    private[this] def decodeModuleRef(lfId: PLF.ModuleId): (PackageId, ModuleName) = {
      val modName = getInternedDottedName(lfId.getModuleNameInternedDname)
      import PLF.SelfOrImportedPackageId.{SumCase => SC}

      val pkgId = lfId.getPackageId.getSumCase match {
        case SC.SELF_PACKAGE_ID =>
          this.packageId
        case SC.IMPORTED_PACKAGE_ID_INTERNED_STR =>
          getInternedPackageId(lfId.getPackageId.getImportedPackageIdInternedStr)
        case SC.SUM_NOT_SET =>
          throw Error.Parsing("PackageId.SUM_NOT_SET")
      }
      optDependencyTracker.foreach(_.markDependency(pkgId))
      (pkgId, modName)
    }

    private[this] def decodeValId(lfVal: PLF.ValueId): ValueRef = {
      val (packageId, module) = decodeModuleRef(lfVal.getModule)
      val name = getInternedDottedName(lfVal.getNameInternedDname)
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConId(lfTyConName: PLF.TypeConId): TypeConName = {
      val (packageId, module) = decodeModuleRef(lfTyConName.getModule)
      val name = getInternedDottedName(lfTyConName.getNameInternedDname)
      Identifier(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeSynId(lfTySynName: PLF.TypeSynId): TypeSynName = {
      val (packageId, module) = decodeModuleRef(lfTySynName.getModule)
      val name = getInternedDottedName(lfTySynName.getNameInternedDname)
      Identifier(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConApp(lfTyConApp: PLF.Type.Con): Work[TypeConApp] = {
      Work.sequence(lfTyConApp.getArgsList.asScala.view.map(decodeType(_)(Ret(_)))) { types =>
        Ret(TypeConApp(decodeTypeConId(lfTyConApp.getTycon), types.to(ImmArray)))
      }
    }

    private def decodeExpr[T](lfExpr: PLF.Expr, definition: String)(k: Expr => Work[T]): Work[T] = {
      Work.Bind(Work.Delay(() => decodeExpr1(lfExpr, definition)), k)
    }

    private def decodeExpr1(lfExpr: PLF.Expr, definition: String): Work[Expr] = {
      Work.bind(lfExpr.getSumCase match {
        case PLF.Expr.SumCase.VAR_INTERNED_STR =>
          Ret(EVar(getInternedName(lfExpr.getVarInternedStr)))

        case PLF.Expr.SumCase.VAL =>
          Ret(EVal(decodeValId(lfExpr.getVal)))

        case PLF.Expr.SumCase.BUILTIN_LIT =>
          Ret(EBuiltinLit(decodeBuiltinLit(lfExpr.getBuiltinLit)))

        case PLF.Expr.SumCase.BUILTIN_CON =>
          Ret(lfExpr.getBuiltinCon match {
            case PLF.BuiltinCon.CON_UNIT => EUnit
            case PLF.BuiltinCon.CON_FALSE => EFalse
            case PLF.BuiltinCon.CON_TRUE => ETrue
            case PLF.BuiltinCon.UNRECOGNIZED =>
              throw Error.Parsing("BuiltinCon.UNRECOGNIZED")
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
            val field = internedName(recProj.getFieldInternedStr)
            decodeExpr(recProj.getRecord, definition) { record =>
              Ret(ERecProj(tycon, field, record))
            }
          }

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          Work.bind(decodeTypeConApp(recUpd.getTycon)) { tycon =>
            val field = internedName(recUpd.getFieldInternedStr)
            decodeExpr(recUpd.getRecord, definition) { record =>
              decodeExpr(recUpd.getUpdate, definition) { update =>
                Ret(ERecUpd(tycon, field, record, update))
              }
            }
          }

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          Work.bind(decodeTypeConApp(varCon.getTycon)) { tycon =>
            val name = internedName(varCon.getVariantConInternedStr)
            decodeExpr(varCon.getVariantArg, definition) { expr =>
              Ret(EVariantCon(tycon, name, expr))
            }
          }

        case PLF.Expr.SumCase.ENUM_CON =>
          val enumCon = lfExpr.getEnumCon
          Ret(
            EEnumCon(
              decodeTypeConId(enumCon.getTycon),
              internedName(enumCon.getEnumConInternedStr),
            )
          )

        case PLF.Expr.SumCase.STRUCT_CON =>
          val structCon = lfExpr.getStructCon
          Work.sequence(
            structCon.getFieldsList.asScala.view.map(x => decodeFieldWithExpr(x, definition))
          ) { xs => Ret(EStructCon(xs.to(ImmArray))) }

        case PLF.Expr.SumCase.STRUCT_PROJ =>
          val structProj = lfExpr.getStructProj
          val field = internedName(structProj.getFieldInternedStr)
          decodeExpr(structProj.getStruct, definition) { struct =>
            Ret(EStructProj(field, struct))
          }

        case PLF.Expr.SumCase.STRUCT_UPD =>
          val structUpd = lfExpr.getStructUpd
          val field = internedName(structUpd.getFieldInternedStr)
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
          assertSince(LV.Features.scenarios, "Scenarios")
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
            decodeExpr(some.getValue, definition) { expr =>
              Ret(ESome(typ, expr))
            }
          }

        case PLF.Expr.SumCase.TO_ANY =>
          decodeType(lfExpr.getToAny.getType) { typ =>
            decodeExpr(lfExpr.getToAny.getExpr, definition) { expr =>
              Ret(EToAny(typ, expr))
            }
          }

        case PLF.Expr.SumCase.FROM_ANY =>
          decodeType(lfExpr.getFromAny.getType) { typ =>
            decodeExpr(lfExpr.getFromAny.getExpr, definition) { expr =>
              Ret(EFromAny(typ, expr))
            }
          }

        case PLF.Expr.SumCase.TYPE_REP =>
          decodeType(lfExpr.getTypeRep) { typ =>
            Ret(ETypeRep(typ))
          }

        case PLF.Expr.SumCase.THROW =>
          assertSince(LV.Features.exceptions, "Expr.from_any_exception")
          val eThrow = lfExpr.getThrow
          decodeType(eThrow.getReturnType) { returnType =>
            decodeType(eThrow.getExceptionType) { exceptionType =>
              decodeExpr(eThrow.getExceptionExpr, definition) { exception =>
                Ret(EThrow(returnType, exceptionType, exception))
              }
            }
          }

        case PLF.Expr.SumCase.TO_ANY_EXCEPTION =>
          assertSince(LV.Features.exceptions, "Expr.to_any_exception")
          val toAnyException = lfExpr.getToAnyException
          decodeType(toAnyException.getType) { typ =>
            decodeExpr(toAnyException.getExpr, definition) { value =>
              Ret(EToAnyException(typ, value))
            }
          }

        case PLF.Expr.SumCase.FROM_ANY_EXCEPTION =>
          assertSince(LV.Features.exceptions, "Expr.from_any_exception")
          val fromAnyException = lfExpr.getFromAnyException
          decodeType(fromAnyException.getType) { typ =>
            decodeExpr(fromAnyException.getExpr, definition) { value =>
              Ret(EFromAnyException(typ, value))
            }
          }

        case PLF.Expr.SumCase.TO_INTERFACE =>
          val toInterface = lfExpr.getToInterface
          val interfaceId = decodeTypeConId(toInterface.getInterfaceType)
          val templateId = decodeTypeConId(toInterface.getTemplateType)
          decodeExpr(toInterface.getTemplateExpr, definition) { value =>
            Ret(EToInterface(interfaceId, templateId, value))
          }

        case PLF.Expr.SumCase.FROM_INTERFACE =>
          val fromInterface = lfExpr.getFromInterface
          val interfaceId = decodeTypeConId(fromInterface.getInterfaceType)
          val templateId = decodeTypeConId(fromInterface.getTemplateType)
          decodeExpr(fromInterface.getInterfaceExpr, definition) { value =>
            Ret(EFromInterface(interfaceId, templateId, value))
          }

        case PLF.Expr.SumCase.CALL_INTERFACE =>
          val callInterface = lfExpr.getCallInterface
          val interfaceId = decodeTypeConId(callInterface.getInterfaceType)
          val methodName =
            getInternedName(callInterface.getMethodInternedName)
          decodeExpr(callInterface.getInterfaceExpr, definition) { value =>
            Ret(ECallInterface(interfaceId, methodName, value))
          }

        case PLF.Expr.SumCase.SIGNATORY_INTERFACE =>
          val signatoryInterface = lfExpr.getSignatoryInterface
          val ifaceId = decodeTypeConId(signatoryInterface.getInterface)
          decodeExpr(signatoryInterface.getExpr, definition) { body =>
            Ret(ESignatoryInterface(ifaceId, body))
          }

        case PLF.Expr.SumCase.OBSERVER_INTERFACE =>
          val observerInterface = lfExpr.getObserverInterface
          decodeExpr(observerInterface.getExpr, definition) { body =>
            Ret(
              EObserverInterface(ifaceId = decodeTypeConId(observerInterface.getInterface), body)
            )
          }

        case PLF.Expr.SumCase.UNSAFE_FROM_INTERFACE =>
          val unsafeFromInterface = lfExpr.getUnsafeFromInterface
          val interfaceId = decodeTypeConId(unsafeFromInterface.getInterfaceType)
          val templateId = decodeTypeConId(unsafeFromInterface.getTemplateType)
          decodeExpr(unsafeFromInterface.getContractIdExpr, definition) { contractIdExpr =>
            decodeExpr(unsafeFromInterface.getInterfaceExpr, definition) { ifaceExpr =>
              Ret(EUnsafeFromInterface(interfaceId, templateId, contractIdExpr, ifaceExpr))
            }
          }

        case PLF.Expr.SumCase.TO_REQUIRED_INTERFACE =>
          val toRequiredInterface = lfExpr.getToRequiredInterface
          val requiredIfaceId = decodeTypeConId(toRequiredInterface.getRequiredInterface)
          val requiringIfaceId = decodeTypeConId(toRequiredInterface.getRequiringInterface)
          decodeExpr(toRequiredInterface.getExpr, definition) { body =>
            Ret(EToRequiredInterface(requiredIfaceId, requiringIfaceId, body))
          }

        case PLF.Expr.SumCase.FROM_REQUIRED_INTERFACE =>
          val fromRequiredInterface = lfExpr.getFromRequiredInterface
          decodeExpr(fromRequiredInterface.getExpr, definition) { body =>
            Ret(
              EFromRequiredInterface(
                requiredIfaceId = decodeTypeConId(fromRequiredInterface.getRequiredInterface),
                requiringIfaceId = decodeTypeConId(fromRequiredInterface.getRequiringInterface),
                body,
              )
            )
          }

        case PLF.Expr.SumCase.UNSAFE_FROM_REQUIRED_INTERFACE =>
          val unsafeFromRequiredInterface = lfExpr.getUnsafeFromRequiredInterface
          val requiredIfaceId = decodeTypeConId(unsafeFromRequiredInterface.getRequiredInterface)
          val requiringIfaceId =
            decodeTypeConId(unsafeFromRequiredInterface.getRequiringInterface)
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
          val interfaceTemplateTypeRep = lfExpr.getInterfaceTemplateTypeRep
          val ifaceId = decodeTypeConId(interfaceTemplateTypeRep.getInterface)
          decodeExpr(interfaceTemplateTypeRep.getExpr, definition) { body =>
            Ret(EInterfaceTemplateTypeRep(ifaceId, body))
          }

        case PLF.Expr.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("Expr.SUM_NOT_SET")

        case PLF.Expr.SumCase.VIEW_INTERFACE =>
          val viewInterface = lfExpr.getViewInterface
          val ifaceId = decodeTypeConId(viewInterface.getInterface)
          decodeExpr(viewInterface.getExpr, definition) { expr =>
            Ret(EViewInterface(ifaceId, expr))
          }

        case PLF.Expr.SumCase.CHOICE_CONTROLLER =>
          assertSince(LV.Features.choiceFuncs, "Expr.choice_controller")
          val choiceController = lfExpr.getChoiceController
          val tplCon = decodeTypeConId(choiceController.getTemplate)
          val choiceName = internedName(choiceController.getChoiceInternedStr)
          decodeExpr(choiceController.getContractExpr, definition) { contractExpr =>
            decodeExpr(choiceController.getChoiceArgExpr, definition) { choiceArgExpr =>
              Ret(EChoiceController(tplCon, choiceName, contractExpr, choiceArgExpr))
            }
          }

        case PLF.Expr.SumCase.CHOICE_OBSERVER =>
          assertSince(LV.Features.choiceFuncs, "Expr.choice_observer")
          val choiceObserver = lfExpr.getChoiceObserver
          val tplCon = decodeTypeConId(choiceObserver.getTemplate)
          val choiceName = internedName(choiceObserver.getChoiceInternedStr)
          decodeExpr(choiceObserver.getContractExpr, definition) { contractExpr =>
            decodeExpr(choiceObserver.getChoiceArgExpr, definition) { choiceArgExpr =>
              Ret(EChoiceObserver(tplCon, choiceName, contractExpr, choiceArgExpr))
            }
          }

        case PLF.Expr.SumCase.EXPERIMENTAL =>
          assertSince(LV.Features.unstable, "Expr.experimental")
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
            tycon = decodeTypeConId(variant.getCon),
            variant = internedName(variant.getVariantInternedStr),
            binder = internedName(variant.getBinderInternedStr),
          )
        case PLF.CaseAlt.SumCase.ENUM =>
          val enumeration = lfCaseAlt.getEnum
          CPEnum(
            tycon = decodeTypeConId(enumeration.getCon),
            constructor = internedName(enumeration.getConstructorInternedStr),
          )
        case PLF.CaseAlt.SumCase.BUILTIN_CON =>
          decodeBuiltinCon(lfCaseAlt.getBuiltinCon)
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(
            head = internedName(cons.getVarHeadInternedStr),
            tail = internedName(cons.getVarTailInternedStr),
          )

        case PLF.CaseAlt.SumCase.OPTIONAL_NONE =>
          CPNone

        case PLF.CaseAlt.SumCase.OPTIONAL_SOME =>
          val some = lfCaseAlt.getOptionalSome
          CPSome(
            internedName(some.getVarBodyInternedStr)
          )

        case PLF.CaseAlt.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("CaseAlt.SUM_NOT_SET")
      }
      decodeExpr(lfCaseAlt.getBody, definition) { rhs =>
        Ret(CaseAlt(pat, rhs))
      }
    }

    private[this] def decodeRetrieveByKey(value: PLF.Update.RetrieveByKey): Work[TypeConName] = {
      assertSince(LV.Features.contractKeys, "RetrieveByKey")
      Ret(decodeTypeConId(value.getTemplate))
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
            Ret(UpdateCreate(templateId = decodeTypeConId(create.getTemplate), arg))
          }

        case PLF.Update.SumCase.CREATE_INTERFACE =>
          val create = lfUpdate.getCreateInterface
          decodeExpr(create.getExpr, definition) { arg =>
            Ret(UpdateCreateInterface(interfaceId = decodeTypeConId(create.getInterface), arg))
          }

        case PLF.Update.SumCase.EXERCISE =>
          val exercise = lfUpdate.getExercise
          val templateId = decodeTypeConId(exercise.getTemplate)
          val choice = internedName(exercise.getChoiceInternedStr)
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(UpdateExercise(templateId, choice, cidE, argE))
            }
          }

        case PLF.Update.SumCase.SOFT_EXERCISE =>
          val exercise = lfUpdate.getSoftExercise
          val templateId = decodeTypeConId(exercise.getTemplate)
          val choice = internedName(exercise.getChoiceInternedStr)
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(
                UpdateSoftExercise(templateId, choice, cidE, argE)
              )
            }
          }

        case PLF.Update.SumCase.DYNAMIC_EXERCISE =>
          val exercise = lfUpdate.getDynamicExercise
          val templateId = decodeTypeConId(exercise.getTemplate)
          val choice = internedName(exercise.getChoiceInternedStr)
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Ret(UpdateDynamicExercise(templateId, choice, cidE, argE))
            }
          }

        case PLF.Update.SumCase.EXERCISE_INTERFACE =>
          val exercise = lfUpdate.getExerciseInterface
          decodeExpr(exercise.getCid, definition) { cidE =>
            decodeExpr(exercise.getArg, definition) { argE =>
              Work.bind(
                if (exercise.hasGuard) {
                  assertSince(LV.Features.extendedInterfaces, "exerciseInterface.guard")
                  decodeExpr(exercise.getGuard, definition) { e =>
                    Ret(Some(e))
                  }
                } else
                  Ret(None)
              ) { guardE =>
                val interfaceId = decodeTypeConId(exercise.getInterface)
                val choice = internedName(exercise.getChoiceInternedStr)
                Ret(UpdateExerciseInterface(interfaceId, choice, cidE, argE, guardE))
              }
            }
          }

        case PLF.Update.SumCase.EXERCISE_BY_KEY =>
          assertSince(LV.Features.contractKeys, "exercise_by_key")
          val exerciseByKey = lfUpdate.getExerciseByKey
          val templateId = decodeTypeConId(exerciseByKey.getTemplate)
          val choice = getInternedName(exerciseByKey.getChoiceInternedStr)
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
            Ret(UpdateFetchTemplate(templateId = decodeTypeConId(fetch.getTemplate), contractId))
          }

        case PLF.Update.SumCase.SOFT_FETCH =>
          val softFetch = lfUpdate.getSoftFetch
          decodeExpr(softFetch.getCid, definition) { contractId =>
            Ret(
              UpdateSoftFetchTemplate(
                templateId = decodeTypeConId(softFetch.getTemplate),
                contractId,
              )
            )
          }

        case PLF.Update.SumCase.FETCH_INTERFACE =>
          val fetch = lfUpdate.getFetchInterface
          decodeExpr(fetch.getCid, definition) { contractId =>
            Ret(
              UpdateFetchInterface(interfaceId = decodeTypeConId(fetch.getInterface), contractId)
            )
          }

        case PLF.Update.SumCase.FETCH_BY_KEY =>
          assertSince(LV.Features.contractKeys, "fetch_by_key")
          Work.bind(decodeRetrieveByKey(lfUpdate.getFetchByKey)) { tmplId =>
            Ret(UpdateFetchByKey(tmplId))
          }

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          assertSince(LV.Features.contractKeys, "lookup_by_key")
          Work.bind(decodeRetrieveByKey(lfUpdate.getLookupByKey)) { tmplId =>
            Ret(UpdateLookupByKey(tmplId))
          }

        case PLF.Update.SumCase.EMBED_EXPR =>
          val embedExpr = lfUpdate.getEmbedExpr
          decodeType(embedExpr.getType) { typ =>
            decodeExpr(embedExpr.getBody, definition) { expr =>
              Ret(UpdateEmbedExpr(typ, expr))
            }
          }

        case PLF.Update.SumCase.TRY_CATCH =>
          assertSince(LV.Features.exceptions, "Update.try_catch")
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

    private[this] def decodeTypeVarWithKind(
        lfTypeVarWithKind: PLF.TypeVarWithKind
    ): Work[(TypeVarName, Kind)] = {
      val name =
        internedName(lfTypeVarWithKind.getVarInternedStr)
      Work.bind(decodeKind(lfTypeVarWithKind.getKind)) { kind =>
        Ret {
          name -> kind
        }
      }
    }

    private[this] def decodeBinding(lfBinding: PLF.Binding, definition: String): Work[Binding] = {
      Work.bind(decodeBinder(lfBinding.getBinder)) { case (binder, typ) =>
        decodeExpr(lfBinding.getBound, definition) { expr =>
          Ret(Binding(Some(binder), typ, expr))
        }
      }
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): Work[(ExprVarName, Type)] = {
      decodeType(lfBinder.getType) { typ =>
        Ret(
          internedName(lfBinder.getVarInternedStr) -> typ
        )
      }
    }

    private[this] def decodeBuiltinCon(lfBuiltinCon: PLF.BuiltinCon): CPBuiltinCon =
      lfBuiltinCon match {
        case PLF.BuiltinCon.CON_UNIT =>
          CPUnit
        case PLF.BuiltinCon.CON_FALSE =>
          CPFalse
        case PLF.BuiltinCon.CON_TRUE =>
          CPTrue
        case _ => throw Error.Parsing("Unknown BuiltinCon: " + lfBuiltinCon.toString)
      }

    private[this] def decodeBuiltinLit(lfBuiltinLit: PLF.BuiltinLit): BuiltinLit =
      lfBuiltinLit.getSumCase match {
        case PLF.BuiltinLit.SumCase.INT64 =>
          BLInt64(lfBuiltinLit.getInt64)
        case PLF.BuiltinLit.SumCase.TIMESTAMP =>
          val t = Time.Timestamp.fromLong(lfBuiltinLit.getTimestamp)
          t.fold(e => throw Error.Parsing("error decoding timestamp: " + e), BLTimestamp)
        case PLF.BuiltinLit.SumCase.DATE =>
          val d = Time.Date.fromDaysSinceEpoch(lfBuiltinLit.getDate)
          d.fold(e => throw Error.Parsing("error decoding date: " + e), BLDate)
        case PLF.BuiltinLit.SumCase.TEXT_INTERNED_STR =>
          BLText(getInternedStr(lfBuiltinLit.getTextInternedStr))
        case PLF.BuiltinLit.SumCase.NUMERIC_INTERNED_STR =>
          toBLNumeric(getInternedStr(lfBuiltinLit.getNumericInternedStr))
        case PLF.BuiltinLit.SumCase.ROUNDING_MODE =>
          assertSince(LV.Features.bigNumeric, "Expr.rounding_mode")
          BLRoundingMode(java.math.RoundingMode.valueOf(lfBuiltinLit.getRoundingModeValue))
        case PLF.BuiltinLit.SumCase.SUM_NOT_SET =>
          throw Error.Parsing("BuiltinLit.SUM_NOT_SET")
      }
  }

  private def versionIsOlderThan(minVersion: LV): Boolean =
    languageVersion < minVersion

  private[this] def toName(s: String): Name =
    eitherToParseError(Name.fromString(s))

  private def toPackageName(s: String): PackageName = {
    eitherToParseError(PackageName.fromString(s))
  }

  private def toPackageVersion(s: String) = {
    eitherToParseError(PackageVersion.fromString(s))
  }

  private[this] def toBLNumeric(s: String) =
    BLNumeric(eitherToParseError(Numeric.fromString(s)))

  private[this] def notSupportedError(description: String): Error.Parsing =
    Error.Parsing(s"$description is not supported by Daml-LF 2.$minor")

  // maxVersion excluded
  private[this] def assertUntil(maxVersion: LV, description: => String): Unit =
    if (!versionIsOlderThan(maxVersion))
      throw notSupportedError(description)

  // minVersion included
  private[this] def assertSince(minVersion: LV, description: => String): Unit =
    if (versionIsOlderThan(minVersion))
      throw notSupportedError(description)

  private def assertNonEmpty(s: collection.Seq[_], description: => String): Unit =
    if (s.isEmpty) throw Error.Parsing(s"Unexpected empty $description")

  private[this] def assertEmpty(s: collection.Seq[_], description: => String): Unit =
    if (s.nonEmpty) throw Error.Parsing(s"Unexpected non-empty $description")

  private[this] def assertEmpty(s: util.List[_], description: => String): Unit =
    if (!s.isEmpty) throw Error.Parsing(s"Unexpected non-empty $description")
}

private[lf] object DecodeV2 {

  private def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw Error.Parsing(err), identity)

  case class BuiltinTypeInfo(
      proto: PLF.BuiltinType,
      bTyp: BuiltinType,
      minVersion: LV = LV.Features.default,
  ) {
    val typ = TBuiltin(bTyp)
  }

  val builtinTypeInfos: List[BuiltinTypeInfo] = {
    import PLF.BuiltinType._
    List(
      BuiltinTypeInfo(UNIT, BTUnit),
      BuiltinTypeInfo(BOOL, BTBool),
      BuiltinTypeInfo(TEXT, BTText),
      BuiltinTypeInfo(INT64, BTInt64),
      BuiltinTypeInfo(TIMESTAMP, BTTimestamp),
      BuiltinTypeInfo(PARTY, BTParty),
      BuiltinTypeInfo(LIST, BTList),
      BuiltinTypeInfo(UPDATE, BTUpdate),
      BuiltinTypeInfo(SCENARIO, BTScenario, minVersion = LV.Features.scenarios),
      BuiltinTypeInfo(CONTRACT_ID, BTContractId),
      BuiltinTypeInfo(DATE, BTDate),
      BuiltinTypeInfo(OPTIONAL, BTOptional),
      BuiltinTypeInfo(TEXTMAP, BTTextMap, minVersion = LV.Features.textMap),
      BuiltinTypeInfo(GENMAP, BTGenMap),
      BuiltinTypeInfo(ARROW, BTArrow),
      BuiltinTypeInfo(NUMERIC, BTNumeric),
      BuiltinTypeInfo(ANY, BTAny),
      BuiltinTypeInfo(TYPE_REP, BTTypeRep),
      BuiltinTypeInfo(BIGNUMERIC, BTBigNumeric, minVersion = LV.Features.bigNumeric),
      BuiltinTypeInfo(ROUNDING_MODE, BTRoundingMode, minVersion = LV.Features.bigNumeric),
      BuiltinTypeInfo(ANY_EXCEPTION, BTAnyException, minVersion = LV.Features.exceptions),
    )
  }

  private val builtinTypeInfoMap =
    builtinTypeInfos
      .map(info => info.proto -> info)
      .toMap

  case class BuiltinFunctionInfo(
      proto: PLF.BuiltinFunction,
      builtin: BuiltinFunction,
      minVersion: LV = LV.Features.default, // first version that does support the builtin
      maxVersion: Option[LV] = None, // first version that does not support the builtin
      implicitParameters: List[Type] = List.empty,
  ) {
    val expr: Expr = implicitParameters.foldLeft[Expr](EBuiltinFun(builtin))(ETyApp)
  }

  val builtinFunctionInfos: List[BuiltinFunctionInfo] = {
    import LV.Features._
    import PLF.BuiltinFunction._
    List(
      BuiltinFunctionInfo(ADD_NUMERIC, BAddNumeric),
      BuiltinFunctionInfo(SUB_NUMERIC, BSubNumeric),
      BuiltinFunctionInfo(MUL_NUMERIC, BMulNumeric),
      BuiltinFunctionInfo(DIV_NUMERIC, BDivNumeric),
      BuiltinFunctionInfo(ROUND_NUMERIC, BRoundNumeric),
      BuiltinFunctionInfo(CAST_NUMERIC, BCastNumeric),
      BuiltinFunctionInfo(SHIFT_NUMERIC, BShiftNumeric),
      BuiltinFunctionInfo(ADD_INT64, BAddInt64),
      BuiltinFunctionInfo(SUB_INT64, BSubInt64),
      BuiltinFunctionInfo(MUL_INT64, BMulInt64),
      BuiltinFunctionInfo(DIV_INT64, BDivInt64),
      BuiltinFunctionInfo(MOD_INT64, BModInt64),
      BuiltinFunctionInfo(EXP_INT64, BExpInt64),
      BuiltinFunctionInfo(INT64_TO_NUMERIC, BInt64ToNumeric),
      BuiltinFunctionInfo(NUMERIC_TO_INT64, BNumericToInt64),
      BuiltinFunctionInfo(FOLDL, BFoldl),
      BuiltinFunctionInfo(FOLDR, BFoldr),
      BuiltinFunctionInfo(TEXTMAP_EMPTY, BTextMapEmpty, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(TEXTMAP_INSERT, BTextMapInsert, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(TEXTMAP_LOOKUP, BTextMapLookup, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(TEXTMAP_DELETE, BTextMapDelete, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(TEXTMAP_TO_LIST, BTextMapToList, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(TEXTMAP_SIZE, BTextMapSize, minVersion = LV.Features.textMap),
      BuiltinFunctionInfo(GENMAP_EMPTY, BGenMapEmpty),
      BuiltinFunctionInfo(GENMAP_INSERT, BGenMapInsert),
      BuiltinFunctionInfo(GENMAP_LOOKUP, BGenMapLookup),
      BuiltinFunctionInfo(GENMAP_DELETE, BGenMapDelete),
      BuiltinFunctionInfo(GENMAP_KEYS, BGenMapKeys),
      BuiltinFunctionInfo(GENMAP_VALUES, BGenMapValues),
      BuiltinFunctionInfo(GENMAP_SIZE, BGenMapSize),
      BuiltinFunctionInfo(APPEND_TEXT, BAppendText),
      BuiltinFunctionInfo(ERROR, BError),
      BuiltinFunctionInfo(INT64_TO_TEXT, BInt64ToText),
      BuiltinFunctionInfo(NUMERIC_TO_TEXT, BNumericToText),
      BuiltinFunctionInfo(TIMESTAMP_TO_TEXT, BTimestampToText),
      BuiltinFunctionInfo(PARTY_TO_TEXT, BPartyToText),
      BuiltinFunctionInfo(CONTRACT_ID_TO_TEXT, BContractIdToText),
      BuiltinFunctionInfo(CODE_POINTS_TO_TEXT, BCodePointsToText),
      BuiltinFunctionInfo(TEXT_TO_PARTY, BTextToParty),
      BuiltinFunctionInfo(TEXT_TO_INT64, BTextToInt64),
      BuiltinFunctionInfo(TEXT_TO_NUMERIC, BTextToNumeric),
      BuiltinFunctionInfo(TEXT_TO_CODE_POINTS, BTextToCodePoints),
      BuiltinFunctionInfo(SHA256_TEXT, BSHA256Text),
      BuiltinFunctionInfo(DATE_TO_UNIX_DAYS, BDateToUnixDays),
      BuiltinFunctionInfo(EXPLODE_TEXT, BExplodeText),
      BuiltinFunctionInfo(IMPLODE_TEXT, BImplodeText),
      BuiltinFunctionInfo(TIMESTAMP_TO_UNIX_MICROSECONDS, BTimestampToUnixMicroseconds),
      BuiltinFunctionInfo(DATE_TO_TEXT, BDateToText),
      BuiltinFunctionInfo(UNIX_DAYS_TO_DATE, BUnixDaysToDate),
      BuiltinFunctionInfo(UNIX_MICROSECONDS_TO_TIMESTAMP, BUnixMicrosecondsToTimestamp),
      BuiltinFunctionInfo(EQUAL, BEqual),
      BuiltinFunctionInfo(LESS, BLess),
      BuiltinFunctionInfo(LESS_EQ, BLessEq),
      BuiltinFunctionInfo(GREATER, BGreater),
      BuiltinFunctionInfo(GREATER_EQ, BGreaterEq),
      BuiltinFunctionInfo(EQUAL_LIST, BEqualList),
      BuiltinFunctionInfo(TRACE, BTrace),
      BuiltinFunctionInfo(COERCE_CONTRACT_ID, BCoerceContractId),
      BuiltinFunctionInfo(SCALE_BIGNUMERIC, BScaleBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(PRECISION_BIGNUMERIC, BPrecisionBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(ADD_BIGNUMERIC, BAddBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(SUB_BIGNUMERIC, BSubBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(MUL_BIGNUMERIC, BMulBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(DIV_BIGNUMERIC, BDivBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(SHIFT_RIGHT_BIGNUMERIC, BShiftRightBigNumeric, minVersion = bigNumeric),
      BuiltinFunctionInfo(BIGNUMERIC_TO_NUMERIC, BBigNumericToNumeric, minVersion = bigNumeric),
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
}
