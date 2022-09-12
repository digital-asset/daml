// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package typesig
package reader

import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.lf.data.ImmArray
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{DottedName, QualifiedName}
import com.daml.lf.language.Ast
import com.daml.lf.language.LanguageVersion
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.\/-
import scalaz.syntax.functor._

import scala.language.implicitConversions

class SignatureReaderSpec extends AnyWordSpec with Matchers with Inside {
  import PackageSignature.TypeDecl

  private def dnfs(args: String*): Ref.DottedName = Ref.DottedName.assertFromSegments(args)
  private val moduleName: Ref.ModuleName = dnfs("Main")
  private val packageId: Ref.PackageId = Ref.PackageId.assertFromString("dummy-package-id")

  "variant should extract a variant with type params" in {
    val tyVars = ImmArray(starKindedTypeVar("call"), starKindedTypeVar("put"))
    val dataName = dnfs("Option")
    val qualifiedName = Ref.QualifiedName(moduleName, dnfs("Option"))
    val variantDataType = Ast.DDataType(
      serializable = true,
      params = tyVars,
      cons = Ast.DataVariant(ImmArray(varField("Call", "call"), varField("Put", "put"))),
    )

    val actual = SignatureReader.foldModule(wrappInModule(dataName, variantDataType))

    val expectedResult = Map(
      qualifiedName ->
        TypeDecl.Normal(
          DefDataType(
            ImmArray[Ref.Name]("call", "put").toSeq,
            Variant(ImmArray(name("Call") -> TypeVar("call"), name("Put") -> TypeVar("put")).toSeq),
          )
        )
    )

    actual.typeDecls shouldBe expectedResult
  }

  private[this] def nameClashRecordVariantName(tail: String): TypeConName =
    TypeConName(
      Ref.Identifier(
        packageId,
        Ref.QualifiedName(dnfs("Main"), dnfs("NameClashRecordVariant", tail)),
      )
    )

  "variant should extract a variant, nested records are not be resolved" in {
    val variantDataType = Ast.DDataType(
      serializable = true,
      ImmArray.Empty,
      Ast.DataVariant(
        ImmArray(
          typeConstructorField(
            "NameClashRecordVariantA",
            List("NameClashRecordVariant", "NameClashRecordVariantA"),
          ),
          typeConstructorField(
            "NameClashRecordVariantB",
            List("NameClashRecordVariant", "NameClashRecordVariantB"),
          ),
        )
      ),
    )

    val actual =
      SignatureReader.foldModule(wrappInModule(dnfs("NameClashRecordVariant"), variantDataType))
    val expectedResult = Map(
      Ref.QualifiedName(moduleName, dnfs("NameClashRecordVariant")) ->
        TypeDecl.Normal(
          DefDataType(
            ImmArraySeq.Empty,
            Variant(
              ImmArraySeq[(Ref.Name, TypeCon)](
                (
                  "NameClashRecordVariantA",
                  TypeCon(nameClashRecordVariantName("NameClashRecordVariantA"), ImmArraySeq()),
                ),
                (
                  "NameClashRecordVariantB",
                  TypeCon(nameClashRecordVariantName("NameClashRecordVariantB"), ImmArraySeq()),
                ),
              )
            ),
          )
        )
    )

    actual.typeDecls shouldBe expectedResult
  }

  "record should extract a nested record" in {
    val dataType = Ast.DDataType(
      serializable = true,
      ImmArray.Empty,
      Ast.DataRecord(
        ImmArray(
          primField("wait", Ast.BTInt64),
          primField("wait_", Ast.BTInt64),
          primField("wait__", Ast.BTInt64),
        )
      ),
    )

    val actual = SignatureReader.foldModule(wrappInModule(dnfs("Record"), dataType))

    val expectedResult = Map(
      Ref.QualifiedName(moduleName, dnfs("Record")) ->
        TypeDecl.Normal(
          DefDataType(
            ImmArraySeq.Empty,
            Record(
              ImmArraySeq[(Ref.Name, TypePrim)](
                ("wait", TypePrim(PrimTypeInt64, ImmArraySeq())),
                ("wait_", TypePrim(PrimTypeInt64, ImmArraySeq())),
                ("wait__", TypePrim(PrimTypeInt64, ImmArraySeq())),
              )
            ),
          )
        )
    )

    actual.typeDecls shouldBe expectedResult
  }

  "map should extract a TextMap" in {
    val dataType = Ast.DDataType(
      serializable = true,
      ImmArray.Empty,
      Ast.DataRecord(
        ImmArray(
          primField("map", Ast.BTTextMap, Ast.TBuiltin(Ast.BTInt64))
        )
      ),
    )

    val actual = SignatureReader.foldModule(wrappInModule(dnfs("MapRecord"), dataType))
    val expectedResult = Map(
      Ref.QualifiedName(moduleName, dnfs("MapRecord")) ->
        TypeDecl.Normal(
          DefDataType(
            ImmArraySeq.Empty,
            Record(
              ImmArraySeq[(Ref.Name, TypePrim)](
                (
                  "map",
                  TypePrim(PrimTypeTextMap, ImmArraySeq(TypePrim(PrimTypeInt64, ImmArraySeq()))),
                )
              )
            ),
          )
        )
    )

    actual.typeDecls shouldBe expectedResult
  }

  "Package metadata should be extracted if present" in {
    def pkg(metadata: Option[Ast.PackageMetadata]) =
      Ast.Package(
        modules = Map.empty,
        directDeps = Set.empty,
        languageVersion = LanguageVersion.default,
        metadata = metadata,
      )
    val notPresent = pkg(None)
    val name = Ref.PackageName.assertFromString("my-package")
    val version = Ref.PackageVersion.assertFromString("1.2.3")
    val present = pkg(Some(Ast.PackageMetadata(name, version)))
    SignatureReader
      .readPackageSignature(() => \/-((packageId, notPresent)))
      ._2
      .metadata shouldBe None
    SignatureReader.readPackageSignature(() => \/-((packageId, present)))._2.metadata shouldBe Some(
      PackageMetadata(name, version)
    )
  }

  "a real dar" should {
    import archive.DarReader.readArchiveFromFile
    import QualifiedName.{assertFromString => qn}
    import Ref.ChoiceName.{assertFromString => cn}

    lazy val itp = {
      val file = requiredResource("daml-lf/api-type-signature/InterfaceTestPackage.dar")
      inside(readArchiveFromFile(file)) { case Right(dar) =>
        dar.map { payload =>
          val (errors, ii) = typesig.PackageSignature.read(payload)
          errors should ===(Errors.zeroErrors)
          ii
        }
      }
    }
    lazy val itpES = EnvironmentSignature.fromPackageSignatures(itp).resolveChoices

    lazy val itpWithoutRetroImplements = itp.copy(
      main = itp.main.copy(
        interfaces = itp.main.interfaces - qn("RetroInterface:RetroIf")
      )
    )
    lazy val itpESWithoutRetroImplements =
      EnvironmentSignature.fromPackageSignatures(itpWithoutRetroImplements).resolveChoices

    "load without errors" in {
      itp shouldBe itp
    }

    val Foo = qn("InterfaceTestPackage:Foo")
    val Bar = cn("Bar")
    val Archive = cn("Archive")
    val TIf = qn("InterfaceTestPackage:TIf")
    val LibTIf = qn("InterfaceTestLib:TIf")
    val RetroIf = qn("RetroInterface:RetroIf")
    val LibTIfView = qn("InterfaceTestLib:TIfView")
    val Useless = cn("Useless")
    val UselessTy = qn("InterfaceTestPackage:Useless")
    import itp.main.{packageId => itpPid}

    "exclude interface choices with template choices" in {
      inside(itp.main.typeDecls get Foo) { case Some(TypeDecl.Template(_, tpl)) =>
        tpl.tChoices.directChoices.keySet should ===(Set("Bar", "Archive"))
      }
    }

    "include interface choices in separate inheritedChoices" in {
      inside(itp.main.typeDecls get Foo) {
        case Some(
              TypeDecl.Template(_, DefTemplate(TemplateChoices.Unresolved(_, inherited), _, _))
            ) =>
          inherited.map(_.qualifiedName) should ===(Set(TIf, LibTIf))
      }
    }

    object TheUselessChoice {
      def unapply(ty: TemplateChoice.FWT): Option[(QualifiedName, QualifiedName)] = {
        val ItpPid = itpPid
        ty match {
          case TemplateChoice(
                TypeCon(TypeConName(Ref.Identifier(ItpPid, uselessTy)), Seq()),
                true,
                TypePrim(
                  PrimType.ContractId,
                  Seq(TypeCon(TypeConName(Ref.Identifier(ItpPid, tIf)), Seq())),
                ),
              ) =>
            Some((uselessTy, tIf))
          case _ => None
        }
      }
    }

    "have interfaces with choices" in {
      itp.main.interfaces.keySet should ===(Set(LibTIf, TIf, RetroIf))
      inside(itp.main.interfaces(TIf).choices get Useless) {
        case Some(TheUselessChoice(UselessTy, TIf)) =>
      }
    }

    "have interfaces with retroImplements" in {
      itp.main.interfaces.keySet should ===(Set(LibTIf, TIf, RetroIf))
      itp.main.interfaces(RetroIf).retroImplements should ===(
        Set(Ref.TypeConName(itp.main.packageId, Foo))
      )
    }

    "identify a record interface view" in {
      inside(itp.main.interfaces(LibTIf).viewType) { case Some(Ref.TypeConName(_, LibTIfView)) =>
      }
    }

    def viewNameExpectsRec =
      inside(itp.main.interfaces(LibTIf).viewType) { case Some(viewName) =>
        (
          viewName,
          inside(itp.main.typeDecls(viewName.qualifiedName)) {
            case TypeDecl.Normal(DefDataType(_, rec)) =>
              rec
          },
        )
      }

    "finds an interface view from Interface sets" in {
      val (viewName, expectedRec) = viewNameExpectsRec
      PackageSignature.resolveInterfaceViewType {
        case id if id == itp.main.packageId => itp.main
      }(viewName) should ===(expectedRec)
    }

    "finds an interface view from EnvironmentInterface" in {
      val (viewName, expectedRec) = viewNameExpectsRec
      itpES.resolveInterfaceViewType(viewName) should ===(Some(expectedRec))
    }

    def foundResolvedChoices(foo: Option[TypeDecl]) = inside(foo) {
      case Some(TypeDecl.Template(_, DefTemplate(TemplateChoices.Resolved(resolved), _, _))) =>
        resolved
    }

    def foundUselessChoice(foo: Option[TypeDecl]) =
      inside(foundResolvedChoices(foo).get(Useless).map(_.forgetNE.toSeq)) {
        case Some(Seq((Some(origin1), choice1), (Some(origin2), choice2))) =>
          Seq(origin1, origin2) should contain theSameElementsAs Seq(
            Ref.Identifier(itpPid, TIf),
            Ref.Identifier(itpPid, LibTIf),
          )
          inside(choice1) { case TheUselessChoice(_, tIf) => tIf should ===(origin1.qualifiedName) }
          inside(choice2) { case TheUselessChoice(_, tIf) => tIf should ===(origin2.qualifiedName) }
      }

    "resolve inherited choices" in {
      foundUselessChoice(itpES.typeDecls get Ref.Identifier(itpPid, Foo))
    }

    "resolve choices internally" in {
      foundUselessChoice(
        itp.main.resolveChoicesAndIgnoreUnresolvedChoices(PartialFunction.empty).typeDecls get Foo
      )
    }

    "collect direct and resolved choices in one map" in {
      foundResolvedChoices(itpES.typeDecls get Ref.Identifier(itpPid, Foo))
        .transform((_, cs) => cs.keySet) should contain theSameElementsAs Map(
        Useless -> Set(Some(Ref.Identifier(itpPid, TIf)), Some(Ref.Identifier(itpPid, LibTIf))),
        Bar -> Set(None),
        Archive -> Set(None),
      )
    }

    "resolve retro implements harmlessly when there are none" in {
      PackageSignature.resolveRetroImplements((), itpWithoutRetroImplements.all)((_, _) =>
        None
      ) should ===((), itpWithoutRetroImplements.all)
      itpESWithoutRetroImplements.resolveRetroImplements should ===(itpESWithoutRetroImplements)
    }

    "resolve retro implements" in {
      val (_, itpResolvedRetro) =
        PackageSignature.resolveRetroImplements((), itp.all)((_, _) => None)
      itpResolvedRetro should !==(itp.all)
      inside(
        itpResolvedRetro.find(_.packageId == itp.main.packageId)
      ) { case Some(packageSignature) =>
        inside(packageSignature.interfaces.get(RetroIf)) {
          case Some(DefInterface(_, retroImplements, _)) =>
            retroImplements shouldBe empty
        }
        inside(packageSignature.typeDecls.get(Foo)) {
          case Some(TypeDecl.Template(_, DefTemplate(_, _, implementedInterfaces))) =>
            implementedInterfaces should contain(Ref.TypeConName(itp.main.packageId, RetroIf))
        }
      }

      val itsESResolvedRetro = itpES.resolveRetroImplements
      itsESResolvedRetro should !==(itpES)
      inside(
        itsESResolvedRetro.interfaces.get(Ref.TypeConName(itp.main.packageId, RetroIf))
      ) { case Some(DefInterface(_, retroImplements, _)) =>
        retroImplements shouldBe empty
      }

      inside(itsESResolvedRetro.typeDecls.get(Ref.TypeConName(itp.main.packageId, Foo))) {
        case Some(TypeDecl.Template(_, DefTemplate(_, _, implementedInterfaces))) =>
          implementedInterfaces should contain(Ref.TypeConName(itp.main.packageId, RetroIf))
      }
    }
  }

  private def wrappInModule(dataName: DottedName, dfn: Ast.DDataType) =
    Ast.Module(
      name = moduleName,
      definitions = Map(dataName -> dfn),
      templates = Map.empty,
      exceptions = Map.empty,
      interfaces = Map.empty,
      featureFlags = Ast.FeatureFlags.default,
    )

  private def dottedName(segments: Iterable[String]): DottedName =
    DottedName.assertFromSegments(segments)

  private def starKindedTypeVar(var_ : Ref.Name) =
    var_ -> Ast.KStar

  private def varField(field: Ref.Name, var_ : Ref.Name) =
    field -> Ast.TVar(var_)

  private def primField(field: Ref.Name, primType: Ast.BuiltinType, args: Ast.Type*) =
    field -> (args foldLeft (Ast.TBuiltin(primType): Ast.Type))(Ast.TApp)

  private def typeConstructorField(field: Ast.FieldName, segments: List[String]) =
    field -> typeConName(segments)

  private def typeConName(segments: List[String]): Ast.TTyCon =
    Ast.TTyCon(Ref.Identifier(packageId, QualifiedName(moduleName, dottedName(segments))))

  private implicit def name(s: String): Ref.Name = Ref.Name.assertFromString(s)

}
