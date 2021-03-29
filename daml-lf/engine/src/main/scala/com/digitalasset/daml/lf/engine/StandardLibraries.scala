package com.daml.lf.language

import com.daml.lf.data.{ImmArray, Ref}

object StandardLibraries {

  private[this] val lf_stdlib_1_dev = Ref.PackageId.assertFromString("lf_stdlib_1_dev")
  private[this] val lf_math_mod = Ref.ModuleName.assertFromString("LF.Math")

  abstract class DataType(
      pkg: Ref.PackageId,
      mod: Ref.ModuleName,
      consName: Ref.DottedName,
  ) {
    val qualifiedName = Ref.QualifiedName(mod, consName)
    val tyConName = Ref.Identifier(pkg, qualifiedName)
    protected def dataCons: Ast.DataCons
    def definition =
      consName -> Ast.DDataType(
        serializable = false,
        params = ImmArray.empty,
        cons = dataCons,
      )
  }

  abstract class DataTypeEnum(
      pkg: Ref.PackageId,
      mod: Ref.ModuleName,
      consName: Ref.DottedName,
  ) extends DataType(pkg, mod, consName) {
    protected def valuesNames: List[Ref.Name]
    val Values: List[Ast.EEnumCon] = valuesNames.map(Ast.EEnumCon(tyConName, _))

    override protected def dataCons =
      Ast.DataEnum(valuesNames.to(ImmArray))
  }

  object RoundingMode
      extends DataTypeEnum(
        lf_stdlib_1_dev,
        lf_math_mod,
        Ref.DottedName.assertFromString("RoundingMode"),
      ) {
    override protected def valuesNames: List[Ref.Name] =
      List(
        "Up",
        "Down",
        "Ceiling",
        "Floor",
        "HalfUp",
        "HalfDown",
        "HalfEven",
        "Unnecessary",
      ).map(Ref.Name.assertFromString)

    val List(up, down, ceiling, floor, halfUp, halfDown, halfEven, unnecessary) = Values
  }

  val vDev = {
    val mathMod = Ast.Module(
      name = lf_math_mod,
      definitions = List(RoundingMode.definition),
      templates = List.empty,
      exceptions = List.empty,
      featureFlags = Ast.FeatureFlags(true),
    )
    Ast.Package(
      modules = Map(mathMod.name -> mathMod),
      directDeps = Set.empty[Ref.PackageId],
      languageVersion = LanguageVersion.v1_dev,
      metadata = None,
    )
  }

  val v1_7 = {
    """


      module {
        record @serializable Wrapper1 (n1: nat) (a: *) =
          { zero1: Numeric n1, x: a };
        record @serializable Wrapper2 (n1: nat) (n2: nat) (a: *) =
          { zero1: Numeric n1, zero2: Numeric n2, x: a };
        record @serializable Wrapper3 (n1: nat) (n2: nat) (n3:nat) (a: *) =
          { zero1: Numeric n1, zero2: Numeric n2, zero2: Numeric n3, x: a };

        record R (n: nat) = {...}

        (R @n { ... })

        if n is now
          Wrapper @n (R @n) {scale1 = Zero_n, R @n {...}}




        val consR: forall (n: nat). Numeric n -> Wrapper @n (R @n) =
          /\ (a: nat). \(n: Numeric n) -> Wrapper @n (R @n) { scale = n, x =

      """

  }

  val packages: Map[Ref.PackageId, Ast.Package] =
    Map(lf_stdlib_1_dev -> vDev)
  val signatures: Map[Ref.PackageId, Ast.PackageSignature] =
    Util.toSignatures(packages)

}
