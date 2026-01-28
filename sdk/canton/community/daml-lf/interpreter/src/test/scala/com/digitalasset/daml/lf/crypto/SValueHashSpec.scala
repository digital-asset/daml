// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Numeric, Ref, Time}
import com.digitalasset.daml.lf.speedy.{SValue => SV}
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq
import scala.io.Source
import scala.language.implicitConversions
import scala.util.Using

class SValueHashSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  private val packageId0 = Ref.PackageId.assertFromString("package")
  private val packageId1 = Ref.PackageId.assertFromString("package-1")
  private val packageName0 = Ref.PackageName.assertFromString("package-name-0")
  private val packageName1 = Ref.PackageName.assertFromString("package-name-1")

  private def assertHashContractKey(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      key: SV,
  ): Hash =
    SValueHash.assertHashContractKey(packageName, templateName, key)

  private def hashContractKey(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      key: SV,
  ): Either[Hash.HashingError, Hash] =
    SValueHash.hashContractKey(packageName, templateName, key)

  private def assertHashContractInstance(
      packageName: Ref.PackageName,
      templateName: Ref.QualifiedName,
      key: SV,
  ): Hash =
    SValueHash.assertHashContractInstance(packageName, templateName: Ref.QualifiedName, key: SV)

  /** A complex record value whose expected hashes are [[expectedComplexRecordContractHash]] and
    * [[expectedComplexRecordKeyHash]]
    */
  private val complexRecord: SV =
    sRecord(
      defRef(name = "ComplexRecord"),
      List(
        "fInt0" -> SV.SInt64(0L),
        "fInt1" -> SV.SInt64(123456L),
        "fInt2" -> SV.SInt64(-1L),
        "fNumeric0" -> SV.SNumeric(Numeric.assertFromString("0.0000000000")),
        "fNumeric1" -> SV.SNumeric(Numeric.assertFromString("0.3333333333")),
        "fBool0" -> SV.SBool(true),
        "fBool1" -> SV.SBool(false),
        "fDate0" -> SV.SDate(Time.Date.assertFromDaysSinceEpoch(0)),
        "fDate1" -> SV.SDate(Time.Date.assertFromDaysSinceEpoch(123456)),
        "fTime0" -> SV.STimestamp(Time.Timestamp.assertFromLong(0)),
        "fTime1" -> SV.STimestamp(Time.Timestamp.assertFromLong(123456)),
        "fText0" -> SV.SText(""),
        "fText1" -> SV.SText("abcd-äöü€"),
        "fParty" -> SV.SParty(Ref.Party.assertFromString("Alice")),
        "fUnit" -> SV.SUnit,
        "fOpt0" -> SV.SOptional(None),
        "fOpt1" -> SV.SOptional(Some(SV.SText("Some"))),
        "fList" -> SV.SList(FrontStack("A", "B", "C").map(SV.SText)),
        "fVariant" -> SV.SVariant(
          defRef(name = "Variant"),
          Ref.Name.assertFromString("VariantCons"),
          0,
          SV.SInt64(0L),
        ),
        "fRecord" -> sRecord(
          defRef(name = "Record"),
          List(
            "field1" -> SV.SText("field1"),
            "field2" -> SV.SText("field2"),
          ),
        ),
        "fTextMap" -> SV.SMap(
          isTextMap = true,
          SV.SText("keyA") -> SV.SInt64(0L),
          SV.SText("keyB") -> SV.SInt64(1L),
        ),
        "fGenMap" -> SV.SMap(
          isTextMap = false,
          SV.SText("keyA") -> SV.SInt64(0L),
          SV.SText("keyB") -> SV.SInt64(1L),
        ),
      ),
    )

  /** The expected hash of [[complexRecord]] for the contract hasher. */
  private val expectedComplexRecordContractHash =
    "f6bd0206e6114de43279d53641159d223e9c6f28c2c1bfe65e0bcb2e3a567979"

  /** The expected hash of [[complexRecord]] for the key hasher */
  private val expectedComplexRecordKeyHash =
    "115251e29818e522e262fcaa70a1ca3c92c3102697b6661d0138bf5841ae1adb"

  /** A list of values whose expected hashes are recorded in expected-stability-values-contract-hashes.txt and
    * expected-stability-values-key-hashes.txt
    */
  private val stabilityValues: List[SV] = {
    val pkgId = Ref.PackageId.assertFromString("pkgId")

    implicit def toTypeConId(s: String): Ref.TypeConId =
      Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

    implicit def toName(s: String): Ref.Name =
      Ref.Name.assertFromString(s)

    val units = List(SV.SUnit)
    val bools = List(true, false).map(SV.SBool(_))
    val ints = List(-1L, 0L, 1L).map(SV.SInt64(_))
    val numeric10s = List("-10000.0000000000", "0.0000000000", "10000.0000000000")
      .map(Numeric.assertFromString)
      .map(SV.SNumeric(_))
    val numeric0s = List("-10000.", "0.", "10000.")
      .map(Numeric.assertFromString)
      .map(SV.SNumeric(_))
    val texts = List("", "someText", "a¶‱😂").map(SV.SText(_))
    val dates =
      List(
        Time.Date.assertFromDaysSinceEpoch(0),
        Time.Date.assertFromString("1969-07-21"),
        Time.Date.assertFromString("2019-12-16"),
      ).map(SV.SDate(_))
    val timestamps =
      List(
        Time.Timestamp.assertFromLong(0),
        Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z"),
        Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z"),
      ).map(SV.STimestamp(_))
    val parties =
      List(
        Ref.Party.assertFromString("alice"),
        Ref.Party.assertFromString("bob"),
      ).map(SV.SParty(_))

    val enums = List(
      SV.SEnum("Color", "Red", 0),
      SV.SEnum("Color", "Green", 1),
      SV.SEnum("ColorBis", "Green", 1),
    )

    val records0 =
      List(
        sRecord(toTypeConId("Unit"), List.empty),
        sRecord(toTypeConId("UnitBis"), List.empty),
      )
    val records2 =
      List(
        sRecord("Tuple", List("_1" -> SV.SBool(false), "_2" -> SV.SBool(false))),
        sRecord("Tuple", List("_1" -> SV.SBool(true), "_2" -> SV.SBool(false))),
        sRecord("Tuple", List("_1" -> SV.SBool(false), "_2" -> SV.SBool(true))),
        sRecord("TupleBis", List("_1" -> SV.SBool(false), "_2" -> SV.SBool(false))),
      )

    val variants = List(
      SV.SVariant("Either", "Left", 0, SV.SBool(false)),
      SV.SVariant("Either", "Left", 0, SV.SBool(true)),
      SV.SVariant("Either", "Right", 1, SV.SBool(false)),
      SV.SVariant("EitherBis", "Left", 0, SV.SBool(false)),
    )

    def list(elements: Boolean*) = sList(elements.map(SV.SBool(_)))

    val lists = List(
      list(),
      list(false),
      list(true),
      list(false, false),
      list(false, true),
      list(true, false),
    )

    def textMap(entries: (String, Boolean)*) =
      SV.SMap(
        isTextMap = true,
        entries.map { case (k, v) => SV.SText(k) -> SV.SBool(v) },
      )

    val textMaps = List[SV](
      textMap(),
      textMap("a" -> false),
      textMap("a" -> true),
      textMap("b" -> false),
      textMap("a" -> false, "b" -> false),
      textMap("a" -> true, "b" -> false),
      textMap("a" -> false, "b" -> true),
      textMap("a" -> false, "c" -> false),
    )

    def genMap(entries: (String, Boolean)*) =
      SV.SMap(
        isTextMap = false,
        entries.map { case (k, v) => SV.SText(k) -> SV.SBool(v) },
      )

    val genMaps = List[SV](
      genMap(),
      genMap("a" -> false),
      genMap("a" -> true),
      genMap("b" -> false),
      genMap("a" -> false, "b" -> false),
      genMap("a" -> true, "b" -> false),
      genMap("a" -> false, "b" -> true),
      genMap("a" -> false, "c" -> false),
    )

    val optionals0 =
      List(
        SV.SOptional(None),
        SV.SOptional(Some(SV.SBool(false))),
        SV.SOptional(Some(SV.SBool(true))),
      )
    val optionals1 =
      List(
        SV.SOptional(Some(SV.SOptional(None))),
        SV.SOptional(Some(SV.SOptional(Some(SV.SBool(false))))),
      )
    val optionals = optionals0 ++ optionals1

    List.concat(
      units,
      bools,
      ints,
      numeric10s,
      numeric0s,
      dates,
      timestamps,
      texts,
      parties,
      optionals,
      lists,
      textMaps,
      genMaps,
      enums,
      records0,
      records2,
      variants,
    )
  }

  /** A list of values whose expected hashes are recorded in expected-stability-values-key-hashes.txt */
  private val contractIdValues: List[SV] =
    List(
      "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5",
      "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b",
    ).map(ContractId.assertFromString).map(SV.SContractId(_))

  /** A table listing ways an SValue can be nested inside another SValue. */
  private val nestingCases: TableFor1[SV => SV] = {
    def notNested(sv: SV) = sv

    def nestedInsideRecord(sv: SV) =
      sRecord(
        defRef(name = "Wrapper"),
        List(
          "f1" -> sv,
          "f2" -> sv,
        ),
      )

    def nestedInsideList(sv: SV) =
      sList(sv, sv)

    def nestedInsideVariant(sv: SV) =
      SV.SVariant(defRef(name = "VariantWrapper"), Ref.Name.assertFromString("Cons"), 0, sv)

    def nestedInsideTextMap(sv: SV) =
      SV.SMap(isTextMap = true, Map(SV.SText("key1") -> sv, SV.SText("key2") -> sv))

    def nestedInsideMap(sv: SV) =
      SV.SMap(isTextMap = false, Map(SV.SInt64(0) -> sv, SV.SInt64(1) -> sv))

    Table(
      "nesting function",
      notNested,
      nestedInsideRecord,
      nestedInsideList,
      nestedInsideVariant,
      nestedInsideTextMap,
      nestedInsideMap,
    )
  }

  /** Loads the contents of a resource into a [[String]].
    */
  private def readGoldenFile(fileName: String): String =
    Using(
      Source.fromFile(
        getClass.getClassLoader.getResource(fileName).toURI,
        "UTF-8",
      )
    )(_.getLines().mkString(System.lineSeparator()))
      .getOrElse(throw new RuntimeException(s"Could not read $fileName"))

  // Tests that only apply to the contract hasher
  "contract hasher" should {
    "be stable on contract IDs" in {
      val sep = System.lineSeparator()
      val actualOutput = contractIdValues
        .map { value =>
          val hash = assertHashContractInstance(
            packageName0,
            defQualName("module", "name"),
            value,
          ).toHexString
          s"${value.toString}$sep$hash"
        }
        .mkString(sep)
      actualOutput shouldBe readGoldenFile("expected-stability-contract-id-hashes.txt")
    }
  }

  // Tests that only apply to the key hasher
  "key hasher" should {
    "reject contract IDs" in {
      forEvery(nestingCases) { nest =>
        forEvery(Table("contract id", contractIdValues: _*)) { value =>
          inside(hashContractKey(packageName0, defQualName("module", "name"), nest(value))) {
            case Left(error) =>
              error shouldBe a[Hash.HashingError.ForbiddenContractId]
          }
        }
      }
    }
  }

  // Tests that apply to both the contract hasher and the key hasher
  for (
    (
      hasherName,
      assertHashWithPackageName,
      expectedComplexRecordHash,
      expectedStabilityValuesHashes,
    ) <- List(
      (
        "contract hasher",
        assertHashContractInstance _,
        expectedComplexRecordContractHash,
        readGoldenFile("expected-stability-values-contract-hashes.txt"),
      ),
      (
        "key hasher",
        assertHashContractKey _,
        expectedComplexRecordKeyHash,
        readGoldenFile("expected-stability-values-key-hashes.txt"),
      ),
    )
  ) {

    // Most test cases are not sensitive to the package name, so we declare this shorthand.
    def assertHash(
        templateName: Ref.QualifiedName,
        value: SV,
    ): Hash =
      assertHashWithPackageName(packageName0, templateName, value)

    hasherName should {

      "be stable on complex record" in {
        assertHash(
          defQualName("Module", "Template"),
          complexRecord,
        ).toHexString shouldBe expectedComplexRecordHash
      }

      "be stable on all types of values " in {
        val sep = System.lineSeparator()
        val actualOutput = stabilityValues
          .map { value =>
            val hash = assertHash(defQualName("module", "name"), value).toHexString
            s"${value.toString}$sep$hash"
          }
          .mkString(sep)
        actualOutput shouldBe expectedStabilityValuesHashes
      }

      "be deterministic and thread safe" in {
        // Compute many hashes in parallel, check that they are all equal
        val hashes = Vector
          .fill(1000)(defQualName("Module", "Template") -> complexRecord)
          .map(Function.tupled(assertHash))

        hashes.toSet.size shouldBe 1
      }

      "not produce collision in template name" in {
        // Same value but different template ID should produce a different hash
        val value = SV.SText("A")

        val hash1 = assertHash(defQualName("AA", "A"), value)
        val hash2 = assertHash(defQualName("A", "AA"), value)

        hash1 should !==(hash2)
      }

      "not produce collision in list of texts" in {
        // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
        val value1 = sList(SV.SText("AA"), SV.SText("A"))
        val value2 = sList(SV.SText("A"), SV.SText("AA"))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in list of decimals" in {
        // Testing whether numeric are delimited: [10, 10] vs [101, 0]
        def list(elements: String*) =
          sList(elements.map(e => SV.SNumeric(Numeric.assertFromString(e))))

        val value1 = list("10.0000000000", "10.0000000000")
        val value2 = list("101.0000000000", "0.0000000000")

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in list of lists" in {
        // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
        def list(elements: Vector[Unit]*) =
          sList(elements.map(e => sList(e.map(_ => SV.SUnit))))

        val value1 = list(Vector(()), Vector((), ()))
        val value2 = list(Vector((), ()), Vector(()))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in list of records (1)" in {
        val value1 = sList(
          sRecord(
            defRef(name = "Tuple2"),
            List("_1" -> SV.SText("A"), "_2" -> SV.SText("B")),
          ),
          sRecord(
            defRef(name = "Tuple2"),
            List("_1" -> SV.SText(""), "_2" -> SV.SText("")),
          ),
        )
        val value2 = sList(
          sRecord(
            defRef(name = "Tuple2"),
            List("_1" -> SV.SText("A"), "_2" -> SV.SText("")),
          ),
          sRecord(
            defRef(name = "Tuple2"),
            List("_1" -> SV.SText(""), "_2" -> SV.SText("B")),
          ),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in list of records (2)" in {
        val emptyRecord = sRecord(defRef(name = "Record"), List.empty)
        val nonEmptyRecord = sRecord(defRef(name = "Record"), List("i" -> SV.SInt64(1L)))
        val value1 = sList(emptyRecord, nonEmptyRecord)
        val value2 = sList(nonEmptyRecord, emptyRecord)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Variant constructor" in {
        val value1 =
          SV.SVariant(defRef(name = "Variant"), Ref.Name.assertFromString("A"), 0, SV.SUnit)
        val value2 =
          SV.SVariant(defRef(name = "Variant"), Ref.Name.assertFromString("B"), 1, SV.SUnit)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Variant value" in {
        val value1 =
          SV.SVariant(defRef(name = "Variant"), Ref.Name.assertFromString("A"), 0, SV.SInt64(0L))
        val value2 =
          SV.SVariant(defRef(name = "Variant"), Ref.Name.assertFromString("A"), 0, SV.SInt64(1L))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in TextMap keys" in {
        val value1 = SV.SMap(
          isTextMap = true,
          SV.SText("A") -> SV.SInt64(0L),
          SV.SText("B") -> SV.SInt64(0L),
        )
        val value2 = SV.SMap(
          isTextMap = true,
          SV.SText("A") -> SV.SInt64(0L),
          SV.SText("C") -> SV.SInt64(0L),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in TextMap values" in {
        val value1 = SV.SMap(
          isTextMap = true,
          SV.SText("A") -> SV.SInt64(0L),
          SV.SText("B") -> SV.SInt64(0L),
        )
        val value2 = SV.SMap(
          isTextMap = true,
          SV.SText("A") -> SV.SInt64(0L),
          SV.SText("B") -> SV.SInt64(1L),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in GenMap keys" in {
        def genMap(elements: (String, Long)*) =
          SV.SMap(isTextMap = false, elements.map { case (k, v) => SV.SText(k) -> SV.SInt64(v) })

        val value1 = genMap("A" -> 0, "B" -> 0)
        val value2 = genMap("A" -> 0, "C" -> 0)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in GenMap values" in {
        def genMap(elements: (String, Long)*) =
          SV.SMap(isTextMap = false, elements.map { case (k, v) => SV.SText(k) -> SV.SInt64(v) })

        val value1 = genMap("A" -> 0, "B" -> 0)
        val value2 = genMap("A" -> 0, "B" -> 1)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Bool" in {
        val value1 = SV.SBool(true)
        val value2 = SV.SBool(false)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Int64" in {
        val value1 = SV.SInt64(0L)
        val value2 = SV.SInt64(1L)

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Numeric" in {
        val value1 = SV.SNumeric(Numeric.assertFromString("0."))
        val value2 = SV.SNumeric(Numeric.assertFromString("1."))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Date" in {
        val value1 = SV.SDate(Time.Date.assertFromDaysSinceEpoch(0))
        val value2 = SV.SDate(Time.Date.assertFromDaysSinceEpoch(1))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Timestamp" in {
        val value1 = SV.STimestamp(Time.Timestamp.assertFromLong(0))
        val value2 = SV.STimestamp(Time.Timestamp.assertFromLong(1))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Optional" in {
        val value1 = SV.SOptional(None)
        val value2 = SV.SOptional(Some(SV.SUnit))

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "not produce collision in Record" in {
        val value1 = sRecord(
          defRef(name = "Tuple2"),
          List("_1" -> SV.SText("A"), "_2" -> SV.SText("B")),
        )
        val value2 = sRecord(
          defRef(name = "Tuple2"),
          List("_1" -> SV.SText("A"), "_2" -> SV.SText("C")),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, value1)
        val hash2 = assertHash(templateName, value2)

        hash1 should !==(hash2)
      }

      "ignore trailing Nones in records" in {
        val record1 = sRecord(
          defRef(name = "Record"),
          List(
            "a" -> SV.SOptional(None),
            "b" -> SV.SOptional(Some(SV.SInt64(1))),
          ),
        )
        val record2 = sRecord(
          defRef(name = "Record"),
          List(
            "a" -> SV.SOptional(None),
            "b" -> SV.SOptional(Some(SV.SInt64(1))),
            "c" -> SV.SOptional(None),
          ),
        )
        val record3 = sRecord(
          defRef(name = "Record"),
          List(
            "a" -> SV.SOptional(None),
            "b" -> SV.SOptional(Some(SV.SInt64(1))),
            "c" -> SV.SOptional(None),
            "d" -> SV.SOptional(None),
          ),
        )

        forEvery(nestingCases) { nester =>
          val nRecord1 = nester(record1)
          val nRecord2 = nester(record2)
          val nRecord3 = nester(record3)

          Set(nRecord1, nRecord2, nRecord3)
            .map(assertHash(defQualName("module", "name"), _))
            .size shouldBe 1
        }
      }

      "not identify values with different package names" in {
        val sval = SV.SInt64(0L)
        val templateName = defQualName("module", "name")

        val hash1 = assertHashWithPackageName(packageName0, templateName, sval)
        val hash2 = assertHashWithPackageName(packageName1, templateName, sval)

        hash1 should !==(hash2)
      }

      "not identify values with different template names" in {
        val sval = SV.SInt64(0L)

        val hash1 = assertHash(defQualName("module", "name1"), sval)
        val hash2 = assertHash(defQualName("module", "name2"), sval)

        hash1 should !==(hash2)
      }

      "identify records with different package IDs but otherwise same package, template, and qualified names" in {
        val record1 = sRecord(
          Ref.Identifier(
            packageId0,
            defQualName("module", "Record"),
          ),
          List.empty,
        )
        val record2 = sRecord(
          Ref.Identifier(
            packageId1,
            defQualName("module", "Record"),
          ),
          List.empty,
        )
        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, record1)
        val hash2 = assertHash(templateName, record2)

        hash1 shouldBe hash2
      }

      "not identify records with different template names" in {
        val record = sRecord(
          defRef(name = "Record"),
          List.empty,
        )

        val hash1 = assertHash(defQualName("module", "name1"), record)
        val hash2 = assertHash(defQualName("module", "name2"), record)

        hash1 should !==(hash2)
      }

      "not identify records with different record names" in {
        val record1 = sRecord(
          defRef(name = "Record1"),
          List.empty,
        )
        val record2 = sRecord(
          defRef(name = "Record2"),
          List.empty,
        )
        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, record1)
        val hash2 = assertHash(templateName, record2)

        hash1 should !==(hash2)
      }

      "not identify records with different labels" in {
        val record1 = sRecord(
          defRef(name = "Record"),
          List("a" -> SV.SInt64(0L)),
        )
        val record2 = sRecord(
          defRef(name = "Record"),
          List("b" -> SV.SInt64(0L)),
        )
        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, record1)
        val hash2 = assertHash(templateName, record2)

        hash1 should !==(hash2)
      }

      "identify variants with different package IDs but otherwise same package, template, and qualified names" in {
        val variant1 = SV.SVariant(
          Ref.Identifier(
            packageId0,
            defQualName("module", "Variant"),
          ),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )
        val variant2 = SV.SVariant(
          Ref.Identifier(
            packageId1,
            defQualName("module", "Variant"),
          ),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )
        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, variant1)
        val hash2 = assertHash(templateName, variant2)

        hash1 shouldBe hash2
      }

      "not identify variants with different template names" in {
        val record = SV.SVariant(
          defRef(name = "Variant"),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )

        val hash1 = assertHash(defQualName("module", "name1"), record)
        val hash2 = assertHash(defQualName("module", "name2"), record)

        hash1 should !==(hash2)
      }

      "not identify variants with different variant names" in {
        val variant1 = SV.SVariant(
          defRef(name = "Variant1"),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )
        val variant2 = SV.SVariant(
          defRef(name = "Variant2"),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, variant1)
        val hash2 = assertHash(templateName, variant2)

        hash1 should !==(hash2)
      }

      "not identify variants constructors with different ranks" in {
        val variant1 = SV.SVariant(
          defRef(name = "Variant"),
          Ref.Name.assertFromString("Cons"),
          0,
          SV.SInt64(0L),
        )
        val variant2 = SV.SVariant(
          defRef(name = "Variant"),
          Ref.Name.assertFromString("Cons"),
          1,
          SV.SInt64(0L),
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, variant1)
        val hash2 = assertHash(templateName, variant2)

        hash1 should !==(hash2)
      }

      "identify enums with different package IDs but otherwise same package, template, and qualified names" in {
        val enum1 = SV.SEnum(
          Ref.Identifier(
            packageId0,
            defQualName("module", "Enum"),
          ),
          Ref.Name.assertFromString("Cons"),
          0,
        )
        val enum2 = SV.SEnum(
          Ref.Identifier(
            packageId1,
            defQualName("module", "Enum"),
          ),
          Ref.Name.assertFromString("Cons"),
          0,
        )
        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, enum1)
        val hash2 = assertHash(templateName, enum2)

        hash1 shouldBe hash2
      }

      "not identify enums with different template names" in {
        val record = SV.SEnum(
          defRef(name = "Enum"),
          Ref.Name.assertFromString("Cons"),
          0,
        )

        val hash1 = assertHash(defQualName("module", "name1"), record)
        val hash2 = assertHash(defQualName("module", "name2"), record)

        hash1 should !==(hash2)
      }

      "not identify enums with different enum names" in {
        val enum1 = SV.SEnum(
          defRef(name = "Enum1"),
          Ref.Name.assertFromString("Cons"),
          0,
        )
        val enum2 = SV.SEnum(
          defRef(name = "Enum2"),
          Ref.Name.assertFromString("Cons"),
          0,
        )

        val templateName = defQualName("module", "name")

        val hash1 = assertHash(templateName, enum1)
        val hash2 = assertHash(templateName, enum2)

        hash1 should !==(hash2)
      }

      "not identify values of different types" in {
        // These SValues would all hash to the same value if it weren't for the type tag
        val potentialCollisions = Table[List[SV]](
          "potential collisions",
          List(
            // without type tags, this hashes to [0]
            SV.SBool(false),
            // without type tags, this hashes to [0]
            SV.SOptional(None),
          ),
          List(
            // without type tags, this hashes to [0,0,0,4] ++ [0,0,0,0]
            SV.SList(
              FrontStack(SV.SBool(false), SV.SBool(false), SV.SBool(false), SV.SBool(false))
            ),
            // without type tags, this hashes to [0,0,0,4,0,0,0,0]
            SV.SInt64(4L * 256 * 256 * 256 * 256),
          ),
          List(
            // without type tags, this hashes to [0,0,0,0]
            SV.SList(FrontStack.empty),
            // without type tags, this hashes to [0,0,0,0]
            SV.SText(""),
          ),
        )
        forEvery(potentialCollisions) { values =>
          values.map(assertHash(defQualName("module", "name"), _)).toSet.size shouldBe values.size
        }
      }
    }
  }

  private def sRecord(id: Ref.Identifier, fields: Iterable[(String, SV)]): SV.SRecord =
    SV.SRecord(
      id,
      ImmArray.from(fields.map(f => Ref.Name.assertFromString(f._1))),
      ArraySeq.from(fields.map(_._2)),
    )

  private def sList(elements: SV*): SV.SList =
    SV.SList(FrontStack.from(elements))

  private def sList(elements: IterableOnce[SV]): SV.SList =
    SV.SList(FrontStack.from(elements))

  private def defRef(module: String = "Module", name: String): Ref.Identifier =
    Ref.Identifier(
      packageId0,
      defQualName(module, name),
    )

  private def defQualName(module: String, name: String): Ref.QualifiedName =
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString(module),
      Ref.DottedName.assertFromString(name),
    )
}
