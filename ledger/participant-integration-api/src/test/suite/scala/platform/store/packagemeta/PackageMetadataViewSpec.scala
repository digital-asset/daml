package com.daml.platform.store.packagemeta

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.platform.store.packagemeta.PackageMetadataView.MetadataDefinitions
import com.daml.scalatest.FlatSpecCheckLaws
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.scalacheck.ScalazProperties
import com.daml.lf.value.test.ValueGenerators.idGen
import com.daml.platform.store.packagemeta.PackageMetadataViewSpec._
import org.scalatest.EitherValues

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import scala.util.Using

class PackageMetadataViewSpec
    extends AnyFlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with EitherValues {

  behavior of "MetadataDefinitions Monoid"
  checkLaws(ScalazProperties.monoid.laws[MetadataDefinitions])

  behavior of "MetadataDefinitions from"

  it should "handle archives" in new Scope {
    val darFile: File = new File(rlocation(ModelTestDar.path))
    val packages: List[DamlLf.Archive] = Using(new ZipInputStream(new FileInputStream(darFile))) {
      stream =>
        DarParser.readArchive("smth", stream)
    }.get.value.all
    val allDecodedPackages = packages.map(MetadataDefinitions.from)
    allDecodedPackages.exists(_.templates.nonEmpty) shouldBe true
    allDecodedPackages.exists(_.interfaces.nonEmpty) shouldBe true
    allDecodedPackages.exists(_.interfacesImplementedBy.nonEmpty) shouldBe true
  }

  behavior of "PackageMetadataView"

  it should "noop in case of empty MetadataDefinitions" in new Scope {
    val view = PackageMetadataView.create
    val read1: PackageMetadataView.PackageMetadata = view.read()
    read1.templateExists(template1) shouldBe false
    read1.interfaceExists(iface1) shouldBe false
    read1.interfaceImplementedBy(iface1) shouldBe Set.empty
    view.update(MetadataDefinitions())
    val read2: PackageMetadataView.PackageMetadata = view.read()
    read2.templateExists(template1) shouldBe false
    read2.interfaceExists(iface1) shouldBe false
    read2.interfaceImplementedBy(iface1) shouldBe Set.empty
  }

  it should "update exposed values by adding interfaces and templates" in new Scope {
    val view = PackageMetadataView.create
    val read1: PackageMetadataView.PackageMetadata = view.read()
    read1.templateExists(template1) shouldBe false
    read1.interfaceExists(iface1) shouldBe false
    read1.interfaceImplementedBy(iface1) shouldBe Set.empty

    view.update(MetadataDefinitions(templates = Set(template1)))
    val read2: PackageMetadataView.PackageMetadata = view.read()
    read2.templateExists(template1) shouldBe true
    read2.interfaceExists(iface1) shouldBe false
    read2.interfaceImplementedBy(iface1) shouldBe Set.empty

    view.update(MetadataDefinitions(interfaces = Set(iface1)))
    val read3: PackageMetadataView.PackageMetadata = view.read()
    read3.templateExists(template1) shouldBe true
    read3.interfaceExists(iface1) shouldBe true
    read3.interfaceImplementedBy(iface1) shouldBe Set.empty

    view.update(MetadataDefinitions(templates = Set(template2)))
    val read4: PackageMetadataView.PackageMetadata = view.read()
    read4.templateExists(template1) shouldBe true
    read4.templateExists(template2) shouldBe true
    read4.interfaceExists(iface1) shouldBe true

    view.update(
      MetadataDefinitions(interfacesImplementedBy = Map(iface1 -> Set(template1, template2)))
    )
    val read5: PackageMetadataView.PackageMetadata = view.read()
    read5.interfaceImplementedBy(iface1) shouldBe Set(template1, template2)
  }

}

object PackageMetadataViewSpec {
  private def entry: Gen[(Ref.Identifier, Set[Ref.Identifier])] =
    for {
      key <- idGen
      values <- Gen.listOf(idGen)
    } yield (key, values.toSet)

  private def interfacesImplementedByMap: Gen[Map[Ref.Identifier, Set[Ref.Identifier]]] =
    for {
      entries <- Gen.listOf(entry)
    } yield entries.toMap

  private val definitionGen = for {
    map <- interfacesImplementedByMap
  } yield MetadataDefinitions(map.keySet, map.values.flatten.toSet, map)

  implicit def definitionArb: Arbitrary[MetadataDefinitions] = Arbitrary(definitionGen)

  trait Scope {
    val template1 = Ref.Identifier.assertFromString("PackageName:ModuleName:template1")
    val template2 = Ref.Identifier.assertFromString("PackageName:ModuleName:template2")
    val iface1 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface1")
    val iface2 = Ref.Identifier.assertFromString("PackageName:ModuleName:iface2")
  }
}
