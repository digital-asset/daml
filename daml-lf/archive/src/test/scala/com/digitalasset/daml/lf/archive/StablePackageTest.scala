package com.daml.lf
package archive

import java.io.File
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.StablePackage
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StablePackageTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with BazelRunfiles
    with Inspectors
    with TryValues {

  private def resource(path: String): File = {
    val f = new File(path).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  "stable packages" should {

    lazy val darFile = resource(rlocation("daml-lf/archive/DarReaderTest.dar"))
    lazy val pkgsInDar = UniversalArchiveDecoder.assertReadFile(darFile).all.toMap

    // fix as new packages are added to the std lib
    assert(pkgsInDar.size == StablePackage.values.size + 3)

    "be in any dar file" in {
      val pkgIdsInDar = pkgsInDar.keySet
      StablePackage.values.foreach(pkg => pkgIdsInDar should contain(pkg.packageId))
    }

    "have te proper version" in {
      StablePackage.values.foreach(pkg =>
        pkg.languageVersion shouldBe pkgsInDar(pkg.packageId).languageVersion
      )
    }

  }

}
