package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.data.ScalazEqual
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.VersionTimeline.SubVersion
import scalaz.Equal
import scalaz.syntax.equal._

final case class Versioned[Version, +X](version: Version, x: X) {

  /** Increase the `version` if appropriate for `languageVersions`. */
  def typedBy(languageVersions: LanguageVersion*)(
      implicit A: SubVersion[Version]): Versioned[Version, X] = {
    import com.digitalasset.daml.lf.transaction.VersionTimeline
    import VersionTimeline._
    import Implicits._
    copy(
      version = latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*))
  }
}

object Versioned {
  implicit def `Versioned Equal instance`[Version, Val: Equal]: Equal[Versioned[Version, Val]] =
    ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
      import a._
      val Versioned(bVersion, bX) = b
      version == bVersion && x === bX
    }
}
