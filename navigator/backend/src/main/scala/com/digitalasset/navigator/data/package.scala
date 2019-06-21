package com.digitalasset.navigator

import cats.data.NonEmptyList
import doobie.{Get, Put}
import org.postgresql.util.PGobject

package object data {

  implicit val getStrings: Get[Seq[String]] = {
    import spray.json._
    import DefaultJsonProtocol._
    Get.Advanced.other[PGobject](NonEmptyList.of("json")).map(_.getValue.parseJson.convertTo[Seq[String]])
  }

  implicit val putStrings: Put[Seq[String]] = {
    import spray.json._
    import DefaultJsonProtocol._
    Put.Advanced.other[PGobject](NonEmptyList.of("json")).contramap { strings =>
      val o = new PGobject()
      o.setType("json")
      o.setValue(strings.toJson.compactPrint)
      o
    }
  }

}
