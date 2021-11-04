package com.daml.platform

import io.circe.{Json, JsonObject, ParsingFailure, yaml}
import org.apache.commons.io.IOUtils

case class Service(name: String, method: String)

case class Change(service: Service, v1_grpc_code: String, v2_grpc_code: String)

object PbatkoErrorsYml {
  def main(args: Array[String]): Unit = {
    val in = getClass.getClassLoader.getResourceAsStream("pbatko/errors_pbatko.yml");
    import java.nio.charset.StandardCharsets
    val content: String = IOUtils.toString(in, StandardCharsets.UTF_8)
//    println(content)
    val jsonE: Either[ParsingFailure, Json] = for {
      json <- yaml.parser.parse(content)
    } yield json
    val json: Json = jsonE match {
      case Left(e) => throw e
      case Right(json) => json
    }
//    println(json)

    val x: JsonObject = json.asObject.get
    val changes: Seq[Change] = x.toList.flatMap { case (key, value) =>
      println(key)
      println(value)

      val changeArray = value.asObject.get.apply("change").get.asArray.get
      val List(fromRaw, toRaw) = changeArray.toList
      val from = fromRaw.asString.get
      val to = toRaw.asString.getOrElse(throw new Exception(s"Change array: |$changeArray|"))
      val servicesRaw: Seq[(String, Json)] = value.asObject.get.toList.filter{ case (key, _) => key != "change" }

      val services: Seq[Service] = servicesRaw.flatMap { case (serviceName, methodsRaw) =>
        val methods = methodsRaw.asArray.get.map(_.toString()).toList
        val services = for(m <- methods)
          yield Service(name = serviceName, method = m)
        services
      }

      for {
        s <- services
      } yield Change(service = s, v1_grpc_code = from, v2_grpc_code = to)
    }
//    for {
//      x <- json.asArray.get
//    }
//
//    json.as

    changes.foreach(println)
  }
}
