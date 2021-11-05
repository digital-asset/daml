package com.daml.platform

import io.circe.{Json, JsonObject, ParsingFailure, yaml}
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Service(name: String, method: String) {
  def mkString: String = s"$name.$method"
}

object Service {
  def apply(name: String, methods: Seq[String]): Seq[Service] = {
    for {
      m <- methods
    } yield Service(name, m)
  }

}

case class Change0(v1_grpc_codes: List[String], v2_grpc_codes: List[String], comments: Option[String]) {
//  def mkString: String = {
//    val remarks = ""
//    val v1 = v1_line_string
//    val v2 = v2_line_string
//    s"""$v1 -> $v2$remarks""".stripMargin
//  }

  def v1_line_string: String = v1_grpc_codes.mkString(", ")

  def v2_line_string: String = v2_grpc_codes.mkString(", ")

}

case class Change(service: Service, changes: List[Change0]){

  def withComment(s: String): Change = {
    val changes2 = changes.map(_.copy(comments = Some(s)))
    copy(changes = changes2)
  }

}

object Change {
  def apply(service: Service, v1_grpc_code: String, v2_grpc_code: String, comment: Option[String]): Change = {
    val c0 = Change0(List(v1_grpc_code), List(v2_grpc_code), comment)
    new Change(service, List(c0))
  }
}

object PbatkoErrorsYml {
  def main(args: Array[String]): Unit = {
    val cs1: Seq[Change] = bar_submit()
    val cs2: Seq[Change] = foo_errors()
    val all = (cs1 ++ cs2).distinct

    val serviceToChanges: mutable.Map[Service, ArrayBuffer[Change0]] = mutable.Map.empty[Service, ArrayBuffer[Change0]]

    all.foreach { change
    =>
      if (!serviceToChanges.contains(change.service)) {
        serviceToChanges.put(change.service, new ArrayBuffer[Change0])
      }
      serviceToChanges.apply(change.service).addAll(change.changes)
    }

    val all2: Seq[(Service, ArrayBuffer[Change0])] = serviceToChanges.toList


    val csvLines: Seq[String] = for {
      (s, changes) <- all2
      serviceName = s.mkString
      c <- changes
    } yield {
      s"$serviceName|${c.v1_line_string}|${c.v2_line_string}|${c.comments.getOrElse("").replace('\n',' ')}"
    }

    val csvLinesSorted = csvLines.distinct.sorted

    println("==============================")
    println("==============================")
    println("==============================")
    println(csvLinesSorted.mkString("\n"))
  }

  def bar_submit(): Seq[Change] = {
    val json: JsonObject = readJsonObject("pbatko/submit_pbatko.yml")
    val services = parseServices(json("service").get)
    val changes = parseChanges(json("changes").get)
    //    println(services)
    //    println(changes)
    //    println("111")
    for {
      s <- services
    } yield
      Change(
        service = s, changes = changes.toList
      )
  }

  def parseChanges(json: Json): Seq[Change0] = {
    val rawChanges: Seq[JsonObject] = json.asArray.get.map(_.asObject.get)
    rawChanges.map(parseChange)
  }

  def parseChange(json: JsonObject): Change0 = {
    val desc = json("desc").get.asString.get
    val fromRaw = json("from").get.asArray.get
    val toRaw = json("to").get.asArray.get

    val from_codes: Seq[String] = parseFromOrTo(fromRaw).map(_._1)
    val to = parseFromOrTo(toRaw)
    val to_codes = to.map(_._1)
    val comments = to.map { case (code, comment) => s"$code: $comment" }.mkString("\n")
    val comment: String = {
      s"""$desc
         |
         |$comments
         |""".stripMargin
    }

    Change0(
      v1_grpc_codes = from_codes.toList,
      v2_grpc_codes = to_codes.toList,
      comments = Some(comment)
    )
  }

  def parseFromOrTo(raw: Vector[Json]): Seq[(String, String)] = {
    raw.map {
      case json if json.isString => (json.asString.get, "")
      case json if json.isObject => {
        val h = json.asObject.get.toList.head
        (h._1, h._2.asString.get)
      }
      case x => throw new Exception(x.toString())
    }
  }

  def parseServices(json: Json): Seq[Service] = {
    val x: Seq[(String, Json)] = json.asObject.get.toList
    val name = x.head._1
    val methods: Seq[String] = x.head._2.asArray.get.map(_.asString.get)
    Service(name, methods)
  }

  def foo_errors(): Seq[Change] = {
    val x: JsonObject = readJsonObject("pbatko/errors_pbatko.yml")
    val changes: mutable.Map[Change, ArrayBuffer[String]] = mutable.Map.empty[Change, ArrayBuffer[String]]

    x.toList.foreach { case (errorFactoriesMethodName, value) =>

      val changeArray = value.asObject.get.apply("change").get.asArray.get
      val List(fromRaw, toRaw) = changeArray.toList
      val from = fromRaw.asString.get
      val to = toRaw.asString.getOrElse(throw new Exception(s"Change array: |$changeArray|"))
      val servicesRaw: Seq[(String, Json)] = value.asObject.get.toList.filter { case (key, _) => key != "change" }

      val services: Seq[Service] = servicesRaw.flatMap { case (serviceName, methodsRaw) =>
        val methods = methodsRaw.asArray.get.map(_.asString.get).toList
        val services = for (m <- methods)
          yield Service(name = serviceName, method = m)
        services
      }

      for {
        s <- services
      } {
        val c = Change(service = s, v1_grpc_code = from, v2_grpc_code = to, comment = None)
        if (!changes.contains(c)){
          changes.put(c, new ArrayBuffer[String]())
        }
        changes.apply(c).addOne(errorFactoriesMethodName)
      }
    }
    //    for {
    //      x <- json.asArray.get
    //    }
    //
    //    json.as

    changes.map{
      case (c: Change, comments) =>
        c.withComment(comments.distinct.mkString(" "))
    }.toSeq
  }

  private def readJsonObject(resourcePath: String): JsonObject = {
    val in = getClass.getClassLoader.getResourceAsStream(resourcePath);
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
    x
  }
}
