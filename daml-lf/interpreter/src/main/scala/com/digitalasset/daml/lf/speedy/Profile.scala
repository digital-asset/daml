// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import collection.JavaConverters._
import java.lang.System
import java.nio.file.{Files, Path}
import java.util.ArrayList

/** Class for profiling information collected by Speedy. We use [[AnyRef]] for
  * the labels to avoid any conversions into a common label format at runtime
  * since this might skew the profile. Instead, we convert the labels to strings
  * when we write out the profile.
  */
final class Profile {
  import Profile._
  private val start: Long = System.nanoTime()
  private[lf] val events: ArrayList[Event] = new ArrayList()
  var name: String = "DAML Engine profile"

  def addOpenEvent(label: AnyRef) = {
    val time = System.nanoTime()
    events.add(Event(true, label, time))
  }

  def addCloseEvent(label: AnyRef) = {
    val time = System.nanoTime()
    events.add(Event(false, label, time))
  }

  def writeSpeedscopeJson(path: Path) = {
    val fileJson = SpeedscopeJson.FileJson.fromProfile(this)
    fileJson.write(path)
  }
}

object Profile {
  // NOTE(MH): See the documenation of [[Profile]] above for why we use
  // [[AnyRef]] for the labels.
  private[lf] final case class Event(val open: Boolean, val rawLabel: AnyRef, val time: Long) {
    def label: String = {
      import com.daml.lf.speedy.SExpr._
      rawLabel match {
        case null => "<null>" // NOTE(MH): We should never see this but it's no problem if we do.
        case AnonymousClosure => "<lambda>"
        case LfDefRef(ref) => ref.qualifiedName.toString()
        case ChoiceDefRef(tmplRef, name) => s"exercise @${tmplRef.qualifiedName} ${name}"
        case ref: SEBuiltinRecursiveDefinition.Reference => ref.toString().toLowerCase()
        case str: String => str
        case any => s"<unknown ${any}>"
      }
    }
  }

  private def unmangleLenient(str: String): String = {
    val builder = new StringBuilder(str.length)
    var i = 0
    while (i < str.length) {
      if (str(i) == '$' && i + 1 < str.length) {
        str(i + 1) match {
          case '$' =>
            builder.append('$')
            i += 2
          case 'u' if i + 5 < str.length =>
            try {
              val cp = Integer.parseUnsignedInt(str.substring(i + 2, i + 6), 16)
              builder.appendAll(Character.toChars(cp))
              i += 6
            } catch {
              case _: NumberFormatException =>
                builder.append('$')
                i += 1
            }
          case 'U' if i + 9 < str.length =>
            try {
              val cp = Integer.parseUnsignedInt(str.substring(i + 2, i + 10), 16)
              builder.appendAll(Character.toChars(cp))
              i += 10
            } catch {
              case _: NumberFormatException =>
                builder.append('$')
                i += 1
            }
          case _ =>
            builder.append('$')
            i += 1
        }
      } else {
        builder.append(str(i))
        i += 1
      }
    }
    builder.toString
  }

  /** Utility object to convert the profile into the JSON format required by
    * https://www.speedscope.app/. For a description of the format, see
    * https://github.com/jlfwong/speedscope/wiki/Importing-from-custom-sources#speedscopes-file-format
    */
  private object SpeedscopeJson {
    import spray.json._

    val schemaURI = "https://www.speedscope.app/file-format-schema.json"

    object JsonProtocol extends DefaultJsonProtocol {
      implicit val eventFormat = jsonFormat3(EventJson.apply)
      implicit val profileFormat = jsonFormat6(ProfileJson.apply)
      implicit val frameFormat = jsonFormat1(FrameJson.apply)
      implicit val sharedFormat = jsonFormat1(SharedJson.apply)
      implicit val fileFormat = jsonFormat6(FileJson.apply)
    }

    case class EventJson(`type`: String, at: Long, frame: Int)
    case class ProfileJson(
        `type`: String,
        name: String,
        unit: String,
        startValue: Long,
        endValue: Long,
        events: List[EventJson],
    )
    case class FrameJson(name: String)
    case class SharedJson(frames: List[FrameJson])
    case class FileJson(
        `$schema`: String,
        profiles: List[ProfileJson],
        shared: SharedJson,
        activeProfileIndex: Int,
        exporter: String,
        name: String,
    ) {
      def write(path: Path): Unit = {
        import JsonProtocol.fileFormat
        val _ = Files.write(path, Seq(this.toJson.compactPrint).asJava)
      }
    }

    object FileJson {
      def fromProfile(profile: Profile) = {
        import scala.collection.mutable.HashMap
        import scala.collection.JavaConverters._

        val frames = new ArrayList[FrameJson]()
        val frameIndices = new HashMap[String, Int]()
        var endValue = 0L
        val events = profile.events.asScala.toList.map { event =>
          val eventType = if (event.open) "O" else "C"
          val label = event.label
          val frameIndex = frameIndices.get(label) match {
            case Some(index) => index
            case None =>
              val index = frames.size()
              frames.add(FrameJson(unmangleLenient(label)))
              frameIndices.put(label, index)
              index
          }
          val at = event.time - profile.start
          if (at > endValue) {
            endValue = at
          }
          EventJson(`type` = eventType, at = at, frame = frameIndex)
        }
        val profileJson = ProfileJson(
          `type` = "evented",
          name = profile.name,
          unit = "nanoseconds",
          startValue = 0,
          endValue = endValue,
          events = events,
        )
        val sharedJson = SharedJson(frames.asScala.toList)
        FileJson(
          `$schema` = schemaURI,
          profiles = List(profileJson),
          shared = sharedJson,
          activeProfileIndex = 0,
          exporter = "DAML Engine",
          name = profile.name,
        )
      }
    }
  }
}
