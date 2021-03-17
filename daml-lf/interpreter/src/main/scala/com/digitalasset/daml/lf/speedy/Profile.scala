// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import java.lang.System
import java.nio.file.{Files, Path}
import java.util.ArrayList
import scala.jdk.CollectionConverters._

/** Class for profiling information collected by Speedy.
  *
  *    Profiling works as follows:
  *
  *    1. The speedy compiler wraps some expressions in SELabelClosure
  *    with the label being some type of identifier which will later be
  *    used in the profiling results (e.g., the choice this expression
  *    corresponds to)
  *
  *    2. When executing SELabelClosure we push a KLabelClosure
  *    continuation and then proceed with the wrapped expression.
  *
  *    3. When we execute KLabelClosure, we look at the return value. If
  *    it is a closure, we modify the closure to contain the
  *    corresponding the label. If it is not a closure (this can happen
  *    for top-level definitions or let-bindings that are not functions),
  *    we do nothing.
  *
  *    4. When we execute a KFun and the corresponding closure has a
  *    label, we emit an open event for this label and push a
  *    KLeaveClosure.
  *
  *    5. When we execute the KLeaveClosure, we emit a close event for
  *    the label.
  */
final class Profile {
  import Profile._
  private val start: Long = System.nanoTime()
  private[lf] val events: ArrayList[Event] = new ArrayList()
  var name: String = "DAML Engine profile"

  def addOpenEvent(label: Label) = {
    val time = System.nanoTime()
    events.add(Event(true, label, time))
  }

  def addCloseEvent(label: Label) = {
    val time = System.nanoTime()
    events.add(Event(false, label, time))
  }

  def writeSpeedscopeJson(path: Path) = {
    val fileJson = SpeedscopeJson.FileJson.fromProfile(this)
    fileJson.write(path)
  }
}

object Profile {
  private[lf] final case class Event(val open: Boolean, val rawLabel: Label, val time: Long) {
    def label: String = LabelModule.Allowed.renderLabel(rawLabel)
  }

  private[speedy] def unmangleLenient(str: String): String = {
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

  final case class CreateAndExerciseLabel(tplId: Ref.DefinitionRef, choiceId: Ref.ChoiceName)

  sealed trait ScenarioLabel

  final case object SubmitLabel extends ScenarioLabel
  final case object SubmitMustFailLabel extends ScenarioLabel
  final case object PassLabel extends ScenarioLabel
  final case object GetPartyLabel extends ScenarioLabel

  type Label = LabelModule.Module.T
  val LabelUnset: Label = LabelModule.Module(null)

  /** We avoid any conversions into a common label format at runtime
    * since this might skew the profile. Instead, we convert the labels to strings
    * when we write out the profile.
    */
  sealed abstract class LabelModule {
    type T <: AnyRef
    private[Profile] def apply(t: AnyRef): T
  }

  object LabelModule {
    // NOTE(MH): See the documentation of [[LabelModule]] above for why we use
    // [[AnyRef]] for the labels.
    val Module: LabelModule = new LabelModule {
      type T = AnyRef
      override def apply(t: AnyRef) = t
    }

    import scala.language.implicitConversions

    // assumes -Xsource:2.13 in clients, which we should just do always,
    // this is in scope wherever the expected type is `Label`
    implicit def fromAllowed[T: Allowed](t: T with AnyRef): Label = Module(t)

    final class Allowed[-T] private ()
    object Allowed {
      import com.daml.lf.speedy.SExpr._
      private[this] val allowAll = new Allowed[Any]
      implicit val anonClosure: Allowed[AnonymousClosure.type] = allowAll
      implicit val lfDefRef: Allowed[LfDefRef] = allowAll
      implicit val createDefRef: Allowed[CreateDefRef] = allowAll
      implicit val choiceDefRef: Allowed[ChoiceDefRef] = allowAll
      implicit val fetchDefRef: Allowed[FetchDefRef] = allowAll
      implicit val choiceByKeyDefRef: Allowed[ChoiceByKeyDefRef] = allowAll
      implicit val fetchByKeyDefRef: Allowed[FetchByKeyDefRef] = allowAll
      implicit val lookupByKeyDefRef: Allowed[LookupByKeyDefRef] = allowAll
      implicit val createAndExerciseLabel: Allowed[CreateAndExerciseLabel] = allowAll
      implicit val exceptionMessageDefRef: Allowed[ExceptionMessageDefRef] = allowAll
      implicit val sebrdr: Allowed[SEBuiltinRecursiveDefinition.Reference] = allowAll
      implicit val scenarioLabel: Allowed[ScenarioLabel] = allowAll
      implicit val exprVarName: Allowed[Ast.ExprVarName] = allowAll

      // below cases must cover above set

      private[Profile] def renderLabel(rawLabel: Label): String =
        (rawLabel: AnyRef) match {
          case null => "<null>" // NOTE(MH): We should never see this but it's no problem if we do.
          case AnonymousClosure => "<lambda>"
          case LfDefRef(ref) => ref.qualifiedName.toString()
          case CreateDefRef(tmplRef) => s"create @${tmplRef.qualifiedName}"
          case ChoiceDefRef(tmplRef, name) => s"exercise @${tmplRef.qualifiedName} ${name}"
          case FetchDefRef(tmplRef) => s"fetch @${tmplRef.qualifiedName}"
          case ChoiceByKeyDefRef(tmplRef, name) =>
            s"exerciseByKey @${tmplRef.qualifiedName} ${name}"
          case FetchByKeyDefRef(tmplRef) => s"fetchByKey @${tmplRef.qualifiedName}"
          case LookupByKeyDefRef(tmplRef) => s"lookupByKey @${tmplRef.qualifiedName}"
          case CreateAndExerciseLabel(tmplRef, name) =>
            s"createAndExercise @${tmplRef.qualifiedName} ${name}"
          case ExceptionMessageDefRef(typeId) => s"message @${typeId.qualifiedName}"
          case ref: SEBuiltinRecursiveDefinition.Reference => ref.toString().toLowerCase()
          case SubmitLabel => "submit"
          case SubmitMustFailLabel => "submitMustFail"
          case PassLabel => "pass"
          case GetPartyLabel => "getParty"
          // This is only used for ExprVarName but we cannot do a runtime check due to
          // type erasure.
          case v: String => v
          case any => s"<unknown ${any}>"
        }
    }

  }
}
