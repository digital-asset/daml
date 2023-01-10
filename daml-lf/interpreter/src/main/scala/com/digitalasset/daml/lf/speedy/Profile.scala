// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.scalautil.Statement.discard
import java.lang.System
import java.nio.file.{Files, Path}
import java.util
import scala.annotation.nowarn
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
  private[lf] val events: util.ArrayList[Event] = ArrayList.empty
  var name: String = "Daml Engine profile"

  def addOpenEvent(label: Label): Unit = {
    val time = System.nanoTime()
    discard(events.add(Event(true, label, time)))
  }

  def addCloseEvent(label: Label): Unit = {
    val time = System.nanoTime()
    discard(events.add(Event(false, label, time)))
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
    def append(c: Char) = discard(builder.append(c))
    def appendAll(cs: Array[Char]) = discard(builder.appendAll(cs))
    var i = 0
    while (i < str.length) {
      if (str(i) == '$' && i + 1 < str.length) {
        str(i + 1) match {
          case '$' =>
            append('$')
            i += 2
          case 'u' if i + 5 < str.length =>
            try {
              val cp = Integer.parseUnsignedInt(str.substring(i + 2, i + 6), 16)
              appendAll(Character.toChars(cp))
              i += 6
            } catch {
              case _: NumberFormatException =>
                append('$')
                i += 1
            }
          case 'U' if i + 9 < str.length =>
            try {
              val cp = Integer.parseUnsignedInt(str.substring(i + 2, i + 10), 16)
              appendAll(Character.toChars(cp))
              i += 10
            } catch {
              case _: NumberFormatException =>
                append('$')
                i += 1
            }
          case _ =>
            append('$')
            i += 1
        }
      } else {
        append(str(i))
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

    final case class EventJson(`type`: String, at: Long, frame: Int)
    final case class ProfileJson(
        `type`: String,
        name: String,
        unit: String,
        startValue: Long,
        endValue: Long,
        events: List[EventJson],
    )
    final case class FrameJson(name: String)
    final case class SharedJson(frames: List[FrameJson])
    final case class FileJson(
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

        val frames = ArrayList.empty[FrameJson]
        val frameIndices = HashMap.empty[String, Int]
        var endValue = 0L
        val events = profile.events.asScala.toList.map { event =>
          val eventType = if (event.open) "O" else "C"
          val label = event.label
          val frameIndex = frameIndices.get(label) match {
            case Some(index) => index
            case None =>
              val index = frames.size()
              discard(frames.add(FrameJson(unmangleLenient(label))))
              discard(frameIndices.put(label, index))
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
          exporter = "Daml Engine",
          name = profile.name,
        )
      }
    }
  }

  final case class CreateAndExerciseLabel(tplId: Ref.DefinitionRef, choiceId: Ref.ChoiceName)

  sealed abstract class ScenarioLabel extends Product with Serializable

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
    @nowarn("msg=parameter value evidence.* is never used")
    implicit def fromAllowed[T: Allowed](t: T with AnyRef): Label = Module(t)

    final class Allowed[-T] private ()
    object Allowed {
      import com.daml.lf.speedy.SExpr._
      private[this] val allowAll = new Allowed[Any]
      implicit val anonClosure: Allowed[AnonymousClosure.type] = allowAll
      implicit val lfDefRef: Allowed[LfDefRef] = allowAll
      implicit val createDefRef: Allowed[CreateDefRef] = allowAll
      implicit val templatePreConditionDefRef: Allowed[TemplatePreConditionDefRef] = allowAll
      implicit val agreementTextDefRef: Allowed[AgreementTextDefRef] = allowAll
      implicit val signatoriesDefRef: Allowed[SignatoriesDefRef] = allowAll
      implicit val observersDefRef: Allowed[ObserversDefRef] = allowAll
      implicit val interfaceInstanceMethodDefRef: Allowed[InterfaceInstanceMethodDefRef] = allowAll
      implicit val interfaceInstanceViewDefRef: Allowed[InterfaceInstanceViewDefRef] = allowAll
      implicit val templateChoiceDefRef: Allowed[TemplateChoiceDefRef] = allowAll
      implicit val interfaceChoiceDefRef: Allowed[InterfaceChoiceDefRef] = allowAll
      implicit val fetchTemplateDefRef: Allowed[FetchTemplateDefRef] = allowAll
      implicit val fetchInterfaceDefRef: Allowed[FetchInterfaceDefRef] = allowAll
      implicit val choiceByKeyDefRef: Allowed[ChoiceByKeyDefRef] = allowAll
      implicit val fetchByKeyDefRef: Allowed[FetchByKeyDefRef] = allowAll
      implicit val lookupByKeyDefRef: Allowed[LookupByKeyDefRef] = allowAll
      implicit val contractKeyWithMaintainersDefRef: Allowed[ContractKeyWithMaintainersDefRef] =
        allowAll
      implicit val createAndExerciseLabel: Allowed[CreateAndExerciseLabel] = allowAll
      implicit val exceptionMessageDefRef: Allowed[ExceptionMessageDefRef] = allowAll
      implicit val scenarioLabel: Allowed[ScenarioLabel] = allowAll
      implicit val exprVarName: Allowed[Ast.ExprVarName] = allowAll

      // below cases must cover above set

      private[Profile] def renderLabel(rawLabel: Label): String =
        (rawLabel: AnyRef) match {
          case null => "<null>" // NOTE(MH): We should never see this but it's no problem if we do.
          case AnonymousClosure => "<lambda>"
          case LfDefRef(ref) => ref.qualifiedName.toString()
          case CreateDefRef(tmplRef) => s"create @${tmplRef.qualifiedName}"
          case TemplatePreConditionDefRef(tmplRef) => s"ensures @${tmplRef.qualifiedName}"
          case AgreementTextDefRef(tmplRef) => s"agreement @${tmplRef.qualifiedName}"
          case SignatoriesDefRef(tmplRef) => s"signatories @${tmplRef.qualifiedName}"
          case ObserversDefRef(tmplRef) => s"observers @${tmplRef.qualifiedName}"
          case ContractKeyWithMaintainersDefRef(tmplRef) => s"key @${tmplRef.qualifiedName}"
          case ToCachedContractDefRef(tmplRef) => s"toAnyContract @${tmplRef.qualifiedName}"
          case InterfaceInstanceMethodDefRef(ii, methodName) =>
            s"interfaceInstanceMethod @${ii.parent.qualifiedName} @${ii.interfaceId.qualifiedName} @${ii.templateId.qualifiedName} ${methodName}"
          case InterfaceInstanceViewDefRef(ii) =>
            s"interfaceInstanceView @${ii.parent.qualifiedName} @${ii.interfaceId.qualifiedName} @${ii.templateId.qualifiedName}"
          case TemplateChoiceDefRef(tmplRef, name) =>
            s"exercise @${tmplRef.qualifiedName} ${name}"
          case InterfaceChoiceDefRef(ifaceRef, name) =>
            s"exercise @${ifaceRef.qualifiedName} ${name}"
          case FetchTemplateDefRef(tmplRef) => s"fetch_template @${tmplRef.qualifiedName}"
          case FetchInterfaceDefRef(ifaceRef) => s"fetch_interface @${ifaceRef.qualifiedName}"
          case ChoiceByKeyDefRef(tmplRef, name) =>
            s"exerciseByKey @${tmplRef.qualifiedName} ${name}"
          case FetchByKeyDefRef(tmplRef) => s"fetchByKey @${tmplRef.qualifiedName}"
          case LookupByKeyDefRef(tmplRef) => s"lookupByKey @${tmplRef.qualifiedName}"
          case CreateAndExerciseLabel(tmplRef, name) =>
            s"createAndExercise @${tmplRef.qualifiedName} ${name}"
          case ExceptionMessageDefRef(typeId) => s"message @${typeId.qualifiedName}"
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
