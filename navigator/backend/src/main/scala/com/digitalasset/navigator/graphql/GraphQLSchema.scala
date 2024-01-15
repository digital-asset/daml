// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.CustomEndpoint
import com.daml.navigator.model._
import com.daml.navigator.query._
import com.daml.navigator.store.Store._
import com.daml.navigator.time.{TimeProviderType, TimeProviderWithType}
import com.daml.navigator.model.converter.GenericConversionError
import sangria.ast.StringValue
import sangria.macros.derive.GraphQLDeprecated
import sangria.schema.InputObjectType.DefaultInput
import sangria.schema._
import sangria.validation.{ValueCoercionViolation, Violation}
import scalaz.Tag

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class GraphQLContext(party: PartyState, store: ActorRef) {
  def ledger: Ledger = party.ledger
  def templates: PackageRegistry = party.packageRegistry
}

case class LedgerTime(time: TimeProviderWithType, id: String = "CURRENT")

case class UserFacingError(message: String)
    extends Throwable
    with sangria.execution.UserFacingError {
  override def getMessage: String = message
}

private object Implicits {

  implicit final class `ExtendedInterfaceType Ops`[Ctx, Val](
      private val self: InterfaceType[Ctx, Val]
  ) extends AnyVal {
    // Hack around https://github.com/scala/bug/issues/11662 which
    // otherwise results in the implicit PossibleObject constructor
    // being ambiguous due to the also implicit apply method.
    def withPossibleObjectTypes(possible: () => List[ObjectType[Ctx, _]]) =
      self.withPossibleTypes(() => possible().map(new PossibleObject[Ctx, Val](_)))
  }
}

/** Schema definition for the UI backend GraphQL API. */
@SuppressWarnings(
  Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Serializable")
)
final class GraphQLSchema(customEndpoints: Set[CustomEndpoint[_]]) {

  import Implicits._

  implicit private val actorTimeout: Timeout = Timeout(60, TimeUnit.SECONDS)
  implicit private val executionContext: ExecutionContext = ExecutionContext.global

  // ------------------------------------------------------------------------------------------------------------------
  // Scalar and enum types
  // ------------------------------------------------------------------------------------------------------------------
  val DirectionType = EnumType(
    "Direction",
    values = List(
      EnumValue("ASCENDING", value = SortDirection.ASCENDING),
      EnumValue("DESCENDING", value = SortDirection.DESCENDING),
    ),
  )

  val TimeTypeType = EnumType(
    "TimeType",
    values = List(
      EnumValue("static", value = TimeProviderType.Static),
      EnumValue("wallclock", value = TimeProviderType.WallClock),
      EnumValue("simulated", value = TimeProviderType.Simulated),
    ),
  )

  val SortingType = ObjectType(
    "Sorting",
    fields[Unit, SortCriterion](
      Field("field", StringType, resolve = _.value.field),
      Field("direction", DirectionType, resolve = _.value.direction),
    ),
  )

  val TimeType: ScalarType[Instant] = ScalarType[Instant](
    "Time",
    coerceOutput = (instant, _) => DateTimeFormatter.ISO_INSTANT.format(instant),
    coerceUserInput = {
      case s: String => parseInstant(s)
      case _ => Left(InstantCoercionViolation)
    },
    coerceInput = {
      case StringValue(s, _, _, _, _) => parseInstant(s)
      case _ => Left(InstantCoercionViolation)
    },
  )

  val PartyType = StringAliasType("Party")
  val OffsetType = StringAliasType("LedgerOffset")
  val CommandIdType: ScalarType[ApiTypes.CommandId] = StringAliasType.tagged("CommandId")

  // ------------------------------------------------------------------------------------------------------------------
  // Composite types
  // ------------------------------------------------------------------------------------------------------------------
  val LedgerTimeType = ObjectType(
    "LedgerTime",
    fields[Unit, TimeProviderWithType](
      Field("id", IDType, resolve = _ => "CURRENT"),
      Field("time", TimeType, resolve = _.value.time.getCurrentTime),
      Field("type", TimeTypeType, resolve = _.value.`type`),
    ),
  )

  // noinspection ForwardReference
  val NodeType: InterfaceType[GraphQLContext, Node[_]] = InterfaceType[GraphQLContext, Node[_]](
    "Node",
    fields[GraphQLContext, Node[_]](
      Field("id", IDType, resolve = _.value.idString)
    ),
  ).withPossibleObjectTypes(() =>
    List(
      TransactionType,
      TemplateType,
      ContractType,
      CreateCommandType,
      ExerciseCommandType,
      CreatedEventType,
      ExercisedEventType,
      DamlLfDefDataTypeType,
    )
  )

  // noinspection ForwardReference
  val DamlLfNodeType: InterfaceType[GraphQLContext, DamlLfNode] = InterfaceType(
    "DamlLfNode",
    fields[GraphQLContext, DamlLfNode](
      Field("id", IDType, resolve = _.value.idString),
      Field("package", StringType, resolve = _.value.id.packageId),
      Field("module", StringType, resolve = _.value.id.qualifiedName.module.toString()),
      Field("name", StringType, resolve = _.value.id.qualifiedName.name.toString()),
    ),
  ).withPossibleObjectTypes(() =>
    List(
      DamlLfDefDataTypeType,
      TemplateType,
    )
  )

  val ChoiceType = ObjectType(
    "Choice",
    fields[Unit, Choice](
      Field(
        "name",
        StringType,
        resolve = context => Tag.unwrap[String, ApiTypes.ChoiceTag](context.value.name),
      ),
      Field("parameter", JsonType.DamlLfTypeType, resolve = _.value.parameter),
      Field("consuming", BooleanType, resolve = _.value.consuming),
      Field(
        "inheritedInterface",
        OptionType(StringType),
        resolve = _.value.inheritedInterface.map(_.asOpaqueString),
      ),
    ),
  )

  // noinspection ForwardReference
  val TemplateType: ObjectType[GraphQLContext, Template] = ObjectType(
    "Template",
    interfaces[GraphQLContext, Template](NodeType, DamlLfNodeType),
    () =>
      fields[GraphQLContext, Template](
        Field("id", IDType, resolve = _.value.idString),
        Field("topLevelDecl", StringType, resolve = _.value.topLevelDecl),
        Field("parameter", JsonType.DamlLfTypeType, resolve = _.value.parameter),
        Field(
          "parameterDef",
          DamlLfDefDataTypeType,
          resolve = context =>
            context.ctx.templates
              .damlLfDefDataType(context.value.id)
              .map(DamlLfDefDataTypeBoxed(context.value.id, _))
              .get,
        ),
        Field("choices", ListType(ChoiceType), resolve = _.value.choices),
        Field(
          "contracts",
          ContractPaginationType,
          arguments =
            SearchArg :: FilterArg :: IncludeArchivedArg :: CountArg :: StartArg :: SortArg :: Nil,
          resolve = context =>
            buildTemplateContractPager(context).fetch(context.ctx.ledger, context.ctx.templates),
        ),
        Field(
          "implementedInterfaces",
          ListType(StringType),
          resolve = _.value.implementedInterfaces.toList.map(_.asOpaqueString),
        ),
      ),
  )

  val TemplateEdgeType = ObjectType(
    "TemplateEdge",
    fields[GraphQLContext, Template](
      Field("node", TemplateType, resolve = _.value),
      Field("cursor", StringType, resolve = _.value.idString),
    ),
  )

  val TemplatePaginationType = ObjectType(
    "TemplatePagination",
    fields[GraphQLContext, Page[Template]](
      Field("beforeCount", IntType, resolve = _.value.offset),
      Field("totalCount", IntType, resolve = _.value.total),
      Field("sortings", OptionType(ListType(SortingType)), resolve = _.value.sortedLike),
      Field("edges", ListType(TemplateEdgeType), resolve = _.value.rows),
    ),
  )

  // noinspection ForwardReference
  val EventType: InterfaceType[GraphQLContext, Event] = InterfaceType(
    "Event",
    () =>
      fields[GraphQLContext, Event](
        Field("id", IDType, resolve = _.value.idString),
        Field(
          "parent",
          OptionType(EventType),
          resolve = context =>
            context.value.parentId.flatMap(pid =>
              context.ctx.ledger.event(pid, context.ctx.templates)
            ),
        ),
        Field(
          "transaction",
          TransactionType,
          resolve = context =>
            context.ctx.ledger.transaction(context.value.transactionId, context.ctx.templates).get,
        ),
        Field(
          "witnessParties",
          ListType(PartyType),
          resolve = _.value.witnessParties.map(Tag.unwrap),
        ),
        Field(
          "workflowId",
          StringType,
          resolve = context => Tag.unwrap[String, ApiTypes.WorkflowIdTag](context.value.workflowId),
        ),
      ),
  ).withPossibleObjectTypes(() => List(CreatedEventType, ExercisedEventType))

  // noinspection ForwardReference
  val CreatedEventType: ObjectType[GraphQLContext, ContractCreated] = ObjectType(
    "CreatedEvent",
    interfaces[GraphQLContext, ContractCreated](NodeType, EventType),
    () =>
      fields[GraphQLContext, ContractCreated](
        Field(
          "transaction",
          TransactionType,
          resolve = context =>
            context.ctx.ledger.transaction(context.value.transactionId, context.ctx.templates).get,
        ),
        Field(
          "contract",
          ContractType,
          resolve = context =>
            context.ctx.ledger.contract(context.value.contractId, context.ctx.templates).get,
        ),
        Field("argument", JsonType.ApiRecordType, resolve = _.value.argument),
      ),
  )

  // noinspection ForwardReference
  val ExercisedEventType: ObjectType[GraphQLContext, ChoiceExercised] = ObjectType(
    "ExercisedEvent",
    interfaces[GraphQLContext, ChoiceExercised](NodeType, EventType),
    () =>
      fields[GraphQLContext, ChoiceExercised](
        Field(
          "transaction",
          TransactionType,
          resolve = context =>
            context.ctx.ledger.transaction(context.value.transactionId, context.ctx.templates).get,
        ),
        Field(
          "contract",
          ContractType,
          resolve = context =>
            context.ctx.ledger.contract(context.value.contractId, context.ctx.templates).get,
        ),
        Field(
          "choice",
          StringType,
          resolve = context => Tag.unwrap[String, ApiTypes.ChoiceTag](context.value.choice),
        ),
        Field("argument", JsonType.ApiValueType, resolve = _.value.argument),
        Field(
          "actingParties",
          ListType(PartyType),
          resolve = _.value.actingParties.map(Tag.unwrap),
        ),
        Field("consuming", BooleanType, resolve = _.value.consuming),
        Field(
          "children",
          ListType(EventType),
          resolve =
            context => context.ctx.ledger.childEvents(context.value.id, context.ctx.templates),
        ),
      ),
  )

  val TransactionType: ObjectType[GraphQLContext, Transaction] = ObjectType(
    "Transaction",
    interfaces[GraphQLContext, Transaction](NodeType),
    () =>
      fields[GraphQLContext, Transaction](
        Field("id", IDType, resolve = _.value.idString),
        Field("offset", OffsetType, resolve = context => context.value.offset),
        Field("effectiveAt", TimeType, resolve = _.value.effectiveAt),
        Field("commandId", OptionType(CommandIdType), resolve = _.value.commandId),
        Field("events", ListType(EventType), resolve = _.value.events),
      ),
  )

  // noinspection ForwardReference
  val ContractType = ObjectType(
    "Contract",
    interfaces[GraphQLContext, Contract](NodeType),
    () =>
      fields[GraphQLContext, Contract](
        Field("id", IDType, resolve = _.value.idString),
        Field("template", TemplateType, resolve = _.value.template),
        Field(
          "createEvent",
          CreatedEventType,
          resolve =
            context => context.ctx.ledger.createEventOf(context.value, context.ctx.templates),
        ),
        Field(
          "archiveEvent",
          OptionType(ExercisedEventType),
          resolve =
            context => context.ctx.ledger.archiveEventOf(context.value, context.ctx.templates),
        ),
        Field(
          "exerciseEvents",
          ListType(ExercisedEventType),
          resolve =
            context => context.ctx.ledger.exercisedEventsOf(context.value, context.ctx.templates),
        ),
        Field("argument", JsonType.ApiRecordType, resolve = _.value.argument),
        Field("agreementText", OptionType(StringType), resolve = _.value.agreementText),
        Field("signatories", ListType(StringType), resolve = _.value.signatories.map(Tag.unwrap)),
        Field("observers", ListType(StringType), resolve = _.value.observers.map(Tag.unwrap)),
        Field("key", OptionType(JsonType.ApiValueType), resolve = _.value.key),
      ),
  )

  val ContractEdgeType = ObjectType(
    "ContractEdge",
    fields[Unit, Contract](
      Field("node", ContractType, resolve = _.value),
      Field("cursor", StringType, resolve = _.value.idString),
    ),
  )

  val ContractPaginationType = ObjectType(
    "ContractPagination",
    fields[Unit, Page[Contract]](
      Field("beforeCount", IntType, resolve = _.value.offset),
      Field("totalCount", IntType, resolve = _.value.total),
      Field("sortings", OptionType(ListType(SortingType)), resolve = _.value.sortedLike),
      Field("edges", ListType(ContractEdgeType), resolve = _.value.rows),
    ),
  )

  val FilterCriterionType = InputObjectType(
    "FilterCriterion",
    List(
      InputField("field", StringType),
      InputField("value", StringType),
    ),
  )

  val SortCriterionType = InputObjectType(
    "SortCriterion",
    List(
      InputField("field", StringType),
      InputField("direction", DirectionType),
    ),
  )

  // noinspection ForwardReference
  val CommandStatusType: InterfaceType[GraphQLContext, CommandStatus] = InterfaceType(
    "CommandStatus",
    () =>
      fields[GraphQLContext, CommandStatus](
        Field("completed", BooleanType, resolve = _.value.isCompleted)
      ),
  ).withPossibleObjectTypes(() =>
    List(
      CommandStatusErrorType,
      CommandStatusWaitingType,
      CommandStatusSuccessType,
      CommandStatusUnknownType,
    )
  )

  val CommandStatusErrorType: ObjectType[GraphQLContext, CommandStatusError] = ObjectType(
    "CommandStatusError",
    interfaces[GraphQLContext, CommandStatusError](CommandStatusType),
    () =>
      fields[GraphQLContext, CommandStatusError](
        Field("code", StringType, resolve = _.value.code),
        Field("details", StringType, resolve = _.value.details),
      ),
  )

  val CommandStatusWaitingType: ObjectType[GraphQLContext, CommandStatusWaiting] = ObjectType(
    "CommandStatusWaiting",
    interfaces[GraphQLContext, CommandStatusWaiting](CommandStatusType),
    () =>
      fields[GraphQLContext, CommandStatusWaiting](
      ),
  )

  val CommandStatusUnknownType: ObjectType[GraphQLContext, CommandStatusUnknown] = ObjectType(
    "CommandStatusUnknown",
    interfaces[GraphQLContext, CommandStatusUnknown](CommandStatusType),
    () =>
      fields[GraphQLContext, CommandStatusUnknown](
      ),
  )

  val CommandStatusSuccessType: ObjectType[GraphQLContext, CommandStatusSuccess] = ObjectType(
    "CommandStatusSuccess",
    interfaces[GraphQLContext, CommandStatusSuccess](CommandStatusType),
    () =>
      fields[GraphQLContext, CommandStatusSuccess](
        Field("transaction", TransactionType, resolve = _.value.tx)
      ),
  )

  // noinspection ForwardReference
  val CommandType: InterfaceType[GraphQLContext, Command] = InterfaceType(
    "Command",
    () =>
      fields[GraphQLContext, Command](
        Field("id", IDType, resolve = _.value.idString),
        Field("index", IntType, resolve = _.value.index.toInt),
        Field(
          "workflowId",
          IDType,
          resolve = context => Tag.unwrap[String, ApiTypes.WorkflowIdTag](context.value.workflowId),
        ),
        Field("platformTime", TimeType, resolve = _.value.platformTime),
        Field(
          "status",
          CommandStatusType,
          resolve = context =>
            context.ctx.ledger
              .statusOf(context.value.id, context.ctx.templates)
              .getOrElse(CommandStatusUnknown()),
        ),
      ),
  ).withPossibleObjectTypes(() => List(CreateCommandType, ExerciseCommandType))

  val CreateCommandType: ObjectType[GraphQLContext, CreateCommand] = ObjectType(
    "CreateCommand",
    interfaces[GraphQLContext, CreateCommand](NodeType, CommandType),
    () =>
      fields[GraphQLContext, CreateCommand](
        Field(
          "template",
          OptionType(TemplateType),
          resolve = context => context.ctx.templates.template(context.value.template),
        ),
        Field("templateId", StringType, resolve = context => context.value.template.asOpaqueString),
        Field("argument", JsonType.ApiRecordType, resolve = _.value.argument),
      ),
  )

  val ExerciseCommandType: ObjectType[GraphQLContext, ExerciseCommand] = ObjectType(
    "ExerciseCommand",
    interfaces[GraphQLContext, ExerciseCommand](NodeType, CommandType),
    () =>
      fields[GraphQLContext, ExerciseCommand](
        Field(
          "contract",
          OptionType(ContractType),
          resolve =
            context => context.ctx.ledger.contract(context.value.contract, context.ctx.templates),
        ),
        Field(
          "contractId",
          StringType,
          resolve = context => Tag.unwrap[String, ApiTypes.ContractIdTag](context.value.contract),
        ),
        Field(
          "interfaceId",
          OptionType(StringType),
          resolve = _.value.interfaceId.map(_.asOpaqueString),
        ),
        Field(
          "choice",
          StringType,
          resolve = context => Tag.unwrap[String, ApiTypes.ChoiceTag](context.value.choice),
        ),
        Field("argument", JsonType.ApiValueType, resolve = _.value.argument),
      ),
  )

  val CommandEdgeType = ObjectType(
    "CommandEdge",
    fields[GraphQLContext, Command](
      Field("node", CommandType, resolve = _.value),
      Field("cursor", StringType, resolve = _.value.idString),
    ),
  )

  val CommandPaginationType = ObjectType(
    "CommandPagination",
    fields[GraphQLContext, Page[Command]](
      Field("beforeCount", IntType, resolve = _.value.offset),
      Field("totalCount", IntType, resolve = _.value.total),
      Field("sortings", OptionType(ListType(SortingType)), resolve = _.value.sortedLike),
      Field("edges", ListType(CommandEdgeType), resolve = _.value.rows),
    ),
  )

  // noinspection ForwardReference
  val DamlLfDefDataTypeType: ObjectType[GraphQLContext, DamlLfDefDataTypeBoxed] = ObjectType(
    "DamlLfDefDataType",
    interfaces[GraphQLContext, DamlLfDefDataTypeBoxed](NodeType, DamlLfNodeType),
    () =>
      fields[GraphQLContext, DamlLfDefDataTypeBoxed](
        Field("dataType", JsonType.DamlLfDataTypeType, resolve = _.value.value.dataType),
        Field("typeVars", ListType(StringType), resolve = _.value.value.typeVars),
        Field(
          "dependencies",
          ListType(DamlLfDefDataTypeType),
          arguments = DepthArg :: Nil,
          resolve = context =>
            context.ctx.templates
              .typeDependencies(context.value.value, context.arg(DepthArg).getOrElse(Int.MaxValue))
              .toList
              .map(t => DamlLfDefDataTypeBoxed(t._1, t._2)),
        ),
      ),
  )

  val DamlLfFieldWithTypeType = ObjectType(
    "DamlLfFieldWithType",
    fields[GraphQLContext, DamlLfFieldWithType](
      Field("name", StringType, resolve = _.value._1),
      Field("type", JsonType.DamlLfTypeType, resolve = _.value._2),
    ),
  )

  // ------------------------------------------------------------------------------------------------------------------
  // Queries
  // ------------------------------------------------------------------------------------------------------------------
  val TypeArg = Argument("typename", StringType)
  val IDArg = Argument("id", IDType)
  val IDListArg = Argument("ids", ListInputType(IDType))

  @GraphQLDeprecated("use filter intead")
  val SearchArg = Argument("search", OptionInputType(StringType))

  val FilterArg = Argument("filter", OptionInputType(ListInputType(FilterCriterionType)))
  val CountArg = Argument("count", OptionInputType(IntType))
  val StartArg = Argument("start", OptionInputType(StringType))
  val SortArg = Argument("sort", OptionInputType(ListInputType(SortCriterionType)))
  val IncludeArchivedArg = Argument("includeArchived", OptionInputType(BooleanType))
  val DeclarationArg = Argument("topLevelDecl", StringType)
  val DepthArg = Argument("depth", OptionInputType(IntType))

  val QueryType = ObjectType(
    "Query",
    fields[GraphQLContext, Unit](
      Field(
        "parties",
        ListType(PartyType),
        arguments = SearchArg :: Nil,
        resolve = context =>
          (context.ctx.store ? GetParties(context.arg(SearchArg).getOrElse("")))
            .mapTo[PartyList]
            .map(response => Tag.unsubst(response.parties)),
      ),
      Field(
        "ledgerTime",
        LedgerTimeType,
        resolve = context =>
          (context.ctx.store ? ReportCurrentTime)
            .mapTo[Try[TimeProviderWithType]]
            .map(t => t.get),
      ),
      Field(
        "latestTransaction",
        OptionType(TransactionType),
        resolve = context => context.ctx.ledger.latestTransaction(context.ctx.templates),
      ),
      Field(
        "node",
        OptionType(NodeType),
        arguments = TypeArg :: IDArg :: Nil,
        resolve = context =>
          context.arg(TypeArg) match {
            case ContractType.name =>
              context.ctx.ledger
                .contract(ApiTypes.ContractId(context.arg(IDArg)), context.ctx.templates)
            case TemplateType.name =>
              context.ctx.templates.templateByStringId(TemplateStringId(context.arg(IDArg)))
            case CommandType.name =>
              context.ctx.ledger
                .command(ApiTypes.CommandId(context.arg(IDArg)), context.ctx.templates)
            case EventType.name =>
              context.ctx.ledger.event(ApiTypes.EventId(context.arg(IDArg)), context.ctx.templates)
            case TransactionType.name =>
              context.ctx.ledger
                .transaction(ApiTypes.TransactionId(context.arg(IDArg)), context.ctx.templates)
            case DamlLfDefDataTypeType.name =>
              for {
                id <- parseOpaqueIdentifier(context.arg(IDArg))
                ddt <- context.ctx.templates.damlLfDefDataType(id)
              } yield DamlLfDefDataTypeBoxed(id, ddt)
            case _ => None
          },
      ),
      Field(
        "template",
        ListType(TemplateType),
        arguments = DeclarationArg :: Nil,
        resolve = context => context.ctx.templates.templatesByName(context.arg(DeclarationArg)),
      ),
      Field(
        "nodes",
        ListType(NodeType),
        arguments = TypeArg :: IDListArg :: Nil,
        resolve = context => {
          val ledger = context.ctx.ledger
          val ids = context.arg(IDListArg).toSet
          context.arg(TypeArg) match {
            case ContractType.name =>
              ids
                .map(id => ApiTypes.ContractId(id.toString))
                .flatMap(ledger.contract(_, context.ctx.templates).toList)
                .toSeq
            case TemplateType.name =>
              ids
                .map(id => TemplateStringId(id.toString))
                .flatMap(context.ctx.templates.templateByStringId(_).toList)
                .toSeq
            case CommandType.name =>
              ids
                .map(id => ApiTypes.CommandId(id.toString))
                .flatMap(context.ctx.ledger.command(_, context.ctx.templates).toList)
                .toSeq
            case EventType.name =>
              ids
                .map(id => ApiTypes.EventId(id.toString))
                .flatMap(context.ctx.ledger.event(_, context.ctx.templates).toList)
                .toSeq
            case TransactionType.name =>
              ids
                .map(id => ApiTypes.TransactionId(id.toString))
                .flatMap(context.ctx.ledger.transaction(_, context.ctx.templates).toList)
                .toSeq
            case _ => Seq.empty[Node[_]]
          }
        },
      ),
      Field(
        "templates",
        TemplatePaginationType,
        arguments = SearchArg :: FilterArg :: CountArg :: StartArg :: SortArg :: Nil,
        resolve =
          context => buildTemplatePager(context).fetch(context.ctx.ledger, context.ctx.templates),
      ),
      Field(
        "contracts",
        ContractPaginationType,
        arguments =
          SearchArg :: FilterArg :: IncludeArchivedArg :: CountArg :: StartArg :: SortArg :: Nil,
        resolve =
          context => buildContractPager(context).fetch(context.ctx.ledger, context.ctx.templates),
      ),
      Field(
        "commands",
        CommandPaginationType,
        arguments = SearchArg :: FilterArg :: CountArg :: StartArg :: SortArg :: Nil,
        resolve =
          context => buildCommandPager(context).fetch(context.ctx.ledger, context.ctx.templates),
      ),
      Field(
        "commandStatus",
        OptionType(CommandStatusType),
        arguments = IDArg :: Nil,
        resolve = context =>
          context.ctx.ledger
            .statusOf(ApiTypes.CommandId(context.arg(IDArg)), context.ctx.templates)
            .getOrElse(CommandStatusUnknown()),
      ),
    ) ++ customEndpoints.map(_.endpoint),
  )

  // ------------------------------------------------------------------------------------------------------------------
  // Mutations
  // ------------------------------------------------------------------------------------------------------------------
  val TimeArgument = Argument("time", TimeType)
  val TemplateIdArgument = Argument("templateId", IDType)
  val ContractIdArgument = Argument("contractId", IDType)
  val ChoiceIdArgument = Argument("choiceId", IDType)
  val InterfaceIdArgument = Argument("interfaceId", OptionInputType(IDType))
  val AnyArgument = Argument("argument", OptionInputType(JsonType.ApiValueType))

  val MutationType = ObjectType(
    "Mutation",
    fields[GraphQLContext, Unit](
      Field(
        "advanceTime",
        LedgerTimeType,
        arguments = TimeArgument :: Nil,
        resolve = context =>
          (context.ctx.store ? AdvanceTime(context.arg(TimeArgument)))
            .mapTo[Try[TimeProviderWithType]]
            .map(t => t.get),
      ),
      Field(
        "create",
        CommandIdType,
        arguments = TemplateIdArgument :: AnyArgument :: Nil,
        resolve = context => {
          val command = CreateContract(
            context.ctx.party,
            TemplateStringId(context.arg(TemplateIdArgument)),
            context.arg(AnyArgument).collect({ case r: ApiRecord => r }).orNull,
          )
          wrapError((context.ctx.store ? command).mapTo[Try[ApiTypes.CommandId]])
        },
      ),
      Field(
        "exercise",
        CommandIdType,
        arguments =
          ContractIdArgument :: InterfaceIdArgument :: ChoiceIdArgument :: AnyArgument :: Nil,
        resolve = context => {
          val command = ExerciseChoice(
            context.ctx.party,
            ApiTypes.ContractId(context.arg(ContractIdArgument)),
            context.arg(InterfaceIdArgument).map(InterfaceStringId(_)),
            ApiTypes.Choice(context.arg(ChoiceIdArgument)),
            context.arg(AnyArgument).orNull,
          )
          wrapError((context.ctx.store ? command).mapTo[Try[ApiTypes.CommandId]])
        },
      ),
    ),
  )

  val QuerySchema = Schema(QueryType, Some(MutationType))

  case object InstantCoercionViolation extends ValueCoercionViolation("ISO-8601 timestamp expected")

  private def buildContractPager(context: Context[GraphQLContext, _]): Pager[Contract] =
    buildContractPager(
      context,
      if (shouldIncludeArchived(context)) AllContractsPager else ActiveContractsPager,
    )

  private def buildTemplateContractPager(
      context: Context[GraphQLContext, Template]
  ): Pager[Contract] =
    buildContractPager(
      context,
      if (shouldIncludeArchived(context)) {
        new TemplateContractPager(context.value)
      } else {
        new ActiveTemplateContractPager(context.value)
      },
    )

  private def shouldIncludeArchived(context: Context[_, _]): Boolean =
    context.arg(IncludeArchivedArg).getOrElse(false)

  def contractSearchToFilter(search: String): FilterCriterionBase =
    OrFilterCriterion(
      List(
        FilterCriterion("id", search),
        FilterCriterion("template.id", search),
        FilterCriterion("template.topLevelDecl", search),
      )
    )

  private def buildContractPager(
      context: Context[GraphQLContext, _],
      base: Pager[Contract],
  ): Pager[Contract] = {
    val ps = context.ctx.templates.damlLfDefDataType(_)
    val searched = context
      .arg(SearchArg)
      .map(arg => new ContractFilter(contractSearchToFilter(arg), ps, base))
      .getOrElse(base)
    val filtered = context
      .arg(FilterArg)
      .map(arg => new ContractFilter(filterCriteria(arg), ps, searched))
      .getOrElse(searched)
    val sorted = context
      .arg(SortArg)
      .map(arg => new ContractSorter(sortCriteria(arg), ps, filtered))
      .getOrElse(filtered)
    buildNodePager(context, sorted)
  }

  def templateSearchToFilter(search: String): FilterCriterionBase =
    OrFilterCriterion(List(FilterCriterion("id", search), FilterCriterion("topLevelDecl", search)))

  private def buildTemplatePager(
      context: Context[GraphQLContext, _],
      base: Pager[Template] = TemplatePager,
  ): Pager[Template] = {
    val ps = context.ctx.templates.damlLfDefDataType(_)
    val searched = context
      .arg(SearchArg)
      .map(arg => new TemplateFilter(templateSearchToFilter(arg), ps, base))
      .getOrElse(base)
    val filtered = context
      .arg(FilterArg)
      .map(arg => new TemplateFilter(filterCriteria(arg), ps, searched))
      .getOrElse(searched)
    val sorted = context
      .arg(SortArg)
      .map(arg => new TemplateSorter(sortCriteria(arg), ps, filtered))
      .getOrElse(filtered)
    buildNodePager(context, sorted)
  }

  def commandSearchToFilter(search: String): FilterCriterionBase =
    OrFilterCriterion(List(FilterCriterion("id", search), FilterCriterion("workflowId", search)))

  private def buildCommandPager(
      context: Context[GraphQLContext, _],
      base: Pager[Command] = CommandPager,
  ): Pager[Command] = {
    val ps = context.ctx.templates.damlLfDefDataType(_)
    val searched = context
      .arg(SearchArg)
      .map(arg => new CommandFilter(commandSearchToFilter(arg), ps, base))
      .getOrElse(base)
    val filtered = context
      .arg(FilterArg)
      .map(arg => new CommandFilter(filterCriteria(arg), ps, searched))
      .getOrElse(searched)
    val sorted = context
      .arg(SortArg)
      .map(arg => new CommandSorter(sortCriteria(arg), ps, filtered))
      .getOrElse(filtered)
    buildNodePager(context, sorted)
  }

  private def buildNodePager[N <: Node[_]](context: Context[_, _], sorted: Pager[N]): Pager[N] = {
    val shifted = context.arg(StartArg).map(new ShiftingPager(_, sorted)).getOrElse(sorted)
    val bounded = context.arg(CountArg).map(new BoundingPager(_, shifted)).getOrElse(shifted)
    bounded
  }

  def sortCriteria(arg: Seq[DefaultInput]): List[SortCriterion] =
    arg
      .map(input =>
        SortCriterion(
          input("field").toString,
          SortDirection.withName(input("direction").toString),
        )
      )
      .toList

  def filterCriteria(arg: Seq[DefaultInput]): AndFilterCriterion =
    AndFilterCriterion(
      arg.view
        .map(input =>
          FilterCriterion(
            input("field").toString,
            input("value").toString,
          )
        )
        .toList
    )

  private def parseInstant(s: String): Either[Violation, Instant] = Try(Instant.parse(s)) match {
    case Success(instant) => Right(instant)
    case Failure(_) => Left(InstantCoercionViolation)
  }

  private def mapError(error: Throwable): Throwable = error match {
    case GenericConversionError(message) => UserFacingError(message)
    case _ => error
  }

  private def wrapError[T](value: Try[T]): Try[T] = {
    value.recoverWith({ case e: Throwable => Failure(mapError(e)) })
  }

  private def wrapError[T](value: Future[Try[T]]): Future[T] = {
    value
      .recoverWith({ case e: Throwable => Future.failed(mapError(e)) })
      .flatMap(v => Future.fromTry(wrapError(v)))
  }

}
