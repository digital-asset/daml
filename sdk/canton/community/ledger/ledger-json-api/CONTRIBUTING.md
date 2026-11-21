# Hints for developers extending Ledger JSON API
## also known as Json v2


## Adding new endpoint to existing service
For example: service `JsUserManagementService` - endpoint - list user rights

1. First you need to have a corresponding grpc service working.
In the example:
`UserManagementServiceGrpc#listUserRights(request: com.daml.ledger.api.v2.admin.user_management_service.ListUserRightsRequest): scala.concurrent.Future[com.daml.ledger.api.v2.admin.user_management_service.ListUserRightsResponse]`
2. Decide about url and params for the endpoint.
In the example: `GET /v2/users/<user-id>/rights`

Naming conventions:
 - prefer `kebab-case` for paths and query parameters,
 - use `camelCase` for json fields (to match generated grpc stub classes),
 - while creating mirror classes (below0 try to keep names as close to grpc as possible (avoid prefixes such as Js).



3. Encode this Endpoint as Tapir Endpoint inside service object.
```scala
  val listUserRightsEndpoint =
    users.get
      .in(path[String](userIdPath))
      .in("rights")
      .out(jsonBody[user_management_service.ListUserRightsResponse])
      .description("List user rights.")
```

Notice that there is no business logic added yet (only endpoint definition).

4. Compile and add Codecs

When you try to compile you will probably get errors about missing Circe codecs for:

Typical solution is to add Codec inside Codecs object for service:

in example: `JsUserManagementCodecs`
```scala
 implicit val listUserRightsResponseRW: Codec[user_management_service.ListUserRightsResponse] =
    deriveRelaxedCodec
```

Sometimes you need to add codecs for nested types as well.

5. Add endpoint to the list of documented endpoints:

```scala
override def documentation: Seq[AnyEndpoint] = List(
  ...
    revokeUserRightsEndpoint,
+  listUserRightsEndpoint,
    updateUserIdentityProviderEndpoint,
  )
}
```

6. Add logic to the endpoint.

Go to the `endpoints` method in service and add logic to the endpoint.
Typically you first add a method:
```scala
private def listUserRights(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, user_management_service.ListUserRightsResponse]
  ] = req =>
    UserId.fromString(req.in) match {
      case Right(userId) =>
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .listUserRights(
            new user_management_service.ListUserRightsRequest(
              userId = userId,
              identityProviderId = "",
            )
          )
          .resultToRight
      case Left(error) => malformedUserId(error)(req.traceContext)
    }
```

Then you actually add the endpoint to the list of handled endpoints:
```scala
withServerLogic(
  JsUserManagementService.listUserRightsEndpoint,
  listUserRights,
),
```

7. If the Json structure returned or consumed by the endpoint contains DAML Record it needs to be specially handled by so called `transcode`.
      You also need to add "mirror" class for existing grpc stub, where DAML Record is replaced by `Json` type.

      See ` JsCommand.CreateCommand` for example.
```scala
object JsCommand {
  sealed trait Command
  final case class CreateCommand(
      templateId: Identifier,
      createArguments: Json,
  ) extends Command
```
And finally you need to write mapping between grpc stub and json api class.
Put your mapping method inside `ProtocolConverters` object.
See `ProtocolConverters.Command` for example.

If DAML record is nested you need to create mirror class for the outer classes as well (this is sometimes quite tedious).


## Writing a new service

1. Write `JsMYSERVICE` object - define base endpoint:
```scala
  private val myservice = v2Endpoint.in(sttp.tapir.stringToPath("myservice"))
```
2. Write `JsMYSERVICECodecs` object - define codecs for types used in service.

3. Write `JsMYSERVICE` class - implement service logic and endpoints method.

Copy of existing small service like `JsVersionService` is a good starting point.

## Final steps / testing

1. Typically you need to regenerate OpenAPI documentation.
2. Run object `GenerateJSONApiDocs` or use sbt task `sbt packageJsonApiDocsArtifacts`.
3. Check that the `openapi.yml` file is updated - analyze if the changes look correct
4. There is `OpenapiTypesTest` which checks that openapi  generated classes (java) match the actual json api classes (scala).
Usually you need to add missing mapping (for newly used types).
5. If you added a new ProtocolConverter - add a test for it in `ProtocolConvertersTest`.
6. Best way to test endpoint is to use our IT tests framework - add a test for your new endpoint such as in `PartyManagementServiceIT` - tests will run both using grpc and json/http

## Design decisions - explanations

1. Ledger JSON Api is designed to be a mirror of grpc - but we do not automate / generate "urls" from grpc api -
in order to keep API clean and understandable.

2. We use Tapir to define endpoints - which gives us a lot of flexibility and power.

3. We use Circe - but with semi automated codec generation - this means it is necessary to add codecs for new types
but this saves us from random changes in the circe encoding of more complex types.

4. We have some custom Encoders for circe like `deriveRelaxedCodec` ,`stringEncoderForEnum`,
check code for uses.

5. In tapir version we use (for Scala2) Circe encoding and actual documentation schema (for OpenAPI) is generated
independently - which means sometimes it diverges (documentation does not match actually expected types).
Such cases should be detected automatically in tests. You might need to use custom circe encoder and or Schema wrapper.
See code for examples. Usually problematic are ADTs and Enums.

6. We write mirror classes and use transcode for inputs/output with Daml records to keep jsons "readable".
