====================
Ledger API Reference
====================

The specification for the services, methods, and messages
for interacting with the gRPC-based Ledger API.

----

.. _com/daml/ledger/api/v2/admin/command_inspection_service.proto:

``com/daml/ledger/api/v2/admin/command_inspection_service.proto``

.. _com.daml.ledger.api.v2.admin.CommandInspectionService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CommandInspectionService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Status: experimental interface, will change before it is deemed production
ready

The inspection service provides methods for the ledger administrator
to look under the hood of a running system.
In V2 Ledger API this service is not available.

.. _com.daml.ledger.api.v2.admin.CommandInspectionService.GetCommandStatus:

GetCommandStatus method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Inquire about the status of a command.
This service is used for debugging only. The command status is only tracked in memory and is not persisted.
The service can be used to understand the failure status and the structure of a command.
Requires admin privileges
The service is alpha without backward compatibility guarantees.

* Request: :ref:`GetCommandStatusRequest <com.daml.ledger.api.v2.admin.GetCommandStatusRequest>`
* Response: :ref:`GetCommandStatusResponse <com.daml.ledger.api.v2.admin.GetCommandStatusResponse>`



.. _com.daml.ledger.api.v2.admin.CommandStatus:

CommandStatus message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.CommandStatus.started:

``started`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.completed:

``completed`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.completion:

``completion`` : :ref:`com.daml.ledger.api.v2.Completion <com.daml.ledger.api.v2.Completion>`

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.state:

``state`` : :ref:`CommandState <com.daml.ledger.api.v2.admin.CommandState>`

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.commands:

``commands`` : :ref:`com.daml.ledger.api.v2.Command <com.daml.ledger.api.v2.Command>` (repeated)

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.request_statistics:

``request_statistics`` : :ref:`RequestStatistics <com.daml.ledger.api.v2.admin.RequestStatistics>`

 

.. _com.daml.ledger.api.v2.admin.CommandStatus.updates:

``updates`` : :ref:`CommandUpdates <com.daml.ledger.api.v2.admin.CommandUpdates>`

 

.. _com.daml.ledger.api.v2.admin.CommandUpdates:

CommandUpdates message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.CommandUpdates.created:

``created`` : :ref:`Contract <com.daml.ledger.api.v2.admin.Contract>` (repeated)

 

.. _com.daml.ledger.api.v2.admin.CommandUpdates.archived:

``archived`` : :ref:`Contract <com.daml.ledger.api.v2.admin.Contract>` (repeated)

 

.. _com.daml.ledger.api.v2.admin.CommandUpdates.exercised:

``exercised`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.admin.CommandUpdates.fetched:

``fetched`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.admin.CommandUpdates.looked_up_by_key:

``looked_up_by_key`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.admin.Contract:

Contract message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.Contract.template_id:

``template_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.admin.Contract.contract_id:

``contract_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.admin.Contract.contract_key:

``contract_key`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>`

 

.. _com.daml.ledger.api.v2.admin.GetCommandStatusRequest:

GetCommandStatusRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetCommandStatusRequest.command_id_prefix:

``command_id_prefix`` : :ref:`string <string>`

optional filter by command id 

.. _com.daml.ledger.api.v2.admin.GetCommandStatusRequest.state:

``state`` : :ref:`CommandState <com.daml.ledger.api.v2.admin.CommandState>`

optional filter by state 

.. _com.daml.ledger.api.v2.admin.GetCommandStatusRequest.limit:

``limit`` : :ref:`uint32 <uint32>`

optional limit of returned statuses, defaults to 100 

.. _com.daml.ledger.api.v2.admin.GetCommandStatusResponse:

GetCommandStatusResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetCommandStatusResponse.command_status:

``command_status`` : :ref:`CommandStatus <com.daml.ledger.api.v2.admin.CommandStatus>` (repeated)

 

.. _com.daml.ledger.api.v2.admin.RequestStatistics:

RequestStatistics message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.RequestStatistics.envelopes:

``envelopes`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.admin.RequestStatistics.request_size:

``request_size`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.admin.RequestStatistics.recipients:

``recipients`` : :ref:`uint32 <uint32>`

 




.. _com.daml.ledger.api.v2.admin.CommandState:

CommandState enum, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================





.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.admin.CommandState.COMMAND_STATE_UNSPECIFIED:

       COMMAND_STATE_UNSPECIFIED
     - 0
     - This value acts as wildcard in the queries

   * - .. _com.daml.ledger.api.v2.admin.CommandState.COMMAND_STATE_PENDING:

       COMMAND_STATE_PENDING
     - 1
     - 

   * - .. _com.daml.ledger.api.v2.admin.CommandState.COMMAND_STATE_SUCCEEDED:

       COMMAND_STATE_SUCCEEDED
     - 2
     - 

   * - .. _com.daml.ledger.api.v2.admin.CommandState.COMMAND_STATE_FAILED:

       COMMAND_STATE_FAILED
     - 3
     - 

   

----

.. _com/daml/ledger/api/v2/admin/identity_provider_config_service.proto:

``com/daml/ledger/api/v2/admin/identity_provider_config_service.proto``

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
IdentityProviderConfigService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Identity Provider Config Service makes it possible for participant node administrators
to setup and manage additional identity providers at runtime.

This allows using access tokens from identity providers unknown at deployment time. When an identity
provider is configured, independent IDP administrators can manage their own set of parties and users.
Such parties and users have a matching `identity_provider_id` defined and are inaccessible to
administrators from other identity providers. A user will only be authenticated if the corresponding JWT
token is issued by the appropriate identity provider.
Users and parties without `identity_provider_id` defined are assumed to be using the default identity provider,
which is configured statically at the participant node's deployment time.

The Ledger API uses the "iss" claim of a JWT token to match the token to a specific IDP. If there is no match,
the default IDP is assumed.

The fields of request messages (and sub-messages) are marked either as ``Optional`` or ``Required``:

1. ``Optional`` denoting the client may leave the field unset when sending a request.
2. ``Required`` denoting the client must set the field to a non-default value when sending a request.

An identity provider config resource is described by the ``IdentityProviderConfig`` message,
An identity provider config resource, once it has been created, can be modified.
In order to update the properties represented by the ``IdentityProviderConfig`` message use the ``UpdateIdentityProviderConfig`` RPC.
The only fields that can be modified are those marked as ``Modifiable``.

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService.CreateIdentityProviderConfig:

CreateIdentityProviderConfig method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Create a new identity provider configuration.
The request will fail if the maximum allowed number of separate configurations is reached.

* Request: :ref:`CreateIdentityProviderConfigRequest <com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigRequest>`
* Response: :ref:`CreateIdentityProviderConfigResponse <com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigResponse>`



.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService.GetIdentityProviderConfig:

GetIdentityProviderConfig method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Get the identity provider configuration data by id.

* Request: :ref:`GetIdentityProviderConfigRequest <com.daml.ledger.api.v2.admin.GetIdentityProviderConfigRequest>`
* Response: :ref:`GetIdentityProviderConfigResponse <com.daml.ledger.api.v2.admin.GetIdentityProviderConfigResponse>`



.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService.UpdateIdentityProviderConfig:

UpdateIdentityProviderConfig method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Update selected modifiable attribute of an identity provider config resource described
by the ``IdentityProviderConfig`` message.

* Request: :ref:`UpdateIdentityProviderConfigRequest <com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigRequest>`
* Response: :ref:`UpdateIdentityProviderConfigResponse <com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigResponse>`



.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService.ListIdentityProviderConfigs:

ListIdentityProviderConfigs method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

List all existing identity provider configurations.

* Request: :ref:`ListIdentityProviderConfigsRequest <com.daml.ledger.api.v2.admin.ListIdentityProviderConfigsRequest>`
* Response: :ref:`ListIdentityProviderConfigsResponse <com.daml.ledger.api.v2.admin.ListIdentityProviderConfigsResponse>`



.. _com.daml.ledger.api.v2.admin.IdentityProviderConfigService.DeleteIdentityProviderConfig:

DeleteIdentityProviderConfig method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Delete an existing identity provider configuration.

* Request: :ref:`DeleteIdentityProviderConfigRequest <com.daml.ledger.api.v2.admin.DeleteIdentityProviderConfigRequest>`
* Response: :ref:`DeleteIdentityProviderConfigResponse <com.daml.ledger.api.v2.admin.DeleteIdentityProviderConfigResponse>`



.. _com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigRequest:

CreateIdentityProviderConfigRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigRequest.identity_provider_config:

``identity_provider_config`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>`

Required 

.. _com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigResponse:

CreateIdentityProviderConfigResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.CreateIdentityProviderConfigResponse.identity_provider_config:

``identity_provider_config`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>`

 

.. _com.daml.ledger.api.v2.admin.DeleteIdentityProviderConfigRequest:

DeleteIdentityProviderConfigRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.DeleteIdentityProviderConfigRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The identity provider config to delete.
Required 

.. _com.daml.ledger.api.v2.admin.DeleteIdentityProviderConfigResponse:

DeleteIdentityProviderConfigResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Does not (yet) contain any data.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.GetIdentityProviderConfigRequest:

GetIdentityProviderConfigRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetIdentityProviderConfigRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

Required 

.. _com.daml.ledger.api.v2.admin.GetIdentityProviderConfigResponse:

GetIdentityProviderConfigResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetIdentityProviderConfigResponse.identity_provider_config:

``identity_provider_config`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>`

 

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig:

IdentityProviderConfig message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The identity provider identifier
Must be a valid LedgerString (as describe in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig.is_deactivated:

``is_deactivated`` : :ref:`bool <bool>`

When set, the callers using JWT tokens issued by this identity provider are denied all access
to the Ledger API.
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig.issuer:

``issuer`` : :ref:`string <string>`

Specifies the issuer of the JWT token.
The issuer value is a case sensitive URL using the https scheme that contains scheme, host,
and optionally, port number and path components and no query or fragment components.
Required
Modifiable 

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig.jwks_url:

``jwks_url`` : :ref:`string <string>`

The JWKS (JSON Web Key Set) URL.
The Ledger API uses JWKs (JSON Web Keys) from the provided URL to verify that the JWT has been
signed with the loaded JWK. Only RS256 (RSA Signature with SHA-256) signing algorithm is supported.
Required
Modifiable 

.. _com.daml.ledger.api.v2.admin.IdentityProviderConfig.audience:

``audience`` : :ref:`string <string>`

Specifies the audience of the JWT token.
When set, the callers using JWT tokens issued by this identity provider are allowed to get an access
only if the "aud" claim includes the string specified here
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.ListIdentityProviderConfigsRequest:

ListIdentityProviderConfigsRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Pagination is not required as the resulting data set is small enough to be returned in a single call

Message has no fields.

.. _com.daml.ledger.api.v2.admin.ListIdentityProviderConfigsResponse:

ListIdentityProviderConfigsResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ListIdentityProviderConfigsResponse.identity_provider_configs:

``identity_provider_configs`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>` (repeated)

 

.. _com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigRequest:

UpdateIdentityProviderConfigRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigRequest.identity_provider_config:

``identity_provider_config`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>`

The identity provider config to update.
Required,
Modifiable 

.. _com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigRequest.update_mask:

``update_mask`` :  `google.protobuf.FieldMask <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#fieldmask>`__

An update mask specifies how and which properties of the ``IdentityProviderConfig`` message are to be updated.
An update mask consists of a set of update paths.
A valid update path points to a field or a subfield relative to the ``IdentityProviderConfig`` message.
A valid update mask must:

1. contain at least one update path,
2. contain only valid update paths.

Fields that can be updated are marked as ``Modifiable``.
For additional information see the documentation for standard protobuf3's ``google.protobuf.FieldMask``.
Required 

.. _com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigResponse:

UpdateIdentityProviderConfigResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.UpdateIdentityProviderConfigResponse.identity_provider_config:

``identity_provider_config`` : :ref:`IdentityProviderConfig <com.daml.ledger.api.v2.admin.IdentityProviderConfig>`

Updated identity provider config 


----

.. _com/daml/ledger/api/v2/admin/metering_report_service.proto:

``com/daml/ledger/api/v2/admin/metering_report_service.proto``

.. _com.daml.ledger.api.v2.admin.MeteringReportService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MeteringReportService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Experimental API to retrieve metering reports.

Metering reports aim to provide the information necessary for billing participant
and application operators.

.. _com.daml.ledger.api.v2.admin.MeteringReportService.GetMeteringReport:

GetMeteringReport method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Retrieve a metering report.

* Request: :ref:`GetMeteringReportRequest <com.daml.ledger.api.v2.admin.GetMeteringReportRequest>`
* Response: :ref:`GetMeteringReportResponse <com.daml.ledger.api.v2.admin.GetMeteringReportResponse>`



.. _com.daml.ledger.api.v2.admin.GetMeteringReportRequest:

GetMeteringReportRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Authorized if and only if the authenticated user is a participant admin.

.. _com.daml.ledger.api.v2.admin.GetMeteringReportRequest.from:

``from`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The from timestamp (inclusive).
Required. 

.. _com.daml.ledger.api.v2.admin.GetMeteringReportRequest.to:

``to`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The to timestamp (exclusive).
If not provided, the server will default to its current time. 

.. _com.daml.ledger.api.v2.admin.GetMeteringReportRequest.application_id:

``application_id`` : :ref:`string <string>`

If set to a non-empty value, then the report will only be generated for that application.
Optional. 

.. _com.daml.ledger.api.v2.admin.GetMeteringReportResponse:

GetMeteringReportResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetMeteringReportResponse.request:

``request`` : :ref:`GetMeteringReportRequest <com.daml.ledger.api.v2.admin.GetMeteringReportRequest>`

The actual request that was executed. 

.. _com.daml.ledger.api.v2.admin.GetMeteringReportResponse.report_generation_time:

``report_generation_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time at which the report was computed. 

.. _com.daml.ledger.api.v2.admin.GetMeteringReportResponse.metering_report_json:

``metering_report_json`` :  `google.protobuf.Struct <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#struct>`__

The metering report json.  For a JSON Schema definition of the JSon see:
https://github.com/digital-asset/daml/blob/main/ledger-api/grpc-definitions/metering-report-schema.json 


----

.. _com/daml/ledger/api/v2/admin/object_meta.proto:

``com/daml/ledger/api/v2/admin/object_meta.proto``

.. _com.daml.ledger.api.v2.admin.ObjectMeta:

ObjectMeta message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Represents metadata corresponding to a participant resource (e.g. a participant user or participant local information about a party).

Based on ``ObjectMeta`` meta used in Kubernetes API.
See https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/generated.proto#L640

.. _com.daml.ledger.api.v2.admin.ObjectMeta.resource_version:

``resource_version`` : :ref:`string <string>`

An opaque, non-empty value, populated by a participant server which represents the internal version of the resource
this ``ObjectMeta`` message is attached to. The participant server will change it to a unique value each time the corresponding resource is updated.
You must not rely on the format of resource version. The participant server might change it without notice.
You can obtain the newest resource version value by issuing a read request.
You may use it for concurrent change detection by passing it back unmodified in an update request.
The participant server will then compare the passed value with the value maintained by the system to determine
if any other updates took place since you had read the resource version.
Upon a successful update you are guaranteed that no other update took place during your read-modify-write sequence.
However, if another update took place during your read-modify-write sequence then your update will fail with an appropriate error.
Concurrent change control is optional. It will be applied only if you include a resource version in an update request.
When creating a new instance of a resource you must leave the resource version empty.
Its value will be populated by the participant server upon successful resource creation.
Optional 

.. _com.daml.ledger.api.v2.admin.ObjectMeta.annotations:

``annotations`` : :ref:`ObjectMeta.AnnotationsEntry <com.daml.ledger.api.v2.admin.ObjectMeta.AnnotationsEntry>` (repeated)

A set of modifiable key-value pairs that can be used to represent arbitrary, client-specific metadata.
Constraints:

1. The total size over all keys and values cannot exceed 256kb in UTF-8 encoding.
2. Keys are composed of an optional prefix segment and a required name segment such that:

   - key prefix, when present, must be a valid DNS subdomain with at most 253 characters, followed by a '/' (forward slash) character,
   - name segment must have at most 63 characters that are either alphanumeric ([a-z0-9A-Z]), or a '.' (dot), '-' (dash) or '_' (underscore);
     and it must start and end with an alphanumeric character.

3. Values can be any non-empty strings.

Keys with empty prefix are reserved for end-users.
Properties set by external tools or internally by the participant server must use non-empty key prefixes.
Duplicate keys are disallowed by the semantics of the protobuf3 maps.
See: https://developers.google.com/protocol-buffers/docs/proto3#maps
Annotations may be a part of a modifiable resource.
Use the resource's update RPC to update its annotations.
In order to add a new annotation or update an existing one using an update RPC, provide the desired annotation in the update request.
In order to remove an annotation using an update RPC, provide the target annotation's key but set its value to the empty string in the update request.
Optional
Modifiable 

.. _com.daml.ledger.api.v2.admin.ObjectMeta.AnnotationsEntry:

ObjectMeta.AnnotationsEntry message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ObjectMeta.AnnotationsEntry.key:

``key`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.admin.ObjectMeta.AnnotationsEntry.value:

``value`` : :ref:`string <string>`

 


----

.. _com/daml/ledger/api/v2/admin/package_management_service.proto:

``com/daml/ledger/api/v2/admin/package_management_service.proto``

.. _com.daml.ledger.api.v2.admin.PackageManagementService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PackageManagementService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Status: experimental interface, will change before it is deemed production
ready

Query the Daml-LF packages supported by the ledger participant and upload
DAR files. We use 'backing participant' to refer to this specific participant
in the methods of this API.

.. _com.daml.ledger.api.v2.admin.PackageManagementService.ListKnownPackages:

ListKnownPackages method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Returns the details of all Daml-LF packages known to the backing participant.

* Request: :ref:`ListKnownPackagesRequest <com.daml.ledger.api.v2.admin.ListKnownPackagesRequest>`
* Response: :ref:`ListKnownPackagesResponse <com.daml.ledger.api.v2.admin.ListKnownPackagesResponse>`



.. _com.daml.ledger.api.v2.admin.PackageManagementService.UploadDarFile:

UploadDarFile method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Upload a DAR file to the backing participant.
Depending on the ledger implementation this might also make the package
available on the whole ledger. This call might not be supported by some
ledger implementations. Canton could be an example, where uploading a DAR
is not sufficient to render it usable, it must be activated first.
This call may:

- Succeed, if the package was successfully uploaded, or if the same package
  was already uploaded before.
- Respond with a gRPC error

* Request: :ref:`UploadDarFileRequest <com.daml.ledger.api.v2.admin.UploadDarFileRequest>`
* Response: :ref:`UploadDarFileResponse <com.daml.ledger.api.v2.admin.UploadDarFileResponse>`



.. _com.daml.ledger.api.v2.admin.PackageManagementService.ValidateDarFile:

ValidateDarFile method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Performs the same checks that UploadDarFile call perform, but doesn't
upload the DAR and does not make it available on the whole ledger.
This call may:
- Succeed if the package is valid
- Respond with a gRPC error if the package is not valid

* Request: :ref:`ValidateDarFileRequest <com.daml.ledger.api.v2.admin.ValidateDarFileRequest>`
* Response: :ref:`ValidateDarFileResponse <com.daml.ledger.api.v2.admin.ValidateDarFileResponse>`



.. _com.daml.ledger.api.v2.admin.ListKnownPackagesRequest:

ListKnownPackagesRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.admin.ListKnownPackagesResponse:

ListKnownPackagesResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ListKnownPackagesResponse.package_details:

``package_details`` : :ref:`PackageDetails <com.daml.ledger.api.v2.admin.PackageDetails>` (repeated)

The details of all Daml-LF packages known to backing participant.
Required 

.. _com.daml.ledger.api.v2.admin.PackageDetails:

PackageDetails message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.PackageDetails.package_id:

``package_id`` : :ref:`string <string>`

The identity of the Daml-LF package.
Must be a valid PackageIdString (as describe in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.admin.PackageDetails.package_size:

``package_size`` : :ref:`uint64 <uint64>`

Size of the package in bytes.
The size of the package is given by the size of the ``daml_lf``
ArchivePayload. See further details in ``daml_lf.proto``.
Required 

.. _com.daml.ledger.api.v2.admin.PackageDetails.known_since:

``known_since`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Indicates since when the package is known to the backing participant.
Required 

.. _com.daml.ledger.api.v2.admin.PackageDetails.name:

``name`` : :ref:`string <string>`

Name of the package as defined by the package metadata 

.. _com.daml.ledger.api.v2.admin.PackageDetails.version:

``version`` : :ref:`string <string>`

Version of the package as defined by the package metadata 

.. _com.daml.ledger.api.v2.admin.UploadDarFileRequest:

UploadDarFileRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.UploadDarFileRequest.dar_file:

``dar_file`` : :ref:`bytes <bytes>`

Contains a Daml archive DAR file, which in turn is a jar like zipped
container for ``daml_lf`` archives. See further details in
``daml_lf.proto``.
Required 

.. _com.daml.ledger.api.v2.admin.UploadDarFileRequest.submission_id:

``submission_id`` : :ref:`string <string>`

Unique submission identifier.
Optional, defaults to a random identifier. 

.. _com.daml.ledger.api.v2.admin.UploadDarFileResponse:

UploadDarFileResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

A message that is received when the upload operation succeeded.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.ValidateDarFileRequest:

ValidateDarFileRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Performs the same checks that UploadDarFileRequest would perform, but doesn't
upload the DAR.

.. _com.daml.ledger.api.v2.admin.ValidateDarFileRequest.dar_file:

``dar_file`` : :ref:`bytes <bytes>`

Contains a Daml archive DAR file, which in turn is a jar like zipped
container for ``daml_lf`` archives. See further details in
``daml_lf.proto``.
Required 

.. _com.daml.ledger.api.v2.admin.ValidateDarFileRequest.submission_id:

``submission_id`` : :ref:`string <string>`

Unique submission identifier.
Optional, defaults to a random identifier. 

.. _com.daml.ledger.api.v2.admin.ValidateDarFileResponse:

ValidateDarFileResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



Message has no fields.


----

.. _com/daml/ledger/api/v2/admin/participant_pruning_service.proto:

``com/daml/ledger/api/v2/admin/participant_pruning_service.proto``

.. _com.daml.ledger.api.v2.admin.ParticipantPruningService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ParticipantPruningService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Prunes/truncates the "oldest" transactions from the participant (the participant Ledger Api Server plus any other
participant-local state) by removing a portion of the ledger in such a way that the set of future, allowed
commands are not affected.

This enables:

1. keeping the "inactive" portion of the ledger to a manageable size and
2. removing inactive state to honor the right to be forgotten.

.. _com.daml.ledger.api.v2.admin.ParticipantPruningService.Prune:

Prune method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Prune the ledger specifying the offset before and at which ledger transactions should be removed. Only returns when
the potentially long-running prune request ends successfully or with an error.

* Request: :ref:`PruneRequest <com.daml.ledger.api.v2.admin.PruneRequest>`
* Response: :ref:`PruneResponse <com.daml.ledger.api.v2.admin.PruneResponse>`



.. _com.daml.ledger.api.v2.admin.PruneRequest:

PruneRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.PruneRequest.prune_up_to:

``prune_up_to`` : :ref:`int64 <int64>`

Inclusive valid absolute offset (positive integer) up to which the ledger is to be pruned.
By default the following data is pruned:

1. All normal and divulged contracts that have been archived before
   `prune_up_to`.
2. All transaction events and completions before `prune_up_to` 

.. _com.daml.ledger.api.v2.admin.PruneRequest.submission_id:

``submission_id`` : :ref:`string <string>`

Unique submission identifier.
Optional, defaults to a random identifier, used for logging. 

.. _com.daml.ledger.api.v2.admin.PruneRequest.prune_all_divulged_contracts:

``prune_all_divulged_contracts`` : :ref:`bool <bool>`

Prune all immediately and retroactively divulged contracts created before `prune_up_to`
independent of whether they were archived before `prune_up_to`. Useful to avoid leaking
storage on participant nodes that can see a divulged contract but not its archival.

Application developers SHOULD write their Daml applications
such that they do not rely on divulged contracts; i.e., no warnings from
using divulged contracts as inputs to transactions are emitted.

Participant node operators SHOULD set the `prune_all_divulged_contracts` flag to avoid leaking
storage due to accumulating unarchived divulged contracts PROVIDED that:

1. no application using this participant node relies on divulgence OR
2. divulged contracts on which applications rely have been re-divulged after the `prune_up_to` offset. 

.. _com.daml.ledger.api.v2.admin.PruneResponse:

PruneResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Empty for now, but may contain fields in the future

Message has no fields.


----

.. _com/daml/ledger/api/v2/admin/party_management_service.proto:

``com/daml/ledger/api/v2/admin/party_management_service.proto``

.. _com.daml.ledger.api.v2.admin.PartyManagementService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PartyManagementService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

This service allows inspecting the party management state of the ledger known to the participant
and managing the participant-local party metadata.

The authorization rules for its RPCs are specified on the ``<RpcName>Request``
messages as boolean expressions over these facts:

1. ``HasRight(r)`` denoting whether the authenticated user has right ``r`` and
2. ``IsAuthenticatedIdentityProviderAdmin(idp)`` denoting whether ``idp`` is equal to the ``identity_provider_id``
   of the authenticated user and the user has an IdentityProviderAdmin right.

If `identity_provider_id` is set to an empty string, then it's effectively set to the value of access token's 'iss' field if that is provided.
If `identity_provider_id` remains an empty string, the default identity provider will be assumed.

The fields of request messages (and sub-messages) are marked either as ``Optional`` or ``Required``:

1. ``Optional`` denoting the client may leave the field unset when sending a request.
2. ``Required`` denoting the client must set the field to a non-default value when sending a request.

A party details resource is described by the ``PartyDetails`` message,
A party details resource, once it has been created, can be modified using the ``UpdatePartyDetails`` RPC.
The only fields that can be modified are those marked as ``Modifiable``.

.. _com.daml.ledger.api.v2.admin.PartyManagementService.GetParticipantId:

GetParticipantId method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Return the identifier of the participant.
All horizontally scaled replicas should return the same id.
daml-on-kv-ledger: returns an identifier supplied on command line at launch time
canton: returns globally unique identifier of the participant

* Request: :ref:`GetParticipantIdRequest <com.daml.ledger.api.v2.admin.GetParticipantIdRequest>`
* Response: :ref:`GetParticipantIdResponse <com.daml.ledger.api.v2.admin.GetParticipantIdResponse>`



.. _com.daml.ledger.api.v2.admin.PartyManagementService.GetParties:

GetParties method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Get the party details of the given parties. Only known parties will be
returned in the list.

* Request: :ref:`GetPartiesRequest <com.daml.ledger.api.v2.admin.GetPartiesRequest>`
* Response: :ref:`GetPartiesResponse <com.daml.ledger.api.v2.admin.GetPartiesResponse>`



.. _com.daml.ledger.api.v2.admin.PartyManagementService.ListKnownParties:

ListKnownParties method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

List the parties known by the participant.
The list returned contains parties whose ledger access is facilitated by
the participant and the ones maintained elsewhere.

* Request: :ref:`ListKnownPartiesRequest <com.daml.ledger.api.v2.admin.ListKnownPartiesRequest>`
* Response: :ref:`ListKnownPartiesResponse <com.daml.ledger.api.v2.admin.ListKnownPartiesResponse>`



.. _com.daml.ledger.api.v2.admin.PartyManagementService.AllocateParty:

AllocateParty method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Allocates a new party on a ledger and adds it to the set managed by the participant.
Caller specifies a party identifier suggestion, the actual identifier
allocated might be different and is implementation specific.
Caller can specify party metadata that is stored locally on the participant.
This call may:

- Succeed, in which case the actual allocated identifier is visible in
  the response.
- Respond with a gRPC error

daml-on-kv-ledger: suggestion's uniqueness is checked by the validators in
the consensus layer and call rejected if the identifier is already present.
canton: completely different globally unique identifier is allocated.
Behind the scenes calls to an internal protocol are made. As that protocol
is richer than the surface protocol, the arguments take implicit values
The party identifier suggestion must be a valid party name. Party names are required to be non-empty US-ASCII strings built from letters, digits, space,
colon, minus and underscore limited to 255 chars

* Request: :ref:`AllocatePartyRequest <com.daml.ledger.api.v2.admin.AllocatePartyRequest>`
* Response: :ref:`AllocatePartyResponse <com.daml.ledger.api.v2.admin.AllocatePartyResponse>`



.. _com.daml.ledger.api.v2.admin.PartyManagementService.UpdatePartyDetails:

UpdatePartyDetails method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Update selected modifiable participant-local attributes of a party details resource.
Can update the participant's local information for local parties.

* Request: :ref:`UpdatePartyDetailsRequest <com.daml.ledger.api.v2.admin.UpdatePartyDetailsRequest>`
* Response: :ref:`UpdatePartyDetailsResponse <com.daml.ledger.api.v2.admin.UpdatePartyDetailsResponse>`



.. _com.daml.ledger.api.v2.admin.PartyManagementService.UpdatePartyIdentityProviderId:

UpdatePartyIdentityProviderId method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Update the assignment of a party from one IDP to another.

* Request: :ref:`UpdatePartyIdentityProviderIdRequest <com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdRequest>`
* Response: :ref:`UpdatePartyIdentityProviderIdResponse <com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdResponse>`



.. _com.daml.ledger.api.v2.admin.AllocatePartyRequest:

AllocatePartyRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.AllocatePartyRequest.party_id_hint:

``party_id_hint`` : :ref:`string <string>`

A hint to the participant which party ID to allocate. It can be
ignored.
Must be a valid PartyIdString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.admin.AllocatePartyRequest.local_metadata:

``local_metadata`` : :ref:`ObjectMeta <com.daml.ledger.api.v2.admin.ObjectMeta>`

Participant-local metadata to be stored in the ``PartyDetails`` of this newly allocated party.
Optional 

.. _com.daml.ledger.api.v2.admin.AllocatePartyRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the party is managed by the default identity provider or party is not hosted by the participant. 

.. _com.daml.ledger.api.v2.admin.AllocatePartyResponse:

AllocatePartyResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.AllocatePartyResponse.party_details:

``party_details`` : :ref:`PartyDetails <com.daml.ledger.api.v2.admin.PartyDetails>`

 

.. _com.daml.ledger.api.v2.admin.GetParticipantIdRequest:

GetParticipantIdRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin)``

Message has no fields.

.. _com.daml.ledger.api.v2.admin.GetParticipantIdResponse:

GetParticipantIdResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetParticipantIdResponse.participant_id:

``participant_id`` : :ref:`string <string>`

Identifier of the participant, which SHOULD be globally unique.
Must be a valid LedgerString (as describe in ``value.proto``). 

.. _com.daml.ledger.api.v2.admin.GetPartiesRequest:

GetPartiesRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.GetPartiesRequest.parties:

``parties`` : :ref:`string <string>` (repeated)

The stable, unique identifier of the Daml parties.
Must be valid PartyIdStrings (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.admin.GetPartiesRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider`` whose parties should be retrieved.
Optional, if not set, assume the party is managed by the default identity provider or party is not hosted by the participant. 

.. _com.daml.ledger.api.v2.admin.GetPartiesResponse:

GetPartiesResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetPartiesResponse.party_details:

``party_details`` : :ref:`PartyDetails <com.daml.ledger.api.v2.admin.PartyDetails>` (repeated)

The details of the requested Daml parties by the participant, if known.
The party details may not be in the same order as requested.
Required 

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesRequest:

ListKnownPartiesRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesRequest.page_token:

``page_token`` : :ref:`string <string>`

Pagination token to determine the specific page to fetch. Using the token guarantees that parties on a subsequent
page are all lexically greater than the last party on a previous page. Server does not store intermediate results
between calls chained by a series of page tokens. As a consequence, if new parties are being added and a page is
requested twice using the same token, more parties can be returned on the second call.
Leave empty to fetch the first page.
Optional 

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesRequest.page_size:

``page_size`` : :ref:`int32 <int32>`

Maximum number of results to be returned by the server. The server will return no more than that many results,
but it might return fewer. If the page_size is 0, the server will decide the number of results to be returned.
If the page_size exceeds the maximum supported by the server, an error will be returned. To obtain the server's
maximum consult the PartyManagementFeature descriptor available in the VersionService.
Optional 

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider`` whose parties should be retrieved.
Optional, if not set, assume the party is managed by the default identity provider or party is not hosted by the participant. 

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesResponse:

ListKnownPartiesResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ListKnownPartiesResponse.party_details:

``party_details`` : :ref:`PartyDetails <com.daml.ledger.api.v2.admin.PartyDetails>` (repeated)

The details of all Daml parties known by the participant.
Required 

.. _com.daml.ledger.api.v2.admin.ListKnownPartiesResponse.next_page_token:

``next_page_token`` : :ref:`string <string>`

Pagination token to retrieve the next page.
Empty, if there are no further results. 

.. _com.daml.ledger.api.v2.admin.PartyDetails:

PartyDetails message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.PartyDetails.party:

``party`` : :ref:`string <string>`

The stable unique identifier of a Daml party.
Must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.admin.PartyDetails.is_local:

``is_local`` : :ref:`bool <bool>`

true if party is hosted by the participant and the party shares the same identity provider as the user issuing the request.
Optional 

.. _com.daml.ledger.api.v2.admin.PartyDetails.local_metadata:

``local_metadata`` : :ref:`ObjectMeta <com.daml.ledger.api.v2.admin.ObjectMeta>`

Participant-local metadata of this party.
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.PartyDetails.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, there could be 3 options:

1. the party is managed by the default identity provider.
2. party is not hosted by the participant.
3. party is hosted by the participant, but is outside of the user's identity provider. 

.. _com.daml.ledger.api.v2.admin.UpdatePartyDetailsRequest:

UpdatePartyDetailsRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(party_details.identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.UpdatePartyDetailsRequest.party_details:

``party_details`` : :ref:`PartyDetails <com.daml.ledger.api.v2.admin.PartyDetails>`

Party to be updated
Required,
Modifiable 

.. _com.daml.ledger.api.v2.admin.UpdatePartyDetailsRequest.update_mask:

``update_mask`` :  `google.protobuf.FieldMask <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#fieldmask>`__

An update mask specifies how and which properties of the ``PartyDetails`` message are to be updated.
An update mask consists of a set of update paths.
A valid update path points to a field or a subfield relative to the ``PartyDetails`` message.
A valid update mask must:

1. contain at least one update path,
2. contain only valid update paths.

Fields that can be updated are marked as ``Modifiable``.
An update path can also point to non-``Modifiable`` fields such as 'party' and 'local_metadata.resource_version'
because they are used:

1. to identify the party details resource subject to the update,
2. for concurrent change control.

An update path can also point to non-``Modifiable`` fields such as 'is_local'
as long as the values provided in the update request match the server values.
Examples of update paths: 'local_metadata.annotations', 'local_metadata'.
For additional information see the documentation for standard protobuf3's ``google.protobuf.FieldMask``.
For similar Ledger API see ``com.daml.ledger.api.v2.admin.UpdateUserRequest``.
Required 

.. _com.daml.ledger.api.v2.admin.UpdatePartyDetailsResponse:

UpdatePartyDetailsResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.UpdatePartyDetailsResponse.party_details:

``party_details`` : :ref:`PartyDetails <com.daml.ledger.api.v2.admin.PartyDetails>`

Updated party details 

.. _com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdRequest:

UpdatePartyIdentityProviderIdRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin)``

.. _com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdRequest.party:

``party`` : :ref:`string <string>`

Party to update 

.. _com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdRequest.source_identity_provider_id:

``source_identity_provider_id`` : :ref:`string <string>`

Current identity provider id of the party 

.. _com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdRequest.target_identity_provider_id:

``target_identity_provider_id`` : :ref:`string <string>`

Target identity provider id of the party 

.. _com.daml.ledger.api.v2.admin.UpdatePartyIdentityProviderIdResponse:

UpdatePartyIdentityProviderIdResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



Message has no fields.


----

.. _com/daml/ledger/api/v2/admin/user_management_service.proto:

``com/daml/ledger/api/v2/admin/user_management_service.proto``

.. _com.daml.ledger.api.v2.admin.UserManagementService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UserManagementService, |version com.daml.ledger.api.v2.admin|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Service to manage users and their rights for interacting with the Ledger API
served by a participant node.

The authorization rules for its RPCs are specified on the ``<RpcName>Request``
messages as boolean expressions over these facts:
(1) ``HasRight(r)`` denoting whether the authenticated user has right ``r`` and
(2) ``IsAuthenticatedUser(uid)`` denoting whether ``uid`` is the empty string or equal to the id of the authenticated user.
(3) ``IsAuthenticatedIdentityProviderAdmin(idp)`` denoting whether ``idp`` is equal to the ``identity_provider_id``
of the authenticated user and the user has an IdentityProviderAdmin right.
If `user_id` is set to the empty string (the default), then the data for the authenticated user will be retrieved.
If `identity_provider_id` is set to an empty string, then it's effectively set to the value of access token's 'iss' field if that is provided.
If `identity_provider_id` remains an empty string, the default identity provider will be assumed.

The fields of request messages (and sub-messages) are marked either as ``Optional`` or ``Required``:
(1) ``Optional`` denoting the client may leave the field unset when sending a request.
(2) ``Required`` denoting the client must set the field to a non-default value when sending a request.

A user resource consists of:
(1) a set of properties represented by the ``User`` message,
(2) a set of user rights, where each right is represented by the ``Right`` message.

A user resource, once it has been created, can be modified.
In order to update the properties represented by the ``User`` message use the ``UpdateUser`` RPC. The only fields that can be modified are those marked as ``Modifiable``.
In order to grant or revoke user rights use ``GrantRights' and ``RevokeRights`` RPCs.

.. _com.daml.ledger.api.v2.admin.UserManagementService.CreateUser:

CreateUser method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Create a new user.

* Request: :ref:`CreateUserRequest <com.daml.ledger.api.v2.admin.CreateUserRequest>`
* Response: :ref:`CreateUserResponse <com.daml.ledger.api.v2.admin.CreateUserResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.GetUser:

GetUser method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Get the user data of a specific user or the authenticated user.

* Request: :ref:`GetUserRequest <com.daml.ledger.api.v2.admin.GetUserRequest>`
* Response: :ref:`GetUserResponse <com.daml.ledger.api.v2.admin.GetUserResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.UpdateUser:

UpdateUser method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Update selected modifiable attribute of a user resource described by the ``User`` message.

* Request: :ref:`UpdateUserRequest <com.daml.ledger.api.v2.admin.UpdateUserRequest>`
* Response: :ref:`UpdateUserResponse <com.daml.ledger.api.v2.admin.UpdateUserResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.DeleteUser:

DeleteUser method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Delete an existing user and all its rights.

* Request: :ref:`DeleteUserRequest <com.daml.ledger.api.v2.admin.DeleteUserRequest>`
* Response: :ref:`DeleteUserResponse <com.daml.ledger.api.v2.admin.DeleteUserResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.ListUsers:

ListUsers method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

List all existing users.

* Request: :ref:`ListUsersRequest <com.daml.ledger.api.v2.admin.ListUsersRequest>`
* Response: :ref:`ListUsersResponse <com.daml.ledger.api.v2.admin.ListUsersResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.GrantUserRights:

GrantUserRights method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Grant rights to a user.
Granting rights does not affect the resource version of the corresponding user.

* Request: :ref:`GrantUserRightsRequest <com.daml.ledger.api.v2.admin.GrantUserRightsRequest>`
* Response: :ref:`GrantUserRightsResponse <com.daml.ledger.api.v2.admin.GrantUserRightsResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.RevokeUserRights:

RevokeUserRights method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Revoke rights from a user.
Revoking rights does not affect the resource version of the corresponding user.

* Request: :ref:`RevokeUserRightsRequest <com.daml.ledger.api.v2.admin.RevokeUserRightsRequest>`
* Response: :ref:`RevokeUserRightsResponse <com.daml.ledger.api.v2.admin.RevokeUserRightsResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.ListUserRights:

ListUserRights method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

List the set of all rights granted to a user.

* Request: :ref:`ListUserRightsRequest <com.daml.ledger.api.v2.admin.ListUserRightsRequest>`
* Response: :ref:`ListUserRightsResponse <com.daml.ledger.api.v2.admin.ListUserRightsResponse>`



.. _com.daml.ledger.api.v2.admin.UserManagementService.UpdateUserIdentityProviderId:

UpdateUserIdentityProviderId method, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Update the assignment of a user from one IDP to another.

* Request: :ref:`UpdateUserIdentityProviderIdRequest <com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdRequest>`
* Response: :ref:`UpdateUserIdentityProviderIdResponse <com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdResponse>`



.. _com.daml.ledger.api.v2.admin.CreateUserRequest:

CreateUserRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(user.identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.CreateUserRequest.user:

``user`` : :ref:`User <com.daml.ledger.api.v2.admin.User>`

The user to create.
Required 

.. _com.daml.ledger.api.v2.admin.CreateUserRequest.rights:

``rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

The rights to be assigned to the user upon creation,
which SHOULD include appropriate rights for the ``user.primary_party``.
Optional 

.. _com.daml.ledger.api.v2.admin.CreateUserResponse:

CreateUserResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.CreateUserResponse.user:

``user`` : :ref:`User <com.daml.ledger.api.v2.admin.User>`

Created user. 

.. _com.daml.ledger.api.v2.admin.DeleteUserRequest:

DeleteUserRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.DeleteUserRequest.user_id:

``user_id`` : :ref:`string <string>`

The user to delete.
Required 

.. _com.daml.ledger.api.v2.admin.DeleteUserRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.DeleteUserResponse:

DeleteUserResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Does not (yet) contain any data.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.GetUserRequest:

GetUserRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id) OR IsAuthenticatedUser(user_id)``

.. _com.daml.ledger.api.v2.admin.GetUserRequest.user_id:

``user_id`` : :ref:`string <string>`

The user whose data to retrieve.
If set to empty string (the default), then the data for the authenticated user will be retrieved.
Optional 

.. _com.daml.ledger.api.v2.admin.GetUserRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.GetUserResponse:

GetUserResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GetUserResponse.user:

``user`` : :ref:`User <com.daml.ledger.api.v2.admin.User>`

Retrieved user. 

.. _com.daml.ledger.api.v2.admin.GrantUserRightsRequest:

GrantUserRightsRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Add the rights to the set of rights granted to the user.

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.GrantUserRightsRequest.user_id:

``user_id`` : :ref:`string <string>`

The user to whom to grant rights.
Required 

.. _com.daml.ledger.api.v2.admin.GrantUserRightsRequest.rights:

``rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

The rights to grant.
Optional 

.. _com.daml.ledger.api.v2.admin.GrantUserRightsRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.GrantUserRightsResponse:

GrantUserRightsResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.GrantUserRightsResponse.newly_granted_rights:

``newly_granted_rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

The rights that were newly granted by the request. 

.. _com.daml.ledger.api.v2.admin.ListUserRightsRequest:

ListUserRightsRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id) OR IsAuthenticatedUser(user_id)``

.. _com.daml.ledger.api.v2.admin.ListUserRightsRequest.user_id:

``user_id`` : :ref:`string <string>`

The user for which to list the rights.
If set to empty string (the default), then the rights for the authenticated user will be listed.
Required 

.. _com.daml.ledger.api.v2.admin.ListUserRightsRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.ListUserRightsResponse:

ListUserRightsResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ListUserRightsResponse.rights:

``rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

All rights of the user. 

.. _com.daml.ledger.api.v2.admin.ListUsersRequest:

ListUsersRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.ListUsersRequest.page_token:

``page_token`` : :ref:`string <string>`

Pagination token to determine the specific page to fetch.
Leave empty to fetch the first page.
Optional 

.. _com.daml.ledger.api.v2.admin.ListUsersRequest.page_size:

``page_size`` : :ref:`int32 <int32>`

Maximum number of results to be returned by the server. The server will return no more than that many results, but it might return fewer.
If 0, the server will decide the number of results to be returned.
Optional 

.. _com.daml.ledger.api.v2.admin.ListUsersRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.ListUsersResponse:

ListUsersResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.ListUsersResponse.users:

``users`` : :ref:`User <com.daml.ledger.api.v2.admin.User>` (repeated)

A subset of users of the participant node that fit into this page. 

.. _com.daml.ledger.api.v2.admin.ListUsersResponse.next_page_token:

``next_page_token`` : :ref:`string <string>`

Pagination token to retrieve the next page.
Empty, if there are no further results. 

.. _com.daml.ledger.api.v2.admin.RevokeUserRightsRequest:

RevokeUserRightsRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Remove the rights from the set of rights granted to the user.

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.RevokeUserRightsRequest.user_id:

``user_id`` : :ref:`string <string>`

The user from whom to revoke rights.
Required 

.. _com.daml.ledger.api.v2.admin.RevokeUserRightsRequest.rights:

``rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

The rights to revoke.
Optional 

.. _com.daml.ledger.api.v2.admin.RevokeUserRightsRequest.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the ``Identity Provider``
Optional, if not set, assume the user is managed by the default identity provider. 

.. _com.daml.ledger.api.v2.admin.RevokeUserRightsResponse:

RevokeUserRightsResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.RevokeUserRightsResponse.newly_revoked_rights:

``newly_revoked_rights`` : :ref:`Right <com.daml.ledger.api.v2.admin.Right>` (repeated)

The rights that were actually revoked by the request. 

.. _com.daml.ledger.api.v2.admin.Right:

Right message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

A right granted to a user.

.. _com.daml.ledger.api.v2.admin.Right.kind.participant_admin:

``oneof kind.participant_admin`` : :ref:`Right.ParticipantAdmin <com.daml.ledger.api.v2.admin.Right.ParticipantAdmin>`

The user can administer the participant node. 

.. _com.daml.ledger.api.v2.admin.Right.kind.can_act_as:

``oneof kind.can_act_as`` : :ref:`Right.CanActAs <com.daml.ledger.api.v2.admin.Right.CanActAs>`

The user can act as a specific party. 

.. _com.daml.ledger.api.v2.admin.Right.kind.can_read_as:

``oneof kind.can_read_as`` : :ref:`Right.CanReadAs <com.daml.ledger.api.v2.admin.Right.CanReadAs>`

The user can read ledger data visible to a specific party. 

.. _com.daml.ledger.api.v2.admin.Right.kind.identity_provider_admin:

``oneof kind.identity_provider_admin`` : :ref:`Right.IdentityProviderAdmin <com.daml.ledger.api.v2.admin.Right.IdentityProviderAdmin>`

The user can administer users and parties assigned to the same identity provider as the one of the user. 

.. _com.daml.ledger.api.v2.admin.Right.kind.can_read_as_any_party:

``oneof kind.can_read_as_any_party`` : :ref:`Right.CanReadAsAnyParty <com.daml.ledger.api.v2.admin.Right.CanReadAsAnyParty>`

The user can read as any party on a participant 

.. _com.daml.ledger.api.v2.admin.Right.CanActAs:

Right.CanActAs message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.Right.CanActAs.party:

``party`` : :ref:`string <string>`

The right to authorize commands for this party. 

.. _com.daml.ledger.api.v2.admin.Right.CanReadAs:

Right.CanReadAs message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.Right.CanReadAs.party:

``party`` : :ref:`string <string>`

The right to read ledger data visible to this party. 

.. _com.daml.ledger.api.v2.admin.Right.CanReadAsAnyParty:

Right.CanReadAsAnyParty message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

The rights of a participant's super reader. Its utility is predominantly for
feeding external tools, such as PQS, continually without the need to change subscriptions
as new parties pop in and out of existence.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.Right.IdentityProviderAdmin:

Right.IdentityProviderAdmin message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

The right to administer the identity provider that the user is assigned to.
It means, being able to manage users and parties that are also assigned
to the same identity provider.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.Right.ParticipantAdmin:

Right.ParticipantAdmin message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

The right to administer the participant node.

Message has no fields.

.. _com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdRequest:

UpdateUserIdentityProviderIdRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin)``

.. _com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdRequest.user_id:

``user_id`` : :ref:`string <string>`

User to update 

.. _com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdRequest.source_identity_provider_id:

``source_identity_provider_id`` : :ref:`string <string>`

Current identity provider id of the user 

.. _com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdRequest.target_identity_provider_id:

``target_identity_provider_id`` : :ref:`string <string>`

Target identity provider id of the user 

.. _com.daml.ledger.api.v2.admin.UpdateUserIdentityProviderIdResponse:

UpdateUserIdentityProviderIdResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.admin.UpdateUserRequest:

UpdateUserRequest message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Required authorization: ``HasRight(ParticipantAdmin) OR IsAuthenticatedIdentityProviderAdmin(user.identity_provider_id)``

.. _com.daml.ledger.api.v2.admin.UpdateUserRequest.user:

``user`` : :ref:`User <com.daml.ledger.api.v2.admin.User>`

The user to update.
Required,
Modifiable 

.. _com.daml.ledger.api.v2.admin.UpdateUserRequest.update_mask:

``update_mask`` :  `google.protobuf.FieldMask <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#fieldmask>`__

An update mask specifies how and which properties of the ``User`` message are to be updated.
An update mask consists of a set of update paths.
A valid update path points to a field or a subfield relative to the ``User`` message.
A valid update mask must:
(1) contain at least one update path,
(2) contain only valid update paths.
Fields that can be updated are marked as ``Modifiable``.
An update path can also point to a non-``Modifiable`` fields such as 'id' and 'metadata.resource_version'
because they are used:
(1) to identify the user resource subject to the update,
(2) for concurrent change control.
Examples of valid update paths: 'primary_party', 'metadata', 'metadata.annotations'.
For additional information see the documentation for standard protobuf3's ``google.protobuf.FieldMask``.
For similar Ledger API see ``com.daml.ledger.api.v2.admin.UpdatePartyDetailsRequest``.
Required 

.. _com.daml.ledger.api.v2.admin.UpdateUserResponse:

UpdateUserResponse message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.admin.UpdateUserResponse.user:

``user`` : :ref:`User <com.daml.ledger.api.v2.admin.User>`

Updated user 

.. _com.daml.ledger.api.v2.admin.User:

User message, |version com.daml.ledger.api.v2.admin|
========================================================================================================================================================================================================

Users are used to dynamically manage the rights given to Daml applications.
They are stored and managed per participant node.

Read the :doc:`Authorization documentation </app-dev/authorization>` to learn more.

.. _com.daml.ledger.api.v2.admin.User.id:

``id`` : :ref:`string <string>`

The user identifier, which must be a non-empty string of at most 128
characters that are either alphanumeric ASCII characters or one of the symbols "@^$.!`-#+'~_|:".
Required 

.. _com.daml.ledger.api.v2.admin.User.primary_party:

``primary_party`` : :ref:`string <string>`

The primary party as which this user reads and acts by default on the ledger
*provided* it has the corresponding ``CanReadAs(primary_party)`` or
``CanActAs(primary_party)`` rights.
Ledger API clients SHOULD set this field to a non-empty value for all users to
enable the users to act on the ledger using their own Daml party.
Users for participant administrators MAY have an associated primary party.
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.User.is_deactivated:

``is_deactivated`` : :ref:`bool <bool>`

When set, then the user is denied all access to the Ledger API.
Otherwise, the user has access to the Ledger API as per the user's rights.
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.User.metadata:

``metadata`` : :ref:`ObjectMeta <com.daml.ledger.api.v2.admin.ObjectMeta>`

The metadata of this user.
Note that the ``metadata.resource_version`` tracks changes to the properties described by the ``User`` message and not the user's rights.
Optional,
Modifiable 

.. _com.daml.ledger.api.v2.admin.User.identity_provider_id:

``identity_provider_id`` : :ref:`string <string>`

The id of the identity provider configured by ``Identity Provider Config``
Optional, if not set, assume the user is managed by the default identity provider. 


----

.. _com/daml/ledger/api/v2/command_completion_service.proto:

``com/daml/ledger/api/v2/command_completion_service.proto``

.. _com.daml.ledger.api.v2.CommandCompletionService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CommandCompletionService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to observe the status of their submissions.
Commands may be submitted via the Command Submission Service.
The on-ledger effects of their submissions are disclosed by the Update Service.

Commands may fail in 2 distinct manners:

1. Failure communicated synchronously in the gRPC error of the submission.
2. Failure communicated asynchronously in a Completion, see ``completion.proto``.

Note that not only successfully submitted commands MAY produce a completion event. For example, the participant MAY
choose to produce a completion event for a rejection of a duplicate command.

Clients that do not receive a successful completion about their submission MUST NOT assume that it was successful.
Clients SHOULD subscribe to the CompletionStream before starting to submit commands to prevent race conditions.

.. _com.daml.ledger.api.v2.CommandCompletionService.CompletionStream:

CompletionStream method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Subscribe to command completion events.

* Request: :ref:`CompletionStreamRequest <com.daml.ledger.api.v2.CompletionStreamRequest>`
* Response: :ref:`CompletionStreamResponse <com.daml.ledger.api.v2.CompletionStreamResponse>`



.. _com.daml.ledger.api.v2.CompletionStreamRequest:

CompletionStreamRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.CompletionStreamRequest.application_id:

``application_id`` : :ref:`string <string>`

Only completions of commands submitted with the same application_id will be visible in the stream.
Must be a valid ApplicationIdString (as described in ``value.proto``).
Required unless authentication is used with a user token or a custom token specifying an application-id.
In that case, the token's user-id, respectively application-id, will be used for the request's application_id. 

.. _com.daml.ledger.api.v2.CompletionStreamRequest.parties:

``parties`` : :ref:`string <string>` (repeated)

Non-empty list of parties whose data should be included.
Only completions of commands for which at least one of the ``act_as`` parties is in the given set of parties
will be visible in the stream.
Must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.CompletionStreamRequest.begin_exclusive:

``begin_exclusive`` : :ref:`int64 <int64>`

This field indicates the minimum offset for completions. This can be used to resume an earlier completion stream.
Optional, if not set the ledger uses the ledger begin offset instead.
If specified, it must be a valid absolute offset (positive integer) or zero (ledger begin offset).
If the ledger has been pruned, this parameter must be specified and greater than the pruning offset. 

.. _com.daml.ledger.api.v2.CompletionStreamResponse:

CompletionStreamResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.CompletionStreamResponse.completion_response.completion:

``oneof completion_response.completion`` : :ref:`Completion <com.daml.ledger.api.v2.Completion>`

 

.. _com.daml.ledger.api.v2.CompletionStreamResponse.completion_response.offset_checkpoint:

``oneof completion_response.offset_checkpoint`` : :ref:`OffsetCheckpoint <com.daml.ledger.api.v2.OffsetCheckpoint>`

 


----

.. _com/daml/ledger/api/v2/command_service.proto:

``com/daml/ledger/api/v2/command_service.proto``

.. _com.daml.ledger.api.v2.CommandService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CommandService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Command Service is able to correlate submitted commands with completion data, identify timeouts, and return contextual
information with each tracking result. This supports the implementation of stateless clients.

Note that submitted commands generally produce completion events as well, even in case a command gets rejected.
For example, the participant SHOULD produce a completion event for a rejection of a duplicate command.

.. _com.daml.ledger.api.v2.CommandService.SubmitAndWait:

SubmitAndWait method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Submits a single composite command and waits for its result.
Propagates the gRPC error of failed submissions including Daml interpretation errors.

* Request: :ref:`SubmitAndWaitRequest <com.daml.ledger.api.v2.SubmitAndWaitRequest>`
* Response: :ref:`SubmitAndWaitResponse <com.daml.ledger.api.v2.SubmitAndWaitResponse>`



.. _com.daml.ledger.api.v2.CommandService.SubmitAndWaitForTransaction:

SubmitAndWaitForTransaction method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Submits a single composite command, waits for its result, and returns the transaction.
Propagates the gRPC error of failed submissions including Daml interpretation errors.

* Request: :ref:`SubmitAndWaitRequest <com.daml.ledger.api.v2.SubmitAndWaitRequest>`
* Response: :ref:`SubmitAndWaitForTransactionResponse <com.daml.ledger.api.v2.SubmitAndWaitForTransactionResponse>`



.. _com.daml.ledger.api.v2.CommandService.SubmitAndWaitForTransactionTree:

SubmitAndWaitForTransactionTree method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Submits a single composite command, waits for its result, and returns the transaction tree.
Propagates the gRPC error of failed submissions including Daml interpretation errors.

* Request: :ref:`SubmitAndWaitRequest <com.daml.ledger.api.v2.SubmitAndWaitRequest>`
* Response: :ref:`SubmitAndWaitForTransactionTreeResponse <com.daml.ledger.api.v2.SubmitAndWaitForTransactionTreeResponse>`



.. _com.daml.ledger.api.v2.SubmitAndWaitForTransactionResponse:

SubmitAndWaitForTransactionResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.SubmitAndWaitForTransactionResponse.transaction:

``transaction`` : :ref:`Transaction <com.daml.ledger.api.v2.Transaction>`

The flat transaction that resulted from the submitted command.
Required 

.. _com.daml.ledger.api.v2.SubmitAndWaitForTransactionTreeResponse:

SubmitAndWaitForTransactionTreeResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.SubmitAndWaitForTransactionTreeResponse.transaction:

``transaction`` : :ref:`TransactionTree <com.daml.ledger.api.v2.TransactionTree>`

The transaction tree that resulted from the submitted command.
Required 

.. _com.daml.ledger.api.v2.SubmitAndWaitRequest:

SubmitAndWaitRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

These commands are atomic, and will become transactions.

.. _com.daml.ledger.api.v2.SubmitAndWaitRequest.commands:

``commands`` : :ref:`Commands <com.daml.ledger.api.v2.Commands>`

The commands to be submitted.
Required 

.. _com.daml.ledger.api.v2.SubmitAndWaitResponse:

SubmitAndWaitResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.SubmitAndWaitResponse.update_id:

``update_id`` : :ref:`string <string>`

The id of the transaction that resulted from the submitted command.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.SubmitAndWaitResponse.completion_offset:

``completion_offset`` : :ref:`int64 <int64>`

The details of the offset field are described in ``community/ledger-api/README.md``.
Required 


----

.. _com/daml/ledger/api/v2/command_submission_service.proto:

``com/daml/ledger/api/v2/command_submission_service.proto``

.. _com.daml.ledger.api.v2.CommandSubmissionService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CommandSubmissionService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to attempt advancing the ledger's state by submitting commands.
The final states of their submissions are disclosed by the Command Completion Service.
The on-ledger effects of their submissions are disclosed by the Update Service.

Commands may fail in 2 distinct manners:

1. Failure communicated synchronously in the gRPC error of the submission.
2. Failure communicated asynchronously in a Completion, see ``completion.proto``.

Note that not only successfully submitted commands MAY produce a completion event. For example, the participant MAY
choose to produce a completion event for a rejection of a duplicate command.

Clients that do not receive a successful completion about their submission MUST NOT assume that it was successful.
Clients SHOULD subscribe to the CompletionStream before starting to submit commands to prevent race conditions.

.. _com.daml.ledger.api.v2.CommandSubmissionService.Submit:

Submit method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Submit a single composite command.

* Request: :ref:`SubmitRequest <com.daml.ledger.api.v2.SubmitRequest>`
* Response: :ref:`SubmitResponse <com.daml.ledger.api.v2.SubmitResponse>`



.. _com.daml.ledger.api.v2.CommandSubmissionService.SubmitReassignment:

SubmitReassignment method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Submit a single reassignment.

* Request: :ref:`SubmitReassignmentRequest <com.daml.ledger.api.v2.SubmitReassignmentRequest>`
* Response: :ref:`SubmitReassignmentResponse <com.daml.ledger.api.v2.SubmitReassignmentResponse>`



.. _com.daml.ledger.api.v2.SubmitReassignmentRequest:

SubmitReassignmentRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.SubmitReassignmentRequest.reassignment_command:

``reassignment_command`` : :ref:`ReassignmentCommand <com.daml.ledger.api.v2.ReassignmentCommand>`

The reassignment command to be submitted.
Required 

.. _com.daml.ledger.api.v2.SubmitReassignmentResponse:

SubmitReassignmentResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.SubmitRequest:

SubmitRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

The submitted commands will be processed atomically in a single transaction. Moreover, each ``Command`` in ``commands`` will be executed in the order specified by the request.

.. _com.daml.ledger.api.v2.SubmitRequest.commands:

``commands`` : :ref:`Commands <com.daml.ledger.api.v2.Commands>`

The commands to be submitted in a single transaction.
Required 

.. _com.daml.ledger.api.v2.SubmitResponse:

SubmitResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Message has no fields.


----

.. _com/daml/ledger/api/v2/commands.proto:

``com/daml/ledger/api/v2/commands.proto``

.. _com.daml.ledger.api.v2.Command:

Command message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A command can either create a new contract or exercise a choice on an existing contract.

.. _com.daml.ledger.api.v2.Command.command.create:

``oneof command.create`` : :ref:`CreateCommand <com.daml.ledger.api.v2.CreateCommand>`

 

.. _com.daml.ledger.api.v2.Command.command.exercise:

``oneof command.exercise`` : :ref:`ExerciseCommand <com.daml.ledger.api.v2.ExerciseCommand>`

 

.. _com.daml.ledger.api.v2.Command.command.exercise_by_key:

``oneof command.exercise_by_key`` : :ref:`ExerciseByKeyCommand <com.daml.ledger.api.v2.ExerciseByKeyCommand>`

 

.. _com.daml.ledger.api.v2.Command.command.create_and_exercise:

``oneof command.create_and_exercise`` : :ref:`CreateAndExerciseCommand <com.daml.ledger.api.v2.CreateAndExerciseCommand>`

 

.. _com.daml.ledger.api.v2.Commands:

Commands message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A composite command that groups multiple commands together.

.. _com.daml.ledger.api.v2.Commands.workflow_id:

``workflow_id`` : :ref:`string <string>`

Identifier of the on-ledger workflow that this command is a part of.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Commands.application_id:

``application_id`` : :ref:`string <string>`

Uniquely identifies the application or participant user that issued the command.
Must be a valid ApplicationIdString (as described in ``value.proto``).
Required unless authentication is used with a user token or a custom token specifying an application-id.
In that case, the token's user-id, respectively application-id, will be used for the request's application_id. 

.. _com.daml.ledger.api.v2.Commands.command_id:

``command_id`` : :ref:`string <string>`

Uniquely identifies the command.
The triple (application_id, act_as, command_id) constitutes the change ID for the intended ledger change,
where act_as is interpreted as a set of party names.
The change ID can be used for matching the intended ledger changes with all their completions.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.Commands.commands:

``commands`` : :ref:`Command <com.daml.ledger.api.v2.Command>` (repeated)

Individual elements of this atomic command. Must be non-empty.
Required 

.. _com.daml.ledger.api.v2.Commands.deduplication_period.deduplication_duration:

``oneof deduplication_period.deduplication_duration`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

Specifies the length of the deduplication period.
It is interpreted relative to the local clock at some point during the submission's processing.
Must be non-negative. Must not exceed the maximum deduplication time. 

.. _com.daml.ledger.api.v2.Commands.deduplication_period.deduplication_offset:

``oneof deduplication_period.deduplication_offset`` : :ref:`int64 <int64>`

Specifies the start of the deduplication period by a completion stream offset (exclusive).
Must be a valid absolute offset (positive integer) or participant begin (zero). 

.. _com.daml.ledger.api.v2.Commands.min_ledger_time_abs:

``min_ledger_time_abs`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Lower bound for the ledger time assigned to the resulting transaction.
Note: The ledger time of a transaction is assigned as part of command interpretation.
Use this property if you expect that command interpretation will take a considerate amount of time, such that by
the time the resulting transaction is sequenced, its assigned ledger time is not valid anymore.
Must not be set at the same time as min_ledger_time_rel.
Optional 

.. _com.daml.ledger.api.v2.Commands.min_ledger_time_rel:

``min_ledger_time_rel`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

Same as min_ledger_time_abs, but specified as a duration, starting from the time the command is received by the server.
Must not be set at the same time as min_ledger_time_abs.
Optional 

.. _com.daml.ledger.api.v2.Commands.act_as:

``act_as`` : :ref:`string <string>` (repeated)

Set of parties on whose behalf the command should be executed.
If ledger API authorization is enabled, then the authorization metadata must authorize the sender of the request
to act on behalf of each of the given parties.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Required, must be non-empty. 

.. _com.daml.ledger.api.v2.Commands.read_as:

``read_as`` : :ref:`string <string>` (repeated)

Set of parties on whose behalf (in addition to all parties listed in ``act_as``) contracts can be retrieved.
This affects Daml operations such as ``fetch``, ``fetchByKey``, ``lookupByKey``, ``exercise``, and ``exerciseByKey``.
Note: A participant node of a Daml network can host multiple parties. Each contract present on the participant
node is only visible to a subset of these parties. A command can only use contracts that are visible to at least
one of the parties in ``act_as`` or ``read_as``. This visibility check is independent from the Daml authorization
rules for fetch operations.
If ledger API authorization is enabled, then the authorization metadata must authorize the sender of the request
to read contract data on behalf of each of the given parties.
Optional 

.. _com.daml.ledger.api.v2.Commands.submission_id:

``submission_id`` : :ref:`string <string>`

A unique identifier to distinguish completions for different submissions with the same change ID.
Typically a random UUID. Applications are expected to use a different UUID for each retry of a submission
with the same change ID.
Must be a valid LedgerString (as described in ``value.proto``).

If omitted, the participant or the committer may set a value of their choice.
Optional 

.. _com.daml.ledger.api.v2.Commands.disclosed_contracts:

``disclosed_contracts`` : :ref:`DisclosedContract <com.daml.ledger.api.v2.DisclosedContract>` (repeated)

Additional contracts used to resolve contract & contract key lookups.
Optional 

.. _com.daml.ledger.api.v2.Commands.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

Must be a valid synchronizer id
Optional 

.. _com.daml.ledger.api.v2.Commands.package_id_selection_preference:

``package_id_selection_preference`` : :ref:`string <string>` (repeated)

The package-id selection preference of the client for resolving
package names and interface instances in command submission and interpretation 

.. _com.daml.ledger.api.v2.Commands.prefetch_contract_keys:

``prefetch_contract_keys`` : :ref:`PrefetchContractKey <com.daml.ledger.api.v2.PrefetchContractKey>` (repeated)

Fetches the contract keys into the caches to speed up the command processing.
Should only contain contract keys that are expected to be resolved during interpretation of the commands.
Keys of disclosed contracts do not need prefetching.

Optional 

.. _com.daml.ledger.api.v2.CreateAndExerciseCommand:

CreateAndExerciseCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Create a contract and exercise a choice on it in the same transaction.

.. _com.daml.ledger.api.v2.CreateAndExerciseCommand.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of the contract the client wants to create.
Required 

.. _com.daml.ledger.api.v2.CreateAndExerciseCommand.create_arguments:

``create_arguments`` : :ref:`Record <com.daml.ledger.api.v2.Record>`

The arguments required for creating a contract from this template.
Required 

.. _com.daml.ledger.api.v2.CreateAndExerciseCommand.choice:

``choice`` : :ref:`string <string>`

The name of the choice the client wants to exercise.
Must be a valid NameString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.CreateAndExerciseCommand.choice_argument:

``choice_argument`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The argument for this choice.
Required 

.. _com.daml.ledger.api.v2.CreateCommand:

CreateCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Create a new contract instance based on a template.

.. _com.daml.ledger.api.v2.CreateCommand.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of contract the client wants to create.
Required 

.. _com.daml.ledger.api.v2.CreateCommand.create_arguments:

``create_arguments`` : :ref:`Record <com.daml.ledger.api.v2.Record>`

The arguments required for creating a contract from this template.
Required 

.. _com.daml.ledger.api.v2.DisclosedContract:

DisclosedContract message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

An additional contract that is used to resolve
contract & contract key lookups.

.. _com.daml.ledger.api.v2.DisclosedContract.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template id of the contract.
Required 

.. _com.daml.ledger.api.v2.DisclosedContract.contract_id:

``contract_id`` : :ref:`string <string>`

The contract id
Required 

.. _com.daml.ledger.api.v2.DisclosedContract.created_event_blob:

``created_event_blob`` : :ref:`bytes <bytes>`

Opaque byte string containing the complete payload required by the Daml engine
to reconstruct a contract not known to the receiving participant.
Required 

.. _com.daml.ledger.api.v2.DisclosedContract.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

The ID of the synchronizer where the contract is currently assigned
Optional 

.. _com.daml.ledger.api.v2.ExerciseByKeyCommand:

ExerciseByKeyCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Exercise a choice on an existing contract specified by its key.

.. _com.daml.ledger.api.v2.ExerciseByKeyCommand.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of contract the client wants to exercise.
Required 

.. _com.daml.ledger.api.v2.ExerciseByKeyCommand.contract_key:

``contract_key`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The key of the contract the client wants to exercise upon.
Required 

.. _com.daml.ledger.api.v2.ExerciseByKeyCommand.choice:

``choice`` : :ref:`string <string>`

The name of the choice the client wants to exercise.
Must be a valid NameString (as described in ``value.proto``)
Required 

.. _com.daml.ledger.api.v2.ExerciseByKeyCommand.choice_argument:

``choice_argument`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The argument for this choice.
Required 

.. _com.daml.ledger.api.v2.ExerciseCommand:

ExerciseCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Exercise a choice on an existing contract.

.. _com.daml.ledger.api.v2.ExerciseCommand.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of contract the client wants to exercise.
Required 

.. _com.daml.ledger.api.v2.ExerciseCommand.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the contract the client wants to exercise upon.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ExerciseCommand.choice:

``choice`` : :ref:`string <string>`

The name of the choice the client wants to exercise.
Must be a valid NameString (as described in ``value.proto``)
Required 

.. _com.daml.ledger.api.v2.ExerciseCommand.choice_argument:

``choice_argument`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The argument for this choice.
Required 

.. _com.daml.ledger.api.v2.PrefetchContractKey:

PrefetchContractKey message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Preload contracts

.. _com.daml.ledger.api.v2.PrefetchContractKey.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of contract the client wants to prefetch.
Both package-name and package-id reference identifier formats for the template-id are supported.

Required 

.. _com.daml.ledger.api.v2.PrefetchContractKey.contract_key:

``contract_key`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The key of the contract the client wants to prefetch.
Required 


----

.. _com/daml/ledger/api/v2/completion.proto:

``com/daml/ledger/api/v2/completion.proto``

.. _com.daml.ledger.api.v2.Completion:

Completion message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A completion represents the status of a submitted command on the ledger: it can be successful or failed.

.. _com.daml.ledger.api.v2.Completion.command_id:

``command_id`` : :ref:`string <string>`

The ID of the succeeded or failed command.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.Completion.status:

``status`` :  `google.rpc.Status <https://cloud.google.com/tasks/docs/reference/rpc/google.rpc#status>`__

Identifies the exact type of the error.
It uses the same format of conveying error details as it is used for the RPC responses of the APIs.
Optional 

.. _com.daml.ledger.api.v2.Completion.update_id:

``update_id`` : :ref:`string <string>`

The update_id of the transaction or reassignment that resulted from the command with command_id.
Only set for successfully executed commands.
Must be a valid LedgerString (as described in ``value.proto``). 

.. _com.daml.ledger.api.v2.Completion.application_id:

``application_id`` : :ref:`string <string>`

The application-id or user-id that was used for the submission, as described in ``commands.proto``.
Must be a valid ApplicationIdString (as described in ``value.proto``).
Optional for historic completions where this data is not available. 

.. _com.daml.ledger.api.v2.Completion.act_as:

``act_as`` : :ref:`string <string>` (repeated)

The set of parties on whose behalf the commands were executed.
Contains the ``act_as`` parties from ``commands.proto``
filtered to the requesting parties in CompletionStreamRequest.
The order of the parties need not be the same as in the submission.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Optional for historic completions where this data is not available. 

.. _com.daml.ledger.api.v2.Completion.submission_id:

``submission_id`` : :ref:`string <string>`

The submission ID this completion refers to, as described in ``commands.proto``.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Completion.deduplication_period.deduplication_offset:

``oneof deduplication_period.deduplication_offset`` : :ref:`int64 <int64>`

Specifies the start of the deduplication period by a completion stream offset (exclusive).

Must be a valid absolute offset (positive integer) or participant begin (zero). 

.. _com.daml.ledger.api.v2.Completion.deduplication_period.deduplication_duration:

``oneof deduplication_period.deduplication_duration`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

Specifies the length of the deduplication period.
It is measured in record time of completions.

Must be non-negative. 

.. _com.daml.ledger.api.v2.Completion.trace_context:

``trace_context`` : :ref:`TraceContext <com.daml.ledger.api.v2.TraceContext>`

Optional; ledger api trace context

The trace context transported in this message corresponds to the trace context supplied
by the client application in a HTTP2 header of the original command submission.
We typically use a header to transfer this type of information. Here we use message
body, because it is used in gRPC streams which do not support per message headers.
This field will be populated with the trace context contained in the original submission.
If that was not provided, a unique ledger-api-server generated trace context will be used
instead. 

.. _com.daml.ledger.api.v2.Completion.offset:

``offset`` : :ref:`int64 <int64>`

May be used in a subsequent CompletionStreamRequest to resume the consumption of this stream at a later time.
Required, must be a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.Completion.synchronizer_time:

``synchronizer_time`` : :ref:`SynchronizerTime <com.daml.ledger.api.v2.SynchronizerTime>`

The synchronizer along with its record time.
The synchronizer id provided, in case of

  - successful/failed transactions: identifies the synchronizer of the transaction
  - for successful/failed unassign commands: identifies the source synchronizer
  - for successful/failed assign commands: identifies the target synchronizer

Required 


----

.. _com/daml/ledger/api/v2/event.proto:

``com/daml/ledger/api/v2/event.proto``

.. _com.daml.ledger.api.v2.ArchivedEvent:

ArchivedEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Records that a contract has been archived, and choices may no longer be exercised on it.

.. _com.daml.ledger.api.v2.ArchivedEvent.offset:

``offset`` : :ref:`int64 <int64>`

The offset of origin.
Offsets are managed by the participant nodes.
Transactions can thus NOT be assumed to have the same offsets on different participant nodes.
Required, it is a valid absolute offset (positive integer) 

.. _com.daml.ledger.api.v2.ArchivedEvent.node_id:

``node_id`` : :ref:`int32 <int32>`

The position of this event in the originating transaction or reassignment.
Node ids are not necessarily equal across participants,
as these may see different projections/parts of transactions.
Required, must be valid node ID (non-negative integer) 

.. _com.daml.ledger.api.v2.ArchivedEvent.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the archived contract.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ArchivedEvent.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of the archived contract.
Required 

.. _com.daml.ledger.api.v2.ArchivedEvent.witness_parties:

``witness_parties`` : :ref:`string <string>` (repeated)

The parties that are notified of this event. For an ``ArchivedEvent``,
these are the intersection of the stakeholders of the contract in
question and the parties specified in the ``TransactionFilter``. The
stakeholders are the union of the signatories and the observers of
the contract.
Each one of its elements must be a valid PartyIdString (as described
in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ArchivedEvent.package_name:

``package_name`` : :ref:`string <string>`

The package name of the contract.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent:

CreatedEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Records that a contract has been created, and choices may now be exercised on it.

.. _com.daml.ledger.api.v2.CreatedEvent.offset:

``offset`` : :ref:`int64 <int64>`

The offset of origin, which has contextual meaning, please see description at usage.
Offsets are managed by the participant nodes.
Transactions can thus NOT be assumed to have the same offsets on different participant nodes.
Required, it is a valid absolute offset (positive integer) 

.. _com.daml.ledger.api.v2.CreatedEvent.node_id:

``node_id`` : :ref:`int32 <int32>`

The position of this event in the originating transaction or reassignment.
The origin has contextual meaning, please see description at usage.
Node ids are not necessarily equal across participants,
as these may see different projections/parts of transactions.
Required, must be valid node ID (non-negative integer) 

.. _com.daml.ledger.api.v2.CreatedEvent.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the created contract.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of the created contract.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.contract_key:

``contract_key`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The key of the created contract.
This will be set if and only if ``create_arguments`` is set and ``template_id`` defines a contract key.
Optional 

.. _com.daml.ledger.api.v2.CreatedEvent.create_arguments:

``create_arguments`` : :ref:`Record <com.daml.ledger.api.v2.Record>`

The arguments that have been used to create the contract.
Set either:

  - if there was a party, which is in the ``witness_parties`` of this event,
    and for which a ``CumulativeFilter`` exists with the ``template_id`` of this event
    among the ``template_filters``,
  - or if there was a party, which is in the ``witness_parties`` of this event,
    and for which a wildcard filter exists (``Filters`` with a ``CumulativeFilter`` of ``WildcardFilter``).

Optional 

.. _com.daml.ledger.api.v2.CreatedEvent.created_event_blob:

``created_event_blob`` : :ref:`bytes <bytes>`

Opaque representation of contract create event payload intended for forwarding
to an API server as a contract disclosed as part of a command
submission.
Optional 

.. _com.daml.ledger.api.v2.CreatedEvent.interface_views:

``interface_views`` : :ref:`InterfaceView <com.daml.ledger.api.v2.InterfaceView>` (repeated)

Interface views specified in the transaction filter.
Includes an ``InterfaceView`` for each interface for which there is a ``InterfaceFilter`` with

  - its party in the ``witness_parties`` of this event,
  - and which is implemented by the template of this event,
  - and which has ``include_interface_view`` set.

Optional 

.. _com.daml.ledger.api.v2.CreatedEvent.witness_parties:

``witness_parties`` : :ref:`string <string>` (repeated)

The parties that are notified of this event. When a ``CreatedEvent``
is returned as part of a transaction tree or ledger-effects transaction, this will include all
the parties specified in the ``TransactionFilter`` that are informees
of the event. If served as part of a ACS delta transaction those will
be limited to all parties specified in the ``TransactionFilter`` that
are stakeholders of the contract (i.e. either signatories or observers).
If the ``CreatedEvent`` is returned as part of an AssignedEvent,
ActiveContract or IncompleteUnassigned (so the event is related to
an assignment or unassignment): this will include all parties of the
``TransactionFilter`` that are stakeholders of the contract.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.signatories:

``signatories`` : :ref:`string <string>` (repeated)

The signatories for this contract as specified by the template.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.observers:

``observers`` : :ref:`string <string>` (repeated)

The observers for this contract as specified explicitly by the template or implicitly as choice controllers.
This field never contains parties that are signatories.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.created_at:

``created_at`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Ledger effective time of the transaction that created the contract.
Required 

.. _com.daml.ledger.api.v2.CreatedEvent.package_name:

``package_name`` : :ref:`string <string>`

The package name of the created contract.
Required 

.. _com.daml.ledger.api.v2.Event:

Event message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

In the update service the events are restricted to the events
visible for the parties specified in the transaction filter. Each
event message type below contains a ``witness_parties`` field which
indicates the subset of the requested parties that can see the event
in question.

.. _com.daml.ledger.api.v2.Event.event.created:

``oneof event.created`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

 

.. _com.daml.ledger.api.v2.Event.event.archived:

``oneof event.archived`` : :ref:`ArchivedEvent <com.daml.ledger.api.v2.ArchivedEvent>`

 

.. _com.daml.ledger.api.v2.Event.event.exercised:

``oneof event.exercised`` : :ref:`ExercisedEvent <com.daml.ledger.api.v2.ExercisedEvent>`

 

.. _com.daml.ledger.api.v2.ExercisedEvent:

ExercisedEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Records that a choice has been exercised on a target contract.

.. _com.daml.ledger.api.v2.ExercisedEvent.offset:

``offset`` : :ref:`int64 <int64>`

The offset of origin.
Offsets are managed by the participant nodes.
Transactions can thus NOT be assumed to have the same offsets on different participant nodes.
Required, it is a valid absolute offset (positive integer) 

.. _com.daml.ledger.api.v2.ExercisedEvent.node_id:

``node_id`` : :ref:`int32 <int32>`

The position of this event in the originating transaction or reassignment.
Node ids are not necessarily equal across participants,
as these may see different projections/parts of transactions.
Required, must be valid node ID (non-negative integer) 

.. _com.daml.ledger.api.v2.ExercisedEvent.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the target contract.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of the target contract.
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.interface_id:

``interface_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The interface where the choice is defined, if inherited.
Optional 

.. _com.daml.ledger.api.v2.ExercisedEvent.choice:

``choice`` : :ref:`string <string>`

The choice that was exercised on the target contract.
Must be a valid NameString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.choice_argument:

``choice_argument`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The argument of the exercised choice.
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.acting_parties:

``acting_parties`` : :ref:`string <string>` (repeated)

The parties that exercised the choice.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.consuming:

``consuming`` : :ref:`bool <bool>`

If true, the target contract may no longer be exercised.
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.witness_parties:

``witness_parties`` : :ref:`string <string>` (repeated)

The parties that are notified of this event. The witnesses of an exercise
node will depend on whether the exercise was consuming or not.
If consuming, the witnesses are the union of the stakeholders and
the actors.
If not consuming, the witnesses are the union of the signatories and
the actors. Note that the actors might not necessarily be observers
and thus signatories. This is the case when the controllers of a
choice are specified using "flexible controllers", using the
``choice ... controller`` syntax, and said controllers are not
explicitly marked as observers.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.last_descendant_node_id:

``last_descendant_node_id`` : :ref:`int32 <int32>`

Specifies the upper boundary of the node ids of the events in the same transaction that appeared as a result of
this ``ExercisedEvent``. This allows unambiguous identification of all the members of the subtree rooted at this
node. A full subtree can be constructed when all descendant nodes are present in the stream. If nodes are heavily
filtered, it is only possible to determine if a node is in a consequent subtree or not.
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.exercise_result:

``exercise_result`` : :ref:`Value <com.daml.ledger.api.v2.Value>`

The result of exercising the choice.
Required 

.. _com.daml.ledger.api.v2.ExercisedEvent.package_name:

``package_name`` : :ref:`string <string>`

The package name of the contract.
Required 

.. _com.daml.ledger.api.v2.InterfaceView:

InterfaceView message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

View of a create event matched by an interface filter.

.. _com.daml.ledger.api.v2.InterfaceView.interface_id:

``interface_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The interface implemented by the matched event.
Required 

.. _com.daml.ledger.api.v2.InterfaceView.view_status:

``view_status`` :  `google.rpc.Status <https://cloud.google.com/tasks/docs/reference/rpc/google.rpc#status>`__

Whether the view was successfully computed, and if not,
the reason for the error. The error is reported using the same rules
for error codes and messages as the errors returned for API requests.
Required 

.. _com.daml.ledger.api.v2.InterfaceView.view_value:

``view_value`` : :ref:`Record <com.daml.ledger.api.v2.Record>`

The value of the interface's view method on this event.
Set if it was requested in the ``InterfaceFilter`` and it could be
sucessfully computed.
Optional 


----

.. _com/daml/ledger/api/v2/event_query_service.proto:

``com/daml/ledger/api/v2/event_query_service.proto``

.. _com.daml.ledger.api.v2.EventQueryService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
EventQueryService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Query events by contract id.

Note that querying by contract key is not (yet) supported, as contract keys
are not supported (yet) in multi-synchronizer scenarios.

.. _com.daml.ledger.api.v2.EventQueryService.GetEventsByContractId:

GetEventsByContractId method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Get the create and the consuming exercise event for the contract with the provided ID.
No events will be returned for contracts that have been pruned because they
have already been archived before the latest pruning offset.

* Request: :ref:`GetEventsByContractIdRequest <com.daml.ledger.api.v2.GetEventsByContractIdRequest>`
* Response: :ref:`GetEventsByContractIdResponse <com.daml.ledger.api.v2.GetEventsByContractIdResponse>`



.. _com.daml.ledger.api.v2.Archived:

Archived message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.Archived.archived_event:

``archived_event`` : :ref:`ArchivedEvent <com.daml.ledger.api.v2.ArchivedEvent>`

Required 

.. _com.daml.ledger.api.v2.Archived.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

Required
The synchronizer which sequenced the archival of the contract 

.. _com.daml.ledger.api.v2.Created:

Created message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.Created.created_event:

``created_event`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

Required
The event as it appeared in the context of its original update (i.e. daml transaction or
reassignment) on this participant node. You can use its offset and node_id to find the
corresponding update and the node within it. 

.. _com.daml.ledger.api.v2.Created.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

The synchronizer which sequenced the creation of the contract
Required 

.. _com.daml.ledger.api.v2.GetEventsByContractIdRequest:

GetEventsByContractIdRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetEventsByContractIdRequest.contract_id:

``contract_id`` : :ref:`string <string>`

The contract id being queried.
Required 

.. _com.daml.ledger.api.v2.GetEventsByContractIdRequest.requesting_parties:

``requesting_parties`` : :ref:`string <string>` (repeated)

The parties whose events the client expects to see.
The events associated with the contract id will only be returned if the requesting parties includes
at least one party that is a stakeholder of the event. For a definition of stakeholders see
https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#contract-observers-and-stakeholders
Required 

.. _com.daml.ledger.api.v2.GetEventsByContractIdResponse:

GetEventsByContractIdResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetEventsByContractIdResponse.created:

``created`` : :ref:`Created <com.daml.ledger.api.v2.Created>`

The create event for the contract with the ``contract_id`` given in the request
provided it exists and has not yet been pruned.
Optional 

.. _com.daml.ledger.api.v2.GetEventsByContractIdResponse.archived:

``archived`` : :ref:`Archived <com.daml.ledger.api.v2.Archived>`

The archive event for the contract with the ``contract_id`` given in the request
provided such an archive event exists and it has not yet been pruned.
Optional 


----

.. _com/daml/ledger/api/v2/experimental_features.proto:

``com/daml/ledger/api/v2/experimental_features.proto``

.. _com.daml.ledger.api.v2.ExperimentalCommandInspectionService:

ExperimentalCommandInspectionService message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Whether the Ledger API supports command inspection service

.. _com.daml.ledger.api.v2.ExperimentalCommandInspectionService.supported:

``supported`` : :ref:`bool <bool>`

 

.. _com.daml.ledger.api.v2.ExperimentalFeatures:

ExperimentalFeatures message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

See the feature message definitions for descriptions.

.. _com.daml.ledger.api.v2.ExperimentalFeatures.static_time:

``static_time`` : :ref:`ExperimentalStaticTime <com.daml.ledger.api.v2.ExperimentalStaticTime>`

 

.. _com.daml.ledger.api.v2.ExperimentalFeatures.command_inspection_service:

``command_inspection_service`` : :ref:`ExperimentalCommandInspectionService <com.daml.ledger.api.v2.ExperimentalCommandInspectionService>`

 

.. _com.daml.ledger.api.v2.ExperimentalFeatures.interactive_submission_service:

``interactive_submission_service`` : :ref:`ExperimentalInteractiveSubmissionService <com.daml.ledger.api.v2.ExperimentalInteractiveSubmissionService>`

 

.. _com.daml.ledger.api.v2.ExperimentalFeatures.party_topology_events:

``party_topology_events`` : :ref:`ExperimentalPartyTopologyEvents <com.daml.ledger.api.v2.ExperimentalPartyTopologyEvents>`

 

.. _com.daml.ledger.api.v2.ExperimentalInteractiveSubmissionService:

ExperimentalInteractiveSubmissionService message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Whether the Ledger API supports interactive submission service

.. _com.daml.ledger.api.v2.ExperimentalInteractiveSubmissionService.supported:

``supported`` : :ref:`bool <bool>`

 

.. _com.daml.ledger.api.v2.ExperimentalPartyTopologyEvents:

ExperimentalPartyTopologyEvents message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Whether the Ledger API supports party events

.. _com.daml.ledger.api.v2.ExperimentalPartyTopologyEvents.supported:

``supported`` : :ref:`bool <bool>`

 

.. _com.daml.ledger.api.v2.ExperimentalStaticTime:

ExperimentalStaticTime message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Ledger is in the static time mode and exposes a time service.

.. _com.daml.ledger.api.v2.ExperimentalStaticTime.supported:

``supported`` : :ref:`bool <bool>`

 


----

.. _com/daml/ledger/api/v2/interactive/interactive_submission_common_data.proto:

``com/daml/ledger/api/v2/interactive/interactive_submission_common_data.proto``

.. _com.daml.ledger.api.v2.interactive.GlobalKey:

GlobalKey message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.GlobalKey.template_id:

``template_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.interactive.GlobalKey.package_name:

``package_name`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.GlobalKey.key:

``key`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>`

 

.. _com.daml.ledger.api.v2.interactive.GlobalKey.hash:

``hash`` : :ref:`bytes <bytes>`

 


----

.. _com/daml/ledger/api/v2/interactive/interactive_submission_service.proto:

``com/daml/ledger/api/v2/interactive/interactive_submission_service.proto``

.. _com.daml.ledger.api.v2.interactive.InteractiveSubmissionService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
InteractiveSubmissionService, |version com.daml.ledger.api.v2.interactive|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

The InteractiveSubmissionService allows to submit commands in 2-steps:
1) prepare transaction from commands, 2) submit the prepared transaction
This gives callers the ability to sign the daml transaction with their own signing keys

.. _com.daml.ledger.api.v2.interactive.InteractiveSubmissionService.PrepareSubmission:

PrepareSubmission method, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Requires `readAs` scope for the submitting party when LAPI User authorization is enabled

* Request: :ref:`PrepareSubmissionRequest <com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest>`
* Response: :ref:`PrepareSubmissionResponse <com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse>`



.. _com.daml.ledger.api.v2.interactive.InteractiveSubmissionService.ExecuteSubmission:

ExecuteSubmission method, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



* Request: :ref:`ExecuteSubmissionRequest <com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest>`
* Response: :ref:`ExecuteSubmissionResponse <com.daml.ledger.api.v2.interactive.ExecuteSubmissionResponse>`



.. _com.daml.ledger.api.v2.interactive.DamlTransaction:

DamlTransaction message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Daml Transaction.
This represents the effect on the ledger if this transaction is successfully committed.

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.version:

``version`` : :ref:`string <string>`

Transaction version, will be >= max(nodes version) 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.roots:

``roots`` : :ref:`string <string>` (repeated)

Root nodes of the transaction 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.nodes:

``nodes`` : :ref:`DamlTransaction.Node <com.daml.ledger.api.v2.interactive.DamlTransaction.Node>` (repeated)

List of nodes in the transaction 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.node_seeds:

``node_seeds`` : :ref:`DamlTransaction.NodeSeed <com.daml.ledger.api.v2.interactive.DamlTransaction.NodeSeed>` (repeated)

Node seeds are values associated with certain nodes used for generating cryptographic salts 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.Node:

DamlTransaction.Node message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

A transaction may contain nodes with different versions.
Each node must be hashed using the hashing algorithm corresponding to its specific version.

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.Node.node_id:

``node_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.Node.versioned_node.v1:

``oneof versioned_node.v1`` : :ref:`transaction.v1.Node <com.daml.ledger.api.v2.interactive.transaction.v1.Node>`

Start at 1000 so we can add more fields before if necessary
When new versions will be added, they will show here 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.NodeSeed:

DamlTransaction.NodeSeed message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.DamlTransaction.NodeSeed.node_id:

``node_id`` : :ref:`int32 <int32>`

 

.. _com.daml.ledger.api.v2.interactive.DamlTransaction.NodeSeed.seed:

``seed`` : :ref:`bytes <bytes>`

 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest:

ExecuteSubmissionRequest message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.prepared_transaction:

``prepared_transaction`` : :ref:`PreparedTransaction <com.daml.ledger.api.v2.interactive.PreparedTransaction>`

the prepared transaction
Typically this is the value of the `prepared_transaction` field in `PrepareSubmissionResponse`
obtained from calling `prepareSubmission`. 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.party_signatures:

``party_signatures`` : :ref:`PartySignatures <com.daml.ledger.api.v2.interactive.PartySignatures>`

The party(ies) signatures that authorize the prepared submission to be executed by this node.
Each party can provide one or more signatures..
and one or more parties can sign.
Note that currently, only single party submissions are supported. 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.deduplication_period.deduplication_duration:

``oneof deduplication_period.deduplication_duration`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

Specifies the length of the deduplication period.
It is interpreted relative to the local clock at some point during the submission's processing.
Must be non-negative. Must not exceed the maximum deduplication time. 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.deduplication_period.deduplication_offset:

``oneof deduplication_period.deduplication_offset`` : :ref:`int64 <int64>`

Specifies the start of the deduplication period by a completion stream offset (exclusive).
Must be a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.submission_id:

``submission_id`` : :ref:`string <string>`

A unique identifier to distinguish completions for different submissions with the same change ID.
Typically a random UUID. Applications are expected to use a different UUID for each retry of a submission
with the same change ID.
Must be a valid LedgerString (as described in ``value.proto``).

Required 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.application_id:

``application_id`` : :ref:`string <string>`

See [PrepareSubmissionRequest.application_id] 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionRequest.hashing_scheme_version:

``hashing_scheme_version`` : :ref:`HashingSchemeVersion <com.daml.ledger.api.v2.interactive.HashingSchemeVersion>`

The hashing scheme version used when building the hash 

.. _com.daml.ledger.api.v2.interactive.ExecuteSubmissionResponse:

ExecuteSubmissionResponse message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.interactive.Metadata:

Metadata message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Transaction Metadata
Refer to the hashing documentation for information on how it should be hashed.

.. _com.daml.ledger.api.v2.interactive.Metadata._ledger_effective_time.ledger_effective_time:

``oneof _ledger_effective_time.ledger_effective_time`` : :ref:`uint64 <uint64>` (optional)

Ledger effective time picked during preparation of the command.
This is optional, because if the transaction does NOT depend on time, this value will not be set.
Instead, it will be chosen when the command is submitted through the [execute] RPC.
This allows increasing the time window necessary to acquire the signature and submit the transaction
(effectively the time between when [prepare] and [execute] are called).
Note however that if the transaction does use time, then this value will bet set and will constrain the available
time to perform the signature and submit the transaction. The time window allowed is a configuration of the ledger
and usually is counted in minutes. 

.. _com.daml.ledger.api.v2.interactive.Metadata.submitter_info:

``submitter_info`` : :ref:`Metadata.SubmitterInfo <com.daml.ledger.api.v2.interactive.Metadata.SubmitterInfo>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.mediator_group:

``mediator_group`` : :ref:`uint32 <uint32>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.transaction_uuid:

``transaction_uuid`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.submission_time:

``submission_time`` : :ref:`uint64 <uint64>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.disclosed_events:

``disclosed_events`` : :ref:`Metadata.ProcessedDisclosedContract <com.daml.ledger.api.v2.interactive.Metadata.ProcessedDisclosedContract>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.Metadata.global_key_mapping:

``global_key_mapping`` : :ref:`Metadata.GlobalKeyMappingEntry <com.daml.ledger.api.v2.interactive.Metadata.GlobalKeyMappingEntry>` (repeated)

Contextual information needed to process the transaction but not signed, either because it's already indirectly
signed by signing the transaction, or because it doesn't impact the ledger state 

.. _com.daml.ledger.api.v2.interactive.Metadata.GlobalKeyMappingEntry:

Metadata.GlobalKeyMappingEntry message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.Metadata.GlobalKeyMappingEntry.key:

``key`` : :ref:`GlobalKey <com.daml.ledger.api.v2.interactive.GlobalKey>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.GlobalKeyMappingEntry._value.value:

``oneof _value.value`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>` (optional)

 

.. _com.daml.ledger.api.v2.interactive.Metadata.ProcessedDisclosedContract:

Metadata.ProcessedDisclosedContract message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.Metadata.ProcessedDisclosedContract.contract.v1:

``oneof contract.v1`` : :ref:`transaction.v1.Create <com.daml.ledger.api.v2.interactive.transaction.v1.Create>`

When new versions will be added, they will show here 

.. _com.daml.ledger.api.v2.interactive.Metadata.ProcessedDisclosedContract.created_at:

``created_at`` : :ref:`uint64 <uint64>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.ProcessedDisclosedContract.driver_metadata:

``driver_metadata`` : :ref:`bytes <bytes>`

 

.. _com.daml.ledger.api.v2.interactive.Metadata.SubmitterInfo:

Metadata.SubmitterInfo message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.Metadata.SubmitterInfo.act_as:

``act_as`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.Metadata.SubmitterInfo.command_id:

``command_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.MinLedgerTime:

MinLedgerTime message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.MinLedgerTime.time.min_ledger_time_abs:

``oneof time.min_ledger_time_abs`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Lower bound for the ledger time assigned to the resulting transaction.
The ledger time of a transaction is assigned as part of command interpretation.
Important note: for interactive submissions, if the transaction depends on time, it **must** be signed
and submitted within a time window around the ledger time assigned to the transaction during the prepare method.
The time delta around that ledger time is a configuration of the ledger, usually short, around 1 minute.
If however the transaction does not depend on time, the available time window to sign and submit the transaction is bound
by the submission timestamp, which is also assigned in the "prepare" step (this request),
but can be configured with a much larger skew, allowing for more time to sign the request (in the order of hours).
Must not be set at the same time as min_ledger_time_rel.
Optional 

.. _com.daml.ledger.api.v2.interactive.MinLedgerTime.time.min_ledger_time_rel:

``oneof time.min_ledger_time_rel`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

Same as min_ledger_time_abs, but specified as a duration, starting from the time this request is received by the server.
Must not be set at the same time as min_ledger_time_abs.
Optional 

.. _com.daml.ledger.api.v2.interactive.PartySignatures:

PartySignatures message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Additional signatures provided by the submitting parties

.. _com.daml.ledger.api.v2.interactive.PartySignatures.signatures:

``signatures`` : :ref:`SinglePartySignatures <com.daml.ledger.api.v2.interactive.SinglePartySignatures>` (repeated)

Additional signatures provided by all individual parties 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest:

PrepareSubmissionRequest message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.application_id:

``application_id`` : :ref:`string <string>`

Uniquely identifies the application or participant user that prepares the transaction.
Must be a valid ApplicationIdString (as described in ``value.proto``).
Required unless authentication is used with a user token or a custom token specifying an application-id.
In that case, the token's user-id, respectively application-id, will be used for the request's application_id. 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.command_id:

``command_id`` : :ref:`string <string>`

Uniquely identifies the command.
The triple (application_id, act_as, command_id) constitutes the change ID for the intended ledger change,
where act_as is interpreted as a set of party names.
The change ID can be used for matching the intended ledger changes with all their completions.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.commands:

``commands`` : :ref:`com.daml.ledger.api.v2.Command <com.daml.ledger.api.v2.Command>` (repeated)

Individual elements of this atomic command. Must be non-empty.
Required 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.min_ledger_time:

``min_ledger_time`` : :ref:`MinLedgerTime <com.daml.ledger.api.v2.interactive.MinLedgerTime>`

Optional 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.act_as:

``act_as`` : :ref:`string <string>` (repeated)

Set of parties on whose behalf the command should be executed, if submitted.
If ledger API authorization is enabled, then the authorization metadata must authorize the sender of the request
to **read** (not act) on behalf of each of the given parties. This is because this RPC merely prepares a transaction
and does not execute it. Therefore read authorization is sufficient even for actAs parties.
Note: This may change, and more specific authorization scope may be introduced in the future.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Required, must be non-empty. 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.read_as:

``read_as`` : :ref:`string <string>` (repeated)

Set of parties on whose behalf (in addition to all parties listed in ``act_as``) contracts can be retrieved.
This affects Daml operations such as ``fetch``, ``fetchByKey``, ``lookupByKey``, ``exercise``, and ``exerciseByKey``.
Note: A command can only use contracts that are visible to at least
one of the parties in ``act_as`` or ``read_as``. This visibility check is independent from the Daml authorization
rules for fetch operations.
If ledger API authorization is enabled, then the authorization metadata must authorize the sender of the request
to read contract data on behalf of each of the given parties.
Optional 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.disclosed_contracts:

``disclosed_contracts`` : :ref:`com.daml.ledger.api.v2.DisclosedContract <com.daml.ledger.api.v2.DisclosedContract>` (repeated)

Additional contracts used to resolve contract & contract key lookups.
Optional 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.package_id_selection_preference:

``package_id_selection_preference`` : :ref:`string <string>` (repeated)

The package-id selection preference of the client for resolving
package names and interface instances in command submission and interpretation 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.verbose_hashing:

``verbose_hashing`` : :ref:`bool <bool>`

When true, the response will contain additional details on how the transaction was encoded and hashed
This can be useful for troubleshooting of hash mismatches. Should only be used for debugging. 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.prefetch_contract_keys:

``prefetch_contract_keys`` : :ref:`com.daml.ledger.api.v2.PrefetchContractKey <com.daml.ledger.api.v2.PrefetchContractKey>` (repeated)

Fetches the contract keys into the caches to speed up the command processing.
Should only contain contract keys that are expected to be resolved during interpretation of the commands.
Keys of disclosed contracts do not need prefetching.

Optional 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse:

PrepareSubmissionResponse message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse.prepared_transaction:

``prepared_transaction`` : :ref:`PreparedTransaction <com.daml.ledger.api.v2.interactive.PreparedTransaction>`

The interpreted transaction, it represents the ledger changes necessary to execute the commands specified in the request.
Clients MUST display the content of the transaction to the user for them to validate before signing the hash if the preparing participant is not trusted. 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse.prepared_transaction_hash:

``prepared_transaction_hash`` : :ref:`bytes <bytes>`

Hash of the transaction, this is what needs to be signed by the party to authorize the transaction.
Only provided for convenience, clients MUST recompute the hash from the raw transaction if the preparing participant is not trusted.
May be removed in future versions 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse.hashing_scheme_version:

``hashing_scheme_version`` : :ref:`HashingSchemeVersion <com.daml.ledger.api.v2.interactive.HashingSchemeVersion>`

The hashing scheme version used when building the hash 

.. _com.daml.ledger.api.v2.interactive.PrepareSubmissionResponse._hashing_details.hashing_details:

``oneof _hashing_details.hashing_details`` : :ref:`string <string>` (optional)

Optional additional details on how the transaction was encoded and hashed. Only set if verbose_hashing = true in the request
Note that there are no guarantees on the stability of the format or content of this field.
Its content should NOT be parsed and should only be used for troubleshooting purposes. 

.. _com.daml.ledger.api.v2.interactive.PreparedTransaction:

PreparedTransaction message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Prepared Transaction Message

.. _com.daml.ledger.api.v2.interactive.PreparedTransaction.transaction:

``transaction`` : :ref:`DamlTransaction <com.daml.ledger.api.v2.interactive.DamlTransaction>`

Daml Transaction representing the ledger effect if executed. See below 

.. _com.daml.ledger.api.v2.interactive.PreparedTransaction.metadata:

``metadata`` : :ref:`Metadata <com.daml.ledger.api.v2.interactive.Metadata>`

Metadata context necessary to execute the transaction 

.. _com.daml.ledger.api.v2.interactive.Signature:

Signature message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.Signature.format:

``format`` : :ref:`SignatureFormat <com.daml.ledger.api.v2.interactive.SignatureFormat>`

 

.. _com.daml.ledger.api.v2.interactive.Signature.signature:

``signature`` : :ref:`bytes <bytes>`

 

.. _com.daml.ledger.api.v2.interactive.Signature.signed_by:

``signed_by`` : :ref:`string <string>`

The fingerprint/id of the keypair used to create this signature and needed to verify. 

.. _com.daml.ledger.api.v2.interactive.Signature.signing_algorithm_spec:

``signing_algorithm_spec`` : :ref:`SigningAlgorithmSpec <com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec>`

The signing algorithm specification used to produce this signature 

.. _com.daml.ledger.api.v2.interactive.SinglePartySignatures:

SinglePartySignatures message, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================

Signatures provided by a single party

.. _com.daml.ledger.api.v2.interactive.SinglePartySignatures.party:

``party`` : :ref:`string <string>`

Submitting party 

.. _com.daml.ledger.api.v2.interactive.SinglePartySignatures.signatures:

``signatures`` : :ref:`Signature <com.daml.ledger.api.v2.interactive.Signature>` (repeated)

Signatures 




.. _com.daml.ledger.api.v2.interactive.HashingSchemeVersion:

HashingSchemeVersion enum, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================



The hashing scheme version used when building the hash of the PreparedTransaction

.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.interactive.HashingSchemeVersion.HASHING_SCHEME_VERSION_UNSPECIFIED:

       HASHING_SCHEME_VERSION_UNSPECIFIED
     - 0
     - 

   * - .. _com.daml.ledger.api.v2.interactive.HashingSchemeVersion.HASHING_SCHEME_VERSION_V1:

       HASHING_SCHEME_VERSION_V1
     - 1
     - 

   



.. _com.daml.ledger.api.v2.interactive.SignatureFormat:

SignatureFormat enum, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================





.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.interactive.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED:

       SIGNATURE_FORMAT_UNSPECIFIED
     - 0
     - 

   * - .. _com.daml.ledger.api.v2.interactive.SignatureFormat.SIGNATURE_FORMAT_RAW:

       SIGNATURE_FORMAT_RAW
     - 1
     - Signature scheme specific signature format Legacy format no longer used, except for migrations

   * - .. _com.daml.ledger.api.v2.interactive.SignatureFormat.SIGNATURE_FORMAT_DER:

       SIGNATURE_FORMAT_DER
     - 2
     - ASN.1 + DER-encoding of the `r` and `s` integers, as defined in https://datatracker.ietf.org/doc/html/rfc3279#section-2.2.3 Used for ECDSA signatures

   * - .. _com.daml.ledger.api.v2.interactive.SignatureFormat.SIGNATURE_FORMAT_CONCAT:

       SIGNATURE_FORMAT_CONCAT
     - 3
     - Concatenation of the integers `r || s` in little-endian form, as defined in https://datatracker.ietf.org/doc/html/rfc8032#section-3.3 Note that this is different from the format defined in IEEE P1363, which uses concatenation in big-endian form. Used for EdDSA signatures

   * - .. _com.daml.ledger.api.v2.interactive.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC:

       SIGNATURE_FORMAT_SYMBOLIC
     - 10000
     - Symbolic crypto, must only be used for testing

   



.. _com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec:

SigningAlgorithmSpec enum, |version com.daml.ledger.api.v2.interactive|
========================================================================================================================================================================================================





.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED:

       SIGNING_ALGORITHM_SPEC_UNSPECIFIED
     - 0
     - 

   * - .. _com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519:

       SIGNING_ALGORITHM_SPEC_ED25519
     - 1
     - EdDSA Signature based on Curve25519 with SHA-512 http://ed25519.cr.yp.to/

   * - .. _com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256:

       SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256
     - 2
     - Elliptic Curve Digital Signature Algorithm with SHA256

   * - .. _com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384:

       SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384
     - 3
     - Elliptic Curve Digital Signature Algorithm with SHA384

   

----

.. _com/daml/ledger/api/v2/interactive/transaction/v1/interactive_submission_data.proto:

``com/daml/ledger/api/v2/interactive/transaction/v1/interactive_submission_data.proto``

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create:

Create message, |version com.daml.ledger.api.v2.interactive.transaction.v1|
========================================================================================================================================================================================================

Create Node

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.lf_version:

``lf_version`` : :ref:`string <string>`

Specific LF version of the node 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.contract_id:

``contract_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.package_name:

``package_name`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.template_id:

``template_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.argument:

``argument`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.signatories:

``signatories`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Create.stakeholders:

``stakeholders`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise:

Exercise message, |version com.daml.ledger.api.v2.interactive.transaction.v1|
========================================================================================================================================================================================================

Exercise node

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.lf_version:

``lf_version`` : :ref:`string <string>`

Specific LF version of the node 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.contract_id:

``contract_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.package_name:

``package_name`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.template_id:

``template_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.signatories:

``signatories`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.stakeholders:

``stakeholders`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.acting_parties:

``acting_parties`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.interface_id:

``interface_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.choice_id:

``choice_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.chosen_value:

``chosen_value`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.consuming:

``consuming`` : :ref:`bool <bool>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.children:

``children`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.exercise_result:

``exercise_result`` : :ref:`com.daml.ledger.api.v2.Value <com.daml.ledger.api.v2.Value>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Exercise.choice_observers:

``choice_observers`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch:

Fetch message, |version com.daml.ledger.api.v2.interactive.transaction.v1|
========================================================================================================================================================================================================

Fetch node

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.lf_version:

``lf_version`` : :ref:`string <string>`

Specific LF version of the node 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.contract_id:

``contract_id`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.package_name:

``package_name`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.template_id:

``template_id`` : :ref:`com.daml.ledger.api.v2.Identifier <com.daml.ledger.api.v2.Identifier>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.signatories:

``signatories`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.stakeholders:

``stakeholders`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Fetch.acting_parties:

``acting_parties`` : :ref:`string <string>` (repeated)

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Node:

Node message, |version com.daml.ledger.api.v2.interactive.transaction.v1|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.interactive.transaction.v1.Node.node_type.create:

``oneof node_type.create`` : :ref:`Create <com.daml.ledger.api.v2.interactive.transaction.v1.Create>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Node.node_type.fetch:

``oneof node_type.fetch`` : :ref:`Fetch <com.daml.ledger.api.v2.interactive.transaction.v1.Fetch>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Node.node_type.exercise:

``oneof node_type.exercise`` : :ref:`Exercise <com.daml.ledger.api.v2.interactive.transaction.v1.Exercise>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Node.node_type.rollback:

``oneof node_type.rollback`` : :ref:`Rollback <com.daml.ledger.api.v2.interactive.transaction.v1.Rollback>`

 

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Rollback:

Rollback message, |version com.daml.ledger.api.v2.interactive.transaction.v1|
========================================================================================================================================================================================================

Rollback Node

.. _com.daml.ledger.api.v2.interactive.transaction.v1.Rollback.children:

``children`` : :ref:`string <string>` (repeated)

 


----

.. _com/daml/ledger/api/v2/offset_checkpoint.proto:

``com/daml/ledger/api/v2/offset_checkpoint.proto``

.. _com.daml.ledger.api.v2.OffsetCheckpoint:

OffsetCheckpoint message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

OffsetCheckpoints may be used to:

* detect time out of commands.
* provide an offset which can be used to restart consumption.

.. _com.daml.ledger.api.v2.OffsetCheckpoint.offset:

``offset`` : :ref:`int64 <int64>`

The participant's offset, the details of the offset field are described in ``community/ledger-api/README.md``.
Required, must be a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.OffsetCheckpoint.synchronizer_times:

``synchronizer_times`` : :ref:`SynchronizerTime <com.daml.ledger.api.v2.SynchronizerTime>` (repeated)

 

.. _com.daml.ledger.api.v2.SynchronizerTime:

SynchronizerTime message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.SynchronizerTime.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

The id of the synchronizer.
Required 

.. _com.daml.ledger.api.v2.SynchronizerTime.record_time:

``record_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

All commands with a maximum record time below this value MUST be considered lost if their completion has not arrived before this checkpoint.
Required 


----

.. _com/daml/ledger/api/v2/package_service.proto:

``com/daml/ledger/api/v2/package_service.proto``

.. _com.daml.ledger.api.v2.PackageService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PackageService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to query the Daml-LF packages that are supported by the server.

.. _com.daml.ledger.api.v2.PackageService.ListPackages:

ListPackages method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Returns the identifiers of all supported packages.

* Request: :ref:`ListPackagesRequest <com.daml.ledger.api.v2.ListPackagesRequest>`
* Response: :ref:`ListPackagesResponse <com.daml.ledger.api.v2.ListPackagesResponse>`



.. _com.daml.ledger.api.v2.PackageService.GetPackage:

GetPackage method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Returns the contents of a single package.

* Request: :ref:`GetPackageRequest <com.daml.ledger.api.v2.GetPackageRequest>`
* Response: :ref:`GetPackageResponse <com.daml.ledger.api.v2.GetPackageResponse>`



.. _com.daml.ledger.api.v2.PackageService.GetPackageStatus:

GetPackageStatus method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Returns the status of a single package.

* Request: :ref:`GetPackageStatusRequest <com.daml.ledger.api.v2.GetPackageStatusRequest>`
* Response: :ref:`GetPackageStatusResponse <com.daml.ledger.api.v2.GetPackageStatusResponse>`



.. _com.daml.ledger.api.v2.GetPackageRequest:

GetPackageRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetPackageRequest.package_id:

``package_id`` : :ref:`string <string>`

The ID of the requested package.
Must be a valid PackageIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetPackageResponse:

GetPackageResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetPackageResponse.hash_function:

``hash_function`` : :ref:`HashFunction <com.daml.ledger.api.v2.HashFunction>`

The hash function we use to calculate the hash.
Required 

.. _com.daml.ledger.api.v2.GetPackageResponse.archive_payload:

``archive_payload`` : :ref:`bytes <bytes>`

Contains a ``daml_lf`` ArchivePayload. See further details in ``daml_lf.proto``.
Required 

.. _com.daml.ledger.api.v2.GetPackageResponse.hash:

``hash`` : :ref:`string <string>`

The hash of the archive payload, can also used as a ``package_id``.
Must be a valid PackageIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetPackageStatusRequest:

GetPackageStatusRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetPackageStatusRequest.package_id:

``package_id`` : :ref:`string <string>`

The ID of the requested package.
Must be a valid PackageIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetPackageStatusResponse:

GetPackageStatusResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetPackageStatusResponse.package_status:

``package_status`` : :ref:`PackageStatus <com.daml.ledger.api.v2.PackageStatus>`

The status of the package. 

.. _com.daml.ledger.api.v2.ListPackagesRequest:

ListPackagesRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.ListPackagesResponse:

ListPackagesResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.ListPackagesResponse.package_ids:

``package_ids`` : :ref:`string <string>` (repeated)

The IDs of all Daml-LF packages supported by the server.
Each element must be a valid PackageIdString (as described in ``value.proto``).
Required 




.. _com.daml.ledger.api.v2.HashFunction:

HashFunction enum, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================





.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.HashFunction.HASH_FUNCTION_SHA256:

       HASH_FUNCTION_SHA256
     - 0
     - 

   



.. _com.daml.ledger.api.v2.PackageStatus:

PackageStatus enum, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================





.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.PackageStatus.PACKAGE_STATUS_UNSPECIFIED:

       PACKAGE_STATUS_UNSPECIFIED
     - 0
     - The server is not aware of such a package.

   * - .. _com.daml.ledger.api.v2.PackageStatus.PACKAGE_STATUS_REGISTERED:

       PACKAGE_STATUS_REGISTERED
     - 1
     - The server is able to execute Daml commands operating on this package.

   

----

.. _com/daml/ledger/api/v2/reassignment.proto:

``com/daml/ledger/api/v2/reassignment.proto``

.. _com.daml.ledger.api.v2.AssignedEvent:

AssignedEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Records that a contract has been assigned, and it can be used on the target synchronizer.

.. _com.daml.ledger.api.v2.AssignedEvent.source:

``source`` : :ref:`string <string>`

The ID of the source synchronizer.
Must be a valid synchronizer id.
Required 

.. _com.daml.ledger.api.v2.AssignedEvent.target:

``target`` : :ref:`string <string>`

The ID of the target synchronizer.
Must be a valid synchronizer id.
Required 

.. _com.daml.ledger.api.v2.AssignedEvent.unassign_id:

``unassign_id`` : :ref:`string <string>`

The ID from the unassigned event.
For correlation capabilities.
For one contract the (unassign_id, source synchronizer) pair is unique.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.AssignedEvent.submitter:

``submitter`` : :ref:`string <string>`

Party on whose behalf the assign command was executed.
Empty if the assignment happened offline via the repair service.
Must be a valid PartyIdString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.AssignedEvent.reassignment_counter:

``reassignment_counter`` : :ref:`uint64 <uint64>`

Each corresponding assigned and unassigned event has the same reassignment_counter. This strictly increases
with each unassign command for the same contract. Creation of the contract corresponds to reassignment_counter
equals zero.
Required 

.. _com.daml.ledger.api.v2.AssignedEvent.created_event:

``created_event`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

Required
The offset of the emitted event refers to the offset of this assignment, while the node_id is always set to 0. 

.. _com.daml.ledger.api.v2.Reassignment:

Reassignment message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Complete view of an on-ledger reassignment.

.. _com.daml.ledger.api.v2.Reassignment.update_id:

``update_id`` : :ref:`string <string>`

Assigned by the server. Useful for correlating logs.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.Reassignment.command_id:

``command_id`` : :ref:`string <string>`

The ID of the command which resulted in this reassignment. Missing for everyone except the submitting party on the submitting participant.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Reassignment.workflow_id:

``workflow_id`` : :ref:`string <string>`

The workflow ID used in reassignment command submission. Only set if the ``workflow_id`` for the command was set.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Reassignment.offset:

``offset`` : :ref:`int64 <int64>`

The participant's offset. The details of this field are described in ``community/ledger-api/README.md``.
Required, must be a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.Reassignment.event.unassigned_event:

``oneof event.unassigned_event`` : :ref:`UnassignedEvent <com.daml.ledger.api.v2.UnassignedEvent>`

 

.. _com.daml.ledger.api.v2.Reassignment.event.assigned_event:

``oneof event.assigned_event`` : :ref:`AssignedEvent <com.daml.ledger.api.v2.AssignedEvent>`

 

.. _com.daml.ledger.api.v2.Reassignment.trace_context:

``trace_context`` : :ref:`TraceContext <com.daml.ledger.api.v2.TraceContext>`

Optional; ledger api trace context

The trace context transported in this message corresponds to the trace context supplied
by the client application in a HTTP2 header of the original command submission.
We typically use a header to transfer this type of information. Here we use message
body, because it is used in gRPC streams which do not support per message headers.
This field will be populated with the trace context contained in the original submission.
If that was not provided, a unique ledger-api-server generated trace context will be used
instead. 

.. _com.daml.ledger.api.v2.Reassignment.record_time:

``record_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time at which the reassignment was recorded. The record time refers to the source/target
synchronizer for an unassign/assign event respectively.
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent:

UnassignedEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Records that a contract has been unassigned, and it becomes unusable on the source synchronizer

.. _com.daml.ledger.api.v2.UnassignedEvent.unassign_id:

``unassign_id`` : :ref:`string <string>`

The ID of the unassignment. This needs to be used as an input for a assign ReassignmentCommand.
For one contract the (unassign_id, source synchronizer) pair is unique.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the reassigned contract.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The template of the reassigned contract.
 Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.source:

``source`` : :ref:`string <string>`

The ID of the source synchronizer
Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.target:

``target`` : :ref:`string <string>`

The ID of the target synchronizer
Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.submitter:

``submitter`` : :ref:`string <string>`

Party on whose behalf the unassign command was executed.
Empty if the unassignment happened offline via the repair service.
Must be a valid PartyIdString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.UnassignedEvent.reassignment_counter:

``reassignment_counter`` : :ref:`uint64 <uint64>`

Each corresponding assigned and unassigned event has the same reassignment_counter. This strictly increases
with each unassign command for the same contract. Creation of the contract corresponds to reassignment_counter
equals zero.
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.assignment_exclusivity:

``assignment_exclusivity`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Assignment exclusivity
Before this time (measured on the target synchronizer), only the submitter of the unassignment can initiate the assignment
Defined for reassigning participants.
Optional 

.. _com.daml.ledger.api.v2.UnassignedEvent.witness_parties:

``witness_parties`` : :ref:`string <string>` (repeated)

The parties that are notified of this event.
Required 

.. _com.daml.ledger.api.v2.UnassignedEvent.package_name:

``package_name`` : :ref:`string <string>`

The package name of the contract.
Required 


----

.. _com/daml/ledger/api/v2/reassignment_command.proto:

``com/daml/ledger/api/v2/reassignment_command.proto``

.. _com.daml.ledger.api.v2.AssignCommand:

AssignCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Assign a contract

.. _com.daml.ledger.api.v2.AssignCommand.unassign_id:

``unassign_id`` : :ref:`string <string>`

The ID from the unassigned event to be completed by this assignment.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.AssignCommand.source:

``source`` : :ref:`string <string>`

The ID of the source synchronizer
Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.AssignCommand.target:

``target`` : :ref:`string <string>`

The ID of the target synchronizer
Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.ReassignmentCommand:

ReassignmentCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.ReassignmentCommand.workflow_id:

``workflow_id`` : :ref:`string <string>`

Identifier of the on-ledger workflow that this command is a part of.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.ReassignmentCommand.application_id:

``application_id`` : :ref:`string <string>`

Uniquely identifies the application or participant user that issued the command.
Must be a valid ApplicationIdString (as described in ``value.proto``).
Required unless authentication is used with a user token or a custom token specifying an application-id.
In that case, the token's user-id, respectively application-id, will be used for the request's application_id. 

.. _com.daml.ledger.api.v2.ReassignmentCommand.command_id:

``command_id`` : :ref:`string <string>`

Uniquely identifies the command.
The triple (application_id, submitter, command_id) constitutes the change ID for the intended ledger change.
The change ID can be used for matching the intended ledger changes with all their completions.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ReassignmentCommand.submitter:

``submitter`` : :ref:`string <string>`

Party on whose behalf the command should be executed.
If ledger API authorization is enabled, then the authorization metadata must authorize the sender of the request
to act on behalf of the given party.
Must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.ReassignmentCommand.command.unassign_command:

``oneof command.unassign_command`` : :ref:`UnassignCommand <com.daml.ledger.api.v2.UnassignCommand>`

 

.. _com.daml.ledger.api.v2.ReassignmentCommand.command.assign_command:

``oneof command.assign_command`` : :ref:`AssignCommand <com.daml.ledger.api.v2.AssignCommand>`

 

.. _com.daml.ledger.api.v2.ReassignmentCommand.submission_id:

``submission_id`` : :ref:`string <string>`

A unique identifier to distinguish completions for different submissions with the same change ID.
Typically a random UUID. Applications are expected to use a different UUID for each retry of a submission
with the same change ID.
Must be a valid LedgerString (as described in ``value.proto``).

If omitted, the participant or the committer may set a value of their choice.
Optional 

.. _com.daml.ledger.api.v2.UnassignCommand:

UnassignCommand message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Unassign a contract

.. _com.daml.ledger.api.v2.UnassignCommand.contract_id:

``contract_id`` : :ref:`string <string>`

The ID of the contract the client wants to unassign.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.UnassignCommand.source:

``source`` : :ref:`string <string>`

The ID of the source synchronizer
Must be a valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.UnassignCommand.target:

``target`` : :ref:`string <string>`

The ID of the target synchronizer
Must be a valid synchronizer id
Required 


----

.. _com/daml/ledger/api/v2/state_service.proto:

``com/daml/ledger/api/v2/state_service.proto``

.. _com.daml.ledger.api.v2.StateService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
StateService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to get state from the ledger.

.. _com.daml.ledger.api.v2.StateService.GetActiveContracts:

GetActiveContracts method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Returns a stream of the snapshot of the active contracts and incomplete (un)assignments at a ledger offset.
If there are no active contracts, the stream returns a single response message with the offset at which the snapshot has been taken.
Clients SHOULD use the offset in the last GetActiveContractsResponse message to continue streaming transactions with the update service.
Clients SHOULD NOT assume that the set of active contracts they receive reflects the state at the ledger end.

* Request: :ref:`GetActiveContractsRequest <com.daml.ledger.api.v2.GetActiveContractsRequest>`
* Response: :ref:`GetActiveContractsResponse <com.daml.ledger.api.v2.GetActiveContractsResponse>`



.. _com.daml.ledger.api.v2.StateService.GetConnectedSynchronizers:

GetConnectedSynchronizers method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Get the list of connected synchronizers at the time of the query.

* Request: :ref:`GetConnectedSynchronizersRequest <com.daml.ledger.api.v2.GetConnectedSynchronizersRequest>`
* Response: :ref:`GetConnectedSynchronizersResponse <com.daml.ledger.api.v2.GetConnectedSynchronizersResponse>`



.. _com.daml.ledger.api.v2.StateService.GetLedgerEnd:

GetLedgerEnd method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Get the current ledger end.
Subscriptions started with the returned offset will serve events after this RPC was called.

* Request: :ref:`GetLedgerEndRequest <com.daml.ledger.api.v2.GetLedgerEndRequest>`
* Response: :ref:`GetLedgerEndResponse <com.daml.ledger.api.v2.GetLedgerEndResponse>`



.. _com.daml.ledger.api.v2.StateService.GetLatestPrunedOffsets:

GetLatestPrunedOffsets method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Get the latest successfully pruned ledger offsets

* Request: :ref:`GetLatestPrunedOffsetsRequest <com.daml.ledger.api.v2.GetLatestPrunedOffsetsRequest>`
* Response: :ref:`GetLatestPrunedOffsetsResponse <com.daml.ledger.api.v2.GetLatestPrunedOffsetsResponse>`



.. _com.daml.ledger.api.v2.ActiveContract:

ActiveContract message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.ActiveContract.created_event:

``created_event`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

Required
The event as it appeared in the context of its last update (i.e. daml transaction or
reassignment). In particular, the last offset, node_id pair is preserved.
The last update is the most recent update created or assigned this contract on synchronizer_id synchronizer.
The offset of the CreatedEvent might point to an already pruned update, therefore it cannot necessarily be used
for lookups. 

.. _com.daml.ledger.api.v2.ActiveContract.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

A valid synchronizer id
Required 

.. _com.daml.ledger.api.v2.ActiveContract.reassignment_counter:

``reassignment_counter`` : :ref:`uint64 <uint64>`

Each corresponding assigned and unassigned event has the same reassignment_counter. This strictly increases
with each unassign command for the same contract. Creation of the contract corresponds to reassignment_counter
equals zero.
This field will be the reassignment_counter of the latest observable activation event on this synchronizer, which is
before the active_at_offset.
Required 

.. _com.daml.ledger.api.v2.GetActiveContractsRequest:

GetActiveContractsRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

If the given offset is different than the ledger end, and there are (un)assignments in-flight at the given offset,
the snapshot may fail with "FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED".
Note that it is ok to request acs snapshots for party migration with offsets other than ledger end, because party
migration is not concerned with incomplete (un)assignments.

.. _com.daml.ledger.api.v2.GetActiveContractsRequest.filter:

``filter`` : :ref:`TransactionFilter <com.daml.ledger.api.v2.TransactionFilter>`

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Templates to include in the served snapshot, per party.
Optional, if specified event_format must be unset, if not specified event_format must be set. 

.. _com.daml.ledger.api.v2.GetActiveContractsRequest.verbose:

``verbose`` : :ref:`bool <bool>`

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
If enabled, values served over the API will contain more information than strictly necessary to interpret the data.
In particular, setting the verbose flag to true triggers the ledger to include labels for record fields.
Optional, if specified event_format must be unset. 

.. _com.daml.ledger.api.v2.GetActiveContractsRequest.active_at_offset:

``active_at_offset`` : :ref:`int64 <int64>`

The offset at which the snapshot of the active contracts will be computed.
Must be no greater than the current ledger end offset.
Must be greater than or equal to the last pruning offset.
Required, must be a valid absolute offset (positive integer) or ledger begin offset (zero).
If zero, the empty set will be returned. 

.. _com.daml.ledger.api.v2.GetActiveContractsRequest.event_format:

``event_format`` : :ref:`EventFormat <com.daml.ledger.api.v2.EventFormat>`

Format of the contract_entries in the result. In case of CreatedEvent the presentation will be of
TRANSACTION_SHAPE_ACS_DELTA.
Optional for backwards compatibility, defaults to an EventFormat where:

  - filter is the filter field from this request
  - verbose is the verbose field from this request 

.. _com.daml.ledger.api.v2.GetActiveContractsResponse:

GetActiveContractsResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetActiveContractsResponse.workflow_id:

``workflow_id`` : :ref:`string <string>`

The workflow ID used in command submission which corresponds to the contract_entry. Only set if
the ``workflow_id`` for the command was set.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.GetActiveContractsResponse.contract_entry.active_contract:

``oneof contract_entry.active_contract`` : :ref:`ActiveContract <com.daml.ledger.api.v2.ActiveContract>`

The contract is active on the assigned synchronizer, meaning: there was an activation event on the given synchronizer (
created, assigned), which is not followed by a deactivation event (archived, unassigned) on the same
synchronizer, until the active_at_offset.
Since activeness is defined as a per synchronizer concept, it is possible, that a contract is active on one
synchronizer, but already archived on another.
There will be one such message for each synchronizer the contract is active on. 

.. _com.daml.ledger.api.v2.GetActiveContractsResponse.contract_entry.incomplete_unassigned:

``oneof contract_entry.incomplete_unassigned`` : :ref:`IncompleteUnassigned <com.daml.ledger.api.v2.IncompleteUnassigned>`

Included iff the unassigned event was before or at the active_at_offset, but there was no corresponding
assigned event before or at the active_at_offset. 

.. _com.daml.ledger.api.v2.GetActiveContractsResponse.contract_entry.incomplete_assigned:

``oneof contract_entry.incomplete_assigned`` : :ref:`IncompleteAssigned <com.daml.ledger.api.v2.IncompleteAssigned>`

Important: this message is not indicating that the contract is active on the target synchronizer!
Included iff the assigned event was before or at the active_at_offset, but there was no corresponding
unassigned event before or at the active_at_offset. 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersRequest:

GetConnectedSynchronizersRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetConnectedSynchronizersRequest.party:

``party`` : :ref:`string <string>`

The party of interest
Must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersRequest.participant_id:

``participant_id`` : :ref:`string <string>`

The id of a participant whose mapping of a party to connected synchronizers is requested.
Must be a valid participant-id retrieved through a prior call to getParticipantId.
Defaults to the participant id of the host participant.
Optional 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse:

GetConnectedSynchronizersResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.connected_synchronizers:

``connected_synchronizers`` : :ref:`GetConnectedSynchronizersResponse.ConnectedSynchronizer <com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.ConnectedSynchronizer>` (repeated)

 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.ConnectedSynchronizer:

GetConnectedSynchronizersResponse.ConnectedSynchronizer message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.ConnectedSynchronizer.synchronizer_alias:

``synchronizer_alias`` : :ref:`string <string>`

The alias of the synchronizer
Required 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.ConnectedSynchronizer.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

The ID of the synchronizer
Required 

.. _com.daml.ledger.api.v2.GetConnectedSynchronizersResponse.ConnectedSynchronizer.permission:

``permission`` : :ref:`ParticipantPermission <com.daml.ledger.api.v2.ParticipantPermission>`

The permission on the synchronizer
Required 

.. _com.daml.ledger.api.v2.GetLatestPrunedOffsetsRequest:

GetLatestPrunedOffsetsRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Empty for now, but may contain fields in the future.

Message has no fields.

.. _com.daml.ledger.api.v2.GetLatestPrunedOffsetsResponse:

GetLatestPrunedOffsetsResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetLatestPrunedOffsetsResponse.participant_pruned_up_to_inclusive:

``participant_pruned_up_to_inclusive`` : :ref:`int64 <int64>`

It will always be a non-negative integer.
If positive, the absolute offset up to which the ledger has been pruned,
disregarding the state of all divulged contracts pruning.
If zero, the ledger has not been pruned yet. 

.. _com.daml.ledger.api.v2.GetLatestPrunedOffsetsResponse.all_divulged_contracts_pruned_up_to_inclusive:

``all_divulged_contracts_pruned_up_to_inclusive`` : :ref:`int64 <int64>`

It will always be a non-negative integer.
If positive, the absolute offset up to which all divulged events have been pruned on the ledger.
It can be at or before the ``participant_pruned_up_to_inclusive`` offset.
For more details about all divulged events pruning,
see ``PruneRequest.prune_all_divulged_contracts`` in ``participant_pruning_service.proto``.
If zero, the divulged events have not been pruned yet. 

.. _com.daml.ledger.api.v2.GetLedgerEndRequest:

GetLedgerEndRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.GetLedgerEndResponse:

GetLedgerEndResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetLedgerEndResponse.offset:

``offset`` : :ref:`int64 <int64>`

It will always be a non-negative integer.
If zero, the participant view of the ledger is empty.
If positive, the absolute offset of the ledger as viewed by the participant. 

.. _com.daml.ledger.api.v2.IncompleteAssigned:

IncompleteAssigned message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.IncompleteAssigned.assigned_event:

``assigned_event`` : :ref:`AssignedEvent <com.daml.ledger.api.v2.AssignedEvent>`

Required 

.. _com.daml.ledger.api.v2.IncompleteUnassigned:

IncompleteUnassigned message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.IncompleteUnassigned.created_event:

``created_event`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

Required
The event as it appeared in the context of its last activation update (i.e. daml transaction or
reassignment). In particular, the last activation offset, node_id pair is preserved.
The last activation update is the most recent update created or assigned this contract on synchronizer_id synchronizer before
the unassigned_event.
The offset of the CreatedEvent might point to an already pruned update, therefore it cannot necessarily be used
for lookups. 

.. _com.daml.ledger.api.v2.IncompleteUnassigned.unassigned_event:

``unassigned_event`` : :ref:`UnassignedEvent <com.daml.ledger.api.v2.UnassignedEvent>`

Required 




.. _com.daml.ledger.api.v2.ParticipantPermission:

ParticipantPermission enum, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Enum indicating the permission level that the participant has for the party
whose connected synchronizers are being listed.

.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED:

       PARTICIPANT_PERMISSION_UNSPECIFIED
     - 0
     - 

   * - .. _com.daml.ledger.api.v2.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION:

       PARTICIPANT_PERMISSION_SUBMISSION
     - 1
     - 

   * - .. _com.daml.ledger.api.v2.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION:

       PARTICIPANT_PERMISSION_CONFIRMATION
     - 2
     - participant can only confirm transactions

   * - .. _com.daml.ledger.api.v2.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION:

       PARTICIPANT_PERMISSION_OBSERVATION
     - 3
     - participant can only observe transactions

   

----

.. _com/daml/ledger/api/v2/testing/time_service.proto:

``com/daml/ledger/api/v2/testing/time_service.proto``

.. _com.daml.ledger.api.v2.testing.TimeService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TimeService, |version com.daml.ledger.api.v2.testing|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Optional service, exposed for testing static time scenarios.

.. _com.daml.ledger.api.v2.testing.TimeService.GetTime:

GetTime method, |version com.daml.ledger.api.v2.testing|
========================================================================================================================================================================================================

Returns the current time according to the ledger server.

* Request: :ref:`GetTimeRequest <com.daml.ledger.api.v2.testing.GetTimeRequest>`
* Response: :ref:`GetTimeResponse <com.daml.ledger.api.v2.testing.GetTimeResponse>`



.. _com.daml.ledger.api.v2.testing.TimeService.SetTime:

SetTime method, |version com.daml.ledger.api.v2.testing|
========================================================================================================================================================================================================

Allows clients to change the ledger's clock in an atomic get-and-set operation.

* Request: :ref:`SetTimeRequest <com.daml.ledger.api.v2.testing.SetTimeRequest>`
* Response:  `.google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#empty>`__



.. _com.daml.ledger.api.v2.testing.GetTimeRequest:

GetTimeRequest message, |version com.daml.ledger.api.v2.testing|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.testing.GetTimeResponse:

GetTimeResponse message, |version com.daml.ledger.api.v2.testing|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.testing.GetTimeResponse.current_time:

``current_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The current time according to the ledger server. 

.. _com.daml.ledger.api.v2.testing.SetTimeRequest:

SetTimeRequest message, |version com.daml.ledger.api.v2.testing|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.testing.SetTimeRequest.current_time:

``current_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

MUST precisely match the current time as it's known to the ledger server. 

.. _com.daml.ledger.api.v2.testing.SetTimeRequest.new_time:

``new_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time the client wants to set on the ledger.
MUST be a point int time after ``current_time``. 


----

.. _com/daml/ledger/api/v2/topology_transaction.proto:

``com/daml/ledger/api/v2/topology_transaction.proto``

.. _com.daml.ledger.api.v2.ParticipantAuthorizationChanged:

ParticipantAuthorizationChanged message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.ParticipantAuthorizationChanged.party_id:

``party_id`` : :ref:`string <string>`

Required 

.. _com.daml.ledger.api.v2.ParticipantAuthorizationChanged.participant_id:

``participant_id`` : :ref:`string <string>`

Required 

.. _com.daml.ledger.api.v2.ParticipantAuthorizationChanged.participant_permission:

``participant_permission`` : :ref:`ParticipantPermission <com.daml.ledger.api.v2.ParticipantPermission>`

Required 

.. _com.daml.ledger.api.v2.ParticipantAuthorizationRevoked:

ParticipantAuthorizationRevoked message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.ParticipantAuthorizationRevoked.party_id:

``party_id`` : :ref:`string <string>`

Required 

.. _com.daml.ledger.api.v2.ParticipantAuthorizationRevoked.participant_id:

``participant_id`` : :ref:`string <string>`

Required 

.. _com.daml.ledger.api.v2.TopologyEvent:

TopologyEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.TopologyEvent.event.participant_authorization_changed:

``oneof event.participant_authorization_changed`` : :ref:`ParticipantAuthorizationChanged <com.daml.ledger.api.v2.ParticipantAuthorizationChanged>`

 

.. _com.daml.ledger.api.v2.TopologyEvent.event.participant_authorization_revoked:

``oneof event.participant_authorization_revoked`` : :ref:`ParticipantAuthorizationRevoked <com.daml.ledger.api.v2.ParticipantAuthorizationRevoked>`

 

.. _com.daml.ledger.api.v2.TopologyTransaction:

TopologyTransaction message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.TopologyTransaction.update_id:

``update_id`` : :ref:`string <string>`

Assigned by the server. Useful for correlating logs.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.TopologyTransaction.offset:

``offset`` : :ref:`int64 <int64>`

The absolute offset. The details of this field are described in ``community/ledger-api/README.md``.
Required, it is a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.TopologyTransaction.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

A valid synchronizer id.
Identifies the synchronizer that synchronized the topology transaction.
Required 

.. _com.daml.ledger.api.v2.TopologyTransaction.record_time:

``record_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time at which the changes in the topology transaction become effective. There is a small delay between a
topology transaction being sequenced and the changes it contains becoming effective. Topology transactions appear
in order relative to a synchronizer based on their effective time rather than their sequencing time.
Required 

.. _com.daml.ledger.api.v2.TopologyTransaction.events:

``events`` : :ref:`TopologyEvent <com.daml.ledger.api.v2.TopologyEvent>` (repeated)

A non-empty list of topology events.
Required 

.. _com.daml.ledger.api.v2.TopologyTransaction.trace_context:

``trace_context`` : :ref:`TraceContext <com.daml.ledger.api.v2.TraceContext>`

Optional; ledger api trace context

The trace context transported in this message corresponds to the trace context supplied
by the client application in a HTTP2 header of the original command submission.
We typically use a header to transfer this type of information. Here we use message
body, because it is used in gRPC streams which do not support per message headers.
This field will be populated with the trace context contained in the original submission.
If that was not provided, a unique ledger-api-server generated trace context will be used
instead. 


----

.. _com/daml/ledger/api/v2/trace_context.proto:

``com/daml/ledger/api/v2/trace_context.proto``

.. _com.daml.ledger.api.v2.TraceContext:

TraceContext message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.TraceContext._traceparent.traceparent:

``oneof _traceparent.traceparent`` : :ref:`string <string>` (optional)

https://www.w3.org/TR/trace-context/ 

.. _com.daml.ledger.api.v2.TraceContext._tracestate.tracestate:

``oneof _tracestate.tracestate`` : :ref:`string <string>` (optional)

 


----

.. _com/daml/ledger/api/v2/transaction.proto:

``com/daml/ledger/api/v2/transaction.proto``

.. _com.daml.ledger.api.v2.Transaction:

Transaction message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Filtered view of an on-ledger transaction's create and archive events.

.. _com.daml.ledger.api.v2.Transaction.update_id:

``update_id`` : :ref:`string <string>`

Assigned by the server. Useful for correlating logs.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.Transaction.command_id:

``command_id`` : :ref:`string <string>`

The ID of the command which resulted in this transaction. Missing for everyone except the submitting party.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Transaction.workflow_id:

``workflow_id`` : :ref:`string <string>`

The workflow ID used in command submission.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.Transaction.effective_at:

``effective_at`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Ledger effective time.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.Transaction.events:

``events`` : :ref:`Event <com.daml.ledger.api.v2.Event>` (repeated)

The collection of events.
Contains:

  - ``CreatedEvent`` or ``ArchivedEvent`` in case of ACS_DELTA transaction shape
  - ``CreatedEvent`` or ``ExercisedEvent`` in case of LEDGER_EFFECTS transaction shape

Required 

.. _com.daml.ledger.api.v2.Transaction.offset:

``offset`` : :ref:`int64 <int64>`

The absolute offset. The details of this field are described in ``community/ledger-api/README.md``.
Required, it is a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.Transaction.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

A valid synchronizer id.
Identifies the synchronizer that synchronized the transaction.
Required 

.. _com.daml.ledger.api.v2.Transaction.trace_context:

``trace_context`` : :ref:`TraceContext <com.daml.ledger.api.v2.TraceContext>`

Optional; ledger api trace context

The trace context transported in this message corresponds to the trace context supplied
by the client application in a HTTP2 header of the original command submission.
We typically use a header to transfer this type of information. Here we use message
body, because it is used in gRPC streams which do not support per message headers.
This field will be populated with the trace context contained in the original submission.
If that was not provided, a unique ledger-api-server generated trace context will be used
instead. 

.. _com.daml.ledger.api.v2.Transaction.record_time:

``record_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time at which the transaction was recorded. The record time refers to the synchronizer
which synchronized the transaction.
Required 

.. _com.daml.ledger.api.v2.TransactionTree:

TransactionTree message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Complete view of an on-ledger transaction.

.. _com.daml.ledger.api.v2.TransactionTree.update_id:

``update_id`` : :ref:`string <string>`

Assigned by the server. Useful for correlating logs.
Must be a valid LedgerString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.TransactionTree.command_id:

``command_id`` : :ref:`string <string>`

The ID of the command which resulted in this transaction. Missing for everyone except the submitting party.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.TransactionTree.workflow_id:

``workflow_id`` : :ref:`string <string>`

The workflow ID used in command submission. Only set if the ``workflow_id`` for the command was set.
Must be a valid LedgerString (as described in ``value.proto``).
Optional 

.. _com.daml.ledger.api.v2.TransactionTree.effective_at:

``effective_at`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

Ledger effective time.
Required 

.. _com.daml.ledger.api.v2.TransactionTree.offset:

``offset`` : :ref:`int64 <int64>`

The absolute offset. The details of this field are described in ``community/ledger-api/README.md``.
Required, it is a valid absolute offset (positive integer). 

.. _com.daml.ledger.api.v2.TransactionTree.events_by_id:

``events_by_id`` : :ref:`TransactionTree.EventsByIdEntry <com.daml.ledger.api.v2.TransactionTree.EventsByIdEntry>` (repeated)

Changes to the ledger that were caused by this transaction. Nodes of the transaction tree.
Each key must be a valid node ID (non-negative integer).
Required 

.. _com.daml.ledger.api.v2.TransactionTree.synchronizer_id:

``synchronizer_id`` : :ref:`string <string>`

A valid synchronizer id.
Identifies the synchronizer that synchronized the transaction.
Required 

.. _com.daml.ledger.api.v2.TransactionTree.trace_context:

``trace_context`` : :ref:`TraceContext <com.daml.ledger.api.v2.TraceContext>`

Optional; ledger api trace context

The trace context transported in this message corresponds to the trace context supplied
by the client application in a HTTP2 header of the original command submission.
We typically use a header to transfer this type of information. Here we use message
body, because it is used in gRPC streams which do not support per message headers.
This field will be populated with the trace context contained in the original submission.
If that was not provided, a unique ledger-api-server generated trace context will be used
instead. 

.. _com.daml.ledger.api.v2.TransactionTree.record_time:

``record_time`` :  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#timestamp>`__

The time at which the transaction was recorded. The record time refers to the synchronizer
which synchronized the transaction.
Required 

.. _com.daml.ledger.api.v2.TransactionTree.EventsByIdEntry:

TransactionTree.EventsByIdEntry message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.TransactionTree.EventsByIdEntry.key:

``key`` : :ref:`int32 <int32>`

 

.. _com.daml.ledger.api.v2.TransactionTree.EventsByIdEntry.value:

``value`` : :ref:`TreeEvent <com.daml.ledger.api.v2.TreeEvent>`

 

.. _com.daml.ledger.api.v2.TreeEvent:

TreeEvent message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Each tree event message type below contains a ``witness_parties`` field which
indicates the subset of the requested parties that can see the event
in question.

Note that transaction trees might contain events with
_no_ witness parties, which were included simply because they were
children of events which have witnesses.

.. _com.daml.ledger.api.v2.TreeEvent.kind.created:

``oneof kind.created`` : :ref:`CreatedEvent <com.daml.ledger.api.v2.CreatedEvent>`

 

.. _com.daml.ledger.api.v2.TreeEvent.kind.exercised:

``oneof kind.exercised`` : :ref:`ExercisedEvent <com.daml.ledger.api.v2.ExercisedEvent>`

 


----

.. _com/daml/ledger/api/v2/transaction_filter.proto:

``com/daml/ledger/api/v2/transaction_filter.proto``

.. _com.daml.ledger.api.v2.CumulativeFilter:

CumulativeFilter message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A filter that matches all contracts that are either an instance of one of
the ``template_filters`` or that match one of the ``interface_filters``.

.. _com.daml.ledger.api.v2.CumulativeFilter.identifier_filter.wildcard_filter:

``oneof identifier_filter.wildcard_filter`` : :ref:`WildcardFilter <com.daml.ledger.api.v2.WildcardFilter>`

A wildcard filter that matches all templates
Optional 

.. _com.daml.ledger.api.v2.CumulativeFilter.identifier_filter.interface_filter:

``oneof identifier_filter.interface_filter`` : :ref:`InterfaceFilter <com.daml.ledger.api.v2.InterfaceFilter>`

Include an ``InterfaceView`` for every ``InterfaceFilter`` matching a contract.
The ``InterfaceFilter`` instances MUST each use a unique ``interface_id``.
Optional 

.. _com.daml.ledger.api.v2.CumulativeFilter.identifier_filter.template_filter:

``oneof identifier_filter.template_filter`` : :ref:`TemplateFilter <com.daml.ledger.api.v2.TemplateFilter>`

A template for which the data will be included in the
``create_arguments`` of a matching ``CreatedEvent``.
If a contract is simultaneously selected by a template filter and one or more interface filters,
the corresponding ``include_created_event_blob`` are consolidated using an OR operation.
Optional 

.. _com.daml.ledger.api.v2.EventFormat:

EventFormat message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A format for events which defines both which events should be included
and what data should be computed and included for them.

Note that some of the filtering behavior depends on the `TransactionShape`,
which is expected to be specified alongside usages of `EventFormat`.

.. _com.daml.ledger.api.v2.EventFormat.filters_by_party:

``filters_by_party`` : :ref:`EventFormat.FiltersByPartyEntry <com.daml.ledger.api.v2.EventFormat.FiltersByPartyEntry>` (repeated)

Each key must be a valid PartyIdString (as described in ``value.proto``).
The interpretation of the filter depends on the transaction-shape being filtered:

1. For **ledger-effects** create and exercise events are returned, for which the witnesses include at least one of
    the listed parties and match the per-party filter.
2. For **transaction and active-contract-set streams** create and archive events are returned for all contracts whose
    stakeholders include at least one of the listed parties and match the per-party filter.

Optional 

.. _com.daml.ledger.api.v2.EventFormat.filters_for_any_party:

``filters_for_any_party`` : :ref:`Filters <com.daml.ledger.api.v2.Filters>`

Wildcard filters that apply to all the parties existing on the participant. The interpretation of the filters is the same
with the per-party filter as described above.
Optional 

.. _com.daml.ledger.api.v2.EventFormat.verbose:

``verbose`` : :ref:`bool <bool>`

If enabled, values served over the API will contain more information than strictly necessary to interpret the data.
In particular, setting the verbose flag to true triggers the ledger to include labels for record fields.
Optional 

.. _com.daml.ledger.api.v2.EventFormat.FiltersByPartyEntry:

EventFormat.FiltersByPartyEntry message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.EventFormat.FiltersByPartyEntry.key:

``key`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.EventFormat.FiltersByPartyEntry.value:

``value`` : :ref:`Filters <com.daml.ledger.api.v2.Filters>`

 

.. _com.daml.ledger.api.v2.Filters:

Filters message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

The union of a set of template filters, interface filters, or a wildcard.

.. _com.daml.ledger.api.v2.Filters.cumulative:

``cumulative`` : :ref:`CumulativeFilter <com.daml.ledger.api.v2.CumulativeFilter>` (repeated)

Every filter in the cumulative list expands the scope of the resulting stream. Each interface,
template or wildcard filter means additional events that will match the query.
The impact of include_interface_view and include_created_event_blob fields in the filters will
also be accumulated.
At least one cumulative filter MUST be specified.
A template or an interface SHOULD NOT appear twice in the accumulative field.
A wildcard filter SHOULD NOT be defined more than once in the accumulative field.
Optional 

.. _com.daml.ledger.api.v2.InterfaceFilter:

InterfaceFilter message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

This filter matches contracts that implement a specific interface.

.. _com.daml.ledger.api.v2.InterfaceFilter.interface_id:

``interface_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

The interface that a matching contract must implement.
The ``interface_id`` needs to be valid: corresponding interface should be defined in
one of the available packages at the time of the query.
Required 

.. _com.daml.ledger.api.v2.InterfaceFilter.include_interface_view:

``include_interface_view`` : :ref:`bool <bool>`

Whether to include the interface view on the contract in the returned ``CreatedEvent``.
Use this to access contract data in a uniform manner in your API client.
Optional 

.. _com.daml.ledger.api.v2.InterfaceFilter.include_created_event_blob:

``include_created_event_blob`` : :ref:`bool <bool>`

Whether to include a ``created_event_blob`` in the returned ``CreatedEvent``.
Use this to access the contract create event payload in your API client
for submitting it as a disclosed contract with future commands.
Optional 

.. _com.daml.ledger.api.v2.ParticipantAuthorizationTopologyFormat:

ParticipantAuthorizationTopologyFormat message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A format specifying which participant authorization topology transactions to include and how to render them.

.. _com.daml.ledger.api.v2.ParticipantAuthorizationTopologyFormat.parties:

``parties`` : :ref:`string <string>` (repeated)

List of parties for which the topology transactions should be sent.
Empty means: for all parties. 

.. _com.daml.ledger.api.v2.TemplateFilter:

TemplateFilter message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

This filter matches contracts of a specific template.

.. _com.daml.ledger.api.v2.TemplateFilter.template_id:

``template_id`` : :ref:`Identifier <com.daml.ledger.api.v2.Identifier>`

A template for which the payload should be included in the response.
The ``template_id`` needs to be valid: corresponding template should be defined in
one of the available packages at the time of the query.
Required 

.. _com.daml.ledger.api.v2.TemplateFilter.include_created_event_blob:

``include_created_event_blob`` : :ref:`bool <bool>`

Whether to include a ``created_event_blob`` in the returned ``CreatedEvent``.
Use this to access the contract event payload in your API client
for submitting it as a disclosed contract with future commands.
Optional 

.. _com.daml.ledger.api.v2.TopologyFormat:

TopologyFormat message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A format specifying which topology transactions to include and how to render them.

.. _com.daml.ledger.api.v2.TopologyFormat.include_participant_authorization_events:

``include_participant_authorization_events`` : :ref:`ParticipantAuthorizationTopologyFormat <com.daml.ledger.api.v2.ParticipantAuthorizationTopologyFormat>`

Include participant authorization topology events in streams.
Optional, if unset no participant authorization topology events are emitted in the stream. 

.. _com.daml.ledger.api.v2.TransactionFilter:

TransactionFilter message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Used both for filtering create and archive events as well as for filtering transaction trees.

.. _com.daml.ledger.api.v2.TransactionFilter.filters_by_party:

``filters_by_party`` : :ref:`TransactionFilter.FiltersByPartyEntry <com.daml.ledger.api.v2.TransactionFilter.FiltersByPartyEntry>` (repeated)

Each key must be a valid PartyIdString (as described in ``value.proto``).
The interpretation of the filter depends on the transaction-shape being filtered:

1. For **transaction trees** (used in GetUpdateTreesResponse for backwards compatibility) all party keys used as
    wildcard filters, and all subtrees whose root has one of the listed parties as an informee are returned.
    If there are ``CumulativeFilter``s, those will control returned ``CreatedEvent`` fields where applicable, but will
    not be used for template/interface filtering.
2. For **ledger-effects** create and exercise events are returned, for which the witnesses include at least one of
    the listed parties and match the per-party filter.
3. For **transaction and active-contract-set streams** create and archive events are returned for all contracts whose
    stakeholders include at least one of the listed parties and match the per-party filter.

Required 

.. _com.daml.ledger.api.v2.TransactionFilter.filters_for_any_party:

``filters_for_any_party`` : :ref:`Filters <com.daml.ledger.api.v2.Filters>`

Wildcard filters that apply to all the parties existing on the participant. The interpretation of the filters is the same
with the per-party filter as described above. 

.. _com.daml.ledger.api.v2.TransactionFilter.FiltersByPartyEntry:

TransactionFilter.FiltersByPartyEntry message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.TransactionFilter.FiltersByPartyEntry.key:

``key`` : :ref:`string <string>`

 

.. _com.daml.ledger.api.v2.TransactionFilter.FiltersByPartyEntry.value:

``value`` : :ref:`Filters <com.daml.ledger.api.v2.Filters>`

 

.. _com.daml.ledger.api.v2.TransactionFormat:

TransactionFormat message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A format that specifies what events to include in Daml transactions
and what data to compute and include for them.

.. _com.daml.ledger.api.v2.TransactionFormat.event_format:

``event_format`` : :ref:`EventFormat <com.daml.ledger.api.v2.EventFormat>`

Required 

.. _com.daml.ledger.api.v2.TransactionFormat.transaction_shape:

``transaction_shape`` : :ref:`TransactionShape <com.daml.ledger.api.v2.TransactionShape>`

What transaction shape to use for interpreting the filters of the event format.
Required 

.. _com.daml.ledger.api.v2.UpdateFormat:

UpdateFormat message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

A format specifying what updates to include and how to render them.

.. _com.daml.ledger.api.v2.UpdateFormat.include_transactions:

``include_transactions`` : :ref:`TransactionFormat <com.daml.ledger.api.v2.TransactionFormat>`

Include daml transactions in streams.
Optional, if unset, no transactions are emitted in the stream. 

.. _com.daml.ledger.api.v2.UpdateFormat.include_reassignments:

``include_reassignments`` : :ref:`EventFormat <com.daml.ledger.api.v2.EventFormat>`

Include (un)assignments in the stream.
The events in the result, will take shape TRANSACTION_SHAPE_ACS_DELTA.
Optional, if unset, no (un)assignments are emitted in the stream. 

.. _com.daml.ledger.api.v2.UpdateFormat.include_topology_events:

``include_topology_events`` : :ref:`TopologyFormat <com.daml.ledger.api.v2.TopologyFormat>`

Include topology events in streams.
Optional, if unset no topology events are emitted in the stream. 

.. _com.daml.ledger.api.v2.WildcardFilter:

WildcardFilter message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

This filter matches all templates.

.. _com.daml.ledger.api.v2.WildcardFilter.include_created_event_blob:

``include_created_event_blob`` : :ref:`bool <bool>`

Whether to include a ``created_event_blob`` in the returned ``CreatedEvent``.
Use this to access the contract create event payload in your API client
for submitting it as a disclosed contract with future commands.
Optional 




.. _com.daml.ledger.api.v2.TransactionShape:

TransactionShape enum, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Event shape for Transactions.
Shapes are exclusive and only one of them can be defined in queries.

.. list-table::
   :header-rows: 0
   :width: 100%

   * - .. _com.daml.ledger.api.v2.TransactionShape.TRANSACTION_SHAPE_UNSPECIFIED:

       TRANSACTION_SHAPE_UNSPECIFIED
     - 0
     - Following official proto3 convention, not intended for actual use.

   * - .. _com.daml.ledger.api.v2.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA:

       TRANSACTION_SHAPE_ACS_DELTA
     - 1
     - Transaction shape that is sufficient to maintain an accurate ACS view. The field witness_parties in events are populated as stakeholders, transaction filter will apply accordingly. This translates to create and archive events.

   * - .. _com.daml.ledger.api.v2.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS:

       TRANSACTION_SHAPE_LEDGER_EFFECTS
     - 2
     - Transaction shape that allows maintaining an ACS and also conveys detailed information about all exercises. The field witness_parties in events are populated as cumulative informees, transaction filter will apply accordingly. This translates to create, consuming exercise and non-consuming exercise.

   

----

.. _com/daml/ledger/api/v2/update_service.proto:

``com/daml/ledger/api/v2/update_service.proto``

.. _com.daml.ledger.api.v2.UpdateService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UpdateService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to read updates (transactions, (un)assignments, topology events) from the ledger.

``GetUpdates`` and ``GetUpdateTrees`` provide a comprehensive stream of updates/changes
which happened on the virtual shared ledger. These streams are indexed with ledger
offsets, which are strictly increasing.
The virtual shared ledger consist of changes happening on multiple synchronizers which are
connected to the serving participant. Each update belongs to one synchronizer, this is
provided in the result (the ``synchronizer_id`` field in ``Transaction`` and ``TransactionTree``
for transactions, the ``source`` field in ``UnassignedEvent`` and the ``target`` field in ``AssignedEvent``).
Consumers can rely on strong causal guarantees on the virtual shared ledger for a single
synchronizer: updates which have greater offsets are happened after than updates with smaller
offsets for the same synchronizer. Across different synchronizers this is not guaranteed.

.. _com.daml.ledger.api.v2.UpdateService.GetUpdates:

GetUpdates method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Read the ledger's filtered update stream for the specified contents and filters.
It returns the event types in accordance with the stream contents selected. Also the selection criteria
for individual events depends on the transaction shape chosen.
- ACS delta: a requesting party must be a stakeholder of an event for it to be included.
- ledger effects: a requesting party must be a witness of an en event for it to be included.

* Request: :ref:`GetUpdatesRequest <com.daml.ledger.api.v2.GetUpdatesRequest>`
* Response: :ref:`GetUpdatesResponse <com.daml.ledger.api.v2.GetUpdatesResponse>`



.. _com.daml.ledger.api.v2.UpdateService.GetUpdateTrees:

GetUpdateTrees method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Read the ledger's complete transaction tree stream and related (un)assignments for a set of parties.
The stream will be filtered only by the parties as wildcard parties.
The template/interface filters describe the respective fields in the ``CreatedEvent`` results.

* Request: :ref:`GetUpdatesRequest <com.daml.ledger.api.v2.GetUpdatesRequest>`
* Response: :ref:`GetUpdateTreesResponse <com.daml.ledger.api.v2.GetUpdateTreesResponse>`



.. _com.daml.ledger.api.v2.UpdateService.GetTransactionTreeByOffset:

GetTransactionTreeByOffset method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Lookup a transaction tree by its offset.
For looking up a transaction instead of a transaction tree, please see GetTransactionByOffset.

* Request: :ref:`GetTransactionByOffsetRequest <com.daml.ledger.api.v2.GetTransactionByOffsetRequest>`
* Response: :ref:`GetTransactionTreeResponse <com.daml.ledger.api.v2.GetTransactionTreeResponse>`



.. _com.daml.ledger.api.v2.UpdateService.GetTransactionTreeById:

GetTransactionTreeById method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Lookup a transaction tree by its ID.
For looking up a transaction instead of a transaction tree, please see GetTransactionById.

* Request: :ref:`GetTransactionByIdRequest <com.daml.ledger.api.v2.GetTransactionByIdRequest>`
* Response: :ref:`GetTransactionTreeResponse <com.daml.ledger.api.v2.GetTransactionTreeResponse>`



.. _com.daml.ledger.api.v2.UpdateService.GetTransactionByOffset:

GetTransactionByOffset method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Lookup a transaction by its offset.

* Request: :ref:`GetTransactionByOffsetRequest <com.daml.ledger.api.v2.GetTransactionByOffsetRequest>`
* Response: :ref:`GetTransactionResponse <com.daml.ledger.api.v2.GetTransactionResponse>`



.. _com.daml.ledger.api.v2.UpdateService.GetTransactionById:

GetTransactionById method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Lookup a transaction by its ID.

* Request: :ref:`GetTransactionByIdRequest <com.daml.ledger.api.v2.GetTransactionByIdRequest>`
* Response: :ref:`GetTransactionResponse <com.daml.ledger.api.v2.GetTransactionResponse>`



.. _com.daml.ledger.api.v2.GetTransactionByIdRequest:

GetTransactionByIdRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetTransactionByIdRequest.update_id:

``update_id`` : :ref:`string <string>`

The ID of a particular transaction.
Must be a valid LedgerString (as describe in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetTransactionByIdRequest.requesting_parties:

``requesting_parties`` : :ref:`string <string>` (repeated)

The parties whose events the client expects to see.
Events that are not visible for the parties in this collection will not be present in the response.
Each element be a valid PartyIdString (as describe in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetTransactionByOffsetRequest:

GetTransactionByOffsetRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetTransactionByOffsetRequest.offset:

``offset`` : :ref:`int64 <int64>`

The offset of the transaction being looked up.
Must be a valid absolute offset (positive integer).
Required 

.. _com.daml.ledger.api.v2.GetTransactionByOffsetRequest.requesting_parties:

``requesting_parties`` : :ref:`string <string>` (repeated)

The parties whose events the client expects to see.
Events that are not visible for the parties in this collection will not be present in the response.
Each element must be a valid PartyIdString (as described in ``value.proto``).
Required 

.. _com.daml.ledger.api.v2.GetTransactionResponse:

GetTransactionResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetTransactionResponse.transaction:

``transaction`` : :ref:`Transaction <com.daml.ledger.api.v2.Transaction>`

Required 

.. _com.daml.ledger.api.v2.GetTransactionTreeResponse:

GetTransactionTreeResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetTransactionTreeResponse.transaction:

``transaction`` : :ref:`TransactionTree <com.daml.ledger.api.v2.TransactionTree>`

Required 

.. _com.daml.ledger.api.v2.GetUpdateTreesResponse:

GetUpdateTreesResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.

.. _com.daml.ledger.api.v2.GetUpdateTreesResponse.update.transaction_tree:

``oneof update.transaction_tree`` : :ref:`TransactionTree <com.daml.ledger.api.v2.TransactionTree>`

 

.. _com.daml.ledger.api.v2.GetUpdateTreesResponse.update.reassignment:

``oneof update.reassignment`` : :ref:`Reassignment <com.daml.ledger.api.v2.Reassignment>`

 

.. _com.daml.ledger.api.v2.GetUpdateTreesResponse.update.offset_checkpoint:

``oneof update.offset_checkpoint`` : :ref:`OffsetCheckpoint <com.daml.ledger.api.v2.OffsetCheckpoint>`

 

.. _com.daml.ledger.api.v2.GetUpdateTreesResponse.update.topology_transaction:

``oneof update.topology_transaction`` : :ref:`TopologyTransaction <com.daml.ledger.api.v2.TopologyTransaction>`

Message returned only when the experimental feature is turned on 

.. _com.daml.ledger.api.v2.GetUpdatesRequest:

GetUpdatesRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetUpdatesRequest.begin_exclusive:

``begin_exclusive`` : :ref:`int64 <int64>`

Beginning of the requested ledger section (non-negative integer).
The response will only contain transactions whose offset is strictly greater than this.
If zero, the stream will start from the beginning of the ledger.
If positive, the streaming will start after this absolute offset.
If the ledger has been pruned, this parameter must be specified and be greater than the pruning offset. 

.. _com.daml.ledger.api.v2.GetUpdatesRequest._end_inclusive.end_inclusive:

``oneof _end_inclusive.end_inclusive`` : :ref:`int64 <int64>` (optional)

End of the requested ledger section.
The response will only contain transactions whose offset is less than or equal to this.
Optional, if empty, the stream will not terminate.
If specified, the stream will terminate after this absolute offset (positive integer) is reached. 

.. _com.daml.ledger.api.v2.GetUpdatesRequest.filter:

``filter`` : :ref:`TransactionFilter <com.daml.ledger.api.v2.TransactionFilter>`

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
Requesting parties with template filters.
Template filters must be empty for GetUpdateTrees requests.
Optional for backwards compatibility, if defined update_format must be unset 

.. _com.daml.ledger.api.v2.GetUpdatesRequest.verbose:

``verbose`` : :ref:`bool <bool>`

TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.
If enabled, values served over the API will contain more information than strictly necessary to interpret the data.
In particular, setting the verbose flag to true triggers the ledger to include labels, record and variant type ids
for record fields.
Optional for backwards compatibility, if defined update_format must be unset 

.. _com.daml.ledger.api.v2.GetUpdatesRequest.update_format:

``update_format`` : :ref:`UpdateFormat <com.daml.ledger.api.v2.UpdateFormat>`

Must be unset for GetUpdateTrees request.
Optional for backwards compatibility for GetUpdates request: defaults to an UpdateFormat where:

  - include_transactions.event_format.filter = the same filter specified on the request
  - include_transactions.event_format.verbose = the same flag specified on the request
  - include_transactions.transaction_shape = TRANSACTION_SHAPE_ACS_DELTA
  - include_reassignments.filter = the same filter specified on the request
  - include_reassignments.verbose = the same flag specified on the request
  - include_topology_events.include_participant_authorization_events.parties = all the parties specified in filter 

.. _com.daml.ledger.api.v2.GetUpdatesResponse:

GetUpdatesResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetUpdatesResponse.update.transaction:

``oneof update.transaction`` : :ref:`Transaction <com.daml.ledger.api.v2.Transaction>`

 

.. _com.daml.ledger.api.v2.GetUpdatesResponse.update.reassignment:

``oneof update.reassignment`` : :ref:`Reassignment <com.daml.ledger.api.v2.Reassignment>`

 

.. _com.daml.ledger.api.v2.GetUpdatesResponse.update.offset_checkpoint:

``oneof update.offset_checkpoint`` : :ref:`OffsetCheckpoint <com.daml.ledger.api.v2.OffsetCheckpoint>`

 

.. _com.daml.ledger.api.v2.GetUpdatesResponse.update.topology_transaction:

``oneof update.topology_transaction`` : :ref:`TopologyTransaction <com.daml.ledger.api.v2.TopologyTransaction>`

Message returned only when the experimental feature is turned on 


----

.. _com/daml/ledger/api/v2/version_service.proto:

``com/daml/ledger/api/v2/version_service.proto``

.. _com.daml.ledger.api.v2.VersionService:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
VersionService, |version com.daml.ledger.api.v2|
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Allows clients to retrieve information about the ledger API version

.. _com.daml.ledger.api.v2.VersionService.GetLedgerApiVersion:

GetLedgerApiVersion method, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================

Read the Ledger API version

* Request: :ref:`GetLedgerApiVersionRequest <com.daml.ledger.api.v2.GetLedgerApiVersionRequest>`
* Response: :ref:`GetLedgerApiVersionResponse <com.daml.ledger.api.v2.GetLedgerApiVersionResponse>`



.. _com.daml.ledger.api.v2.FeaturesDescriptor:

FeaturesDescriptor message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.FeaturesDescriptor.experimental:

``experimental`` : :ref:`ExperimentalFeatures <com.daml.ledger.api.v2.ExperimentalFeatures>`

Features under development or features that are used
for ledger implementation testing purposes only.

Daml applications SHOULD not depend on these in production. 

.. _com.daml.ledger.api.v2.FeaturesDescriptor.user_management:

``user_management`` : :ref:`UserManagementFeature <com.daml.ledger.api.v2.UserManagementFeature>`

If set, then the Ledger API server supports user management.
It is recommended that clients query this field to gracefully adjust their behavior for
ledgers that do not support user management. 

.. _com.daml.ledger.api.v2.FeaturesDescriptor.party_management:

``party_management`` : :ref:`PartyManagementFeature <com.daml.ledger.api.v2.PartyManagementFeature>`

If set, then the Ledger API server supports party management configurability.
It is recommended that clients query this field to gracefully adjust their behavior to
maximum party page size. 

.. _com.daml.ledger.api.v2.FeaturesDescriptor.offset_checkpoint:

``offset_checkpoint`` : :ref:`OffsetCheckpointFeature <com.daml.ledger.api.v2.OffsetCheckpointFeature>`

It contains the timeouts related to the periodic offset checkpoint emission 

.. _com.daml.ledger.api.v2.GetLedgerApiVersionRequest:

GetLedgerApiVersionRequest message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



Message has no fields.

.. _com.daml.ledger.api.v2.GetLedgerApiVersionResponse:

GetLedgerApiVersionResponse message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.GetLedgerApiVersionResponse.version:

``version`` : :ref:`string <string>`

The version of the ledger API. 

.. _com.daml.ledger.api.v2.GetLedgerApiVersionResponse.features:

``features`` : :ref:`FeaturesDescriptor <com.daml.ledger.api.v2.FeaturesDescriptor>`

The features supported by this Ledger API endpoint.

Daml applications CAN use the feature descriptor on top of
version constraints on the Ledger API version to determine
whether a given Ledger API endpoint supports the features
required to run the application.

See the feature descriptions themselves for the relation between
Ledger API versions and feature presence. 

.. _com.daml.ledger.api.v2.OffsetCheckpointFeature:

OffsetCheckpointFeature message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.OffsetCheckpointFeature.max_offset_checkpoint_emission_delay:

``max_offset_checkpoint_emission_delay`` :  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#duration>`__

The maximum delay to emmit a new OffsetCheckpoint if it exists 

.. _com.daml.ledger.api.v2.PartyManagementFeature:

PartyManagementFeature message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.PartyManagementFeature.max_parties_page_size:

``max_parties_page_size`` : :ref:`int32 <int32>`

The maximum number of parties the server can return in a single response (page). 

.. _com.daml.ledger.api.v2.UserManagementFeature:

UserManagementFeature message, |version com.daml.ledger.api.v2|
========================================================================================================================================================================================================



.. _com.daml.ledger.api.v2.UserManagementFeature.supported:

``supported`` : :ref:`bool <bool>`

Whether the Ledger API server provides the user management service. 

.. _com.daml.ledger.api.v2.UserManagementFeature.max_rights_per_user:

``max_rights_per_user`` : :ref:`int32 <int32>`

The maximum number of rights that can be assigned to a single user.
Servers MUST support at least 100 rights per user.
A value of 0 means that the server enforces no rights per user limit. 

.. _com.daml.ledger.api.v2.UserManagementFeature.max_users_page_size:

``max_users_page_size`` : :ref:`int32 <int32>`

The maximum number of users the server can return in a single response (page).
Servers MUST support at least a 100 users per page.
A value of 0 means that the server enforces no page size limit. 






----

.. _scalarvaluetypes:

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Scalar Value Types
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



.. _double:

**double**

  

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``double``
      - ``float``
      - ``double``
      - ``double``



.. _float:

**float**

  

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``float``
      - ``float``
      - ``float``
      - ``float``



.. _int32:

**int32**

  Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``int``
      - ``int``
      - ``int32``
      - ``int``



.. _int64:

**int64**

  Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``long``
      - ``int/long``
      - ``int64``
      - ``long``



.. _uint32:

**uint32**

  Uses variable-length encoding.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``int``
      - ``int/long``
      - ``uint32``
      - ``uint``



.. _uint64:

**uint64**

  Uses variable-length encoding.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``long``
      - ``int/long``
      - ``uint64``
      - ``ulong``



.. _sint32:

**sint32**

  Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``int``
      - ``int``
      - ``int32``
      - ``int``



.. _sint64:

**sint64**

  Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``long``
      - ``int/long``
      - ``int64``
      - ``long``



.. _fixed32:

**fixed32**

  Always four bytes. More efficient than uint32 if values are often greater than 2^28.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``int``
      - ``int``
      - ``uint32``
      - ``uint``



.. _fixed64:

**fixed64**

  Always eight bytes. More efficient than uint64 if values are often greater than 2^56.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``long``
      - ``int/long``
      - ``uint64``
      - ``ulong``



.. _sfixed32:

**sfixed32**

  Always four bytes.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``int``
      - ``int``
      - ``int32``
      - ``int``



.. _sfixed64:

**sfixed64**

  Always eight bytes.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``long``
      - ``int/long``
      - ``int64``
      - ``long``



.. _bool:

**bool**

  

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``boolean``
      - ``boolean``
      - ``bool``
      - ``bool``



.. _string:

**string**

  A string must always contain UTF-8 encoded or 7-bit ASCII text.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``String``
      - ``str/unicode``
      - ``string``
      - ``string``



.. _bytes:

**bytes**

  May contain any arbitrary sequence of bytes.

  .. list-table::
    :header-rows: 1
    :width: 100%
    :widths: 25 25 25 25

    * - Java
      - Python
      - C++
      - C#

    * - ``ByteString``
      - ``str``
      - ``string``
      - ``ByteString``




.. |version com.daml.ledger.api.v1| replace:: v1
.. |version com.daml.ledger.api.v1.testing| replace:: v1/testing
.. |version com.daml.ledger.api.v1.admin| replace:: v1/admin
.. |version com.daml.ledger.api.v2| replace:: v2
.. |version com.daml.ledger.api.v2.testing| replace:: v2/testing
.. |version com.daml.ledger.api.v2.admin| replace:: v2/admin
.. |version com.daml.ledger.api.v2.interactive| replace:: v2/interactive
.. |version com.daml.ledger.api.v2.interactive.transaction.v1| replace:: v2/interactive/transaction/v1
