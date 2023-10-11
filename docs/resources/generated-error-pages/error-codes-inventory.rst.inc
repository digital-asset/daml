1. ParticipantErrorGroup
===================================================================================================================




1.1. ParticipantErrorGroup / CommonErrors
===================================================================================================================

Common errors raised in Daml services and components.



.. _error_code_REQUEST_TIME_OUT:

REQUEST_TIME_OUT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a request processing status is not known and a time-out is reached.

    **Category**: DeadlineExceededRequestStateUnknown

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status DEADLINE_EXCEEDED including a detailed error message.

    **Resolution**: Retry for transient problems. If non-transient contact the operator as the time-out limit might be too short.




.. _error_code_SERVER_IS_SHUTTING_DOWN:

SERVER_IS_SHUTTING_DOWN
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the participant server is shutting down.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message.

    **Resolution**: Contact the participant operator.




.. _error_code_SERVICE_INTERNAL_ERROR:

SERVICE_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if one of the services encountered an unexpected exception.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact support.




.. _error_code_SERVICE_NOT_RUNNING:

SERVICE_NOT_RUNNING
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the requested service has already been closed.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message.

    **Resolution**: Retry re-submitting the request. If the error persists, contact the participant operator.




.. _error_code_UNSUPPORTED_OPERATION:

UNSUPPORTED_OPERATION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request.

    **Category**: InternalUnsupportedOperation

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status UNIMPLEMENTED without any details for security reasons.

    **Resolution**: This error is caused by a participant node misconfiguration or by an implementation bug. Resolution requires participant operator intervention.



1.2. ParticipantErrorGroup / IndexErrors
===================================================================================================================

Errors raised by the Participant Index persistence layer.


1.2.1. ParticipantErrorGroup / IndexErrors / DatabaseErrors
===================================================================================================================





.. _error_code_INDEX_DB_SQL_NON_TRANSIENT_ERROR:

INDEX_DB_SQL_NON_TRANSIENT_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a non-transient error arises when executing a query against the index database.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact the participant operator.




.. _error_code_INDEX_DB_SQL_TRANSIENT_ERROR:

INDEX_DB_SQL_TRANSIENT_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a transient error arises when executing a query against the index database.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message.

    **Resolution**: Re-submit the request.



1.3. ParticipantErrorGroup / LedgerApiErrors
===================================================================================================================

Errors raised by or forwarded by the Ledger API.



.. _error_code_LEDGER_API_INTERNAL_ERROR:

LEDGER_API_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if there was an unexpected error in the Ledger API.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact support.



1.3.1. ParticipantErrorGroup / LedgerApiErrors / AdminServices
===================================================================================================================

Errors raised by Ledger API admin services.



.. _error_code_CONFIGURATION_ENTRY_REJECTED:

CONFIGURATION_ENTRY_REJECTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a new configuration is rejected.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Fetch newest configuration and/or retry.



1.3.1.1. ParticipantErrorGroup / LedgerApiErrors / AdminServices / IdentityProviderConfigServiceErrorGroup
===================================================================================================================





.. _error_code_IDP_CONFIG_ALREADY_EXISTS:

IDP_CONFIG_ALREADY_EXISTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There already exists an identity provider configuration with the same identity provider id.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use an identity provider that already exists.




.. _error_code_IDP_CONFIG_ISSUER_ALREADY_EXISTS:

IDP_CONFIG_ISSUER_ALREADY_EXISTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There already exists an identity provider configuration with the same issuer.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and the identity provider id is spelled correctly, or use an identity provider that already exists.




.. _error_code_IDP_CONFIG_NOT_FOUND:

IDP_CONFIG_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The identity provider config referred to by the request was not found.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and the identity provider config is spelled correctly, or create the configuration.




.. _error_code_INVALID_IDENTITY_PROVIDER_UPDATE_REQUEST:

INVALID_IDENTITY_PROVIDER_UPDATE_REQUEST
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There was an attempt to update an identity provider config using an invalid update request.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error details for specific information on what made the request invalid. Retry with an adjusted update request.



1.3.1.2. ParticipantErrorGroup / LedgerApiErrors / AdminServices / PartyManagementServiceErrorGroup
===================================================================================================================





.. _error_code_CONCURRENT_PARTY_DETAILS_UPDATE_DETECTED:

CONCURRENT_PARTY_DETAILS_UPDATE_DETECTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Concurrent updates to a party can be controlled by supplying an update request with a resource version (this is optional). A party's resource version can be obtained by reading the party on the Ledger API. There was attempt to update a party using a stale resource version, indicating that a different process had updated the party earlier.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ABORTED including a detailed error message.

    **Resolution**: Read this party again to obtain its most recent state and in particular its most recent resource version. Use the obtained information to build and send a new update request.




.. _error_code_INVALID_PARTY_DETAILS_UPDATE_REQUEST:

INVALID_PARTY_DETAILS_UPDATE_REQUEST
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There was an attempt to update a party using an invalid update request.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error details for specific information on what made the request invalid. Retry with an adjusted update request.




.. _error_code_PARTY_NOT_FOUND:

PARTY_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The party referred to by the request was not found.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and that the party is spelled correctly.



1.3.1.3. ParticipantErrorGroup / LedgerApiErrors / AdminServices / UserManagementServiceErrorGroup
===================================================================================================================





.. _error_code_CONCURRENT_USER_UPDATE_DETECTED:

CONCURRENT_USER_UPDATE_DETECTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Concurrent updates to a user can be controlled by supplying an update request with a resource version (this is optional). A user's resource version can be obtained by reading the user on the Ledger API. There was attempt to update a user using a stale resource version, indicating that a different process had updated the user earlier.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ABORTED including a detailed error message.

    **Resolution**: Read this user again to obtain its most recent state and in particular its most recent resource version. Use the obtained information to build and send a new update request.




.. _error_code_INVALID_USER_UPDATE_REQUEST:

INVALID_USER_UPDATE_REQUEST
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There was an attempt to update a user using an invalid update request.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error details for specific information on what made the request invalid. Retry with an adjusted update request.




.. _error_code_TOO_MANY_USER_RIGHTS:

TOO_MANY_USER_RIGHTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A user can have only a limited number of user rights. There was an attempt to create a user with too many rights or grant too many rights to a user.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Retry with a smaller number of rights or delete some of the already existing rights of this user. Contact the participant operator if the limit is too low.




.. _error_code_USER_ALREADY_EXISTS:

USER_ALREADY_EXISTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There already exists a user with the same user-id.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and the user-id is spelled correctly, or use the user that already exists.




.. _error_code_USER_NOT_FOUND:

USER_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The user referred to by the request was not found.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Check that you are connecting to the right participant node and the user-id is spelled correctly, if yes, create the user.



1.3.2. ParticipantErrorGroup / LedgerApiErrors / AuthorizationChecks
===================================================================================================================

Authentication and authorization errors.



.. _error_code_INTERNAL_AUTHORIZATION_ERROR:

INTERNAL_AUTHORIZATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An internal system authorization error occurred.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact the participant operator.




.. _error_code_PERMISSION_DENIED:

PERMISSION_DENIED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given if the supplied authorization token is not sufficient for the intended command. The exact reason is logged on the participant, but not given to the user for security reasons.

    **Category**: InsufficientPermission

    **Conveyance**: This error is logged with log-level WARN on the server side. It is exposed on the API with grpc-status PERMISSION_DENIED without any details for security reasons.

    **Resolution**: Inspect your command and your token or ask your participant operator for an explanation why this command failed.




.. _error_code_STALE_STREAM_AUTHORIZATION:

STALE_STREAM_AUTHORIZATION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The stream was aborted because the authenticated user's rights changed, and the user might thus no longer be authorized to this stream. 

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ABORTED including a detailed error message.

    **Resolution**: The application should automatically retry fetching the stream. It will either succeed, or fail with an explicit denial of authentication or permission.




.. _error_code_UNAUTHENTICATED:

UNAUTHENTICATED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given if the submitted command does not contain a JWT token on a participant enforcing JWT authentication.

    **Category**: AuthInterceptorInvalidAuthenticationCredentials

    **Conveyance**: This error is logged with log-level WARN on the server side. It is exposed on the API with grpc-status UNAUTHENTICATED without any details for security reasons.

    **Resolution**: Ask your participant operator to provide you with an appropriate JWT token.



1.3.3. ParticipantErrorGroup / LedgerApiErrors / CommandExecution
===================================================================================================================

Errors raised during the command execution phase of the command submission evaluation.


1.3.3.1. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Interpreter
===================================================================================================================

Errors raised during the command interpretation phase of the command submission evaluation.



.. _error_code_CONTRACT_DOES_NOT_IMPLEMENT_INTERFACE:

CONTRACT_DOES_NOT_IMPLEMENT_INTERFACE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you try to coerce/use a contract via an interface that it does not implement.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Ensure the contract you are calling does implement the interface you are using to do so. Avoid writing LF/low-level interface implementation classes manually.




.. _error_code_CONTRACT_DOES_NOT_IMPLEMENT_REQUIRING_INTERFACE:

CONTRACT_DOES_NOT_IMPLEMENT_REQUIRING_INTERFACE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you try to create/use a contract that does not implement the requiring interfaces of some other interface that it does implement.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Ensure you implement all required interfaces correctly, and avoid writing LF/low-level interface implementation classes manually.




.. _error_code_CONTRACT_ID_COMPARABILITY:

CONTRACT_ID_COMPARABILITY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you attempt to compare a global and local contract ID of the same discriminator.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Avoid constructing contract IDs manually.




.. _error_code_CONTRACT_ID_IN_CONTRACT_KEY:

CONTRACT_ID_IN_CONTRACT_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when a contract key contains a contract ID, which is illegal for hashing reasons.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Ensure your contracts key field cannot contain a contract ID.




.. _error_code_CONTRACT_NOT_ACTIVE:

CONTRACT_NOT_ACTIVE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if an exercise or fetch happens on a transaction-locally consumed contract.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: This error indicates an application error.




.. _error_code_CREATE_EMPTY_CONTRACT_KEY_MAINTAINERS:

CREATE_EMPTY_CONTRACT_KEY_MAINTAINERS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you try to create a contract that has a key, but with empty maintainers.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Check the definition of the contract key's maintainers, and ensure this list won't be empty given your creation arguments.




.. _error_code_DAML_AUTHORIZATION_ERROR:

DAML_AUTHORIZATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a Daml transaction fails due to an authorization error. An authorization means that the Daml transaction computed a different set of required submitters than you have provided during the submission as `actAs` parties.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: This error type occurs if there is an application error.




.. _error_code_DISCLOSED_CONTRACT_KEY_HASHING_ERROR:

DISCLOSED_CONTRACT_KEY_HASHING_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a user attempts to provide a key hash for a disclosed contract which we have already cached to be different.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Ensure the contract ID and contract payload you have provided in your disclosed contract is correct.




.. _error_code_FETCH_EMPTY_CONTRACT_KEY_MAINTAINERS:

FETCH_EMPTY_CONTRACT_KEY_MAINTAINERS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you try to fetch a contract by key, but that key would have empty maintainers.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Check the definition of the contract key's maintainers, and ensure this list won't be empty given the contract key you are fetching.




.. _error_code_NON_COMPARABLE_VALUES:

NON_COMPARABLE_VALUES
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you attempt to compare two values of different types using the built-in comparison types.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Avoid using the low level comparison build, and instead use the Eq class.




.. _error_code_TEMPLATE_PRECONDITION_VIOLATED:

TEMPLATE_PRECONDITION_VIOLATED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when a contract's pre-condition (the ensure clause) is violated on contract creation.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Ensure the contract argument you are passing into your create doesn't violate the conditions of the contract.




.. _error_code_UNHANDLED_EXCEPTION:

UNHANDLED_EXCEPTION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when a user throws an error and does not catch it with try-catch.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Either your error handling in a choice body is insufficient, or you are using a contract incorrectly.




.. _error_code_VALUE_NESTING:

VALUE_NESTING
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you nest values too deeply.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Restructure your code and reduce value nesting.




.. _error_code_WRONGLY_TYPED_CONTRACT:

WRONGLY_TYPED_CONTRACT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when you try to fetch/use a contract in some way with a contract ID that doesn't match the template type on the ledger.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Ensure the contract IDs you are using are of the type we expect on the ledger. Avoid unsafely coercing contract IDs.



1.3.3.1.1. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Interpreter / LookupErrors
===================================================================================================================

Errors raised in lookups during the command interpretation phase.



.. _error_code_CONTRACT_KEY_NOT_FOUND:

CONTRACT_KEY_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the Daml engine interpreter cannot resolve a contract key to an active contract. This can be caused by either the contract key not being known to the participant, or not being known to the submitting parties or the contract representing an already archived key.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: This error type occurs if there is contention on a contract.



1.3.3.2. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Preprocessing
===================================================================================================================

Errors raised during command conversion to the internal data representation.



.. _error_code_COMMAND_PREPROCESSING_FAILED:

COMMAND_PREPROCESSING_FAILED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a command fails during interpreter pre-processing.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect error details and correct your application.



1.3.4. ParticipantErrorGroup / LedgerApiErrors / ConsistencyErrors
===================================================================================================================

Potential consistency errors raised due to race conditions during command submission or returned as submission rejections by the backing ledger.



.. _error_code_CONTRACT_NOT_FOUND:

CONTRACT_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the Daml engine can not find a referenced contract. This can be caused by either the contract not being known to the participant, or not being known to the submitting parties or already being archived.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: This error type occurs if there is contention on a contract.




.. _error_code_DUPLICATE_COMMAND:

DUPLICATE_COMMAND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A command with the given command id has already been successfully processed.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message.

    **Resolution**: The correct resolution depends on the use case. If the error received pertains to a submission retried due to a timeout, do nothing, as the previous command has already been accepted. If the intent is to submit a new command, re-submit using a distinct command id. 




.. _error_code_DUPLICATE_CONTRACT_KEY:

DUPLICATE_CONTRACT_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error signals that within the transaction we got to a point where two contracts with the same key were active.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message.

    **Resolution**: This error indicates an application error.




.. _error_code_INCONSISTENT_CONTRACT_KEY:

INCONSISTENT_CONTRACT_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An input contract key was re-assigned to a different contract by a concurrent transaction submission.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Retry the transaction submission.




.. _error_code_SUBMISSION_ALREADY_IN_FLIGHT:

SUBMISSION_ALREADY_IN_FLIGHT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Another command submission with the same change ID (application ID, command ID, actAs) is already being processed.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ABORTED including a detailed error message.

    **Resolution**: Listen to the command completion stream until a completion for the in-flight command submission is published. Alternatively, resubmit the command. If the in-flight submission has finished successfully by then, this will return more detailed information about the earlier one. If the in-flight submission has failed by then, the resubmission will attempt to record the new transaction on the ledger. 



1.3.5. ParticipantErrorGroup / LedgerApiErrors / PackageServiceError
===================================================================================================================

Errors raised by the Package Management Service on package uploads.



.. _error_code_DAR_NOT_SELF_CONSISTENT:

DAR_NOT_SELF_CONSISTENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the uploaded Dar is broken because it is missing internal dependencies.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Contact the supplier of the Dar.




.. _error_code_DAR_VALIDATION_ERROR:

DAR_VALIDATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the validation of the uploaded dar failed.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error message and contact support.




.. _error_code_PACKAGE_SERVICE_INTERNAL_ERROR:

PACKAGE_SERVICE_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates an internal issue within the package service.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Inspect the error message and contact support.



1.3.5.1. ParticipantErrorGroup / LedgerApiErrors / PackageServiceError / Reading
===================================================================================================================

Package parsing errors raised during package upload.



.. _error_code_INVALID_DAR:

INVALID_DAR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied dar file was invalid.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error message for details and contact support.




.. _error_code_INVALID_ZIP_ENTRY:

INVALID_ZIP_ENTRY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied zipped dar file was invalid.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the error message for details and contact support.




.. _error_code_ZIP_BOMB:

ZIP_BOMB
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied zipped dar is regarded as zip-bomb.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the dar and contact support.



1.3.6. ParticipantErrorGroup / LedgerApiErrors / RequestValidation
===================================================================================================================

Validation errors raised when evaluating requests in the Ledger API.



.. _error_code_INVALID_ARGUMENT:

INVALID_ARGUMENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command contains an invalid argument.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_INVALID_DEDUPLICATION_PERIOD:

INVALID_DEDUPLICATION_PERIOD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command specifies an invalid deduplication period.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Inspect the error message, adjust the value of the deduplication period or ask the participant operator to increase the maximum deduplication period.




.. _error_code_INVALID_FIELD:

INVALID_FIELD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command contains a field value that cannot be understood.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_LEDGER_ID_MISMATCH:

LEDGER_ID_MISMATCH
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Every ledger API command contains a ledger-id which is verified against the running ledger.           This error indicates that the provided ledger-id does not match the expected one.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Ensure that your application is correctly configured to use the correct ledger.




.. _error_code_MISSING_FIELD:

MISSING_FIELD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a mandatory field is not set in a submitted ledger API command.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_NON_HEXADECIMAL_OFFSET:

NON_HEXADECIMAL_OFFSET
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The supplied offset could not be converted to a binary offset.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message.

    **Resolution**: Ensure the offset is specified as a hexadecimal string.




.. _error_code_OFFSET_AFTER_LEDGER_END:

OFFSET_AFTER_LEDGER_END
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request uses an offset beyond the current ledger end.

    **Category**: InvalidGivenCurrentSystemStateSeekAfterEnd

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status OUT_OF_RANGE including a detailed error message.

    **Resolution**: Use an offset that is before the ledger end.




.. _error_code_OFFSET_OUT_OF_RANGE:

OFFSET_OUT_OF_RANGE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request uses an offset invalid in the requests' context.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Inspect the error message and use a valid offset.




.. _error_code_PARTICIPANT_PRUNED_DATA_ACCESSED:

PARTICIPANT_PRUNED_DATA_ACCESSED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request tries to access pruned data.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message.

    **Resolution**: Use an offset that is after the pruning offset.



1.3.6.1. ParticipantErrorGroup / LedgerApiErrors / RequestValidation / NotFound
===================================================================================================================





.. _error_code_LEDGER_CONFIGURATION_NOT_FOUND:

LEDGER_CONFIGURATION_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The ledger configuration could not be retrieved. This could happen due to incomplete initialization of the participant or due to an internal system error.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Contact the participant operator.




.. _error_code_PACKAGE_NOT_FOUND:

PACKAGE_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request tries to access a package which does not exist on the ledger.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Use a package id pertaining to a package existing on the ledger.




.. _error_code_TEMPLATES_OR_INTERFACES_NOT_FOUND:

TEMPLATES_OR_INTERFACES_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The queried template or interface ids do not exist.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Use valid template or interface ids in your query or ask the participant operator to upload the package containing the necessary interfaces/templates.




.. _error_code_TRANSACTION_NOT_FOUND:

TRANSACTION_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The transaction does not exist or the requesting set of parties are not authorized to fetch it.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Check the transaction id and verify that the requested transaction is visible to the requesting parties.



1.3.7. ParticipantErrorGroup / LedgerApiErrors / WriteServiceRejections
===================================================================================================================

Generic submission rejection errors returned by the backing ledger's write service.



.. _error_code_DISPUTED:

DISPUTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Deprecation**: Corresponds to transaction submission rejections that are not produced anymore. Since: 1.18.0
    
    **Explanation**: An invalid transaction submission was not detected by the participant.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact support.




.. _error_code_OUT_OF_QUOTA:

OUT_OF_QUOTA
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Deprecation**: Corresponds to transaction submission rejections that are not produced anymore. Since: 1.18.0
    
    **Explanation**: The Participant node did not have sufficient resource quota to submit the transaction.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status ABORTED including a detailed error message.

    **Resolution**: Inspect the error message and retry after after correcting the underlying issue.




.. _error_code_PARTY_NOT_KNOWN_ON_LEDGER:

PARTY_NOT_KNOWN_ON_LEDGER
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: One or more informee parties have not been allocated.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status NOT_FOUND including a detailed error message.

    **Resolution**: Check that all the informee party identifiers are correct, allocate all the informee parties, request their allocation or wait for them to be allocated before retrying the transaction submission.

