# Ledger API version V2

This package contains the modified protobuf services and messages from V1 and new V2 protobuf definitions.
V1 services which are not explicitly marked with "In V2 Ledger API this service is not available anymore",
will be part of the V2 Ledger API, and served with unchanged semantics. Several V1 messages are reused in
V2 services as well.

Please note: this file will be removed before the Daml SDK 3.0GA release, the change/migration list will be
moved to official daml documentation, the to be revisited list won't be applicable anymore.

## Changes from version V1, and migration recommendations:

### 1. Virtual shared ledger and participant offsets

Ledger API is a gateway for a Canton participant which can be connected to several domains.
To experience this aggregation of changes synchronized over many domains, the domain ID
is introduced to several V1 messages:
- command_completion_service/CompletionStreamResponse - each completion belongs to exactly one domain.
- commands/Commands - a Command will be synchronized via exactly one domain.
- transaction/Transaction, TransactionTree - Each transaction is synchronized via exactly one domain.
- event_query_service/Created,Archived - transitively: each event synchronized via exactly one domain.

Virtual shared ledger also means a local ordering is established: the participant offset
(see more in participant_offset.proto for the semantics). Syntactically this means no change
on the Ledger API: all ledger offset became participant offset.

### 2. Reassignment commands

To support reassignment of a contract (unassigning from one domain, then assigning to another), the
CommandSubmissionService.SubmitReassignment synchronous rpc endpoint was added.

Completions for Reassignment commands are returned by the CommandCompletionService the same way as for
transactions.

### 3. Update service

In V1 the TransactionService provided access to Transaction and TransactionTree streams. In V2 these
streams are also include Reassignment-s as well (which can be an UnassignedEvent or an AssignedEvent).
To accommodate this unification, the Update notation is introduced:
- TransactionService became UpdateService, and the GetTransactions, GetTransactionTrees endpoints
became GetUpdates, GetUpdateTrees respectively.
- What was called in V1 transaction ID is called in v2 update ID (the semantics did not change).
Affected V1 messages:
  - command_service/SubmitAndWaitForUpdateIdResponse
  - completion/Completion
  - transaction/Transaction
  - transaction/TransactionTree
  - update_service/GetTransactionByIdRequest
  
The point-wise lookup queries for transactions are renamed to follow naming convention wih streams:
GetTransaction* to GetTransactionTree* and GetFlatTransaction* to GetTransaction*.

### 4. State service

All ledger state related endpoints moved to StateService:
- ActiveContractsService.GetActiveContracts
- CommandCompletionService.CompletionEnd
- TransactionService.GetLedgerEnd
- TransactionService.GetLatestPrunedOffsets 

GetActiveContracts service is enhanced to provide a snapshot for the virtual shared ledger (see more in 
state_service.proto):
- Contract activeness is defined per domain.
- State stream includes IncompleteAssigned, IncompleteUnassigned to bootstrap applications
reassigning contracts.

A new endpoint is introduced: GetConnectedDomains. This is an initial version to make Canton topology
state accessible over the Ledger API. This endpoint likely will change/improve before 3.0GA (see
To be revisited #6 below).

### 5. EventQueryService

The GetEventsByContractKey endpoint is dropped because of the lack of contract key support.

### 6. TimeService

The GetTime endpoint is made synchronous due to lack of correct support for streaming updates.

### 7. Interface subscription for transaction trees

Support is added in a backwards compatible way: previously template/interface filters were not
allowed for TransactionTree-s. In V2 this is allowed, but only configures how the CreatedEvent
results are rendered, still all parties treated as wildcard filters for filtration.
(see TransactionFilter)

### 8. Simplified stream entries

In V1 all streaming endpoints were returning repeated results, but implementation was always giving
a single entry per stream element. This has been made explicit on the Ledger API - all streaming
results returning a single value. Affected messages:
- command_completion_service/CompletionStreamResponse
- update_service/GetUpdatesResponse, GetUpdateTreesResponse
- state_service/GetActiveContractsResponse

### 9. Removed Ledger Identity

Ledger Identity was a deprecated concept, therefore in V2 the ledger_id is removed from all
request messages:
- testing/time_service/GetTimeRequest, SetTimeRequest
- command_completion_service/CompletionStreamRequest
- commands/Commands
- package_service/ListPackagesRequest, GetPackageRequest, GetPackageStatusRequest
- version_service/GetLedgerApiVersionRequest

Also the LedgerIdentityService is not available anymore.

### 10. Removed Ledger Configuration

LedgerConfigurationService and the admin/ConfigManagementService is not available anymore. The
maximum deduplication period (the default deduplication period for daml command interpretation)
for the participant can be set statically in Canton configuration instead.


| Endpoint migration table | V1 | V2 |
| ---- | ---- | ---- |
| Not supported in V2 | admin/ConfigManagementService.*                |                                             |
| Not supported in V2 | EventQueryService.GetEventsByContractKey       |                                             |
| Not supported in V2 | LedgerConfigurationService.*                   |                                             |
| Not supported in V2 | LedgerIdentityService.*                        |                                             |
| Same in V2 (service in v1 namespace) | admin/IdentityProviderConfigService.*                                                        |
| Same in V2 (service in v1 namespace) | admin/MeteringReportService.*                                                                |
| Same in V2 (service in v1 namespace) | admin/PackageManagementService.*                                                             |
| Same in V2 (service in v1 namespace) | admin/ParticipantPruningService.*                                                            |
| Same in V2 (service in v1 namespace) | admin/PartyManagementService.*                                                               |
| Same in V2 (service in v1 namespace) | admin/UserManagementService.*                                                                |
| Supported in V2 (service in v2 namespace) | testing/TimeService.*                          | testing/TimeService.*                       |
| Supported in V2 (service in v2 namespace) | ActiveContractsService.GetActiveContracts      | StateService.GetActiveContracts             |
| Supported in V2 (service in v2 namespace) | CommandCompletionService.CompletionStream      | CommandCompletionService.CompletionStream   |
| Supported in V2 (service in v2 namespace) | CommandCompletionService.CompletionEnd         | StateService.GetLedgerEnd                   |
| Supported in V2 (service in v2 namespace) | CommandSubmissionService.Submit                | CommandSubmissionService.Submit             |
| Supported in V2 (service in v2 namespace) | CommandService.*                               | CommandService.*                            |
| Supported in V2 (service in v2 namespace) | EventQueryService.GetEventsByContractId        | EventQueryService.GetEventsByContractId     |
| Supported in V2 (service in v2 namespace) | PackageService.*                               | PackageService.*                            |
| Supported in V2 (service in v2 namespace) | TransactionService.GetTransactions             | UpdateService.GetUpdates                    |
| Supported in V2 (service in v2 namespace) | TransactionService.GetTransactionTrees         | UpdateService.GetUpdateTrees                |
| Supported in V2 (service in v2 namespace) | TransactionService.GetTransactionByEventId     | UpdateService.GetTransactionTreeByEventId   |
| Supported in V2 (service in v2 namespace) | TransactionService.GetTransactionById          | UpdateService.GetTransactionTreeById        |
| Supported in V2 (service in v2 namespace) | TransactionService.GetFlatTransactionByEventId | UpdateService.GetTransactionByEventId       |
| Supported in V2 (service in v2 namespace) | TransactionService.GetFlatTransactionById      | UpdateService.GetTransactionById            |
| Supported in V2 (service in v2 namespace) | TransactionService.GetLedgerEnd                | StateService.GetLedgerEnd                   |
| Supported in V2 (service in v2 namespace) | TransactionService.GetLatestPrunedOffsets      | StateService.GetLatestPrunedOffsets         |
| Supported in V2 (service in v2 namespace) | VersionService.*                               | VersionService.*                            |
| New in V2 |                                                | CommandSubmissionService.SubmitReassignment |
| New in V2 |                                                | StateService.GetConnectedDomains            |

## To be revisited (potential changes) until Daml SDK 3.0GA:

1. **Removing application_id**: This is already the user ID, if the authentication token provides it. Making this
mandatory on the authorization layer could mean we can remove the custom application ID in favor of the user ID
all places.
2. **Enhance EventQueryService**: With contract key support for the virtual shared ledger the
EventQueryService.GetEventsByContractKey endpoint can be re-added with adapted semantics. Also the
GetEventsByContractId could be enhanced to also return all reassignment events for a contract.
3. **Simplifying offset usage**: The ParticipantOffset.boundary values could be removed in favor of the optional
absolute (plain string) value, and the streaming endpoints could be simplified not to allow streaming from
an accidental ledger end state.
4. **Change offset data type to int64**: This would give simplifications on client side, implementation code and
in database with a trade-off on future upgrading.
5. **Remove verbose mode**: A simplification on the API and in implementation which might be acceptable for clients.
6. **Introduction of topology events**: Canton topology is introduced in a limited fashion with
StateService.GetConnectedDomains. For complex topologies and multi-hosted parties proper offset based snapshot
endpoint is needed alongside with adding topology events to the update streams.
7. **Introduction of reassigning parties**: With complex topologies the information of which querying users can
conduct assignments for an IncompleteUnassigned entry might be needed for bootstrapping proper reassignement
operations.
8. **Deduplication for reassignments**: Adding this feature might include enriching reassingment commands (and events)
with more deduplication related fields.
9. **Completing the API for reassignments**: Currently the CommandService offers submit-and-wait endpoints for
transactions and Update service offers point-wise lookup endpoints for transactions only. This is about to
extend these features naturally for reassignments.
10. **Improving party and package management admin endpoints**: add streaming capabilities, and streamline capabilities
considering improved topology information and Canton admin API features.
