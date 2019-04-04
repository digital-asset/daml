# Command submission error handling

Errors upon command submission are communicated either in a response to the submitting rpc call, or through the command completion stream.
It is not prescribed where each error should appear, all of them (except for UNKNOWN class errors) might be served through either channel.

The error data structure contains an integer code, which can be used to identify the type of error.
The tables below contain a description of each error code that may appear on the API.

The tables are incomplete for now and their purpose is to showcase the classification.

## Errors

The list of possible errors is incomplete for now

### 0000: UNKNOWN

Errors of this kind may be present in the submission responses only, since they require tracking the command completion stream to determine the command's eventual state.

|Code|Name| Meaning|Parameters|
|---:|----|--------|----------|
|0000|Unknown|The command's status cannot be determined.|message|


### 1000: INVALID

Errors of this kind signal a fundamental problem in the submitted command. Resubmitting after getting an error of this category is futile as it will fail again with the same error.

#### 1100: DAML Error

|Code|Name| Meaning|Parameters|
|---:|----|--------|----------|
|1100|Template not found||template_name <> package_id|
|1101|Template ambiguous|Package ID was not provided for unique identification, and there are multiple templates with the same name.|template_name|
|1102|Contract match failed||contract_id|
|1103|Argument type mismatch|The type of a submitted argument does not match the expected one.|submitted <> expected|
|1104|Invalid choice on contract|There is no such choice on the contract the client wants to exercise upon.|contract_id <> choice <> template_name|

### 2000: FAILED

Commands resulting in this class of errors may be safely resubmitted with updated ledger effective times and record times.

|Code|Name| Meaning|Parameters|
|---:|----|--------|----------|
|2000|Unavailable|Command submission failed due to the malfunction of a critical component.||
|2001|Duplicate command|Ledger Server identified the submitted command as a duplicate of a previous one.|transaction_id|

