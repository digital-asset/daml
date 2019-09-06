# HTTP JSON Service

## How to start

### Start sandbox from a DAML Assistant project directory
```
daml-head sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar
```

### Start HTTP service from a DAML Assistant project directory
```
$ daml-head json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 --max-inbound-message-size 4194304 --application-id HTTP-JSON-API-Gateway
```
Where:
 - localhost 6865 -- sandbox host and port
 - 7575 -- HTTP service port
 - 4194304 -- max inbound message size in bytes (the max size of the message received from the ledger). To set the same limit on the sandbox side, use ` --maxInboundMessageSize` command line parameter.

## Example session

```
$ cd <daml-root>/
$ daml-sdk-head

$ cd $HOME
$ daml-head new iou-quickstart-java quickstart-java
$ cd iou-quickstart-java/
$ daml-head build
$ daml-head sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar
$ daml-head json-api --ledger-host localhost --ledger-port 6865 --http-port 7575
```

### Choosing a party

You specify your party and other settings with JWT.  In testing
environments, you can use https://jwt.io to generate your token.

The default "header" is fine.  Under "Payload", fill in:

```
{
  "ledgerId": "MyLedger",
  "applicationId": "foobar",
  "party": "Alice"
}
```

Keep in mind:
- the value of `ledgerId` payload field has to match `--ledgerid` passed to the sandbox.
- you can replace `Alice` with whatever party you want to use.

Under "Verify Signature", put `secret` as the secret (_not_ base64
encoded); that is the hardcoded secret for testing.

Then the "Encoded" box should have your token; set HTTP header
`Authorization: Bearer copy-paste-token-here`.

Here are two tokens you can use for testing:
- `{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Alice"}`
  `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0`

- `{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Bob"}`
  `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQm9iIn0.2LE3fAvUzLx495JWpuSzHye9YaH3Ddt4d2Pj0L1jSjA`
  
For production use, we have a tool in development for generating proper
RSA-encrypted tokens locally, which will arrive when the service also
supports such tokens.

### GET http://localhost:7575/contracts/search

### POST http://localhost:7575/contracts/search
application/json body:
```
{"templateIds": [{"moduleName": "Iou", "entityName": "Iou"}]}
```

### POST http://localhost:7575/command/create
application/json body:
 ```
 {
   "templateId": {
     "moduleName": "Iou",
     "entityName": "Iou"
   },
   "argument": {
     "observers": [],
     "issuer": "Alice",
     "amount": "999.99",
     "currency": "USD",
     "owner": "Alice"
   }
 }
```
output:
```
 {
     "status": 200,
     "result": {
         "agreementText": "",
         "contractId": "#20:0",
         "templateId": {
             "packageId": "bede798df37ce01fc402d266ae89d5bc4c61d70968b6a4f0baf69b24140579aa",
             "moduleName": "Iou",
             "entityName": "Iou"
         },
         "witnessParties": [
             "Alice"
         ],
         "argument": {
             "observers": [],
             "issuer": "Alice",
             "amount": "999.99",
             "currency": "USD",
             "owner": "Alice"
         }
     }
 } 
```
 
### POST http://localhost:44279/command/exercise
`"contractId": "#20:0"` is the value from the create output
application/json body:
```
{
    "templateId": {
        "moduleName": "Iou",
        "entityName": "Iou"
    },
    "contractId": "#20:0",
    "choice": "Iou_Transfer",
    "argument": {
        "newOwner": "Alice"
    }
}
```
output:
```
{
    "status": 200,
    "result": [
        {
            "agreementText": "",
            "contractId": "#160:1",
            "templateId": {
                "packageId": "bede798df37ce01fc402d266ae89d5bc4c61d70968b6a4f0baf69b24140579aa",
                "moduleName": "Iou",
                "entityName": "IouTransfer"
            },
            "witnessParties": [
                "Alice"
            ],
            "argument": {
                "iou": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "newOwner": "Alice"
            }
        }
    ]
}
```
