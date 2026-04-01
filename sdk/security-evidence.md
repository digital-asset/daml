# Security tests, by category

## Integrity:
- Smart Contract Upgrade: Can catch different errors thrown by different choice version within Update, using AnyException: [Exceptions.daml](daml-script/test/daml/upgrades/stable/Exceptions.daml#L263)
- Smart Contract Upgrade: Can catch different errors thrown by different choice version, where one is new to V2 within Update, using AnyException: [Exceptions.daml](daml-script/test/daml/upgrades/stable/Exceptions.daml#L267)
- Smart Contract Upgrade: Can catch same errors thrown by different choice versions within Update: [Exceptions.daml](daml-script/test/daml/upgrades/stable/Exceptions.daml#L259)
- Smart Contract Upgrade: Cannot catch DowngradeDropDefinedField upgrade exceptions within Update: [Exceptions.daml](daml-script/test/daml/upgrades/stable/Exceptions.daml#L227)
- Smart Contract Upgrade: Cannot catch ValidationFailed upgrade exceptions within Update: [Exceptions.daml](daml-script/test/daml/upgrades/stable/Exceptions.daml#L216)


