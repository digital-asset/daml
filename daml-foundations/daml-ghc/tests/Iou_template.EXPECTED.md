# Module Iou_template



## Templates

### Template `Iou`


| Field        | Type/Description |
| :----------- | :----------------
| `issuer`     | `Party` |
| `owner`      | `Party` |
| `currency`   | `Text` |
|              | only 3-letter symbols are allowed |
| `amount`     | `Decimal` |
|              | must be positive |
| `regulators` | `[` `Party` `]` |
|              | `regulators` may observe any use of the `Iou` |



  #### Choices

* `Merge`
  merges two "compatible" `Iou`s

  | Field      | Type/Description |
  | :--------- | :----------------
  | `otherCid` | `ContractId` `Iou` |
  |            | Must have same owner, issuer, and currency. The regulators may differ, and are taken from the original `Iou`. |

* `Split`
  splits into two `Iou`s with
  smaller amounts

  | Field         | Type/Description |
  | :------------ | :----------------
  | `splitAmount` | `Decimal` |
  |               | must be between zero and original amount |

* `Transfer`
  changes the owner

  | Field      | Type/Description |
  | :--------- | :----------------
  | `newOwner` | `Party` |




