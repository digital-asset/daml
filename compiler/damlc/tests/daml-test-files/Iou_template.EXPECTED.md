# <a name="module-ioutemplate-98694"></a>Module Iou\_template

## Templates

### <a name="type-ioutemplate-iou-55222"></a>template Iou

| Field      | Type/Description |
| :--------- | :----------------
| issuer     | Party |
| owner      | Party |
| currency   | Text |
|            | only 3-letter symbols are allowed |
| amount     | Decimal |
|            | must be positive |
| regulators | \[Party\] |
|            | `regulators` may observe any use of the `Iou` |


Choices:

* Merge
  
  merges two "compatible" `Iou`s
  
  | Field    | Type/Description |
  | :------- | :----------------
  | otherCid | ContractId [Iou](#type-ioutemplate-iou-55222) |
  |          | Must have same owner, issuer, and currency. The regulators may differ, and are taken from the original `Iou`. |
  
* Split
  
  splits into two `Iou`s with
  smaller amounts
  
  | Field       | Type/Description |
  | :---------- | :----------------
  | splitAmount | Decimal |
  |             | must be between zero and original amount |
  
* Transfer
  
  changes the owner
  
  | Field   | Type/Description |
  | :------ | :----------------
  | owner\_ | Party |
  

## Functions

<a name="function-ioutemplate-main-13221"></a>**main**  
&nbsp; : Scenario ()

A single test scenario covering all functionality that `Iou` implements.
This description contains [a link](http://example.com), some bogus <inline html>,
and words_ with_ underscore, to test damldoc capabilities.


