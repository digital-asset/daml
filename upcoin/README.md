
# Upcoin example

This example shows how Daml interfaces can be used to structure a design which is more amenable to upgrading.

### Build/Test
```
  daml build
  daml test
```

### Motivation: initial inflexible system

We begin with a rather [InflexibleDesign](daml/InflexibleDesign.daml) which demonstrates the essence of the small payment system we want to build.

This design does not make use of interfaces.

It contains templates: `UnitCoin`, `Invoice` and `Receipt`, with the core user functionality being captured by the: `Transfer` choice of `UnitCoin`.

This designs is inflexible because the _invoicing_ part of the system is tied the one fixed kind of coin.


### Using interfaces to allow an upgradable system

Here we proceed to develop a more [UpgradableDesign](daml/UpgradableDesign.daml).

This makes use of interfaces to allow new kinds of coins to be introduced into the system even after the original system is deployed.
