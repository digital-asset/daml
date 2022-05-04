
# Upcoin example

This example shows how Daml interfaces can be used to structure a design which is more amenable to upgrading.

### Build/Test
```
  daml build
  daml test
```

### Motivation: initial inflexible system

We begin with a rather [`InflexibleDesign`](daml/InflexibleDesign.daml) which demonstrates the essence of the small payment system we want to build.

It contains templates for: `UnitCoin`, `Invoice` and `Receipt`. The core functionality of _coin transfer_ is captured by the `Transfer` choice of `UnitCoin`. The `Invoice` template builds on this by allowing an invoicer to raise an invoice for a given integer `price` which can then be settled by the `buyer` invoking the `Pay` choice with a list of `UnitCoin`s.

The design does not make use of interfaces. The designs is inflexible because the _invoicing_ part of the system is tied to a fixed kind of coin, the `UnitCoin`.


### Using interfaces to allow an upgradable system

Here we proceed to develop a more [`UpgradableDesign`](daml/UpgradableDesign.daml).

This design defines an interface `AnyCoin` which captures the idea that anything which supports a `Transfer` choice can be regarded as a coin. The `Invoice` template builds on this by accepting `AnyCoin` as payment, without knowing about any concrete coin implementations. But immediately we can see a problem: The invoice issuer will surely want to limit what coins can be used as payment.

- Some coins might be badly implemented, so that the `Transfer` choice does not work as expected.
- Some coins might carry unacceptable obligations.
- Some coins might not have any value (to the issuer).

And even for coins which are well behaved, we would like to support the idea of different coins having different values.

To address these concerns, we allow the invoice issuer can choose what coins are deemed _acceptable_ and how each kind of coin is valued, by having the `Invoice` contract contain an `AcceptedCoins` _whitelist_, which maps different kinds of coin to the value chosen by the invoicer.

This design allow new kinds of coins to be introduced into the system even after the original system is deployed. The set of acceptable coins is chosen only when an invoice is raised, on a per-invoice basis.


### Some different coins

We define some different coins in [`SomeCoins.daml`](daml/SomeCoins.daml), including the original `UnitCoin`, a new and improved `DoubleCoin`, and a badly behaved coin `Dodgecoin`.

Finally in [`Test.daml`](daml/Test.daml) we see how the set of `AcceptableCoins` is selected at invoice creation time.
