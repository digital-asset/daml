Forked or extended Daml and third-party libraries:

- [pekko](https://github.com/akka/akka/) - Forked some files for adding more debug logging, then migrated from Akka to Pekko.
  TODO(#9883) remove when no longer needed

- [daml](https://github.com/digital-asset/daml)
  - Explanation and details described in [CONTRIBUTING.md](../CONTRIBUTING.md) under "Managing Daml repo upstream "mismatches""

- [BouncyCastle Blake2b](http://git.bouncycastle.org/repositories/bc-java) - Blake2b implementation in Java. Forked as the original doesn't support the parameters required to build Blake2xb on top of Blake2b.

- [slick](https://github.com/slick/slick) - Modified sql interpolations to set read and write effects

- [wartremover](http://www.wartremover.org/) - Implementations of custom warts
