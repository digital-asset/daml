Forked or extended Daml and third-party libraries:

- [BouncyCastle Blake2b](http://git.bouncycastle.org/repositories/bc-java) - Blake2b implementation in Java. Forked as the original doesn't support the parameters required to build Blake2xb on top of Blake2b.

- [slick](https://github.com/slick/slick) - Modified sql interpolations to set read and write effects

- [wartremover](http://www.wartremover.org/) - Implementations of custom warts

- [magnolify](https://github.com/spotify/magnolify) - Implementations of our own type class derivations

- [scalatest](https://www.scalatest.org/) - Modified assertions to test for compilation errors

- [sphinxcontrib-mermaid](https://github.com/mgaitan/sphinxcontrib-mermaid)
  - In a perfect world, this would be pulled in via nix instead, but this is unfortunately not packaged up as a nix package. But luckily for us, the actual plugin is fairly small, with all the heavy lifting handled by Mermaid itself.

    If `sphinxcontrib-mermaid` ever makes its way to nix, this copy can be deleted, and a line added instead to `shell.nix`.
