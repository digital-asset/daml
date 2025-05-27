NOTE: These files have been taken wholesale from
https://github.com/mgaitan/sphinxcontrib-mermaid

In a perfect world, this would be pulled in via nix instead, but this is
unfortunately not packaged up as a nix package. But luckily for us, the
actual plugin is fairly small, with all the heavy lifting handled by
Mermaid itself.

If `sphinxcontrib-mermaid` ever makes its way to nix, this copy can be
deleted, and a line added instead to `shell.nix`.
