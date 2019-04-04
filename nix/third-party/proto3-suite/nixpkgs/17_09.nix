# Given a Git revision hash `<rev>`, you get the new SHA256 by running:
#
# ```bash
# $ nix-prefetch-url "https://github.com/NixOS/nixpkgs/archive/<rev>.tar.gz"
# ```
#
# The SHA256 will be printed as the last line of stdout.

import ./fetch-nixpkgs.nix {
   rev    = "74286ec9e76be7cd00c4247b9acb430c4bd9f1ce";
   sha256 = "0njb3qd2wxj7gil8y61lwh7zacmvr6zklv67w5zmvifi1fvalvdg";
}
