# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Helpers for setting up Hazel rules

# Our ghc-lib libraries
def hazel_ghclibs(version, shaParser, shaLibrary):
    return [
        # Read [Working on ghc-lib] for ghc-lib update instructions at
        # https://github.com/DACH-NY/daml/blob/master/ghc-lib/working-on-ghc-lib.md
        (
            "ghc-lib-parser",
            {
                "url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-parser-" + version + ".tar.gz",
                "stripPrefix": "ghc-lib-parser-" + version,
                "sha256": shaParser,
            },
        ),
        (
            "ghc-lib",
            {
                "url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-" + version + ".tar.gz",
                "stripPrefix": "ghc-lib-" + version,
                "sha256": shaLibrary,
            },
        ),
    ]

# Things we override from Hackage
def hazel_hackage(name, version, sha):
    return [(name, {"version": version, "sha256": sha})]

# Things we override from GitHub
def hazel_github(project, name, commit, sha):
    return [(name, {"url": "https://github.com/" + project + "/" + name + "/archive/" + commit + ".zip", "sha256": sha, "stripPrefix": name + "-" + commit})]
