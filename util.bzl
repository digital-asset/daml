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
def hazel_hackage(name, version, sha, **kwargs):
    args = {"version": version, "sha256": sha}
    args.update(kwargs)
    return [(name, args)]

# Things we override from GitHub
def hazel_github_external(project, repoName, commit, sha, directory = "", name = None):
    return [(
        name or repoName,
        {
            "url": "https://github.com/" + project + "/" + repoName + "/archive/" + commit + ".zip",
            "sha256": sha,
            "stripPrefix": repoName + "-" + commit + directory,
        },
    )]

# Things we get from the digital-asset GitHub
def hazel_github(repoName, commit, sha, directory = "", name = None):
    return hazel_github_external("digital-asset", repoName, commit, sha, directory = directory, name = name)
