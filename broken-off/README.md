# Per-team projects

This is a temporary storage space for build targets that have been broken off
from the main Bazel build tree.

This is part of a larger project to break down the build along team boundaries,
such that each piece of code has a distinct, well-identified owner. Individual
teams are free to choose how they manage their own projects moving forward: a
single Bazel build in a single team-owned repo, many different builds using
different build systems in a single team repo, a separate GitHub repo per
project, or anything in-between.

In this space, we'll have a folder per team, and each team can organize things
as they see fit within that folder, and choose when, if ever, to move out
specific projects (or the whole team folder at once) to a separate repo.
