package(default_visibility = ["//visibility:public"])

genrule(
    name = "quickstart",
    srcs = glob(
        ["docs/code-samples/getting-started/**/*"],
        exclude = [
            "docs/code-samples/getting-started/daml.yaml",
            "docs/code-samples/getting-started/NO_AUTO_COPYRIGHT",
        ],
    ),
    outs = ["daml-finance-quickstart.tar.gz"],
    cmd = """
        tar czhf $(OUTS) \\
            --transform 's|^.*docs/code-samples/getting-started/||' \\
            --owner=1000 \\
            --group=1000 \\
            --mtime='2000-01-01 00:00Z' \\
            --no-acls \\
            --no-xattrs \\
            --no-selinux \\
            --sort=name \\
            $(SRCS)
    """,
)

genrule(
    name = "lifecycling",
    srcs = glob(
        ["docs/code-samples/lifecycling/**/*"],
        exclude = [
            "docs/code-samples/lifecycling/daml.yaml",
            "docs/code-samples/lifecycling/NO_AUTO_COPYRIGHT",
        ],
    ),
    outs = ["daml-finance-lifecycling.tar.gz"],
    cmd = """
        tar czhf $(OUTS) \\
            --transform 's|^.*docs/code-samples/lifecycling/||' \\
            --owner=1000 \\
            --group=1000 \\
            --mtime='2000-01-01 00:00Z' \\
            --no-acls \\
            --no-xattrs \\
            --no-selinux \\
            --sort=name \\
            $(SRCS)
    """,
)

genrule(
    name = "settlement",
    srcs = glob(
        ["docs/code-samples/settlement/**/*"],
        exclude = [
            "docs/code-samples/settlement/daml.yaml",
            "docs/code-samples/settlement/NO_AUTO_COPYRIGHT",
        ],
    ),
    outs = ["daml-finance-settlement.tar.gz"],
    cmd = """
        tar czhf $(OUTS) \\
            --transform 's|^.*docs/code-samples/settlement/||' \\
            --owner=1000 \\
            --group=1000 \\
            --mtime='2000-01-01 00:00Z' \\
            --no-acls \\
            --no-xattrs \\
            --no-selinux \\
            --sort=name \\
            $(SRCS)
    """,
)

genrule(
    name = "upgrades",
    srcs = glob(
        ["docs/code-samples/upgrades/**/*"],
        exclude = [
            "docs/code-samples/upgrades/daml.yaml",
            "docs/code-samples/upgrades/NO_AUTO_COPYRIGHT",
        ],
    ),
    outs = ["daml-finance-upgrades.tar.gz"],
    cmd = """
        tar czhf $(OUTS) \\
            --transform 's|^.*docs/code-samples/upgrades/||' \\
            --owner=1000 \\
            --group=1000 \\
            --mtime='2000-01-01 00:00Z' \\
            --no-acls \\
            --no-xattrs \\
            --no-selinux \\
            --sort=name \\
            $(SRCS)
    """,
)

genrule(
    name = "payoff-modeling",
    srcs = glob(
        ["docs/code-samples/payoff-modeling/**/*"],
        exclude = [
            "docs/code-samples/payoff-modeling/daml.yaml",
            "docs/code-samples/payoff-modeling/NO_AUTO_COPYRIGHT",
        ],
    ),
    outs = ["daml-finance-payoff-modeling.tar.gz"],
    cmd = """
        tar czhf $(OUTS) \\
            --transform 's|^.*docs/code-samples/payoff-modeling/||' \\
            --owner=1000 \\
            --group=1000 \\
            --mtime='2000-01-01 00:00Z' \\
            --no-acls \\
            --no-xattrs \\
            --no-selinux \\
            --sort=name \\
            $(SRCS)
    """,
)
