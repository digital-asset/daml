package(default_visibility = ["//visibility:public"])

CODE_SAMPLES = {
    "quickstart": "getting-started",
    "lifecycling": "lifecycling",
    "settlement": "settlement",
    "upgrades": "upgrades",
    "payoff-modeling": "payoff-modeling",
}

[
    genrule(
        name = name,
        srcs = glob(
            ["docs/code-samples/{}/**/*".format(sample_dir)],
            exclude = [
                "docs/code-samples/{}/daml.yaml".format(sample_dir),
                "docs/code-samples/{}/NO_AUTO_COPYRIGHT".format(sample_dir),
            ],
        ),
        outs = ["daml-finance-{}.tar.gz".format(name)],
        cmd = """
        DIR=$$(pwd)
        STAGE=$$(mktemp -d)
        trap "rm -rf $$STAGE" EXIT
        for src in $(SRCS); do
            rel=$${{src##*/docs/code-samples/{sample_dir}/}}
            mkdir -p "$$STAGE/$$(dirname "$$rel")"
            cp -L "$$src" "$$STAGE/$$rel"
        done
        cd $$STAGE
        $$DIR/$(execpath @//bazel_tools/sh:mktgz) $$DIR/$@ .
    """.format(sample_dir = sample_dir),
        tools = [
            "@//bazel_tools/sh:mktgz",
        ],
    )
    for name, sample_dir in CODE_SAMPLES.items()
]
