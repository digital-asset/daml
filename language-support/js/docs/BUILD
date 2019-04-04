genrule(
    name = "docs",
    tools = ['@npm//jsdoc/bin:jsdoc'],
    srcs = [
        '//language-support/js/daml-ledger:src',
        '//language-support/js/daml-ledger:README',
        'jsdoc.json',
        '@npm//ink-docstrap'
    ],
    outs = ['docs.tar.gz'],
    cmd = """
        mv language-support/js/daml-ledger/* .
        ln -s external/npm/node_modules .
        $(execpath @npm//jsdoc/bin:jsdoc) -c $(location jsdoc.json)
        tar czf $@ docs
    """,
    visibility = [
        '//:__subpackages__',
    ]
)
