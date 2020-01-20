load("@language_support_ts_deps//eslint:index.bzl", _eslint_test = "eslint_test")

def eslint_test(name, srcs, tsconfig = ":tsconfig.json", package_json = ":package.json", data = [], **kwargs):
    eslint_deps = [
        "@language_support_ts_deps//@typescript-eslint",
        "@language_support_ts_deps//tsutils",
    ]
    templated_args = [
        "--ext",
        "ts",
        "--config",
        "$(rlocation $(location %s))" % package_json,
    ]
    for src in srcs:
        templated_args.append("$(rlocation $(location %s))" % src)
    _eslint_test(
        name = name,
        data = srcs + [tsconfig, package_json] + data + eslint_deps,
        templated_args = templated_args,
    )
