load("@go_googleapis//:repository_rules.bzl", "switched_rules_by_language")

def _googleapis_impl(module_ctx):
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        grpc = True,
        java = True,
    )

googleapis_extension = module_extension(
    implementation = _googleapis_impl,
)