resolved = [
     {
          "original_rule_class": "local_repository",
          "original_attributes": {
               "name": "bazel_tools",
               "path": "/home/aj/.cache/bazel/_bazel_aj/install/c98303d89cd2fa8a744968a61ef8336f/embedded_tools"
          },
          "native": "local_repository(name = \"bazel_tools\", path = __embedded_dir__ + \"/\" + \"embedded_tools\")"
     },
     {
          "original_rule_class": "local_repository",
          "original_attributes": {
               "name": "daml",
               "path": ".."
          },
          "native": "local_repository(name = \"daml\", path = \"..\")"
     },
     {
          "original_rule_class": "@@daml//bazel_tools:os_info.bzl%os_info",
          "definition_information": "Repository os_info instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:15:8: in <toplevel>\nRepository rule os_info defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/daml/bazel_tools/os_info.bzl:43:26: in <toplevel>\n",
          "original_attributes": {
               "name": "os_info"
          },
          "repositories": [
               {
                    "rule_class": "@@daml//bazel_tools:os_info.bzl%os_info",
                    "attributes": {
                         "name": "os_info"
                    },
                    "output_tree_hash": "decd81990944982086dae8709133ac0ed2649852ff8b45566e5bfa1efe6cb006"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository bazel_features instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:22:13: in <toplevel>\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_features",
               "url": "https://github.com/bazel-contrib/bazel_features/releases/download/v0.0.1/bazel_features-v0.0.1.tar.gz",
               "sha256": "5836c7e7b23cd20bcaef703838ee320580fe535d0337b981fb2c8367ec2a070b",
               "strip_prefix": "bazel_features-0.0.1"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazel-contrib/bazel_features/releases/download/v0.0.1/bazel_features-v0.0.1.tar.gz",
                         "urls": [],
                         "sha256": "5836c7e7b23cd20bcaef703838ee320580fe535d0337b981fb2c8367ec2a070b",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "bazel_features-0.0.1",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "bazel_features"
                    },
                    "output_tree_hash": "47bbf5df6e284592bc64b02aee7b3356a275bceaa4fe383f627724946291d600"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_haskell instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:68:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell",
               "generator_name": "rules_haskell",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_haskell/archive/a361943682c2f312de4afff0e4438259bfd8119c.tar.gz"
               ],
               "sha256": "f2b04c7dd03f8adacc44f44e6232cd086c02395a03236228d8f09335a931ab9c",
               "strip_prefix": "rules_haskell-a361943682c2f312de4afff0e4438259bfd8119c",
               "patches": [
                    "@@daml//bazel_tools:haskell-import-dirs-empty-libs.patch",
                    "@@daml//bazel_tools:haskell-windows-extra-libraries.patch"
               ],
               "patch_args": [
                    "-p1"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_haskell/archive/a361943682c2f312de4afff0e4438259bfd8119c.tar.gz"
                         ],
                         "sha256": "f2b04c7dd03f8adacc44f44e6232cd086c02395a03236228d8f09335a931ab9c",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_haskell-a361943682c2f312de4afff0e4438259bfd8119c",
                         "add_prefix": "",
                         "type": "",
                         "patches": [
                              "@@daml//bazel_tools:haskell-import-dirs-empty-libs.patch",
                              "@@daml//bazel_tools:haskell-windows-extra-libraries.patch"
                         ],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p1"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_haskell"
                    },
                    "output_tree_hash": "a4fda52043ebda5a67df905a28f85a74201de11997bd7a10307d2f9f9bb68b23"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository io_bazel_rules_go instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:188:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "io_bazel_rules_go",
               "generator_name": "io_bazel_rules_go",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
                    "https://github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip"
               ],
               "sha256": "b78f77458e77162f45b4564d6b20b6f92f56431ed59eaaab09e7819d1d850313"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
                              "https://github.com/bazelbuild/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip"
                         ],
                         "sha256": "b78f77458e77162f45b4564d6b20b6f92f56431ed59eaaab09e7819d1d850313",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "io_bazel_rules_go"
                    },
                    "output_tree_hash": "f6475a04da80eb4e1ac078f7e539987804d642f7932a64337ca89f33913366e4"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository bazel_gazelle instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:179:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_gazelle",
               "generator_name": "bazel_gazelle",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/bazel-contrib/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz"
               ],
               "sha256": "5d80e62a70314f39cc764c1c3eaa800c5936c9f1ea91625006227ce4d20cd086"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/bazel-contrib/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz"
                         ],
                         "sha256": "5d80e62a70314f39cc764c1c3eaa800c5936c9f1ea91625006227ce4d20cd086",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "bazel_gazelle"
                    },
                    "output_tree_hash": "75db8f19cf53beb69e28bb8fff8627571349d20459e4c01711a38aa0406594f5"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository bazel_skylib instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:40:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_skylib",
               "generator_name": "bazel_skylib",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
                    "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz"
               ],
               "sha256": "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
                              "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz"
                         ],
                         "sha256": "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "bazel_skylib"
                    },
                    "output_tree_hash": "30429147a3cbfa04f675c4bf0801bb2aab98de7087c21d7383704d8f00d7ce7a"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_sh instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:66:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_sh",
               "generator_name": "rules_sh",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_sh/releases/download/v0.4.0/rules_sh-0.4.0.tar.gz"
               ],
               "sha256": "3243af3fcb3768633fd39f3654de773e5fb61471a2fae5762a1653c22c412d2c",
               "strip_prefix": "rules_sh-0.4.0"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_sh/releases/download/v0.4.0/rules_sh-0.4.0.tar.gz"
                         ],
                         "sha256": "3243af3fcb3768633fd39f3654de773e5fb61471a2fae5762a1653c22c412d2c",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_sh-0.4.0",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_sh"
                    },
                    "output_tree_hash": "89488f1e909afc9718ae236f342a1af12b575d013ca26b334c05eef133e26d23"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository build_bazel_rules_nodejs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:148:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "build_bazel_rules_nodejs",
               "generator_name": "build_bazel_rules_nodejs",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.5/rules_nodejs-5.8.5.tar.gz"
               ],
               "sha256": "a1295b168f183218bc88117cf00674bcd102498f294086ff58318f830dd9d9d1",
               "patches": [
                    "@@daml//bazel_tools:rules_nodejs_hotfix.patch"
               ],
               "patch_args": [
                    "-p1"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.5/rules_nodejs-5.8.5.tar.gz"
                         ],
                         "sha256": "a1295b168f183218bc88117cf00674bcd102498f294086ff58318f830dd9d9d1",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [
                              "@@daml//bazel_tools:rules_nodejs_hotfix.patch"
                         ],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p1"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "build_bazel_rules_nodejs"
                    },
                    "output_tree_hash": "57173687cde7cf1223c35ae33f2bc135c0062eec8d5bddf5d5fbe2fa18a3e3d6"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nodejs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:64:38: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/build_bazel_rules_nodejs/repositories.bzl:25:10: in build_bazel_rules_nodejs_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nodejs",
               "generator_name": "rules_nodejs",
               "generator_function": "build_bazel_rules_nodejs_dependencies",
               "generator_location": None,
               "urls": [
                    "https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.5/rules_nodejs-core-5.8.5.tar.gz"
               ],
               "sha256": "0c2277164b1752bb71ecfba3107f01c6a8fb02e4835a790914c71dfadcf646ba"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/bazelbuild/rules_nodejs/releases/download/5.8.5/rules_nodejs-core-5.8.5.tar.gz"
                         ],
                         "sha256": "0c2277164b1752bb71ecfba3107f01c6a8fb02e4835a790914c71dfadcf646ba",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nodejs"
                    },
                    "output_tree_hash": "bc3107a32b11260d08fe9dbc8be88fd6c76dc366b0f7fd101c87786cdccd8525"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository io_tweag_rules_nixpkgs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:87:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "io_tweag_rules_nixpkgs",
               "generator_name": "io_tweag_rules_nixpkgs",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0",
               "patch_args": [
                    "-p1"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p1"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "io_tweag_rules_nixpkgs"
                    },
                    "output_tree_hash": "5f3bc1ad55b60da54dade802a46025b3f1d042aa13df650217eff3c462e29732"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_python instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:111:25: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_python",
               "generator_name": "rules_nixpkgs_python",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/python"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/python",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_python"
                    },
                    "output_tree_hash": "6cd90a46438c7d7cb9e011fa8e8b809d0ccc90180bdabdeb3da6d030eb242bcf"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_cc instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:111:25: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_cc",
               "generator_name": "rules_nixpkgs_cc",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/cc"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/cc",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_cc"
                    },
                    "output_tree_hash": "d3519b67fb4733d27fe1d48c650a5654bfff0149141a2e0fb9f9d54a825f7809"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_rust instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:111:25: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_rust",
               "generator_name": "rules_nixpkgs_rust",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/rust"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/rust",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_rust"
                    },
                    "output_tree_hash": "ed59cae224d41b2b6933107fedf00770519ffee1c688a6360329ca2a17507356"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_java instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:111:25: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_java",
               "generator_name": "rules_nixpkgs_java",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/java"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/java",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_java"
                    },
                    "output_tree_hash": "b4381de0de07586237121aa83f61630ef4d761dc0136e691f95dc8ceef33d19e"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_nodejs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:103:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_nodejs",
               "generator_name": "rules_nixpkgs_nodejs",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/nodejs"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/nodejs",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_nodejs"
                    },
                    "output_tree_hash": "126917e2d7b10c52279694f18fb898df7d3d551105ba3c4767f0badc0367dcd4"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_core instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:95:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_core",
               "generator_name": "rules_nixpkgs_core",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/core",
               "patch_args": [
                    "-p2"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/core",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p2"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_core"
                    },
                    "output_tree_hash": "18e37c30dc69aafeef165b4e28773a2925c5b96be4a4f2c28310c3ffcfffd6ca"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_cc instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:50:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_cc",
               "generator_name": "rules_cc",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://github.com/bazelbuild/rules_cc/releases/download/0.0.9/rules_cc-0.0.9.tar.gz"
               ],
               "sha256": "2037875b9a4456dce4a79d112a8ae885bbc4aad968e6587dca6e64f3a0900cdf",
               "strip_prefix": "rules_cc-0.0.9"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/bazelbuild/rules_cc/releases/download/0.0.9/rules_cc-0.0.9.tar.gz"
                         ],
                         "sha256": "2037875b9a4456dce4a79d112a8ae885bbc4aad968e6587dca6e64f3a0900cdf",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_cc-0.0.9",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_cc"
                    },
                    "output_tree_hash": "eefc332fe980e25b58e7a4bd9fccd9e1a06dcb06f81423ce66248b4f1e7f8f74"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_nixpkgs_posix instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:111:25: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_nixpkgs_posix",
               "generator_name": "rules_nixpkgs_posix",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
               ],
               "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
               "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/posix"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/tweag/rules_nixpkgs/releases/download/v0.13.0/rules_nixpkgs-0.13.0.tar.gz"
                         ],
                         "sha256": "30271f7bd380e4e20e4d7132c324946c4fdbc31ebe0bbb6638a0f61a37e74397",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_nixpkgs-0.13.0/toolchains/posix",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_nixpkgs_posix"
                    },
                    "output_tree_hash": "ef4585dab2ca33a28b874189035b4dab4427c9d0cf2425581bbddf8058ec96cc"
               }
          ]
     },
     {
          "original_rule_class": "@@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl%dadew",
          "definition_information": "Repository dadew instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:55:6: in <toplevel>\nRepository rule dadew defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/daml/bazel_tools/dev_env_tool/dev_env_tool.bzl:103:24: in <toplevel>\n",
          "original_attributes": {
               "name": "dadew"
          },
          "repositories": [
               {
                    "rule_class": "@@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl%dadew",
                    "attributes": {
                         "name": "dadew"
                    },
                    "output_tree_hash": "8fbefebea3d4a4935293ce2d623351e6f12aace147e48599ec37eecfa6a1ba08"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository io_bazel_rules_scala instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:167:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "io_bazel_rules_scala",
               "generator_name": "io_bazel_rules_scala",
               "generator_function": "daml_deps",
               "generator_location": None,
               "url": "https://github.com/bazelbuild/rules_scala/releases/download/v6.6.0/rules_scala-v6.6.0.tar.gz",
               "sha256": "e734eef95cf26c0171566bdc24d83bd82bdaf8ca7873bec6ce9b0d524bdaf05d",
               "strip_prefix": "rules_scala-6.6.0",
               "patches": [
                    "@@daml//bazel_tools:scala-escape-jvmflags.patch"
               ],
               "patch_args": [
                    "-p1"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazelbuild/rules_scala/releases/download/v6.6.0/rules_scala-v6.6.0.tar.gz",
                         "urls": [],
                         "sha256": "e734eef95cf26c0171566bdc24d83bd82bdaf8ca7873bec6ce9b0d524bdaf05d",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_scala-6.6.0",
                         "add_prefix": "",
                         "type": "",
                         "patches": [
                              "@@daml//bazel_tools:scala-escape-jvmflags.patch"
                         ],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p1"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "io_bazel_rules_scala"
                    },
                    "output_tree_hash": "45c005ccc3b5004d145cda10d78ce92ad3c7f3048f9aaefa8fd4e93c14b343bb"
               }
          ]
     },
     {
          "original_rule_class": "@@daml//bazel_tools:scala_version.bzl%scala_version_configure",
          "definition_information": "Repository scala_version instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:35:24: in <toplevel>\nRepository rule scala_version_configure defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/daml/bazel_tools/scala_version.bzl:60:42: in <toplevel>\n",
          "original_attributes": {
               "name": "scala_version"
          },
          "repositories": [
               {
                    "rule_class": "@@daml//bazel_tools:scala_version.bzl%scala_version_configure",
                    "attributes": {
                         "name": "scala_version"
                    },
                    "output_tree_hash": "afa5e763025cee2e30a9fddbe722605c3968353c3778ffd58f5d9e0e8cc0376e"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository com_github_google_bazel_common instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:198:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "com_github_google_bazel_common",
               "generator_name": "com_github_google_bazel_common",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/google/bazel-common/releases/download/0.0.1/bazel-common-0.0.1.tar.gz"
               ],
               "sha256": "97bfda563b1c755d5cb4fae7aa1f48b43b2f4c2ca6fd6202076c910dcbeae772",
               "strip_prefix": "bazel-common-0.0.1"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/google/bazel-common/releases/download/0.0.1/bazel-common-0.0.1.tar.gz"
                         ],
                         "sha256": "97bfda563b1c755d5cb4fae7aa1f48b43b2f4c2ca6fd6202076c910dcbeae772",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "bazel-common-0.0.1",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "com_github_google_bazel_common"
                    },
                    "output_tree_hash": "8339ad5613e2cdb594c0efda5b7548a8198c0ee8b3d17b734a2db5b6e78cdafb"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_java instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:59:17: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_java",
               "generator_name": "rules_java",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/bazelbuild/rules_java/releases/download/7.3.1/rules_java-7.3.1.tar.gz"
               ],
               "sha256": "4018e97c93f97680f1650ffd2a7530245b864ac543fd24fae8c02ba447cb2864"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/bazelbuild/rules_java/releases/download/7.3.1/rules_java-7.3.1.tar.gz"
                         ],
                         "sha256": "4018e97c93f97680f1650ffd2a7530245b864ac543fd24fae8c02ba447cb2864",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_java"
                    },
                    "output_tree_hash": "e40562ba55d5cf887e4a34e6bed1d7f3c7675453d301e8df57a9446df62e41c2"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_jvm_external instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:159:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_jvm_external",
               "generator_name": "rules_jvm_external",
               "generator_function": "daml_deps",
               "generator_location": None,
               "url": "https://github.com/bazel-contrib/rules_jvm_external/releases/download/6.7/rules_jvm_external-6.7.tar.gz",
               "sha256": "a1e351607f04fed296ba33c4977d3fe2a615ed50df7896676b67aac993c53c18",
               "strip_prefix": "rules_jvm_external-6.7"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazel-contrib/rules_jvm_external/releases/download/6.7/rules_jvm_external-6.7.tar.gz",
                         "urls": [],
                         "sha256": "a1e351607f04fed296ba33c4977d3fe2a615ed50df7896676b67aac993c53c18",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_jvm_external-6.7",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_jvm_external"
                    },
                    "output_tree_hash": "5b710de7b30289b0b5fc2592dbf3dd8444b76b4caf84e57931e3bd12a679e495"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_features//private:version_repo.bzl%version_repo",
          "definition_information": "Repository bazel_features_version instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:31:20: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_features/deps.bzl:6:17: in bazel_features_deps\nRepository rule version_repo defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_features/private/version_repo.bzl:5:31: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_features_version",
               "generator_name": "bazel_features_version",
               "generator_function": "bazel_features_deps",
               "generator_location": None
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_features//private:version_repo.bzl%version_repo",
                    "attributes": {
                         "name": "bazel_features_version",
                         "generator_name": "bazel_features_version",
                         "generator_function": "bazel_features_deps",
                         "generator_location": None
                    },
                    "output_tree_hash": "638707fdc073cc50a2979c91c781ab428b0290a498e4725f20535a8cfdfb1647"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_features//private:globals_repo.bzl%globals_repo",
          "definition_information": "Repository bazel_features_globals instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:31:20: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_features/deps.bzl:7:17: in bazel_features_deps\nRepository rule globals_repo defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_features/private/globals_repo.bzl:20:31: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_features_globals",
               "generator_name": "bazel_features_globals",
               "generator_function": "bazel_features_deps",
               "generator_location": None,
               "globals": {
                    "RunEnvironmentInfo": "5.3.0",
                    "DefaultInfo": "0.0.1",
                    "__TestingOnly_NeverAvailable": "1000000000.0.0"
               }
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_features//private:globals_repo.bzl%globals_repo",
                    "attributes": {
                         "name": "bazel_features_globals",
                         "generator_name": "bazel_features_globals",
                         "generator_function": "bazel_features_deps",
                         "generator_location": None,
                         "globals": {
                              "RunEnvironmentInfo": "5.3.0",
                              "DefaultInfo": "0.0.1",
                              "__TestingOnly_NeverAvailable": "1000000000.0.0"
                         }
                    },
                    "output_tree_hash": "55f2aff2606a2802c678ef8ba697b5bf1232a3029a0787b37cb6d7ee15d00f48"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_license instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:173:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_license",
               "generator_name": "rules_license",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz",
                    "https://github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz"
               ],
               "sha256": "26d4021f6898e23b82ef953078389dd49ac2b5618ac564ade4ef87cced147b38"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz",
                              "https://github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz"
                         ],
                         "sha256": "26d4021f6898e23b82ef953078389dd49ac2b5618ac564ade4ef87cced147b38",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_license"
                    },
                    "output_tree_hash": "41c12579d6666817c7ff729112bd25965b7c97eb99a7cead8ef6e773a97e92c0"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_jvm_external//private/rules:coursier.bzl%pinned_coursier_fetch",
          "definition_information": "Repository maven instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:343:14: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_jvm_external/private/rules/maven_install.bzl:154:30: in maven_install\nRepository rule pinned_coursier_fetch defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_jvm_external/private/rules/coursier.bzl:1395:40: in <toplevel>\n",
          "original_attributes": {
               "name": "maven",
               "generator_name": "maven",
               "generator_function": "maven_install",
               "generator_location": None,
               "resolver": "coursier",
               "repositories": [
                    "{ \"repo_url\": \"https://repo1.maven.org/maven2\" }"
               ],
               "artifacts": [
                    "{ \"group\": \"com.daml\", \"artifact\": \"bindings-akka_2.13\", \"version\": \"2.7.6\" }",
                    "{ \"group\": \"com.daml\", \"artifact\": \"daml-lf-archive-reader_2.13\", \"version\": \"2.8.0\" }",
                    "{ \"group\": \"com.daml\", \"artifact\": \"daml-lf-transaction_2.13\", \"version\": \"2.8.0\" }",
                    "{ \"group\": \"com.daml\", \"artifact\": \"ledger-api-common_2.13\", \"version\": \"2.8.0\" }",
                    "{ \"group\": \"com.daml\", \"artifact\": \"lf-value-json_2.13\", \"version\": \"2.8.0\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-api\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-core\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-netty\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-protobuf\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-services\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-stub\", \"version\": \"1.67.1\" }",
                    "{ \"group\": \"com.github.scopt\", \"artifact\": \"scopt_2.13\", \"version\": \"3.7.1\" }",
                    "{ \"group\": \"org.wartremover\", \"artifact\": \"wartremover_2.13.16\", \"version\": \"3.2.5\" }",
                    "{ \"group\": \"io.spray\", \"artifact\": \"spray-json_2.13\", \"version\": \"1.3.5\" }"
               ],
               "boms": [],
               "fetch_sources": False,
               "fetch_javadoc": False,
               "generate_compat_repositories": False,
               "maven_install_json": "//:maven_install.json",
               "override_targets": {},
               "strict_visibility": False,
               "strict_visibility_value": [
                    "//visibility:private"
               ],
               "additional_netrc_lines": [],
               "use_credentials_from_home_netrc_file": False,
               "fail_if_repin_required": False,
               "use_starlark_android_rules": False,
               "aar_import_bzl_label": "@build_bazel_rules_android//android:rules.bzl",
               "duplicate_version_warning": "warn",
               "excluded_artifacts": []
          },
          "repositories": [
               {
                    "rule_class": "@@rules_jvm_external//private/rules:coursier.bzl%pinned_coursier_fetch",
                    "attributes": {
                         "name": "maven",
                         "generator_name": "maven",
                         "generator_function": "maven_install",
                         "generator_location": None,
                         "resolver": "coursier",
                         "repositories": [
                              "{ \"repo_url\": \"https://repo1.maven.org/maven2\" }"
                         ],
                         "artifacts": [
                              "{ \"group\": \"com.daml\", \"artifact\": \"bindings-akka_2.13\", \"version\": \"2.7.6\" }",
                              "{ \"group\": \"com.daml\", \"artifact\": \"daml-lf-archive-reader_2.13\", \"version\": \"2.8.0\" }",
                              "{ \"group\": \"com.daml\", \"artifact\": \"daml-lf-transaction_2.13\", \"version\": \"2.8.0\" }",
                              "{ \"group\": \"com.daml\", \"artifact\": \"ledger-api-common_2.13\", \"version\": \"2.8.0\" }",
                              "{ \"group\": \"com.daml\", \"artifact\": \"lf-value-json_2.13\", \"version\": \"2.8.0\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-api\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-core\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-netty\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-protobuf\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-services\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"io.grpc\", \"artifact\": \"grpc-stub\", \"version\": \"1.67.1\" }",
                              "{ \"group\": \"com.github.scopt\", \"artifact\": \"scopt_2.13\", \"version\": \"3.7.1\" }",
                              "{ \"group\": \"org.wartremover\", \"artifact\": \"wartremover_2.13.16\", \"version\": \"3.2.5\" }",
                              "{ \"group\": \"io.spray\", \"artifact\": \"spray-json_2.13\", \"version\": \"1.3.5\" }"
                         ],
                         "boms": [],
                         "fetch_sources": False,
                         "fetch_javadoc": False,
                         "generate_compat_repositories": False,
                         "maven_install_json": "//:maven_install.json",
                         "override_targets": {},
                         "strict_visibility": False,
                         "strict_visibility_value": [
                              "//visibility:private"
                         ],
                         "additional_netrc_lines": [],
                         "use_credentials_from_home_netrc_file": False,
                         "fail_if_repin_required": False,
                         "use_starlark_android_rules": False,
                         "aar_import_bzl_label": "@build_bazel_rules_android//android:rules.bzl",
                         "duplicate_version_warning": "warn",
                         "excluded_artifacts": []
                    },
                    "output_tree_hash": "7261dde5ee3f47c91076548dc69ebeb56ba3c94381311ac95603928f20ffe3c0"
               }
          ]
     },
     {
          "original_rule_class": "@@io_bazel_rules_scala//:scala_config.bzl%_config_repository",
          "definition_information": "Repository io_bazel_rules_scala_config instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:374:13: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_scala/scala_config.bzl:92:23: in scala_config\nRepository rule _config_repository defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_scala/scala_config.bzl:70:37: in <toplevel>\n",
          "original_attributes": {
               "name": "io_bazel_rules_scala_config",
               "generator_name": "io_bazel_rules_scala_config",
               "generator_function": "scala_config",
               "generator_location": None,
               "scala_version": "2.13.16",
               "scala_versions": [],
               "enable_compiler_dependency_tracking": False
          },
          "repositories": [
               {
                    "rule_class": "@@io_bazel_rules_scala//:scala_config.bzl%_config_repository",
                    "attributes": {
                         "name": "io_bazel_rules_scala_config",
                         "generator_name": "io_bazel_rules_scala_config",
                         "generator_function": "scala_config",
                         "generator_location": None,
                         "scala_version": "2.13.16",
                         "scala_versions": [],
                         "enable_compiler_dependency_tracking": False
                    },
                    "output_tree_hash": "b41911677c7ba19b9061b9c2b1ae3ef05c6530a596c073cf7b657dfe2e096323"
               }
          ]
     },
     {
          "original_rule_class": "local_repository",
          "original_attributes": {
               "name": "rules_java_builtin",
               "path": "/home/aj/.cache/bazel/_bazel_aj/install/c98303d89cd2fa8a744968a61ef8336f/rules_java"
          },
          "native": "local_repository(name = \"rules_java_builtin\", path = __embedded_dir__ + \"/\" + \"rules_java\")"
     },
     {
          "original_rule_class": "local_config_platform",
          "original_attributes": {
               "name": "local_config_platform"
          },
          "native": "local_config_platform(name = 'local_config_platform')"
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository platforms instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:30:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "platforms",
               "generator_name": "platforms",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
                    "https://github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz"
               ],
               "sha256": "218efe8ee736d26a3572663b374a253c012b716d8af0c07e842e82f238a0a7ee"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz",
                              "https://github.com/bazelbuild/platforms/releases/download/0.0.10/platforms-0.0.10.tar.gz"
                         ],
                         "sha256": "218efe8ee736d26a3572663b374a253c012b716d8af0c07e842e82f238a0a7ee",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "platforms"
                    },
                    "output_tree_hash": "ad99d2c6fedcf39b61c65aa76de4b7f5f28d7460bcf26f99ac64a4be9d3616e2"
               }
          ]
     },
     {
          "original_rule_class": "local_repository",
          "original_attributes": {
               "name": "head_sdk",
               "path": "head_sdk"
          },
          "native": "local_repository(name = \"head_sdk\", path = \"head_sdk\")"
     },
     {
          "original_rule_class": "@@rules_nixpkgs_posix//:posix.bzl%_nixpkgs_sh_posix_toolchain",
          "definition_information": "Repository rules_haskell_sh_posix_nixpkgs_toolchain instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:150:29: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:412:31: in haskell_register_ghc_nixpkgs\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_posix/posix.bzl:171:32: in nixpkgs_sh_posix_configure\nRepository rule _nixpkgs_sh_posix_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_posix/posix.bzl:129:46: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_sh_posix_nixpkgs_toolchain",
               "generator_name": "rules_haskell_sh_posix_nixpkgs_toolchain",
               "generator_function": "haskell_register_ghc_nixpkgs",
               "generator_location": None,
               "workspace": "rules_haskell_sh_posix_nixpkgs"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_posix//:posix.bzl%_nixpkgs_sh_posix_toolchain",
                    "attributes": {
                         "name": "rules_haskell_sh_posix_nixpkgs_toolchain",
                         "generator_name": "rules_haskell_sh_posix_nixpkgs_toolchain",
                         "generator_function": "haskell_register_ghc_nixpkgs",
                         "generator_location": None,
                         "workspace": "rules_haskell_sh_posix_nixpkgs"
                    },
                    "output_tree_hash": "e17f2d7004883cc16b44c0badb1a54a2963631424848b8306e86acb1461fb881"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_cc//:cc.bzl%_nixpkgs_cc_toolchain",
          "definition_information": "Repository local_config_cc_toolchains instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:112:21: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_cc/cc.bzl:489:26: in nixpkgs_cc_configure\nRepository rule _nixpkgs_cc_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_cc/cc.bzl:299:40: in <toplevel>\n",
          "original_attributes": {
               "name": "local_config_cc_toolchains",
               "generator_name": "local_config_cc_toolchains",
               "generator_function": "nixpkgs_cc_configure",
               "generator_location": None,
               "cc_toolchain_config": "local_config_cc",
               "cross_cpu": ""
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_cc//:cc.bzl%_nixpkgs_cc_toolchain",
                    "attributes": {
                         "name": "local_config_cc_toolchains",
                         "generator_name": "local_config_cc_toolchains",
                         "generator_function": "nixpkgs_cc_configure",
                         "generator_location": None,
                         "cc_toolchain_config": "local_config_cc",
                         "cross_cpu": ""
                    },
                    "output_tree_hash": "7cf53387edc1ef451143da41898d60073eaaea94141550b3bc2f7db98fdbbad8"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nodejs//nodejs/private:toolchains_repo.bzl%toolchains_repo",
          "definition_information": "Repository nodejs_toolchains instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:68:18: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/build_bazel_rules_nodejs/internal/node/node_repositories.bzl:50:31: in node_repositories\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nodejs/nodejs/repositories.bzl:425:20: in nodejs_register_toolchains\nRepository rule toolchains_repo defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nodejs/nodejs/private/toolchains_repo.bzl:127:34: in <toplevel>\n",
          "original_attributes": {
               "name": "nodejs_toolchains",
               "generator_name": "nodejs_toolchains",
               "generator_function": "node_repositories",
               "generator_location": None,
               "user_node_repository_name": "nodejs"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nodejs//nodejs/private:toolchains_repo.bzl%toolchains_repo",
                    "attributes": {
                         "name": "nodejs_toolchains",
                         "generator_name": "nodejs_toolchains",
                         "generator_function": "node_repositories",
                         "generator_location": None,
                         "user_node_repository_name": "nodejs"
                    },
                    "output_tree_hash": "9d1cdbde62627f38d6066fcb7c51f3d02c685662707e1e80e1471d6a6150a8cc"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/sh:sh_configure.bzl%sh_config",
          "definition_information": "Repository local_config_sh instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:174:13: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/sh/sh_configure.bzl:83:14: in sh_configure\nRepository rule sh_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/sh/sh_configure.bzl:72:28: in <toplevel>\n",
          "original_attributes": {
               "name": "local_config_sh",
               "generator_name": "local_config_sh",
               "generator_function": "sh_configure",
               "generator_location": None
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/sh:sh_configure.bzl%sh_config",
                    "attributes": {
                         "name": "local_config_sh",
                         "generator_name": "local_config_sh",
                         "generator_function": "sh_configure",
                         "generator_location": None
                    },
                    "output_tree_hash": "6f23d1fe2cb1a440e0cf2265222dcb2b38d93fd55b4a568d0009c7733f7a586b"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_haskell//haskell:nixpkgs.bzl%_ghc_nixpkgs_toolchain",
          "definition_information": "Repository rules_haskell_ghc_nixpkgs_toolchain instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:150:29: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:418:38: in haskell_register_ghc_nixpkgs\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:291:27: in register_ghc_from_nixpkgs_package\nRepository rule _ghc_nixpkgs_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:208:41: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_ghc_nixpkgs_toolchain",
               "generator_name": "rules_haskell_ghc_nixpkgs_toolchain",
               "generator_function": "haskell_register_ghc_nixpkgs",
               "generator_location": None,
               "exec_constraints": [],
               "target_constraints": [],
               "haskell_toolchain_repo_name": "rules_haskell_ghc_nixpkgs_haskell_toolchain"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_haskell//haskell:nixpkgs.bzl%_ghc_nixpkgs_toolchain",
                    "attributes": {
                         "name": "rules_haskell_ghc_nixpkgs_toolchain",
                         "generator_name": "rules_haskell_ghc_nixpkgs_toolchain",
                         "generator_function": "haskell_register_ghc_nixpkgs",
                         "generator_location": None,
                         "exec_constraints": [],
                         "target_constraints": [],
                         "haskell_toolchain_repo_name": "rules_haskell_ghc_nixpkgs_haskell_toolchain"
                    },
                    "output_tree_hash": "63cedfc52a191b3fa97914abd2080490112687d33cf97530b043646675fbcaa1"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_python//:python.bzl%_nixpkgs_python_toolchain",
          "definition_information": "Repository nixpkgs_python_toolchain instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:160:25: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_python/python.bzl:170:30: in nixpkgs_python_configure\nRepository rule _nixpkgs_python_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_python/python.bzl:57:44: in <toplevel>\n",
          "original_attributes": {
               "name": "nixpkgs_python_toolchain",
               "generator_name": "nixpkgs_python_toolchain",
               "generator_function": "nixpkgs_python_configure",
               "generator_location": None,
               "python3_repo": "@nixpkgs_python_toolchain_python3//"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_python//:python.bzl%_nixpkgs_python_toolchain",
                    "attributes": {
                         "name": "nixpkgs_python_toolchain",
                         "generator_name": "nixpkgs_python_toolchain",
                         "generator_function": "nixpkgs_python_configure",
                         "generator_location": None,
                         "python3_repo": "@nixpkgs_python_toolchain_python3//"
                    },
                    "output_tree_hash": "063890830ac16ad316613d652303925fa1d479ee5aeed1de7c865d7800e4f1f4"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_java//:java.bzl%_nixpkgs_java_toolchain",
          "definition_information": "Repository nixpkgs_java_runtime_toolchain instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:323:23: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_java/java.bzl:391:32: in nixpkgs_java_configure\nRepository rule _nixpkgs_java_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_java/java.bzl:99:42: in <toplevel>\n",
          "original_attributes": {
               "name": "nixpkgs_java_runtime_toolchain",
               "generator_name": "nixpkgs_java_runtime_toolchain",
               "generator_function": "nixpkgs_java_configure",
               "generator_location": None,
               "runtime_repo": "nixpkgs_java_runtime",
               "runtime_version": "17",
               "runtime_name": "nixpkgs_java"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_java//:java.bzl%_nixpkgs_java_toolchain",
                    "attributes": {
                         "name": "nixpkgs_java_runtime_toolchain",
                         "generator_name": "nixpkgs_java_runtime_toolchain",
                         "generator_function": "nixpkgs_java_configure",
                         "generator_location": None,
                         "runtime_repo": "nixpkgs_java_runtime",
                         "runtime_version": "17",
                         "runtime_name": "nixpkgs_java"
                    },
                    "output_tree_hash": "0b0c1c0a580bfa312bceb881e57e6f8f27c00afd60a76ce8c0940660d6785d17"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_linux_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:183:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_linux_toolchain_config_repo",
               "generator_name": "remotejdk11_linux_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_linux_toolchain_config_repo",
                         "generator_name": "remotejdk11_linux_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "0a170bf4f31e6c4621aeb4d4ce4b75b808be2f3a63cb55dc8172c27707d299ab"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk21_win_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:528:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:503:10: in remote_jdk21_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk21_win_toolchain_config_repo",
               "generator_name": "remotejdk21_win_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_win//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk21_win_toolchain_config_repo",
                         "generator_name": "remotejdk21_win_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_win//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "87012328b07a779503deec0ef47132a0de50efd69afe7df87619bcc07b1dc4ed"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk21_macos_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:528:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:488:10: in remote_jdk21_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk21_macos_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk21_macos_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk21_macos_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk21_macos_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "706d910cc6809ea7f77fa4f938a4f019dd90d9dad927fb804a14b04321300a36"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_linux_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:199:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_linux_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk11_linux_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_linux_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk11_linux_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "bef508c068dd47d605f62c53ab0628f1f7f5101fdcc8ada09b2067b36c47931f"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk21_macos_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:528:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:472:10: in remote_jdk21_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk21_macos_toolchain_config_repo",
               "generator_name": "remotejdk21_macos_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk21_macos_toolchain_config_repo",
                         "generator_name": "remotejdk21_macos_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_macos//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "434446eddb7f6a3dcc7a2a5330ed9ab26579c5142c19866b197475a695fbb32f"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_linux_ppc64le_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:215:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_linux_ppc64le_toolchain_config_repo",
               "generator_name": "remotejdk11_linux_ppc64le_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_ppc64le//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_ppc64le//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_linux_ppc64le_toolchain_config_repo",
                         "generator_name": "remotejdk11_linux_ppc64le_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_ppc64le//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_ppc64le//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "3272b586976beea589d09ea8029fd5d714da40127c8850e3480991c2440c5825"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_linux_s390x_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:231:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_linux_s390x_toolchain_config_repo",
               "generator_name": "remotejdk11_linux_s390x_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_s390x//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_s390x//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_linux_s390x_toolchain_config_repo",
                         "generator_name": "remotejdk11_linux_s390x_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_s390x//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_linux_s390x//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "244e11245106a8495ac4744a90023b87008e3e553766ba11d47a9fe5b4bb408d"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_macos_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:247:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_macos_toolchain_config_repo",
               "generator_name": "remotejdk11_macos_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_macos_toolchain_config_repo",
                         "generator_name": "remotejdk11_macos_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "45b3b36d22d3e614745e7a5e838351c32fe0eabb09a4a197bac0f4d416a950ce"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk21_linux_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:528:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:456:10: in remote_jdk21_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk21_linux_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk21_linux_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk21_linux_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk21_linux_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "bb33021f243382d2fb849ec204c5c8be5083c37e081df71d34a84324687cf001"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_macos_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:263:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_macos_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk11_macos_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_macos_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk11_macos_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_macos_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "ca1d067909669aa58188026a7da06d43bdec74a3ba5c122af8a4c3660acd8d8f"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_win_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:279:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_win_toolchain_config_repo",
               "generator_name": "remotejdk11_win_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_win_toolchain_config_repo",
                         "generator_name": "remotejdk11_win_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "d0587a4ecc9323d5cf65314b2d284b520ffb5ee1d3231cc6601efa13dadcc0f4"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk11_win_arm64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:526:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:295:10: in remote_jdk11_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk11_win_arm64_toolchain_config_repo",
               "generator_name": "remotejdk11_win_arm64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win_arm64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win_arm64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk11_win_arm64_toolchain_config_repo",
                         "generator_name": "remotejdk11_win_arm64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_11\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"11\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win_arm64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk11_win_arm64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "c237bd9668de9b6437c452c020ea5bc717ff80b1a5ffd581adfdc7d4a6c5fe03"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk21_linux_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:528:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:440:10: in remote_jdk21_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk21_linux_toolchain_config_repo",
               "generator_name": "remotejdk21_linux_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk21_linux_toolchain_config_repo",
                         "generator_name": "remotejdk21_linux_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_21\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"21\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk21_linux//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "ee548ad054c9b75286ff3cd19792e433a2d1236378d3a0d8076fca0bb1a88e05"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_linux_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:312:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_linux_toolchain_config_repo",
               "generator_name": "remotejdk17_linux_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_linux_toolchain_config_repo",
                         "generator_name": "remotejdk17_linux_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "f0f07fe0f645f2dc7b8c9953c7962627e1c7425cc52f543729dbff16cd20e461"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_linux_ppc64le_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:360:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_linux_ppc64le_toolchain_config_repo",
               "generator_name": "remotejdk17_linux_ppc64le_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_ppc64le//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_ppc64le//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_linux_ppc64le_toolchain_config_repo",
                         "generator_name": "remotejdk17_linux_ppc64le_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_ppc64le//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:ppc\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_ppc64le//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "fdc8ae00f2436bfc46b2f54c84f2bd84122787ede232a4d61ffc284bfe6f61ec"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_linux_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:328:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_linux_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk17_linux_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_linux_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk17_linux_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "b169b01ac1a169d7eb5e3525454c3e408e9127993ac0f578dc2c5ad183fd4e3e"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_win_arm64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:422:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_win_arm64_toolchain_config_repo",
               "generator_name": "remotejdk17_win_arm64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win_arm64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win_arm64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_win_arm64_toolchain_config_repo",
                         "generator_name": "remotejdk17_win_arm64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win_arm64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:arm64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win_arm64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "86b129d9c464a9b08f97eca7d8bc5bdb3676b581f8aac044451dbdfaa49e69d3"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_linux_s390x_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:344:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_linux_s390x_toolchain_config_repo",
               "generator_name": "remotejdk17_linux_s390x_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_s390x//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_s390x//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_linux_s390x_toolchain_config_repo",
                         "generator_name": "remotejdk17_linux_s390x_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_s390x//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:linux\", \"@platforms//cpu:s390x\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_linux_s390x//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "6ba1870e09fccfdcd423f4169b966a73f8e9deaff859ec6fb3b626ed61ebd8b5"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_macos_aarch64_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:392:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_macos_aarch64_toolchain_config_repo",
               "generator_name": "remotejdk17_macos_aarch64_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos_aarch64//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_macos_aarch64_toolchain_config_repo",
                         "generator_name": "remotejdk17_macos_aarch64_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos_aarch64//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:aarch64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos_aarch64//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "0eb17f6d969bc665a21e55d29eb51e88a067159ee62cf5094b17658a07d3accb"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_win_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:407:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_win_toolchain_config_repo",
               "generator_name": "remotejdk17_win_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_win_toolchain_config_repo",
                         "generator_name": "remotejdk17_win_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:windows\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_win//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "170c3c9a35e502555dc9f04b345e064880acbf7df935f673154011356f4aad34"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
          "definition_information": "Repository remotejdk17_macos_toolchain_config_repo instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:93:24: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:527:23: in rules_java_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/java/repositories.bzl:376:10: in remote_jdk17_repos\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:57:22: in remote_java_repository\nRepository rule _toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/remote_java_repository.bzl:27:36: in <toplevel>\n",
          "original_attributes": {
               "name": "remotejdk17_macos_toolchain_config_repo",
               "generator_name": "remotejdk17_macos_toolchain_config_repo",
               "generator_function": "rules_java_dependencies",
               "generator_location": None,
               "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos//:jdk\",\n)\n"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:remote_java_repository.bzl%_toolchain_config",
                    "attributes": {
                         "name": "remotejdk17_macos_toolchain_config_repo",
                         "generator_name": "remotejdk17_macos_toolchain_config_repo",
                         "generator_function": "rules_java_dependencies",
                         "generator_location": None,
                         "build_file": "\nconfig_setting(\n    name = \"prefix_version_setting\",\n    values = {\"java_runtime_version\": \"remotejdk_17\"},\n    visibility = [\"//visibility:private\"],\n)\nconfig_setting(\n    name = \"version_setting\",\n    values = {\"java_runtime_version\": \"17\"},\n    visibility = [\"//visibility:private\"],\n)\nalias(\n    name = \"version_or_prefix_version_setting\",\n    actual = select({\n        \":version_setting\": \":version_setting\",\n        \"//conditions:default\": \":prefix_version_setting\",\n    }),\n    visibility = [\"//visibility:private\"],\n)\ntoolchain(\n    name = \"toolchain\",\n    target_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos//:jdk\",\n)\ntoolchain(\n    name = \"bootstrap_runtime_toolchain\",\n    # These constraints are not required for correctness, but prevent fetches of remote JDK for\n    # different architectures. As every Java compilation toolchain depends on a bootstrap runtime in\n    # the same configuration, this constraint will not result in toolchain resolution failures.\n    exec_compatible_with = [\"@platforms//os:macos\", \"@platforms//cpu:x86_64\"],\n    target_settings = [\":version_or_prefix_version_setting\"],\n    toolchain_type = \"@bazel_tools//tools/jdk:bootstrap_runtime_toolchain_type\",\n    toolchain = \"@remotejdk17_macos//:jdk\",\n)\n"
                    },
                    "output_tree_hash": "41aa7b3317f8d9001746e908454760bf544ffaa058abe22f40711246608022ba"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_local_repository",
          "definition_information": "Repository nixpkgs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:102:25: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:371:30: in nixpkgs_local_repository\nRepository rule _nixpkgs_local_repository defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:326:44: in <toplevel>\n",
          "original_attributes": {
               "name": "nixpkgs",
               "generator_name": "nixpkgs",
               "generator_function": "nixpkgs_local_repository",
               "generator_location": None,
               "nix_file": "@@daml//nix:nixpkgs.nix",
               "nix_file_deps": [
                    "@@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix"
               ],
               "unmangled_name": "nixpkgs"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_local_repository",
                    "attributes": {
                         "name": "nixpkgs",
                         "generator_name": "nixpkgs",
                         "generator_function": "nixpkgs_local_repository",
                         "generator_location": None,
                         "nix_file": "@@daml//nix:nixpkgs.nix",
                         "nix_file_deps": [
                              "@@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix"
                         ],
                         "unmangled_name": "nixpkgs"
                    },
                    "output_tree_hash": "5c60a4bd1c69f2a880c8cbb1440b568eea22c213b7fac4490835d87fdd035184"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_python instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:58:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_python",
               "generator_name": "rules_python",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "url": "https://github.com/bazelbuild/rules_python/releases/download/0.21.0/rules_python-0.21.0.tar.gz",
               "sha256": "94750828b18044533e98a129003b6a68001204038dc4749f40b195b24c38f49f",
               "strip_prefix": "rules_python-0.21.0"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazelbuild/rules_python/releases/download/0.21.0/rules_python-0.21.0.tar.gz",
                         "urls": [],
                         "sha256": "94750828b18044533e98a129003b6a68001204038dc4749f40b195b24c38f49f",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_python-0.21.0",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_python"
                    },
                    "output_tree_hash": "0e1a4b7b77391134eb1ed8079427c1fd92a4766b2ed0fbcac7af97cae7c87dcd"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_java_builtin//toolchains:local_java_repository.bzl%_local_java_repository_rule",
          "definition_information": "Repository local_jdk instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:85:6: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/local_java_repository.bzl:315:32: in local_java_repository\nRepository rule _local_java_repository_rule defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_java_builtin/toolchains/local_java_repository.bzl:270:46: in <toplevel>\n",
          "original_attributes": {
               "name": "local_jdk",
               "generator_name": "local_jdk",
               "generator_function": "maybe",
               "generator_location": None,
               "build_file_content": "load(\"@rules_java//java:defs.bzl\", \"java_runtime\")\n\npackage(default_visibility = [\"//visibility:public\"])\n\nexports_files([\"WORKSPACE\", \"BUILD.bazel\"])\n\nfilegroup(\n    name = \"jre\",\n    srcs = glob(\n        [\n            \"jre/bin/**\",\n            \"jre/lib/**\",\n        ],\n        allow_empty = True,\n        # In some configurations, Java browser plugin is considered harmful and\n        # common antivirus software blocks access to npjp2.dll interfering with Bazel,\n        # so do not include it in JRE on Windows.\n        exclude = [\"jre/bin/plugin2/**\"],\n    ),\n)\n\nfilegroup(\n    name = \"jdk-bin\",\n    srcs = glob(\n        [\"bin/**\"],\n        # The JDK on Windows sometimes contains a directory called\n        # \"%systemroot%\", which is not a valid label.\n        exclude = [\"**/*%*/**\"],\n    ),\n)\n\n# This folder holds security policies.\nfilegroup(\n    name = \"jdk-conf\",\n    srcs = glob(\n        [\"conf/**\"],\n        allow_empty = True,\n    ),\n)\n\nfilegroup(\n    name = \"jdk-include\",\n    srcs = glob(\n        [\"include/**\"],\n        allow_empty = True,\n    ),\n)\n\nfilegroup(\n    name = \"jdk-lib\",\n    srcs = glob(\n        [\"lib/**\", \"release\"],\n        allow_empty = True,\n        exclude = [\n            \"lib/missioncontrol/**\",\n            \"lib/visualvm/**\",\n        ],\n    ),\n)\n\njava_runtime(\n    name = \"jdk\",\n    srcs = [\n        \":jdk-bin\",\n        \":jdk-conf\",\n        \":jdk-include\",\n        \":jdk-lib\",\n        \":jre\",\n    ],\n    # Provide the 'java` binary explicitly so that the correct path is used by\n    # Bazel even when the host platform differs from the execution platform.\n    # Exactly one of the two globs will be empty depending on the host platform.\n    # When --incompatible_disallow_empty_glob is enabled, each individual empty\n    # glob will fail without allow_empty = True, even if the overall result is\n    # non-empty.\n    java = glob([\"bin/java.exe\", \"bin/java\"], allow_empty = True)[0],\n    version = {RUNTIME_VERSION},\n)\n",
               "java_home": "",
               "version": ""
          },
          "repositories": [
               {
                    "rule_class": "@@rules_java_builtin//toolchains:local_java_repository.bzl%_local_java_repository_rule",
                    "attributes": {
                         "name": "local_jdk",
                         "generator_name": "local_jdk",
                         "generator_function": "maybe",
                         "generator_location": None,
                         "build_file_content": "load(\"@rules_java//java:defs.bzl\", \"java_runtime\")\n\npackage(default_visibility = [\"//visibility:public\"])\n\nexports_files([\"WORKSPACE\", \"BUILD.bazel\"])\n\nfilegroup(\n    name = \"jre\",\n    srcs = glob(\n        [\n            \"jre/bin/**\",\n            \"jre/lib/**\",\n        ],\n        allow_empty = True,\n        # In some configurations, Java browser plugin is considered harmful and\n        # common antivirus software blocks access to npjp2.dll interfering with Bazel,\n        # so do not include it in JRE on Windows.\n        exclude = [\"jre/bin/plugin2/**\"],\n    ),\n)\n\nfilegroup(\n    name = \"jdk-bin\",\n    srcs = glob(\n        [\"bin/**\"],\n        # The JDK on Windows sometimes contains a directory called\n        # \"%systemroot%\", which is not a valid label.\n        exclude = [\"**/*%*/**\"],\n    ),\n)\n\n# This folder holds security policies.\nfilegroup(\n    name = \"jdk-conf\",\n    srcs = glob(\n        [\"conf/**\"],\n        allow_empty = True,\n    ),\n)\n\nfilegroup(\n    name = \"jdk-include\",\n    srcs = glob(\n        [\"include/**\"],\n        allow_empty = True,\n    ),\n)\n\nfilegroup(\n    name = \"jdk-lib\",\n    srcs = glob(\n        [\"lib/**\", \"release\"],\n        allow_empty = True,\n        exclude = [\n            \"lib/missioncontrol/**\",\n            \"lib/visualvm/**\",\n        ],\n    ),\n)\n\njava_runtime(\n    name = \"jdk\",\n    srcs = [\n        \":jdk-bin\",\n        \":jdk-conf\",\n        \":jdk-include\",\n        \":jdk-lib\",\n        \":jre\",\n    ],\n    # Provide the 'java` binary explicitly so that the correct path is used by\n    # Bazel even when the host platform differs from the execution platform.\n    # Exactly one of the two globs will be empty depending on the host platform.\n    # When --incompatible_disallow_empty_glob is enabled, each individual empty\n    # glob will fail without allow_empty = True, even if the overall result is\n    # non-empty.\n    java = glob([\"bin/java.exe\", \"bin/java\"], allow_empty = True)[0],\n    version = {RUNTIME_VERSION},\n)\n",
                         "java_home": "",
                         "version": ""
                    },
                    "output_tree_hash": "f58dd20b2d3427e2115511092867ad8ebe7d68caeb66dacf9231d35ab51aa4f6"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_haskell//haskell:cabal.bzl%_fetch_stack",
          "definition_information": "Repository rules_haskell_stack instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:285:18: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/bazel-haskell-deps.bzl:23:19: in daml_haskell_deps\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/cabal.bzl:2649:21: in stack_snapshot\nRepository rule _fetch_stack defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/cabal.bzl:2421:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_stack",
               "generator_name": "rules_haskell_stack",
               "generator_function": "daml_haskell_deps",
               "generator_location": None
          },
          "repositories": [
               {
                    "rule_class": "@@rules_haskell//haskell:cabal.bzl%_fetch_stack",
                    "attributes": {
                         "name": "rules_haskell_stack",
                         "generator_name": "rules_haskell_stack",
                         "generator_function": "daml_haskell_deps",
                         "generator_location": None
                    },
                    "output_tree_hash": "8636356ffaa9dd60e982a305bfafd22a75b9a6dcea19277f93315668392d4a6d"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository openssl_nix instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:218:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "openssl_nix",
               "generator_name": "openssl_nix",
               "generator_function": "nixpkgs_package",
               "generator_location": None,
               "attribute_path": "openssl",
               "build_file_content": "",
               "fail_not_supported": False,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "nixopts": [],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "openssl_nix"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "openssl_nix",
                         "generator_name": "openssl_nix",
                         "generator_function": "nixpkgs_package",
                         "generator_location": None,
                         "attribute_path": "openssl",
                         "build_file_content": "",
                         "fail_not_supported": False,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "openssl_nix"
                    },
                    "output_tree_hash": "0963cd2bd48bbfcbfa3ae1f25c6c5436bfb994f860cc3a1100e23cb2c0c53911"
               }
          ]
     },
     {
          "original_rule_class": "@@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl%dev_env_tool",
          "definition_information": "Repository openssl_dev_env instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:227:13: in <toplevel>\nRepository rule dev_env_tool defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/daml/bazel_tools/dev_env_tool/dev_env_tool.bzl:149:31: in <toplevel>\n",
          "original_attributes": {
               "name": "openssl_dev_env",
               "tools": [
                    "openssl"
               ],
               "win_tool": "msys2",
               "win_include": [
                    "usr/bin",
                    "usr/ssl"
               ],
               "win_paths": [
                    "usr/bin/openssl.exe"
               ],
               "nix_label": "@@openssl_nix//:openssl_nix",
               "nix_include": [
                    "bin/openssl"
               ],
               "nix_paths": [
                    "bin/openssl"
               ]
          },
          "repositories": [
               {
                    "rule_class": "@@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl%dev_env_tool",
                    "attributes": {
                         "name": "openssl_dev_env",
                         "tools": [
                              "openssl"
                         ],
                         "win_tool": "msys2",
                         "win_include": [
                              "usr/bin",
                              "usr/ssl"
                         ],
                         "win_paths": [
                              "usr/bin/openssl.exe"
                         ],
                         "nix_label": "@@openssl_nix//:openssl_nix",
                         "nix_include": [
                              "bin/openssl"
                         ],
                         "nix_paths": [
                              "bin/openssl"
                         ]
                    },
                    "output_tree_hash": "bd878aa125019fddec930eaf23d4ada99f37e396fe5851ed075a326bea08f080"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository rules_haskell_sh_posix_nixpkgs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:150:29: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:412:31: in haskell_register_ghc_nixpkgs\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_posix/posix.bzl:163:28: in nixpkgs_sh_posix_configure\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_posix/posix.bzl:23:20: in nixpkgs_sh_posix_config\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_sh_posix_nixpkgs",
               "generator_name": "rules_haskell_sh_posix_nixpkgs",
               "generator_function": "haskell_register_ghc_nixpkgs",
               "generator_location": None,
               "attribute_path": "",
               "build_file_content": "\nload(\"//:nixpkgs_sh_posix.bzl\", \"create_posix_toolchain\")\ncreate_posix_toolchain()\n",
               "fail_not_supported": True,
               "nix_file_content": "\nwith import <nixpkgs> { config = {}; overlays = []; };\n\nlet\n  # `packages` might include lists, e.g. `stdenv.initialPath` is a list itself,\n  # so we need to flatten `packages`.\n  flatten = builtins.concatMap (x: if builtins.isList x then x else [x]);\n  env = buildEnv {\n    name = \"posix-toolchain\";\n    paths = flatten [ stdenv.initialPath ];\n  };\n  cmd_glob = \"${env}/bin/*\";\n  os = if stdenv.isDarwin then \"osx\" else \"linux\";\nin\n\nrunCommand \"bazel-nixpkgs-posix-toolchain\"\n  { executable = false;\n    # Pointless to do this on a remote machine.\n    preferLocalBuild = true;\n    allowSubstitutes = false;\n  }\n  ''\n    n=$out/nixpkgs_sh_posix.bzl\n    mkdir -p \"$(dirname \"$n\")\"\n\n    cat >>$n <<EOF\n    load(\"@rules_sh//sh:posix.bzl\", \"posix\", \"sh_posix_toolchain\")\n    discovered = {\n    EOF\n    for cmd in ${cmd_glob}; do\n        if [[ -x $cmd ]]; then\n            echo \"    '$(basename $cmd)': '$cmd',\" >>$n\n        fi\n    done\n    cat >>$n <<EOF\n    }\n    def create_posix_toolchain():\n        sh_posix_toolchain(\n            name = \"nixpkgs_sh_posix\",\n            cmds = {\n                cmd: discovered[cmd]\n                for cmd in posix.commands\n                if cmd in discovered\n            }\n        )\n    EOF\n  ''\n",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "rules_haskell_sh_posix_nixpkgs"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "rules_haskell_sh_posix_nixpkgs",
                         "generator_name": "rules_haskell_sh_posix_nixpkgs",
                         "generator_function": "haskell_register_ghc_nixpkgs",
                         "generator_location": None,
                         "attribute_path": "",
                         "build_file_content": "\nload(\"//:nixpkgs_sh_posix.bzl\", \"create_posix_toolchain\")\ncreate_posix_toolchain()\n",
                         "fail_not_supported": True,
                         "nix_file_content": "\nwith import <nixpkgs> { config = {}; overlays = []; };\n\nlet\n  # `packages` might include lists, e.g. `stdenv.initialPath` is a list itself,\n  # so we need to flatten `packages`.\n  flatten = builtins.concatMap (x: if builtins.isList x then x else [x]);\n  env = buildEnv {\n    name = \"posix-toolchain\";\n    paths = flatten [ stdenv.initialPath ];\n  };\n  cmd_glob = \"${env}/bin/*\";\n  os = if stdenv.isDarwin then \"osx\" else \"linux\";\nin\n\nrunCommand \"bazel-nixpkgs-posix-toolchain\"\n  { executable = false;\n    # Pointless to do this on a remote machine.\n    preferLocalBuild = true;\n    allowSubstitutes = false;\n  }\n  ''\n    n=$out/nixpkgs_sh_posix.bzl\n    mkdir -p \"$(dirname \"$n\")\"\n\n    cat >>$n <<EOF\n    load(\"@rules_sh//sh:posix.bzl\", \"posix\", \"sh_posix_toolchain\")\n    discovered = {\n    EOF\n    for cmd in ${cmd_glob}; do\n        if [[ -x $cmd ]]; then\n            echo \"    '$(basename $cmd)': '$cmd',\" >>$n\n        fi\n    done\n    cat >>$n <<EOF\n    }\n    def create_posix_toolchain():\n        sh_posix_toolchain(\n            name = \"nixpkgs_sh_posix\",\n            cmds = {\n                cmd: discovered[cmd]\n                for cmd in posix.commands\n                if cmd in discovered\n            }\n        )\n    EOF\n  ''\n",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "rules_haskell_sh_posix_nixpkgs"
                    },
                    "output_tree_hash": "8bda430a3491aa268ddb7eb481930a15536c1c8b4d50b4205137a600b0dc74e8"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository local_config_cc_info instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:112:21: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_cc/cc.bzl:466:20: in nixpkgs_cc_configure\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "local_config_cc_info",
               "generator_name": "local_config_cc_info",
               "generator_function": "nixpkgs_cc_configure",
               "generator_location": None,
               "attribute_path": "",
               "build_file_content": "exports_files(['CC_TOOLCHAIN_INFO'])",
               "fail_not_supported": True,
               "nix_file": "@@rules_nixpkgs_cc//:cc.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix",
                    "@@daml//nix:tools/bazel-cc-toolchain/default.nix": "@daml//nix:tools/bazel-cc-toolchain/default.nix",
                    "@@daml//nix:bazel-cc-toolchain.nix": "@daml//nix:bazel-cc-toolchain.nix"
               },
               "nixopts": [
                    "--argstr",
                    "ccType",
                    "ccTypeExpression",
                    "--arg",
                    "ccExpr",
                    "import $(location @daml//nix:bazel-cc-toolchain.nix)",
                    "--argstr",
                    "ccLang",
                    "c++",
                    "--argstr",
                    "ccStd",
                    "c++0x"
               ],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "local_config_cc_info"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "local_config_cc_info",
                         "generator_name": "local_config_cc_info",
                         "generator_function": "nixpkgs_cc_configure",
                         "generator_location": None,
                         "attribute_path": "",
                         "build_file_content": "exports_files(['CC_TOOLCHAIN_INFO'])",
                         "fail_not_supported": True,
                         "nix_file": "@@rules_nixpkgs_cc//:cc.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix",
                              "@@daml//nix:tools/bazel-cc-toolchain/default.nix": "@daml//nix:tools/bazel-cc-toolchain/default.nix",
                              "@@daml//nix:bazel-cc-toolchain.nix": "@daml//nix:bazel-cc-toolchain.nix"
                         },
                         "nixopts": [
                              "--argstr",
                              "ccType",
                              "ccTypeExpression",
                              "--arg",
                              "ccExpr",
                              "import $(location @daml//nix:bazel-cc-toolchain.nix)",
                              "--argstr",
                              "ccLang",
                              "c++",
                              "--argstr",
                              "ccStd",
                              "c++0x"
                         ],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "local_config_cc_info"
                    },
                    "output_tree_hash": "b2821a8a4a5c32ea4cbc6826374c310e0d321afcd02b33482f10f5d91c211e40"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_cc//:cc.bzl%_nixpkgs_cc_toolchain_config",
          "definition_information": "Repository local_config_cc instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:112:21: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_cc/cc.bzl:479:33: in nixpkgs_cc_configure\nRepository rule _nixpkgs_cc_toolchain_config defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_cc/cc.bzl:236:47: in <toplevel>\n",
          "original_attributes": {
               "name": "local_config_cc",
               "generator_name": "local_config_cc",
               "generator_function": "nixpkgs_cc_configure",
               "generator_location": None,
               "cc_toolchain_info": "@@local_config_cc_info//:CC_TOOLCHAIN_INFO",
               "fail_not_supported": True,
               "cross_cpu": ""
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_cc//:cc.bzl%_nixpkgs_cc_toolchain_config",
                    "attributes": {
                         "name": "local_config_cc",
                         "generator_name": "local_config_cc",
                         "generator_function": "nixpkgs_cc_configure",
                         "generator_location": None,
                         "cc_toolchain_info": "@@local_config_cc_info//:CC_TOOLCHAIN_INFO",
                         "fail_not_supported": True,
                         "cross_cpu": ""
                    },
                    "output_tree_hash": "7f7c7bf1dd5a10cc63bec5e0c9fc90674f093b68b6d40366e6d3e55580b94023"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/osx:xcode_configure.bzl%xcode_autoconf",
          "definition_information": "Repository local_config_xcode instantiated at:\n  /DEFAULT.WORKSPACE.SUFFIX:171:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/osx/xcode_configure.bzl:312:19: in xcode_configure\nRepository rule xcode_autoconf defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/osx/xcode_configure.bzl:297:33: in <toplevel>\n",
          "original_attributes": {
               "name": "local_config_xcode",
               "generator_name": "local_config_xcode",
               "generator_function": "xcode_configure",
               "generator_location": None,
               "xcode_locator": "@bazel_tools//tools/osx:xcode_locator.m"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/osx:xcode_configure.bzl%xcode_autoconf",
                    "attributes": {
                         "name": "local_config_xcode",
                         "generator_name": "local_config_xcode",
                         "generator_function": "xcode_configure",
                         "generator_location": None,
                         "xcode_locator": "@bazel_tools//tools/osx:xcode_locator.m"
                    },
                    "output_tree_hash": "ec026961157bb71cf68d1b568815ad68147ed16f318161bc0da40f6f3d7d79fd"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository com_github_madler_zlib instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:127:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "com_github_madler_zlib",
               "generator_name": "com_github_madler_zlib",
               "generator_function": "daml_deps",
               "generator_location": None,
               "urls": [
                    "https://github.com/madler/zlib/archive/v1.2.11.tar.gz"
               ],
               "sha256": "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff",
               "strip_prefix": "zlib-1.2.11",
               "build_file": "@@daml//3rdparty/c:zlib.BUILD"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/madler/zlib/archive/v1.2.11.tar.gz"
                         ],
                         "sha256": "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "zlib-1.2.11",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file": "@@daml//3rdparty/c:zlib.BUILD",
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "com_github_madler_zlib"
                    },
                    "output_tree_hash": "7698602af54c8af10792e4d0fc11103207cac0b9ae78b47528cbb66922c291e9"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository diffutils_nix instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:136:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "diffutils_nix",
               "generator_name": "diffutils_nix",
               "generator_function": "nixpkgs_package",
               "generator_location": None,
               "attribute_path": "diffutils",
               "build_file_content": "",
               "fail_not_supported": False,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "nixopts": [],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "diffutils_nix"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "diffutils_nix",
                         "generator_name": "diffutils_nix",
                         "generator_function": "nixpkgs_package",
                         "generator_location": None,
                         "attribute_path": "diffutils",
                         "build_file_content": "",
                         "fail_not_supported": False,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "diffutils_nix"
                    },
                    "output_tree_hash": "945043f66ccfdbcc8be3662b6432aa5f53f94903d5f9b081ff3a25f23eefbf3f"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository nixpkgs_python_toolchain_python3 instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:160:25: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_python/python.bzl:161:24: in nixpkgs_python_configure\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "nixpkgs_python_toolchain_python3",
               "generator_name": "nixpkgs_python_toolchain_python3",
               "generator_function": "nixpkgs_python_configure",
               "generator_location": None,
               "attribute_path": "",
               "build_file_content": "",
               "fail_not_supported": True,
               "nix_file_content": "\nwith import <nixpkgs> { config = {}; overlays = []; };\nlet\n  addShebang = true;\n  interpreterPath = \"${python3}/bin/python\";\n  shebangLine = interpreter: writers.makeScriptWriter { inherit interpreter; } \"shebang\" \"\";\nin\nrunCommand \"bazel-nixpkgs-python-toolchain\"\n  { executable = false;\n    # Pointless to do this on a remote machine.\n    preferLocalBuild = true;\n    allowSubstitutes = false;\n  }\n  ''\n    n=$out/BUILD.bazel\n    mkdir -p \"$(dirname \"$n\")/bin\"\n    ln -s ${interpreterPath} $out/bin/python\n\n    cat >>$n <<EOF\n    py_runtime(\n        name = \"runtime\",\n        interpreter_path = \"${interpreterPath}\",\n        python_version = \"PY3\",\n        ${lib.optionalString addShebang ''\n          stub_shebang = \"$(cat ${shebangLine interpreterPath})\",\n        ''}\n        visibility = [\"//visibility:public\"],\n    )\n    exports_files([\"bin/python\"])\n    EOF\n  ''\n",
               "nix_file_deps": {},
               "nixopts": [],
               "quiet": False,
               "repositories": {},
               "repository": "@@nixpkgs//:nixpkgs",
               "unmangled_name": "nixpkgs_python_toolchain_python3"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "nixpkgs_python_toolchain_python3",
                         "generator_name": "nixpkgs_python_toolchain_python3",
                         "generator_function": "nixpkgs_python_configure",
                         "generator_location": None,
                         "attribute_path": "",
                         "build_file_content": "",
                         "fail_not_supported": True,
                         "nix_file_content": "\nwith import <nixpkgs> { config = {}; overlays = []; };\nlet\n  addShebang = true;\n  interpreterPath = \"${python3}/bin/python\";\n  shebangLine = interpreter: writers.makeScriptWriter { inherit interpreter; } \"shebang\" \"\";\nin\nrunCommand \"bazel-nixpkgs-python-toolchain\"\n  { executable = false;\n    # Pointless to do this on a remote machine.\n    preferLocalBuild = true;\n    allowSubstitutes = false;\n  }\n  ''\n    n=$out/BUILD.bazel\n    mkdir -p \"$(dirname \"$n\")/bin\"\n    ln -s ${interpreterPath} $out/bin/python\n\n    cat >>$n <<EOF\n    py_runtime(\n        name = \"runtime\",\n        interpreter_path = \"${interpreterPath}\",\n        python_version = \"PY3\",\n        ${lib.optionalString addShebang ''\n          stub_shebang = \"$(cat ${shebangLine interpreterPath})\",\n        ''}\n        visibility = [\"//visibility:public\"],\n    )\n    exports_files([\"bin/python\"])\n    EOF\n  ''\n",
                         "nix_file_deps": {},
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {},
                         "repository": "@@nixpkgs//:nixpkgs",
                         "unmangled_name": "nixpkgs_python_toolchain_python3"
                    },
                    "output_tree_hash": "5ffeec53b5f5d4895a14ba0841dc17b9b869311427fdf7ffe2b188d6a328ff76"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository rules_haskell_ghc_nixpkgs instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:150:29: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:390:20: in haskell_register_ghc_nixpkgs\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_ghc_nixpkgs",
               "generator_name": "rules_haskell_ghc_nixpkgs",
               "generator_function": "haskell_register_ghc_nixpkgs",
               "generator_location": None,
               "attribute_path": "ghc",
               "build_file": "@@io_tweag_rules_nixpkgs//nixpkgs:BUILD.pkg",
               "fail_not_supported": True,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "rules_haskell_ghc_nixpkgs"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "rules_haskell_ghc_nixpkgs",
                         "generator_name": "rules_haskell_ghc_nixpkgs",
                         "generator_function": "haskell_register_ghc_nixpkgs",
                         "generator_location": None,
                         "attribute_path": "ghc",
                         "build_file": "@@io_tweag_rules_nixpkgs//nixpkgs:BUILD.pkg",
                         "fail_not_supported": True,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "rules_haskell_ghc_nixpkgs"
                    },
                    "output_tree_hash": "9012828d8a0a7d5071b53284ee6b718cd161509ebb6051d99e058eaf383c381d"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository postgresql_nix instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:202:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "postgresql_nix",
               "generator_name": "postgresql_nix",
               "generator_function": "nixpkgs_package",
               "generator_location": None,
               "attribute_path": "postgresql_12",
               "build_file_content": "\npackage(default_visibility = [\"//visibility:public\"])\nfilegroup(\n    name = \"all\",\n    srcs = glob([\"**\"]),\n)\n",
               "fail_not_supported": False,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "nixopts": [],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "postgresql_nix"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "postgresql_nix",
                         "generator_name": "postgresql_nix",
                         "generator_function": "nixpkgs_package",
                         "generator_location": None,
                         "attribute_path": "postgresql_12",
                         "build_file_content": "\npackage(default_visibility = [\"//visibility:public\"])\nfilegroup(\n    name = \"all\",\n    srcs = glob([\"**\"]),\n)\n",
                         "fail_not_supported": False,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "postgresql_nix"
                    },
                    "output_tree_hash": "f77110e67c109eadd948b147f93177f9c422754a129f37a01b69b146edc9cb3e"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_haskell//haskell:nixpkgs.bzl%_ghc_nixpkgs_haskell_toolchain",
          "definition_information": "Repository rules_haskell_ghc_nixpkgs_haskell_toolchain instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:150:29: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:418:38: in haskell_register_ghc_nixpkgs\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:272:35: in register_ghc_from_nixpkgs_package\nRepository rule _ghc_nixpkgs_haskell_toolchain defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/nixpkgs.bzl:123:49: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_haskell_ghc_nixpkgs_haskell_toolchain",
               "generator_name": "rules_haskell_ghc_nixpkgs_haskell_toolchain",
               "generator_function": "haskell_register_ghc_nixpkgs",
               "generator_location": None,
               "version": "9.0.2",
               "static_runtime": False,
               "fully_static_link": False,
               "locale_archive": "@glibc_locales//:locale-archive",
               "nixpkgs_ghc_repo_name": "rules_haskell_ghc_nixpkgs",
               "nixpkgs_ghc": "@@rules_haskell_ghc_nixpkgs//:bin/ghc"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_haskell//haskell:nixpkgs.bzl%_ghc_nixpkgs_haskell_toolchain",
                    "attributes": {
                         "name": "rules_haskell_ghc_nixpkgs_haskell_toolchain",
                         "generator_name": "rules_haskell_ghc_nixpkgs_haskell_toolchain",
                         "generator_function": "haskell_register_ghc_nixpkgs",
                         "generator_location": None,
                         "version": "9.0.2",
                         "static_runtime": False,
                         "fully_static_link": False,
                         "locale_archive": "@glibc_locales//:locale-archive",
                         "nixpkgs_ghc_repo_name": "rules_haskell_ghc_nixpkgs",
                         "nixpkgs_ghc": "@@rules_haskell_ghc_nixpkgs//:bin/ghc"
                    },
                    "output_tree_hash": "1b940c743295bfffd3e734a46a57fb6780b49821907bd698b23f05f0e95d3f92"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_proto instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:381:19: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_scala/scala/private/macros/scala_repositories.bzl:167:26: in scala_repositories\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_scala/scala/private/macros/scala_repositories.bzl:113:21: in rules_scala_setup\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_proto",
               "generator_name": "rules_proto",
               "generator_function": "scala_repositories",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
                    "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz"
               ],
               "sha256": "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
               "strip_prefix": "rules_proto-5.3.0-21.7"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
                              "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz"
                         ],
                         "sha256": "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_proto-5.3.0-21.7",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_proto"
                    },
                    "output_tree_hash": "9534782371e091f25cc063e2930a188d6ae96f4e2bcb64c0bd53d2ef16caff8b"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository glibc_locales instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:120:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "glibc_locales",
               "generator_name": "glibc_locales",
               "generator_function": "nixpkgs_package",
               "generator_location": None,
               "attribute_path": "glibcLocales",
               "build_file_content": "\npackage(default_visibility = [\"//visibility:public\"])\nfilegroup(\n    name = \"locale-archive\",\n    srcs = [\"lib/locale/locale-archive\"],\n)\n",
               "fail_not_supported": True,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "nixopts": [],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "glibc_locales"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "glibc_locales",
                         "generator_name": "glibc_locales",
                         "generator_function": "nixpkgs_package",
                         "generator_location": None,
                         "attribute_path": "glibcLocales",
                         "build_file_content": "\npackage(default_visibility = [\"//visibility:public\"])\nfilegroup(\n    name = \"locale-archive\",\n    srcs = [\"lib/locale/locale-archive\"],\n)\n",
                         "fail_not_supported": True,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "glibc_locales"
                    },
                    "output_tree_hash": "21a5411880d37652071a9f8f99edf2d5ace329bdb6c4967b0074e4ab207e668a"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository com_google_protobuf instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:121:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "com_google_protobuf",
               "generator_name": "com_google_protobuf",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz"
               ],
               "sha256": "22fdaf641b31655d4b2297f9981fa5203b2866f8332d3c6333f6b0107bb320de",
               "strip_prefix": "protobuf-21.12"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz"
                         ],
                         "sha256": "22fdaf641b31655d4b2297f9981fa5203b2866f8332d3c6333f6b0107bb320de",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "protobuf-21.12",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "com_google_protobuf"
                    },
                    "output_tree_hash": "e71629e44d891e1c628f678b848f7c6a33876708eb2a523db81780e39907ebd0"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_pkg instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:160:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_pkg",
               "generator_name": "rules_pkg",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
                    "https://github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz"
               ],
               "sha256": "d20c951960ed77cb7b341c2a59488534e494d5ad1d30c4818c736d57772a9fef"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz",
                              "https://github.com/bazelbuild/rules_pkg/releases/download/1.0.1/rules_pkg-1.0.1.tar.gz"
                         ],
                         "sha256": "d20c951960ed77cb7b341c2a59488534e494d5ad1d30c4818c736d57772a9fef",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_pkg"
                    },
                    "output_tree_hash": "b6b3fbe6552389be93f802344544406e8fd9cb2fdb4551563412a97d74c2ecd1"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository zlib instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:43:27: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/repositories.bzl:152:10: in rules_haskell_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/utils.bzl:240:18: in maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "zlib",
               "generator_name": "zlib",
               "generator_function": "rules_haskell_dependencies",
               "generator_location": None,
               "urls": [
                    "https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz"
               ],
               "sha256": "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23",
               "strip_prefix": "zlib-1.3.1",
               "build_file": "@@com_google_protobuf//:third_party/zlib.BUILD"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "",
                         "urls": [
                              "https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz"
                         ],
                         "sha256": "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "zlib-1.3.1",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file": "@@com_google_protobuf//:third_party/zlib.BUILD",
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "zlib"
                    },
                    "output_tree_hash": "506ce6905a7606bd9ad1dbed584a3ecf31c32b5262e0585a6556127e4e0809b4"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_haskell//haskell:cabal.bzl%_stack_snapshot",
          "definition_information": "Repository stackage instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:285:18: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/bazel-haskell-deps.bzl:23:19: in daml_haskell_deps\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/cabal.bzl:2685:20: in stack_snapshot\nRepository rule _stack_snapshot defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_haskell/haskell/cabal.bzl:2214:34: in <toplevel>\n",
          "original_attributes": {
               "name": "stackage",
               "generator_name": "stackage",
               "generator_function": "daml_haskell_deps",
               "generator_location": None,
               "unmangled_repo_name": "stackage",
               "stack_snapshot_json": "//:stackage_snapshot.json",
               "snapshot": "",
               "local_snapshot": "//:stack-snapshot.yaml",
               "packages": [
                    "aeson",
                    "aeson-extra",
                    "async",
                    "base",
                    "bytestring",
                    "conduit",
                    "conduit-extra",
                    "containers",
                    "cryptonite",
                    "directory",
                    "extra",
                    "filepath",
                    "http-client",
                    "http-conduit",
                    "jwt",
                    "lens",
                    "lens-aeson",
                    "memory",
                    "monad-loops",
                    "mtl",
                    "network",
                    "optparse-applicative",
                    "process",
                    "safe",
                    "safe-exceptions",
                    "semver",
                    "split",
                    "stm",
                    "tagged",
                    "tar-conduit",
                    "tasty",
                    "tasty-hunit",
                    "text",
                    "typed-process",
                    "unix-compat",
                    "unordered-containers",
                    "utf8-string",
                    "uuid"
               ],
               "vendored_packages": {},
               "flags": {},
               "haddock": False,
               "extra_deps": {
                    "@@com_github_madler_zlib//:libz": "zlib"
               },
               "setup_deps": {},
               "tools": [],
               "components": {
                    "attoparsec": [
                         "lib:attoparsec",
                         "lib:attoparsec-internal"
                    ]
               },
               "components_dependencies": {
                    "attoparsec": "{\"lib:attoparsec\": [\"lib:attoparsec-internal\"]}"
               },
               "stack": "@@rules_haskell_stack//:stack",
               "stack_update": "@rules_haskell_stack_update//:stack_update",
               "verbose": False,
               "enable_custom_toolchain_libraries": False
          },
          "repositories": [
               {
                    "rule_class": "@@rules_haskell//haskell:cabal.bzl%_stack_snapshot",
                    "attributes": {
                         "name": "stackage",
                         "generator_name": "stackage",
                         "generator_function": "daml_haskell_deps",
                         "generator_location": None,
                         "unmangled_repo_name": "stackage",
                         "stack_snapshot_json": "//:stackage_snapshot.json",
                         "snapshot": "",
                         "local_snapshot": "//:stack-snapshot.yaml",
                         "packages": [
                              "aeson",
                              "aeson-extra",
                              "async",
                              "base",
                              "bytestring",
                              "conduit",
                              "conduit-extra",
                              "containers",
                              "cryptonite",
                              "directory",
                              "extra",
                              "filepath",
                              "http-client",
                              "http-conduit",
                              "jwt",
                              "lens",
                              "lens-aeson",
                              "memory",
                              "monad-loops",
                              "mtl",
                              "network",
                              "optparse-applicative",
                              "process",
                              "safe",
                              "safe-exceptions",
                              "semver",
                              "split",
                              "stm",
                              "tagged",
                              "tar-conduit",
                              "tasty",
                              "tasty-hunit",
                              "text",
                              "typed-process",
                              "unix-compat",
                              "unordered-containers",
                              "utf8-string",
                              "uuid"
                         ],
                         "vendored_packages": {},
                         "flags": {},
                         "haddock": False,
                         "extra_deps": {
                              "@@com_github_madler_zlib//:libz": "zlib"
                         },
                         "setup_deps": {},
                         "tools": [],
                         "components": {
                              "attoparsec": [
                                   "lib:attoparsec",
                                   "lib:attoparsec-internal"
                              ]
                         },
                         "components_dependencies": {
                              "attoparsec": "{\"lib:attoparsec\": [\"lib:attoparsec-internal\"]}"
                         },
                         "stack": "@@rules_haskell_stack//:stack",
                         "stack_update": "@rules_haskell_stack_update//:stack_update",
                         "verbose": False,
                         "enable_custom_toolchain_libraries": False
                    },
                    "output_tree_hash": "1efe3c9d3d3b3959dc341971366491033a285ee27d1a4d72fc1b3f81104cc44a"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_gazelle//internal:is_bazel_module.bzl%is_bazel_module",
          "definition_information": "Repository bazel_gazelle_is_bazel_module instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:51:21: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_gazelle/deps.bzl:115:20: in gazelle_dependencies\nRepository rule is_bazel_module defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_gazelle/internal/is_bazel_module.bzl:30:34: in <toplevel>\n",
          "original_attributes": {
               "name": "bazel_gazelle_is_bazel_module",
               "generator_name": "bazel_gazelle_is_bazel_module",
               "generator_function": "gazelle_dependencies",
               "generator_location": None,
               "is_bazel_module": False
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_gazelle//internal:is_bazel_module.bzl%is_bazel_module",
                    "attributes": {
                         "name": "bazel_gazelle_is_bazel_module",
                         "generator_name": "bazel_gazelle_is_bazel_module",
                         "generator_function": "gazelle_dependencies",
                         "generator_location": None,
                         "is_bazel_module": False
                    },
                    "output_tree_hash": "dda7e5d5aa9d766c3d9c95f78a31564192edad9cc7e5448bcaeb039f33b87c8f"
               }
          ]
     },
     {
          "original_rule_class": "@@daml//bazel_tools:build_environment.bzl%build_environment",
          "definition_information": "Repository build_environment instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:20:18: in <toplevel>\nRepository rule build_environment defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/daml/bazel_tools/build_environment.bzl:36:36: in <toplevel>\n",
          "original_attributes": {
               "name": "build_environment"
          },
          "repositories": [
               {
                    "rule_class": "@@daml//bazel_tools:build_environment.bzl%build_environment",
                    "attributes": {
                         "name": "build_environment"
                    },
                    "output_tree_hash": "6917b396915e2c3feef18ed4708c66488b9b724e79b3f5a906807b2a5339df60"
               }
          ]
     },
     {
          "original_rule_class": "@@io_bazel_rules_go//go/private:nogo.bzl%go_register_nogo",
          "definition_information": "Repository io_bazel_rules_nogo instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:47:22: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:311:12: in go_rules_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:343:18: in _maybe\nRepository rule go_register_nogo defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/nogo.bzl:54:35: in <toplevel>\n",
          "original_attributes": {
               "name": "io_bazel_rules_nogo",
               "generator_name": "io_bazel_rules_nogo",
               "generator_function": "go_rules_dependencies",
               "generator_location": None,
               "nogo": "@io_bazel_rules_go//:default_nogo"
          },
          "repositories": [
               {
                    "rule_class": "@@io_bazel_rules_go//go/private:nogo.bzl%go_register_nogo",
                    "attributes": {
                         "name": "io_bazel_rules_nogo",
                         "generator_name": "io_bazel_rules_nogo",
                         "generator_function": "go_rules_dependencies",
                         "generator_location": None,
                         "nogo": "@io_bazel_rules_go//:default_nogo"
                    },
                    "output_tree_hash": "b414d771116ea38248c456980d0530fe9f067be3b2788ab130fe8d828a181379"
               }
          ]
     },
     {
          "original_rule_class": "@@io_bazel_rules_go//go/private:polyfill_bazel_features.bzl%polyfill_bazel_features",
          "definition_information": "Repository io_bazel_rules_go_bazel_features instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:47:22: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:317:11: in go_rules_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:343:18: in _maybe\nRepository rule polyfill_bazel_features defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/polyfill_bazel_features.bzl:42:42: in <toplevel>\n",
          "original_attributes": {
               "name": "io_bazel_rules_go_bazel_features",
               "generator_name": "io_bazel_rules_go_bazel_features",
               "generator_function": "go_rules_dependencies",
               "generator_location": None
          },
          "repositories": [
               {
                    "rule_class": "@@io_bazel_rules_go//go/private:polyfill_bazel_features.bzl%polyfill_bazel_features",
                    "attributes": {
                         "name": "io_bazel_rules_go_bazel_features",
                         "generator_name": "io_bazel_rules_go_bazel_features",
                         "generator_function": "go_rules_dependencies",
                         "generator_location": None
                    },
                    "output_tree_hash": "6155013dadfc43721a75bda2302470359ed5f14d26007b34ff6749ed298cb521"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository rules_shell instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:47:22: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:327:12: in go_rules_dependencies\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/io_bazel_rules_go/go/private/repositories.bzl:343:18: in _maybe\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "rules_shell",
               "generator_name": "rules_shell",
               "generator_function": "go_rules_dependencies",
               "generator_location": None,
               "url": "https://github.com/bazelbuild/rules_shell/releases/download/v0.3.0/rules_shell-v0.3.0.tar.gz",
               "sha256": "d8cd4a3a91fc1dc68d4c7d6b655f09def109f7186437e3f50a9b60ab436a0c53",
               "strip_prefix": "rules_shell-0.3.0"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazelbuild/rules_shell/releases/download/v0.3.0/rules_shell-v0.3.0.tar.gz",
                         "urls": [],
                         "sha256": "d8cd4a3a91fc1dc68d4c7d6b655f09def109f7186437e3f50a9b60ab436a0c53",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "rules_shell-0.3.0",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "rules_shell"
                    },
                    "output_tree_hash": "99e2d7bd9e5675fa50a3472f228a03d3a4b78673274d0b82c7cd29d1cf636ebc"
               }
          ]
     },
     {
          "original_rule_class": "//bazel_tools:daml_sdk.bzl%_daml_sdk",
          "definition_information": "Repository daml-sdk-0.0.0 instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:294:14: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/bazel_tools/daml_sdk.bzl:156:14: in daml_sdk_head\nRepository rule _daml_sdk defined at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/bazel_tools/daml_sdk.bzl:131:28: in <toplevel>\n",
          "original_attributes": {
               "name": "daml-sdk-0.0.0",
               "generator_name": "daml-sdk-0.0.0",
               "generator_function": "daml_sdk_head",
               "generator_location": None,
               "version": "0.0.0",
               "os_name": "linux",
               "sdk_tarball": "@@head_sdk//:sdk-release-tarball-ce.tar.gz",
               "daml_types_tarball": "@@head_sdk//:daml-types-0.0.0.tgz",
               "daml_ledger_tarball": "@@head_sdk//:daml-ledger-0.0.0.tgz",
               "daml_react_tarball": "@@head_sdk//:daml-react-0.0.0.tgz"
          },
          "repositories": [
               {
                    "rule_class": "//bazel_tools:daml_sdk.bzl%_daml_sdk",
                    "attributes": {
                         "name": "daml-sdk-0.0.0",
                         "generator_name": "daml-sdk-0.0.0",
                         "generator_function": "daml_sdk_head",
                         "generator_location": None,
                         "version": "0.0.0",
                         "os_name": "linux",
                         "sdk_tarball": "@@head_sdk//:sdk-release-tarball-ce.tar.gz",
                         "daml_types_tarball": "@@head_sdk//:daml-types-0.0.0.tgz",
                         "daml_ledger_tarball": "@@head_sdk//:daml-ledger-0.0.0.tgz",
                         "daml_react_tarball": "@@head_sdk//:daml-react-0.0.0.tgz"
                    },
                    "output_tree_hash": "8a658aa2b89f02b8985299d7c13445af5c85f5ce3bdcf45115b363f562d7c1fc"
               }
          ]
     },
     {
          "original_rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
          "definition_information": "Repository hlint_nix instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:241:16: in <toplevel>\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:726:21: in nixpkgs_package\nRepository rule _nixpkgs_package defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/rules_nixpkgs_core/nixpkgs.bzl:594:35: in <toplevel>\n",
          "original_attributes": {
               "name": "hlint_nix",
               "generator_name": "hlint_nix",
               "generator_function": "nixpkgs_package",
               "generator_location": None,
               "attribute_path": "hlint",
               "build_file_content": "",
               "fail_not_supported": True,
               "nix_file": "@@daml//nix:bazel.nix",
               "nix_file_content": "",
               "nix_file_deps": {
                    "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                    "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                    "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                    "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                    "@@daml//nix:system.nix": "@daml//nix:system.nix"
               },
               "nixopts": [],
               "quiet": False,
               "repositories": {
                    "@@nixpkgs//:nixpkgs": "nixpkgs"
               },
               "unmangled_name": "hlint_nix"
          },
          "repositories": [
               {
                    "rule_class": "@@rules_nixpkgs_core//:nixpkgs.bzl%_nixpkgs_package",
                    "attributes": {
                         "name": "hlint_nix",
                         "generator_name": "hlint_nix",
                         "generator_function": "nixpkgs_package",
                         "generator_location": None,
                         "attribute_path": "hlint",
                         "build_file_content": "",
                         "fail_not_supported": True,
                         "nix_file": "@@daml//nix:bazel.nix",
                         "nix_file_content": "",
                         "nix_file_deps": {
                              "@@daml//nix:bazel.nix": "@daml//nix:bazel.nix",
                              "@@daml//nix:nixpkgs.nix": "@daml//nix:nixpkgs.nix",
                              "@@daml//nix:nixpkgs/default.nix": "@daml//nix:nixpkgs/default.nix",
                              "@@daml//nix:nixpkgs/default.src.json": "@daml//nix:nixpkgs/default.src.json",
                              "@@daml//nix:system.nix": "@daml//nix:system.nix"
                         },
                         "nixopts": [],
                         "quiet": False,
                         "repositories": {
                              "@@nixpkgs//:nixpkgs": "nixpkgs"
                         },
                         "unmangled_name": "hlint_nix"
                    },
                    "output_tree_hash": "c66d8e7ab27ef2f1c0c14478c75ce647819077149162adcbf7ebff603d708bec"
               }
          ]
     },
     {
          "original_rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
          "definition_information": "Repository com_github_bazelbuild_buildtools instantiated at:\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/WORKSPACE:39:10: in <toplevel>\n  /home/aj/tweag.io/da/da-bzlmod/sdk/compatibility/deps.bzl:119:21: in daml_deps\nRepository rule http_archive defined at:\n  /home/aj/.cache/bazel/_bazel_aj/0bd0eec9af2fe50baf63942d0c3002b7/external/bazel_tools/tools/build_defs/repo/http.bzl:369:31: in <toplevel>\n",
          "original_attributes": {
               "name": "com_github_bazelbuild_buildtools",
               "generator_name": "com_github_bazelbuild_buildtools",
               "generator_function": "daml_deps",
               "generator_location": None,
               "url": "https://github.com/bazelbuild/buildtools/archive/b163fcf72b7def638f364ed129c9b28032c1d39b.tar.gz",
               "sha256": "c2399161fa569f7c815f8e27634035557a2e07a557996df579412ac73bf52c23",
               "strip_prefix": "buildtools-b163fcf72b7def638f364ed129c9b28032c1d39b"
          },
          "repositories": [
               {
                    "rule_class": "@@bazel_tools//tools/build_defs/repo:http.bzl%http_archive",
                    "attributes": {
                         "url": "https://github.com/bazelbuild/buildtools/archive/b163fcf72b7def638f364ed129c9b28032c1d39b.tar.gz",
                         "urls": [],
                         "sha256": "c2399161fa569f7c815f8e27634035557a2e07a557996df579412ac73bf52c23",
                         "integrity": "",
                         "netrc": "",
                         "auth_patterns": {},
                         "canonical_id": "",
                         "strip_prefix": "buildtools-b163fcf72b7def638f364ed129c9b28032c1d39b",
                         "add_prefix": "",
                         "type": "",
                         "patches": [],
                         "remote_patches": {},
                         "remote_patch_strip": 0,
                         "patch_tool": "",
                         "patch_args": [
                              "-p0"
                         ],
                         "patch_cmds": [],
                         "patch_cmds_win": [],
                         "build_file_content": "",
                         "workspace_file_content": "",
                         "name": "com_github_bazelbuild_buildtools"
                    },
                    "output_tree_hash": "79b727beffbb62cff4ff4751c5c5d5177c635ed51607b8e0e39483ad1330ef35"
               }
          ]
     }
]