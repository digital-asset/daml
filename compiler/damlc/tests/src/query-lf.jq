def get_int64(pkg): .int64;

def resolve_interned_string(pkg): pkg.interned_strings[.];

def resolve_interned_dname(pkg): pkg.interned_dotted_names[.] | .segments_interned_str | map(resolve_interned_string(pkg));

def get_value_name(pkg): .name_interned_dname | resolve_interned_dname(pkg);

def get_template_name(pkg): .tycon_interned_dname | resolve_interned_dname(pkg);

def get_module_name(pkg): .module.module_name_interned_dname // 0 | resolve_interned_dname(pkg);

def get_dotted_name(pkg): .name_interned_dname | resolve_interned_dname(pkg);

def get_field(pkg): .field_interned_str | resolve_interned_string(pkg);

def get_name(pkg): .name_interned_str | resolve_interned_string(pkg);

def get_text(pkg): .text_interned_str | resolve_interned_string(pkg);

def norm_ty(pkg): if has("interned") then pkg.interned_types[.interned] else . end;

# @SINCE-LF 1.7
def norm_qualified_module(pkg; f):
    .type | norm_ty(pkg) | .struct.fields |
    map(.type | norm_ty(pkg)) |
    { "package":
        .[0] |
        (try (.struct.fields[0] | get_field(pkg)) catch null)
    , "module":
        .[1] |
        .struct.fields |
        map (
            .type | norm_ty(pkg) | .struct.fields[] |
            get_field(pkg)
        )
    } + f;

def norm_occ_name_0(pkg):
    .struct.fields |
    map(.type | norm_ty(pkg) | .struct.fields[0] | get_field(pkg)) |
    { namespace: .[0], value: .[1]};

def norm_occ_name(pkg):
    .type | norm_ty(pkg) | norm_occ_name_0(pkg);

# @SINCE-LF 1.7
def norm_imports(pkg):
    .type | norm_ty(pkg) | .struct.fields |
    map(norm_qualified_module(pkg; {}));

# @SINCE-LF 1.7
def norm_qualified_module_name(pkg):
    norm_qualified_module(pkg;
        { name: .[2] | norm_occ_name_0(pkg) }
    );

# @SINCE-LF 1.7
def norm_field_label(pkg):
    .type | norm_ty(pkg) | .struct.fields |
    { label:
        .[0] |
        .type | norm_ty(pkg) | .struct.fields[0] |
        get_field(pkg)
    , is_overloaded:
        .[1] |
        .type | norm_ty(pkg) | .struct.fields[0] |
        get_field(pkg) | (. == "True")
    , selector:
        .[2] |
        norm_qualified_module_name(pkg)
    };

# @SINCE-LF 1.7
def norm_export_info(pkg):
    .type | norm_ty(pkg) | .struct.fields[0] |
    { (get_field(pkg)):
        .type | norm_ty(pkg) | .struct.fields |
        { name:
            .[0] |
            norm_qualified_module_name(pkg)
        , pieces:
            .[1] |
            (try (.type | norm_ty(pkg) | .struct.fields |
            map(norm_qualified_module_name(pkg))) catch null)
        , fields:
            .[2] |
            (try (.type | norm_ty(pkg) | .struct.fields |
            map(norm_field_label(pkg))) catch null)
        }
    };

def norm_fixity(pkg):
    .type | norm_ty(pkg) | .struct.fields |
    map(.type | norm_ty(pkg) | .struct.fields[0] | get_field(pkg)) |
    { precedence: .[0] | ltrimstr("_") | tonumber
    , direction: .[1]
    };

def norm_fixity_info(pkg):
    .type | norm_ty(pkg) | .struct.fields |
    { "name": .[0] | norm_occ_name(pkg)
    , "fixity": .[1] | norm_fixity(pkg)
    };

# Stream utils

# Gets the value at a field, but lazily (will work for deep packages). Returns a non-stream.
def top_level_field_stream($field): fromstream(1|truncate_stream(inputs | select(.[0][0] == $field)));

def interned_strings_stream: top_level_field_stream("interned_strings");

def interned_dotted_names_stream: top_level_field_stream("interned_dotted_names");
