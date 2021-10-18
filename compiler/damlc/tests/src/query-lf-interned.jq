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

# @SINCE-LF 1.7
def norm_imports(pkg):
    .type | norm_ty(pkg) | .struct.fields |
    map(norm_qualified_module(pkg; {}));

# @SINCE-LF 1.7
def norm_qualified_module_name(pkg):
    norm_qualified_module(pkg;
        .[2].struct.fields |
        map(.type | norm_ty(pkg) | .struct.fields[0] | get_field(pkg)) |
        { name: { namespace: .[0], value: .[1]}}
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
