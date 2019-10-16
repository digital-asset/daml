def get_value_name(pkg): pkg.interned_dotted_names[.name_interned_id] | .segment_ids | map(pkg.interned_strings[.]);

def get_int64(pkg): .int64;

def get_dotted_name(pkg): pkg.interned_dotted_names[.segments_interned_id // 0] | .segment_ids | map(pkg.interned_strings[.]);

def get_field(pkg): pkg.interned_strings[.interned_id];

def get_name(pkg): .name;
