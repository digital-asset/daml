// TODO: all these functions should be part of a rust/protobuf DSL?
use crate::protobuf::value as lf;
use protobuf::Message;

pub fn get_field(mut r: lf::Value, f: usize) -> lf::Value {
  if r.has_record() {
    return r.take_record().fields[f].value.clone().unwrap();
  } else {
    panic!();
  }
}

#[allow(non_snake_case)]
pub unsafe fn to_Value(ptr: *const u8, size: usize) -> lf::Value {
  return lf::Value::parse_from_bytes(std::slice::from_raw_parts(ptr, size)).unwrap();
}
