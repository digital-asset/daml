/Range:/ {
  s/^ *Range: */range=/
  s/$/;/
  h
}
/Severity:/ {
  s/^ *Severity: *Ds//
  s/Warning/Warn/
  y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/
  s/^/-- @/
  G
  h
}
/Message:/ {
  n
  s/^ *//
  H
  g
  s/\n/ /g
  p
}
