# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import subprocess
import os
import sys
from dataclasses import dataclass

from python.runfiles import runfiles

filetypes = \
  { 'bat'    : { 'start' : '::'     , 'line' : '::'   , 'end' : ''    , 'empty_line' : '' } ,
    'bats'   : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '', 'skip' : '#!' } ,
    'bazel'  : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '', 'skip': '#!' } ,
    'bzl'    : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '', 'skip': '#!' } ,
    'c'      : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'cc'     : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'chs'    : { 'start' : '-' * 80 , 'line' : '--'   , 'end' : ''    , 'empty_line' : '' } ,
    'cmd'    : { 'start' : '::'     , 'line' : '::'   , 'end' : ''    , 'empty_line' : '' } ,
    'cpp'    : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'css'    : { 'start' : '/*'     , 'line' : ''     , 'end' : ' */' } ,
    'daml'   : { 'start' : '-' * 80 , 'line' : '--'   , 'end' : ''    , 'empty_line' : '' } ,
    'groovy' : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'h'      : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'hh'     : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'hpp'    : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'hs'     : { 'start' : '-' * 80 , 'line' : '--'   , 'end' : ''    , 'empty_line' : '', 'skip' : '#!'  } ,
    'html'   : { 'start' : '<!--'   , 'line' : ''     , 'end' : '-->' , 'skip' : '<!' } ,
    'java'   : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'js'     : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'lf'     : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'lhs'    : { 'start' : '-' * 80 , 'line' : '--'   , 'end' : ''    , 'empty_line' : '' } ,
    'proto'  : { 'start' : '/*'     , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'py'     : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '', 'skip' : '#!' } ,
    'rb'     : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '' } ,
    'rst'    : { 'start' : ''       , 'line' : '..'   , 'end' : ''    , 'empty_line' : '' } ,
    'scala'  : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'scss'   : { 'start' : '/*'     , 'line' : ''     , 'end' : ' */' } ,
    'sh'     : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '', 'skip' : '#!' } ,
    'sql'    : { 'start' : '/*'     , 'line' : '--'   , 'end' : ''    , 'empty_line' : '' } ,
    'tex'    : { 'start' : '%' * 80 , 'line' : '%'    , 'end' : ''    , 'empty_line' : '' } ,
    'tf'     : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '' } ,
    'ts'     : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'tsx'    : { 'start' : ''       , 'line' : '//'   , 'end' : ''    , 'empty_line' : '' } ,
    'yaml'   : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '' } ,
    'yml'    : { 'start' : '#' * 80 , 'line' : '#'    , 'end' : ''    , 'empty_line' : '' } ,
  }

tool_generated_basenames = { 'pnpm-lock.yaml' }

update_command = "bazelisk run //bazel/checks/copyright -- update"

git = os.environ.get("GIT", "git")

section_titles = {
  'missing'   : "Missing header",
  'outdated'  : "Out-of-date header",
  'malformed' : "Malformed header",
}

@dataclass
class HeaderProblem:
  filename: str
  line: int
  kind: str
  found: str = ""

def extension(filename):
  return os.path.splitext(filename)[1][1:]

def workspace_directory():
  if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
    return os.environ["BUILD_WORKSPACE_DIRECTORY"]
  module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
  return os.path.dirname(os.path.realpath(module_file))

def list_files(dir):
  if dir is None:
    dir = "."

  all_files = subprocess                       \
    .check_output([git, "ls-files", dir], universal_newlines=True)    \
    .strip()                                   \
    .split("\n")

  ignores = [ os.path.dirname(f) + "/" for f in all_files
              if os.path.basename(f) == 'NO_AUTO_COPYRIGHT' ]

  unignored = [ f for f in all_files
                if not any(map(lambda x: f.startswith(x), ignores))  ]

  return [ f for f in unignored
           if extension(f) in filetypes
           and os.path.isfile(f)
           and not os.path.islink(f)
           and os.path.basename(f) not in tool_generated_basenames ]

def process_file(filename, check_only):
  ext = extension(filename)

  line_mark = filetypes[ext]['line']
  empty_line_mark = filetypes[ext].get('empty_line', line_mark)
  end_mark = filetypes[ext]['end'] + "\n"

  def comment(line):
    if line == '':
      return empty_line_mark
    else:
      if line_mark == '':
          return filetypes[ext]['start'] + ' ' + line + ' ' + filetypes[ext]['end']
      else:
          return line_mark + ' ' + line

  notice_lines = list(map(comment, LEGAL_NOTICES))
  suffix="\n"
  header = suffix.join(notice_lines) + "\n"

  with open(filename, 'r', encoding="UTF-8") as fp:
    lines = fp.readlines()
  lines.append('\n')

  state='start'
  index=0
  before_header=[]
  after_header=[]
  problem = HeaderProblem(filename, 1, 'missing')
  for line_number, line in enumerate(lines, start=1):
    if state == 'start':
      if (line.strip(filetypes[ext]['line']).strip(filetypes[ext]['start'])[1:14] == "Copyright (c)"):
        state='notice'

        if line != notice_lines[0]+"\n":
            problem = HeaderProblem(filename, line_number, 'outdated', line.strip())
            state='find_end_and_fail'
        else:
            index += 1

      else:
        before_header.append(line)

    elif state == 'notice':
      if (line != notice_lines[1] + "\n"):
        problem = HeaderProblem(filename, line_number, 'outdated', line.strip())
        state='fail'

      else:
        index += 1
        state='compare'

    elif state == 'compare':
      if index == len(notice_lines):
        if line != end_mark and not line.endswith(end_mark) and line != empty_line_mark + "\n":
          problem = HeaderProblem(filename, line_number, 'malformed', line.strip())
          state='fail'
        else:
          state='ok'

      elif line != notice_lines[index]+"\n":
        problem = HeaderProblem(filename, line_number, 'outdated', line.strip())
        state='find_end_and_fail'

      else:
        index += 1

    elif state == 'find_end_and_fail':
      if not line.startswith(line_mark) \
              and not line.startswith(' *') \
              and not line.startswith('    ') \
              or line == end_mark\
              or line.endswith(end_mark)\
              or line == empty_line_mark + "\n":
        state = 'fail'

    elif state == 'fail' or state == 'ok':
      after_header.append(line)

    else:
      sys.exit("abort: unknown state %s" % state)

  if state == 'ok':
    return None

  if not check_only:
    if state == 'start':
      before_header.pop()
    header_separator = "\n" if before_header else ""
    with open(filename, 'w') as fp:
      if state == 'fail':
        fp.write("".join(before_header))
        fp.write(header)
        fp.write("".join(after_header[:-1]))
      elif 'skip' in filetypes[ext]:
        skip_prefix = filetypes[ext]['skip']
        index=0
        while index < len(before_header):
          line = before_header[index]
          if line.startswith(skip_prefix):
            fp.write(line)
            index += 1
          else:
            break
        fp.write(header + header_separator)
        while index < len(before_header):
          fp.write(before_header[index])
          index += 1
      else:
        fp.write(header + header_separator)
        fp.write("".join(before_header))
  return problem

def print_section(title, problems):
  print(f"{title} ({len(problems)}):")
  for problem in sorted(problems, key=lambda p: (p.filename, p.line)):
    location = f"{problem.filename}:{problem.line}"
    if problem.found:
      print(f"  {location}: found '{problem.found}'")
    else:
      print(f"  {location}")
  print()

def report(problems, checked_count, check_only):
  if not problems:
    print(f"Copyright headers are correct in all {checked_count} checked files.")
    return

  if check_only:
    print(f"Copyright header check failed for {len(problems)} of {checked_count} checked files.")
  else:
    print(f"Fixed copyright headers in {len(problems)} of {checked_count} checked files.")
  print()

  for kind, title in section_titles.items():
    selected = [p for p in problems if p.kind == kind]
    if selected:
      print_section(title, selected)

  if check_only:
    print("Expected header:")
    for notice in LEGAL_NOTICES:
      print(f"  {notice}")
    print()
    print("To fix them, run:")
    print(update_command)

def usage():
  print("usage: copyright-headers <check|update> [DIRECTORY]")
  print("  If DIRECTORY is provided, checks only in that directory")
  print("  Otherwise, checks the whole sdk directory.")
  print("  Note that the provided directory must be relative to the")
  print("  sdk directory of the git repository.")
  sys.exit(1)

os.chdir(workspace_directory())
with open('COPY', 'r') as f:
  LEGAL_NOTICES = list(map(lambda x: x.strip(), f.readlines()))

if len(sys.argv) != 2 and len(sys.argv) != 3:
  usage()

if sys.argv[1] == "update":
  check_only = False
elif sys.argv[1] == "check":
  check_only = True
else:
  usage()

dir = None
if len(sys.argv) > 2:
  dir = sys.argv[2]

files = list_files(dir)
problems = [ p for p in (process_file(f, check_only) for f in files) if p ]
report(problems, len(files), check_only)

sys.exit(1 if check_only and problems else 0)
