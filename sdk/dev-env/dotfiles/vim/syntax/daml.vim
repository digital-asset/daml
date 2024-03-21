" Vim syntax file
" Language:     DA Contract Specification Language Syntax
" Maintainer:
" Last Change:  2015 10 08

" Quit when a syntax file was already loaded
if exists("b:current_syntax")
  finish
endif

syn match clDelimiter  "{|\||}\|{@\|@}\|(\|)\|\[\|\]\|,\|;\|\<_\>\|{\|}\|\."

syn region clLiteral      start="\"" end="\"" contains=clPlaceholder
syn region clParty        start="'" end="'"
syn match clPlaceholder   "{{\w*}}" contained

syn keyword clDeclaration template after agree agrees as at await choose chooses commit commits else ensure exercises identified if import in let match module must on scenario such that then until update whenever where with qualified
syn keyword clFunction pure assert create createTransient delete assertIsActiveAt mustFailAt fromRelTime toRelTime fromInteger toInteger pass toText divD remD round

syn keyword clAnnotation DESC

syn match clTypeMatch     "\<[A-Z]\w\+"

syn match clAtom          "*"
syn match clAtom          "+"
syn match clAtom          "-"
syn match clAtom          "<"
syn match clAtom          "<="
syn match clAtom          ">"
syn match clAtom          ">="
syn match clAtom          "="
syn match clAtom          "\~"

syn match clNumber        "[0-9]\+"

syn match clTypeConnector "->"
syn match clBinder        "<-"
syn match clTypeDefinition "::"
syn match clDivider        ":"
syn match clLogicOp       "||"
syn match clLogicOp       "&&"

syn match clVariable    "\l\w*"

" Comments
syn keyword clTodo        contained TODO FIXME XXX
syn keyword clFixMe       contained FIXME
syn region  clComment     start="{-" end="-}" contains=clTodo,clFixme
syn match   clLineComment "--.*" contains=clTodo,clFixMe


hi def link clDelimiter      Delimiter

hi def link clAnnotation     Special
hi def link clDeclaration    Keyword
hi def link clFunction       Function
hi def link clQuantifier     Constant
hi def link clSays           Statement
hi def link clSort           Constant
hi def link clTypeMatch      Type
hi def link clTypeConnector  Constant
hi def link clBinder         Constant
hi def link clTypeDefinition Statement
hi def link clDivider        Statement
hi def link clConstant       Identifier
hi def link clPlaceholder    Special
hi def link clNumber         Number
hi def link clParty          Constant

hi def link clAtom           Constant
hi def link clLogicOp        Constant

hi def link clTodo           Todo
hi def link clFixMe          Error

hi def link clSortConnector  Type
hi def link clPredicates     Type
hi def link clComment        Comment
hi def link clLineComment    Comment

hi def link clVariable       Identifier

let b:current_syntax="daml"
let b:spell_options="contained"

" vim: ts=8
