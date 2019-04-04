" Vim syntax file
" Language:     DA Authorization Logic Program Syntax
" Maintainer:
" Last Change:  2015 10 08

" Quit when a syntax file was already loaded
if exists("b:current_syntax")
  finish
endif

syn match alDelimiter  "(\|)\|\[\|\]\|,\|;\|_\|{\|}\|\."

syn region alLiteral      start="\"" end="\""


syn keyword alDeclaration query language: sorts: predicates: constructors: definitions:
syn keyword alQuantifier  forall exists not
syn keyword alSort        time form prin
syn keyword alSays        says

syn match alSortMatch     ":\w\+"ms=s+1
syn match alSortMatch     "\w\+ ->"me=e-3

syn match alAtom          ":="
syn match alAtom          "="
syn match alAtom          "@"
syn match alAtom          "<"
syn match alAtom          ">"

syn match alSortConnector "->"
syn match alLogicOp       "==>"
syn match alLogicOp       "|"
syn match alLogicOp       "&"

syn match alPredicates    "[a-z-_]\+/[a-z-_/]*"

syn match alVariable      "\<[A-Z-_][A-Z0-9-_]*\>"
syn match alVariable      "\<[A-Z-_][A-Z0-9-_]*\W"me=e-1
syn match alVariable      "'[A-Z-_][A-Z0-9-_]*'"

syn match alConstant      "\<infty\>"
syn match alConstant      "\W-infty\>"ms=s+1

syn region  alBlock       start="\[" end="\]" contains=alVariable, alConstant

" Comments
syn keyword alTodo        contained TODO FIXME XXX
syn keyword alFixMe       contained FIXME
syn region  alComment     start="/\*" end="\*/" contains=alTodo,alFixme
syn match   alLineComment "\/\/.*" contains=alTodo,alFixMe


hi def link alBlock         PreProc

hi def link alLiteral       String
hi def link alDelimiter     Delimiter

hi def link alDeclaration   Keyword
hi def link alQuantifier    Constant
hi def link alSays          Statement
hi def link alSort          Constant
hi def link alSortMatch     Constant
hi def link alConstant      Identifier

hi def link alAtom          Constant
hi def link alLogicOp       Constant
hi def link alVariable      Identifier

hi def link alTodo          Todo
hi def link alFixMe         Error

hi def link alSortConnector Type
hi def link alPredicates    Type
hi def link alComment       Comment
hi def link alLineComment   Comment

let b:current_syntax="alp"
let b:spell_options="contained"

" vim: ts=8
