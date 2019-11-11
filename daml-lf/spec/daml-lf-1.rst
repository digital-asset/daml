.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Copyright © 2019, `Digital Asset (Switzerland) GmbH
<https://www.digitalasset.com/>`_ and/or its affiliates.  All rights
reserved.

DAML-LF 1 specification
=======================

.. contents:: Contents


Introduction
^^^^^^^^^^^^

This document specifies version 1 of the DAML-LF language — the
language that DAML ledgers execute. DAML compiles to DAML-LF which
executes on DAML ledgers, similar to how Java compiles to JVM byte
code which executes on the JVM. “LF” in DAML-LF stands for “Ledger
Fragment”. DAML-LF is a small, strongly typed, functional language
with strict evaluation that includes native representations for core
DAML concepts such as templates, updates, and parties. It is primarily
intended as a compilation target.


How to view and edit this document
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To view this document correctly, we recommend you install the `DejaVu
Sans family of fonts <https://dejavu-fonts.github.io/>`_, which is
free (as in freedom) and provide exceptional Unicode coverage. The
sphinx style sheets specify DejaVu Sans Mono as the font to use for
code, and if you want to view/edit this section you should use it
for your editor, too.

Moreover, if you want to edit this section comfortably, we highly
recommend using Emacs' TeX input mode. You can turn it on using ``M-x
set-input-method TeX``, and then you can input symbols as you would in
TeX, mostly using ``\symbol-name`` and ``_letter``. If you don't know
how to input a character, go over it with your cursor and ``M-x
describe-char``. Its TeX code will be listed under ``to input``.

Moreover, add the following to your ``~/.emacs`` to enable additional
symbols used in this doc::

  (with-temp-buffer
    (activate-input-method "TeX")
    (let ((quail-current-package (assoc "TeX" quail-package-alist)))
      (quail-defrule "\\limage" ?⦇ nil t)
      (quail-defrule "\\rimage" ?⦈ nil t)
      (quail-defrule "\\rwave" ?↝ nil t)
      (quail-defrule "\\lwave" ?↜ nil t)
      (quail-defrule "\\lwbrace" ?⦃ nil t)
      (quail-defrule "\\rwbrace" ?⦄ nil t)))


Version history
~~~~~~~~~~~~~~~

The DAML-LF language is versioned using a major and minor component.
Increasing the major component allows us to drop features, change
the semantics of existing features, or update the serialization format.
Changes to the minor component cannot break backward compatibility,
and operate on the same major version of the serialization format in
a backward compatible way. This document describes DAML-LF major version
1, including all its minor versions.

Each DAML-LF program is accompanied by the version identifier of the
language it was serialized in. This number enables the DAML-LF engine
to interpret previous versions of the language in a backward
compatibility way.

In the following of this document, we will use annotations between
square brackets such as *[Available in version < x.y]*, *[Available in
versions >= x.y]*, and *[Changed in version x.y]* to emphasize that a
particular feature is concerned with a change introduced in DAML x.y
version. In addition, we will mark lines within inference rules with
annotations of the form ``[DAML-LF < x.y]`` and ``[DAML-LF ≥ x.y]`` to
make the respective line conditional upon the DAML-LF version.

The version 1.dev is a special staging area for the next 1.x version to
be released. Compliant implementations are not required to implement any
features exclusive to version 1.dev, but should take them under
advisement as likely elements of the next 1.x version.

Below, we list the versions of DAML-LF 1.x that a DAML-LF
engine compliant with the present specification must handle [except for
1.dev], in ascending order.  The list comes with a brief description of
the changes, and some links to help unfamiliar readers learn about the
features involved in the change.  One can refer also to the
`Serialization` section which is particularly concerned about versioning
and backward compatibility.


Version 1.0
...........

 * Introduction date:

      2018-12-11

  * Description:

      Initial version

Version: 1.1
............

  * Introduction date:

      2019-01-25

  * Last amendment date:

      2019-01-29

  * Description:

    * **Add** support for `option type
      <https://en.wikipedia.org/wiki/Option_type>`_.

      For more details, one can refer to the `Abstract Syntax`_,
      `Operational semantics`_ and `Type system`_ sections. There, the
      option type is denoted by ``'Option'`` and populated thanks to
      the constructor ``'None'`` and ``'Some'``.

    * **Add** built-in functions to order party literals.

      For more details about party literal order functions, one can to
      `Party built-in functions <Party functions_>`_ section.

    * **Change** the representation of serialized function
      type. Deprecate the ``'Fun'`` type in favor of the more general
      built-in type ``'TArrow'``.

      For more details about the type ``'TArrow'``, one can refer to
      the sections "`Abstract Syntax`_", "`Operational semantics`_"
      and "`Type system`_".  For details about the ``'Fun'`` type, one
      can refer to section `Function type vs arrow type`.


Version: 1.2
............

  * Introduction date:

      2019-03-18

  * Last amendment date:

      2019-03-22

  * Description:

    * **Add** a built-in function to perform `SHA-256
      <https://en.wikipedia.org/wiki/SHA-2>`_ hashing of strings

    * **Add** built-in functions to convert from ``'Party'`` to
      ``'Text'`` and vice versa.

    * **Change** the scope when the controllers of a choice are
      computed. Needed to support the so-called `flexible controllers`_
      in the surface language


Version: 1.3
............

  * Introduction date:

      2019-03-25

  * Last amendment date:

      2019-03-25

  * **Add** support for contract keys.

  * **Add** support for built-in ``'Map'`` type.

Version: 1.4
............

  * Introduction date:

      2019-05-21

  * Last amendment date:

      2019-05-21

  * **Add** support for complex contract keys.

Version: 1.5
............

  * Introduction date:

      2019-05-27

  * **Change** serializability condition for ``ContractId`` such that
    ``ContractId a`` is serializable whenever ``a`` is so. This is more
    relaxed than the previous condition.

  * **Add** ``COERCE_CONTRACT_ID`` primitive for coercing ``ContractId``s.

  * **Change** ``Update.Exercise`` such that ``actor`` is now optional.

  * **Add** ``FROM_TEXT_INT64`` and ``FROM_TEXT_DECIMAL`` primitives for
    parsing integer and decimal values.

Version: 1.6
............

  * Introduction date:

      2019-07-01

  * **Add** support for built-in ``'Enum'`` type.

  * **Add** ``TEXT_FROM_CODE_POINTS`` and ``TEXT_TO_CODE_POINTS``
    primitives for (un)packing strings.

  * **Add** package IDs interning in external package references.

Version: 1.7
............

  * **Add** Nat kind and Nat type.
    - add `nat` kind
    - add `nat` type

  * **Add** parametrically scaled Numeric type.
    - add `NUMERIC` primitive type
    - add `numeric` primitive literal
    - add numeric builtins, namely  `ADD_NUMERIC`, `SUB_NUMERIC`, `MUL_NUMERIC`, `DIV_NUMERIC`, `ROUND_NUMERIC`, `CAST_NUMERIC`, `SHIFT_NUMERIC`, `LEQ_NUMERIC`, `LESS_NUMERIC`, `GEQ_NUMERIC`, `GREATER_NUMERIC`, `FROM_TEXT_NUMERIC`, `TO_TEXT_NUMERIC`, `INT64_TO_NUMERIC`, `NUMERIC_TO_INT64`, `EQUAL_NUMERIC`

  * **Drop** support for Decimal type. Use Numeric of scale 10 instead.
    - drop `DECIMAL` primitive type
    - drop `decimal` primitive literal
    - drop decimal builtins, namely  `ADD_DECIMAL`, `SUB_DECIMAL`, `MUL_DECIMAL`, `DIV_DECIMAL`, `ROUND_DECIMAL`, `LEQ_DECIMAL`, `LESS_DECIMAL`, `GEQ_DECIMAL`, `GREATER_DECIMAL`, `FROM_TEXT_DECIMAL`, `TO_TEXT_DECIMAL`, `INT64_TO_DECIMAL`, `DECIMAL_TO_INT64`, `EQUAL_DECIMAL`

  * **Add** string interning in external package references.

  * **Add** name interning in external package references.

  * **Add** existential ``Any`` type
    - add `'Any'` primitive type
    - add `'to_an'y` and `'from_any'` expression to convert from/to
      an arbitrary ground type (i.e. a type with no free type variables)
       to ``Any``.

  * **Add** for Type representation.
    - add `'TypeRep'` primitive type
    - add `type_rep` expression to reify a arbitrary ground type
      (i.e. a type with no free type variables) to a value.

Version: 1.dev
..............

  * **Change** Transaction submitter must be in the contract key
    maintainers when performing lookup or fetches by key. See
    `issue #1866 <https://github.com/digital-asset/daml/issues/1866>`_

  * **Add** generic map type ``GenMap``.

Abstract syntax
^^^^^^^^^^^^^^^

This section specifies the abstract syntax tree of a DAML-LF
package. We define identifiers, literals, types, expressions, and
definitions.


Notation
~~~~~~~~

Terminals are specified as such::

  description:
    symbols ∈ regexp                               -- Unique identifier

Where:

* The ``description`` describes the terminal being defined.
* The ``symbols`` define how we will refer of the terminal in type rules /
  operational semantics / ....
* The ``regexp`` is a `java regular expression
  <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_
  describing the members of the terminal. In particular, we will use
  the following conventions:

  * ``\xhh`` matches the character with hexadecimal value ``0xhh``.
  * ``\n`` matches the carriage return character ``\x0A``,
  * ``\r`` matches the carriage return ``\x0D``,
  * ``\"`` matches the double quote character ``\x22``.
  * ``\$`` match the dollar character ``\x24``.
  * ``\.`` matches the full stop character ``\x2e\``.
  * ``\\`` matches the backslash character ``\x5c``.
  * ``\d`` to match a digit: ``[0-9]``.

* The ``Unique identifier`` is a string that uniquely identifies the
  non-terminal.

Sometimes the symbol might be the same as the unique identifier, in
the instances where a short symbol is not needed because we do not
mention it very often.

Non-terminals are specified as such::

  Description:
    symbols
      ::= non-terminal alternative                 -- Unique identifier for alternative: description for alternative
       |   ⋮

Where description and symbols have the same meaning as in the terminal
rules, and:

* each non-terminal alternative is a piece of syntax describing the
  alternative;
* each alternative has a unique identifier (think of them as
  constructors of a datatype).

Note that the syntax defined by the non-terminals is not intended to
be parseable or non-ambiguous, rather it is intended to be read and
interpreted by humans.  However, for the sake of clarity, we enclose
strings that are part of the syntax with single quotes. We do not
enclose symbols such as ``.`` or ``→`` in quotes for the sake of
brevity and readability.


Literals
~~~~~~~~

In this section, we define a bunch of literals that can be handled by
DAML-LF programs.

We first define two types of *strings*::

  Strings:
               Str ::= " "                          -- Str
                    |  " StrChars "

  Sequences of string characters:
          StrChars ::= StrChar                      -- StrChars
                    |  StrChars StrChar
                    |  EscapedStrChar StrChar

  String chars:
           StrChar  ∈  [^\n\r\"\\]                  -- StrChar

  String character escape sequences:
    EscapedStrChar  ∈  \\\n|\\\r|\\\"|\\\\          -- EscapedStrChar

*Strings* are possibly empty sequences of legal `Unicode
<https://en.wikipedia.org/wiki/Unicode>` code points where the line
feed character ``\n``, the carriage return character ``\r``, the
double quote character ``\"``, and the backslash character ``\\`` must
be escaped with backslash ``\\``. DAML-LF considers legal `Unicode
code point <https://unicode.org/glossary/#code_point>` that is not a
`Surrogate Code Point
<https://unicode.org/glossary/#surrogate_code_point>`, in other words
any code point with an integer value in the range from ``0x000000`` to
``0x00D7FF`` or in the range from ``0x00DFFF`` to ``0x10FFFF`` (bounds
included).


Then, we define the so-called *PackageId strings* and *PartyId
strings*.  Those are non-empty strings built with a limited set of
US-ASCII characters (See the rules `PackageIdChar` and `PartyIdChar`
below for the exact sets of characters). We use those string in
instances when we want to avoid empty identifiers, escaping problems,
and other similar pitfalls. ::

  PackageId strings
   PackageIdString ::= ' PackageIdChars '             -- PackageIdString

  Sequences of PackageId character
    PackageIdChars ::= PackageIdChar                  -- PackageIdChars
                    |  PackageIdChars PackageIdChar

  PackageId character
     PackageIdChar  ∈  [a-zA-Z0-9\-_ ]               -- PackageIdChar

  PartyId strings
     PartyIdString ::= ' PartyIdChars '               -- PartyIdString

  Sequences of PartyId character
      PartyIdChars ::= PartyIdChar                    -- PartyIdChars
                    |  PartyIdChars PartyIdChar

  PartyId character
       PartyIdChar  ∈  [a-zA-Z0-9:\-_ ]              -- PartyIdChar

We can now define all the literals that a program can handle::

  Nat type literals:                                -- LitNatType
       n ∈  \d+

  64-bit integer literals:
        LitInt64  ∈  (-?)\d+                         -- LitInt64

  Numeric literals:
      LitNumeric  ∈  ([+-]?)([1-9]\d+|0).\d*        -- LitNumeric

  Date literals:
         LitDate  ∈  \d{4}-\d{2}-\d{2}               -- LitDate

  UTC timestamp literals:
     LitTimestamp ∈  \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{1,3})?Z
                                                     -- LitTimestamp
  UTF8 string literals:
         LitText ::= String                          -- LitText

  Party literals:
        LitParty ::= PartyIdString                   -- LitParty

The literals represent actual DAML-LF values:

* A ``LitNatType`` represents a natural number between ``0`` and
  ``38``, bounds inclusive.
* A ``LitInt64`` represents a standard signed 64-bit integer (integer
  between ``−2⁶³`` to ``2⁶³−1``).
* A ``LitNumeric`` represents a signed number that can be represented
  in base-10 without loss of precision with at most 38 digits
  (ignoring possible leading 0 and with a scale (the number of
  significant digits on the right of the decimal point) between ``0``
  and ``37`` (bounds inclusive). In the following, we will use
  ``scale(LitNumeric)`` to denote the scale of the decimal number.
* A ``LitDate`` represents the number of day since
  ``1970-01-01`` with allowed range from ``0001-01-01`` to
  ``9999-12-31`` and using a year-month-day format.
* A ``LitTimestamp`` represents the number of microseconds
  since ``1970-01-01T00:00:00.000000Z`` with allowed range
  ``0001-01-01T00:00:00.000000Z`` to ``9999-12-31T23:59:59.999999Z``
  using a
  year-month-day-hour-minute-second-microsecond
  format.
* A ``LitText`` represents a `UTF8 string
  <https://en.wikipedia.org/wiki/UTF-8>`_.
* A ``LitParty`` represents a *party*.

.. note:: A literal which is not backed by an actual value is not
   valid and is implicitly rejected by the syntax presented here.
   For instance, the literal ``9223372036854775808`` is not a valid
   ``LitInt64`` since it cannot be encoded as a signed 64-bits
   integer, i.e. it equals ``2⁶³``.  Similarly,``2019-13-28`` is not a
   valid ``LitDate`` because there are only 12 months in a year.


Identifiers
~~~~~~~~~~~

We define now a generic notion of *identifier* and *name*::

  identifiers:
          Ident  ∈  [a-zA-Z_\$][a-zA-Z0-9_\$]       -- Ident

  names:
         Name   ::= Identifier                      -- Name
                 |  Name \. Identifier

Identifiers are standard `java identifiers
<https://docs.oracle.com/javase/specs/jls/se8/html/jls-3.html#jls-3.8>`_
restricted to US-ASCII while names are sequences of identifiers
intercalated with dots.

The character ``%`` is reserved for external languages built on
DAML-LF as a "not an Ident" notation, so should not be considered for
future addition to allowed identifier characters.

In the following, we will use identifiers to represent *built-in
functions*, term and type *variable names*, record and tuple *field
names*, *variant constructors*, and *template choices*. On the other
hand, we will use names to represent *type constructors*, *value
references*, and *module names*. Finally, we will use PackageId
strings as *package identifiers*.  ::

  Expression variables
        x, y, z ::= Ident                           -- VarExp

  Type variables
           α, β ::= Ident                           -- VarTy

  Built-in function names
              F ::= Ident                           -- Builtin

  Record and tuple field names
              f ::= Ident                           -- Field

  Variant data constructors
              V ::= Ident                           -- VariantCon

  Enum data constructors
              E ::= Ident                           -- EnumCon

  Template choice names
             Ch ::= Ident                           -- ChoiceName

  Value references
              W ::= Name                            -- ValRef

  Type constructors
              T ::= Name                            -- TyConName

  Module names
        ModName ::= Name                            -- ModName

  Contract identifiers
           cid                                      -- ContractId

  Package identifiers
           pid  ::=  PackageIdString                -- PkgId

We do not specify an explicit syntax for contract identifiers as it is
not possible to refer to them statically within a program. In
practice, contract identifiers can be created dynamically through
interactions with the underlying ledger. See the `operation semantics
of update statements <Update Interpretation_>`_ for the formal
specification of those interactions.

Also note that package identifiers are typically `cryptographic hash
<Package hash_>`_ of the content of the package itself.


Kinds, types, and expressions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. TODO We might want to consider changing the syntax for ``Mod``,
   since in our software we use the colon to separate the module name
   from the definition name inside the module.

Then we can define our kinds, types, and expressions::

  Kinds
    k
      ::= ⋆                                         -- KindStar
       |  'nat'                                     -- KindNat
       |  k₁ → k₂                                   -- KindArrow

  Module references
    Mod
      ::= PkdId:ModName                             -- ModPackage: module from a package

  Built-in types
    BuiltinType
      ::= 'TArrow'                                  -- BTArrow: Arrow type
       |  'Int64'                                   -- BTyInt64: 64-bit integer
       |  'Numeric'                                 -- BTyNumeric: numeric, precision 38, parametric scale between 0 and 37
       |  'Text'                                    -- BTyText: UTF-8 string
       |  'Date'                                    -- BTyDate
       |  'Timestamp'                               -- BTyTime: UTC timestamp
       |  'Party'                                   -- BTyParty
       |  'Date'                                    -- BTyDate: year, month, date triple
       |  'Unit'                                    -- BTyUnit
       |  'Bool'                                    -- BTyBool
       |  'List'                                    -- BTyList
       |  'Option'                                  -- BTyOption
       |  'Map'                                     -- BTMap
       |  'GenMap'                                  -- BTGenMap
       |  'Update'                                  -- BTyUpdate
       |  'ContractId'                              -- BTyContractId
       |  'Any'                                     -- BTyAny
       |  'TypeRep'                                 -- BTTypeRep

  Types (mnemonic: tau for type)
    τ, σ
      ::= α                                         -- TyVar: Type variable
       |  n                                         -- TyNat: Nat Type
       |  τ σ                                       -- TyApp: Type application
       |  ∀ α : k . τ                               -- TyForall: Universal quantification
       |  BuiltinType                               -- TyBuiltin: Builtin type
       |  Mod:T                                     -- TyCon: type constructor
       |  ⟨ f₁: τ₁, …, fₘ: τₘ ⟩                     -- TyTuple: Tuple type

  Expressions
    e ::= x                                         -- ExpVar: Local variable
       |  e₁ e₂                                     -- ExpApp: Application
       |  e @τ                                      -- ExpTyApp: Type application
       |  λ x : τ . e                               -- ExpAbs: Abstraction
       |  Λ α : k . e                               -- ExpTyAbs: Type abstraction
       |  'let' x : τ = e₁ 'in' e₂                  -- ExpLet: Let
       |  'case' e 'of' p₁ → e₁ '|' … '|' pₙ → eₙ   -- ExpCase: Pattern matching
       |  ()                                        -- ExpUnit
       |  'True'                                    -- ExpTrue
       |  'False'                                   -- ExpFalse
       |  'Nil' @τ                                  -- ExpListNil: Empty list
       |  'Cons' @τ e₁ e₂                           -- ExpListCons: Cons list
       |  'None' @τ                                 -- ExpOptionNone: Empty Option
       |  'Some' @τ e                               -- ExpOptionSome: Non-empty Option
       |  LitInt64                                  -- ExpLitInt64: 64-bit integer literal
       |  LitNumeric                                -- ExpLitNumeric: Numeric literal
       |  LitText                                   -- ExpLitText: UTF-8 string literal
       |  LitDate                                   -- ExpLitDate: Date literal
       |  LitTimestamp                              -- ExpLitTimestamp: UTC timestamp literal
       |  LitParty                                  -- ExpLitParty: Party literal
       |  cid                                       -- ExpLitContractId: Contract identifiers
       |  F                                         -- ExpBuiltin: Builtin function
       |  Mod:W                                     -- ExpVal: Defined value
       |  Mod:T @τ₁ … @τₙ { f₁ = e₁, …, fₘ = eₘ }   -- ExpRecCon: Record construction
       |  Mod:T @τ₁ … @τₙ {f} e                     -- ExpRecProj: Record projection
       |  Mod:T @τ₁ … @τₙ { e₁ 'with' f = e₂ }      -- ExpRecUpdate: Record update
       |  Mod:T:V @τ₁ … @τₙ e                       -- ExpVariantCon: Variant construction
       |  Mod:T:E                                   -- ExpEnumCon:Enum construction
       |  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩                   -- ExpTupleCon: Tuple construction
       |  e.f                                       -- ExpTupleProj: Tuple projection
       |  ⟨ e₁ 'with' f = e₂ ⟩                      -- ExpTupleUpdate: Tuple update
       |  u                                         -- ExpUpdate: Update expression
       | 'to_any' @τ t                              -- ExpToAny: Wrap a value of the given type in Any
       | 'from_any' @τ t                            -- ExpToAny: Extract a value of the given from Any or return None
       | 'type_rep' @τ                              -- ExpToTypeRep: A type representation

  Patterns
    p
      ::= Mod:T:V x                                 -- PatternVariant
       |  Mod:T:E                                   -- PatternEnum
       |  'Nil'                                     -- PatternNil
       |  'Cons' xₕ xₜ                              -- PatternCons
       |  'None'                                    -- PatternNone
       |  'Some' x                                  -- PatternSome
       |  'True'                                    -- PatternTrue
       |  'False'                                   -- PatternFalse
       |  ()                                        -- PatternUnit
       |  _                                         -- PatternDefault

  Updates
    u ::= 'pure' @τ e                               -- UpdatePure
       |  'bind' x₁ : τ₁ ← e₁ 'in' e₂               -- UpdateBlock
       |  'create' @Mod:T e                         -- UpdateCreate
       |  'fetch' @Mod:T e                          -- UpdateFetch
       |  'exercise' @Mod:T Ch e₁ e₂ e₃             -- UpdateExercise
       |  'exercise_without_actors' @Mod:T Ch e₁ e₂ -- UpdateExerciseWithoutActors
       |  'get_time'                                -- UpdateGetTime
       |  'fetch_by_key' @τ e                       -- UpdateFecthByKey
       |  'lookup_by_key' @τ e                      -- UpdateLookUpByKey
       |  'embed_expr' @τ e                         -- UpdateEmbedExpr


.. (RH) is better?
    *  Mod:T @τ₁ … @τₙ {f} e
    *  e.(Mod:T @τ₁ … @τₙ)


In the following, we will use ``τ₁ → τ₂`` as syntactic sugar for the
type application ``('TArrow' τ₁ τ₂)`` where ``τ₁`` and ``τ₂`` are
types.


Definitions
~~~~~~~~~~~

Expressions and types contain references to definitions in packages
available for usage::

  Template choice kind
    ChKind
      ::= 'consuming'                               -- ChKindConsuming
       |  'non-consuming'                           -- ChKindNonConsuming

  Template key definition
    KeyDef
      ::= 'no_key'
       |  'key' τ eₖ eₘ

  Template choice definition
    ChDef ::= 'choice' ChKind Ch (y : τ) (z: 'ContractId' Mod:T) : σ 'by' eₚ ↦ e
                                                    -- ChDef
  Definitions
    Def
      ::=
       |  'record' T (α₁: k₁)… (αₙ: kₙ) ↦ { f₁ : τ₁, …, fₘ : τₘ }
                                                    -- DefRecord
       |  'variant' T (α₁: k₁)… (αₙ: kₙ) ↦ V₁ : τ₁ | … | Vₘ : τₘ
                                                    -- DefVariant
       |  'enum' T  ↦ E₁ | … | Eₘ                    -- DefEnum
       |  'val' W : τ ↦ e                           -- DefValue
       |  'tpl' (x : T) ↦                           -- DefTemplate
            { 'precondition' e₁
            , 'signatories' e₂
            , 'observers' e₃
            , 'agreement' e₄
            , 'choices' { ChDef₁, …, ChDefₘ }
            , KeyDef
            }

  Module (mnemonic: delta for definitions)
    Δ ::= ε                                         -- DefCtxEmpty
       |  Def · Δ                                   -- DefCtxCons

  Package
    Package ∈ ModName ↦ Δ                           -- Package

  Package collection
    Ξ ∈ pid ↦ Package                               -- Packages


Feature flags
~~~~~~~~~~~~~

Modules are annotated with a set of feature flags. Those flags enables
syntactical restrictions and semantics changes on the annotated
module. The following feature flags are available:

 +-------------------------------------------+----------------------------------------------------------+
 | Flag                                      | Semantic meaning                                         |
 +===========================================+==========================================================+
 | ForbidPartyLiterals                       | Party literals are not allowed in a DAML-LF module.      |
 |                                           | (See `Party Literal restriction`_ for more details)      |
 +-------------------------------------------+----------------------------------------------------------+
 | DontDivulgeContractIdsInCreateArguments   | Contract ids captured in ``create`` arguments are not    |
 |                                           | divulged, ``fetch`` is authorized if and only if the     |
 |                                           | authorizing parties contain at least one stakeholder of  |
 |                                           | the fetched contract id.                                 |
 |                                           | The contract id on which a choice is exercised           |
 |                                           | is divulged to all parties that witness the choice.      |
 +-------------------------------------------+----------------------------------------------------------+
 | DontDiscloseNonConsumingChoicesToObservers| When a non-consuming choice of a contract is exercised,  |
 |                                           | the resulting sub-transaction is not disclosed to the    |
 |                                           | observers of the contract.                               |
 +-------------------------------------------+----------------------------------------------------------+


Well-formed programs
^^^^^^^^^^^^^^^^^^^^

The section describes the type system of language and introduces some
other restrictions over programs that are statically verified at
loading.


Type system
~~~~~~~~~~~

In all the type checking rules, we will carry around the packages
available for usage ``Ξ``. Given a module reference ``Mod`` equals to
``('Package' pid ModName)``, we will denote the corresponding
definitions as ``〚Ξ〛Mod`` where ``ModName`` is looked up in package
``Ξ(pid)``;

Expressions do also contain references to built-in functions. Any
built-in function ``F`` comes with a fixed type, which we will denote
as ``𝕋(F)``. See the `Built-in functions`_ section for the complete
list of built-in functions and their respective types.


Well-formed types
.................


First, we formally defined *well-formed types*. ::

 Type context:
   Γ ::= ε                                 -- CtxEmpty
      |  α : k · Γ                         -- CtxVarTyKind
      |  x : τ · Γ                         -- CtxVarExpType

                       ┌───────────────┐
  Well-formed types    │ Γ  ⊢  τ  :  k │
                       └───────────────┘

      α : k ∈ Γ
    ————————————————————————————————————————————— TyVar
      Γ  ⊢  α  :  k

    ————————————————————————————————————————————— TyNat
      Γ  ⊢  n  :  'nat'

      Γ  ⊢  τ  :  k₁ → k₂      Γ  ⊢  σ  :  k₂
    ————————————————————————————————————————————— TyApp
      Γ  ⊢  τ σ  :  k₁

      α : k · Γ  ⊢  τ : ⋆
    ————————————————————————————————————————————— TyForall
      Γ  ⊢  ∀ α : k . τ  :  ⋆

    ————————————————————————————————————————————— TyInt64
      Γ  ⊢  'TArrow' : ⋆ → ⋆

    ————————————————————————————————————————————— TyInt64
      Γ  ⊢  'Int64' : ⋆

    ————————————————————————————————————————————— TyNumeric
      Γ  ⊢  'Numeric' : 'nat' → ⋆

    ————————————————————————————————————————————— TyText
      Γ  ⊢  'Text' : ⋆

    ————————————————————————————————————————————— TyDate
      Γ  ⊢  'Date' : ⋆

    ————————————————————————————————————————————— TyTimestamp
      Γ  ⊢  'Timestamp' : ⋆

    ————————————————————————————————————————————— TyParty
      Γ  ⊢  'Party' : ⋆

    ————————————————————————————————————————————— TyUnit
      Γ  ⊢  'Unit' : ⋆

    ————————————————————————————————————————————— TyBool
      Γ  ⊢  'Bool' : ⋆

    ————————————————————————————————————————————— TyDate
      Γ  ⊢  'Date' : ⋆

    ————————————————————————————————————————————— TyList
      Γ  ⊢  'List' : ⋆ → ⋆

    ————————————————————————————————————————————— TyOption
      Γ  ⊢  'Option' : ⋆ → ⋆

    ————————————————————————————————————————————— TyMap
      Γ  ⊢  'Map' : ⋆ → ⋆

    ————————————————————————————————————————————— TyGenMap
      Γ  ⊢  'GenMap' : ⋆ → ⋆ → ⋆

    ————————————————————————————————————————————— TyUpdate
      Γ  ⊢  'Update' : ⋆ → ⋆

    ————————————————————————————————————————————— TyContractId
      Γ  ⊢  'ContractId' : ⋆  → ⋆

    ————————————————————————————————————————————— TyAny
      Γ  ⊢  'Any' : ⋆

    ————————————————————————————————————————————— TyTypeRep
      Γ  ⊢  'TypeRep' : ⋆

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ … ∈ 〚Ξ〛Mod
    ————————————————————————————————————————————— TyRecordCon
      Γ  ⊢  Mod:T : k₁ → … → kₙ  → ⋆

      'variant' T (α₁:k₁) … (αₙ:kₙ) ↦ … ∈ 〚Ξ〛Mod
    ————————————————————————————————————————————— TyVariantCon
      Γ  ⊢  Mod:T : k₁ → … → kₙ  → ⋆

      'enum' T ↦ … ∈ 〚Ξ〛Mod
    ————————————————————————————————————————————— TyEnumCon
      Γ  ⊢  Mod:T :  ⋆

      Γ  ⊢  τ₁  :  ⋆    …    Γ  ⊢  τₙ  :  ⋆
    ————————————————————————————————————————————— TyTuple
      Γ  ⊢  ⟨ f₁: τ₁, …, fₙ: τₙ ⟩  :  ⋆


Well-formed expression
......................

Then we define *well-formed expressions*. ::

                          ┌───────────────┐
  Well-formed expressions │ Γ  ⊢  e  :  τ │
                          └───────────────┘

      x : τ  ∈  Γ
    ——————————————————————————————————————————————————————————————— ExpDefVar
      Γ  ⊢  x  :  τ

      Γ  ⊢  e₁  :  τ₁ → τ₂      Γ  ⊢  e₂  :  τ₁
    ——————————————————————————————————————————————————————————————— ExpApp
      Γ  ⊢  e₁ e₂  :  τ₂

      Γ  ⊢  τ  :  k      Γ  ⊢  e  :  ∀ α : k . σ
    ——————————————————————————————————————————————————————————————— ExpTyApp
      Γ  ⊢  e @τ  :  σ[α ↦ τ]

      x : τ · Γ  ⊢  e  :  σ     Γ  ⊢ τ  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpAbs
      Γ  ⊢  λ x : τ . e  :  τ → σ

      α : k · Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpTyAbs
      Γ  ⊢  Λ α : k . e  :  ∀ α : k . τ

      Γ  ⊢  e₁  :  τ      Γ  ⊢  τ  :  ⋆
      x : τ · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpLet
      Γ  ⊢  'let' x : τ = e₁ 'in' e₂  :  σ

    ——————————————————————————————————————————————————————————————— ExpUnit
      Γ  ⊢  ()  :  'Unit'

    ——————————————————————————————————————————————————————————————— ExpTrue
      Γ  ⊢  'True'  :  'Bool'

    ——————————————————————————————————————————————————————————————— ExpFalse
      Γ  ⊢  'False'  :  'Bool'

      Γ  ⊢  τ  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpListNil
      Γ  ⊢  'Nil' @τ  :  'List' τ

      Γ  ⊢  τ  :  ⋆     Γ  ⊢  eₕ  :  τ     Γ  ⊢  eₜ  :  'List' τ
    ——————————————————————————————————————————————————————————————— ExpListCons
      Γ  ⊢  'Cons' @τ eₕ eₜ  :  'List' τ

      Γ  ⊢  τ  :  ⋆
     —————————————————————————————————————————————————————————————— ExpOptionNone
      Γ  ⊢  'None' @τ  :  'Option' τ

      Γ  ⊢  τ  :  ⋆     Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpOptionSome
      Γ  ⊢  'Some' @τ e  :  'Option' τ

      τ contains no quantifiers
      ε  ⊢  τ : *     Γ  ⊢  e  : τ
    ——————————————————————————————————————————————————————————————— ExpToAny
      Γ  ⊢  'to_any' @τ e  :  'Any'

      τ contains no quantifiers
      ε  ⊢  τ : *     Γ  ⊢  e  : Any
    ——————————————————————————————————————————————————————————————— ExpFromAny
      Γ  ⊢  'from_any' @τ e  :  'Optional' τ

      ε  ⊢  τ : *     τ contains no quantifiers
    ——————————————————————————————————————————————————————————————— ExpTypeRep
      Γ  ⊢  'type_rep' @τ  :  'TypeRep'

    ——————————————————————————————————————————————————————————————— ExpBuiltin
      Γ  ⊢  F : 𝕋(F)

    ——————————————————————————————————————————————————————————————— ExpLitInt64
      Γ  ⊢  LitInt64  :  'Int64'

      n = scale(LitNumeric)
    ——————————————————————————————————————————————————————————————— ExpLitNumeric
      Γ  ⊢  LitNumeric  :  'Numeric' n

    ——————————————————————————————————————————————————————————————— ExpLitText
      Γ  ⊢  LitText  :  'Text'

    ——————————————————————————————————————————————————————————————— ExpLitDate
      Γ  ⊢  LitDate  :  'Date'

    ——————————————————————————————————————————————————————————————— ExpLitTimestamp
      Γ  ⊢  LitTimestamp  :  'Timestamp'

    ——————————————————————————————————————————————————————————————— ExpLitParty
      Γ  ⊢  LitParty  :  'Party'

      'tpl' (x : T) ↦ { … }  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpLitContractId
      Γ  ⊢  cid  :  'ContractId' Mod:T

      'val' W : τ ↦ …  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpVal
      Γ  ⊢  Mod:W  :  τ

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { f₁:τ₁, …, fₘ:τₘ }  ∈ 〚Ξ〛Mod
      Γ  ⊢  σ₁ : k₁    …     Γ  ⊢  σₙ : kₙ
      Γ  ⊢  e₁ :  τ₁[α₁ ↦ σ₁, …, αₙ ↦ σₙ]
            ⋮
      Γ  ⊢  eₘ :  τₘ[α₁ ↦ σ₁, …, αₙ ↦ σₙ]
    ———————————————————————————————————————————————————————————————— ExpRecCon
      Γ  ⊢
        Mod:T @σ₁ … @σₙ { f₁ = e₁, …, fₘ = eₘ }  :  Mod:T σ₁ … σₙ

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { …, f : σ, … }  ∈ 〚Ξ〛Mod
      Γ  ⊢  τ₁ : k₁    …     Γ  ⊢  τₙ : kₙ
      Γ  ⊢  e  :  Mod:T τ₁ … τₙ
    ——————————————————————————————————————————————————————————————— ExpRecProj
      Γ  ⊢  Mod:T @τ₁ … @τₙ {f} e  :  σ[α₁ ↦ τ₁, …, αₙ ↦ τₙ]

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { …, fᵢ : τᵢ, … }  ∈ 〚Ξ〛Mod
      Γ  ⊢  e  :  Mod:T σ₁  ⋯  σₙ
      Γ  ⊢  eᵢ  :  τᵢ[α₁ ↦ σ₁, …, αₙ ↦ σₙ]
    ———————————————————————————————————————————————————————————————– ExpRecUpdate
      Γ  ⊢
          Mod:T @σ₁ … @σₙ { e 'with' fᵢ = eᵢ }  :  Mod:T σ₁ … σₙ

      'variant' T (α₁:k₁) … (αₙ:kₙ) ↦ … | Vᵢ : σᵢ | …  ∈  〚Ξ〛Mod
      Γ  ⊢  τ₁ : k₁    ⋯     Γ  ⊢  τₙ : kₙ
      Γ  ⊢  e  :  σᵢ[α₁ ↦ τ₁, …, αₙ ↦ τₙ]
    ——————————————————————————————————————————————————————————————— ExpVarCon
      Γ  ⊢  Mod:T:Vᵢ @τ₁ … @τₙ e  :  Mod:T τ₁ … τₙ

      'enum' T ↦ … | Eᵢ | …  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpEnumCon
      Γ  ⊢  Mod:T:Eᵢ  :  Mod:T

      Γ  ⊢  e₁  :  τ₁      …      Γ  ⊢  eₘ  :  τₘ
    ——————————————————————————————————————————————————————————————— ExpTupleCon
      Γ  ⊢  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩  :  ⟨ f₁: τ₁, …, fₘ: τₘ ⟩

      Γ  ⊢  e  :  ⟨ …, fᵢ: τᵢ, … ⟩
    ——————————————————————————————————————————————————————————————— ExpTupleProj
      Γ  ⊢  e.fᵢ  :  τᵢ

      Γ  ⊢  e  :  ⟨ f₁: τ₁, …, fᵢ: τᵢ, …, fₙ: τₙ ⟩
      Γ  ⊢  eᵢ  :  τᵢ
    ——————————————————————————————————————————————————————————————— ExpTupleUpdate
      Γ  ⊢   ⟨ e 'with' fᵢ = eᵢ ⟩  :  ⟨ f₁: τ₁, …, fₙ: τₙ ⟩

      'variant' T (α₁:k₁) … (αₙ:kn) ↦ … | V : τ | …  ∈  〚Ξ〛Mod
      Γ  ⊢  e₁  :  Mod:T τ₁ … τₙ
      x : τ[α₁ ↦ τ₁, …, αₙ ↦ τₙ] · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseVariant
      Γ  ⊢  'case' e₁ 'of' Mod:T:V x → e₂ : σ

      'enum' T ↦ … | E | …  ∈  〚Ξ〛Mod
      Γ  ⊢  e₁  :  Mod:T
      Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseEnum
      Γ  ⊢  'case' e₁ 'of' Mod:T:E → e₂ : σ

      Γ  ⊢  e₁  : 'List' τ      Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseNil
      Γ  ⊢  'case' e₁ 'of' 'Nil' → e₂ : σ

      xₕ ≠ xₜ
      Γ  ⊢  e₁  : 'List' τ
      Γ  ⊢  xₕ : τ · xₜ : 'List' τ · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseCons
      Γ  ⊢  'case' e₁ 'of' Cons xₕ xₜ → e₂  :  σ

      Γ  ⊢  e₁  : 'Option' τ      Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseNone
      Γ  ⊢  'case' e₁ 'of' 'None' → e₂ : σ

      Γ  ⊢  e₁  : 'Option' τ      Γ  ⊢  x : τ · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseSome
      Γ  ⊢  'case' e₁ 'of' 'Some' x → e₂  :  σ

      Γ  ⊢  e₁  :  'Bool'       Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseTrue
      Γ  ⊢  'case' e₁ 'of 'True' → e₂  :  σ

      Γ  ⊢  e₁  :  'Bool'       Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseFalse
      Γ  ⊢  'case' e₁ 'of 'False' → e₂  :  σ

      Γ  ⊢  e₁  :  'Unit'       Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseUnit
      Γ  ⊢  'case' e₁ 'of' () → e₂  :  σ

      Γ  ⊢  e₁  :  τ       Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpCaseDefault
      Γ  ⊢  'case' e₁ 'of' _ → e₂  :  σ

      n > 1
      Γ  ⊢  'case' e 'of' alt₁ : σ
        ⋮
      Γ  ⊢  'case' e 'of' altₙ : σ
    ——————————————————————————————————————————————————————————————— ExpCaseOr
      Γ  ⊢  'case' e 'of' alt₁ | … | altₙ : σ

      Γ  ⊢  τ  : ⋆      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— UpdPure
      Γ  ⊢  'pure' e  :  'Update' τ

      Γ  ⊢  τ₁  : ⋆       Γ  ⊢  e₁  :  'Update' τ₁
      Γ  ⊢  x₁ : τ₁ · Γ  ⊢  e₂  :  'Update' τ₂
    ——————————————————————————————————————————————————————————————— UpdBlock
      Γ  ⊢  'bind' x₁ : τ₁ ← e₁ 'in' e₂  :  'Update' τ₂

      'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod       Γ  ⊢  e  : Mod:T
    ——————————————————————————————————————————————————————————————— UpdCreate
      Γ  ⊢  'create' @Mod:T e  : 'Update' ('ContractId' Mod:T)

      'tpl' (x : T)
          ↦ { …, 'choices' { …, 'choice' ChKind Ch (y : τ) (z : 'ContractId' Mod:T) : σ 'by' … ↦ …, … } }
        ∈ 〚Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod:T
      Γ  ⊢  e₂  :  'List' 'Party'
      Γ  ⊢  e₃  :  τ
    ——————————————————————————————————————————————————————————————— UpdExercise
      Γ  ⊢  'exercise' @Mod:T Ch e₁ e₂ e₃  : 'Update' σ

      'tpl' (x : T)
          ↦ { …, 'choices' { …, 'choice' ChKind Ch (y : τ) (z : 'ContractId' Mod:T) : σ 'by' … ↦ …, … } }
        ∈ 〚Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod:T
      Γ  ⊢  e₂  :  τ
    ——————————————————————————————————————————————————————————————— UpdExerciseWithouActors
      Γ  ⊢  'exercise_without_actors' @Mod:T Ch e₁ e₂  : 'Update' σ

      'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod:T
    ——————————————————————————————————————————————————————————————— UpdFetch
      Γ  ⊢  'fetch' @Mod:T e₁ : 'Update' Mod:T

    ——————————————————————————————————————————————————————————————— UpdGetTime
      Γ  ⊢  'get_time'  : 'Update' 'Timestamp'

      'tpl' (x : T)  ↦ { …, 'key' τ …, … } ∈ 〚Ξ〛Mod
      Γ  ⊢  e : τ
    ——————————————————————————————————————————————————————————————— UpdFetchByKey
      Γ  ⊢  'fetch_by_key' @Mod:T e
              :
        'Update' ⟨
          'contractId' : 'ContractId' @Mod:T
          'contract' : Mod:T
        ⟩

      'tpl' (x : T)  ↦ { …, 'key' τ …, … } ∈ 〚Ξ〛Mod
      Γ  ⊢  e : τ
    ——————————————————————————————————————————————————————————————— UpdLookupByKey
      Γ  ⊢  'lookup_by_key' @Mod:T e
              :
	    'Update' ('Option' (ContractId Mod:T))

      Γ  ⊢  e  :  'Update' τ
    ——————————————————————————————————————————————————————————————— UpdEmbedExpr
      Γ  ⊢  'embed_expr' @τ e  :  Update' τ


Serializable types
..................

To define the validity of definitions, modules, and packages, we need to
first define *serializable* types. As the name suggests, serializable
types are the types whose values can be persisted on the ledger. ::

                         ┌────────┐
  Serializable types     │ ⊢ₛ  τ  │
                         └────────┘

    ———————————————————————————————————————————————————————————————— STyUnit
      ⊢ₛ  'Unit'

    ———————————————————————————————————————————————————————————————— STyBool
      ⊢ₛ  'Bool'

      ⊢ₛ  τ
    ———————————————————————————————————————————————————————————————— STyList
      ⊢ₛ  'List' τ

      ⊢ₛ  τ
    ———————————————————————————————————————————————————————————————— STyOption
      ⊢ₛ  'Option' τ

    ———————————————————————————————————————————————————————————————— STyInt64
      ⊢ₛ  'Int64'

    ———————————————————————————————————————————————————————————————— STyNumeric
      ⊢ₛ  'Numeric' n

    ———————————————————————————————————————————————————————————————— STyText
      ⊢ₛ  'Text'

    ———————————————————————————————————————————————————————————————— STyDate
      ⊢ₛ  'Date'

    ———————————————————————————————————————————————————————————————— STyTimestamp
      ⊢ₛ  'Timestamp'

    ———————————————————————————————————————————————————————————————— STyParty
      ⊢ₛ  'Party'

      'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod
    ———————————————————————————————————————————————————————————————— STyCid [DAML-LF < 1.5]
      ⊢ₛ  'ContractId' Mod:T

      ⊢ₛ  τ
    ———————————————————————————————————————————————————————————————— STyCid [DAML-LF ≥ 1.5]
      ⊢ₛ  'ContractId' τ

      'record' T α₁ … αₙ ↦ { f₁: σ₁, …, fₘ: σₘ }  ∈  〚Ξ〛Mod
      ⊢ₛ  σ₁[α₁ ↦ τ₁, …, αₙ ↦ τₙ]
       ⋮
      ⊢ₛ  σₘ[α₁ ↦ τ₁, …, αₙ ↦ τₙ]
      ⊢ₛ  τ₁
       ⋮
      ⊢ₛ  τₙ
    ———————————————————————————————————————————————————————————————— STyRecConf
      ⊢ₛ  Mod:T τ₁ … τₙ

      'variant' T α₁ … αₙ ↦ V₁: σ₁ | … | Vₘ: σₘ  ∈  〚Ξ〛Mod   m ≥ 1
      ⊢ₛ  σ₁[α₁ ↦ τ₁, …, αₙ ↦ τₙ]
       ⋮
      ⊢ₛ  σₘ[α₁ ↦ τ₁, …, αₙ ↦ τₙ]
      ⊢ₛ  τ₁
       ⋮
      ⊢ₛ  τₙ
    ———————————————————————————————————————————————————————————————— STyVariantCon
      ⊢ₛ  Mod:T τ₁ … τₙ

     'enum' T ↦ E₁: σ₁ | … | Eₘ: σₘ  ∈  〚Ξ〛Mod   m ≥ 1
    ———————————————————————————————————————————————————————————————— STyEnumCon
      ⊢ₛ  Mod:T

Note that

1. Tuples are *not* serializable.
2. Uninhabited variant and enum types are *not* serializable.
3. For a data type to be serializable, *all* type
   parameters must be instantiated with serializable types, even
   phantom ones.


Well-formed-definitions
.......................

Finally, we specify well-formed definitions. Note that these rules
work also under a set of packages available for usage ``Ξ``. Moreover,
they also have the current module name, ``ModName``, in scope (needed
for the ``DefTemplate`` rule). ::

                          ┌────────┐
  Well-formed definitions │ ⊢  Def │
                          └────────┘

    αₙ : kₙ · ⋯ · α₁ : k₁  ⊢  τ₁  :  ⋆
     ⋮
    αₙ : kₙ · ⋯ · α₁ : k₁  ⊢  τₘ  :  ⋆
  ——————————————————————————————————————————————————————————————— DefRec
    ⊢  'record' T (α₁: k₁) … (αₙ: kₙ) ↦ { f₁: τ₁, …, fₘ: τₘ }

    αₙ : kₙ · ⋯ · α₁ : k₁  ⊢  τ₁  :  ⋆
     ⋮
    αₙ : kₙ · ⋯ · α₁ : k₁  ⊢  τₘ  :  ⋆
  ——————————————————————————————————————————————————————————————— DefVariant
    ⊢  'record' T (α₁: k₁) … (αₙ: kₙ) ↦ V₁: τ₁ | … | Vₘ: τₘ

  ——————————————————————————————————————————————————————————————— DefEnum
    ⊢  'enum' T  ↦ E₁ | … | Eₘ

    ε  ⊢  e  :  τ
  ——————————————————————————————————————————————————————————————— DefValue
    ⊢  'val' W : τ ↦ e

    'record' T ↦ { f₁ : τ₁, …, fₙ : tₙ }  ∈  〚Ξ〛Mod
    ⊢ₛ  Mod:T
    x : Mod:T  ⊢  eₚ  :  'Bool'
    x : Mod:T  ⊢  eₛ  :  'List' 'Party'
    x : Mod:T  ⊢  eₒ  :  'List' 'Party'
    x : Mod:T  ⊢  eₐ  :  'Text'
    x : Mod:T  ⊢  ChDef₁      …      x : Mod:T  ⊢  ChDefₘ
    x : Mod:T  ⊢  KeyDef
  ——————————————————————————————————————————————————————————————— DefTemplate
    ⊢  'tpl' (x : T) ↦
         { 'precondition' eₚ
         , 'signatories' eₛ
         , 'observers' eₒ
         , 'agreement' eₐ
         , 'choices' { ChDef₁, …, ChDefₘ }
         , KeyDef
         }

                          ┌───────────────────┐
  Well-formed choices     │ x : Mod:T ⊢ ChDef │
                          └───────────────────┘
    ⊢ₛ  τ
    ⊢ₛ  σ
    x : Mod:T  ⊢  eₚ  :  'List' 'Party'     x ≠ y                        [DAML-LF < 1.2]
    y : τ · x : Mod:T  ⊢  eₚ  :  'List' 'Party'                          [DAML-LF ≥ 1.2]
    z : 'ContractId' Mod:T · y : τ · x : Mod:T  ⊢  e  :  'Update' σ
  ——————————————————————————————————————————————————————————————— ChDef
    x : Mod:T  ⊢  'choice' ChKind Ch (y : τ) (z : 'ContractId' Mod:T) : σ 'by' eₚ ↦ e

            ┌────────────┐
  Valid key │ ⊢ₖ e  :  τ │
            └────────────┘

  ——————————————————————————————————————————————————————————————— ExpRecProj
    ⊢ₖ  x

    ⊢ₖ  e
  ——————————————————————————————————————————————————————————————— ExpRecProj
    ⊢ₖ  Mod:T @τ₁ … @τₙ {f} e

    ⊢ₖ  e₁    …    ⊢ₖ eₘ
  ———————————————————————————————————————————————————————————————— ExpRecCon
    ⊢ₖ  Mod:T @σ₁ … @σₙ { f₁ = e₁, …, fₘ = eₘ }

                          ┌────────────┐
  Well-formed keys        │ Γ ⊢ KeyDef │
                          └────────────┘
  ——————————————————————————————————————————————————————————————— KeyDefNone
   Γ  ⊢  'no_key'

    ⊢ₛ τ      Γ  ⊢  eₖ  :  τ
    ⊢ₖ eₖ                                                         [DAML-LF = 1.3]
    ε  ⊢  eₘ  :  τ → 'List' 'Party'
  ——————————————————————————————————————————————————————————————— KeyDefSome
    Γ  ⊢  'key' τ eₖ eₘ


Naturally, we will say that modules and packages are well-formed if
all the definitions they contain are well-formed.


Template coherence
~~~~~~~~~~~~~~~~~~

Each template definition is paired to a record ``T`` with no type
arguments (see ``DefTemplate`` rule). To avoid ambiguities, we want to
make sure that each record type ``T`` has at most one template
definition associated to it. We term this restriction *template
coherence* since it's a requirement reminiscent of the coherence
requirement of Haskell type classes.

Specifically, a template definition is *coherent* if:

* Its argument data type is defined in the same module that the
  template is defined in;
* Its argument data type is not an argument to any other template.


Party literal restriction
~~~~~~~~~~~~~~~~~~~~~~~~~

.. TODO I think this is incorrect, and actually before the
   ``ForbidPartyLiterals`` feature flag party literals where
   allowed everywhere.

The usage of party literals is restricted in DAML-LF. By default,
party literals are neither allowed in templates nor in values used in
templates directly or indirectly.  In practice, this restricted the
usage of party literals to test cases written in DAML-LF. Usage of
party literals can be completely forbidden thanks to the `feature flag
<Feature flags_>`_ ``ForbidPartyLiterals``. If this flag is on, any
occurrence of a party literal anywhere in the module makes the module
not well-formed.


Name collision restriction
~~~~~~~~~~~~~~~~~~~~~~~~~~

DAML-LF relies on `names and identifiers <Identifiers_>`_ to refer to
different kinds of constructs such as modules, type constructors,
variants constructor, and fields. These are relative; type names are
relative to modules; field names are relative to type record and so
one. They live in different namespaces. For example, the space names
for module and type is different.


Fully resolved name
...................

DAML-LF restricts the way names and identifiers are used within a
package. This restriction relies on the notion of *fully resolved
name* construct as follows:

* The *fully resolved name* of the module ``Mod`` is ``Mod``.
* The *fully resolved name* of a record type constructor ``T`` defined
  in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a variant type constructor ``T``
  defined in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a enum type constructor ``T``
  defined in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a field ``fᵢ`` of a record type
  definition ``'record' T …  ↦ { …, fᵢ: τᵢ, … }`` defined in the module
  ``Mod`` is ``Mod.T.fᵢ``
* The *fully resolved name* of a variant constructor ``Vᵢ`` of a
  variant type definition ``'variant' T … ↦ …  | Vᵢ: τᵢ | …`` defined in
  the module ``Mod`` is ``Mod.T.Vᵢ``.
* The *fully resolved name* of a enum constructor ``Eᵢ`` of a enum type
   definition ``'enum' T ↦ …  | Eᵢ | …`` defined in the module ``Mod``
   is ``Mod.T.Eᵢ``.
* The *fully resolved name* of a choice ``Ch`` of a template
  definition ``'tpl' (x : T) ↦ { …, 'choices' { …, 'choice' ChKind Ch
  … ↦ …, … } }`` defined in the module ``Mod`` is ``Mod.T.Ch``.


Name collisions
...............

A so-called *name collision* occurs if two fully resolved names in a
package are equal *ignoring case*. The following are examples of
collisions:

* A package contains two modules with the same name;
* A module defines two types with the same name, one lowercase and the
  other one uppercase;
* A record contains two fields with the same name;
* A package contains a module ``A.B`` and a module ``A`` that defines
  the type ``B``;
* A package contains a module ``A.B`` that defines the type ``C``
  together with a module ``A`` that defines the type ``B.C``.

Note that templates do not have names, and therefore can not cause
collisions. Note also that value references are not concerned with
collisions as defined here.

Also note that while the collision is case-insensitive, name resolution
is *not* case-insensitive in DAML-LF. In other words, to refer to a
name, one must refer to it with the same case that it was defined with.

The case-insensitivity for collisions is in place since we often generate
files from DAML-LF packages, and we want to make sure for things to work
smoothly when operating in case-insensitive file systems, while at the
same time preserving case sensitivity in the language.


Name collision condition
........................

In DAML-LF, the only permitted name collisions are those occurring
between variant constructors and record types defined in the same
module. Every other collision makes the module (and thus the package)
not well-formed. For example, a module ``Mod`` can contain the following
definitions::

  'variant' Tree (α : ⋆) ↦ Node : Mod:Tree.Node @α | Leaf : Unit

  'record' Tree.Node (α : ⋆) ↦ { value: α, left: Mod:Tree α, right: Mod:Tree α }

The variant constructor ``Node`` (within the definition of the
variant type ``Tree``) and the record type ``Tree.Node`` (within the
first record type definition) have the same fully resolved name
``Mod.Tree.Node``. However this package is well-formed.

Note that name collisions between a record definition and a variant
constructor from different modules are prohibited.

We will say that the *name collision condition* holds for a package if
the only name collisions within this package are those occurring
between variant constructors and record types, as described above.


Well formed packages
~~~~~~~~~~~~~~~~~~~~

Then, a collection of packages ``Ξ`` is well-formed if:

* Each definition in ``Ξ`` is `well-formed <well-formed-definitions_>`_;
* Each template in ``Ξ`` is `coherent <Template coherence_>`_;
* The `party literal restriction`_ is respected for
  every module in ``Ξ`` -- taking the ``ForbidPartyLiterals`` flag into
  account.
* The `name collision condition`_ holds for every
  package of ``Ξ``.
* There are no cycles between modules and packages references.


Operational semantics
^^^^^^^^^^^^^^^^^^^^^

The section presents a big-step call-by value operation semantics of
the language.

Similarly to the type system, every rule for expression evaluation and
update/scenario interpretation operates on the packages available for
usage ``Ξ``.


Values
~~~~~~

To define any call-by-value semantics for DAML-LF expression, we need
first to define the notion of *values*, the expressions which do not
need to be evaluated further. ::

                           ┌───────┐
   Values                  │ ⊢ᵥ  e │
                           └───────┘

   ——————————————————————————————————————————————————— ValExpAbs
     ⊢ᵥ  λ x : τ . e

   ——————————————————————————————————————————————————— ValExpTyAbs
     ⊢ᵥ  Λ α : k . e

   ——————————————————————————————————————————————————— ValExpLitInt64
     ⊢ᵥ  LitInt64

   ——————————————————————————————————————————————————— ValExpLitNumeric
     ⊢ᵥ  LitNumeric

   ——————————————————————————————————————————————————— ValExpLitText
     ⊢ᵥ  LitText

   ——————————————————————————————————————————————————— ValExpLitDate
     ⊢ᵥ  LitDate

   ——————————————————————————————————————————————————— ValExpLitTimestamp
     ⊢ᵥ  LitTimestamp

   ——————————————————————————————————————————————————— ValExpLitContractId
     ⊢ᵥ  cid

   ——————————————————————————————————————————————————— ValExpUnit
     ⊢ᵥ  ()

   ——————————————————————————————————————————————————— ValExpTrue
     ⊢ᵥ  'True'

   ——————————————————————————————————————————————————— ValExpFalse
     ⊢ᵥ  'False'

   ——————————————————————————————————————————————————— ValExpListNil
     ⊢ᵥ  'Nil' @τ

     ⊢ᵥ  eₕ     ⊢ᵥ  eₜ
   ——————————————————————————————————————————————————— ValExpListCons
     ⊢ᵥ  'Cons' @τ eₕ eₜ

   ——————————————————————————————————————————————————— ValExpOptionNone
     ⊢ᵥ  'None' @τ

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpOptionSome
     ⊢ᵥ  'Some' @τ e

     0 ≤ k < m
     𝕋(F) = ∀ (α₁: ⋆) … (αₘ: ⋆). σ₁ → … → σₙ → σ
   ——————————————————————————————————————————————————— ValExpBuiltin₁
     ⊢ᵥ  F @τ₁ … @τₖ

     0 ≤ k < n
     𝕋(F) = ∀ (α₁: ⋆) … (αₘ: ⋆). σ₁ → … → σₙ → σ
     ⊢ᵥ  e₁      …      ⊢ᵥ  eₖ
   ——————————————————————————————————————————————————— ValExpBuiltin₂
     ⊢ᵥ  F @τ₁ … @τₘ e₁ … eₖ

     ⊢ᵥ  e₁      …      ⊢ᵥ  eₙ
   ——————————————————————————————————————————————————— ValExpRecCon
     ⊢ᵥ  Mod:T @τ₁ … @τₙ { f₁ = e₁, …, fₙ = eₙ }

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpVariantCon
     ⊢ᵥ  Mod:T:V @τ₁ … @τₙ e

   ——————————————————————————————————————————————————— ValExpEnumCon
     ⊢ᵥ  Mod:T:E

     ⊢ᵥ  e₁      ⋯      ⊢ᵥ  eₘ
   ——————————————————————————————————————————————————— ValExpTupleCon
     ⊢ᵥ  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpToAny
     ⊢ᵥ  'to_any' @τ e

   ——————————————————————————————————————————————————— ValExpTypeRep
     ⊢ᵥ  'type_rep' @τ

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpUpdPure
     ⊢ᵥ  'pure' e

     ⊢ᵥ  e₁
   ——————————————————————————————————————————————————— ValExpUpdBind
     ⊢ᵥ  'bind' x : τ ← e₁ 'in' e₂

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpUpdCreate
     ⊢ᵥ  'create' @Mod:T e

     ⊢ᵥ  e₁      ⊢ᵥ  e₂      ⊢ᵥ  e₃
   ——————————————————————————————————————————————————— ValExpUpdExercise
     ⊢ᵥ  'exercise' Mod:T.Ch e₁ e₂ e₃

     ⊢ᵥ  e₁      ⊢ᵥ  e₂
   ——————————————————————————————————————————————————— ValExpUpdExerciseWithoutActors
     ⊢ᵥ  'exercise_without_actors' Mod:T.Ch e₁ e₂

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpUpFetchByKey
     ⊢ᵥ  'fetch_by_key' @τ e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpUdpLookupByKey
     ⊢ᵥ  'lookup_by_key' @τ e


   ——————————————————————————————————————————————————— ValExpUpdGetTime
     ⊢ᵥ  'get_time'

   ——————————————————————————————————————————————————— ValExpUdpEmbedExpr
     ⊢ᵥ  'embed_expr' @τ e


Note that the argument of an embedded expression does not need to be a
value for the whole to be so.  In the following, we will use the
symbol ``v`` to represent an expression which is a value.


Pattern matching
~~~~~~~~~~~~~~~~

We now define how patterns *match* values. If a pattern match succeed,
it produces a *substitution*, which tells us how to instantiate variables
bound by pattern.

::

    Substitution
      θ ::= ε                                       -- SubstEmpty
         |  x ↦ v · θ                               -- SubstExpVal

    Pattern matching result
     mr ::= Succ θ                                  -- MatchSuccess
         |  Fail                                    -- MatchFailure

                           ┌─────────────────────┐
    Pattern Matching       │ v 'matches' p ⇝ mr  │
                           └─────────────────────┘


    —————————————————————————————————————————————————————————————————————— MatchVariant
      Mod:T:V @τ₁ … @τₘ v  'matches'  Mod:T:V x  ⇝  Succ (x ↦ v · ε)

    —————————————————————————————————————————————————————————————————————— MatchEnum
      Mod:T:E  'matches'  Mod:T:E  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchNil
      'Nil' @τ  'matches'  'Nil'  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchCons
      'Cons' @τ vₕ vₜ 'matches' 'Cons' xₕ xₜ
        ⇝
      Succ (xₕ ↦ vₕ · xₜ ↦ vₜ · ε)

    —————————————————————————————————————————————————————————————————————— MatchNone
      'None' @τ  'matches'  'None'  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchSome
      'Some' @τ v 'matches' 'Some' x  ⇝  Succ (x ↦ v · ε)

    —————————————————————————————————————————————————————————————————————— MatchTrue
      'True' 'matches' 'True'  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchFalse
      'False' 'matches' 'False'  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchUnit
      '()' 'matches' '()'  ⇝  Succ ε

    —————————————————————————————————————————————————————————————————————— MatchDefault
       v 'matches' _  ⇝  Succ ε

       if none of the rules above apply
    —————————————————————————————————————————————————————————————————————— MatchFail
       v 'matches' p  ⇝  Fail


Expression evaluation
~~~~~~~~~~~~~~~~~~~~~

DAML-LF evaluation is only defined on closed, well-typed expressions.

Note that the evaluation of the body of a value definition is lazy. It
happens only when needed and cached to avoid repeated computations. We
formalize this using an *evaluation environment* that associates to
each value reference the result of the evaluation of the corresponding
definition (See rules ``EvExpVal`` and ``EvExpValCached``.). The
evaluation environment is updated each time a value reference is
encountered for the first time.

Note that we do not specify if and how the evaluation environment is
preserved between different evaluations happening in the ledger. We
only guarantee that within a single evaluation each value definition
is evaluated at most once.

The output of any DAML-LF built-in function ``F`` fully applied to
types ``@τ₁ … @τₘ`` and values ``v₁ … vₙ`` is deterministic. In the
following rules, we abstract this output with the notation ``𝕆(F @τ₁ …
@τₘ v₁ … vₙ)``. Please refer to the `Built-in functions`_ section for the
exact output.

::

  Evaluation environment
    E ::= ε                                         -- EnvEmpty
       |  Mod:W ↦ v · E                             -- EnvVal

  Evaluation result
    r ::= Ok v                                      -- ResOk
       |  Err LitText                               -- ResErr

                           ┌───────────────────┐
  Big-step evaluation      │ e ‖ E₁  ⇓  r ‖ E₂ │
                           └───────────────────┘


    —————————————————————————————————————————————————————————————————————— EvValue
      v ‖ E  ⇓  Ok v ‖ E

      e₁ ‖ E₀  ⇓  Ok (λ x : τ . e) ‖ E₁
      e₂ ‖ E₁  ⇓  Ok v₂ ‖ E₂
      e[x ↦ v₂] ‖ E₂  ⇓  r ‖ E₃
    —————————————————————————————————————————————————————————————————————— EvExpApp
      e₁ e₂ ‖ E₀  ⇓  r ‖ E₃

      e₁ ‖ E₀  ⇓  Ok (Λ α : k . e) ‖ E₁
      e[α ↦ τ] ‖ E₁  ⇓  r ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpTyApp
      e₁ @τ ‖ E₀  ⇓  r ‖ E₂

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
      e₂[x ↦ v₁] ‖ E₁  ⇓  r ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpLet
      'let' x : τ = e₁ 'in' e₂ ‖ E₀  ⇓  r ‖ E₂

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpToAny
      'to_any' @τ e ‖ E₀  ⇓  Ok('to_any' @τ v) ‖ E₁

      e ‖ E₀  ⇓  Ok ('to_any' @τ v) ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpFromAnySucc
      'from_any' @τ e ‖ E₀  ⇓  'Some' @τ v ‖ E₁

      e ‖ E₀  ⇓  Ok ('to_any' @τ₁ v) ‖ E₁     τ₁ ≠ τ₂
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyFail
      'from_any' @τ₂ e ‖ E₀  ⇓  'None' ‖ E₁

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
      v 'matches' p₁  ⇝  Succ (x₁ ↦ v₁ · … · xₘ ↦ vₘ · ε)
      e₁[x₁ ↦ v₁, …, xₘ ↦ vₘ] ‖ E₁  ⇓  r ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpCaseSucc
      'case' e₁ 'of' {  p₁ → e₁ | … |  pₙ → eₙ } ‖ E₀  ⇓  r ‖ E₂

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁    v₁ 'matches' p₁  ⇝  Fail
      'case' v₁ 'of' { p₂ → e₂ … | pₙ → eₙ } ‖ E₁  ⇓  r ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpCaseFail
      'case' e₁ 'of' { p₁ → e₁ | p₂ → e₂ | … | pₙ → eₙ } ‖ E₀
        ⇓
      r ‖ E₂

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁     v 'matches' p  ⇝  Fail
    —————————————————————————————————————————————————————————————————————— EvExpCaseErr
      'case' e₁ 'of' { p → e } ‖ E₀  ⇓  Err "match error" ‖ E₁

       eₕ ‖ E₀  ⇓  Ok vₕ ‖ E₁
       eₜ ‖ E₁  ⇓  Ok vₜ ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpCons
      'Cons' @τ eₕ eₜ ‖ E₀  ⇓  Ok ('Cons' @τ vₕ vₜ) ‖ E₂

       e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpSome
      'Some' @τ e ‖ E₀  ⇓  Ok ('Some' @τ v) ‖ E₂

      𝕋(F) = ∀ (α₁: ⋆). … ∀ (αₘ: ⋆). σ₁ → … → σₙ → σ
      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
        ⋮
      eₙ ‖ Eₙ₋₁  ⇓  Ok vₙ ‖ Eₙ
    —————————————————————————————————————————————————————————————————————— EvExpBuiltin
      F @τ₁ … @τₘ eᵢ … eₙ ‖ E₀  ⇓  𝕆(F @τ₁ … @τₘ v₁ … vₙ) ‖ Eₙ

      'val' W : τ ↦ e  ∈ 〚Ξ〛Mod      Mod:W ↦ … ∉ Eₒ
      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpNonCachedVal
      Mod:W ‖ E₀  ⇓  Ok v ‖ Mod:W ↦ v · E₁

      Mod:W ↦ v ∈ E₀
    —————————————————————————————————————————————————————————————————————— EvExpCachedVal
      Mod:W ‖ E₀  ⇓  Ok v ‖ E₀

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
        ⋮
      eₙ ‖ Eₙ₋₁  ⇓  Ok vₙ ‖ Eₙ
    —————————————————————————————————————————————————————————————————————— EvExpRecCon
      Mod:T @τ₁ … @τₘ {f₁ = e₁, …, fₙ = eₙ} ‖ E₀
        ⇓
      Ok (Mod:T @τ₁ … @τₘ {f₁ = v₁, …, fₙ = ₙ}) ‖ Eₙ

      e ‖ E₀  ⇓  Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ, …, fₙ= vₙ}) ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpRecProj
      Mod:T @τ₁ … @τₘ {fᵢ} e ‖ E₀  ⇓  Ok vᵢ ‖ E₁

      e ‖ E₀  ⇓  Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ, …, fₙ= vₙ}) ‖ E₁
      eᵢ ‖ E₁  ⇓  Ok vᵢ' ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpRecUpd
      Mod:T @τ₁ … @τₘ { e 'with' fᵢ = eᵢ } ‖ E₀
        ⇓
      Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ', …, fₙ= vₙ}) ‖ E₂

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpVarCon
      Mod:T:V @τ₁ … @τₙ e ‖ E₀  ⇓  Ok (Mod:T:V @τ₁ … @τₙ v) ‖ E₁

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
        ⋮
      eₙ ‖ Eₙ₋₁  ⇓  Ok vₙ ‖ Eₙ
    —————————————————————————————————————————————————————————————————————— EvExpTupleCon
      ⟨f₁ = e₁, …, fₙ = eₙ⟩ ‖ E₀  ⇓  Ok ⟨f₁ = v₁, …, fₙ = vₙ⟩ ‖ Eₙ

      e ‖ E₀  ⇓  Ok ⟨ f₁= v₁, …, fᵢ = vᵢ, …, fₙ = vₙ ⟩ ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpTupleProj
      e.fᵢ ‖ E₀  ⇓  Ok vᵢ ‖ E₁

      e ‖ E₀  ⇓  Ok ⟨ f₁= v₁, …, fᵢ = vᵢ, …, fₙ = vₙ ⟩ ‖ E₁
      eᵢ ‖ E₁  ⇓  Ok vᵢ' ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpTupleUpd
      ⟨ e 'with' fᵢ = eᵢ ⟩ ‖ E₀
        ⇓
      Ok ⟨ f₁= v₁, …, fᵢ= vᵢ', …, fₙ= vₙ ⟩ ‖ E₂

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpUpdPure
      'pure' @τ e ‖ E₀  ⇓  Ok ('pure' @τ v) ‖ E₁

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpUpdBind
      'bind' x₁ : τ₁ ← e₁ 'in' e₂ ‖ E₀
        ⇓
      Ok ('bind' x₁ : τ₁ ← v₁ 'in' e₂) ‖ E₁

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpUpCreate
      'create' @Mod:T e ‖ E₀  ⇓  Ok ('create' @Mod:T v) ‖ E₁

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpUpFetch
      'fetch' @Mod:T e ‖ E₀  ⇓  Ok ('fetch' @Mod:T v) ‖ E₁

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
      e₂ ‖ E₁  ⇓  Ok v₂ ‖ E₂
      e₃ ‖ E₂  ⇓  Ok v₃ ‖ E₃
    —————————————————————————————————————————————————————————————————————— EvExpUpExcerise
      'exercise' @Mod:T Ch e₁ e₂ e₃ ‖ E₀
        ⇓
      Ok ('exercise' @Mod:T Ch v₁ v₂ v₃) ‖ E₃

      e₁ ‖ E₀  ⇓  Ok v₁ ‖ E₁
      e₂ ‖ E₁  ⇓  Ok v₂ ‖ E₂
    —————————————————————————————————————————————————————————————————————— EvExpUpExceriseWithoutActors
      'exercise_without_actors' @Mod:T Ch e₁ e₂ ‖ E₀
        ⇓
      Ok ('exercise_without_actors' @Mod:T Ch v₁ v₂) ‖ E₂

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpFetchByKey
      'fetch_by_key' @Mod:T e ‖ E₀
        ⇓
      Ok ('fetch_by_key' @Mod:T v) ‖ E₁

      e ‖ E₀  ⇓  Ok v ‖ E₁
    —————————————————————————————————————————————————————————————————————— EvExpUpLookupByKey
      'lookup_by_key' @Mod:T e ‖ E₀
       ⇓
      Ok ('lookup_by_key' @Mod:T v) ‖ E₁


Note that the rules are designed such that for every expression, at
most one applies. Also note how the chaining of environments within a
rule makes explicit the order of sub-expressions evaluation:
sub-expression are always evaluated from left to right.  For the sake
of brevity and readability, we do not explicitly specify the cases
where one of the sub-expressions *errors out*, that is it
evaluates to a result of the form ``Err v``. However, the user can
rely on the fact that an expression evaluates to ``Err v ‖ E`` as soon
as one of its sub-expression evaluates to ``Err v ‖ E`` without
further evaluating the remaining sub-expressions.

Update interpretation
~~~~~~~~~~~~~~~~~~~~~

We define the operational semantics of the update interpretation
against the ledger model described in the `DA Ledger Model
<https://docs.daml.com/concepts/ledger-model/index.html>`_ theory
report.


Update semantics use the predicate ``=ₛ`` to compare two lists of
party literals as those latter were sets.


..
  (RH) We probably do not need to be so explicit

  Formally the predicate is defined  as follows:::


   —————————————————————————————————————— InHead
     v  in  (Cons @Party v vₜ)

     v  in  vₜ
   —————————————————————————————————————— InTail
     v  in  (Cons @Party vₕ vₜ)

   —————————————————————————————————————— NilSubset
     (Nil @Party)  subset  v

     vₕ  in  v      vₜ  subset  v
   —————————————————————————————————————— ConsSubset
     (Cons @Party vₕ vₜ)  subset  v

     v₁  subset  v₂      v₂  subset  v₁
   —————————————————————————————————————— SetEquality
     v₁  =ₛ  v₂


The operational semantics are restricted to update statements which
are values according to ``⊢ᵥ``. In this section, all updates denoted
by the symbol ``u`` will be implicit values. In practice, what this
means is that an interpreter implementing these semantics will need to
evaluate the update expression first according to the operational
semantics for expressions, before interpreting the update.

The result of an update is a value accompanied by a ledger transaction
as described by the ledger model::

  Contracts on the ledger
    Contract
      ::= (cid, Mod:T, vₜ)                  -- vₜ must be of type Mod:T

  Global contract Key
    GlobalKey
      ::= (Mod:T, vₖ)

  Ledger actions
    act
      ::= 'create' Contract
       |  'exercise' v Contract ChKind tr  -- v must be of type 'List' 'Party'

  Ledger transactions
    tr
      ::= act₁ · … · actₙ

  Contract states
    ContractState
      ::= 'active'
       |  'inactive'

  Contract stores
     st ∈ finite map from cid to (Mod:T, v, ContractState)

  Contract key index
     keys ∈ finite injective map from GlobalKey to cid

  Update result
    ur ::= Ok (v, tr)
        |  Err v


                                    ┌──────────────────────────────┐
  Big-step update interpretation    │ u ‖ E₀ ; S₀ ⇓ᵤ ur ‖ E₁ ; S₁  │
                                    └──────────────────────────────┘

   —————————————————————————————————————————————————————————————————————— EvUpdPure
     'pure' v ‖ E ; (st, keys)  ⇓ᵤ  Ok (v, ε) ‖ E ; (st, keys)

     u₁ ‖ E₀ ; (st₀, keys₀)  ⇓ᵤ  Ok (v₁, tr₁) ‖ E₁ ; (st₁, keys₁)
     e₂[x ↦ v₁] ‖ E₁  ⇓  Ok u₂ ‖ E₂
     u₂ ‖ E₂ ; (st₁, keys₁)  ⇓ᵤ  Ok (v₂, tr₂) ‖ E₃ ; (st₂, keys₂)
   —————————————————————————————————————————————————————————————————————— EvUpdBind
     'bind' x : τ ← u₁ ; e₂ ‖ E₀ ;  (st₀, keys₀)
       ⇓ᵤ
     Ok (v₂, tr₁ · tr₂) ‖ E₃ ;  (st₂, keys₂)

     'tpl' (x : T) ↦ { 'precondition' eₚ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ] ‖ E₀  ⇓  Ok 'True' ‖ E₁
     eₖ[x ↦ vₜ] ‖ E₁  ⇓  Ok vₖ ‖ E₂
     eₘ vₜ ‖ E₁  ⇓  Ok vₘ ‖ E₂
     cid ∉ dom(st₀)      vₖ ∉ dom(keys₀)
     tr = 'create' (cid, Mod:T, vₜ)
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'active')]
     keys₁ = keys₀[(Mod:T, vₖ) ↦ cid]
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeySucceed
     'create' @Mod:T vₜ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     Ok (cid, tr) ‖ E₁ ; (st₁,  keys₁)

     'tpl' (x : T) ↦ { 'precondition' eₚ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ] ‖ E₀  ⇓  Ok 'True' ‖ E₁
     eₖ[x ↦ vₜ] ‖ E₁  ⇓  Ok vₖ ‖ E₂
     cid ∉ dom(st₀)      (Mod:T, vₖ) ∈ dom(keys₀)
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeyFail
     'create' @Mod:T vₜ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     Err "Mod:T template key violation"  ‖ E₁ ; (st₀, keys₀)

     'tpl' (x : T) ↦ { 'precondition' eₚ, … }  ∈  〚Ξ〛Mod
     cid ∉ dom(st₀)
     eₚ[x ↦ vₜ] ‖ E₀  ⇓  Ok 'True' ‖ E₁
     eₖ  ‖ E₁  ⇓  Ok vₖ ‖ E₂
     eₘ vₖ ‖ E₂  ⇓  Ok vₘ ‖ E₃
     tr = 'create' (cid, Mod:T, vₜ, 'no_key')
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'active')]
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWihoutKeySucceed
     'create' @Mod:T vₜ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     Ok (cid, tr) ‖ E₁ ; (st₁, keys₀)

     'tpl' (x : T) ↦ { 'precondition' eₚ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ] ‖ E₁  ⇓  Ok 'False' ‖ E₂
   —————————————————————————————————————————————————————————————————————— EvUpdCreateFail
     'create' @Mod:T vₜ ‖ E₀ ; (st, keys)
       ⇓ᵤ
     Err "template precondition violated"  ‖ E_ ; (st, keys)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'consuming' Ch (y : τ) (z) : σ  'by' eₚ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[y ↦ v₂, x ↦ vₜ] ‖ E₀  ⇓  Ok vₚ ‖ E₁
     v₁ =ₛ vₚ
     eₐ[z ↦ cid, y ↦ v₂, x ↦ vₜ] ‖ E₁  ⇓  Ok uₐ ‖ E₂
     keys₁ = keys₀ - keys₀⁻¹(cid)
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'inactive')]
     uₐ ‖ E₂ ; (st₁, keys₁)  ⇓ᵤ  Ok (vₐ, trₐ) ‖ E₃ ; (st₂, keys₂)
   —————————————————————————————————————————————————————————————————————— EvUpdExercConsum
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     Ok (vₐ, 'exercise' v₁ (cid, Mod:T, vₜ) 'consuming' trₐ) ‖ E₃ ; (st₂, keys₂)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'non-consuming' Ch z (y : τ) (z) : σ  'by' eₚ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[y ↦ v₂, x ↦ vₜ] ‖ E₀  ⇓  Ok vₚ ‖ E₁
     v₁ =ₛ vₚ
     eₐ[z ↦ cid, y ↦ v₂, x ↦ vₜ] ‖ E₁  ⇓  Ok uₐ ‖ E₂
     uₐ ‖ E₂ ; (st₀; keys₀)  ⇓ᵤ  Ok (vₐ, trₐ) ‖ E₃ ; (st₁, keys₁)
   —————————————————————————————————————————————————————————————————————— EvUpdExercNonConsum
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     Ok (vₐ, 'exercise' v₁ (cid, Mod:T, vₜ) 'non-consuming' trₐ) ‖ E₃ ; (st₁, keys₁)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : τ) : σ  'by' eₚ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'inactive')
   —————————————————————————————————————————————————————————————————————— EvUpdExercInactive
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ E₀ ; (st₀; keys₀)
       ⇓ᵤ
     Err "Exercise on inactive contract" ‖ E₀ ; (st₀; keys₀)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : τ) : σ  'by' eₚ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ] ‖ E₀  ⇓  Ok vₚ ‖ E₁
     v₁ ≠ₛ vₚ
   —————————————————————————————————————————————————————————————————————— EvUpdExercBadActors
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ E₀ ; (st; keys)
       ⇓ᵤ
     Err "Exercise actors do not match"  ‖ E₁ ; (st; keys)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : τ) (z) : σ  'by' eₚ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[y ↦ v₂, x ↦ vₜ] ‖ E₀  ⇓  Ok vₚ ‖ E₁
     'exercise' Mod:T.Ch cid vₚ v₁ ‖ E₁ ; (st₀, keys₀)  ⇓ᵤ  ur ‖ E₂ ; (st₁, keys₁)
   —————————————————————————————————————————————————————————————————————— EvUpdExercWithoutActors
     'exercise_without_actors' Mod:T.Ch cid v₁ ‖ E₀ ; (st₀, keys₀)
       ⇓ᵤ
     ur ‖ E₂ ; (st₁, keys₁)

     'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod
     cid ∈ dom(st)
     st(cid) = (Mod:T, vₜ, 'active')
   —————————————————————————————————————————————————————————————————————— EvUpdFetch
     'fetch' @Mod:T cid ‖ E ; (st; keys)
       ⇓ᵤ
     Ok (vₜ, ε) ‖ E ; (st; keys)

      e ‖ E₀  ⇓  Ok vₖ ‖ E₁
      (Mod:T, vₖ) ∈ dom(keys₀)      cid = keys((Mod:T, v))
      st(cid) = (Mod:T, vₜ, 'active')
   —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyFound
     'fetch_by_key' @Mod:T e ‖ E₀ ; (st; keys)
        ⇓ᵤ
     Ok ⟨'contractId': cid, 'contract': vₜ⟩ ‖ E₁ ; (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     e ‖ E₀  ⇓  Ok vₖ ‖ E₁
     (eₘ vₖ) ‖ E₁  ⇓  vₘ ‖ E₂
     (Mod:T, vₖ) ∉ dom(keys₀)
    —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyNotFound
     'fetch_by_key' @Mod:T e ‖ E₀ ; (st; keys)
        ⇓ᵤ
     Err "Lookup key not found"  ‖ E₂ ; (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     e ‖ E₀  ⇓  Ok vₖ ‖ E₁
     (eₘ vₖ) ‖ E₁  ⇓  vₘ ‖ E₂
     (Mod:T, vₖ) ∈ dom(keys)   cid = keys((Mod:T, v))
   —————————————————————————————————————————————————————————————————————— EvUpdLookupByKeyFound
     'look_by_key' @Mod:T e ‖ E₀ ; (st; keys)
       ⇓ᵤ
     Ok ('Some' @(Contract:Id Mod:T) cid) ‖ E₁ ; (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     e ‖ E₀  ⇓  Ok vₖ ‖ E₁
     (eₘ vₖ) ‖ E₁  ⇓  vₘ ‖ E₂
     (Mod:T, vₖ) ∉ dom(keys)
   —————————————————————————————————————————————————————————————————————— EvUpdLookupByKeyNotFound
     'look_by_key' @Mod:T e ‖ E₀ ; (st; keys)
         ⇓ᵤ
     Ok ('None' @(Contract:Id Mod:T)) ‖ E₁ ; (st; keys)

     LitTimestamp is the current ledger time
   —————————————————————————————————————————————————————————————————————— EvUpdGetTime
     'get_time' ‖ E ; (st; keys)
       ⇓ᵤ
     Ok (LitTimestamp, ε) ‖ E ; (st; keys)

     e  ‖ E₀  ⇓  Ok u ‖ E₁
     u ‖ E₁ ; st₀  ⇓ᵤ  ur ‖ E₂ ; st₁
   —————————————————————————————————————————————————————————————————————— EvUpdEmbedExpr
     'embed_expr' @τ e ‖ E₀; st₀  ⇓ᵤ  ur ‖ E₂ ; st₁


Similar to expression evaluation, we do not explicitly specify the
cases where sub-expressions fail. Those case can be inferred in a
straightforward way by following the left-to-right evaluation order.


Built-in functions
^^^^^^^^^^^^^^^^^^

This section lists the built-in functions supported by DAML LF 1.
The functions come with their types and a description of their
behavior.


Boolean functions
~~~~~~~~~~~~~~~~~

* ``EQUAL_BOOL : 'Bool' → 'Bool' → 'Bool'``

  Returns ``'True'`` if the two booleans are syntactically equal,
  ``False`` otherwise.

Int64 functions
~~~~~~~~~~~~~~~

* ``ADD_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Adds the two integers. Throws an error in case of overflow.

* ``SUB_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Subtracts the second integer from the first one. Throws an error in
  case of overflow.

* ``MUL_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Multiplies the two integers. Throws an error in case of overflow.

* ``DIV_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Returns the quotient of division of the first integer by the second
  one. Throws an error if the first integer is ``−2⁶³`` and the second
  one is ``-1``.

* ``MOD_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Returns the remainder of the division of the first integer by the
  second one.

* ``EXP_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Returns the exponentiation of the first integer by the second
  one. Throws an error in case of overflow.

* ``LESS_EQ_INT64 : 'Int64' → 'Int64' → 'Bool'``

  Returns ``'True'`` if the first integer is less or equal than the
  second, ``'False'`` otherwise.

* ``GREATER_EQ_INT64 : 'Int64' → 'Int64' → 'Bool'``

  Returns ``'True'`` if the first integer is greater or equal than
  the second, ``'False'`` otherwise.

* ``LESS_INT64 : 'Int64' → 'Int64' → 'Bool'``

  Returns ``'True'`` if the first integer is strictly less than the
  second, ``'False'`` otherwise.

* ``GREATER_INT64 : 'Int64' → 'Int64' → 'Bool'``

  Returns ``'True'`` if the first integer is strictly greater than
  the second, ``'False'`` otherwise.

* ``EQUAL_INT64 : 'Int64' → 'Int64' → 'Bool'``

  Returns ``'True'`` if the first integer is equal to the second,
  ``'False'`` otherwise.

* ``TO_TEXT_INT64 : 'Int64' → 'Text'``

  Returns the decimal representation of the integer as a string.

* ``FROM_TEXT_INT64 : 'Text' → 'Optional' 'Int64'``

  Given a string representation of an integer returns the integer wrapped
  in ``Some``. If the input does not match the regexp ``[+-]?\d+`` or
  if the result of the conversion overflows, returns ``None``.

  [*Available in versions >= 1.5*]

Numeric functions
~~~~~~~~~~~~~~~~~

* ``ADD_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α  → 'Numeric' α``

  Adds the two decimals.  The scale of the inputs and the output is
  given by the type parameter `α`.  Throws an error in case of
  overflow.

* ``SUB_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Numeric' α``

  Subtracts the second decimal from the first one.  The
  scale of the inputs and the output is given by the type parameter
  `α`.  Throws an error if overflow.

* ``MUL_NUMERIC : ∀ (α₁ α₂ α : nat) . 'Numeric' α₁ → 'Numeric' α₂ → 'Numeric' α``

  Multiplies the two numerics and rounds the result to the closest
  multiple of ``10⁻ᵅ`` using `banker's rounding convention
  <https://en.wikipedia.org/wiki/Rounding#Round_half_to_even>`_.
  The type parameters `α₁`, `α₂`, `α` define the scale of the first
  input, the second input, and the output, respectively. Throws an
  error in case of overflow.

* ``DIV_NUMERIC : ∀ (α₁ α₂ α : nat) . 'Numeric' α₁ → 'Numeric' α₂ → 'Numeric' α``

  Divides the first decimal by the second one and rounds the result to
  the closest multiple of ``10⁻ᵅ`` using `banker's rounding convention
  <https://en.wikipedia.org/wiki/Rounding#Round_half_to_even>`_ (where
  `n` is given as the type parameter).  The type parameters `α₁`,
  `α₂`, `α` define the scale of the first input, the second input, and
  the output, respectively. Throws an error in case of overflow.


* ``CAST_NUMERIC : ∀ (α₁, α₂: nat) . 'Numeric' α₁ → 'Numeric' α₂``

  Converts a decimal of scale `α₁` to a decimal scale `α₂` while
  keeping the value the same. Throws an exception in case of
  overflow or precision loss.

* ``SHIFT_NUMERIC : ∀ (α₁, α₂: nat) . 'Int64' → 'Numeric' α₁ → 'Numeric' α₂``

  Converts a decimal of scale `α₁` to a decimal scale `α₂` to another
  by shifting the decimal point. Thus the ouput will be equal to the input
  multiplied by `1E(α₁-α₂)`.

* ``LESS_EQ_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Bool'``

  Returns ``'True'`` if the first numeric is less or equal than the
  second, ``'False'`` otherwise.  The scale of the inputs is given by
  the type parameter `α`.

* ``GREATER_EQ_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Bool'``

  Returns ``'True'`` if the first numeric is greater or equal than the
  second, ``'False'`` otherwise. The scale of the inputs is given by
  the type parameter `α`.

* ``LESS_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Bool'``

  Returns ``'True'`` if the first numeric is strictly less than the
  second, ``'False'`` otherwise.  The scale of the inputs is given by
  the type parameter `α`.

* ``GREATER_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Bool'``

  Returns ``'True'`` if the first numeric is strictly greater than the
  second, ``'False'`` otherwise.  The scale of the inputs is given by
  the type parameter `α`.

* ``EQUAL_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Numeric' α → 'Bool'``

  Returns ``'True'`` if the first numeric is equal to the second,
  ``'False'`` otherwise.  The scale of the inputs is given by the type
  parameter `α`.

* ``TO_TEXT_NUMERIC : ∀ (α : nat) . 'Numeric' α → 'Text'``

  Returns the numeric string representation of the numeric.  The scale
  of the input is given by the type parameter `α`.

* ``FROM_TEXT_NUMERIC : ∀ (α : nat) .'Text' → 'Optional' 'Numeric' α``

  Given a string representation of a numeric returns the numeric
  wrapped in ``Some``. If the input does not match the regexp
  ``[+-]?\d+(\.d+)?`` or if the result of the conversion cannot
  be mapped into a decimal without loss of precision, returns
  ``None``.  The scale of the output is given by the type parameter
  `α`.

  [*Available in versions >= 1.5*]

String functions
~~~~~~~~~~~~~~~~

* ``APPEND_TEXT : 'Text' → 'Text' → 'Text'``

  Appends the second string at the end of the first one.

* ``EXPLODE_TEXT : 'Text' → List 'Text'``

  Returns the list of the individual `codepoint
  <https://en.wikipedia.org/wiki/Code_point>`_ of the string. Note the
  codepoints of the string are still of type ``'Text'``.

* ``IMPLODE_TEXT : 'List' 'Text' → 'Text'``

  Appends all the strings in the list.

* ``SHA256_TEXT : 'Text' → 'Text'``

  Performs the `SHA-256 <https://en.wikipedia.org/wiki/SHA-2>`_
  hashing of the UTF-8 string and returns it encoded as a Hexadecimal
  string (lower-case).

  [*Available in versions >= 1.2*]

* ``LESS_EQ_TEXT : 'Text' → 'Text' → 'Bool'``

  Returns ``'True'`` if the first string is lexicographically less
  or equal than the second, ``'False'`` otherwise.

* ``GREATER_EQ_TEXT : 'Text' → 'Text' → 'Bool'``

  Returns ``'True'`` if the first string is lexicographically
  greater or equal than the second, ``'False'`` otherwise.

* ``LESS_TEXT : 'Text' → 'Text' → 'Bool'``

  Returns ``'True'`` if the first string is lexicographically
  strictly less than the second, ``'False'`` otherwise.

* ``GREATER_TEXT : 'Text' → 'Text' → 'Bool'``

  Returns ``'True'`` if the first string is lexicographically
  strictly greater than the second, ``'False'`` otherwise.

* ``EQUAL_TEXT : 'Text' → 'Text' → 'Bool'``

  Returns ``'True'`` if the first string is equal to the second,
  ``'False'`` otherwise.

* ``TO_TEXT_TEXT : 'Text' → 'Text'``

  Returns string such as.

* ``TEXT_FROM_CODE_POINTS``: 'Text' → 'List' 'Int64'

  Returns the list of the Unicode `codepoint
  <https://en.wikipedia.org/wiki/Code_point>`_ of the input
  string represented as integer.

  [*Available in versions >= 1.6*]

* ``TEXT_TO_CODE_POINTS``: 'List' 'Int64' → 'Text'

  Given a list of integer representation of Unicode codepoint,
  return the string built from those codepoint. Throws an error
  if one of the elements of the input list is not in the range
  from `0x000000` to `0x00D7FF` or in the range from `0x00DFFF`
  to `0x10FFFF` (bounds included).

  [*Available in versions >= 1.6*]

Timestamp functions
~~~~~~~~~~~~~~~~~~~

* ``LESS_EQ_TIMESTAMP : 'Timestamp' → 'Timestamp' → 'Bool'``

  Returns ``'True'`` if the first timestamp is less or equal than the
  second, ``'False'`` otherwise.

* ``GREATER_EQ_TIMESTAMP : 'Timestamp' → 'Timestamp' → 'Bool'``

  Returns ``'True'`` if the first timestamp is greater or equal than
  the second, ``'False'`` otherwise.

* ``LESS_TIMESTAMP : 'Timestamp' → 'Timestamp' → 'Bool'``

  Returns ``'True'`` if the first timestamp is strictly less than the
  second, ``'False'`` otherwise.

* ``GREATER_TIMESTAMP : 'Timestamp' → 'Timestamp' → 'Bool'``

  Returns ``'True'`` if the first timestamp is strictly greater than
  the second, ``'False'`` otherwise.

* ``EQUAL_TIMESTAMP : 'Timestamp' → 'Timestamp' → 'Bool'``

  Returns ``'True'`` if the first timestamp is equal to the second,
  ``'False'`` otherwise.

* ``TO_TEXT_TIMESTAMP : 'Timestamp' → 'Text'``

  Returns an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_
  compliant string representation of the timestamp.  The actual format
  is as follows. Note that both "``T``" and "``Z``" appear literally
  in the string.  On the one hand "``T``" separates the date part from
  time part, while on the other hand, "``Z``" indicates the zero UTC
  offset. ::

    YYYY-MM-DDThh:mm:ss.SZ

  where:

  * ``YYYY``   = four-digit year
  * ``MM``     = two-digit month (01=January, etc.)
  * ``DD``     = two-digit day of month (01 through 31)
  * ``hh``     = two digits of hour (00 through 23)
  * ``mm``     = two digits of minute (00 through 59)
  * ``ss``     = two digits of second (00 through 59)
  * ``S`` = zero to six digits representing a decimal fraction of a
    second. In case of zero digits the preceding full stop ("``.``")
    is omitted.

  Note the exact number of digits used to represent the decimal fraction of
  a second is not specified, however, it is guaranteed:

  * The output uses at least as many digits as necessary but may be
    padded on the right with an unspecified number of "``0``".

  * The output will not change within minor version of DAML-LF 1.


Date functions
~~~~~~~~~~~~~~

* ``LESS_EQ_DATE : 'Date' → 'Date' → 'Bool'``

  Returns ``'True'`` if the first date is less or equal than the
  second, ``'False'`` otherwise.

* ``GREATER_EQ_DATE : 'Date' → 'Date' → 'Bool'``

  Returns ``'True'`` if the first date is greater or equal than the
  second, ``'False'`` otherwise.

* ``LESS_DATE : 'Date' → 'Date' → 'Bool'``

  Returns ``'True'`` if the first date is strictly less than the
  second, ``'False'`` otherwise.

* ``GREATER_DATE : 'Date' → 'Date' → 'Bool'``

  Returns ``'True'`` if the first date is strictly greater than the
  second, ``'False'`` otherwise.

* ``EQUAL_DATE : 'Date' → 'Date' → 'Bool'``

  Returns ``'True'`` if the first date is equal to the second,
  ``'False'`` otherwise.

* ``TO_TEXT_DATE : 'Date' → 'Text'``

  Returns an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_
  compliant string representation of the timestamp date.  The actual
  format is as follows. ::

    YYYY-MM-DD

  where:

  * ``YYYY``   = four-digit year
  * ``MM``     = two-digit month (01=January, etc.)
  * ``DD``     = two-digit day of month (01 through 31)

Party functions
~~~~~~~~~~~~~~~

.. note:: Since version 1.1, DAML-LF provides four built-in comparison
   functions, which impose a *total order* on party literals.  This
   order is left unspecified. However, it is guaranteed to not change
   within minor version of DAML-LF 1.

   For this reason, it is recommended to *not* store lists sorted using
   this ordering, since the ordering might change in future versions of
   DAML-LF.

* ``LESS_EQ_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is less or equal than the
  second, ``'False'`` otherwise. [*Available in versions >= 1.1*]

* ``GREATER_EQ_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is greater or equal than the
  second, ``'False'`` otherwise. [*Available in versions >= 1.1*]

* ``LESS_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is strictly less than the
  second, ``'False'`` otherwise. [*Available in versions >= 1.1*]

* ``GREATER_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is strictly greater than the
  second, ``'False'`` otherwise. [*Available in versions >= 1.1*]

* ``EQUAL_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is equal to the second,
  ``'False'`` otherwise.

* ``TO_QUOTED_TEXT_PARTY : 'Party' → 'Text'``

  Returns a single-quoted ``Text`` representation of the party. It
  is equivalent to a call to ``TO_TEXT_PARTY``, followed by quoting
  the resulting ``Text`` with single quotes.

* ``TO_TEXT_PARTY : 'Party' → 'Text'``

  Returns the string representation of the party. This function,
  together with ``FROM_TEXT_PARTY``, forms an isomorphism between
  `PartyId strings <Literals_>`_ and parties. In other words,
  the following equations hold::

    ∀ p. FROM_TEXT_PARTY (TO_TEXT_PARTY p) = 'Some' p
    ∀ txt p. FROM_TEXT_PARTY txt = 'Some' p → TO_TEXT_PARTY p = txt

  [*Available in versions >= 1.2*]

* ``FROM_TEXT_PARTY : 'Text' → 'Optional' 'Party'``

  Given the string representation of the party, returns the party,
  if the input string is a `PartyId strings <Literals_>`_.

  [*Available in versions >= 1.2*]

ContractId functions
~~~~~~~~~~~~~~~~~~~~

* ``EQUAL_CONTRACT_ID  : ∀ (α : ⋆) . 'ContractId' α → 'ContractId' α → 'Bool'``

  Returns ``'True'`` if the first contact id is equal to the second,
  ``'False'`` otherwise.

* ``COERCE_CONTRACT_ID  : ∀ (α : ⋆) (β : ⋆) . 'ContractId' α → 'ContractId' β``

  Returns the given contract id unchanged at a different type.

  [*Available in versions >= 1.5*]

List functions
~~~~~~~~~~~~~~

* ``FOLDL : ∀ (α : ⋆) . ∀ (β : ⋆) . (β → α → β) → β  → 'List' α → β``

  Left-associative fold of a list.

* ``FOLDR : ∀ (α : ⋆) . ∀ (β : ⋆) . (α → β → β) →  β → 'List' α → β``

  Right-associative fold of a list.

* ``EQUAL_LIST : ∀ (α : ⋆) . (α → α → 'Bool') → 'List' α → 'List' α → 'Bool'``

  Returns ``'False'`` if the two lists have different length or the
  elements of the two lists are not pairwise equal according to the
  predicate give as first argument.


Map functions
~~~~~~~~~~~~~

* ``MAP_EMPTY : ∀ α. 'Map' α``

  Returns the empty map.

  [*Available in versions >= 1.3*]

* ``MAP_INSERT : ∀ α.  'Text' → α → 'Map' α → 'Map' α``

  Inserts a new key and value in the map. If the key is already
  present in the map, the associated value is replaced with the
  supplied value.

  [*Available in versions >= 1.3*]

* ``MAP_LOOKUP : ∀ α. 'Text' → 'Map' α → 'Optional' α``

  Looks up the value at a key in the map.

  [*Available in versions >= 1.3*]

* ``MAP_DELETE : ∀ α. 'Text' → 'Map' α → 'Map' α``

  Deletes a key and its value from the map. When the key is not a
  member of the map, the original map is returned.

  [*Available in versions >= 1.3*]

* ``MAP_LIST : ∀ α. 'Map' α → 'List' ⟨ key: 'Text', value: α  ⟩``

  Converts to a list of key/value pairs. The output list is guaranteed to be
  sorted according to the ordering of its keys.

  [*Available in versions >= 1.3*]

* ``MAP_SIZE : ∀ α. 'Map' α → 'Int64'``

  Return the number of elements in the map.

  [*Available in versions >= 1.3*]

Type Representation function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``EQUAL_TYPE_REP`` : 'TypeRep' → 'TypeRep' → 'Bool'``

  Returns ``'True'`` if the first type representation is syntactically equal to
  the second one, ``'False'`` otherwise.

  [*Available in versions >= 1.7*]

Generic Map functions
~~~~~~~~~~~~~~~~~~~~~

* ``GENMAP_EMPTY : ∀ α. ∀ β. 'GenMap' α β``

  Returns the empty generic map.

  [*Available in versions >= 1.dev*]

* ``GENMAP_INSERT : ∀ α. ∀ β.  α → β → 'GenMap' α β → 'GenMap' α β``

  Inserts a new key and value in the map. If the key is already
  present in the map, the associated value is replaced with the
  supplied value.

  This raises an error if the key is not a valid map key. Keys are
  compared according to the rules listed below.

  [*Available in versions >= 1.dev*]

* ``GENMAP_LOOKUP : ∀ α. ∀ β.  α → 'GenMap' α β → 'Optional' α``

  Looks up the value at a key in the map.

  This raises an error if the key is not a valid map key. Keys are
  compared according to the rules listed below.

  [*Available in versions >= 1.dev*]

* ``GENMAP_DELETE : ∀ α. ∀ β.  α → 'GenMap' α β → 'GenMap' α β``

  Deletes a key and its value from the map. When the key is not a
  member of the map, the original map is returned.

  This raises an error if the key is not a valid map key. Keys are
  compared according to the rules listed below.

  [*Available in versions >= 1.dev*]

* ``GENMAP_KEYS : ∀ α. ∀ β.  'GenMap' α β → 'List' α``

  Get the list of keys in the map. The keys are returned by first-insertion
  order, so if you insert key ``x`` before key ``y``, then ``x`` will appear
  before ``y`` in the list.

  [*Available in versions >= 1.dev*]

* ``GENMAP_VALUES : ∀ α. ∀ β.  'GenMap' α β → 'List' β``

  Get the list of values in the map. The values are returned in the same
  order as ``GENMAP_KEYS``, so the ith element of ``GENMAP_KEYS`` maps to
  the ith element of ``GENMAP_VALUES``.

  [*Available in versions >= 1.dev*]

* ``GENMAP_SIZE : ∀ α. ∀ β.  'GenMap' α β → 'Int64'``

  Return the number of elements in the map.

  [*Available in versions >= 1.dev*]


**Validity of Keys:** A key is valid if and only if it is equivalent
to itself according to the relation ``~ᵥ`` defined below. Attempts to
use an invalid key in the operations above always result in a runtime
error.

Of particular note, the following values are never valid keys:

* Lambda expressions ``λ x : τ . e``
* Type abstractions ``Λ α : k . e``
* (Partially applied) built-in functions
* Any value containing an invalid key

**Comparison of Keys:** We define here the relation ``~ᵥ`` on value
expressions that is used for key comparison. This is a partial
equivalence relation over all values, but a (total) equivalence
relation over valid keys.

This relation is not exposed as a builtin function, but it coincides
with the builtin equality for any given type, if that builtin is
defined. ::

                                  ┌──────────┐
  Generic Equivalence Relation    │ e₁ ~ᵥ e₂ │
                                  └──────────┘

  LitInt64₁ and LitInt64₂ have the same value
  ——————————————————————————————————————————————————— GenEqLitInt64
  LitInt64₁ ~ᵥ LitInt64₂

  LitNumeric₁ and LitNumeric₂ have the same scale and value
  ——————————————————————————————————————————————————— GenEqLitNumeric
  LitNumeric₁ ~ᵥ LitNumeric₂

  LitText₁ and LitText₂ have the same value
  ——————————————————————————————————————————————————— GenEqLitText
  LitText₁ ~ᵥ LitText₂

  LitDate₁ and LitDate₂ have the same value
  ——————————————————————————————————————————————————— GenEqLitDate
  LitDate₁ ~ᵥ LitDate₂

  LitTimestamp₁ and LitTimestamp₂ have the same value
  ——————————————————————————————————————————————————— GenEqLitTimestamp
  LitTimestamp₁ ~ᵥ LitTimestamp₂

  cid₁ and cid₂ are the same
  ——————————————————————————————————————————————————— GenEqLitContractId
  cid₁ ~ᵥ cid₂

  ——————————————————————————————————————————————————— GenEqUnit
  () ~ᵥ ()

  ——————————————————————————————————————————————————— GenEqTrue
  'True' ~ᵥ 'True'

  ——————————————————————————————————————————————————— GenEqFalse
  'False' ~ᵥ 'False'

  ——————————————————————————————————————————————————— GenEqListNil
  'Nil' @τ₁ ~ᵥ 'Nil' @τ₂

  e₁ ~ᵥ e₁'
  e₂ ~ᵥ e₂'
  ——————————————————————————————————————————————————— GenEqListCons
  'Cons' @τ e₁ e₂  ~ᵥ 'Cons' @τ' e₁' e₂'

  ——————————————————————————————————————————————————— GenEqOptionNone
  'None' @τ₁ ~ᵥ 'None' @τ₂

  e₁ ~ᵥ e₂
  ——————————————————————————————————————————————————— GenEqOptionSome
  'Some' @τ₁ e₁ ~ᵥ 'Some' @τ₂ e₂

  e₁ ~ᵥ e₁'     …       eₙ ~ᵥ eₙ'
  ——————————————————————————————————————————————————— GenEqRecCon
  Mod:T @τ₁ … @τₙ { f₁ = e₁, …, fₙ = eₙ }
    ~ᵥ Mod:T @τ₁' … @τₙ' { f₁ = e₁', …, fₙ = eₙ' }

  e ~ᵥ e'
  ——————————————————————————————————————————————————— GenEqVariantCon
  Mod:T:V @τ₁ … @τₙ e ~ᵥ Mod:T:V @τ₁' … @τₙ' e'

  ——————————————————————————————————————————————————— GenEqEnumCon
  Mod:T:E ~ᵥ Mod:T:E

  e₁ ~ᵥ e₁'     …       eₙ ~ᵥ eₙ'
  ——————————————————————————————————————————————————— GenEqTupleCon
  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩ ~ᵥ ⟨ f₁ = e₁', …, fₘ = eₘ' ⟩


Conversions functions
~~~~~~~~~~~~~~~~~~~~~

* ``INT64_TO_NUMERIC : ∀ (α : nat) . 'Int64' → 'Numeric' α``

  Returns a numeric representation of the integer.  The scale of the
  output and the output is given by the type parameter `α`. Throws an
  error in case of overflow.

* ``NUMERIC_TO_INT64 : ∀ (α : nat) . 'Numeric' α → 'Int64'``

  Returns the integral part of the given numeric -- in other words,
  rounds towards 0. The scale of the input and the output is given by
  the type parameter `α`.  Throws an error in case of overflow.

* ``TIMESTAMP_TO_UNIX_MICROSECONDS : 'Timestamp' → 'Int64'``

  Converts the timestamp in integer.

* ``UNIX_MICROSECONDS_TO_TIMESTAMP : 'Int64' → 'Date'``

  Converts the integer in a timestamp. Throws an error in case of
  overflow.

* ``DATE_TO_UNIX_DAYS : 'Date' → 'Int64'``

  Converts the date in integer.

* ``UNIX_DAYS_TO_DATE : 'Int64' → 'Date'``

  Converts the integer in date. Throws an error in case of overflow.

Error functions
~~~~~~~~~~~~~~~

* ``ERROR : ∀ (α : ⋆) . 'Text' → α``

  Throws an error with the string as message.


Debugging functions
~~~~~~~~~~~~~~~~~~~

* ``TRACE : ∀ (α : ⋆) . 'Text' → α → α``

  Returns the second argument as is. This function is intended to be
  used for debugging purposes, but note that we do not specify how
  ledger implementations make use of it.


Program serialization
^^^^^^^^^^^^^^^^^^^^^

DAML-LF programs are serialized using `Protocol Buffers
<https://developers.google.com/protocol-buffers/>`_.  The
machine-readable definition of the serialization for DAML-LF major
version 1 can be found in the `daml_lf_1.proto
<../archive/src/main/protobuf/com/digitalasset/daml_lf_dev/daml_lf_1.proto>`_ file.

For the sake of brevity, we do no exhaustively describe how DAML-LF
programs are (un)serialized into protocol buffer. In the rest of this
section, we describe the particularities of the encoding and how
DAML-LF version impacts it.


Specificities of DAML-LF serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Required fields
...............

As a rule of the thumb, all non `oneof fields
<https://developers.google.com/protocol-buffers/docs/proto3#oneof>`_
are required in the serialization. Similarly among fields within the
same oneof definition at least one must be defined.  Exceptions are
exhaustively indicated in the `daml_lf_1.proto
<../archive/da/daml_lf_1.proto>`_ file with comment::

  // *Optional*

The deserialization process will reject any message in which a required
field is missing.


Package hash
............

In order to guarantee the integrity when stored on the drive or
communicated through the network, a package is paired with the hash of
its contents. The function used to produce the hash is specified
explicitly. Currently only SHA256 is supported. Software consuming the
serialized package must recompute the hash and make sure that it
matches with the serialized hash.

Package reference
.................

As commented in the `Identifiers`_ section, the package identifier is
actually the hash of the serialized package's AST. To circumvent the
circular dependency problem when computing the hash, package
identifiers are replaced by the so-called *package references* in
serialized AST. Those references are encoded by the following
message::

  message PackageRef {
    oneof Sum {
      Unit self = 1;
      string package_id_str = 2;
    }
  }

One should use either the field ``self`` to refer the current package or
``package_id_str`` to refer to an external package. During deserialization
``self`` references are replaced by the actual digest of the package in
which it appears.


Template precondition
.....................

The precondition of a template is serialized by an optional field in
the corresponding Protocol buffer message. If this field is undefined,
then the deserialization process will use the expression ``True`` as
default.


Data structure compression
..........................

In order to save space and to limit recursion depth, the serialization
generally “compresses” structures that are often repeated, such as
applications, let bindings, abstractions, list constructors, etc.
However, for the sake of simplicity, the specification presented here
uses a normal binary form.

For example, consider the following message that encodes expression
application ::

   message App {
     Expr fun = 1;
     repeated Expr args = 2;
   }

The message is interpreted as n applications ``(e e₁ … eₙ)`` where
``eᵢ`` is the interpretation of the ``iᵗʰ`` elements of ``args``
(whenever ``1 ≤ i ≤ n``) and ``e`` is the interpretation of ``fun``.

Note that the DAML-LF deserialization process verifies the repeated
fields of those compressed structures are non-empty. For instance, the
previous message can be used only if it encodes at least one
application.

Message fields of compressed structure that should not be empty - such
as the ``args`` field of the ``App`` message - are annotated in the
`daml_lf_1.proto <../archive/da/daml_lf_1.proto>`_ file with the
comments::

  // * must be non empty *


Serialization changes since version 1.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As explained in `Version history`_ section, DAML-LF programs are
accompanied by a number version. This enables the DAML-LF
deserialization process to interpret different versions of the
language in a backward compatibility way. During deserialization, any
encoding that does not follow the minor version provided is rejected.
Below we list, in chronological order, all the changes that have been
introduced to the serialization format since version 1.0


Option type
...........

[*Available in versions >= 1.1*]

DAML-LF 1.1 is the first version that supports option type.

The deserialization process will reject any DAML-LF 1.0 program using
this data structure.


Party ordering
..............

[*Available in versions >= 1.1*]

DAML-LF 1.1 is the first version that supports the built-in functions
``LESS_EQ_PARTY``, ``GREATER_EQ_PARTY``, ``LESS_PARTY``, and
``GREATER_PARTY`` to compare party literals.

The deserialization process will reject any DAML-LF 1.0 program using
those functions.


Function type vs arrow type
...........................

[*Changed in version 1.1*]

Version 1.1 introduces a change in the way function types are
represented.

* In version 1.0, functional type are encoded in a "compressed" way
  using the message `message Type.Fun`. ::

    message Fun {
      repeated Type params = 1;
      Type result = 2;
    }

  This message is interpreted as::

    ('TArrow' τ₁ ('TArrow … ('TArrow' τₙ τ)))

  where `τᵢ` is the interpretation of the ``iᵗʰ`` elements of the
  field ``params`` (whenever ``1 ≤ i ≤ n``) and ``τ`` is the
  interpretation of the ``result`` field.  Note that in this version,
  there is no direct way to encode the built-in type ``'TArrow'``.

* In version 1.1 (or later), the primitive type ``'TArrow'`` is
  directly encoded using the enumeration value ``PrimType.ARROW``.

The deserialization process will reject:

* any DAML-LF 1.0 program that uses the enumeration value
  ``PrimType.ARROW``;
* any DAML-LF 1.1 (or later) program that uses the message
  ``Type.Fun``.


Flexible controllers
....................

[*Available in versions >= 1.2*]

Version 1.2 changes what is in scope when the controllers of a choice are
computed.

* In version 1.1 (or earlier), only the template argument is in scope.

* In version 1.2 (or later), the template argument and the choice argument
  are both in scope.

The type checker will reject any DAML-LF < 1.2 program that tries to access
the choice argument in a controller expression.


Validations
~~~~~~~~~~~

To prevent the engine from running buggy, damaged, or malicious
programs, serialized packages must be validated before execution. Two
validation phases can be distinguished.

* The first phase happens during deserialization itself. It is
  responsible for checking the following points:

  * The declared `hash <Package hash_>`_ of the package matches
    the recomputed hash of its serialization.

  * The format of `identifiers`_ and `literals`_ follow this
    specification.

  * Repeated fields of `Compressed structures <Data structure
    compression_>`_ are non-empty.

  * The encoding complies with the declared `version <version
    history_>`_. For example, optional values are only used in version
    1.1 or later.

  The reader may refer to the `daml_lf_1.proto
  <../archive/da/daml_lf_1.proto>`_ file where those requirements are
  exhaustively described as comments between asterisks (``*``).

* The second phase occurs after the deserialization, on the complete
  abstract syntax tree of the package. It is concerned with the
  `well-formedness <Well formed packages_>`_ of the package.

SHA-256 Hashing
...............

[*Available in versions >= 1.2*]

DAML-LF 1.2 is the first version that supports the built-in functions
``SHA256_TEXT`` to hash string.

The deserialization process will reject any DAML-LF 1.1 (or earlier)
program using this functions.

Contract Key
............

[*Available in versions >= 1.3*]

Since DAML-LF 1.3, a contract key can be associated to a contract at
creation. Subsequently, the contract can be retrieved by the corresponding
key using the update statements ``fetch_by_key`` or
``lookup_by_key``.

DAML-LF 1.3 is the first version that supports the statements
``fetch_by_key`` and ``lookup_by_key``. The key is an optional field
``key`` in the Protocol buffer message ``DefTemplate``

The deserialization process will reject any DAML-LF 1.2 (or earlier)
program using the two statements above or the field ``key`` within
the message ``DefTemplate`` .

Map
...

[*Available in versions >= 1.3*]

The deserialization process will reject any DAML-LF 1.2 (or earlier)
program using the builtin functions : ``MAP_EMPTY``, ``MAP_INSERT``,
``MAP_LOOKUP``, ``MAP_DELETE``, ``MAP_LIST``, ``MAP_SIZE``,

Enum
....

[*Available in versions >= 1.6*]

The deserialization process will reject any DAML-LF 1.5 (or earlier)
program using the field ``enum`` in ``DefDataType`` messages, the
field ``enum`` in  ``CaseAlt`` messages, or the field ``enum_con_str``
in ``Expr`` messages.


String Interning
................

[*Available in versions >= 1.6*]

To provide string sharing, the so-called *string interning* mechanism
allows the strings within messages to be stored in a global table and
referenced by their index.

The field ``Package.interned_strings`` is a list of strings. A
so-called `interned string` is a valid zero-based index of this
list. An `interned string` is interpreted as the string it points to
in ``Package.interned_strings``.

+ An `interned package id` is an `interned string` that can be
  interpreted as a valid `PackageId string`.
+ An `interned party` is an `interned string` that can be interpreted
  as a valid `Party string`.
+ An `interned numeric id` is an `interned string` that can be
  interpreted as a valid `numeric` literal.
+ An `interned text` is an `interned string` interpreted as a text
  literal
+ An `interned identifier` is an `interned string` that can be
  interpreted as a valid `identifier`

Starting from DAML-LF 1.6, the field
``PackageRef.package_id_interned_str`` [*Available in versions >=
1.6*] may be used instead of ``PackageRef.package_id_str`` and it
must be a valid *interned packageId*.

Starting from DAML-LF 1.7, all ``string`` (or ``repeated string``)
fields with the suffix ``_str`` are forbidden. Alternative fields of
type ``int32`` (or ``repeated int32``) with the suffix
``_interned_str`` must be used instead.  Except
``PackageRef.package_id_interned_str`` which is [*Available in
versions >= 1.6*], all fields with suffix ``_interned_str`` are
[*Available in versions >= 1.7*].  The deserialization process will
reject any DAML-LF 1.7 (or later) that does not comply with this
restriction.

Name Interning
..............

[*Available in versions >= 1.7*]

To provide sharing of `names <Identifiers_>`_, the so-called *name
interning* mechanism allows the *names* within messages to be stored
in a global table and be referenced by their index.

``InternedDottedName`` is a non-empty list of valid `interned
identifiers`. Such message is interpreted as the name built from the
sequence the interned identifiers it contains.  The field
``Package.interned_dotted_names`` is a list of such messages. A
so-called `interned name` is a valid zero-based index of this list. An
`interned name` is interpreted as the name built form the `name` it
points to in ``Package.interned_dotted_names``.

Starting from DAML-LF 1.7, all ``DottedName`` (or ``repeated
string``) fields with the suffix ``_dname`` are forbidden. Alternative
fields of type ``int32`` with the suffix ``_interned_dname``
[*Available in versions >= 1.7*] must be used instead. The
deserialization process will reject any DAML-LF 1.7 (or later) that
that does not comply this restriction.

Nat kind and Nat types
......................

[*Available in versions >= 1.7*]

The deserialization process will reject any DAML-LF 1.6 (or earlier)
that uses ``nat`` field in ``Kind`` or ``Type`` messages.

Starting from DAML-LF 1.7 those messages are deserialized to ``nat``
kind and ``nat`` type respectively. The field ``nat`` of ``Type``
message must be a positive integer.

Note that despite there being no concrete way to build Nat types in a
DAML-LF 1.6 (or earlier) program, those are implicitly generated when
reading as Numeric type and Numeric builtin as described in the next
section.

Parametric scaled Decimals
..........................

[*Available in versions >= 1.7*]

DAML-LF 1.7 is the first version that supports parametric scaled
decimals. Prior versions have decimal number with a fixed scale of 10
called Decimal.  Backward compatibility with the current specification
is achieved as follows:

On the one hand, in case of DAML-LF 1.6 (or earlier) archive:

- The ``decimal`` fields of the ``PrimLit`` message must match the
  regexp::

    ``[+-]?\d{1,28}(.[0-9]\d{1-10})?``

  The deserialization process will silently convert any message that
  contains such field to a numeric literal of scale 10. The
  deserialization process will reject any non-compliant program.

- ``PrimType`` message with a field ``decimal`` set are translated to
  ``(Numeric 10)`` type when deserialized.

- Decimal ``BuiltinFunction`` messages are translated as follows :

  + ``ADD_DECIMAL`` message is translated to ``(ADD_NUMERIC @10)``
  + ``SUB_DECIMAL`` message is translated to ``(SUB_NUMERIC @10)``
  + ``MUL_DECIMAL`` message is translated to ``(MUL_NUMERIC @10)``
  + ``DIV_DECIMAL`` message is translated to ``(DIV_NUMERIC @10)``
  + ``ROUND_DECIMAL`` message is translated to ``(ROUND_NUMERIC @10)``
  + ``LESS_EQ_DECIMAL`` message is translated to ``(LESS_EQ_NUMERIC @10)``
  + ``GREATER_EQ_DECIMAL`` message is translated to ``(GREATER_EQ_NUMERIC @10)``
  + ``LESS_DECIMAL`` message is translated to ``(LESS_NUMERIC @10)``
  + ``GREATER_DECIMAL`` message is translated to ``(GREATER_NUMERIC @10)``
  + ``GREATER_DECIMAL`` message is translated to ``(GREATER_NUMERIC @10)``
  + ``EQUAL_DECIMAL`` message is translated to ``(EQUAL_NUMERIC @10)``
  + ``TO_TEXT_DECIMAL`` message is translated to ``(TO_TEXT_NUMERIC @10)``
  + ``FROM_TEXT_DECIMAL`` message is translated to ``(FROM_TEXT_NUMERIC @10)``  [*Available in versions >= 1.5*]
  + ``INT64_TO_DECIMAL`` message is translated to ``(INT64_TO_NUMERIC @10)``
  + ``DECIMAL_TO_INT64`` message is translated to ``(NUMERIC_TO_INT64 @10)``

- Numeric types, literals and builtins cannot be referred directly.
  In other words ``numeric`` fields in ``PrimLit`` and ``PrimType``
  messages must remain unset and Numeric ``BuiltinFunction`` (those
  containing ``NUMERIC`` in their name) are forbidden. The
  deserialization process will reject any DAML-LF 1.6 (or earlier)
  that does not comply those restrictions.

On the other hand, starting from DAML-LF 1.7:

- The ``numeric`` field of the ``PrimLit`` message must match the
  regexp:

  ``[-]?([1-9]\d*|0).\d*``

  with the addition constrains that it contains at most 38 digits
  (ignoring a possibly leading ``0``). The deserialization process
  will use the number of digits on the right of the decimal dot
  as scale when converting the message to numeric literals. The
  deserialization process will reject any non-compliant program.

- Decimal types, literals and builtins cannot be referred directly.
  In other words ``decimal`` fields in ``PrimLit`` and ``PrimType``
  messages must remain unset and Decimal ``BuiltinFunction`` (those
  containing ``DECIMAL`` in their name are forbidden). The
  deserialization process will reject any DAML-LF 1.7 (or later)
  that does not comply those restrictions.

Any type and type representation
................................

DAML-LF 1.7 is the first version that supports any type and
type representation.

The deserialization process will reject any DAML-LF 1.0 program using
this data structure.



.. Local Variables:
.. eval: (flyspell-mode 1)
.. eval: (set-input-method "TeX")
.. End:
