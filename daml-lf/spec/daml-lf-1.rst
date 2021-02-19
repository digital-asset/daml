.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Copyright © 2020, `Digital Asset (Switzerland) GmbH
<https://www.digitalasset.com/>`_ and/or its affiliates.  All rights
reserved.

Daml-LF 1 specification
=======================

.. contents:: Contents


Introduction
^^^^^^^^^^^^

This document specifies version 1 of the Daml-LF language — the
language that Daml ledgers execute. Daml compiles to Daml-LF which
executes on Daml ledgers, similar to how Java compiles to JVM byte
code which executes on the JVM. “LF” in Daml-LF stands for “Ledger
Fragment”. Daml-LF is a small, strongly typed, functional language
with strict evaluation that includes native representations for core
Daml concepts such as templates, updates, and parties. It is primarily
intended as a compilation target.


How to view and edit this document
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To view this document correctly, we recommend you install the `DejaVu
Sans family of fonts <https://dejavu-fonts.github.io/>`_, which is
free (as in freedom) and provide exceptional Unicode coverage. The
sphinx style sheets specify DejaVu Sans Mono as the font to use for
code, and if you want to view/edit this section you should use it
for your editor, too.

If you want to edit this section comfortably, we highly recommend using
either VS Code' ``latex-input`` extension or Emacs' TeX input mode.
In VS Code, you can start typing, say, ``\Gamma`` and the autocompleter
will suggest ``Γ``. Similarly ``\to``, ``\->`` and ``\rightarrow`` will
all lead to ``→``. You might need to explicitly trigger the
autocompleter using ``Ctrl+Space``. All autocompletions are triggered
by (one of) their LaTeX names. You can also trigger autocompletions for
subscripts by typing ``\_1`` for ``₁``, ``\_i`` for ``ᵢ``, etc. We have
have added a couple of extra symbols in
``.vscode/restructuredtext.code-snippets`` file. If you want to add
further symbols that's where they could go.

If you want to use Emacs' TeX input mode, , you can turn it on using ``M-x
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

The Daml-LF language is versioned using a major and minor component.
Increasing the major component allows us to drop features, change
the semantics of existing features, or update the serialization format.
Changes to the minor component cannot break backward compatibility,
and operate on the same major version of the serialization format in
a backward compatible way. This document describes Daml-LF major version
1, including all its minor versions.

Starting from SDK 1.0 release, Daml-LF versions older than 1.6 are
deprecated. An engine compliant with the present specification must handle
all versions newer than or equal to Daml-LF 1.6, no requirement is made on
handling older version.

Each Daml-LF program is accompanied by the version identifier of the
language it was serialized in. This number enables the Daml-LF engine
to interpret previous versions of the language in a backward
compatibility way.

In the following of this document, we will use annotations between
square brackets such as *[Available in version < x.y]*, *[Available in
versions >= x.y]*, and *[Changed in version x.y]* to emphasize that a
particular feature is concerned with a change introduced in Daml x.y
version. In addition, we will mark lines within inference rules with
annotations of the form ``[Daml-LF < x.y]`` and ``[Daml-LF ≥ x.y]`` to
make the respective line conditional upon the Daml-LF version.

A *preview* version is an snapshot of the next 1.x version to be
released. It is provided for beta testing purpose and may only be
changed to include bug fixes.  On the other hand, the *development*
version is a special staging area for the development of upcoming
version 1.x version.  It may be used for alpha testing, and can be
changed without notice. Compliant implementations are not required to
implement any features exclusive to development version, but should
take them under advisement as likely elements of the next 1.x version.

Below, we list the versions of Daml-LF 1.x that a Daml-LF engine
compliant with the present specification must handle, in ascending
order. The optional preview version is marked with the tag *(preview)*
while the development version is marked with the tag *(development)*.
Conventionally development version is call 1.dev.  The list comes with
a brief description of the changes, and some links to help unfamiliar
readers learn about the features involved in the change.  One can
refer also to the `Serialization` section which is particularly
concerned about versioning and backward compatibility.

Support for language versions 1.0 to 1.5 was dropped on 2020-11-30.
This breaking change does not impact ledgers created with SDK 1.0.0 or
later.


Version: 1.6
............

* Introduction date:

    2019-07-01

* Description:

  + Initial version

Version: 1.7
............

* Introduction date:

    2019-11-07

* Description:

  + **Add** Nat kind and Nat type.

    - add `nat` kind
    - add `nat` type

  + **Add** parametrically scaled Numeric type.

    - add `NUMERIC` primitive type
    - add `numeric` primitive literal
    - add numeric builtins, namely `ADD_NUMERIC`, `SUB_NUMERIC`,
      `MUL_NUMERIC`, `DIV_NUMERIC`, `ROUND_NUMERIC`, `CAST_NUMERIC`,
      `SHIFT_NUMERIC`, `LEQ_NUMERIC`, `LESS_NUMERIC`, `GEQ_NUMERIC`,
      `GREATER_NUMERIC`, `FROM_TEXT_NUMERIC`, `TO_TEXT_NUMERIC`,
      `INT64_TO_NUMERIC`, `NUMERIC_TO_INT64`, `EQUAL_NUMERIC`

  + **Drop** support for Decimal type. Use Numeric of scale 10 instead.

    - drop `DECIMAL` primitive type
    - drop `decimal` primitive literal
    - drop decimal builtins, namely `ADD_DECIMAL`, `SUB_DECIMAL`,
      `MUL_DECIMAL`, `DIV_DECIMAL`, `ROUND_DECIMAL`, `LEQ_DECIMAL`,
      `LESS_DECIMAL`, `GEQ_DECIMAL`, `GREATER_DECIMAL`,
      `FROM_TEXT_DECIMAL`, `TO_TEXT_DECIMAL`, `INT64_TO_DECIMAL`,
      `DECIMAL_TO_INT64`, `EQUAL_DECIMAL`

  + **Add** string interning in external package references.

  + **Add** name interning in external package references.

  + **Add** existential ``Any`` type

    - add `'Any'` primitive type
    - add `'to_any'` and `'from_any'` expression to convert from/to an
      arbitrary ground type (i.e. a type with no free type variables)
      to ``Any``.

  + **Add** for Type representation.

    - add `'TypeRep'` primitive type
    - add `type_rep` expression to reify a arbitrary ground type
      (i.e. a type with no free type variables) to a value.

Version: 1.8
............

* Introduction date:

    2020-03-02

* Description:

  + **Add** type synonyms.

  + **Add** package metadata.

  + **Rename** structural records from ``Tuple`` to ``Struct``.

  + **Rename** ``Map`` to ``TextMap``.

Version: 1.11
.............

* Introduction date:

    2020-12-14

* Description:

  + **Add** generic equality builtin.

  + **Add** generic order builtin.

  + **Add** generic map type ``GenMap``.

  + **Add** ``TO_TEXT_CONTRACT_ID`` builtin.

  + **Add** `exercise_by_key` Update.

  + **Add** choice observers.


Version: 1.12
.............

* Introduction date:

     2021-01-2

* Description:

  + Drop type constructor in serialized variant and enumeration
    values. Drop type constructor and field names in serialized record
    values. See value version 12 in value specification for more
    details


Version: 1.dev (development)
............................

  + **Add** exception handling.
  + **Add** BigDecimal type.

    - add `BigDecimal` primitive type
    - Add `bigdecimal` primitive literal
    - add `MathContext` primitive type
    - add `RoundingMode` primitive type

Abstract syntax
^^^^^^^^^^^^^^^

This section specifies the abstract syntax tree of a Daml-LF
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

Identifiers
~~~~~~~~~~~
In this section, we define the sorts of strings and identifiers that appear in
Daml-LF programs.

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
<https://en.wikipedia.org/wiki/Unicode>`_ code points where the line
feed character ``\n``, the carriage return character ``\r``, the
double quote character ``\"``, and the backslash character ``\\`` must
be escaped with backslash ``\\``. Daml-LF considers legal `Unicode
code point <https://unicode.org/glossary/#code_point>`_ that is not a
`Surrogate Code Point
<https://unicode.org/glossary/#surrogate_code_point>`_, in other words
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
     PartyIdString  ∈  [a-zA-Z0-9:\-_ ]{1,255}       -- PartyIdChar

  PackageName strings
   PackageNameString ∈ [a-zA-Z0-9:\-_]+             -- PackageNameString

  PackageVersion strings
   PackageVersionString  ∈ (0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))* – PackageVersionString


We can now define a generic notion of *identifier* and *name*::

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
Daml-LF as a "not an Ident" notation, so should not be considered for
future addition to allowed identifier characters.

In the following, we will use identifiers to represent *built-in
functions*, term and type *variable names*, record and struct *field
names*, *variant constructors* and *template choices*. On the other
hand, we will use names to represent *type constructors*, *type synonyms*, *value
references*, and *module names*. Finally, we will use PackageId
strings as *package identifiers*.  ::

  Expression variables
        x, y, z ::= Ident                           -- VarExp

  Type variables
           α, β ::= Ident                           -- VarTy

  Built-in function names
              F ::= Ident                           -- Builtin

  Record and struct field names
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
              T ::= Name                            -- TyCon

  Type synonym
              S ::= Name                            -- TySyn

  Module names
        ModName ::= Name                            -- ModName

  Package identifiers
           pid  ::=  PackageIdString                -- PkgId

  Package names
           pname ::= PackageNameString              -- PackageName

  Package versions
           pversion ::= PackageVersionString        -- PackageVersion

  V0 Contract identifiers:
          cidV0  ∈  #[a-zA-Z0-9\._:-#/ ]{0,254}     -- V0ContractId

  V1 Contract identifiers:
          cidV1  ∈  00([0-9a-f][0-9a-f]){32,126}    -- V1ContractId

Contract identifiers can be created dynamically through interactions
with the underlying ledger. See the `operation semantics of update
statements <Update Interpretation_>`_ for the formal specification of
those interactions. Depending on its configuration, a Daml-LF engine
can produce V0 or V1 contract identifiers.  When configured to produce
V0 contract identifiers, a Daml-LF compliant engine must refuse to
load any Daml-LF >= 1.11 archives.  On the contrary, when configured
to produce V1 contract IDs, a Daml-LF compliant engine must accept to
load any non-deprecated Daml-LF version. V1 Contract IDs allocation
scheme is described in the `V1 Contract ID allocation
scheme specification <./contract-id.rst>`_.

Also note that package identifiers are typically `cryptographic hash
<Package hash_>`_ of the content of the package itself.

Literals
~~~~~~~~

We now define all the literals that a program can handle::

  Nat type literals:                                 -- LitNatType
       n ∈  \d+

  64-bit integer literals:
        LitInt64  ∈  (-?)\d+                         -- LitInt64

  Numeric literals:
      LitNumeric  ∈  ([+-]?)([1-9]\d+|0).\d*         -- LitNumeric

   Date literals:
         LitDate  ∈  \d{4}-\d{2}-\d{2}               -- LitDate

  UTC timestamp literals:
     LitTimestamp ∈  \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{1,3})?Z
                                                     -- LitTimestamp
  UTF8 string literals:
               t ::= String                          -- LitText

  Party literals:
        LitParty ::= PartyIdString                   -- LitParty

  Contract ID literals:
        cid   ::= cidV0 | cidV1                      -- LitCid

  Rounding Mode Literals:
        LitRoundingMode ::=
          | 'RoundingCeiling'
          | 'RoundingFloor'
          | 'RoundingDown'
          | 'RoundingUp'
          | 'RoundingHalfDown'
          | 'RoundingHalfEven'
          | 'RoundingHalfUp'
          | 'RoundingHalfUnnecessary'

The literals represent actual Daml-LF values:

* A ``LitNatType`` represents a natural number between ``0`` and
  ``38``, bounds inclusive.
* A ``LitInt64`` represents a standard signed 64-bit integer (integer
  between ``−2⁶³`` to ``2⁶³−1``).
* A ``LitNumeric`` represents a signed number that can be represented
  in base-10 without loss of precision with at most 38 digits
  (ignoring possible leading 0) and with a scale (the number of
  significant digits on the right of the decimal point) between ``0``
  and ``37`` (bounds inclusive). In the following, we will use
  ``scale(LitNumeric)`` to denote the scale of the decimal number.

* A ``LitBigNumeric`` represents a signed number that can be represented
  as a product `i * 10^-s` where `i` (the *unscaled value* of the number) is a
  integer not divisible by ten and `s` (the *scale* of the number) is a arbitrary integer.
  The *precision* of such number is the number of digit in base-10 of its
  unscaled value. By convention the scale and the precision of zero are 0.

.. TODO specify when a numeric is valid.
   For now we assume there is some predicate `valid` that checks a number
   is within a subset of all the possible BiNumeric numbers and that:
   - java BigDecimal operation never underflow/overflow
   - It is not too expensive to create any of those number for actual testing.


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

Number-like literals (``LitNatTyp``, ``LitInt64``,
``LitNumeric``, ``LitBigNumeric``, ``LitDate``, ``LitTimestamp``) are ordered by natural
ordering. Text-like literals (``LitText``, ``LitParty``, and
``Contract ID``) are ordered lexicographically. Note that in the ASCII
encoding, the character ``#`` comes before digits, meaning V0 Contract
ID are ordered before V1 Contract ID. In the following we will denote
the corresponding (non-strict) order by ``≤ₗ``.

Kinds, types, and expressions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. TODO We might want to consider changing the syntax for ``Mod``,
   since in our software we use the colon to separate the module name
   from the definition name inside the module.

Then we can define our kinds, types, and expressions::

  Kinds
    k
      ::= 'nat'                                     -- KindNat  [Daml-LF ≥ 1.7]
       | ek                                         -- KindErasable

  Erasable Kind
    ek
      ::= ⋆                                         -- KindStar
       | k → ek                                     -- KindArrow

  Module references
    Mod
      ::= PkdId:ModName                             -- ModPackage: module from a package

  Built-in types
    BuiltinType
      ::= 'TArrow'                                  -- BTArrow: Arrow type
       |  'Int64'                                   -- BTyInt64: 64-bit integer
       |  'Numeric'                                 -- BTyNumeric: numeric, precision 38, parametric scale between 0 and 37
       |  'BigNumeric'                              -- BTyBigNumeric: arbitrary precision decimal
       |  'RoundingMode'                            -- BTyRoundingMode: rounding mode to control BigNumeric operations.
       |  'Text'                                    -- BTyText: UTF-8 string
       |  'Date'                                    -- BTyDate
       |  'Timestamp'                               -- BTyTime: UTC timestamp
       |  'Party'                                   -- BTyParty
       |  'Date'                                    -- BTyDate: year, month, date triple
       |  'Unit'                                    -- BTyUnit
       |  'Bool'                                    -- BTyBool
       |  'List'                                    -- BTyList
       |  'Optional'                                -- BTyOptional
       |  'TextMap'                                 -- BTTextMap: map with string keys
       |  'GenMap'                                  -- BTGenMap: map with general value keys [Daml-LF ≥ 1.11]
       |  'ContractId'                              -- BTyContractId
       |  'Any'                                     -- BTyAny [Daml-LF ≥ 1.7]
       |  'TypeRep'                                 -- BTTypeRep [Daml-LF ≥ 1.7]
       |  'Update'                                  -- BTyUpdate
       |  'Scenario'                                -- BTyScenario
       |  'AnyException'                            -- BTyAnyException [Daml-LF ≥ 1.dev]
       |  'GeneralError'                            -- BTyGeneralError [Daml-LF ≥ 1.dev]
       |  'ArithmeticError'                         -- BTyArithmeticError [Daml-LF ≥ 1.dev]
       |  'ContractError'                           -- BTyContractError [Daml-LF ≥ 1.dev]

  Types (mnemonic: tau for type)
    τ, σ
      ::= α                                         -- TyVar: Type variable
       |  n                                         -- TyNat: Nat Type [Daml-LF ≥ 1.7]
       |  τ σ                                       -- TyApp: Type application
       |  ∀ α : k . τ                               -- TyForall: Universal quantification
       |  BuiltinType                               -- TyBuiltin: Builtin type
       |  Mod:T                                     -- TyCon: type constructor
       |  |Mod:S τ₁ … τₘ|                           -- TySyn: type synonym [Daml-LF ≥ 1.8]
       |  ⟨ f₁: τ₁, …, fₘ: τₘ ⟩                     -- TyStruct: Structural record type

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
       |  LitInt64                                  -- ExpLitInt64: 64-bit integer literal
       |  LitNumeric                                -- ExpLitNumeric: Numeric literal
       |  LitBigNumeric                             -- ExpLitBigNumeric: BigNumeric literal
       |  t                                         -- ExpLitText: UTF-8 string literal
       |  LitDate                                   -- ExpLitDate: Date literal
       |  LitTimestamp                              -- ExpLitTimestamp: UTC timestamp literal
       |  LitParty                                  -- ExpLitParty: Party literal
       |  cid                                       -- ExpLitContractId: Contract identifiers
       |  LitRoundingMode                           -- ExpLitRoundingMode: Rounding Mode
       |  F                                         -- ExpBuiltin: Builtin function
       |  Mod:W                                     -- ExpVal: Defined value
       |  Mod:T @τ₁ … @τₙ { f₁ = e₁, …, fₘ = eₘ }   -- ExpRecCon: Record construction
       |  Mod:T @τ₁ … @τₙ {f} e                     -- ExpRecProj: Record projection
       |  Mod:T @τ₁ … @τₙ { e₁ 'with' f = e₂ }      -- ExpRecUpdate: Record update
       |  Mod:T:V @τ₁ … @τₙ e                       -- ExpVariantCon: Variant construction
       |  Mod:T:E                                   -- ExpEnumCon:Enum construction
       |  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩                   -- ExpStructCon: Struct construction
       |  e.f                                       -- ExpStructProj: Struct projection
       |  ⟨ e₁ 'with' f = e₂ ⟩                      -- ExpStructUpdate: Struct update
       |  'Nil' @τ                                  -- ExpListNil: Empty list
       |  'Cons' @τ e₁ e₂                           -- ExpListCons: Cons list
       |  'None' @τ                                 -- ExpOptionalNone: Empty Optional
       |  'Some' @τ e                               -- ExpOptionalSome: Non-empty Optional
       |  [t₁ ↦ e₁; …; tₙ ↦ eₙ]                     -- ExpTextMap
       | 〚e₁ ↦ e₁; …; eₙ ↦ eₙ'〛                    -- ExpGenMap [Daml-LF ≥ 1.11]
       | 'to_any' @τ e                              -- ExpToAny: Wrap a value of the given type in Any [Daml-LF ≥ 1.7]
       | 'from_any' @τ e                            -- ExpToAny: Extract a value of the given from Any or return None [Daml-LF ≥ 1.7]
       | 'type_rep' @τ                              -- ExpToTypeRep: A type representation [Daml-LF ≥ 1.7]
       |  u                                         -- ExpUpdate: Update expression
       |  s                                         -- ExpScenario: Scenario expression
       | 'throw' @σ @τ e                            -- ExpThrow: throw exception [Daml-LF ≥ 1.dev]
       | 'to_any_exception' @τ e                    -- ExpToAnyException: Turn a concrete exception into an 'AnyException' [Daml-LF ≥ 1.dev]
       | 'from_any_exception' @τ e                  -- ExpFromAnyException: Extract a concrete exception from an 'AnyException' [Daml-LF ≥ 1.dev]

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
       |  'exercise_by_key' @Mod:T Ch e₁ e₂         -- UpdateExerciseByKey [Daml-LF ≥ 1.11]
       |  'get_time'                                -- UpdateGetTime
       |  'fetch_by_key' @τ e                       -- UpdateFecthByKey
       |  'lookup_by_key' @τ e                      -- UpdateLookUpByKey
       |  'embed_expr' @τ e                         -- UpdateEmbedExpr
       |  'try' @τ e₁ 'catch' x. e₂                 -- UpdateTryCatch [Daml-LF ≥ 1.dev]

  Scenario
    s ::= 'spure' @τ e                              -- ScenarioPure
       |  'sbind' x₁ : τ₁ ← e₁ 'in' e₂              -- ScenarioBlock
       |  'commit' @τ e u                           -- ScenarioCommit
       |  'must_fail_at' @τ e u                     -- ScenarioMustFailAt
       |  'pass' e                                  -- ScenarioPass
       |  'sget_time'                               -- ScenarioGetTime
       |  'sget_party' e                            -- ScenarioGetParty
       |  'sembed_expr' @τ e                        -- ScenarioEmbedExpr

.. note:: The explicit syntax for maps (cases ``ExpTextMap`` and
  ``ExpGenMap``) is forbidden in serialized programs. It is specified
  here to ease the definition of `values`_, `operational semantics`_
  and `value comparison <Generic comparison functions_>`_. In practice,
  `text map functions`_ and `generic map functions`_ are the only way
  to create and handle those objects.

.. note:: The order of entries in maps (cases ``ExpTextMap`` and
  ``ExpGenMap``) is always significant. For text maps, the entries
  should be always ordered by keys. On the other hand, the order of
  entries in generic maps indicate the order in which the keys have
  been inserted into the map.

.. note:: The distinction between kinds and erasable kinds is significant,
  because erasable kinds have no runtime representation. This affects the
  operational semantics. The right hand side of an arrow is always erasable.

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
    ChDef ::= 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ e
                                                    -- ChDef

  Definitions
    Def
      ::=
       |  'record' T (α₁: k₁)… (αₙ: kₙ) ↦ { f₁ : τ₁, …, fₘ : τₘ }
                                                    -- DefRecord: Nominal record type
       |  'variant' T (α₁: k₁)… (αₙ: kₙ) ↦ V₁ : τ₁ | … | Vₘ : τₘ
                                                    -- DefVariant
       |  'enum' T  ↦ E₁ | … | Eₘ                   -- DefEnum
       |  'synonym' S (α₁: k₁)… (αₙ: kₙ) ↦ τ        -- DefTypeSynonym
       |  'val' W : τ ↦ e                           -- DefValue
       |  'tpl' (x : T) ↦                           -- DefTemplate
            { 'precondition' e₁
            , 'signatories' e₂
            , 'observers' e₃
            , 'agreement' e₄
            , 'choices' { ChDef₁, …, ChDefₘ }
            , KeyDef
            }
       |  'exception' T ↦ { 'message' e }           -- DefException [Daml-LF ≥ 1.dev]

  Module (mnemonic: delta for definitions)
    Δ ::= ε                                         -- DefCtxEmpty
       |  Def · Δ                                   -- DefCtxCons

  PackageMetadata
    PackageMetadata ::= 'metadata' PackageNameString PackageVersionString -- PackageMetadata

  PackageModules
    PackageModules ∈ ModName ↦ Δ                           -- PackageModules

  Package
    Package ::= Package PackageModules PackageMetadata – since Daml-LF 1.8
    Package ::= Package PackageModules -- until Daml-LF 1.8

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
 | ForbidPartyLiterals                       | Party literals are not allowed in a Daml-LF module.      |
 |                                           | (See `Party Literal restriction`_ for more details)      |
 +-------------------------------------------+----------------------------------------------------------+
 | DontDivulgeContractIdsInCreateArguments   | contract IDs captured in ``create`` arguments are not    |
 |                                           | divulged, ``fetch`` is authorized if and only if the     |
 |                                           | authorizing parties contain at least one stakeholder of  |
 |                                           | the fetched contract ID.                                 |
 |                                           | The contract ID on which a choice is exercised           |
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


Type normalization
..................

First, we define the type normalization relation ``↠`` over types,
which inlines type synonym definitions, and normalizes struct types
to remove dependence on the order of fields ::

  ——————————————————————————————————————————————— RewriteVar
   α  ↠  α

  ——————————————————————————————————————————————— RewriteNat
   n  ↠  n

  ——————————————————————————————————————————————— RewriteBuiltin
   BuiltinType ↠ BuiltinType

  ———————————————————————————————————————————————— RewriteTyCon
   Mod:T ↠  Mod:T

   'synonym' S (α₁:k₁) … (αₙ:kₙ) ↦ τ  ∈ 〚Ξ〛Mod
   τ  ↠  σ      τ₁  ↠  σ₁  ⋯  τₙ  ↠  σₙ
  ——————————————————————————————————————————————— RewriteSynonym
   |Mod:S τ₁ … τₙ|   ↠   σ[α₁ ↦ σ₁, …, αₙ ↦ σₙ]

   τ₁ ↠ σ₁   ⋯   τₙ  ↠  σₙ
   [f₁, …, fₘ] sorts lexicographically to [fⱼ₁, …, fⱼₘ]
  ———————————————————————————————————————————————— RewriteStruct
   ⟨ f₁: τ₁, …, fₘ: τₘ ⟩ ↠ ⟨ fⱼ₁: σⱼ₁, …, fⱼₘ: σⱼₘ ⟩

   τ₁  ↠  σ₁        τ₂  ↠  σ₂
  ———————————————————————————————————————————————— RewriteApp
   τ₁ τ₂  ↠  σ₁ σ₂

   τ  ↠  σ
  ———————————————————————————————————————————————— RewriteForall
   ∀ α : k . τ  ↠  ∀ α : k . σ



Note that the relation ``↠`` defines a partial normalization function
over types as soon as:

1. there is at most one definition for a type synonym ``S`` in each
   module

2. there is no cycles between type synonym definitions.

These two properties will be enforced by the notion of
`well-formedness <Well-formed packages_>`_ defined below.

Note ``↠`` is undefined on type contains an undefined type synonym or
a type synonym applied to a wrong number. Such types are assumed non
well-formed and will be rejected by the Daml-LF type checker.


Well-formed types
.................

We now formally defined *well-formed types*. ::

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

     Γ  ⊢  τ  :  k₁ → k₂      Γ  ⊢  σ  :  k₁
   ————————————————————————————————————————————— TyApp
     Γ  ⊢  τ σ  :  k₂

     α : k · Γ  ⊢  τ : ⋆
   ————————————————————————————————————————————— TyForall
     Γ  ⊢  ∀ α : k . τ  :  ⋆

   ————————————————————————————————————————————— TyArrow
     Γ  ⊢  'TArrow' : ⋆ → ⋆

   ————————————————————————————————————————————— TyUnit
     Γ  ⊢  'Unit' : ⋆

   ————————————————————————————————————————————— TyBool
     Γ  ⊢  'Bool' : ⋆

   ————————————————————————————————————————————— TyInt64
     Γ  ⊢  'Int64' : ⋆

   ————————————————————————————————————————————— TyNumeric
     Γ  ⊢  'Numeric' : 'nat' → ⋆

   ————————————————————————————————————————————— TyBigNumeric
     Γ  ⊢  'BigNumeric' : ⋆

   ————————————————————————————————————————————— TyRoundingMode
     Γ  ⊢  'RoundingMode' : ⋆

   ————————————————————————————————————————————— TyText
     Γ  ⊢  'Text' : ⋆

   ————————————————————————————————————————————— TyDate
     Γ  ⊢  'Date' : ⋆

   ————————————————————————————————————————————— TyTimestamp
     Γ  ⊢  'Timestamp' : ⋆

   ————————————————————————————————————————————— TyParty
     Γ  ⊢  'Party' : ⋆

   ————————————————————————————————————————————— TyList
     Γ  ⊢  'List' : ⋆ → ⋆

   ————————————————————————————————————————————— TyOptional
     Γ  ⊢  'Optional' : ⋆ → ⋆

   ————————————————————————————————————————————— TyTextMap
     Γ  ⊢  'TextMap' : ⋆ → ⋆

   ————————————————————————————————————————————— TyGenMap
     Γ  ⊢  'GenMap' : ⋆ → ⋆ → ⋆

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
     f₁ < … < fₙ lexicographically
   ————————————————————————————————————————————— TyStruct
     Γ  ⊢  ⟨ f₁: τ₁, …, fₙ: τₙ ⟩  :  ⋆

   ————————————————————————————————————————————— TyUpdate
     Γ  ⊢  'Update' : ⋆ → ⋆

   ————————————————————————————————————————————— TyScenario
     Γ  ⊢  'Scenario' : ⋆ → ⋆

   ————————————————————————————————————————————— TyAnyException [Daml-LF ≥ 1.dev]
     Γ  ⊢  'AnyException' : ⋆

   ————————————————————————————————————————————— TyGeneralError [Daml-LF ≥ 1.dev]
     Γ  ⊢  'GeneralError' : ⋆

   ————————————————————————————————————————————— TyArithmeticError [Daml-LF ≥ 1.dev]
     Γ  ⊢  'ArithmeticError' : ⋆

   ————————————————————————————————————————————— TyContractError [Daml-LF ≥ 1.dev]
     Γ  ⊢  'ContractError' : ⋆


Exception types
...............

To state the typing rules related to exception handling, we need the notion of
*exception types*. As the name suggests, values of these types are the ones that
can be thrown and caught by the exception handling mechanism. ::

                      ┌────────┐
  Exception types     │ ⊢ₑ  τ  │
                      └────────┘

      'exception' T ↦ …  ∈  〚Ξ〛Mod
    ———————————————————————————————————————————————————————————————— ExnTyDefException
      ⊢ₑ  Mod:T

    ———————————————————————————————————————————————————————————————— ExnTyGeneralError
      ⊢ₑ  'GeneralError'

    ———————————————————————————————————————————————————————————————— ExnTyArithmeticError
      ⊢ₑ  'ArithmeticError'

    ———————————————————————————————————————————————————————————————— ExnTyContractError
      ⊢ₑ  'ContractError'

Note that ``'AnyException'`` is not an exception type in order to avoid having
``'AnyException'`` wrapped into ``'AnyException'``.


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

      τ ↠ τ'     Γ  ⊢  τ'  :  k      Γ  ⊢  e  :  ∀ α : k . σ
    ——————————————————————————————————————————————————————————————— ExpTyApp
      Γ  ⊢  e @τ  :  σ[α ↦ τ']

      τ ↠ τ'      x : τ' · Γ  ⊢  e  :  σ     Γ  ⊢ τ'  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpAbs
      Γ  ⊢  λ x : τ . e  :  τ' → σ

      α : k · Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpTyAbs
      Γ  ⊢  Λ α : k . e  :  ∀ α : k . τ

      τ ↠ τ'      Γ  ⊢  e₁  :  τ'      Γ  ⊢  τ'  :  ⋆
      x : τ' · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpLet
      Γ  ⊢  'let' x : τ = e₁ 'in' e₂  :  σ

    ——————————————————————————————————————————————————————————————— ExpUnit
      Γ  ⊢  ()  :  'Unit'

    ——————————————————————————————————————————————————————————————— ExpTrue
      Γ  ⊢  'True'  :  'Bool'

    ——————————————————————————————————————————————————————————————— ExpFalse
      Γ  ⊢  'False'  :  'Bool'

      τ ↠ τ'      Γ  ⊢  τ'  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpListNil
      Γ  ⊢  'Nil' @τ  :  'List' τ'

      τ ↠ τ'
      Γ  ⊢  τ'  :  ⋆     Γ  ⊢  eₕ  :  τ'     Γ  ⊢  eₜ  :  'List' τ'
    ——————————————————————————————————————————————————————————————— ExpListCons
      Γ  ⊢  'Cons' @τ eₕ eₜ  :  'List' τ'

      τ ↠ τ'     Γ  ⊢  τ'  :  ⋆
     —————————————————————————————————————————————————————————————— ExpOptionalNone
      Γ  ⊢  'None' @τ  :  'Optional' τ'

      τ ↠ τ'     Γ  ⊢  τ'  :  ⋆     Γ  ⊢  e  :  τ'
    ——————————————————————————————————————————————————————————————— ExpOptionalSome
      Γ  ⊢  'Some' @τ e  :  'Optional' τ'


      ∀ i,j ∈ 1, …, n  i > j ∨ tᵢ ≤ tⱼ
      Γ  ⊢  e₁  :  τ     Γ  ⊢  eₙ :  τ
    ——————————————————————————————————————————————————————————————— ExpTextMap
      Γ  ⊢  [t₁ ↦ e₁; …; tₙ ↦ eₙ] : 'TextMap' τ

      Γ  ⊢  e₁  :  σ      Γ  ⊢  eₙ :  σ
      Γ  ⊢  e₁'  :  τ     Γ  ⊢  eₙ' :  τ
    ——————————————————————————————————————————————————————————————— ExpGenMap (*)
      Γ  ⊢  〚e₁ ↦ e₁'; …; eₙ ↦ eₙ'〛: GenMap σ τ

      τ contains no quantifiers and no type synonyms
      ε  ⊢  τ  :  ⋆     Γ  ⊢  e  : τ
    ——————————————————————————————————————————————————————————————— ExpToAny
      Γ  ⊢  'to_any' @τ e  :  'Any'

      τ contains no quantifiers and no type synonyms
      ε  ⊢  τ  :  ⋆     Γ  ⊢  e  :  'Any'
    ——————————————————————————————————————————————————————————————— ExpFromAny
      Γ  ⊢  'from_any' @τ e  :  'Optional' τ

      τ contains no quantifiers and no type synonyms
      ε  ⊢  τ  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpTypeRep
      Γ  ⊢  'type_rep' @τ  :  'TypeRep'

    ——————————————————————————————————————————————————————————————— ExpBuiltin
      Γ  ⊢  F : 𝕋(F)

    ——————————————————————————————————————————————————————————————— ExpLitInt64
      Γ  ⊢  LitInt64  :  'Int64'

      n = scale(LitNumeric)
    ——————————————————————————————————————————————————————————————— ExpLitNumeric
      Γ  ⊢  LitNumeric  :  'Numeric' n

    ——————————————————————————————————————————————————————————————— ExpBigNumeric
      Γ  ⊢  LitBigNumeric  :  'BigNumeric'

    ——————————————————————————————————————————————————————————————— ExpLitText
      Γ  ⊢  t  :  'Text'

    ——————————————————————————————————————————————————————————————— ExpLitDate
      Γ  ⊢  LitDate  :  'Date'

    ——————————————————————————————————————————————————————————————— ExpLitTimestamp
      Γ  ⊢  LitTimestamp  :  'Timestamp'

    ——————————————————————————————————————————————————————————————— ExpLitParty
      Γ  ⊢  LitParty  :  'Party'

      'tpl' (x : T) ↦ { … }  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpLitContractId
      Γ  ⊢  cid  :  'ContractId' Mod:T

    ——————————————————————————————————————————————————————————————— ExpLitRoundingMode
      Γ  ⊢  LitRoundingMode  :  'RoundingMode'

      τ  ↠  τ'      'val' W : τ ↦ …  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpVal
      Γ  ⊢  Mod:W  :  τ'

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { f₁:τ₁, …, fₘ:τₘ }  ∈ 〚Ξ〛Mod
      σ₁  ↠  σ₁'    ⋯    σₙ  ↠  σₙ'
      Γ  ⊢  σ₁' : k₁    ⋯     Γ  ⊢  σₙ' : kₙ
      τ₁  ↠  τ₁'      Γ  ⊢  e₁ :  τ₁'[α₁ ↦ σ₁', …, αₙ ↦ σₙ']
            ⋮
      τₘ  ↠  τₘ'      Γ  ⊢  eₘ :  τₘ'[α₁ ↦ σ₁', …, αₙ ↦ σₙ']
    ———————————————————————————————————————————————————————————————— ExpRecCon
      Γ  ⊢
        Mod:T @σ₁ … @σₙ { f₁ = e₁, …, fₘ = eₘ }  :  Mod:T σ₁' … σₙ'

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { …, fᵢ : τᵢ, … }  ∈ 〚Ξ〛Mod
      τᵢ  ↠  τᵢ'      σ₁  ↠  σ₁'    ⋯    σₙ  ↠  σₙ'
      Γ  ⊢  σ₁' : k₁    ⋯     Γ  ⊢  σₙ' : kₙ
      Γ  ⊢  e  :  Mod:T σ₁' … σₙ'
    ——————————————————————————————————————————————————————————————— ExpRecProj
      Γ  ⊢  Mod:T @σ₁ … @σₙ {f} e  :  τᵢ'[α₁ ↦ σ₁', …, αₙ ↦ σₙ']

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { …, fᵢ : τᵢ, … }  ∈ 〚Ξ〛Mod
      τᵢ  ↠  τᵢ'      σ₁  ↠  σ₁'    ⋯    σₙ  ↠  σₙ'
      Γ  ⊢  σ₁' : k₁    ⋯     Γ  ⊢  σₙ' : kₙ
      Γ  ⊢  e  :  Mod:T σ₁'  ⋯  σₙ'
      Γ  ⊢  eᵢ  :  τᵢ'[α₁ ↦ σ₁', …, αₙ ↦ σₙ']
    ———————————————————————————————————————————————————————————————– ExpRecUpdate
      Γ  ⊢
          Mod:T @σ₁ … @σₙ { e 'with' fᵢ = eᵢ }  :  Mod:T σ₁' … σₙ'

      'variant' T (α₁:k₁) … (αₙ:kₙ) ↦ … | Vᵢ : τᵢ | …  ∈  〚Ξ〛Mod
      τᵢ  ↠  τᵢ'      σ₁  ↠  σ₁'    ⋯    σₙ  ↠  σₙ'
      Γ  ⊢  σ₁' : k₁    ⋯     Γ  ⊢  σₙ' : kₙ
      Γ  ⊢  e  :  τᵢ'[α₁ ↦ σ₁', …, αₙ ↦ σₙ']
    ——————————————————————————————————————————————————————————————— ExpVarCon
      Γ  ⊢  Mod:T:Vᵢ @σ₁ … @σₙ e  :  Mod:T σ₁' … σₙ'

      'enum' T ↦ … | Eᵢ | …  ∈  〚Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpEnumCon
      Γ  ⊢  Mod:T:Eᵢ  :  Mod:T

      ⟨ f₁: τ₁, …, fₘ: τₘ ⟩ ↠ σ
      Γ  ⊢  σ  :  ⋆
      Γ  ⊢  e₁  :  τ₁      ⋯      Γ  ⊢  eₘ  :  τₘ
    ——————————————————————————————————————————————————————————————— ExpStructCon
      Γ  ⊢  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩  :  σ

      Γ  ⊢  e  :  ⟨ …, fᵢ: τᵢ, … ⟩
    ——————————————————————————————————————————————————————————————— ExpStructProj
      Γ  ⊢  e.fᵢ  :  τᵢ

      Γ  ⊢  e  :  ⟨ f₁: τ₁, …, fᵢ: τᵢ, …, fₙ: τₙ ⟩
      Γ  ⊢  eᵢ  :  τᵢ
    ——————————————————————————————————————————————————————————————— ExpStructUpdate
      Γ  ⊢   ⟨ e 'with' fᵢ = eᵢ ⟩  :  ⟨ f₁: τ₁, …, fₙ: τₙ ⟩

      n ≥ 1
      Γ  ⊢  e : τ
      Γ  ⊢  τ // alt₁ : σ
        ⋮
      Γ  ⊢  τ // altₙ : σ
      τ  ⊲  alt₁, …, altₙ
    ——————————————————————————————————————————————————————————————— ExpCase
      Γ  ⊢  'case' e 'of' alt₁ | … | altₙ : σ

      Γ  ⊢  σ  :  ⋆
      ⊢ₑ  τ
      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpThrow [Daml-LF ≥ 1.dev]
      Γ  ⊢  'throw' @σ @τ @e  :  σ

      ⊢ₑ  τ
      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpToAnyException [Daml-LF ≥ 1.dev]
      Γ  ⊢  'to_any_exception' @τ e  :  'AnyException'

      ⊢ₑ  τ
      Γ  ⊢  e  :  'AnyException'
    ——————————————————————————————————————————————————————————————— ExpFromAnyException [Daml-LF ≥ 1.dev]
      Γ  ⊢  'from_any_exception' @τ e  :  'Optional' τ

      Γ  ⊢  τ  :  ⋆      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— UpdPure
      Γ  ⊢  'pure' e  :  'Update' τ

      τ₁  ↠  τ₁'   Γ  ⊢  τ₁'  : ⋆       Γ  ⊢  e₁  :  'Update' τ₁'
      x₁ : τ₁' · Γ  ⊢  e₂  :  'Update' τ₂
    ——————————————————————————————————————————————————————————————— UpdBlock
      Γ  ⊢  'bind' x₁ : τ₁ ← e₁ 'in' e₂  :  'Update' τ₂

      'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod       Γ  ⊢  e  : Mod:T
    ——————————————————————————————————————————————————————————————— UpdCreate
      Γ  ⊢  'create' @Mod:T e  : 'Update' ('ContractId' Mod:T)

      'tpl' (x : T)
          ↦ { …, 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' … ↦ …, … } }
        ∈ 〚Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod:T
      Γ  ⊢  e₂  :  'List' 'Party'
      Γ  ⊢  e₃  :  τ
    ——————————————————————————————————————————————————————————————— UpdExercise
      Γ  ⊢  'exercise' @Mod:T Ch e₁ e₂ e₃  : 'Update' σ

      'tpl' (x : T)
          ↦ { …, 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' … ↦ …, … } }
        ∈ 〚Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod:T
      Γ  ⊢  e₂  :  τ
    ——————————————————————————————————————————————————————————————— UpdExerciseWithouActors
      Γ  ⊢  'exercise_without_actors' @Mod:T Ch e₁ e₂  : 'Update' σ

      'tpl' (x : T)
          ↦ { …, 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' … ↦ …, … }, 'key' τₖ … }
        ∈ 〚Ξ〛Mod
      Γ  ⊢  e₁  :  τₖ
      Γ  ⊢  e₂  :  τ
    ——————————————————————————————————————————————————————————————— UpdExerciseByKey
      Γ  ⊢  'exercise_by_key' @Mod:T Ch e₁ e₂  : 'Update' σ

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
	    'Update' ('Optional' (ContractId Mod:T))

      τ  ↠  τ'     Γ  ⊢  e  :  'Update' τ'
    ——————————————————————————————————————————————————————————————— UpdEmbedExpr
      Γ  ⊢  'embed_expr' @τ e  :  'Update' τ'

      τ  ↠  τ'
      Γ  ⊢  e₁  :  'Update' τ'
      x : 'AnyException' · Γ  ⊢  e₂  :  'Optional' ('Update' τ')
    ——————————————————————————————————————————————————————————————— UpdTryCatch [Daml-LF ≥ 1.dev]
      Γ  ⊢  'try' @τ e₁ 'catch' x. e₂  :  'Update' τ'

      Γ  ⊢  τ  : ⋆      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ScnPure
      Γ  ⊢  'spure' e  :  'Scenario' τ

      τ₁  ↠  τ₁'   Γ  ⊢  τ₁'  : ⋆       Γ  ⊢  e₁  :  'Scenario' τ₁'
      x₁ : τ₁' · Γ  ⊢  e₂  :  'Scenario' τ₂
    ——————————————————————————————————————————————————————————————— ScnBlock
      Γ  ⊢  'sbind' x₁ : τ₁ ← e₁ 'in' e₂  :  'Scenario' τ₂

      Γ  ⊢  e  :  'Party'
      τ  ↠  τ'   Γ  ⊢  τ'  : ⋆    Γ  ⊢  u  :  'Uptate' τ
    ——————————————————————————————————————————————————————————————— ScnCommit
      Γ  ⊢  'commit' @τ e u  :  'Scenario' τ

      Γ  ⊢  e  :  'Party'
      τ  ↠  τ'   Γ  ⊢  τ'  : ⋆    Γ  ⊢  u  :  'Uptate' τ
    ——————————————————————————————————————————————————————————————— ScnMustFailAt
      Γ  ⊢  'must_fail_at' @τ e u  :  'Scenario' 'Unit'

      Γ  ⊢  e  :  'Int64'
    ——————————————————————————————————————————————————————————————— ScnPass
      Γ  ⊢  'pass' e  :  'Scenario' 'Timestamp'

      Γ  ⊢  e  :  'Text'
    ——————————————————————————————————————————————————————————————— ScnGetParty
      Γ  ⊢  'get_party' e  :  'Scenario' 'Party'

      τ  ↠  τ'     Γ  ⊢  e  :  'Scenario' τ'
    ——————————————————————————————————————————————————————————————— ScnEmbedExpr
      Γ  ⊢  'sembed_expr' @τ e  :  'Scenario' τ'


.. note :: Unlike ``ExpTextMap``, the ``ExpGenMap`` rule does not
  enforce uniqueness of key. In practice, the uniqueness is enforced
  by the `builtin functions <Generic Map functions>`_ that are the
  only way to handle generic maps in a serialized program, the
  explicit syntax for maps being forbidden in serialized programs.


Well-formed case alternatives
.............................

Case expressions ``Γ  ⊢  'case' e 'of' alt₁ | … | altₙ : σ`` require the
notion of well-formed case alternatives ``Γ ⊢ τ // alt : σ``  defined here.
To simplify the presentation, we omit the assumption that the scrutinee
type ``τ`` is well-formed, in the rules below. ::

                                ┌──────────────────┐
  Well-formed case alternatives │ Γ ⊢ τ // alt : σ │
                                └──────────────────┘

      'variant' T (α₁:k₁) … (αₙ:kₙ) ↦ … | V : τ | …  ∈  〚Ξ〛Mod
      τ  ↠  τ'
      x : τ'[α₁ ↦ τ₁, …, αₙ ↦ τₙ] · Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltVariant
      Γ  ⊢  Mod:T τ₁ … τₙ  //  Mod:T:V x  →  e : σ

      'enum' T ↦ … | E | …  ∈  〚Ξ〛Mod
      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltEnum
      Γ  ⊢   Mod:T  //  Mod:T:E  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltNil
      Γ  ⊢  'List' τ  //  'Nil'  →  e : σ

      xₕ ≠ xₜ
      xₕ : τ · xₜ : 'List' τ · Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltCons
      Γ  ⊢  'List' τ  //  'Cons' xₕ xₜ  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltNone
      Γ  ⊢  'Optional' τ  //  'None'  →  e : σ

      x : τ · Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltSome
      Γ  ⊢  'Optional' τ  //  'Some' x  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltTrue
      Γ  ⊢  'Bool'  //  'True'  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltFalse
      Γ  ⊢  'Bool'  //  'False'  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltUnit
      Γ  ⊢  'Unit'  //  ()  →  e : σ

      Γ  ⊢  e : σ
    ——————————————————————————————————————————————————————————————— AltDefault
      Γ  ⊢  τ  //  _  →  e : σ


Pattern match exhaustiveness
............................

Case expressions ``Γ  ⊢  'case' e 'of' alt₁ | … | altₙ : σ`` also require
their patterns to be exhaustive, which is defined here. ::

                               ┌─────────────────────┐
  Pattern match exhaustiveness │ τ  ⊲  alt₁, …, altₙ │
                               └─────────────────────┘

    'variant' T (α₁:k₁) … (αᵣ:kᵣ) ↦ V₁ : σ₁ | … | Vₘ : σₘ  ∈  〚Ξ〛Mod
    i₁, i₂, …, iₘ  ∈  {1, …, n}
    altᵢ₁  =  Mod:T:V₁ x₁  →  e₁
    altᵢ₂  =  Mod:T:V₂ x₂  →  e₂
           ⋮
    altᵢₘ  =  Mod:T:Vₘ xₘ  →  eₘ
    ——————————————————————————————————————————————————————————————— ExhaustVariant
    Mod:T τ₁ … τᵣ  ⊲  alt₁, …, altₙ

    'enum' T ↦ E₁ | … | Eₘ  ∈  〚Ξ〛Mod
    i₁, i₂, …, iₘ  ∈  {1, …, n}
    altᵢ₁  =  Mod:T:E₁  →  e₁
    altᵢ₂  =  Mod:T:E₂  →  e₂
           ⋮
    altᵢₘ  =  Mod:T:Eₘ  →  eₘ
    ——————————————————————————————————————————————————————————————— ExhaustEnum
    Mod:T  ⊲  alt₁, …, altₙ

    i, j  ∈  {1, …, n}
    altᵢ  =  'Nil'  →  e₁
    altⱼ  =  'Cons' xₕ xₜ  →  e₂
    ——————————————————————————————————————————————————————————————— ExhaustList
    'List' τ  ⊲  alt₁, …, altₙ

    i, j  ∈  {1, …, n}
    altᵢ  =  'None'  →  e₁
    altⱼ  =  'Some' x  →  e₂
    ——————————————————————————————————————————————————————————————— ExhaustOptional
    'Optional' τ  ⊲  alt₁, …, altₙ

    i, j  ∈  {1, …, n}
    altᵢ  =  'True'  →  e₁
    altⱼ  =  'False'  →  e₂
    ——————————————————————————————————————————————————————————————— ExhaustBool
    'Bool'  ⊲  alt₁, …, altₙ

    i  ∈  {1, …, n}
    altᵢ  =  ()  →  e
    ——————————————————————————————————————————————————————————————— ExhaustUnit
    'Unit'  ⊲  alt₁, …, altₙ

    i  ∈  {1, …, n}
    altᵢ  =  _  →  e
    ——————————————————————————————————————————————————————————————— ExhaustDefault
    τ  ⊲  alt₁, …, altₙ


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
    ———————————————————————————————————————————————————————————————— STyOptional
      ⊢ₛ  'Optional' τ

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

      ⊢ₛ  τ
    ———————————————————————————————————————————————————————————————— STyCid
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

    ———————————————————————————————————————————————————————————————— STyGeneralError
      ⊢ₛ  'GeneralError'

    ———————————————————————————————————————————————————————————————— STyArithmeticError
      ⊢ₛ  'ArithmeticError'

    ———————————————————————————————————————————————————————————————— STyContractError
      ⊢ₛ  'ContractError'

Note that

1. Structs are *not* serializable.
2. Type synonyms are *not* serializable.
3. Uninhabited variant and enum types are *not* serializable.
4. For a data type to be serializable, *all* type
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

     τ  ↠  τ₁'      αₙ : kₙ · … · α₁ : k₁  ⊢  τ₁'  :  ⋆
       ⋮
     τ  ↠  τₘ'      αₙ : kₙ · … · α₁ : k₁  ⊢  τₘ'  :  ⋆
  ——————————————————————————————————————————————————————————————— DefRec
    ⊢  'record' T (α₁: k₁) … (αₙ: kₙ) ↦ { f₁: τ₁, …, fₘ: τₘ }

    τ  ↠  τ₁'      αₙ : kₙ · … · α₁ : k₁  ⊢  τ₁'  :  ⋆
     ⋮
    τ  ↠  τₘ'      αₙ : kₙ · … · α₁ : k₁  ⊢  τₘ'  :  ⋆
  ——————————————————————————————————————————————————————————————— DefVariant
    ⊢  'record' T (α₁: k₁) … (αₙ: kₙ) ↦ V₁: τ₁ | … | Vₘ: τₘ

  ——————————————————————————————————————————————————————————————— DefEnum
    ⊢  'enum' T  ↦ E₁ | … | Eₘ

    τ  ↠  τ'      (α₁:k₁) … (αₙ:kₙ) · Γ  ⊢  τ'  :  ⋆
  ——————————————————————————————————————————————————————————————— DefTypeSynonym
    ⊢  'synonym' S (α₁: k₁) … (αₙ: kₙ) ↦ τ

    τ  ↠  τ'      ε  ⊢  e  :  τ'
  ——————————————————————————————————————————————————————————————— DefValue
    ⊢  'val' W : τ ↦ e

    'record' T ↦ { f₁ : τ₁, …, fₙ : τₙ }  ∈  〚Ξ〛Mod
    ⊢ₛ  Mod:T
    x : Mod:T  ⊢  eₚ  :  'Bool'
    x : Mod:T  ⊢  eₛ  :  'List' 'Party'
    x : Mod:T  ⊢  eₒ  :  'List' 'Party'
    x : Mod:T  ⊢  eₐ  :  'Text'
    x : Mod:T  ⊢  ChDef₁      ⋯      x : Mod:T  ⊢  ChDefₘ
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

    'record' T ↦ { f₁ : τ₁, …, fₙ : τₙ }  ∈  〚Ξ〛Mod
    ⊢ₛ  Mod:T
    ⊢  e  :  Mod:T → 'Text'
  ——————————————————————————————————————————————————————————————— DefException [Daml-LF ≥ 1.dev]
    ⊢  'exception' T ↦ { 'message' e }

                          ┌───────────────────┐
  Well-formed choices     │ x : Mod:T ⊢ ChDef │
                          └───────────────────┘
    ⊢ₛ  τ
    ⊢ₛ  σ
    y : 'ContractId' Mod:T · z : τ · x : Mod:T  ⊢  e  :  'Update' σ
    z : τ · x : Mod:T  ⊢  eₚ  :  'List' 'Party'
    z : τ · x : Mod:T  ⊢  eₒ  :  'List' 'Party'
  ——————————————————————————————————————————————————————————————— ChDef
    x : Mod:T  ⊢  'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ e

            ┌────────────┐
  Valid key │ ⊢ₖ e  :  τ │
            └────────────┘

  ——————————————————————————————————————————————————————————————— ExpRecProj
    ⊢ₖ  x

    ⊢ₖ  e
  ——————————————————————————————————————————————————————————————— ExpRecProj
    ⊢ₖ  Mod:T @τ₁ … @τₙ {f} e

    ⊢ₖ  e₁    ⋯    ⊢ₖ eₘ
  ———————————————————————————————————————————————————————————————— ExpRecCon
    ⊢ₖ  Mod:T @σ₁ … @σₙ { f₁ = e₁, …, fₘ = eₘ }

                          ┌────────────┐
  Well-formed keys        │ Γ ⊢ KeyDef │
                          └────────────┘
  ——————————————————————————————————————————————————————————————— KeyDefNone
   Γ  ⊢  'no_key'

    ⊢ₛ τ      Γ  ⊢  eₖ  :  τ
    ⊢ₖ eₖ                                                         [Daml-LF = 1.3]
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


Exception coherence
~~~~~~~~~~~~~~~~~~~

The *exception coherence* condition is literally the same as the template
coherence condition with "template" replaced by "exception". We further require
that no type has a template definition and an exception definition associated to
it.


Party literal restriction
~~~~~~~~~~~~~~~~~~~~~~~~~

.. TODO I think this is incorrect, and actually before the
   ``ForbidPartyLiterals`` feature flag party literals were
   allowed everywhere.

The usage of party literals is restricted in Daml-LF. By default,
party literals are neither allowed in templates nor in values used in
templates directly or indirectly.  In practice, this restricted the
usage of party literals to test cases written in Daml-LF. Usage of
party literals can be completely forbidden thanks to the `feature flag
<Feature flags_>`_ ``ForbidPartyLiterals``. If this flag is on, any
occurrence of a party literal anywhere in the module makes the module
not well-formed.


Name collision restriction
~~~~~~~~~~~~~~~~~~~~~~~~~~

Daml-LF relies on `names and identifiers <Identifiers_>`_ to refer to
different kinds of constructs such as modules, type constructors,
variants constructor, and fields. These are relative; type names are
relative to modules; field names are relative to type record and so
one. They live in different namespaces. For example, the space names
for module and type is different.


Fully resolved name
...................

Daml-LF restricts the way names and identifiers are used within a
package. This restriction relies on the notion of *fully resolved
name* construct as follows:

* The *fully resolved name* of the module ``Mod`` is ``Mod``.
* The *fully resolved name* of a record type constructor ``T`` defined
  in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a variant type constructor ``T``
  defined in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a enum type constructor ``T`` defined
  in the module ``Mod`` is ``Mod.T``.
* The *fully resolved name* of a type synonym ``S`` defined in the
  module ``Mod`` is ``Mod.S``.
* The *fully resolved name* of a field ``fᵢ`` of a record type
  definition ``'record' T …  ↦ { …, fᵢ: τᵢ, … }`` defined in the
  module ``Mod`` is ``Mod.T.fᵢ``
* The *fully resolved name* of a variant constructor ``Vᵢ`` of a
  variant type definition ``'variant' T … ↦ …  | Vᵢ: τᵢ | …`` defined
  in the module ``Mod`` is ``Mod.T.Vᵢ``.
* The *fully resolved name* of a enum constructor ``Eᵢ`` of a enum
   type definition ``'enum' T ↦ …  | Eᵢ | …`` defined in the module
   ``Mod`` is ``Mod.T.Eᵢ``.
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
is *not* case-insensitive in Daml-LF. In other words, to refer to a
name, one must refer to it with the same case that it was defined with.

The case-insensitivity for collisions is in place since we often generate
files from Daml-LF packages, and we want to make sure for things to work
smoothly when operating in case-insensitive file systems, while at the
same time preserving case sensitivity in the language.


Name collision condition
........................

In Daml-LF, the only permitted name collisions are those occurring
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


Well-formed packages
~~~~~~~~~~~~~~~~~~~~

Then, a collection of packages ``Ξ`` is well-formed if:

* Each definition in ``Ξ`` is `well-formed <well-formed-definitions_>`_;
* Each template in ``Ξ`` is `coherent <Template coherence_>`_;
* Each exception in ``Ξ`` is `coherent <Exception coherence_>`_;
* The `party literal restriction`_ is respected for
  every module in ``Ξ`` -- taking the ``ForbidPartyLiterals`` flag into
  account.
* The `name collision condition`_ holds for every
  package of ``Ξ``.
* There are no cycles between type synonym definitions, modules, and
  packages references.
* Each package ``p`` only depends on packages whose LF version is older
  than or the same as the LF version of ``p`` itself.


Operational semantics
^^^^^^^^^^^^^^^^^^^^^

The section presents a big-step call-by value operation semantics of
the language.

Similarly to the type system, every rule for expression evaluation and
update interpretation operates on the packages available for
usage ``Ξ``.


Values
~~~~~~

To define any call-by-value semantics for Daml-LF expression, we need
first to define the notion of *values*, the expressions which do not
need to be evaluated further. ::

                           ┌───────┐
   Values                  │ ⊢ᵥ  e │
                           └───────┘

   ——————————————————————————————————————————————————— ValExpAbs
     ⊢ᵥ  λ x : τ . e

   ——————————————————————————————————————————————————— ValExpTyAbsNat
     ⊢ᵥ  Λ α : 'nat' . e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpTyAbsErasable
     ⊢ᵥ  Λ α : ek . e

   ——————————————————————————————————————————————————— ValExpLitInt64
     ⊢ᵥ  LitInt64

   ——————————————————————————————————————————————————— ValExpLitNumeric
     ⊢ᵥ  LitNumeric

   ——————————————————————————————————————————————————— ValExpLitBigNumeric
     ⊢ᵥ  LitBigDecimal

   ——————————————————————————————————————————————————— ValExpLitText
     ⊢ᵥ  t

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

   ——————————————————————————————————————————————————— ValExpOptionalNone
     ⊢ᵥ  'None' @τ

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpOptionalSome
     ⊢ᵥ  'Some' @τ e

     ⊢ᵥ  e₁    ⋯    ⊢ᵥ eₙ
   ——————————————————————————————————————————————————— ValExpTextMap
     ⊢ᵥ  [t₁ ↦ e₁; … ; tₙ ↦ eₙ]

     ⊢ᵥ  e₁    ⋯    ⊢ᵥ eₙ
     ⊢ᵥ  e₁'   ⋯    ⊢ᵥ eₙ'
   ——————————————————————————————————————————————————— ValExpGenMap
     ⊢ᵥ  〚e₁ ↦ e₁'; … ; eₙ ↦ eₙ'〛

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
     f₁ < f₂ < … < fₘ lexicographically
   ——————————————————————————————————————————————————— ValExpStructCon
     ⊢ᵥ  ⟨ f₁ = e₁, …, fₘ = eₘ ⟩

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpToAny
     ⊢ᵥ  'to_any' @τ e

   ——————————————————————————————————————————————————— ValExpTypeRep
     ⊢ᵥ  'type_rep' @τ

     ⊢ₑ  τ     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValToAnyException
     ⊢ᵥ  'to_any_exception' @τ e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValGeneralError
     ⊢ᵥ  'MAKE_GENERAL_ERROR' e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValArithmeticError
     ⊢ᵥ  'MAKE_ARITHMETIC_ERROR' e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValContractError
     ⊢ᵥ  'MAKE_CONTRACT_ERROR' e

     ⊢ᵥᵤ  u
   ——————————————————————————————————————————————————— ValUpdate
     ⊢ᵥ  u

     ⊢ᵥₛ  s
   ——————————————————————————————————————————————————— ValScenario
     ⊢ᵥ  s

   ——————————————————————————————————————————————————— ValUnBondedMathContext
     ⊢ᵥ  LitRoundingMode


                           ┌────────┐
   Update Values           │ ⊢ᵥᵤ  u │
                           └────────┘

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValUpdatePure
     ⊢ᵥᵤ  'pure' @τ e

     ⊢ᵥ  e₁
   ——————————————————————————————————————————————————— ValUpdateBind
     ⊢ᵥᵤ  'bind' x₁ : τ₁ ← e₁ 'in' e₂

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValUpdateCreate
     ⊢ᵥᵤ  'create' @Mod:T e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValUpdateFetch
     ⊢ᵥᵤ  'fetch' @Mod:T e

     ⊢ᵥ  e₁
     ⊢ᵥ  e₂
     ⊢ᵥ  e₃
   ——————————————————————————————————————————————————— ValUpdateExercise
     ⊢ᵥᵤ  'exercise' @Mod:T Ch e₁ e₂ e₃

     ⊢ᵥ  e₁
     ⊢ᵥ  e₂
   ——————————————————————————————————————————————————— ValUpdateExerciseWithoutActors
     ⊢ᵥᵤ  'exercise_without_actors' @Mod:T Ch e₁ e₂

     ⊢ᵥ  e₁
     ⊢ᵥ  e₂
   ——————————————————————————————————————————————————— ValUpdateExerciseByKey
     ⊢ᵥᵤ  'exercise_by_key' @Mod:T Ch e₁ e₂

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValUpdateFetchByKey
     ⊢ᵥᵤ  'fetch_by_key' @Mod:T e

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValUpdateLookupByKey
     ⊢ᵥᵤ  'lookup_by_key' @Mod:T e

   ——————————————————————————————————————————————————— ValUpdateEmbedExpr
     ⊢ᵥᵤ   'embed_expr' @τ e

   ——————————————————————————————————————————————————— ValUpdateTryCatch
     ⊢ᵥᵤ   'try' @τ e₁ 'catch' x. e₂


                           ┌────────┐
   Scenario Values         │ ⊢ᵥₛ  s │
                           └────────┘

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValScenarioPure
     ⊢ᵥₛ  'spure' @τ e

     ⊢ᵥ  e₁
   ——————————————————————————————————————————————————— ValScenarioBind
     ⊢ᵥₛ  'sbind' x₁ : τ₁ ← e₁ 'in' e₂

     ⊢ᵥ  e
     ⊢ᵥᵤ  u
   ——————————————————————————————————————————————————— ValScenarioCommit
     ⊢ᵥₛ  'commit' @τ e u

     ⊢ᵥ  e
     ⊢ᵥᵤ  u
   ——————————————————————————————————————————————————— ValScenarioMustFailAt
     ⊢ᵥₛ  'must_fail_at' @τ e u

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValScenarioPass
     ⊢ᵥₛ  'pass' e

   ——————————————————————————————————————————————————— ValScenarioGetTime
     ⊢ᵥₛ  'sget_time'

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValScenarioGetParty
     ⊢ᵥₛ  'sget_party' e

   ——————————————————————————————————————————————————— ValScenarioEmbedExpr
     ⊢ᵥₛ  'sembed_expr' @τ e


Note that the argument of an embedded expression does not need to be a
value for the whole to be so.  In the following, we will use the
symbol ``v`` or ``w`` to represent an expression which is a value.

Note that for type lambdas, the kind of the argument affects whether it
is considered a value. In particular, an erasable kind is handled as if
it were erased, so in this case, the expression is a value only if the
body of the lambda is already a value. Type lambdas where the type
parameter is not erasable (i.e. does not have an erasable kind) are
values. This is captured in the rules ``ValExpTyAbsNat`` and
``ValExpTyAbsErasable``.

Note that the fields of struct values are always ordered lexicographically
by field name, unlike the fields of struct expressions. The field order is
normalized during evaluation.

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


Type ordering
~~~~~~~~~~~~~

In this section, we define a strict partial order relation ``<ₜ`` on
types. Formally, ``<ₜ`` is defined as the least binary relation on
types that satisfies the following rules::

    σ₁ <ₜ τ    τ <ₜ σ₂
  ——————————————————————————————————————————————————— TypeOrderTransitivity
    σ₁ <ₜ σ₂

  ——————————————————————————————————————————————————— TypeOrderUnitBool
    'Unit' <ₜ 'Bool'

  ——————————————————————————————————————————————————— TypeOrderBoolInt64
    'Bool' <ₜ 'Int64'

  ——————————————————————————————————————————————————— TypeOrderInt64Text
    'Int64' <ₜ 'Text'

  —————————————————————————————————————————————————— TypeOrderTextTimestamp
    'Text' <ₜ 'Timestamp'

  ——————————————————————————————————————————————————— TypeOrderTimestampPArty
    'Timestamp' <ₜ 'Party'

  ——————————————————————————————————————————————————— TypeOrderPartyList
    'Party' <ₜ 'List'

  —————————————————————————————————————————————————— TypeOrderListUpdate
    'List' <ₜ 'Update'

  ——————————————————————————————————————————————————— TypeOrderUpdateScenario
    'Update' <ₜ 'Scenario'

  ——————————————————————————————————————————————————— TypeOrderScenarioDate
    'Scenario' <ₜ 'Date'

  ——————————————————————————————————————————————————— TypeOrderDateContractId
    'Date' <ₜ 'ContractId'

  ——————————————————————————————————————————————————— TypeOrderContractIdOptional
    'ContractId' <ₜ 'Optional'

  ——————————————————————————————————————————————————— TypeOrderOptionalArrow
    'Optional' <ₜ 'Arrow'

  ——————————————————————————————————————————————————— TypeOrderArrowTextMap
    'Arrow' <ₜ 'TextMap'

  ——————————————————————————————————————————————————— TypeOrderTextMapNumeric
    'TextMap' <ₜ 'Numeric'

  ——————————————————————————————————————————————————— TypeOrderNumericAny
    'Numeric' <ₜ  'Any'

  ——————————————————————————————————————————————————— TypeOrderAnyTypeRep
    'Any' <ₜ 'TypeRep'

  ——————————————————————————————————————————————————— TypeOrderTypeRepUpdate
    'TypeRep' <ₜ 'GenMap'

  ——————————————————————————————————————————————————— TypeOrderGenMapAny
    'GenMap' <ₜ 'AnyException'

  ——————————————————————————————————————————————————— TypeOrderAnyExceptionGeneralError
    'AnyException' <ₜ 'GeneralError'

  ——————————————————————————————————————————————————— TypeOrderGeneralErrorArithmeticError
    'GeneralError' <ₜ 'ArithmeticError'

  ——————————————————————————————————————————————————— TypeOrderArithmeticErrorContractError
    'ArithmeticError' <ₜ 'ContractError'

  ——————————————————————————————————————————————————— TypeOrderContractErrorBigNumeric
    'ContractError' <ₜ 'BigNumeric'

  ——————————————————————————————————————————————————— TypeOrderBigNumericRoundingMode
    'BigNumeric' <ₜ 'RoundingMode'

  ——————————————————————————————————————————————————— TypeOrderRoundingModeTyCon
    'RoundingMode' <ₜ Mod:T

    PkgId₁ comes lexicographically before PkgId₂
  ——————————————————————————————————————————————————— TypeOrderTyConPackageId
    (PkgId₁:ModName₁):T₁ <ₜ (PkgId₂:ModName₂):T₂

    ModName₁ comes lexicographically before ModName₂
  ——————————————————————————————————————————————————— TypeOrderTyConModName
    (PkgId:ModName₁):T₁ <ₜ (PkgId:ModName₂):T₂

    T₁ comes lexicographically before T₂
  —————————————————————————————————————————————————— TypeOrderTyConName
    Mod:T₁ <ₜ Mod:T₂

  —————————————————————————————————————————————————— TypeOrderTyConNat
    Mod:T <ₜ n

    n₁ is strictly less than n₂
  —————————————————————————————————————————————————— TypeOrderNatNat
    n₁ <ₜ n₂

  —————————————————————————————————————————————————— TypeOrderNatStruct
    n <ₜ ⟨ f₁ : τ₁, …, fₘ : τₘ ⟩

    fᵢ comes lexicographically before gᵢ
  ——————————————————————————————————————————————————— TypeOrderStructFieldName
    ⟨ f₁ : τ₁, …, fₘ : τₘ ⟩ <ₜ
      ⟨ f₁ : σ₁, …, fᵢ₋1 : σᵢ₋₁, gᵢ : σᵢ, …, gₙ : σₙ ⟩

  ——————————————————————————————————————————————————— TypeOrderStructFieldNumber
    ⟨ f₁ : τ₁, …, fₘ : τₘ ⟩ <ₜ
      ⟨ f₁ : τ₁, …, fₘ : τₘ, fₘ₊₁ : τₘ₊₁ ⟩

    τᵢ <ₜ σᵢ
  ——————————————————————————————————————————————————— TypeOrderStructFieldType
    ⟨ f₁ : τ₁, …, fₘ : τₘ ⟩ <ₜ
      ⟨ f₁ : τ₁, …, fᵢ₋₁ : τᵢ₋₁, fᵢ : σᵢ, …, fₘ : σₘ ⟩

  ——————————————————————————————————————————————————— TypeOrderStructTyApp
    ⟨ f₁ : τ₁, …, fₘ : τₘ ⟩ <ₜ τ σ

    τ₁ <ₜ τ₂
  ——————————————————————————————————————————————————— TypeOrderTyAppLeft
    τ₁ σ₁ <ₜ τ₂ σ₂

    σ₁ <ₜ σ₂
  ——————————————————————————————————————————————————— TypeOrderTypeAppRight
    τ σ₁ <ₜ τ σ₂


Note that ``<ₜ`` is undefined on types containing variables,
quantifiers or type synonymes.  ``≤ₜ`` is defined as the reflexive
closure of ``<ₜ``.


Expression evaluation
~~~~~~~~~~~~~~~~~~~~~

Daml-LF evaluation is only defined on closed, well-typed expressions.

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

The output of any Daml-LF built-in function ``F`` fully applied to
types ``@τ₁ … @τₘ`` and values ``v₁ … vₙ`` is deterministic. In the
following rules, we abstract this output with the notation ``𝕆(F @τ₁ …
@τₘ v₁ … vₙ)``. Please refer to the `Built-in functions`_ section for the
exact output.

::

  Evaluation result
    r ::= Ok v                                      -- ResOk
       |  Err @τ v                                  -- ResErr, v a value of exception type τ

                           ┌──────────┐
  Big-step evaluation      │ e  ⇓  r  │
                           └──────────┘

    —————————————————————————————————————————————————————————————————————— EvValue
      v  ⇓  Ok v


      e   ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvTyAbsErasableErr
      Λ α : ek . e  ⇓  Err @τ v


      e   ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvTyAbsErasable
      Λ α : ek . e  ⇓  Ok (Λ α : ek . v)


      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpAppErr1
      e₁ e₂  ⇓  Err @τ v

      e₁  ⇓  Ok (λ x : τ . e)
      e₂  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpAppErr2
      e₁ e₂  ⇓  Err @τ' v

      e₁  ⇓  Ok (λ x : τ . e)
      e₂  ⇓  Ok v₂
      e[x ↦ v₂]  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpApp
      e₁ e₂  ⇓  r

      e₁  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpTyAppErr
      e₁ @τ  ⇓  Err @τ' v

      e₁  ⇓  Ok (Λ α : k . e)
      e[α ↦ τ]  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpTyApp
      e₁ @τ  ⇓  r

      e₁  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpLetErr
      'let' x : τ = e₁ 'in' e₂  ⇓  Err @τ' v

      e₁  ⇓  Ok v₁
      e₂[x ↦ v₁]  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpLet
      'let' x : τ = e₁ 'in' e₂  ⇓  r

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpToAnyErr
      'to_any' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpToAny
      'to_any' @τ e  ⇓  Ok ('to_any' @τ v)

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyErr
      'from_any' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok ('to_any' @τ v)
    —————————————————————————————————————————————————————————————————————— EvExpFromAnySucc
      'from_any' @τ e  ⇓  Ok ('Some' @τ v)

      e  ⇓  Ok ('to_any' @τ₁ v)     τ₁ ≠ τ₂
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyFail
      'from_any' @τ₂ e  ⇓  Ok 'None'

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpCaseErr
      'case' e₁ 'of' {  p₁ → e₁ | … |  pₙ → eₙ }  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      v 'matches' p₁  ⇝  Succ (x₁ ↦ v₁ · … · xₘ ↦ vₘ · ε)
      e₁[x₁ ↦ v₁, …, xₘ ↦ vₘ]  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpCaseSucc
      'case' e₁ 'of' {  p₁ → e₁ | … |  pₙ → eₙ }  ⇓  r

      e₁  ⇓  Ok v₁    v₁ 'matches' p₁  ⇝  Fail
      'case' v₁ 'of' { p₂ → e₂ … | pₙ → eₙ }  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpCaseFail
      'case' e₁ 'of' { p₁ → e₁ | p₂ → e₂ | … | pₙ → eₙ } ⇓ r

      e₁  ⇓  Ok v₁     v 'matches' p  ⇝  Fail
    —————————————————————————————————————————————————————————————————————— EvExpCaseEmpty
      'case' e₁ 'of' { p → e }  ⇓  Err "match error"

       eₕ  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpConsErr1
      'Cons' @τ eₕ eₜ  ⇓  Err @τ' v

       eₕ  ⇓  Ok vₕ
       eₜ  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpConsErr2
      'Cons' @τ eₕ eₜ  ⇓  Err @τ' v

       eₕ  ⇓  Ok vₕ
       eₜ  ⇓  Ok vₜ
    —————————————————————————————————————————————————————————————————————— EvExpCons
      'Cons' @τ eₕ eₜ  ⇓  Ok ('Cons' @τ vₕ vₜ)

       e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpSomeErr
      'Some' @τ e  ⇓  Err @τ' v

       e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpSome
      'Some' @τ e  ⇓  Ok ('Some' @τ v)

      𝕋(F) = ∀ (α₁: ⋆). … ∀ (αₘ: ⋆). σ₁ → … → σₙ → σ
      e₁  ⇓  Ok v₁
        ⋮
      eᵢ₋₁  ⇓  Ok vᵢ₋₁
      eᵢ  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpBuiltinErr
      F @τ₁ … @τₘ e₁ … eₙ  ⇓  Err @τ v

      𝕋(F) = ∀ (α₁: ⋆). … ∀ (αₘ: ⋆). σ₁ → … → σₙ → σ
      e₁  ⇓  Ok v₁
        ⋮
      eₙ  ⇓  Ok vₙ
    —————————————————————————————————————————————————————————————————————— EvExpBuiltin
      F @τ₁ … @τₘ e₁ … eₙ  ⇓  𝕆(F @τ₁ … @τₘ v₁ … vₙ)

      'val' W : τ ↦ e  ∈ 〚Ξ〛Mod
      e  ⇓  r
    —————————————————————————————————————————————————————————————————————— EvExpVal
      Mod:W  ⇓  r

      e₁  ⇓  Ok v₁
        ⋮
      eᵢ₋₁  ⇓  Ok vᵢ₋₁
      eᵢ  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpRecConErr
      Mod:T @τ₁ … @τₘ {f₁ = e₁, …, fₙ = eₙ}
        ⇓
      Err @τ v

      e₁  ⇓  Ok v₁
        ⋮
      eₙ  ⇓  Ok vₙ
    —————————————————————————————————————————————————————————————————————— EvExpRecCon
      Mod:T @τ₁ … @τₘ {f₁ = e₁, …, fₙ = eₙ}
        ⇓
      Ok (Mod:T @τ₁ … @τₘ {f₁ = v₁, …, fₙ = ₙ})

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpRecProjErr
      Mod:T @τ₁ … @τₘ {fᵢ} e  ⇓  Err @τ v

      e  ⇓  Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ, …, fₙ= vₙ})
    —————————————————————————————————————————————————————————————————————— EvExpRecProj
      Mod:T @τ₁ … @τₘ {fᵢ} e  ⇓  Ok vᵢ

      e  ⇓  Err t
    —————————————————————————————————————————————————————————————————————— EvExpRecUpdErr1
      Mod:T @τ₁ … @τₘ { e 'with' fᵢ = eᵢ } ⇓ Err t

      e  ⇓  Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ, …, fₙ= vₙ})
      eᵢ  ⇓  Err t
    —————————————————————————————————————————————————————————————————————— EvExpRecUpdErr2
      Mod:T @τ₁ … @τₘ { e 'with' fᵢ = eᵢ } ⇓ Err t

      e  ⇓  Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ, …, fₙ= vₙ})
      eᵢ  ⇓  Ok vᵢ'
    —————————————————————————————————————————————————————————————————————— EvExpRecUpd
      Mod:T @τ₁ … @τₘ { e 'with' fᵢ = eᵢ }
        ⇓
      Ok (Mod:T @τ₁ … @τₘ {f₁= v₁, …, fᵢ= vᵢ', …, fₙ= vₙ})

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpVarConErr
      Mod:T:V @τ₁ … @τₙ e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpVarCon
      Mod:T:V @τ₁ … @τₙ e  ⇓  Ok (Mod:T:V @τ₁ … @τₙ v)


      e₁  ⇓  Ok v₁
        ⋮
      eᵢ₋₁  ⇓  Ok vᵢ₋₁
      eᵢ  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpStructConErr
      ⟨f₁ = e₁, …, fₙ = eₙ⟩  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
        ⋮
      eₙ  ⇓  Ok vₙ
      [f₁, …, fₙ] sorts lexicographically to [fⱼ₁, …, fⱼₙ]
    —————————————————————————————————————————————————————————————————————— EvExpStructCon
      ⟨f₁ = e₁, …, fₙ = eₙ⟩  ⇓  Ok ⟨fⱼ₁ = vⱼ₁, …, fⱼₙ = vⱼₙ⟩

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpStructProj
      e.fᵢ  ⇓  Err @τ v

      e  ⇓  Ok ⟨ f₁= v₁, …, fᵢ = vᵢ, …, fₙ = vₙ ⟩
    —————————————————————————————————————————————————————————————————————— EvExpStructProj
      e.fᵢ  ⇓  Ok vᵢ

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpStructUpdErr1
      ⟨ e 'with' fᵢ = eᵢ ⟩ ⇓ Err @τ v

      e  ⇓  Ok ⟨ f₁= v₁, …, fᵢ = vᵢ, …, fₙ = vₙ ⟩
      eᵢ  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpStructUpdErr2
      ⟨ e 'with' fᵢ = eᵢ ⟩ ⇓ Err @τ v

      e  ⇓  Ok ⟨ f₁= v₁, …, fᵢ = vᵢ, …, fₙ = vₙ ⟩
      eᵢ  ⇓  Ok vᵢ'
    —————————————————————————————————————————————————————————————————————— EvExpStructUpd
      ⟨ e 'with' fᵢ = eᵢ ⟩
        ⇓
      Ok ⟨ f₁= v₁, …, fᵢ= vᵢ', …, fₙ= vₙ ⟩

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpThrowErr
      'throw' @σ @τ e  ⇓  Err @τ' v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpThrow
      'throw' @σ @τ e  ⇓  Err @τ v

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpToAnyExceptionErr
      'to_any_exception' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpToAnyException
      'to_any_exception' @τ e  ⇓  Ok ('to_any_exception' @τ v)

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyExceptionErr
      'from_any_exception' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok ('to_any_exception' @σ v)
      σ ≠ τ
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyExceptionNone
      'from_any_exception' @τ e  ⇓  Ok ('None' @τ)

      e  ⇓  Ok ('to_any_exception' @σ v)
      σ = τ
    —————————————————————————————————————————————————————————————————————— EvExpFromAnyExceptionSome
      'from_any_exception' @τ e  ⇓  Ok ('Some' @τ v)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageErr
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Err @τ v

      e  ⇓  Ok ('to_any_exception' @Mod:T v)
      'exception' T ↦ { 'message' eₘ }  ∈  〚Ξ〛Mod
      eₘ v  ⇓  Err @τ' v'
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageRecordErr
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Err @τ' v'

      e  ⇓  Ok ('to_any_exception' @Mod:T v)
      'exception' T ↦ { 'message' eₘ }  ∈  〚Ξ〛Mod
      eₘ v  ⇓  Ok vₘ
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageRecord
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Ok vₘ

      e  ⇓  Ok ('to_any_exception' @'GeneralError' ('MAKE_GENERAL_ERROR' v))
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageGeneral
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Ok v

      e  ⇓  Ok ('to_any_exception' @'ArithmeticError' ('MAKE_ARITHMETIC_ERROR' v))
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageArithmetic
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Ok v

      e  ⇓  Ok ('to_any_exception' @'ContractError' ('MAKE_CONTRACT_ERROR' v))
    —————————————————————————————————————————————————————————————————————— EvExpAnyExceptionMessageContract
      'ANY_EXCEPTION_MESSAGE' e  ⇓  Ok v

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpUpPureErr
      'pure' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpUpPure
      'pure' @τ e  ⇓  Ok ('pure' @τ v)

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpBindErr
      'bind' x₁ : τ₁ ← e₁ 'in' e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
    —————————————————————————————————————————————————————————————————————— EvExpUpBind
      'bind' x₁ : τ₁ ← e₁ 'in' e₂
        ⇓
      Ok ('bind' x₁ : τ₁ ← v₁ 'in' e₂)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpCreateErr
      'create' @Mod:T e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpUpCreate
      'create' @Mod:T e  ⇓  Ok ('create' @Mod:T v)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpFetchErr
      'fetch' @Mod:T e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpUpFetch
      'fetch' @Mod:T e  ⇓  Ok ('fetch' @Mod:T v)

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseErr1
      'exercise' @Mod:T Ch e₁ e₂ e₃  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseErr2
      'exercise' @Mod:T Ch e₁ e₂ e₃  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Ok v₂
      e₃  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseErr3
      'exercise' @Mod:T Ch e₁ e₂ e₃  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Ok v₂
      e₃  ⇓  Ok v₃
    —————————————————————————————————————————————————————————————————————— EvExpUpExercise
      'exercise' @Mod:T Ch e₁ e₂ e₃
        ⇓
      Ok ('exercise' @Mod:T Ch v₁ v₂ v₃)

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseWithoutActorsErr1
      'exercise_without_actors' @Mod:T Ch e₁ e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseWithoutActorsErr2
      'exercise_without_actors' @Mod:T Ch e₁ e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Ok v₂
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseWithoutActors
      'exercise_without_actors' @Mod:T Ch e₁ e₂
        ⇓
      Ok ('exercise_without_actors' @Mod:T Ch v₁ v₂)

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseByKeyErr1
      'exercise_by_key' @Mod:T Ch e₁ e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseByKeyErr2
      'exercise_by_key' @Mod:T Ch e₁ e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
      e₂  ⇓  Ok v₂
    —————————————————————————————————————————————————————————————————————— EvExpUpExerciseByKey
      'exercise_by_key' @Mod:T Ch e₁ e₂
        ⇓
      Ok ('exercise_by_key' @Mod:T Ch v₁ v₂)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpFetchByKeyErr
      'fetch_by_key' @Mod:T e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpUpFetchByKey
      'fetch_by_key' @Mod:T e
        ⇓
      Ok ('fetch_by_key' @Mod:T v)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpUpLookupByKeyErr
      'lookup_by_key' @Mod:T e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpUpLookupByKey
      'lookup_by_key' @Mod:T e
       ⇓
      Ok ('lookup_by_key' @Mod:T v)

    —————————————————————————————————————————————————————————————————————— EvExpUpTryCatch
      'try' @τ e₁ 'catch' x. e₂
       ⇓
      Ok ('try' @τ e₁ 'catch' x. e₂)

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioPureErr
      'spure' @τ e  ⇓  Err @τ' v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioPure
      'spure' @τ e  ⇓  Ok ('spure' @τ v)

      e₁  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioBindErr
      'sbind' x₁ : τ₁ ← e₁ 'in' e₂  ⇓  Err @τ v

      e₁  ⇓  Ok v₁
    —————————————————————————————————————————————————————————————————————— EvExpScenarioBind
      'sbind' x₁ : τ₁ ← e₁ 'in' e₂
        ⇓
      Ok ('sbind' x₁ : τ₁ ← v₁ 'in' e₂)


      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioCommitErr1
      'commit' @τ e u  ⇓  Err @τ' v

      e  ⇓  Ok v₁
      u  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioCommitErr2
      'commit' @τ e u  ⇓  Err @τ' v

      e  ⇓  Ok v₁
      u  ⇓  Ok v₂
    —————————————————————————————————————————————————————————————————————— EvExpScenarioCommit
      'commit' @τ e u  ⇓  Ok ('commit' @τ v₁ v₂)

      e  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioMustFailAtErr1
      'must_fail_at' @τ e u  ⇓  Err @τ' v

      e  ⇓  Ok v₁
      u  ⇓  Err @τ' v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioMustFailAtErr2
      'must_fail_at' @τ e u  ⇓  Err @τ' v

      e  ⇓  Ok v₁
      u  ⇓  Ok v₂
    —————————————————————————————————————————————————————————————————————— EvExpScenarioMustFailAt
      'must_fail_at' @τ e u  ⇓  Ok ('must_fail_at' @τ v₁ v₂)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioPassErr
      'pass' e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioPass
      'pass' e  ⇓  Ok ('pass' v)

      e  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioGetPartyErr
      'sget_party' e  ⇓  Err @τ v

      e  ⇓  Ok v
    —————————————————————————————————————————————————————————————————————— EvExpScenarioGetParty
      'sget_party' e  ⇓  Ok ('sget_party' v)

Note that the rules are designed such that for every expression, there is at
most one possible outcome. In other words, results are deterministic. When
two or more derivations apply for the same expression, they yield the same result. For
example, the rules ``EvValue`` and ``EvExpUpPure`` both apply to the expression
``'pure' @Int64 10``, yielding the same result ``Ok ('pure' @Int64 10)``.

In addition, update expressions only evaluate to update values, and scenario
expressions only evaluate to scenario values.

Well-formed record construction expressions evaluate the fields in the order
they were defined in the type. This is implied by the type system, which forces
well-formed record construction expressions to specify the fields in the same
order as in the type definition, and the ``EvExpRecCon`` rules, which evaluate
fields in the order they are given.

These semantics do not require, nor forbid, the cacheing or memoization of
evaluation results for top-level values, or for any other value. This is
considered an implementation detail.


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
are values according to ``⊢ᵥᵤ``. In this section, all updates denoted
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
       |  'rollback' @τ v itr   -- v must be a value of exception type τ

  Ledger transactions
    tr
      ::= act₁ · … · actₙ

  Incomplete ledger actions
    iact
      ::= 'iexercise' v Contract ChKind itr  -- v must be of type 'List' 'Party'

  Incomplete ledger transactions
    itr
      ::= tr
       |  tr · iact

  Contract states
    ContractState
      ::= 'active'
       |  'inactive'

  Contract stores
     st ∈ finite map from cid to (Mod:T, v, ContractState)

  Contract key index
     keys ∈ finite injective map from GlobalKey to cid

  Contract state
    S ::= (st, keys)

  Update result
    ur ::= (Ok v, tr) ‖ S
        |  (Err @τ v, itr)     -- v must be a value of exception type τ

                                    ┌──────────────┐
  Big-step update interpretation    │ u ‖ S₀ ⇓ᵤ ur │  (u is an update value)
                                    └──────────────┘

   —————————————————————————————————————————————————————————————————————— EvUpdPure
     'pure' v ‖ (st, keys)  ⇓ᵤ  (Ok v, ε) ‖ (st, keys)

     u₁ ‖ S₀  ⇓ᵤ  (Err @τ' v, itr)
   —————————————————————————————————————————————————————————————————————— EvUpdBindErr1
     'bind' x : τ ← u₁ ; e₂ ‖ S₀  ⇓ᵤ  (Err @τ' v, itr)

     u₁ ‖ S₀  ⇓ᵤ  (Ok v₁, tr₁) ‖ S₁
     e₂[x ↦ v₁]  ⇓  Err @τ' v₂
   —————————————————————————————————————————————————————————————————————— EvUpdBindErr2
     'bind' x : τ ← u₁ ; e₂ ‖ S₀  ⇓ᵤ  (Err @τ' v₂, tr₁)

     u₁ ‖ S₀  ⇓ᵤ  (Ok v₁, tr₁) ‖ S₁
     e₂[x ↦ v₁]  ⇓  Ok u₂
     u₂ ‖ S₁  ⇓ᵤ  (Err @τ' v₂, itr₂)
   —————————————————————————————————————————————————————————————————————— EvUpdBindErr3
     'bind' x : τ ← u₁ ; e₂ ‖ S₀  ⇓ᵤ  (Err @τ' v₂, tr₁ ⋅ itr₂)

     u₁ ‖ S₀  ⇓ᵤ  Ok (v₁, tr₁) ‖ S₁
     e₂[x ↦ v₁]  ⇓  Ok u₂
     u₂ ‖ S₁  ⇓ᵤ  Ok (v₂, tr₂) ‖ S₂
   —————————————————————————————————————————————————————————————————————— EvUpdBind
     'bind' x : τ ← u₁ ; e₂ ‖ S₀
       ⇓ᵤ
     (Ok v₂, tr₁ · tr₂) ‖ S₂

     'tpl' (x : T) ↦ { 'precondition' eₚ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateErr1
     'create' @Mod:T vₜ ‖ S₀  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'False'
     v = 'MAKE_CONTRACT_ERROR' "Precondition failed on {Mod:T}."
   —————————————————————————————————————————————————————————————————————— EvUpdCreateFail
     'create' @Mod:T vₜ ‖ S₀
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateErr2
     'create' @Mod:T vₜ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ, 'signatories' eₛ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateErr3
     'create' @Mod:T vₜ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, … }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateErr4
     'create' @Mod:T vₜ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)


     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, …, 'no_key' }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Ok vₒ
     cid ∉ dom(st₀)
     tr = 'create' (cid, Mod:T, vₜ, 'no_key')
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'active')]
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithoutKeySucceed
     'create' @Mod:T vₜ ‖ (st₀, keys₀)
       ⇓ᵤ
     Ok (cid, tr) ‖ (st₁, keys₀)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Ok vₒ
     eₖ[x ↦ vₜ]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeyErr1
     'create' @Mod:T vₜ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Ok vₒ
     eₖ[x ↦ vₜ]  ⇓  Ok vₖ
     eₘ vₖ  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeyErr2
     'create' @Mod:T vₜ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Ok vₒ
     eₖ[x ↦ vₜ]  ⇓  Ok vₖ
     eₘ vₖ  ⇓  Ok vₘ
     (Mod:T, vₖ) ∈ dom(keys₀)
     v = 'MAKE_CONTRACT_ERROR' "Mod:T template key violation"
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeyFail
     'create' @Mod:T vₜ ‖ (st₀, keys₀)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T) ↦ { 'precondition' eₚ, 'agreement' eₐ,
        'signatories' eₛ, 'observers' eₒ, …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     eₚ[x ↦ vₜ]  ⇓  Ok 'True'
     eₐ[x ↦ vₜ]  ⇓  Ok vₐ
     eₛ[x ↦ vₜ]  ⇓  Ok vₛ
     eₒ[x ↦ vₜ]  ⇓  Ok vₒ
     eₖ[x ↦ vₜ]  ⇓  Ok vₖ
     eₘ vₖ  ⇓  Ok vₘ
     (Mod:T, vₖ) ∉ dom(keys₀)
     cid ∉ dom(st₀)
     tr = 'create' (cid, Mod:T, vₜ)
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'active')]
     keys₁ = keys₀[(Mod:T, vₖ) ↦ cid]
   —————————————————————————————————————————————————————————————————————— EvUpdCreateWithKeySucceed
     'create' @Mod:T vₜ ‖ (st₀, keys₀)
       ⇓ᵤ
     Ok (cid, tr) ‖ (st₁, keys₁)

     cid ∉ dom(st)
     v = 'MAKE_CONTRACT_ERROR' "Exercise on unknown contract"
   —————————————————————————————————————————————————————————————————————— EvUpdExercMissing
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st; keys)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'inactive')
     v = 'MAKE_CONTRACT_ERROR' "Exercise on inactive contract"
   —————————————————————————————————————————————————————————————————————— EvUpdExercInactive
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀; keys₀)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdExercActorEvalErr
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ ≠ₛ vₚ
     v = 'MAKE_CONTRACT_ERROR' "Exercise actors do not match"
   —————————————————————————————————————————————————————————————————————— EvUpdExercBadActors
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st; keys)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ …, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdExercObserversErr
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     (Err @τ v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₒ
     eₐ[x ↦ vₜ, y ↦ cid, z ↦ v₂]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdExercBodyEvalErr
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     (Err @τ v, 'iexercise' v₁ (cid, Mod:T, vₜ) ChKind ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'consuming' Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₒ
     eₐ[x ↦ vₜ, y ↦ cid, z ↦ v₂]  ⇓  Ok uₐ
     keys₁ = keys₀ - keys₀⁻¹(cid)
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'inactive')]
     uₐ ‖ (st₁, keys₁)  ⇓ᵤ  (Err @τ v, itr)
   —————————————————————————————————————————————————————————————————————— EvUpdExercConsumErr
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     (Err @τ v, 'iexercise' v₁ (cid, Mod:T, vₜ) 'consuming' itr)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'consuming' Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₒ
     eₐ[x ↦ vₜ, y ↦ cid, z ↦ v₂]  ⇓  Ok uₐ
     keys₁ = keys₀ - keys₀⁻¹(cid)
     st₁ = st₀[cid ↦ (Mod:T, vₜ, 'inactive')]
     uₐ ‖ (st₁, keys₁)  ⇓ᵤ  Ok (vₐ, trₐ) ‖ (st₂, keys₂)
   —————————————————————————————————————————————————————————————————————— EvUpdExercConsum
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     Ok (vₐ, 'exercise' v₁ (cid, Mod:T, vₜ) 'consuming' trₐ) ‖ (st₂, keys₂)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'non-consuming' Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₒ
     eₐ[x ↦ vₜ, y ↦ cid, z ↦ v₂]  ⇓  Ok uₐ
     uₐ ‖ (st₀; keys₀)  ⇓ᵤ  (Err @τ v, itr)
   —————————————————————————————————————————————————————————————————————— EvUpdExercNonConsumErr
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     (Err @τ v, 'iexercise' v₁ (cid, Mod:T, vₜ) 'non-consuming' itr)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'non-consuming' Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₚ
     v₁ =ₛ vₚ
     eₒ[x ↦ vₜ, z ↦ v₂]  ⇓  Ok vₒ
     eₐ[x ↦ vₜ, y ↦ cid, z ↦ v₂]  ⇓  Ok uₐ
     uₐ ‖ (st₀; keys₀)  ⇓ᵤ  Ok (vₐ, trₐ) ‖ (st₁, keys₁)
   —————————————————————————————————————————————————————————————————————— EvUpdExercNonConsum
     'exercise' Mod:T.Ch cid v₁ v₂ ‖ (st₀, keys₀)
       ⇓ᵤ
     Ok (vₐ, 'exercise' v₁ (cid, Mod:T, vₜ) 'non-consuming' trₐ) ‖ (st₁, keys₁)

     cid ∉ dom(st)
     v = 'MAKE_CONTRACT_ERROR' "Exercise on unknown contract"
   —————————————————————————————————————————————————————————————————————— EvUpdExercWithoutActorsMissing
     'exercise_without_actors' Mod:T.Ch cid v ‖ (st, keys)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₁]  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdExercWithoutActorsErr
     'exercise_without_actors' Mod:T.Ch cid v₁ ‖ (st₀, keys₀)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' ChKind Ch (y : 'ContractId' Mod:T) (z : τ) : σ 'by' eₚ 'observers' eₒ ↦ eₐ, … }, … }  ∈  〚Ξ〛Mod
     cid ∈ dom(st₀)
     st₀(cid) = (Mod:T, vₜ, 'active')
     eₚ[x ↦ vₜ, z ↦ v₁]  ⇓  Ok vₚ
     'exercise' Mod:T.Ch cid vₚ v₁ ‖ (st₀, keys₀)  ⇓ᵤ  ur
   —————————————————————————————————————————————————————————————————————— EvUpdExercWithoutActors
     'exercise_without_actors' Mod:T.Ch cid v₁ ‖ (st₀, keys₀)  ⇓ᵤ  ur

     cid ∉ dom(st)
     v = 'MAKE_CONTRACT_ERROR' "Exercise on unknown contract"
   —————————————————————————————————————————————————————————————————————— EvUpdFetchMissing
     'fetch' @Mod:T cid ‖ (st; keys)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod
     cid ∈ dom(st)
     st(cid) = (Mod:T, vₜ, 'inactive')
     v = 'MAKE_CONTRACT_ERROR' "Exercise on inactive contract"
   —————————————————————————————————————————————————————————————————————— EvUpdFetchInactive
     'fetch' @Mod:T cid ‖ (st; keys)
       ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T) ↦ …  ∈  〚Ξ〛Mod
     cid ∈ dom(st)
     st(cid) = (Mod:T, vₜ, 'active')
   —————————————————————————————————————————————————————————————————————— EvUpdFetch
     'fetch' @Mod:T cid ‖ (st; keys)
       ⇓ᵤ
     (Ok vₜ, ε) ‖ (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈ 〚Ξ〛Mod
     (eₘ vₖ)  ⇓  Err @τ v
    —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyErr
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  Ok  vₘ
     (Mod:T, vₖ) ∉ dom(keys₀)
     v = 'MAKE_CONTRACT_ERROR' "Lookup key not found"
    —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyNotFound
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)
        ⇓ᵤ
     (Err @'ContractError' v, ε)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  Ok  vₘ
     (Mod:T, vₖ) ∈ dom(keys)
     cid = keys((Mod:T, v))
     st(cid) = (Mod:T, vₜ, 'inactive')
     v = 'MAKE_CONTRACT_ERROR' "Exercise on inactive contract"
   —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyInactive
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)
        ⇓ᵤ
     (Err @'Contractrror' v, ε)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  Ok  vₘ
     (Mod:T, vₖ) ∈ dom(keys)
     cid = keys((Mod:T, v))
     st(cid) = (Mod:T, vₜ, 'active')
   —————————————————————————————————————————————————————————————————————— EvUpdFetchByKeyFound
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)
        ⇓ᵤ
     (Ok ⟨'contractId': cid, 'contract': vₜ⟩, ε) ‖ (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  Err @τ v
   —————————————————————————————————————————————————————————————————————— EvUpdLookupByKeyErr
     'lookup_by_key' @Mod:T vₖ ‖ (st; keys)  ⇓ᵤ  (Err @τ v, ε)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  vₘ
     (Mod:T, vₖ) ∉ dom(keys)
   —————————————————————————————————————————————————————————————————————— EvUpdLookupByKeyNotFound
     'lookup_by_key' @Mod:T vₖ ‖ (st; keys)
       ⇓ᵤ
     (Ok ('None' @('ContractId' Mod:T)), ε) ‖ (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈  〚Ξ〛Mod
     (eₘ vₖ)  ⇓  vₘ
     (Mod:T, vₖ) ∈ dom(keys)
     cid = keys((Mod:T, v))
   —————————————————————————————————————————————————————————————————————— EvUpdLookupByKeyFound
     'lookup_by_key' @Mod:T vₖ ‖ (st; keys)
       ⇓ᵤ
     (Ok ('Some' @('ContractId' Mod:T) cid), ε) ‖ (st; keys)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈ 〚Ξ〛Mod
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)  ⇓ᵤ  (Err @τ v, itr)
   —————————————————————————————————————————————————————————————————————— EvUpdExercByKeyFetchErr
     'exercise_by_key' Mod:T.Ch vₖ v₁ ‖ (st; keys)  ⇓ᵤ  (Err @τ v, itr)

     'tpl' (x : T) ↦ { …, 'key' @σ eₖ eₘ }  ∈ 〚Ξ〛Mod
     'fetch_by_key' @Mod:T vₖ ‖ (st; keys)  ⇓ᵤ  (Ok ⟨'contractId': cid, 'contract': vₜ⟩, ε) ‖ (st'; keys')
     'exercise_without_actor' Mod:T.Ch cid v₁ ‖ (st'; keys')  ⇓ᵤ  ur
   —————————————————————————————————————————————————————————————————————— EvUpdExercByKeyExercise
     'exercise_by_key' Mod:T.Ch vₖ v₁ ‖ (st; keys)  ⇓ᵤ  ur

     LitTimestamp is the current ledger time
   —————————————————————————————————————————————————————————————————————— EvUpdGetTime
     'get_time' ‖ (st; keys)
       ⇓ᵤ
     (Ok LitTimestamp, ε) ‖ (st; keys)

     e  ⇓  Err t
   —————————————————————————————————————————————————————————————————————— EvUpdEmbedExprErr
     'embed_expr' @τ e ‖ (st; keys)  ⇓ᵤ  (Err t, ε)

     e  ⇓  Ok u
     u ‖ (st; keys)  ⇓ᵤ  ur
   —————————————————————————————————————————————————————————————————————— EvUpdEmbedExpr
     'embed_expr' @τ e ‖ (st; keys)  ⇓ᵤ  ur

     e₁  ⇓  Ok u₁
     u₁ ‖ S₀  ⇓ᵤ  (Ok v₁, tr₁) ‖ S₁
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchOk
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Ok v₁, tr₁) ‖ S₁

     e₁  ⇓  Err @τ₁ v₁
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('None' @σ)
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr1_NoHandle
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, ε)

     e₁  ⇓  Ok u₁
     u₁ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, itr₁)
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('None' @σ)
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr2_NoHandle
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, itr₁)

     e₁  ⇓  Err @τ₁ v₁
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('Some' @σ u₂)
     u2 ‖ S₀  ⇓ᵤ  (Ok v₂, tr₂) ‖ S₂
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr1_OkHandle_Ok
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Ok v₂, ('rollback' @τ₁ v₁ ε) ⋅ tr₂) ‖ S₂

     e₁  ⇓  Ok u₁
     u₁ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, itr₁)
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('Some' @σ u₂)
     u2 ‖ S₀  ⇓ᵤ  (Ok v₂, tr₂) ‖ S₂
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr2_OkHandle_Ok
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Ok v₂, ('rollback' @τ₁ v₁ itr₁) ⋅ tr₂) ‖ S₂

     e₁  ⇓  Err @τ₁ v₁
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('Some' @σ u₂)
     u2 ‖ S₀  ⇓ᵤ  (Err @τ₂ v₂, itr₂)
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr1_OkHandle_Err
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Err @τ₂ v₂, ('rollback' @τ₁ v₁ ε) ⋅ itr₂)

     e₁  ⇓  Ok u₁
     u₁ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, itr₁)
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Ok ('Some' @σ u₂)
     u2 ‖ S₀  ⇓ᵤ  (Err @τ₂ v₂, itr₂)
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr2_OkHandle_Err
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Err @τ₂ v₂, ('rollback' v₁ itr₁) ⋅ itr₂)

     e₁  ⇓  Err @τ₁ v₁
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Err @τ₂ v₂
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr1_ErrHandle
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Err @τ₂ v₂, ('rollback' @τ₁ v₁ ε))

     e₁  ⇓  Ok u₁
     u₁ ‖ S₀  ⇓ᵤ  (Err @τ₁ v₁, itr₁)
     e₂[x ↦ 'to_any_exception' @τ₁ v₁]  ⇓  Err @τ₂ v₂
   —————————————————————————————————————————————————————————————————————— EvUpdTryCatchErr2_ErrHandle
     'try' @τ e₁ 'catch' x. e₂ ‖ S₀
       ⇓ᵤ
     (Err @τ₂ v₂, ('rollback' @τ₁ v₁ itr₁))


About scenario interpretation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The interpretation of scenarios is a feature an engine can provide to
test business logic within a Daml-LF archive. Nevertheless, the
present specification does not define how scenarios should be actually
interpreted. An engine compliant with this specification does not have
to provide support for scenario interpretation. It must however accept
loading any `valid <Validation_>`_ archive that contains scenario
expressions, and must handle update statements that actually
manipulate expressions of type `Scenario τ`. Note that the semantics
of `Update interpretation`_ (including evaluation of `expression
<expression evaluation_>`_ and `built-in functions`_) guarantee that
values of type `'Scenario' τ` cannot be scrutinized and can only be
"moved around" as black box arguments by the different functions
evaluated during the interpretation of an update.


Built-in functions
^^^^^^^^^^^^^^^^^^

This section lists the built-in functions supported by Daml LF 1.
The functions come with their types and a description of their
behavior.

Generic comparison functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following builtin functions defines an order on the so-called
`comparable` values. Comparable values are LF values except type
abstractions, functions, partially applied builtin functions, and
updates.

* ``LESS_EQ : ∀ (α:*). α → α → 'Bool'``

  The builtin function ``LESS_EQ`` returns ``'True'`` if the first
  argument is smaller than or equal to the second argument,
  ``'False'`` otherwise. The function raises a runtime error if the
  arguments are incomparable.

  [*Available in version >= 1.11*]

  Formally the builtin function ``LESS_EQ`` semantics is defined by
  the following rules. Note the rules assume ``LESS_EQ`` is fully
  applied and well-typed, in particular ``LESS_EQ`` always compared
  value of the same type.::

    —————————————————————————————————————————————————————————————————————— EvLessEqUnit
      𝕆('LESS_EQ' @σ () ()) = Ok 'True'

    —————————————————————————————————————————————————————————————————————— EvLessEqBool
      𝕆('LESS_EQ' @σ b₁ b₂) = Ok (¬b₁ ∨ b₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqInt64
      𝕆('LESS_EQ' @σ LitInt64₁ LitInt64₂) = Ok (LitInt64₁ ≤ₗ LitInt64₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqDate
      𝕆('LESS_EQ' @σ LitDate₁ LitDate₂) = Ok (LitDate₁ ≤ₗ LitDate₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqTimestamp
      𝕆('LESS_EQ' @σ LitTimestamp₁ LitTimestamp₂) =
          Ok (LitTimestamp₁ ≤ₗ LitTimestamp₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqText
      𝕆('LESS_EQ' @σ LitText₁ LitText₂) = Ok (LitText₁ ≤ₗ LitText₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqParty
      𝕆('LESS_EQ' @σ LitParty₁ LitParty₂) = Ok (LitParty₁ ≤ₗ LitParty₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqNumeric
      𝕆('LESS_EQ' @σ LitNumeric₁ LitNumeric₂) =
          Ok (LitNumeric₁ ≤ₗ LitNumeric₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqBigNumeric
      𝕆('LESS_EQ' @σ LitBigNumeric₁ LitBigNumeric₂) =
          Ok (LitBigNumeric₁ ≤ₗ LitBigNumeric₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqContractId
      𝕆('LESS_EQ' @σ cid₁ cid₂) = Ok (cid₁ ≤ₗ cid₂)

    —————————————————————————————————————————————————————————————————————— EvLessEqStructEmpty
      𝕆('LESS_EQ' @⟨ ⟩ ⟨ ⟩ ⟨ ⟩) = Ok 'True'

      𝕆('LESS_EQ' @τ₀ v₀ v₀') = Err t
    —————————————————————————————————————————————————————————————————————— EvLessEqStructNonEmptyHeadErr
      𝕆('LESS_EQ' @⟨ f₀: τ₀,  f₁: τ₁, …,  fₙ: τₙ ⟩
                   ⟨ f₀= v₀,  f₁= v₁, …,  fₘ= vₘ ⟩
                   ⟨ f₀= v₀', f₁= v₁', …, fₘ= vₘ' ⟩) = Err t

      𝕆('LESS_EQ' @τ₁ v₀ v₀') = Ok 'False'
    ————————————————————————————————————————————————————————————————————— EvLessEqStructNonEmptyHeadBigger
      𝕆('LESS_EQ' @⟨ f₀: τ₀,  f₁: τ₁, …,  fₙ: τₙ  ⟩
                   ⟨ f₀= v₀,  f₁= v₁, …,  fₘ= vₘ  ⟩
      	           ⟨ f₀= v₀', f₁= v₁', …, fₘ= vₘ' ⟩) = Ok 'False'

      𝕆('LESS_EQ' @τ₀ v₀ v₀') = Ok 'True'
      𝕆('LESS_EQ' @τ₀ v₀' v₀) = Ok 'False'
    ————————————————————————————————————————————————————————————————————— EvLessEqStructNonEmptyHeadSmaller
      𝕆('LESS_EQ' @⟨ f₀: τ₀,  f₁: τ₁, …,  fₙ: τₙ  ⟩
                   ⟨ f₀= v₀,  f₁= v₁, …,  fₘ= vₘ  ⟩
                   ⟨ f₀= v₀', f₁= v₁', …, fₘ= vₘ' ⟩) = Ok 'True'

      𝕆('LESS_EQ' @τ₀ v₀ v₀') = Ok 'True'
      𝕆('LESS_EQ' @τ₀ v₀' v₀) = Ok 'True'
      𝕆('LESS_EQ' @⟨ f₁: τ₁, …,  fₙ: τₙ  ⟩
                   ⟨ f₁= v₁, …,  fₘ= vₘ  ⟩
                   ⟨ f₁= v₁', …, fₘ= vₘ' ⟩) = r
    —————————————————————————————————————————————————————————————————————— EvLessEqStructNonEmptyTail
      𝕆('LESS_EQ' @⟨ f₀: τ₀,  f₁: τ₁, …,  fₙ: τₙ ⟩
                   ⟨ f₀= v₀,  f₁= v₁, …,  fₘ= vₘ ⟩
                   ⟨ f₀= v₀', f₁= v₁', …, fₘ= vₘ' ⟩) = r

      'enum' T ↦ E₁: σ₁ | … | Eₘ: σₘ  ∈  〚Ξ〛Mod
    —————————————————————————————————————————————————————————————————————— EvLessEqEnum
      𝕆('LESS_EQ' @σ Mod:T:Eᵢ Mod:T:Eⱼ) = OK (i ≤ j)

      'variant' T α₁ … αₙ ↦ V₁: σ₁ | … | Vₘ: σₘ  ∈  〚Ξ〛Mod     i ≠ j
    —————————————————————————————————————————————————————————————————————— EvLessEqVariantConstructor
      𝕆('LESS_EQ' @σ (Mod:T:Vᵢ @σ₁ … @σₙ v) (Mod:T:Vⱼ @σ₁' … @σₙ' v') =
          OK (i ≤ j)

      'variant' T α₁ … αₙ ↦ V₁: τ₁ | … | Vₘ: τₘ  ∈  〚Ξ〛Mod
      τᵢ  ↠  τᵢ'    𝕆('LESS_EQ' @(τᵢ'[α₁ ↦ σ₁, …, αₙ ↦ σₙ]) v v') = r
    —————————————————————————————————————————————————————————————————————— EvLessEqVariantValue
      𝕆('LESS_EQ' @σ (Mod:T:Vᵢ @σ₁ … @σₙ v) (Mod:T:Vᵢ @σ₁' … @σₙ' v')) = r

      'record' T (α₁:k₁) … (αₙ:kₙ) ↦ { f₁:τ₁, …, fₘ:τₘ }  ∈ 〚Ξ〛Mod
      'τ₁  ↠  τ₁'  …   τᵢ  ↠  τᵢ'
      𝕆('LESS_EQ' @⟨ f₁: τ₁'[α₁ ↦ σ₁, …, αₙ ↦ σₙ],
                       …, fₙ: τₙ'[α₁ ↦ σ₁, …, αₙ ↦ σₙ]⟩
                   ⟨ f₁= v₁, …,  fₘ = vₘ ⟩
   	               ⟨ f₁= v₁', …, fₘ = vₘ' ⟩) = r
    —————————————————————————————————————————————————————————————————————— EvLessEqRecord
      𝕆('LESS_EQ' @σ (Mod:T @σ₁  … @σₙ  { f₁ = v₁ , …, fₘ = vₘ  })
                     (Mod:T @σ₁' … @σₙ' { f₁ = v₁', …, fₘ = vₘ' })) =  r

    —————————————————————————————————————————————————————————————————————— EvLessEqListNil
      𝕆('LESS_EQ' @σ (Nil @τ) v) = Ok 'True'

    —————————————————————————————————————————————————————————————————————— EvLessEqListConsNil
      𝕆('LESS_EQ' @σ (Cons @τ vₕ vₜ)  (Nil @τ')) = Ok 'False'

      𝕆('LESS_EQ' @⟨ h:τ,    t: 'List' τ ⟩
                   ⟨ h= vₕ,  t= vₜ       ⟩
                   ⟨ h= vₕ', t= vₜ'      ⟩) = r
    —————————————————————————————————————————————————————————————————————— EvLessEqListConsCons
      𝕆('LESS_EQ' @σ (Cons @τ vₕ vₜ) (Cons @τ' vₕ vₜ)) = r

    —————————————————————————————————————————————————————————————————————— EvLessEqOptionNoneAny
      𝕆('LESS_EQ' @σ (None @τ) v) = Ok 'True'

    —————————————————————————————————————————————————————————————————————— EvLessEqOptionSomeNone
      𝕆('LESS_EQ' @σ (Some @τ v)  (None @τ')) = Ok 'False'

      𝕆('LESS_EQ' @τ v v') = r
    —————————————————————————————————————————————————————————————————————— EvLessEqOptionSomeSome
      𝕆('LESS_EQ' @σ (Some @τ v) (Some @τ' v')) = r

    —————————————————————————————————————————————————————————————————————— EvLessEqGenMapEmptyAny
      𝕆('LESS_EQ' σ 〚〛v) = Ok 'True'

      n > 0
    —————————————————————————————————————————————————————————————————————— EvLessEqGenMapNonEmptyEmpty
      𝕆('LESS_EQ' σ 〚v₁ ↦ w₁; …; vₙ ↦ wₙ〛〚〛) = Ok 'FALSE'

      𝕆('LESS_EQ' @⟨ hₖ: σₖ,  hᵥ: σᵥ,  t: 'GenMap' σₖ σᵥ ⟩
                   ⟨ hₖ= v₀,  hᵥ= wₒ , t= 〚v₁  ↦ w₁ ; …; vₙ  ↦ wₙ 〛⟩
                   ⟨ hₖ= v₀', hᵥ= wₒ', t= 〚v₁' ↦ w₁'; …; vₙ' ↦ wₙ'〛⟩ = r
    —————————————————————————————————————————————————————————————————————— EvLessEqGenMapNonEmptyNonEmpty
      𝕆('LESS_EQ' @('GenMap' σₖ σᵥ)
                   〚v₀  ↦ w₀ ; v₁  ↦ w₁ ; …; vₙ  ↦ wₙ 〛
                   〚v₀' ↦ w₀'; v₁' ↦ w₁'; …; vₙ' ↦ wₙ'〛) = r

      𝕆('LESS_EQ' @('GenMap' 'Text' σ)
                   〚t₁  ↦ v₁ ; …; tₙ  ↦ vₙ 〛
                   〚t₁' ↦ v₁'; …; tₙ' ↦ vₙ'〛) = r
    —————————————————————————————————————————————————————————————————————— EvLessEqTextMap
      𝕆('LESS_EQ' @('TextMap' σ)
                    [t₁  ↦ v₁ ; …; tₙ  ↦ vₙ ]
                    [t₁' ↦ v₁'; …; tₙ' ↦ vₙ']) = r

    —————————————————————————————————————————————————————————————————————— EvLessEqTypeRep
      𝕆('LESS_EQ' @σ ('type_rep' @σ₁) ('type_rep' @σ₂)) = Ok (σ₁ ≤ₜ σ₂)

      τ <ₜ τ'
    —————————————————————————————————————————————————————————————————————— EvLessEqAnyTypeSmaller
      𝕆('LESS_EQ' @σ ('to_any' @τ v) ('to_any' @τ' v')) = OK 'True'

      τ' <ₜ τ
    —————————————————————————————————————————————————————————————————————— EvLessEqAnyTypeGreater
      𝕆('LESS_EQ' @σ ('to_any' @τ v) ('to_any' @τ' v')) = OK 'False'

      𝕆('LESS_EQ' @τ v v') = r
    —————————————————————————————————————————————————————————————————————— EvLessEqAnyValue
      𝕆('LESS_EQ' @σ ('to_any' @τ v) ('to_any' @τ v')) = r

    —————————————————————————————————————————————————————————————————————— EvLessEqAbs
      𝕆('LESS_EQ' @(σ → τ) v v' = Err 'Try to compare functions'

    —————————————————————————————————————————————————————————————————————— EvLessEqTyAbs
      𝕆('LESS_EQ' @(∀ α : k . σ) v v' = Err 'Try to compare functions'

    —————————————————————————————————————————————————————————————————————— EvLessEqUpdate
      𝕆('LESS_EQ' @('Update' σ) v v' = Err 'Try to compare functions'

    —————————————————————————————————————————————————————————————————————— EvLessEqScenario
      𝕆('LESS_EQ' @('Scenario' σ) v v' = Err 'Try to compare functions'

..
  FIXME: https://github.com/digital-asset/daml/issues/2256
    Handle contract IDs

* ``GREATER_EQ : ∀ (α:*). α → α → 'Bool'``

  The builtin function ``GREATER_EQ`` returns ``'True'`` if the first
  argument is greater than or equal to the second argument,
  ``'False'`` otherwise. The function raises a runtime error if the
  arguments are incomparable.

  [*Available in version >= 1.11*]

  Formally the function is defined as a shortcut for the function::

    'GREATER_EQ' ≡
        Λ α : ⋆. λ x : α . λ y : b.
	    'LESS_EQ' @α y x

* ``EQUAL : ∀ (α:*). α → α → 'Bool'``

  The builtin function ``EQUAL`` returns ``'True'`` if the first
  argument is equal to the second argument, ``'False'`` otherwise. The
  function raises a runtime error if the arguments are incomparable.

  [*Available in version >= 1.11*]

  Formally the function is defined as a shortcut for the function::

    'EQUAL' ≡
        Λ α : ⋆. λ x : α . λ y : b.
	    'case' 'LESS_EQ' @α x y 'of'
	            'True' → 'GREATER_EQ' @α x y
		'|' 'False' → 'False'

  [*Available in version >= 1.11*]

* ``LESS : ∀ (α:*). α → α → 'Bool'``

  The builtin function ``LESS`` returns ``'True'`` if the first
  argument is strictly less that the second argument, ``'False'``
  otherwise. The function raises a runtime error if the arguments are
  incomparable.

  [*Available in version >= 1.11*]

  Formally the function is defined as a shortcut for the function::

    'LESS' ≡
        Λ α : ⋆. λ x : α . λ y : b.
	    'case' 'EQUAL' @α x y 'of'
	           'True' → 'False'
	       '|' 'False' → 'LESS_EQ' α x y

* ``GREATER : ∀ (α:*). α → α → 'Bool'``

  The builtin function ``LESS`` returns ``'True'`` if the first
  argument is strictly greater that the second argument, ``'False'``
  otherwise. The function raises a runtime error if the arguments are
  incomparable.

  [*Available in version >= 1.11*]

  Formally the function is defined as a shortcut for the function::

    'GREATER' ≡
        Λ α : ⋆. λ x : α . λ y : b.
	    'case' 'EQUAL' @α x y 'of'
	          'True' → 'False'
	      '|' 'False' → 'GREATER_EQ' α x y

Boolean functions
~~~~~~~~~~~~~~~~~

* ``EQUAL_BOOL : 'Bool' → 'Bool' → 'Bool'``

  Returns ``'True'`` if the two booleans are syntactically equal,
  ``False`` otherwise.

  [*Available in version < 1.11*]

Int64 functions
~~~~~~~~~~~~~~~

* ``ADD_INT64 : 'Int64' → 'Int64' → 'Int64'``

  Adds the two integers. In case of an overflow, throws an exception
  ``'MAKE_ARITHMETIC_ERROR' t``, where ``t = "Overflow: ADD_INT64 {m} {n}"``
  for ``m`` and ``n`` the actual values of the operands.

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

  [*Available in version < 1.11*]

* ``TO_TEXT_INT64 : 'Int64' → 'Text'``

  Returns the decimal representation of the integer as a string.

* ``FROM_TEXT_INT64 : 'Text' → 'Optional' 'Int64'``

  Given a string representation of an integer returns the integer wrapped
  in ``Some``. If the input does not match the regexp ``[+-]?\d+`` or
  if the result of the conversion overflows, returns ``None``.

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

* ``SHIFT_NUMERIC : ∀ (α₁, α₂: nat) . 'Numeric' α₁ → 'Numeric' α₂``

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

  [*Available in version < 1.11*]

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

BigNumeric functions
~~~~~~~~~~~~~~~~~~~~

* ``ADD_BIGNUMERIC : 'BigNumeric' → 'BigNumeric'  → 'BigNumeric'``

  Adds the two decimals. Throws an ``ArithmeticError`` if the output is not a valid BigNumeric.

* ``SUB_BIGDECIMAL : 'BigNumeric' → 'BigNumeric' → 'BigNumeric'``

  Subtracts the two decimals. Throws an ``ArithmeticError`` if the output is not a valid BigNumeric.

* ``MUL_BIGDECIMAL : 'BigNumeric' → 'BigNumeric' → 'BigNumeric'``

  Multiplies the two numerics. Throws an ``ArithmeticError`` if the output is not a valid BigNumeric.

* ``DIV_BIGDECIMAL : 'RoundingMode' → 'Int' → 'BigNumeric' → 'BigNumeric' → 'BigNumeric'``

  Divides the first decimal by the second one and rounds the result according the rounding mode.
  The scale of the output is given by the second argument.
  If the result cannot be represented exactly in at most ``precision`` digits, the result is
  rounded to ``precision`` accordingly the ``roundingMode`` as follows:

  - ``'RoundingCeiling'`` : Rounds towards positive infinity.

  - ``'RoundingFloor'`` : Rounds towards negative infinity

  - ``'RoundingDown'`` : Rounds towards towards zero

  - ``'RoundingUp'`` : Round towards away from zero

  - ``'RoundingHalfDown'`` : Round towards the nearest neighbor unless
    both neighbors are equidistant, in which case round towards zero.

  - ``'RoundingHalfEven'`` : Rounds towards the nearest neighbor unless
    both neighbors are equidistant, in which case round towards the even
    neighbor.

  - ``'RoundingHalfUp'`` : Round towards the nearest neighbor unless
    both neighbors are equidistant, in which case round away from zero.

  - ``'RoundingUnnecessary'`` : Throw if the exact result cannot be
    represented.

  Throws an ``ArithmeticError`` if the output is not a valid BigNumeric.

* ``SCALE_BIGNUMERIC : 'BigNumeric' → 'Int64'``

  return the scale of the BigNumeric

* ``SCALE_BIGNUMERIC : 'BigNumeric' → 'Int64'``

  return the precision of the BigNumeric

* ``TO_TEXT_BIGNUMERIC : 'BigNumeric' → 'Text'``

  Returns the numeric string representation of the BigNumeric. The result
  will be returned at the smallest precision that can represent the result exactly, i.e.,
  without any trailing zeroes.

* ``'TO_NUMERIC_BIGNUMERIC' : ∀ (α : nat). 'BigNumeric'  → 'Numeric' α``

  Convert the ``BigNumeric`` to a ``Numeric α`` value with scale ``α``.
  Throws an ``ArithmeticError`` in case the result cannot be represented without loss of precision.

* ``'TO_BIGNUMERIC_NUMERIC' : ∀ (α : nat). 'Numeric' α  → 'BigNumeric'``

  Convert the ``Numeric`` to a ``BigDecimal``. This is always exact.

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

  [*Available in version < 1.11*]

* ``TO_TEXT_TEXT : 'Text' → 'Text'``

  Returns string such as.

* ``TEXT_TO_CODE_POINTS``: 'Text' → 'List' 'Int64'

  Returns the list of the Unicode `codepoints
  <https://en.wikipedia.org/wiki/Code_point>`_ of the input
  string represented as integers.

* ``TEXT_FROM_CODE_POINTS``: 'List' 'Int64' → 'Text'

  Given a list of integer representations of Unicode codepoints,
  return the string built from those codepoints. Throws an error
  if one of the elements of the input list is not in the range
  from `0x000000` to `0x00D7FF` or in the range from `0x00DFFF`
  to `0x10FFFF` (bounds included).

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

  [*Available in version < 1.11*]

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

  * The output will not change within minor version of Daml-LF 1.


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

  [*Available in version < 1.11*]

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

* ``LESS_EQ_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is less or equal than the
  second, ``'False'`` otherwise.

* ``GREATER_EQ_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is greater or equal than the
  second, ``'False'`` otherwise.

* ``LESS_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is strictly less than the
  second, ``'False'`` otherwise.

* ``GREATER_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is strictly greater than the
  second, ``'False'`` otherwise.

* ``EQUAL_PARTY : 'Party' → 'Party' → 'Bool'``

  Returns ``'True'`` if the first party is equal to the second,
  ``'False'`` otherwise.

  [*Available in version < 1.11*]

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

* ``FROM_TEXT_PARTY : 'Text' → 'Optional' 'Party'``

  Given the string representation of the party, returns the party,
  if the input string is a `PartyId strings <Literals_>`_.

ContractId functions
~~~~~~~~~~~~~~~~~~~~

* ``EQUAL_CONTRACT_ID  : ∀ (α : ⋆) . 'ContractId' α → 'ContractId' α → 'Bool'``

  Returns ``'True'`` if the first contact id is equal to the second,
  ``'False'`` otherwise.

* ``COERCE_CONTRACT_ID  : ∀ (α : ⋆) (β : ⋆) . 'ContractId' α → 'ContractId' β``

  Returns the given contract ID unchanged at a different type.

* ``TO_TEXT_CONTRACT_ID : ∀ (α : ⋆) . 'ContractId' α -> 'Optional' 'Text'``

  Always returns ``None`` in ledger code. This function is only useful
  for off-ledger code which is not covered by this specification.

  [*Available in versions >= 1.11*]

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


Text map functions
~~~~~~~~~~~~~~~~~~

**Entry order**: The operations below always return a map with entries
ordered by keys.

* ``TEXTMAP_EMPTY : ∀ α. 'TextMap' α``

  Returns the empty TextMap.

* ``TEXTMAP_INSERT : ∀ α.  'Text' → α → 'TextMap' α → 'TextMap' α``

  Inserts a new key and value in the map. If the key is already
  present in the map, the associated value is replaced with the
  supplied value.

* ``TEXTMAP_LOOKUP : ∀ α. 'Text' → 'TextMap' α → 'Optional' α``

  Looks up the value at a key in the map.

* ``TEXTMAP_DELETE : ∀ α. 'Text' → 'TextMap' α → 'TextMap' α``

  Deletes a key and its value from the map. When the key is not a
  member of the map, the original map is returned.

* ``TEXTMAP_TO_LIST : ∀ α. 'TextMap' α → 'List' ⟨ key: 'Text', value: α  ⟩``

  Converts to a list of key/value pairs. The output list is guaranteed to be
  sorted according to the ordering of its keys.

* ``TEXTMAP_SIZE : ∀ α. 'TextMap' α → 'Int64'``

  Return the number of elements in the map.

Generic map functions
~~~~~~~~~~~~~~~~~~~~~

**Validity of Keys:** A key is valid if and only if it is equivalent
to itself according to the builtin function  ``EQUAL``. Attempts to
use an invalid key in the operations listed under always result
in a runtime error.

Of particular note, the following values are never valid keys:

* Lambda expressions ``λ x : τ . e``
* Type abstractions ``Λ α : k . e``
* (Partially applied) built-in functions
* Update statement
* Any value containing an invalid key

**Entry order**: The operations below always return a map with entries
ordered by keys according to the comparison function ``LESS``.

* ``GENMAP_EMPTY : ∀ α. ∀ β. 'GenMap' α β``

  Returns an empty generic map.

  [*Available in versions >= 1.11*]

* ``GENMAP_INSERT : ∀ α. ∀ β.  α → β → 'GenMap' α β → 'GenMap' α β``

  Inserts a new key and value in the map. If the key is already
  present according the builtin function ``EQUAL``, the associated
  value is replaced with the supplied value, otherwise the key/value
  is inserted in order according to the builtin function ``LESS`` applied
  on keys. This raises a runtime error if it tries to compare
  incomparable values.

  [*Available in versions >= 1.11*]

  Formally the builtin function ``GENMAP_INSERT`` semantics is defined
  by the following rules. ::

      𝕆('EQUAL' @σ v v) = Err t
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertReplaceErr
      𝕆('GENMAP_INSERT' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v w) = Err t

    —————————————————————————————————————————————————————————————————————— EvGenMapInsertEmpty
       𝕆('GENMAP_INSERT' @σ @τ 〚〛 v w) = 〚v ↦ w〛

       𝕆('EQUAL' @σ vᵢ v) = Ok 'True'    for some i ∈ 1, …, n
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertReplace
      𝕆('GENMAP_INSERT' @σ @τ 〚v₁ ↦ w₁; …; vₙ ↦ wₙ〛 v w) =
        'Ok' 〚v₁ ↦ w₁; …; vᵢ₋₁ ↦ wᵢ₋₁; vᵢ ↦ w;  vᵢ₊₁ ↦ wᵢ₊₁; …; vₙ ↦ wₙ〛

      𝕆('LESS' @σ v v₁) = Ok 'True'
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertInsertFirst
      𝕆('GENMAP_INSERT' @σ @τ 〚v₁ ↦ w₁; …; vₙ ↦ wₙ〛 v w) =
        'Ok' 〚v ↦ w; v₁ ↦ w₁; …; vₙ ↦ wₙ〛

      𝕆('LESS' @σ vᵢ₋₁ v) = Ok 'True'
      𝕆('LESS' @σ v vᵢ) = Ok 'True'
      for some i ∈ 2, …, n-1
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertInsertMiddle
      𝕆('GENMAP_INSERT' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v w) =
        'Ok' 〚v₁ ↦ w₁; … ; vᵢ₋₁ ↦ wᵢ₋₁; v ↦ w;  vᵢ ↦ wᵢ; … ; vₙ ↦ wₙ〛

      𝕆('LESS' @σ vₙ v) = Ok 'True'
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertInsertLast
      𝕆('GENMAP_INSERT' @σ @τ 〚v₁ ↦ w₁; …; vₙ ↦ wₙ〛 v w) =
        'Ok' 〚v₁ ↦ w₁; …; vₙ ↦ wₙ; v ↦ w〛


* ``GENMAP_LOOKUP : ∀ α. ∀ β.  α → 'GenMap' α β → 'Optional' α``

  Looks up the value at a key in the map using the builtin function
  ``EQUAL`` to test key equality. This raises a runtime error if it
  try to compare incomparable values.

  [*Available in versions >= 1.11*]

  Formally the builtin function ``GENMAP_LOOKUP`` semantics is defined
  by the following rules. ::

      𝕆('EQUAL' @σ v v) = Err t
    —————————————————————————————————————————————————————————————————————— EvGenMapInsertReplaceErr
      𝕆('GENMAP_LOOKUP' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) = Err t

    —————————————————————————————————————————————————————————————————————— EvGenMapLookupErr
      𝕆('GENMAP_LOOKUP' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) = Err t

      𝕆('EQUAL' @σ vᵢ v) = Ok 'True'  for some i ∈ 1, …, n
    —————————————————————————————————————————————————————————————————————— EvGenMapLookupPresent
      𝕆('GENMAP_LOOKUP' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) =
        'Ok' (Some wᵢ)

      𝕆('EQUAL' @σ vᵢ v) = Ok 'False'  for all i ∈ 1, …, n
    —————————————————————————————————————————————————————————————————————— EvGenMapLookupAbsent
      𝕆('GENMAP_LOOKUP' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) =
        'Ok' None

* ``GENMAP_DELETE : ∀ α. ∀ β.  α → 'GenMap' α β → 'GenMap' α β``

  Deletes a key and its value from the map, using the builtin function
  ``EQUAL`` to test key equality. When the key is not a member of the
  map, the original map is returned.  This raises a runtime error if it
  try to compare incomparable values.

  [*Available in versions >= 1.11*]

  Formally the builtin function ``GENMAP_DELETE`` semantics is defined
  by the following rules. ::

      𝕆('EQUAL' @σ v v) = Err t
    —————————————————————————————————————————————————————————————————————— EvGenMapDeleteErr
      𝕆('GENMAP_DELETE' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) = Err t

      𝕆('EQUAL' @σ vᵢ v) = Ok 'True'  for some i ∈ 1, …, n
    —————————————————————————————————————————————————————————————————————— EvGenMapDeletePresent
      𝕆('GENMAP_DELETE' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) =
        Ok' 〚v₁ ↦ w₁; … ; vᵢ₋₁ ↦ wᵢ₋₁; vᵢ₊₁ ↦ wᵢ₊₁; … ; vₙ ↦ wₙ〛

      𝕆('EQUAL' @σ vᵢ v) = Ok 'False'  for all i ∈ 1, …, n
    —————————————————————————————————————————————————————————————————————— EvGenMapDeleteAbsent
      𝕆('GENMAP_DELETE' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛 v) =
        'Ok' 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛

* ``GENMAP_KEYS : ∀ α. ∀ β.  'GenMap' α β → 'List' α``

  Get the list of keys in the map. The keys are returned in the order
  they appear in the map.

  [*Available in versions >= 1.11*]

  Formally the builtin function ``GENMAP_KEYS`` semantics is defined
  by the following rules. ::

    —————————————————————————————————————————————————————————————————————— EvGenMapKeysEmpty
      𝕆('GENMAP_KEYS' @σ @τ 〚〛) = 'Ok' (Nil @σ)

      𝕆('GENMAP_KEYS' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛) = 'Ok' vₗ
    —————————————————————————————————————————————————————————————————————— EvGenMapKeysNonEmpty
      𝕆('GENMAP_KEYS' @σ @τ 〚v₀ ↦ w₀; v₁ ↦ w₁; … ; vₙ ↦ wₙ〛) =
        'Ok' (Cons @σ v₀ vₗ)

* ``GENMAP_VALUES : ∀ α. ∀ β.  'GenMap' α β → 'List' β``

  Get the list of values in the map. The values are returned in the
  order they appear in the map (i.e. sorted by key).

  [*Available in versions >= 1.11*]

  Formally the builtin function ``GENMAP_VALUES`` semantics is defined
  by the following rules. ::

    —————————————————————————————————————————————————————————————————————— EvGenMapValuesEmpty
      𝕆('GENMAP_VALUES' @σ @τ 〚〛) = 'Ok' (Nil @τ)

      𝕆('GENMAP_VALUES' @σ @τ 〚v₁ ↦ w₁; … ; vₙ ↦ wₙ〛) = 'Ok' wₗ
    —————————————————————————————————————————————————————————————————————— EvGenMapValuesNonEmpty
      𝕆('GENMAP_KEYS' @σ @τ 〚v₀ ↦ w₀; v₁ ↦ w₁; … ; vₙ ↦ wₙ〛) =
        'Ok' (Cons @τ w₀ wₗ)

* ``GENMAP_SIZE : ∀ α. ∀ β.  'GenMap' α β → 'Int64'``

  Return the number of elements in the map.

  [*Available in versions >= 1.11*]

Type Representation function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``EQUAL_TYPE_REP`` : 'TypeRep' → 'TypeRep' → 'Bool'``

  Returns ``'True'`` if the first type representation is syntactically equal to
  the second one, ``'False'`` otherwise.

  [*Available in versions >= 1.7*]


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

  Throws a ``'GeneralError'`` with the string as message. Formally the function
  is defined as a shortcut for the function::

    'ERROR' ≡
        Λ (α : ⋆). λ (x : 'Text').
        'throw' @α @'GeneralError' ('MAKE_GENERAL_ERROR' x)

* ``ANY_EXCEPTION_MESSAGE : 'AnyException' → 'Text'``

  [*Available in version >= 1.dev*]

  Extract the error message from an ``'AnyException'``.

* ``MAKE_GENERAL_ERROR : 'Text' → 'GeneralError'``

  [*Available in version >= 1.dev*]

  Construct a ``'GeneralError'`` from its error message.

* ``GENERAL_ERROR_MESSAGE : 'GeneralError' → 'Text'``

  [*Available in version >= 1.dev*]

  Extract the error message from a ``'GeneralError'``.

* ``MAKE_ARITHMETIC_ERROR : 'Text' → 'ArithmeticError'``

  [*Available in version >= 1.dev*]

  Construct an ``'ArithmeticError'`` from its error message.

* ``ARITHMETIC_ERROR_MESSAGE : 'ArithmeticError' → 'Text'``

  [*Available in version >= 1.dev*]

  Extract the error message from ``'ArithmeticError'``.

* ``MAKE_CONTRACT_ERROR : 'Text' → 'ContractError'``

  [*Available in version >= 1.dev*]

  Construct a ``'ContractError'`` from its error message.

* ``CONTRACT_ERROR_MESSAGE : 'ContractError' → 'Text'``

  [*Available in version >= 1.dev*]

  Extract the error message from a ``'ContractError'``.


Debugging functions
~~~~~~~~~~~~~~~~~~~

* ``TRACE : ∀ (α : ⋆) . 'Text' → α → α``

  Returns the second argument as is. This function is intended to be
  used for debugging purposes, but note that we do not specify how
  ledger implementations make use of it.


Program serialization
^^^^^^^^^^^^^^^^^^^^^

Daml-LF programs are serialized using `Protocol Buffers
<https://developers.google.com/protocol-buffers/>`_.  The
machine-readable definition of the serialization for Daml-LF major
version 1 can be found in the `daml_lf_1.proto
<../archive/src/main/protobuf/com/daml/daml_lf_dev/daml_lf_1.proto>`_
file.

For the sake of brevity, we do no exhaustively describe how Daml-LF
programs are (un)serialized into protocol buffer. In the rest of this
section, we describe the particularities of the encoding and how
Daml-LF version impacts it.


Specificities of Daml-LF serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Required fields
...............

As a rule of the thumb, all non `oneof fields
<https://developers.google.com/protocol-buffers/docs/proto3#oneof>`_
are required in the serialization. Similarly among fields within the
same oneof definition at least one must be defined.  Exceptions are
exhaustively indicated in the `daml_lf_1.proto
<../archive/src/main/protobuf/com/daml/daml_lf_dev/daml_lf_1.proto>`_
file with comment::

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

Note that the Daml-LF deserialization process verifies the repeated
fields of those compressed structures are non-empty. For instance, the
previous message can be used only if it encodes at least one
application.

Message fields of compressed structure that should not be empty - such
as the ``args`` field of the ``App`` message - are annotated in the
`daml_lf_1.proto
<../archive/src/main/protobuf/com/daml/daml_lf_dev/daml_lf_1.proto>`_
file with the comments::

  // * must be non empty *


Maps
....

The program serialization format does not provide any direct way to
encode either `TextMap` or `GenMap`. Daml-LF programs can create such
objects only dynamically using the builtin functions prefixed by
`TEXTMAP_` or `'GENMAP_'`


Validation
~~~~~~~~~~

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
  <../archive/src/main/protobuf/com/daml/daml_lf_dev/daml_lf_1.proto>`_
  file where those requirements are exhaustively described as comments
  between asterisks (``*``).

* The second phase occurs after the deserialization, on the complete
  abstract syntax tree of the package. It is concerned with the
  `well-formedness <Well-formed packages_>`_ of the package.

An engine compliant with the present specification must accept loading a
package if and only if the latter of these two validation passes.



Serialization changes since version 1.6
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As explained in `Version history`_ section, Daml-LF programs are
accompanied by a number version. This enables the Daml-LF
deserialization process to interpret different versions of the
language in a backward compatibility way. During deserialization, any
encoding that does not follow the minor version provided is rejected.
Below we list, in chronological order, all the changes that have been
introduced to the serialization format since version 1.6


String Interning
................

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

Starting from Daml-LF 1.7, all ``string`` (or ``repeated string``)
fields with the suffix ``_str`` are forbidden. Alternative fields of
type ``int32`` (or ``repeated int32``) with the suffix
``_interned_str`` must be used instead.  Except
``PackageRef.package_id_interned_str`` which is [*Available in
versions >= 1.6*], all fields with suffix ``_interned_str`` are
[*Available in versions >= 1.7*].  The deserialization process will
reject any Daml-LF 1.7 (or later) that does not comply with this
restriction.

Name Interning
..............

[*Available in versions >= 1.7*]

To provide sharing of `names <Identifiers_>`_, the so-called *name
interning* mechanism allows the *names* within messages to be stored
in a global table and be referenced by their index.

``InternedDottedName`` is a non-empty list of valid interned
identifiers. Such message is interpreted as the name built from the
sequence the interned identifiers it contains.  The field
``Package.interned_dotted_names`` is a list of such messages. A
so-called `interned name` is a valid zero-based index of this list. An
`interned name` is interpreted as the name built form the `name` it
points to in ``Package.interned_dotted_names``.

Starting from Daml-LF 1.7, all ``DottedName`` (or ``repeated
string``) fields with the suffix ``_dname`` are forbidden. Alternative
fields of type ``int32`` with the suffix ``_interned_dname``
[*Available in versions >= 1.7*] must be used instead. The
deserialization process will reject any Daml-LF 1.7 (or later) that
that does not comply this restriction.

Nat kind and Nat types
......................

[*Available in versions >= 1.7*]

The deserialization process will reject any Daml-LF 1.6 (or earlier)
that uses ``nat`` field in ``Kind`` or ``Type`` messages.

Starting from Daml-LF 1.7 those messages are deserialized to ``nat``
kind and ``nat`` type respectively. The field ``nat`` of ``Type``
message must be a positive integer.

Note that despite there being no concrete way to build Nat types in a
Daml-LF 1.6 (or earlier) program, those are implicitly generated when
reading as Numeric type and Numeric builtin as described in the next
section.

Parametric scaled Decimals
..........................

[*Available in versions >= 1.7*]

Daml-LF 1.7 is the first version that supports parametric scaled
decimals. Prior versions have decimal number with a fixed scale of 10
called Decimal.  Backward compatibility with the current specification
is achieved as follows:

On the one hand, in case of Daml-LF 1.6 archive:

- The ``decimal`` field of the ``PrimLit`` message must match the
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
  deserialization process will reject any Daml-LF 1.6 (or earlier)
  that does not comply those restrictions.

On the other hand, starting from Daml-LF 1.7:

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
  deserialization process will reject any Daml-LF 1.7 (or later)
  that does not comply those restrictions.

Any type and type representation
................................

Daml-LF 1.7 is the first version that supports any type and
type representation.

The deserialization process will reject any Daml-LF 1.6 program using
this data structure.

Generic Equality/Order
......................

[*Available in versions >= 1.11*]

The deserialization process will reject any Daml-LF 1.8 (or earlier)
program using the following builtin functions ``EQUAL``, ``LESS_EQ``,
``LESS``, ``GREATER_EQ``, ``GREATER``

The deserialization process will reject any Daml-LF 1.11 (or latter)
program using the following builtin functions , ``EQUAL_INT64``,
``EQUAL_NUMERIC``, ``EQUAL_TEXT``, ``EQUAL_TIMESTAMP``,
``EQUAL_DATE``, ``EQUAL_PARTY``, ``EQUAL_BOOL``,
``EQUAL_CONTRACT_ID``, ``EQUAL_TYPE_REP`` ``LEQ_INT64``,
``LEQ_NUMERIC``, ``LEQ_TEXT``, ``LEQ_TIMESTAMP``, ``LEQ_DATE``,
``LEQ_PARTY``, ``LESS_INT64``, ``LESS_NUMERIC``, ``LESS_TEXT``,
``LESS_TIMESTAMP``, ``LESS_DATE``, ``LESS_PARTY``, ``GEQ_INT64``,
``GEQ_NUMERIC``, ``GEQ_TEXT``, ``GEQ_TIMESTAMP``, ``GEQ_DATE``,
``GEQ_PARTY``, ``GREATER_INT64``, ``GREATER_NUMERIC``,
``GREATER_TEXT``, ``GREATER_TIMESTAMP``, ``GREATER_DATE``,
``GREATER_PARTY``.

Generic Map
............

[*Available in versions >= 1.11*]

The deserialization process will reject any Daml-LF 1.8 (or earlier)
program using the builtin type ``GENMAP`` or the builtin functions
``GENMAP_EMPTY``, ``GENMAP_INSERT``, ``GENMAP_LOOKUP``,
``GENMAP_DELETE``, ``GENMAP_KEYS``, ``GENMAP_VALUES``,
``GENMAP_SIZE``.

exercise_by_key
...............

[*Available in versions >= 1.11*]

The deserialization process will reject any Daml-LF 1.8 (or earlier)
program using the field ``exercise_by_key`` in the ``Update`` message.

TO_TEXT_CONTRACT_ID
...................

[*Available in versions >= 1.11*]

The deserialization process will reject any Daml-LF 1.8 (or earlier)
program using the builtin function ``TO_TEXT_CONTRACT_ID``.

Choice observers
................

[*Available in versions >= 1.11*]

An optional `observer` expression may be attached to a flexible
choice. This allows the specification of additional parties to whom
the sub-transaction is disclosed.

The deserialization process will reject any Daml-LF 1.8 (or earlier)
program using the field ``observers`` in the ``TemplateChoice``
message. The missing ``observers`` field is interpreted as an
empty list of observers.

Exception
.........

[*Available in versions >= 1.1dev*]

The deserialization process will reject any Daml-LF 1.11 (or earlier)
program exception using
- the field ``throw``, ``to_any_exception``, or ``from_any_exception``
  in the ``Expr`` message,
- the field ``try`` in the ``Update message,
- any of the builtin functions ``MAKE_GENERAL_ERROR``,
  ``MAKE_ARITHMETIC_ERROR``, ``MAKE_CONTRACT_ERROR``,
  ``ANY_EXCEPTION_MESSAGE``, ``GENERAL_ERROR_MESSAGE``, or
  ``ARITHMETIC_ERROR_MESSAGE`.

BigDecimal
..........

Daml-LF 1.7 is the first version that supports BigDecimal.

The program serialization format does not provide any direct way to
encode `MathContext`. Daml-LF programs can create such
objects only dynamically using the `MathContext functions`_.

The deserialization process will reject any Daml-LF 1.11 (or earlier)
program exception using:

.. TODO https://github.com/digital-asset/daml/issues/8719





.. Local Variables:
.. eval: (flyspell-mode 1)
.. eval: (set-input-method "TeX")
.. End:
