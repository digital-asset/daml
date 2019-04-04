.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML LF Specification
======================

Brief note on naming: while designing DAML-LF we just called the language
"DAML Core" or "DAML 1.0 Core". However we decided to switch to "DAML-LF",
where the "LF" stands for "Ledger Fragment", to better represent the
role of the language in our stack.

If you find any stale reference to the DAML core terminology please ping
Francesco or Martin H.

..
  TODO (SM): consider the case of legal agreements properly. The current
  solution with ``toText`` is really bad. IMHO, we want monomoprhic
  ``intToText`` functions for conversion to text where necessary; and builtin
  functions for ``LegalText`` along the lines of::

    -- embed a serializable value as a legal text value
    toLegal      :: Serializable a => a -> LegalText

    -- combine two legal text values as separate words in the same paragraph
    wordSepLegal :: LegalText -> LegalText -> LegalText

    -- combine two legal text values as separate paragraphs
    parSepLegal  :: LegalText -> LegalText -> LegalText

  These combinators allow us to construct legal-text values where all embedded
  values are clearly marked. Note that we disallow to serialize legal text
  values to protect from code-injection problems. This implies that one always
  has to build the actual legal terms from non-LegalText expressions.

..
  TODO (SM): what about ``Scenario``? We should think through how we satisfy
  the requirement of being able to write integration tests in the ``Scenario``
  language and execute them via the Ledger API against a ledger. It's correct
  though that this does not belong in the DAML-LF Language.

  FM: We are adding scenarios to the first version of DAML-LF

..
  TODO: explain how we want to compress things like nested abstractions,
  applications, ``let``s and ``bind``s before we serialize them

How to view and edit this section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To view this section correctly, please install the `DejaVu Sans family
of fonts <https://dejavu-fonts.github.io/>`_, which is free (as in
freedom) and provide exceptional Unicode coverage. The sphinx style
sheets specify DejaVu Sans Mono as the font to use for code, and if
you want to view / edit this section you should use it for your
editor, too.

Moreover, if you want to edit this section comfortably, I highly
reccomend using emacs' TeX input mode. You can turn it on using ``M-x
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

Goals
^^^^^

The main constraint for DAML-LF is that we have to
support it in the years to come. This is relevant in two ways:

* Our software including ledger needs to support the old versions of
  DAML-LF for the forseeable future, which in turns means
  that we will have to support every DAML-LF feature we introduce
  for the foreseable future;
* it must be be easy for consumers of DAML-LF to transition
  between old versions to new versions of DAML-LF; which means
  that we must be careful to introduce breaking changes to DAML-LF
  versions even if we keep supporting the old versions.

For this reason, the plan is to start out as small as possible and
then grow it out gradually in the years to come.

Note that the DAML-LF descrided here is not meant to describe how
the compiler works internally: we are free to use a very flexible
ambient calulus in the compiler pipeline, with a final check that
converts it to the DAML-LF that we deploy.

The main constraint that guided the features that did make
it in this DAML-LF proposal is to be able to *comfortably*
interact with a DAML ledger from popular languages. The language that
drives the feature-picking is Java, since it's the most popular in our
target market and also one of the most limited.

That said, to make this constraint a bit more crisp:

**For any template in any DAML 1.0 ledger, there should be a
straight-forward way to generate a type-safe Java interface to create,
exercise, and retrieve template instances without additional
metadata.**

I'll call the above requirement "no fancy templates". With
"straight-forward" I mean that a DAML modeler should be able to
immediately know what the Java interface will look like when he is
programming some templates. For example, the generated Java types and
names should be predictable.

Moreover, we also want to easily store and display every piece
of data that goes in and out of the ledger. This requirement
is worthwhile because:

* We want to store the ledger data in a structured way in existing databases;
* We do not want the data to be coupled with the DAML-LF language,
  so that migrations between DAML-LF versions can be performed easily;
* We want the data to be easy to display and manipulate not only
  by our tooling, but also by applications written by third parties
  (think web apps like the DA Navigator).

Thus:

**Every type used by a template, either as template argument or
as choice input or output, must be first order.**

I'll call the above requirement "serializable templates".

This means that for templates we need to isolate a fragment of the
type system which does not have:

1. Higher-ranked types (no fancy templates);
2. Higher-kinded types (no fancy templates);
3. No anonymous records or variants (no fancy templates);
4. No functions (serializable templates).

The current decision is to not have higher-kinded types at all in
DAML-LF, and have 1, 3, and 4 to not appear in contract templates
and in general any definition for which we want to generate Java
bindings.

Note again how this restriction on DAML-LF would not be a restriction
on the compiler implementation, where we can experiment with more
advanced features, as long as the compiler can produce valid DAML-LF
if required.

My rough plan for the future, if we realize a more powerful language
is needed, is to come up with a clearer way to separate the "external"
API and the internal code. This separation will affect the type
system and the module system and will have to be planned carefully,
but we can definitely investigate it if we find ourselves needing
a more complex type system.

Note that if some features can be implemented fully in the surface
language we should: for example, we will probably need a facility
to easily generate templates programmatically, while keeping the
DAML-LF templates first-order. Such a feature would be implemented
in the surface language but would not make it to DAML-LF.

Abstract syntax
^^^^^^^^^^^^^^^

Notation
~~~~~~~~

Terminals are specified as such::

  description:
    symbols ∈ regexp                               -- Unique identifier

Where:

* the description describes the terminal being defined;
* the symbols define how we will refer of the terminal in type rules / operational semantics / ...;
* the regexp is a regular expression describing the members
  of the terminal. We escape ambiguous characters such
  as ``.`` with a backslash, e.g. ``\.``.
* the unique identifier is a string that uniquely identifies
  the non-terminal.

Sometimes the symbol might be the same as the unique identifier,
in the instances where a short symbol is not needed because
we do not mention it very often.

Non-terminals are specified as such::

  Description:
    symbols
      ::= non-terminal alternative                 -- Unique identifier for alternative: description for alternative
       |   ⋮

Where description and symbols have the same meaning
as in the terminal rules, and:

* each non-terminal alternative is a piece of syntax
  describing the alternative;
* each alternative has a unique identifier (think
  of them as constructors of a datatype).

Note that the syntax defined by the non-terminals is
not intended to be parseable or non-ambiguous, rather
it is intended to be read and interpreted by humans.
However, for the sake of clarity, we enclose strings
that are part of the syntax with single quotes. We
do not enclose symbols such as ``.`` or ``→`` in quotes
for the sake of brevity and readability.


Specification
~~~~~~~~~~~~~

.. warning:: The primitive types listed in this spec are not
	     the ones currently implemented. This is since
	     we need to be in sync with whatever primitive
	     types are implemented in surface DAML, and we
	     have not finalized the design of primitive types
	     in surface DAML yet.
	     Please check out ``daml-foundations/daml-lf-archive/da/daml_lf_1.proto``
	     for a view of the primitive types currently in use.

We first define a bunch of names that we will use throughout
our abstract syntax::

  Term variables
    x, y, z ∈ [a-z_][a-zA-Z0-9_]*                    -- VarExp

  Type variables
    a, b, c ∈ [a-z_][a-zA-Z0-9_]*                    -- VarTy

  Type constructor
    T ∈ ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*  -- TyConName

  Record field
    f ∈ [a-z][a-zA-Z0-9_]*                           -- RecordField

  Variant data constructors
    V ∈ [A-Z][a-zA-Z0-9_]*                           -- VariantCon

  Template choice name
    Ch ∈ [A-Z][a-zA-Z0-9_]*                          -- ChoiceName

  Imported package identifier
    pid ∈ [a-zA-Z0-9]+                               -- PkgId: generated using content addressing

  Module names
    ModName ∈ ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)* -- ModName

  Contract id literal
    cid ∈ #[a-zA-Z0-9]+                              -- ContractIdLit

..
  TODO (FM): explain that package ids will probably be a digest
  of a hash in base16 but that in this spec we're concerned with
  how it'll look in its readable form.

  Check out
  <https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki>
  for inspiration

Note that type constructors and friendly module names are uppercase
names intercalated by dots. For module names this works very
much like qualified modules in Haskell. Moreover, type constructor
names within modules can be qualified because templates and
template choices can implicitly define types, and we want to
group them nicely in the Java interface (more on this in the Java
bindings section).

Note that module names (``ModName``) are human-readable and come
from the surface language. On the other hand package ids (``PkgId``)
are generated hashing the content of the package.

A DAML ledger contains a number of packages each with its unique
``PkgId``, each package contains a number of modules each with a
unique ``ModName``, and each module contains a number of definitions,
all with unique names.

Then we can define our types and expressions::

  Package references
    Pkg
      ::= 'Self'                                       -- PkgSelf: Reference to current package
       |  'Import' pid                                 -- PkgImport: Reference to an imported package

  Module references
    Mod
      ::= 'Package' Pkg ModName                        -- ModPackage: module from a package

  Primitive types
    PrimTy
      ::= 'Int64'                                      -- PrimTyInt64: 64-bit integer
       |  'Decimal'                                    -- PrimTyDecimal: decimal, precision 38, scale 10
       |  'Text'                                       -- PrimTyText: UTF8 string
       |  'Date'                                       -- PrimTyDate: year, month, date triple
       |  'Time'                                       -- PrimTyTime: UTC timestamp
       |  'Party'                                      -- PrimTyParty

  Types (mnemonic: tau for type)
    τ, σ
      ::= Mod.T τ₁ … τₙ                                -- TyCon: Saturated type con
       |  a                                            -- TyVar: Type variable
       |  τ → σ                                        -- TyFun: Function
       |  ∀ a . τ                                      -- TyForall: Universal quantification
       |  PrimTy                                       -- TyPrim: Primitive type
       |  'ContractId' Mod.T                           -- TyCid: Contract id
       |  'Update' τ                                   -- TyUpdate
       |  'List' τ                                     -- TyList
       |  ⦃ f₁: τ₁, …, fₘ: τₘ ⦄                        -- TyTuple
       |  'Unit'                                       -- TyUnit
       |  'Bool'                                       -- TyBool

  Primitive literals
    PrimLit
      ::= Int64Lit                                     -- PrimLitInt64
       |  DecimalLit                                   -- PrimLitDecimal
       |  PartyLit                                     -- PrimLitParty
       |  TextLit                                      -- PrimLitText
       |  DateLit                                      -- PrimLitDate
       |  TimeLit                                      -- PrimLitTime

  Expressions
    e ::= x                                            -- ExpVar: Local variable
       |  Mod.x                                        -- ExpVal: Defined value
       |  Mod.T τ₁ … τₙ { f₁ = e₁, …, fₘ = eₘ }        -- ExpRecCon: Record construction
       |  Mod.T.f τ₁ … τₙ                              -- ExpRecProj: Record projection
       |  Mod.T.V τ₁ … τₙ e                            -- ExpVariantCon: Variant construction
       |  ⦃ f₁ = e₁, …, fₘ = eₘ ⦄                      -- ExpTupleCon: Tuple construction
       |  e.f                                          -- ExpTupleProj: Tuple projection
       |  e₁ e₂                                        -- ExpApp: Application
       |  e τ                                          -- ExpTyApp: Type application
       |  λ x : τ → e                                  -- ExpAbs: Abstraction
       |  Λ a . e                                      -- ExpTyAbs: Type abstraction
       |  'case' e₁ of { alt₁, …, altₙ }               -- ExpCase: Pattern matching
       |  'let' x : τ = e₁; e₂                         -- ExpLet: Let binding
       |  PrimLit                                      -- ExpPrimLit: Primitive literal
       |  PrimOp                                       -- ExpPrimOp: Primitive operation
       |  u                                            -- ExpUpdate: Update expression
       |  'error'                                      -- ExpError: Failure
       |  'Nil' τ                                      -- ExpListNil: Empty list
       |  'Cons' τ e₁ e₂                               -- ExpListCons: Cons list
       |  'foldl'                                      -- ExpListFoldl
       |  cid Mod.T                                    -- ExpContractIdLit
       |  'eq' τ                                       -- ExpEq
       |  ⊤                                            -- ExpUnit
       |  'True'                                       -- ExpTrue
       |  'False'                                      -- ExpFalse

  Updates
    u ::= 'pure' e                                     -- UpdPure
       |  'bind' x : τ ← e₁; e₂                        -- UpdBind
       |  'create' Mod.T e                             -- UpdCreate: tpl argument
       |  'exercise' Mod.T.Ch e₁ e₂ e₃                 -- UpdExercise: coid, party, choice argument
       |  'fetch' Mod.T e₁ e₂                          -- UpdFetch: coid, party
       |  'getTime'                                    -- UpdGetTime

  Pattern matching alternative
    alt
      ::= Mod.T.V x → e                                -- CaseVariant: Variant match
       |  'Nil' → e                                    -- CaseNil
       |  'Cons' x₁ x₂ → e                             -- CaseCons
       |  'True' → e                                   -- CaseTrue
       |  'False' → e                                  -- CaseFalse
       |  x → e                                        -- CaseDefault

  Primitive operations
    PrimOp
      ::= Int64Plus
       |  Int64Minus
       |  Int64Times
       |  Int64DivMod
       |  …


..
  TODO: The current plan is to adopt C#'s decimal type:
  https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/language-specification/types#the-decimal-type , explore this possibility further
  when we come to operational semantics

  * Comment (SSM): Strictly speaking C#'s decimal type is only 102 bits. Also we
    need to think carefully about equality of decimal numbers. There is no
    implicit normalization in C# and hence there are 28 different
    representations of the number 1 as a decimal.

  * Comment (MH): Francesco and I have decided to go fixed precision and scale
    since floating scales are not supported well by DB systems. See comments on
    ``Decimal`` type below.

..
  TODO (SM): decide on 64-bit integer vs. 128-bit integer. There are anyways
  conversions from ``Dec128`` to ``Int64`` that lead to an overflow, so there
  does not seem to be a very strong requirement to have ``Int`` to cover the
  whole range of ``Dec128`` mantissas.

  * FM: I agree, me and Martin already pretty much agreed that 64bit is
    enough for integers.

..
  TODO (SM): built-in equality and ordering for serializable types?

  * FM: We can have that for debugging, but I would not include it in
    the calculus. I'd wait for type classes to implement this
    properly.

  * FM: After talking to Simon I see the use case for this, specifically
    to compare equality of big records. Thus we resolved to add a rule
    to compare for equality restricted on serializable types. We decided
    to postpone ordering since it is hard to order contract ids.

  * MH: Ordering (Unicode) strings can be complicated wrt. to user expectations.

..
  TODO (MH): Consider making the default branch of ``case``-expressions
  optional. This would avoid a lot of noise when translating exhaustive
  ``case``-expressions from the surface language. If we decide to do this, we
  need to include an exhaustiveness checker for ``case``-expressions to the
  package validation.

Note that:

* Definition names are all relative to the module that contains
  them;
* Record fields are relative to the record it defines them;
* Variant constructors are relative to the variant it defines them;
* Choices are relative to the template that defines them.

Expressions contain references to definitions in the current
package and in imported packages. We need to have access to such
definitions::

  Template choice kind
    ChKind
      ::= 'consuming'                                   -- ChKindConsuming
       |  'non-consuming'                               -- ChKindNonConsuming

  Template choice definition
    ChDef ::= 'choice' ChKind Ch (x : τ) : σ 'by' eₚ ↦ e -- ChDef

  Definitions
    Def
      ::=
       |  'data' T a₁ … aₙ ↦ { f₁ : τ₁, …, fₘ : τₘ }    -- DefRecord
       |  'data' T a₁ … aₙ ↦ V₁ : τ₁ | … | Vₘ : τₘ      -- DefVariant
       |  'val' x : τ ↦ e                               -- DefValue
       |  'tpl' (x : T) ↦                               -- DefTemplate
            { 'precondition' e₁
            , 'signatories' eₛ₁ … eₛₙ
            , 'observers' e₂
            , 'agreement' e₃
            , 'choices' { ChDef₁, …, ChDefₘ }
            }

  Module (mnemonic: delta for definitions)
    Δ ::= ε                                             -- DefCtxEmpty
       |  Def · Δ                                       -- DefCtxCons

  Package
    Θ ∈ ModName ↦ Δ                                     -- Package

  Package collection
    Ξ ∈ pid ↦ Package                                   -- Packages


.. NOTE: The ``'signatories'`` of a ``'tpl'`` takes ``n`` individual
   expressions of type ``'Party'`` whereas the ``'observers'`` clause takes
   one expression of type ``'List' 'Party'``. This mismatch is for historic
   reasons and only temporary. We will change ``'signatories'`` to take
   exatly one expression of type ``'List' 'Party'`` as well (most likely in
   Q3 2018).

..
  TODO (SM): the above notation reads as if we could have one mapping
  per fully saturatd application of ``T``; whereas I'd expect that we
  have one mapping per name.

  * FM: Yes, we have to add a condition for unique names.

..
  TODO: propose ``val`` as a keyword in the surface language
  instead of ``def`` since we only bind values

..
  TODO: explain that ``'tpl'`` corresponds to the ``'template'`` in
  the surface language but we keep it shorter here for comfort

In all the type checking rules we will carry around a local
package ``Θ`` and the packages available for import ``Ξ``. Given a module
reference ``Mod``, we'll retrieve the corresponding definitions using
``〚Θ Ξ〛Mod``:

* if ``Mod`` is ``'Package' 'Self' ModName``, ``ModName`` is looked up
  in ``Θ``;
* if ``Mod`` is ``'Package' 'Import' pid ModName``, ``ModName`` is
  looked up in package ``Ξ(pid)``;


Feature flags
.............

..
  TODO(drsk): include FeatureFlags into definition of module

Modules are annotated with a set of feature flags. The following feature flags are available:

 +-------------------------------------------+----------------------------------------------------------+
 | Flag                                      | Semantic meaning                                         |
 +===========================================+==========================================================+
 | ForbidPartyLiterals                       | Party literals are not allowed in a DAML-LF module.      |
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

Type system
^^^^^^^^^^^

As mentioned above, every rule here operates under a local package ``Θ`` and the
packages available for import ``Ξ``.

::

  Type context:
    Γ ::= ε                                 -- CtxEmpty
       |  x : τ · Γ                         -- CtxExpVar
       |  a : ⋆ · Γ                         -- CtxTyVar


                           ┌──────┐
  Well formed contexts     │ ⊢  Γ │
                           └──────┘

    ————————————————————————————————————————————————— CtxEmpty
      ⊢  ε

      ⊢  Γ
    ————————————————————————————————————————————————— CtxExpVar
      ⊢  a : ⋆ · Γ

      ⊢  Γ      Γ  ⊢  τ  :  ⋆
    ————————————————————————————————————————————————– CtxTyVar
      ⊢  x : τ · Γ

                       ┌───────────────┐
  Well formed types    │ Γ  ⊢  τ  :  ⋆ │
                       └───────────────┘

      Γ  ⊢  τ₁  :  ⋆      …      Γ  ⊢  τₙ  :  ⋆
      'data' T a₁ … aₙ ↦ …  ∈  〚Θ Ξ〛Mod
    ————————————————————————————————————————————— TyCon
      Γ  ⊢  Mod.T τ₁ … τₙ  :  ⋆

      a : ⋆  ∈  Γ
    ————————————————————————————————————————————— TyVar
      Γ  ⊢  a  :  ⋆

      Γ  ⊢  τ  :  ⋆      Γ  ⊢  σ  :  ⋆
    ————————————————————————————————————————————— TyFun
      Γ  ⊢  τ → σ  :  ⋆

      a : ⋆ · Γ  ⊢  τ
    ————————————————————————————————————————————— TyForall
      Γ  ⊢  ∀ a . τ  :  ⋆


    ————————————————————————————————————————————— TyPrim
      Γ  ⊢  PrimTy  :  ⋆

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
    ————————————————————————————————————————————— TyCid
      Γ  ⊢  'ContractId' Mod.T  :  ⋆

      Γ  ⊢  τ  :  ⋆
    ————————————————————————————————————————————— TyUpdate
      Γ  ⊢  'Update' τ  :  ⋆

      Γ  ⊢  τ  :  ⋆
    ————————————————————————————————————————————— TyList
      Γ  ⊢  'List' τ  :  ⋆

      Γ  ⊢  τ₁  :  ⋆
       ⋮
      Γ  ⊢  τₙ  :  ⋆
    ————————————————————————————————————————————— TyTuple
      Γ  ⊢  ⦃ f₁: τ₁, …, fₙ: τₙ ⦄  :  *


    ————————————————————————————————————————————— TyUnit
      Γ  ⊢  'Unit'  :  ⋆

    ————————————————————————————————————————————— TyBool
      Γ  ⊢  'Bool'  :  ⋆

                         ┌───────────────┐
  Well typed terms       │ Γ  ⊢  e  :  τ │
                         └───────────────┘

      x : τ  ∈  Γ
    ——————————————————————————————————————————————————————————————— ExpVar
      Γ  ⊢  x  :  τ

      'val' x : τ ↦ …  ∈  〚Θ Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpVal
      Γ  ⊢  Mod.x  :  τ

      T a₁ … aₙ ↦ { …, f : σ, … }  ∈  〚Θ Ξ〛Mod
      Γ  ⊢  τ₁  :  ⋆      ⋯      Γ  ⊢  τₙ  :  ⋆
    ——————————————————————————————————————————————————————————————— ExpRecProj
      Γ
        ⊢
      Mod.T.f τ₁ … τₙ
        :
      Mod.T τ₁ … τₙ → σ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]

      'data' T a₁ … aₙ ↦ { f₁ : τ₁, …, fₘ : τₘ }  ∈  〚Θ Ξ〛Mod
      Γ  ⊢  e₁  :  τ₁[a₁ ↦ σ₁, …, aₙ : σₙ]
       ⋮
      Γ  ⊢  eₘ  :  τₘ[a₁ ↦ σ₁, …, aₙ : σₙ]
    ——————————————————————————————————————————————————————————————— ExpRecCon
      Γ  ⊢  Mod.T σ₁ … σₙ { f₁ = e₁, …, fₘ = eₘ }  :  Mod.T σ₁ … σₙ

      'data' T a₁ … aₙ ↦ … | V : σ | …  ∈  〚Θ Ξ〛Mod
      Γ  ⊢  e  :  σ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
    ——————————————————————————————————————————————————————————————— ExpVarCon
      Γ  ⊢  Mod.T.V τ₁ … τₙ e  :  Mod.T τ₁ … τₙ

      Γ  ⊢  e₁  :  τ₁      ⋯      Γ  ⊢  eₘ  :  τₘ
    ——————————————————————————————————————————————————————————————— ExpTupleCon
      Γ  ⊢  ⦃ f₁ = e₁, …, fₘ = eₘ ⦄  :  ⦃ f₁: τ₁, …, fₘ: τₘ ⦄

      Γ  ⊢  e  :  ⦃ …, f: τ, … ⦄
    ——————————————————————————————————————————————————————————————— ExpTupleProj
      Γ  ⊢  e.f  :  τ

      Γ  ⊢  e₁  :  τ₁ → τ₂      Γ  ⊢  e₂  :  τ₁
    ——————————————————————————————————————————————————————————————— ExpApp
      Γ  ⊢  e₁ e₂  :  τ₂

      Γ  ⊢  e  :  ∀ a . σ      Γ  ⊢  τ  :  *
    ——————————————————————————————————————————————————————————————— ExpTyApp
      Γ  ⊢  e τ  :  σ[a ↦ τ]

      x : τ · Γ  ⊢  e  :  σ
    ——————————————————————————————————————————————————————————————— ExpAbs
      Γ  ⊢  λ x : τ → e  :  τ → σ

      a : ⋆ · Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— ExpTyAbs
      Γ  ⊢  Λ a . e  :  ∀ a . τ


    ——————————————————————————————————————————————————————————————— ExpError
      Γ  ⊢  'error'  :  ∀ a . 'Text' → a

      Γ  ⊢  e₁  :  τ
      x : τ · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— CaseEmpty
      Γ  ⊢  'case' e₁ 'of' { } (x → e₂)  :  σ

      Γ  ⊢  e₁  :  Mod.T τ₁ … τₙ
      'data' T a₁ … aₙ ↦ … | V : τ | …  ∈  〚Θ Ξ〛Mod
      x : τ[a₁ ↦ τ₁, …, aₙ ↦ τₙ] · Γ  ⊢  e₂  :  σ
      Γ  ⊢  'case' e₁ 'of' { alt₁, …, altₙ } (y → e₃) :  σ
    ——————————————————————————————————————————————————————————————— CaseVariant
      Γ
        ⊢
      'case' e₁ 'of' { Mod.T.V x → e₂, alt₁, …, altₙ } (y → e₃)
        :
      σ

      Γ  ⊢  e₁  :  'List' τ
      Γ  ⊢  e₂  :  σ
      Γ  ⊢  'case' e₁ 'of' { alt₁, …, altₙ } (y → e₃) :  σ
    ——————————————————————————————————————————————————————————————— CaseNil
      Γ
        ⊢
      'case' e₁ 'of' { 'Nil' → e₂, alt₁, …, altₙ } (y → e₃)
        :
      σ

      Γ  ⊢  e₁  :  'List' τ
      x₁ : τ · x₂ : 'List' τ · Γ  ⊢  e₂  :  σ
      Γ  ⊢  'case' e₁ 'of' { alt₁, …, altₙ } (y → e₃) :  σ
    ——————————————————————————————————————————————————————————————— CaseCons
      Γ
        ⊢
      'case' e₁ 'of' { 'Cons' x₁ x₂ → e₂, alt₁, …, altₙ } (y → e₃)
        :
      σ

      Γ  ⊢  e₁  :  τ      x : τ · Γ  ⊢  e₂  :  σ
    ——————————————————————————————————————————————————————————————— ExpLet
      Γ  ⊢  'let' x : τ = e₁; e₂  :  σ

      Γ  ⊢  e  :  τ
    ——————————————————————————————————————————————————————————————— UpdPure
      Γ  ⊢  'pure' e  :  'Update' τ

      Γ  ⊢  e₁  :  'Update' τ      x : τ · Γ  ⊢  e₂  :  'Update' σ
    ——————————————————————————————————————————————————————————————— UpdBind
      Γ  ⊢  'bind' x : τ ← e₁; e₂  :  'Update' σ

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
      Γ  ⊢  e  : T
    ——————————————————————————————————————————————————————————————— UpdCreate
      Γ  ⊢  'create' Mod.T e  : 'Update' ('ContractId' Mod.T)


      'tpl' (x : T)
          ↦ { 'choices' { …, 'choice' ChKind Ch (x : τ) : σ 'by' … ↦ …, … }, … }
        ∈  〚Θ Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod.T
      Γ  ⊢  e₂  :  'List' 'Party'
      Γ  ⊢  e₃  :  τ
    ——————————————————————————————————————————————————————————————— UpdExercise
      Γ  ⊢  'exercise' Mod.T.Ch e₁ e₂ e₃  : 'Update' σ

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
      Γ  ⊢  e₁  :  'ContractId' Mod.T
      Γ  ⊢  e₂  :  'Party'
    ——————————————————————————————————————————————————————————————— UpdFetch
      Γ  ⊢  'fetch' Mod.T e₁ e₂  : 'Update' Mod.T


    ——————————————————————————————————————————————————————————————— ExpListNil
      Γ  ⊢  'Nil' τ  :  'List' τ

      Γ  ⊢  e₁  :  'List' τ      Γ  ⊢  e₂  :  'List' τ
    ——————————————————————————————————————————————————————————————— ExpListNil
      Γ  ⊢  'Cons' τ e₁ e₂  :  'List' τ


    ——————————————————————————————————————————————————————————————— ExpListFoldl
      Γ  ⊢  'foldl'  :  ∀ a . ∀ b . (b → a → b) → b → 'List' a → b

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
    ——————————————————————————————————————————————————————————————— ExpListContractIdLit
      Γ  ⊢ cid Mod.T   :  'ContractId' Mod.T

      ⊢ₑ  τ
    ——————————————————————————————————————————————————————————————— ExpEq
      Γ  ⊢  'eq' τ  :  τ → τ → 'Bool'


    ——————————————————————————————————————————————————————————————— ExpTrue
      Γ  ⊢  'True'  :  'Bool'

    ——————————————————————————————————————————————————————————————— ExpFalse
      Γ  ⊢  'False'  :  'Bool'

    ——————————————————————————————————————————————————————————————— ExpUnit
      Γ  ⊢  ⊤  :  'Unit'


We will use ``⊢ τ : ⋆`` and ``⊢ e : τ`` when the contexts are empty.

The rule for ``ExpEq`` refers to the ``⊢ₑ`` judgement, which identifies
*equatable* types and is defined as follows::

                         ┌────────┐
  Equatable types        │ ⊢ₑ  τ  │
                         └────────┘

      'data' T a₁ … aₙ ↦ { f₁: σ₁, …, fₘ: σₘ }  ∈  〚Θ Ξ〛Mod
      ⊢ₑ  σ₁[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
       ⋮
      ⊢ₑ  σₘ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
    ———————————————————————————————————————————————————————————————— ETyRecCon
      ⊢ₑ  Mod.T τ₁ … τₙ

      'data' T a₁ … aₙ ↦ V₁: σ₁ | … | Vₘ: σₘ  ∈  〚Θ Ξ〛Mod
      ⊢ₑ  σ₁[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
       ⋮
      ⊢ₑ  σₘ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
    ———————————————————————————————————————————————————————————————— ETyVariantCon
      ⊢ₑ  Mod.T τ₁ … τₙ

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
    ———————————————————————————————————————————————————————————————— ETyCid
      ⊢ₑ  'ContractId' Mod.T


    ———————————————————————————————————————————————————————————————— ETyPrim
      ⊢ₑ  PrimTy

      ⊢ₑ  τ
    ———————————————————————————————————————————————————————————————— ETyList
      ⊢ₑ  'List' τ

      ⊢ₑ  τ₁
       ⋮
      ⊢ₑ  τₙ
    ———————————————————————————————————————————————————————————————— ETyTuple
      ⊢ₑ  ⦃ f₁: τ₁, …, fₙ: τₙ ⦄

    ———————————————————————————————————————————————————————————————— ETyUnit
      ⊢ₑ  'Unit'

    ———————————————————————————————————————————————————————————————— ETyBool
      ⊢ₑ  'Bool'


Note how all equatable types are closed.

We need to define validity for definitions ``Δ`` and modules ``Θ``. To
do so, we first define a judgement that identifies *serializable*
types::

                         ┌────────┐
  Serializable types     │ ⊢ₛ  τ  │
                         └────────┘

      'data' T a₁ … aₙ ↦ { f₁: σ₁, …, fₘ: σₘ }  ∈  〚Θ Ξ〛Mod
      ⊢ₛ  σ₁[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
       ⋮
      ⊢ₛ  σₘ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
      ⊢ₛ  τ₁
       ⋮
      ⊢ₛ  τₙ
    ———————————————————————————————————————————————————————————————— STyRecCon
      ⊢ₛ  Mod.T τ₁ … τₙ

      'data' T a₁ … aₙ ↦ V₁: σ₁ | … | Vₘ: σₘ  ∈  〚Θ Ξ〛Mod
      ⊢ₛ  σ₁[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
       ⋮
      ⊢ₛ  σₘ[a₁ ↦ τ₁, …, aₙ ↦ τₙ]
      ⊢ₛ  τ₁
       ⋮
      ⊢ₛ  τₙ
    ———————————————————————————————————————————————————————————————— STyVariantCon
      ⊢ₛ  Mod.T τ₁ … τₙ

      'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
    ———————————————————————————————————————————————————————————————— STyCid
      ⊢ₛ  'ContractId' Mod.T


    ———————————————————————————————————————————————————————————————— STyPrim
      ⊢ₛ  PrimTy

      ⊢ₛ  τ
    ———————————————————————————————————————————————————————————————— STyList
      ⊢ₛ  'List' τ

    ———————————————————————————————————————————————————————————————— STyUnit
      ⊢ₛ  'Unit'

    ———————————————————————————————————————————————————————————————— STyBool
      ⊢ₛ  'Bool'


Note how all serializable types are also closed. We'll use ``⊢ₛ τ : ⋆`` to
identify types that are well-formed and serializable.

There are two important differences between equatable and serializable types:

1. Tuples are equatable but not serializable.
2. For a data type to be serializable, *all* type parameters must be
   instantiated with serialazable types, even phantom parameters. Equatable
   types are not restricted that way.

The reason for this discrepancy is that we use the notion of serializable type
also to identify which types we can generate Java APIs for. We can neither
generate Java interfaces for tuples nor translate instances of paramaterized
types to Java types if not all type arguments can be translated themselves.


Next we define the DAML expressions which do not need to be evaluated
further -- "values". This notion is needed when we define what
well-formed modules are and when we define the operational semantics
for DAML.

::

                           ┌───────┐
  Values                   │ ⊢ᵥ  e │
                           └───────┘

     ⊢ᵥ  e₁      …      ⊢ᵥ  eₙ
   ——————————————————————————————————————————————————— ValExpRecCon
     ⊢ᵥ  Mod.T τ₁ … τₙ { f₁ = e₁, …, fₙ = eₙ }


   ——————————————————————————————————————————————————— ValExpRecProj
     ⊢ᵥ  Mod.T.f τ₁ … τₙ

     ⊢ᵥ  e
   ——————————————————————————————————————————————————— ValExpVariantCon
     ⊢ᵥ  Mod.T.V τ₁ … τₙ e


   ——————————————————————————————————————————————————— ValExpAbs
     ⊢ᵥ  λ x : τ → e


   ——————————————————————————————————————————————————— ValExpTyAbs
     ⊢ᵥ  Λ a . e


   ——————————————————————————————————————————————————— ValExpPrimLit
     ⊢ᵥ  PrimLit


   ——————————————————————————————————————————————————— ValExpUpdate
     ⊢ᵥ  u

   ——————————————————————————————————————————————————— ValExpListNil
     ⊢ᵥ  'Nil' τ

     ⊢ᵥ  e₁      ⊢ᵥ  e₂
   ——————————————————————————————————————————————————— ValExpListCons
     ⊢ᵥ  'Cons' τ e₁ e₂

     ⊢ᵥ  e₁      ⋯      ⊢ᵥ  eₘ
   ——————————————————————————————————————————————————— ValExpTupleCon
     ⊢ᵥ  ⦃ f₁ = e₁, …, fₘ = eₘ ⦄


   ——————————————————————————————————————————————————— ValExpError0
     ⊢ᵥ  'error'


   ——————————————————————————————————————————————————— ValExpError1
     ⊢ᵥ  'error' τ


   ——————————————————————————————————————————————————— ValExpFoldl0
     ⊢ᵥ  'foldl'


   ——————————————————————————————————————————————————— ValExpFoldl1
     ⊢ᵥ  'foldl' τ


   ——————————————————————————————————————————————————— ValExpFoldl2
     ⊢ᵥ  'foldl' τ σ

     ⊢ᵥ  e1
   ——————————————————————————————————————————————————— ValExpFoldl3
     ⊢ᵥ  'foldl' e₁

     ⊢ᵥ  e₁      ⊢ᵥ  e₂
   ——————————————————————————————————————————————————— ValExpFoldl4
     ⊢ᵥ  'foldl' τ σ e₁ e₂


   ——————————————————————————————————————————————————— ValExpEq0
     ⊢ᵥ  'eq' τ

     ⊢ᵥ  e₁
   ——————————————————————————————————————————————————— ValExpEq0
     ⊢ᵥ  'eq' τ e₁


   ——————————————————————————————————————————————————— ValExpUnit
     ⊢ᵥ  ⊤

   ——————————————————————————————————————————————————— ValExpTrue
     ⊢ᵥ  'True'

   ——————————————————————————————————————————————————— ValExpFalse
     ⊢ᵥ  'False'


We will use the symbol ``v`` to represent an expression which
is a value.

Then we specify valid definitions. Note that these rules, too, work
under a local package ``Θ`` and imported packages ``Ξ``. Moreover
they also have the current module name, ``ModName``, in scope
(needed for the ``DefTemplate`` rule)::

                          ┌────────┐
  Well-formed definitions │ ⊢  Def │
                          └────────┘

    aₙ : ⋆ · ⋯ · a₁ : ⋆  ⊢  τ₁  :  ⋆
     ⋮
    aₙ : ⋆ · ⋯ · a₁ : ⋆  ⊢  τₘ  :  ⋆
  ——————————————————————————————————————————————————————————————— DefRec
    ⊢  'data' T a₁ … aₙ ↦ { f₁: τ₁, …, fₘ: τₘ }

    aₙ : ⋆ · ⋯ · a₁ : ⋆  ⊢  τ₁  :  ⋆
     ⋮
    aₙ : ⋆ · ⋯ · a₁ : ⋆  ⊢  τₘ  :  ⋆
  ——————————————————————————————————————————————————————————————— DefVariant
    ⊢  'data' T a₁ … aₙ ↦ f₁: τ₁ | … | fₘ: τₘ

    ⊢  v  :  τ
  ——————————————————————————————————————————————————————————————— DefValue
    ⊢  'val' x : τ ↦ v

    'data' T ↦ { f₁ : τ₁, …, fₙ : tₙ }  ∈  Θ(ModName)
    ⊢ₛ  T  :  ⋆
    x : T  ⊢  e₁  :  'Bool'
    x : T  ⊢  e₂  :  'List' 'Party'
    x : T  ⊢  e₃  :  'Text'
    x : T  ⊢  eₛ₁  :  'Party'    …    x : T  ⊢  eₛₙ  :  'Party'
    x : T  ⊢  ChDef₁      …      x : T  ⊢  ChDefₘ
  ——————————————————————————————————————————————————————————————— DefTemplate
    ⊢  'tpl' (x : T) ↦
         { 'precondition' e₁
         , 'signatories' eₛ₁ … eₛₙ
         , 'observers' e₂
         , 'agreement' e₃
         , 'choices' { ChDef₁, …, ChDefₘ }
         }

                          ┌───────────┐
  Well-formed choices     │ Γ ⊢ ChDef │
                          └───────────┘
    x : τ · Γ  ⊢  e  :  'Update' σ
    ⊢ₛ  τ
    ⊢ₛ  σ
    Γ  ⊢  eₚ  :  'List' 'Party'
  ——————————————————————————————————————————————————————————————— ChDef
    Γ  ⊢  'choice' ChKind Ch (x : τ) : σ 'by' eₚ ↦ e

FIXME (RJR): All definitions are checked in the empty context.
How do we model definitions that depend on other ones?

Note that we enforce that all expressions stored in value definitions
to be values. This requirement is to give predictable operational
semantics to DAML, as we will elaborate in the "operational semantics"
section.

Also note that every template needs to refer to a *record type* --
we impose this restriction to guarantee that we can generate column
names for SQL. Moreover, the type it refers to needs to be defined
in the same module, see next section on why that is the case.

Template coherence
^^^^^^^^^^^^^^^^^^

Each template is named after a record ``T`` with no type arguments
(see ``DefTemplate``). To avoid ambiguities, we place conditions for
each template argument type, which we term *template coherence* since
it's a requirement reminiscent of the coherence requirement of Haskell
type classes.

Specifically, a template definition is *coherent* if:

* Its argument data type is defined in the same module that the
  template is defined in;
* Its argument data type is not an argument to any other template.

This restriction is used to generate ergonomic APIs in Java and
similar languages, as we will see. Since it is quite ad-hoc we might
seek to replace it with a more first class type class notion in the
future.

Well-formed packages
^^^^^^^^^^^^^^^^^^^^

Then, a collection of packages ``Ξ`` is well formed is:

* Each definition context for each module is well-formed;
* There are no cycles in the package references nor in the module references;
* There are no cycles involving only value definitions (e.g.  there is
  no recursion for expressions, however templates can be recursive);
* There are no cycles involving type definitions (e.g. types are
  non-recursive, however a data type definition like
  ``data T = {cid : ContractId T; ...}`` is fine as the latter ``T`` refers to
  the template ``T`` rather than the data type ``T``);
* The template coherence condition holds for every template defined in
  any module in ``Ξ``.

..
  TODO: Decide on whether allowing module cycles is a good idea

..
  TODO: consider adding a condition that forces type abstractions
  only on let and values. this would make it easy to fully embed
  DAML in languages without higher-ranked types.

Operational semantics
^^^^^^^^^^^^^^^^^^^^^

Similarly to the type system, every rule for expression evaluation and update
interpretation operates under a local package ``Θ`` and the packages available
for import ``Ξ``.

Expression evaluation
~~~~~~~~~~~~~~~~~~~~~

.. TODO: Polish this by adding more prose and fixing typos

**Note: work in progress, this is a rough draft.**

In this section we give call-by-value semantics to DAML-LF expressions.

Evaluation is specified in small-step style, indicating the small step
symbol with the wave arrow ``↝``. An idealized interpreter would
repeatedly apply the small step rule until a value is reached.

Evaluation always happens on closed, well-typed terms.

Given the presence of ``ExpError``, the result of evaluation is either
a value or a piece of text representing an error.  However, for the
sake of brevity, in all the small step rules below we omit the error
handling for now. Every time the small step arrow ``↝`` is used in the
premise, if that arrow fails then the whole rule fails too.

Also note that the rules are designed such that for every expression,
at most one applies. This means that expressions are _precise_, that
is given

::
    f (error "foo") (error "blah")

the user can rely on the above evaluating to ``error ("foo")``, since
the inner argument will always be evaluated first.

Subject reduction should hold -- applying ``↝`` should preserve
well-typedness and not change the type, or more formally::

  ∀ Γₖ τ e₁ e₂.  (Γₖ ⊢ e₁ : τ) ∧ (Γₖ ⊢ e₁ ↝ e₂)  →  (Γₖ ⊢ e₂ : τ)

we should probably prove this property along other obvious ones
like soundness for the type system.

::

  Evaluation result
    r ::= Ok v                                      -- ResOk
       |  Err v                                     -- ResErr

                           ┌───────┐
  Small-step evaluation    │ e ↝ r │
                           └───────┘

     'val' x : τ ↦ v  ∈  〚Θ Ξ〛Mod
   —————————————————————————————————————————————————————————————————————— EvExpVal
     Mod.x  ↝  Ok v

     eᵢ  ↝  Ok eᵢ'
   —————————————————————————————————————————————————————————————————————— EvExpRecCon
     Mod.T τ₁ … τₘ {f₁ = v₁, …, fᵢ₋₁ = vᵢ₋₁, fᵢ = eᵢ, …, fₙ = eₙ}
       ↝
     Ok (Mod.T τ₁ … τₘ {f₁ = v₁, …, fᵢ₋₁ = vᵢ₋₁, fᵢ = eᵢ', …, fₙ = eₙ})

     e  ↝  Ok e'
   —————————————————————————————————————————————————————————————————————— EvExpVariantCon
     Mod.T.V τ₁ … τₙ e  ↝  Ok (Mod.T.V τ₁ … τₙ e')


   —————————————————————————————————————————————————————————————————————— EvExpRecProj
     Mod.T.fᵢ τ₁ … τₘ (Mod.T σ₁ … σₘ {f₁ = v₁, …, fᵢ = vᵢ, …, fₙ = vₙ})
       ↝
     Ok vᵢ

     e₁  ↝  Ok e₁'
   —————————————————————————————————————————————————————————————————————— EvExpConsHead
     'Cons' τ e₁ e₂  ↝  Ok ('Cons' τ e₁' e₂)

     e₂  ↝  Ok e₂'
   —————————————————————————————————————————————————————————————————————— EvExpConsTail
     'Cons' τ v₁ e₂  ↝  Ok ('Cons' τ v₁ e₂')

   —————————————————————————————————————————————————————————————————————— EvExpAppInst
     (λ x : τ → e) v  ↝  Ok e[x ↦ v]

   —————————————————————————————————————————————————————————————————————— EvExpTyAppInst
     (Λ a . e) τ  ↝  Ok e[a ↦ τ]

     e₁  ↝  Ok e₁'
   —————————————————————————————————————————————————————————————————————— EvExpAppFun
     e₁ e₂  ↝  Ok (e₁' e₂)

     e  ↝  Ok e'
   —————————————————————————————————————————————————————————————————————— EvExpAppArg
     v e  ↝  Ok (v e')

     e  ↝  Ok e'
   —————————————————————————————————————————————————————————————————————— EvExpTyApp
     e τ  ↝  Ok (e' τ)

     e₁ ↝ e₁'
   —————————————————————————————————————————————————————————————————————— EvExpCase
     'case' e₁ 'of' { alt₁, …, altₙ } (x → e₂)
       ↝
     Ok ('case' e₁' 'of' { alt₁, …, altₙ } (x → e₂)


   —————————————————————————————————————————————————————————————————————— EvExpCaseVariantMatch
     'case' Mod.T.V τ₁ … τₘ v 'of' { Mod.T.V x → e₁, … }
       ↝
     Ok e₁[x ↦ v]

     v is not of the form Mod.T.V τ₁ … τₘ v'
   —————————————————————————————————————————————————————————————————————— EvExpCaseVariantNoMatch
     'case' v 'of' { Mod.T.V x → e₁, alt₁, …, altₙ }
       ↝
     Ok ('case' v 'of' { alt₁, …, altₙ })


   —————————————————————————————————————————————————————————————————————— EvExpCaseNilMatch
     'case' 'Nil' τ 'of' { 'Nil' → e₁, … }
       ↝
     Ok e₁

     v is not of the form 'Nil' τ
   —————————————————————————————————————————————————————————————————————— EvExpCaseNilNoMatch
     'case' v 'of' { 'Nil' → e₁, alt₁, …, altₙ }
       ↝
     Ok ('case' v 'of' { alt₁, …, altₙ })


   —————————————————————————————————————————————————————————————————————— EvExpCaseConsMatch
     'case' 'Cons' τ v₁ v₂ 'of' { 'Cons' x₁ x₂ → e₁, … }
       ↝
     Ok e₁[x₁ ↦ v₁, x₂ ↦ v₂]

     v is not of the form 'Cons' τ v₁ v₂
   —————————————————————————————————————————————————————————————————————— EvExpCaseConsNoMatch
     'case' v 'of' { 'Cons' x₁ x₂ → e₁, alt₁, …, altₙ }
       ↝
     Ok ('case' v 'of' { alt₁, …, altₙ })


   —————————————————————————————————————————————————————————————————————— EvExpCaseTrueMatch
     'case' 'True' 'of' { 'True' → e, … }
       ↝
     Ok e

     v is not of the form 'True'
   —————————————————————————————————————————————————————————————————————— EvExpCaseTrueNoMatch
     'case' v 'of' { 'True' → e, alt₁, …, altₙ }
       ↝
     Ok ('case' v 'of' { alt₁, …, altₙ })


   —————————————————————————————————————————————————————————————————————— EvExpCaseFalseMatch
     'case' 'False' 'of' { 'False' → e, … }
       ↝
     Ok e

     v is not of the form 'False'
   —————————————————————————————————————————————————————————————————————— EvExpCaseFalseNoMatch
     'case' v 'of' { 'False' → e₁, alt₁, …, altₙ }
       ↝
     Ok ('case' v 'of' { alt₁, …, altₙ })


   —————————————————————————————————————————————————————————————————————— EvExpCaseFalseNoMatch
     'case' v 'of' { x → e, … }
       ↝
     Ok (e[x ↦ v])


   —————————————————————————————————————————————————————————————————————— EvExpFoldlNil
     'foldl' τ σ v₁ v₂ ('Nil' τ)  ↝  Ok v₂


   —————————————————————————————————————————————————————————————————————— EvExpFoldlCons
     'foldl' τ σ v₁ v₂ ('Cons' τ v₃ v₄)
       ↝
     Ok ('foldl' τ σ v₁ (v₁ v₂ v₃) v₄)

     e₁  ↝  Ok e₁'
   —————————————————————————————————————————————————————————————————————— EvExpLetInst
     'let' x : τ = e₁; e₂
       ↝
     Ok ('let' x : τ = e₁'; e₂)


   —————————————————————————————————————————————————————————————————————— EvExpLetInst
     'let' x : τ = v; e  ↝  Ok e[x ↦ v]


   —————————————————————————————————————————————————————————————————————— EvExpError
     'error' τ v  ↝  Err v


   —————————————————————————————————————————————————————————————————————— EvExpEqTrue
     'eq' τ v v  ↝  Ok 'True'


     v₁ and v₂ are not syntactically equal
   —————————————————————————————————————————————————————————————————————— EvExpEqTrue
     'eq' τ v₁ v₂  ↝  Ok 'False'


     eᵢ  ↝  Ok eᵢ'
   —————————————————————————————————————————————————————————————————————— EvExpTupleCon
     ⦃ f₁ = v₁, …, fᵢ₋₁ = vᵢ₋₁, fᵢ = eᵢ, …, fₘ = eₘ ⦄
       ↝
     Ok ⦃ f₁ = v₁, …, fᵢ₋₁ = vᵢ₋₁, fᵢ = eᵢ', …, fₘ = eₘ ⦄


   —————————————————————————————————————————————————————————————————————— EvExpTupleProj
     ⦃ f₁ = v₁, …, fᵢ = vᵢ, …, fₘ = vₘ ⦄.fᵢ
       ↝
     Ok vᵢ

Note that given the rule ``EvExpVal``, if we did not enforce all defined
values to be values we would repeatedly recompute every time a value
reference is instantiated. This is potentially very confusing to the user,
and so we think that it's better to have them guard top-level
computations explicitly under a function (all functions are values).

..
  TODO (SM): explain that this is a CBV semantics; and illustrate how the
  ``EvExpApp*`` and ``EvExpCons*`` rules are driving evaluation.


Update interpretation
~~~~~~~~~~~~~~~~~~~~~

We define the operational semantics of the update interpretation against the
ledger model described in the "DA Ledger Model" theory report. The result of an
update is a value accompanied by a transaction for this ledger model. For the
sake of clarity, particularly wrt. this transaction, we provide big-step
semantics. To keep track of the current state of the ledger the big-step
relation ``→ᵤ`` also carries a contract store ``st`` around. We further depend
on the reflexive transitive closure ``↝*`` of the small-step evaluation relation
``↝`` for the pure part of the language::


  Contracts on the ledger
    Contract
      ::= (cid, Mod.T, v)                  -- v must be of type Mod.T

  Ledger actions
    act
      ::= 'create' Contract
       |  'exercise' v Contract ChKind tr  -- v must be of type 'List' 'Party'

  Ledger transactions
    tr
      ::= ε
       |  act₁ · … · actₙ

  Contract states
    ContractState
      ::= 'active'
       |  'inactive'

  Contract stores
     st ∈ finite maps from cid to (Mod.T, v, ContractState)

                                    ┌──────────────────────────┐
  Big-step update interpretation    │ u ‖ st₀ →ᵤ (v, tr) ‖ st₁ │
                                    └──────────────────────────┘

     e  ↝*  Ok v
   —————————————————————————————————————————————————————————————————————— EvUpdPure
     'pure' e ‖ st₀  →ᵤ  (v, ε) ‖ st₀

     e₁  ↝*  Ok u₁
     u₁ ‖ st₀  →ᵤ  (v₁, tr₁) ‖ st₁
     e₂[x ↦ v₁] ‖ st₁  →ᵤ  (v₂, tr₂) ‖ st₂
   —————————————————————————————————————————————————————————————————————— EvUpdBind
     'bind' x : τ ← e₁; e₂ ‖ st₀  →ᵤ  (v₂, tr₁ · tr₂) ‖ st₂

     'tpl' (x : T) ↦ { 'precondition' eₚ, … }  ∈  〚Θ Ξ〛Mod
     cid ∉ dom(st₀)
     eₜ  ↝*  Ok vₜ
     eₚ[x↦vₜ]  ↝*  Ok 'True'
     tr = 'create' (cid, Mod.T, vₜ)
     st₁ = st₀[cid↦(Mod.T, vₜ, 'active')]
   —————————————————————————————————————————————————————————————————————— EvUpdCreate
     'create' Mod.T eₜ ‖ st₀  →ᵤ  (cid, tr) ‖ st₁

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'consuming' Ch (y : τ) : σ  'by' eₚ ↦ eₐ, … }, … }
       ∈  〚Θ Ξ〛Mod
     e₁  ↝*  Ok v₁
     e₂  ↝*  Ok v₂
     e₃  ↝*  Ok v₃
     v₁ ∈ dom(st₀)
     st₀(v₁) = (Mod.T, vₜ, 'active')
     eₚ[x↦vₜ]  ↝*  Ok vₚ
     v₂ = vₚ
     st₁ = st₀[v₁↦(Mod.T, vₜ, 'inactive')]
     eₐ[x↦vₜ, y↦v₃]  ↝*  Ok uₐ
     uₐ ‖ st₁  →ᵤ  (vₐ, trₐ) ‖ st₂
     tr = 'exercise' v₂ (v₁, Mod.T, vₜ) 'consuming' trₐ
   —————————————————————————————————————————————————————————————————————— EvUpdExerciseConsuming
     'exercise' Mod.T.Ch e₁ e₂ e₃ ‖ st₀
       →ᵤ
     (vₐ, tr) ‖ st₂

     'tpl' (x : T)
         ↦ { 'choices' { …, 'choice' 'non-consuming' Ch (y : τ) : σ  'by' eₚ ↦ eₐ, … }, … }
       ∈  〚Θ Ξ〛Mod
     e₁  ↝*  Ok v₁
     e₂  ↝*  Ok v₂
     e₃  ↝*  Ok v₃
     v₁ ∈ dom(st₀)
     st₀(v₁) = (Mod.T, vₜ, 'active')
     eₚ[x↦vₜ]  ↝*  Ok vₚ
     v₂ = vₚ
     eₐ[x↦vₜ, y↦v₃]  ↝*  Ok uₐ
     uₐ ‖ st₀  →ᵤ  (vₐ, trₐ) ‖ st₁
     tr = 'exercise' v₂ (v₁, Mod.T, vₜ) 'non-consuming' trₐ
   —————————————————————————————————————————————————————————————————————— EvUpdExerciseNonConsuming
     'exercise' Mod.T.Ch e₁ e₂ e₃ ‖ st₀
       →ᵤ
     (vₐ, tr) ‖ st₁

     'tpl' (x : T) ↦ …  ∈  〚Θ Ξ〛Mod
     e₁  ↝*  Ok v₁
     e₂  ↝*  Ok v₂
     v₁ ∈ dom(st₀)
     st₀(v₁) = (Mod.T, vₜ, 'active')
   —————————————————————————————————————————————————————————————————————— EvUpdFetch
     'fetch' Mod.T e₁ e₂ ‖ st₀  →ᵤ  (vₜ, ε) ‖ st₀

..
  TODO (SM): add support for the ``getTime`` function that receives the
  ledger-effective time at which the update-expression is being interpreted.
.. TODO (SM): restrict fetch such that it is only allowed for stakeholders.
.. TODO (MH): Rewrite "v must be of type ..." comments as typing rules

.. _core-java-interfsace:

Java interface
^^^^^^^^^^^^^^

..
  TODO: attach a sample generated Java class to show that things are
  fine.

..
  TODO: Propose using Java enums for variants that have ``Unit``
  as argument for all constructors. Same for JSON with strings
  instead of objects.

..
  TODO (SM): investigate how this relates to Scala's approach of
  defining records and variants. Ideally, they coincide, such that one
  can either use the Java view onto the Scala generated classes; or
  directly the Java generated classes if one wants to keep Scala out
  of the build-pipeline (not an immediate goal of our SDK efforts).

  * FM: Well, we have two choices: reuse the Java classes, or also
    have a Scala codegen. The latter would have better usability I
    think, especially for variants. But I think the Java interface
    would also be perfectly usable from Scala.

..
  TODO (SM): verify with a Scala expert; ideally Mario P.

  * alignment between Java and Scala codegen
  * impl. of Java codegen as just the Java-view onto the Scala classes
  * best practices wrt module/package naming
  * overall design

..
  TODO (SM): also specify for what function definitions we can
  generate bindings; and how we would do that. I don't want to do it
  straight away, but I'm worried about the interaction between design
  decisions here, and what that would mean for exposing functions.

  In particular, I see a strong benefit in exposing functions of the
  type ``foo :: (Serializable a, Serializable b) => a -> Update b``,
  as we can use them to efficiently construct (and communicate)
  transactions interactively.

  * FM: I'll add a section sketching how we might implement function
    bindings in Java

  * SM: Additional use case have date/time logic defined in DAML
      and then call it from Java

..
  TODO (SM): describe how we handle reserved names in the Java
  ecosystem, e.g., ``class`` cannot be used as an identifier, but our
  DAML-LF spec allows it. I'm thinking of escaping them using a ``daml$``
  prefix.

Here we provide a sketch for an algorithm that converts a DAML-LF
module Θ to a set of Java classes. Each package will then result in a
jar containing the generated classes. Note that since the module names
are unique in each package we know that we can give unique Java names
to all generated classes, too.  On the other hand the naming of the
jar will be left to other tooling.

Here we only specify the Java types, not the implementation (the part
that actually constructs commands and talks to the ledger).

Recap: we have a mapping from module names to definitions::

  θ ∈ ModName ↦ Δ

Each ``Δ`` contains definitions for:

- templates;
- records;
- variants;
- values.

For DAML 1.0 we will generate Java classes for *all* templates,
records, and variants. We will not generate bindings for values.  We
sketch the type conversion / class generation for each type and for
type applications. We denote the Java counterpart to a DAML type ``τ``
by ``⦇τ⦈``.

Overview of Java bindings generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For every module ``Δ`` in ``Θ``, a Java class will be generated.  The
name ``ModName`` is used to generate the file and name. The exact
rules on how the name is generated are to be defined, and might be
tweakable depending on the deployment. E.g. the module
``Corp.Some.Module`` might live in ``com.corp.some.Module``.

Each module class will contain a series of static classes representing
each defined type and template. Obviously the Java classes can import
other generated Java classes, and they can all import a base
environment defining a few classes representing the base DAML
environment (say ``com.digitalasset.daml.*``).

Value / Template interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~

We will define an interface to identify the universe of DAML values::

  interface Value {}


..
  TODO (SM): clarify the relation of this class to the definition of values
  above. It feels like we want to identifiy the serializable values, which is
  a significantly smaller set.

and one to identify templates::

  interface Template extends Value {}

in this proposal there are no methods and the interface are just to
constraint type parameters, but we will probably want to add methods
to it -- for example utilities to pretty print the type and values.

Note that the serialization is handled by the templates -- see
following sections. The main reason for not offering serialization /
deserializing methods in the class themselves is that:

* we cannot easily carve out at the type level the types
  which are serializable;
* we would not be able to provide a "deserialization" method,
  given Java's lack of return type polymorphism.

Note that the above two points are really problematic only in the
presence of generic parameters, which we do have.

Also note that we have ``Template`` to also be a ``Value``. This is
since as we have seen in the previous sections templates are referred
to using their argument type, and we exploit this fact together with
the template coherence condition to attach the functionality relevant
to the template to the class for its argument. This considerably
increases the ergonomics of the Java bindings (see following
sections).

.. _nominal-core-primitives:

Primitive types
~~~~~~~~~~~~~~~

Each primitive type will have a pre-defined Java counterpart.
Monomorphic ones are straight forward::

  class Int64 implements Value {
    protected long n;
    ...
  }

  class Decimal implements Value {
    protected BigDecimal d;
    ...
  }

  class Party implements Value {
    protected String p;
    ...
  }

  class Char implements Value {
    protected int c;
    ...
  }

  class Text implements Value {
    protected String s;
    ...
  }

  class Date implements Value {
    protected java.time.LocalDate d;
    ...
  }

  class Time implements Value {
    protected java.time.Instant d;
    ...
  }

  class Unit implements Value {
    ...
  }

  ...

``'Bool'`` gets compiled to a Java ``boolean``.

.. TODO (FM / MH) complete this once we have all the primitive types.

..
  TODO(MH): Explain that the ``Char`` class needs to hold an ``int`` since
  ``char`` cannot hold all unicode characters.

..
  TODO(MH): Discuss resolution of time: ``1 μs`` vs ``1 ns``.
  ``java.sql.Timestamp`` supports ``1 ns``.

Moreover, we will need some polymorphic ones::

  // DAML functions cannot be represented in Java, but they can
  // be talked about
  class Function<A extends Value, B extends Value> implements Value {}

  class ContractId<Tpl extends Template> implements Value {
    public byte[] cid;
  }

  class List<A extends Value> implements Value {
    public ArrayList<A> elements;
  }

Note that we won't be able to represent DAML functions in Java in DAML
1.0, but we need to have a corresponding Java type (even if it has no
members) to reliably generate Java bindings for all DAML types defined
in a module. Note that functions will never appear in templates, and
so the user will never be in the need of constructing members of
``Functions<A, B>`` anyway.

..
  TODO (SM): consider explaining the mappings for serializable types
  only, which side-steps the problem with ``Function``.

  FM: Agreed, I will remove ``Function`` explain how we can never
  end up with ``Function`` in the generated ``Java``.

..
  TODO: Find out what the best represention of ``ByteString`` is in Java
  for stuff like ``ContractId``.

.. _core-java-records:

Records
~~~~~~~

Each record::

  'data' T a₁ … aₙ ↦ { f₁: τ₁, …, fₙ: τₙ }

gives rise to Java class::

  class T<A₁ extends Value, …, Aₙ extends Value> {
    public ⦇τ₁⦈ f₁;
     ⋮
    public ⦇τₙ⦈ fₙ;

    public T(⦇τ₁⦈ f₁, …, ⦇τₙ⦈ fₙ) {
      this.f₁ = f₁;
       ⋮
      this.fₙ = fₙ;
    }
  }

Note that each type ``τᵢ`` might contain references to any ``aⱼ`` from
the type parameter list. These will be converted to the respective
Java generic parameters.

..
  TODO (SM): explain mapping of type variable names. Are they
  lower-case or not? Do they contain reserved keywords?

..
  TODO (SM): consider using protected fields and getters.

..
  TODO: Look at <http://jamesiry.github.io/jADT/why_adt.html>
  for inspiration / differences

.. _core-java-variants:

Variants
~~~~~~~~

Variants are represented using sealed classes. Each variant::

  'data' T a₁ … aₙ ↦ V₁: τ₁ | … | Vₙ: τₙ

gives rise to class::

  class T<A₁ extends Value, …, Aₙ extends Value> {
    private T() { }

    final static class V₁<A₁ extends Value, …, Aₙ extends Value> extends T<A₁, …, Aₙ> {
      public final τ₁ body;
      public V₁(arg: ⦇τ₁⦈) { this.body = arg }
    }
     ⋮
    final static class Vₙ<A₁ extends Value, …, Aₙ extends Value> extends T<A₁, …, Aₙ> {
      public final τₙ body;
      public Vₙ(arg: ⦇τₙ⦈) { this.body = arg }
    }
  }

As a special case, each τᵢ that refers to type ``T.Vᵢ a₁ … aₙ``, where
``T.Vᵢ`` is a record type::

  'data' T.Vᵢ a₁ … aₙ ↦ { f₁: τ₁′, …, fₙ: τₙ′ }

will instead produce the Java code for ``Vᵢ`` within ``class T`` instead
of the above::

  final static class Vᵢ<A₁ extends Value, …, Aₙ extends Value> extends T<A₁, …, Aₙ> {
    public final ⦇τ₁′⦈ f₁;
     ⋮
    public final ⦇τₙ′⦈ fₙ;

    public T(⦇τ₁′⦈ f₁, …, ⦇τₙ′⦈ fₙ) {
      this.f₁ = f₁;
       ⋮
      this.fₙ = fₙ;
    }
  }

and no other Java type definition or code will be produced for ``T.Vᵢ``.

Type applications
~~~~~~~~~~~~~~~~~

Each applied type constructor::

  T τ₁ … τₙ

gets converted to::

  T<⦇τ₁⦈, …, ⦇τₙ⦈>

Hierarchical names for type constructors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type constructor names can be made up of multiple components. For
example, in module ``Dummy`` we might have definitions::

  'data' Foo ↦ V₁: τ₁ | … | Vₙ: τₙ ·
  'data' Foo.Bar.Baz ↦ { f₁: τ₁, …, fₙ: τₙ } · ⋯

The hierarchical nature of these names is reflected in the Java
class::

  class Module {
    static class Foo {
      // variant Foo bindings...

      static class Bar {
        static class Baz {
          // record Foo.Bar.Baz bindings...
        }
      }
    }
      ⋮
  }

currently this functionality is only used to desugar types that are
defined together with the templates, but we might extend this in the
future.

Updates
~~~~~~~

We do not want to specify the Java interface to the ledger API at the
moment, but we need it to specify the Java template generation, so we
use a dummy interface::

  interface Update<A extends Value> {
    A execute();
  }

to represent commands that can be executed on a server.

Templates
~~~~~~~~~

Due to template coherence, we know that each data type is the argument
of at most one template. Thus, to represent templates, we augment the
argument type with additional definitions.

So if we have definition::

  'data' T ↦ { f₁: τ₁, …, fₙ: τₙ }

which gives rise to class::

  class T extends Value {
    // record definition...
  }

Then the template definition::

  'tpl' (x: T) ↦
    { 'precondition' …
    , 'signatories' …
    , 'observers' …
    , 'agreement' …
    , 'choices'
        { 'choice' Ch₁ (x₁ : τ₁) : σ₁ 'by' eₚ₁ ↦ e₁
	, …
	, 'choice' Chₙ (xₙ : τₙ) : σₙ 'by' eₚₙ ↦ eₙ
	}
    }

will augment the existing class ``T`` with several other
members::

  class T extends Template {
    // record definition...

    // class representing contract ids for the template,
    // together with methods to exercise contract instances
    // of the template
    static class Id extends ContractId<T> {
      // function to exercise each of the choice
      public Update<⦇σ₁⦈> exerciseCh₁(Party actor, ⦇τ₁⦈ arg) { … }
       ⋮
      public Update<⦇σₙ⦈> exerciseChₙ(Party actor, ⦇τₙ⦈ arg) { … }
    }

    // function to create instances of the template
    public Update<Id> create() { … }
  }

the serialization / deserialization burden is pushed on the template
definitions since here we are guaranteed that every involved type is
serializable.

Moreover, if a choice argument is a record, we offer shorthand
functions to be able to exercise choices creating the record on
the fly. For example, consider a surface DAML 1.0 program
containing::

  template Counter {owner: Party, v: Int64} as this = contract
    { choice Add{x: Int64} : ContractId Counter by owner =
        create Counter{this with v = this.v + x} };

which will give rise to a DAML-LF module with definitions::

    'data' Counter ↦ {owner: Party, v: Int64} ·                           -- Record definition for template argument
    'data' Counter.Add ↦ {x: Int64} ·                                     -- Record definition for choice argument
    'tpl' Counter ↦ { 'choice' Add (arg: Counter.Add) : (ContractId Counter) 'by' owner ↦ … } · -- Template definition
    ε

which in turn will give rise to the following Java class::

  class Counter implements Template {
    public Party owner;
    public Int64 v;

    class Add implements Value {
      public Int64 x;
    }

    class Id extends ContractId<Counter> {
      public Update<Id> exerciseAdd(Party actor, Add arg) { … }
      // short-hand form
      public Update<Id> exerciseAdd(Party actor, Int64 x) { … }
    }

    public Update<Id> create() { … }
  }

Scala interface
^^^^^^^^^^^^^^^

..
  TODO: attach a sample generated Scala class to show that things are
  fine.


Here we provide a sketch for an algorithm that converts a DAML-LF
module Θ to a set of Scala classes. Each package will then result in a
jar containing the generated classes. Note that since the module names
are unique in each package we know that we can give unique Scala names
to all generated classes, too.  On the other hand the naming of the
jar will be left to other tooling.

Here we only specify the Scala types, not the implementation (the part
that actually constructs commands and talks to the ledger).

Recap: we have a mapping from module names to definitions::

  θ ∈ ModName ↦ Δ

Each ``Δ`` contains definitions for:

- templates;
- records;
- variants;
- values.

For DAML 1.0 we will generate Scala classes for *all* templates, and
records and variants on which those templates depend.  We will not
generate bindings for values.  We sketch the type conversion / class
generation for each type and for type applications. We denote the Scala
counterpart to a DAML type ``τ`` by ``⦇τ⦈``.

Overview of Scala bindings generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For every module ``Δ`` in ``Θ``, a Scala package will be generated.  The
name ``ModName`` is used to generate the file and name. The exact
rules on how the name is generated are to be defined, and might be
tweakable depending on the deployment. E.g. the module
``Corp.Some.Module`` might live in ``com.corp.some.Module``.

Each module package will contain a series of classes representing
each defined type and template. Obviously the Scala classes can import
other generated Scala classes, and they can all import a base
environment defining a few classes representing the base DAML
environment (say ``com.digitalasset.daml.*``).

Value / Template interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~

We will define a closed typeclass to identify the universe of DAML values::

  sealed abstract class Value[T]

and an interface to identify the universe of DAML records and variants::

  abstract class ValueRef extends Product with Serializable

and one to identify templates::

  abstract class Template[+T] extends ValueRef {self: T =>
    def create: Update[ContractId[T]]
  }

``Value`` contains typeclass methods for encoding and decoding values
for the ledger API.

The ``Value`` typeclass identifies both the primitive types, defined
below, and valid record and variant types.  Primitive types include
types like ``Long`` and ``String`` whose superclass set cannot be
extended; with typeclasses, there is no need for wrapper classes to
represent these types.  All record and variant types extend
``ValueRef``, which permits a guarantee that the typeclass is defined
only for primitives and classes extending ``ValueRef``.

Also note that we have ``Template`` to also be a ``ValueRef``. This is
since as we have seen in the previous sections templates are referred
to using their argument type, and we exploit this fact together with
the template coherence condition to attach the functionality relevant
to the template to the class for its argument. This considerably
increases the ergonomics of the Scala bindings (see following
sections).

.. _nominal-core-scala-primitives:

Primitive types
~~~~~~~~~~~~~~~

Each primitive type will have a pre-defined Scala counterpart, for which
an instance of ``Value`` is provided.  Monomorphic ones are
straightforward::

  sealed abstract class Primitive {
    // publicly visible definitions:
    type Int64 = Long
    type Decimal = BigDecimal
    type Party = com.digitalasset.service.Platform.Party
    val Party: com.digitalasset.service.Platform.Party.type = ...
    type Text = String
    type Date <: java.time.LocalDate
    type Time <: java.time.Instant
    type Unit = scala.Unit
    type Bool = scala.Bool
    type List[+A] = collection.immutable.Seq[A]
    val List: collection.immutable.Seq.type = ...
    ...
  }

  val Primitive: Primitive = new Primitive {
    // private definitions
  }

In ML terms, ``sealed abstract class Primitive`` serves as a signature,
and ``val Primitive: Primitive`` as a structure annotated with the
signature type.  Abstract types must be defined in an ``abstract class``
or ``trait`` in Scala, and some primitive types are abstract.

The primitive ``Char`` is represented with a 32-bit ``Int`` because
Scala-JVM's own primitive ``Char`` is only 16-bit, and therefore isn't
wide enough to represent a Unicode character.

..
  TODO(MH): Discuss resolution of time: ``1 μs`` vs ``1 ns``.
  ``java.sql.Timestamp`` supports ``1 ns``.

Moreover, we will need some polymorphic ones, for which inductive
``Value`` instances are supplied::

  type ContractId[+Tpl]

  implicit def `ContractId Value`[Tpl <: Template[Tpl]]: Value[ContractId[Tpl]]

  implicit def `List Value`[A: Value]: Value[List[A]]

DAML types that are not used by any template or choice type, whether
directly or indirectly, will not have corresponding Scala types emitted.
From a Scala author's perspective, the only use for a generated DAML
type is in conjunction with some template or choice.

..
  TODO: Find out what the best represention of ``ByteString`` is in Java
  for stuff like ``ContractId``.

.. _core-scala-records:

Records
~~~~~~~

Each record::

  'data' T a₁ … aₙ ↦ { f₁: τ₁, …, fₙ: τₙ }

gives rise to Scala class::

  final case class T[+A₁, …, +Aₙ](
    f₁: ⦇τ₁⦈,
     ⋮
    fₙ: ⦇τₙ⦈,
  ) extends ValueRef

  // also extends (⦇τ₁⦈, …, ⦇τₙ⦈) => T iff 0 type parameters
  object T extends ValueRefCompanion {
    // implicit val iff 0 type parameters
    implicit def `T Value`[A₁: Value, …, Aₙ: Value]: Value[T[A₁, …, Aₙ]]
  }

Note that each type ``τᵢ`` might contain references to any ``Aⱼ`` from
the type parameter list. These will be converted to the respective
Scala generic parameters.

Each of ``A₁, …, Aₙ`` is exactly the respective element of ``a₁ … aₙ``,
but quoted with backticks if not an identifier as defined by `SLS §1.1
"Identifiers"
<https://scala-lang.org/files/archive/spec/2.12/01-lexical-syntax.html#identifiers>`_.
This handles reserved words as well; the type variable ``class`` will
simply be surrounded by backticks.

Each of ``f₁ … fₙ`` is treated similarly.  However, if ``fᵢ`` is exactly
one of the following with zero or more trailing underscores, its Scala
name will have one underscore appended to it.

* _root_
* asInstanceOf
* getClass
* hashCode
* isInstanceOf
* notify
* notifyAll
* productArity
* productIterator
* productPrefix
* toString
* wait

The companion's extension of a function type works around `Scala bug
3664 <https://github.com/scala/bug/issues/3664>`_.

.. _core-scala-variants:

Variants
~~~~~~~~

Variants are represented using sealed classes. Each variant::

  'data' T a₁ … aₙ ↦ V₁: τ₁ | … | Vₙ: τₙ

gives rise to classes::

  sealed abstract class T[+A₁, …, +Aₙ] extends ValueRef

  object T extends ValueRefCompanion {
    final case class V₁[+A₁, …, +Aₙ](
      body: ⦇τ₁⦈
    ) extends T[A₁, …, Aₙ]
     ⋮
    final case class Vₙ[+A₁, …, +Aₙ](
      body: ⦇τₙ⦈
    ) extends T[A₁, …, Aₙ]

    implicit def `T Value`[A₁: Value, …, Aₙ: Value]: Value[T[A₁, …, Aₙ]]
  }

As a special case, each τᵢ that refers to type ``T.Vᵢ a₁ … aₙ``, where
``T.Vᵢ`` is a record type::

  'data' T.Vᵢ a₁ … aₙ ↦ { f₁: τ₁′, …, fₙ: τₙ′ }

will instead produce the Scala code for ``Vᵢ`` instead of the above::

  final case class Vᵢ[+A₁, …, +Aₙ](
    f₁: ⦇τ₁′⦈,
     ⋮
    fₙ: ⦇τₙ′⦈,
  ) extends T[A₁, …, Aₙ]

and no other Scala type definition or code will be produced for
``T.Vᵢ``.

..
  TODO (SC): Propose using ``case object`` for data constructors that
  have ``Unit`` as argument, iff T has no invariant type parameters.

Type applications
~~~~~~~~~~~~~~~~~

Each applied type constructor::

  T τ₁ … τₙ

gets converted to::

  T[⦇τ₁⦈, …, ⦇τₙ⦈]

Hierarchical names for type constructors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The type constructor names can be made up of multiple components. For
example, in module ``Dummy`` we might have definitions::

  'data' Foo ↦ V₁: τ₁ | … | Vₙ: τₙ ·
  'data' Foo.Bar.Baz ↦ { f₁: τ₁, …, fₙ: τₙ } · ⋯

The hierarchical nature of these names is reflected in the Scala
class::

  package Module {
    sealed abstract class Foo extends ValueRef

    object Foo extends ValueRefCompanion {
      // variant Foo bindings...

      object Bar {
        final case class Baz // record Foo.Bar.Baz bindings...
      }
    }
      ⋮
  }

Currently this functionality is only used to desugar types that are
defined together with the templates, but we might extend this in the
future.

Updates
~~~~~~~

We do not want to specify the Scala interface to the ledger API at the
moment, but we need it to specify the Scala template generation, so we
use an abstract interface::

  /* abstract */ type Update[+A]

to represent commands that can be executed on a server.  This is not a
member of the ``Value`` typeclass.

Templates
~~~~~~~~~

Due to template coherence, we know that each data type is the argument
of at most one template. Thus, to represent templates, we augment the
argument type with additional definitions.

So if we have definition::

  'data' T ↦ { f₁: τ₁, …, fₙ: τₙ }

which gives rise to class::

  final case class T(/* record definition... */) extends ValueRef

Then the template definition::

  'tpl' (x: T) ↦
    { 'precondition' …
    , 'signatories' …
    , 'observers' …
    , 'agreement' …
    , 'choices'
        { 'choice' Ch₁ (x₁ : τ₁) : σ₁ 'by' eₚ₁ ↦ e₁
	, …
	, 'choice' Chₙ (xₙ : τₙ) : σₙ 'by' eₚₙ ↦ eₙ
	}
    }

will augment the existing class ``T`` with several other members::

  final case class T(/* record definition... */) extends Template[T] {
    // record definition members, if any...
  }

  object T extends TemplateCompanion[T] with ((/* record types... */) => T) {
    // methods to exercise contract instances
    // of the template
    implicit class `T syntax`(private val self: Id) extends AnyVal {
      // function to exercise each of the choice
      def exerciseCh₁(actor: Party, choiceArgument: ⦇τ₁⦈): Update[⦇σ₁⦈] = { … }
       ⋮
      def exerciseChₙ(actor: Party, choiceArgument: ⦇τₙ⦈): Update[⦇σₙ⦈] = { … }
    }
  }

the serialization / deserialization burden is pushed on the template
definitions since here we are guaranteed that every involved type is
serializable.

Moreover, if a choice argument is a record, we offer shorthand
functions to be able to exercise choices creating the record on
the fly. For example, consider a surface DAML 1.0 program
containing::

  template Counter {owner: Party, v: Int64} as this = contract
    { choice Add{x: Int64} : ContractId Counter by owner =
        create Counter{this with v = this.v + x} };

which will give rise to a DAML-LF module with definitions::

    'data' Counter ↦ {owner: Party, v: Int64} ·                           -- Record definition for template argument
    'data' Counter.Add ↦ {x: Int64} ·                                     -- Record definition for choice argument
    'tpl' Counter ↦ { 'choice' Add (arg: Counter.Add) : (ContractId Counter) 'by' owner ↦ … } · -- Template definition
    ε

which in turn will give rise to the following Scala class::

  final case class Counter(owner: Party, v: Int64) extends Template[Counter] {
    def create: Update[ContractId[Counter]] = ...
  }

  object Counter extends TemplateCompanion[Counter] with ((Party, Int64) => Counter) {
    final case class Add(x: Int64) extends ValueRef
    object Add extends ValueRefCompanion with (Int64 => Add) {
      ...
    }

    implicit class `Counter syntax`(private val self: Id) extends AnyVal {
      def exerciseAdd(actor: Party, choiceArgument: Counter.Add): Update[ContractId[Counter]] = { … }
      // short-hand form
      def exerciseAdd(actor: Party, x: Int64): Update[ContractId[Counter]] = { … }
    }

    ...
  }

Auxiliary definitions
~~~~~~~~~~~~~~~~~~~~~

The below is a sketch of the runtime support library on whose types the
above would depend, where not already described above.

``ValueRefCompanion`` and ``TemplateCompanion`` provide the sole
extension point for the ``Value`` typeclass, as well as several methods
that serve as "universal static methods" for all records/variants and
templates, respectively; rather than being generated for each type, they
can simply be inherited::

  abstract class ValueRefCompanion {
    // This function is not generally safe for generic records/variants,
    // so is hidden. It's the only externally visible opening in the
    // Value typeclass.
    protected def `ValueRefCompanion Value`[T <: ValueRef]: Value[T]
  }

  abstract class TemplateCompanion[T <: Template[T]] extends ValueRefCompanion {
    type ContractId = Primitive.ContractId[T]
  }

Aligning DAML types and Java types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Comment (SC)**

Some encodings of DAML types might yield equal Java types for distinct
DAML types; e.g. tuple field names could be erased, thus making tuples
serializable in `Type system`_.  The imperfect encoding of Scala types
in Java (and vice versa) provides some precedence for dealing with this
mismatch.

Here are some cases in Java and Scala's interface with each other where
one language ignores the differences visible in the other.

primitives
  ``Integer`` and ``Int`` in a non-type-parameter position turn to
  ``Integer`` and ``int`` in Java, but ``List[Integer]`` and
  ``List[Int]`` will both convert to ``List<Integer>``

type member abstraction and application
  For example, `the MList encoding`_ will simply drop the ``type T``, so
  that ``head: Object``. The refinements ``{type T = self.T}`` are also
  dropped, so e.g. ``uncons: Option<MCons>``. This is also why newtypes
  defined with ``@@`` as common in scala-ledger-api and elsewhere all
  look like ``Object`` in Java: either they consist of ``{...}``, or a
  reference to a type member whose upper bound is something that erases
  to ``Object``.

intersection
  The Scala types ``A with B`` and ``A with C`` are
  generally not equal (it depends on what B and C are), but both will
  become ``A`` in Java. Even stranger, ``A with B`` and ``B with A``
  _are_ equal, but may become ``A`` and ``B`` respectively in
  Java. It’s too bad because Java supports intersection in a few
  contexts, just not enough.

variance
  It’s common to write ``Function<? super T, ? extends R>``,
  but in Scala, the equivalent rules say ``Function[_ >: T, _ <: R] =
  Function[T, R]``, so no one bothers. But the difference is still seen
  from Java, arbitrarily based on what you happened to write.

Any
  Both it and ``AnyRef`` become ``Object`` in Java, though ``Any`` is a
  strict supertype. Scala considers only ``AnyRef = Object``.

rawtypes
  The Java types ``List`` and ``List<?>`` both translate to
  ``List[_]`` in Scala.

Existential quantifiers
  This will be going away, but as it stands, the Scala types
  ``(Buffer[E], Buffer[E]) forSome {type E}`` and ``(Buffer[_],
  Buffer[_])`` will both be translated to Java type ``Tuple2<Buffer<?>,
  Buffer<?>>``, despite that the former is a strict subtype of the
  latter (permitting exchange of E values between the buffers). The
  suggested replacement is…type member abstraction, as above.

Higher-kinded types
  Type parameters of other kinds are simply assigned kind ``*`` in the
  Java signature, with any parameters passed in the dependent
  signature(s) dropped.  The resulting APIs more or less read like
  nonsense.  For example the ``Functor`` method ``map[A, B](fa: F[A])(f:
  A => B): F[B]`` turns to ``<A, B> F map(F fa, Function1<A, B> f)`` in
  Java.

.. _`the MList encoding`: https://typelevel.org/blog/2015/07/13/type-members-parameters.html#two-lists-all-alike

When you use these features at the Java boundary, your API typically
becomes either unsound or unusable; more “Scala-idiomatic” code is more
likely to use these features, and even basic Scala runs into the
primitives and variance cases. Scala code that avoids them can’t quite
get the same reusability or safety, so Scala code meant to satisfy a
pleasant Java interface must ironically become more Javaesque (in the
negative sense) itself.

The fewer touchpoints between Java and Scala, the less of a problem this
becomes. So you might have a complex Scala library but with just a few
things that need to be Java-accessible.  But if you’re designing a rich
library, you must decide in advance whether Scala-friendliness
(convenience and safety) or Java-friendliness is more important, because
they are in direct conflict.

JSON
^^^^

Primitive types
~~~~~~~~~~~~~~~

.. TODO(FM) complete when we know what the primitive types are.

Defined types
~~~~~~~~~~~~~

Records are serialized in the obvious way -- using JSON records.

Variants are represented using ``{Constructor: Body}``, e.g.
``{"Left": True}``.

Lists are represented using JSON lists.

XML
^^^

Primitive types
~~~~~~~~~~~~~~~

* ``Bool`` -> ``boolean``

* ``Int64`` -> ``long``

* ``Decimal`` ->
  ::

    <simpleType name='decimal_38_8'>
      <restriction base='decimal'>
        <fractionDigits value='8' fixed='true'/>
        <minExclusive value='-1000000000000000000000000000000.0'/>
        <maxExclusive value= '1000000000000000000000000000000.0'/>
      </restriction>
    </simpleType>

* ``Char`` ->
  ::

    <simpleType name='char'>
      <restriction base='string'>
        <length value='1' fixed='true'/>
      </restriction>
    </simpleType>

* ``Text`` -> ``string`` (UTF-8 is supported through encoding of XML document)

* ``Date`` -> ``date``

* ``Time`` -> ``dateTime`` with appropriate values for constraining facets
  ``(min|max)(Inclusive|Exclusive)``

* ``Bool`` -> ``xs:boolean``, that is, either the string ``true`` or ``false``

* ``Unit`` is represented using standard record notation (see next section)

* Reference: https://www.w3.org/TR/xmlschema-2/#built-in-primitive-datatypes

Defined types
~~~~~~~~~~~~~

Records will be represented using the name of the record
as a tag and then one child tag for each field. For if we have example::

  data Person = { name: Text, age: Int64 }

the value ``Person{ name = "Francesco", age = 27 }`` will be represented
as::

  <Person>
    <name>Francesco</name>
    <age>27</age>
  </Person>

Variants will be represented in the same way, but with only the
chosen constructor appearing, e.g.::

  <Either>
    <Left>True</Left>
  </Either>

or for a compound type::

  <Either>
    <Right>
      <Person>
        <name>Francesco</name>
        <age>26</age>
      </Person>
    </Right>
  </Either>

XML schemas
^^^^^^^^^^^

..
  TODO currently unfinished, the XML schema standard is a bit of a
  monster...

We wish to generate an XML schema https://en.wikipedia.org/wiki/XML_Schema_(W3C)
for each DAML type, so that XML documents deriving from DAML
types can easily be validated, both by humans and by tools (e.g.
databases that support XML).

As mentioned, each primitive type is mapped to the corresponding
XML primitive type, if it exists, or to a string otherwise.

For user-defined data types we have to surmount an additional obstacle:
XML schemas support for generics is not straightforward.

..
  TODO (SM): solve this. Options available

  # Use schema instances https://www.w3.org/TR/xmlschema-0/#UseDerivInInstDocs

  # Unroll the type definition by inlining the type arguments and constraining
    the types further down in the tree. This will probably only work for
    non-recursive types.

Records for schema are genreated as one would expect -- for
some record type::

  data T = { f₁: τ₁, …, fₙ: τₙ }

the following schema will be generated::

  <xs:element name="T">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="f₁" type="⦇τ₁⦈"/>
	 ⋮
        <xs:element name="fₙ" type="⦇τₙ⦈"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>

* FIXME (SM): add ``minOccurs=1`` and ``maxOccurs=1`` limits to elements.
* FIXME (SM): add support for lists.


SQL interface
^^^^^^^^^^^^^

We want to give a way to store contract instances on SQL databases.
For DAML 1.0, we aim to only provide the schema for the contract
instance data, without providing a built-in way to query or index it.

However, we want to be able for the users to shape the data in a way
that is easily and efficiently storable in SQL databases. We are
targeting Postgres, Oracle, and Microsoft SQL server as the supported
databases, with a standard SQL fallback.

Recall that every contract template has a record-typed argument. We
exploit this fact to specify the columns of our SQL table. If we have
contract template with argument type::

  data T = {f₁: τ₁, …, fₙ: τₙ}

it will result in table::

  CREATE TABLE T (
    contract_id bytea PRIMARY KEY,
    f₁ ⦇τ₁⦈ NOT NULL,
     ⋮
    fₙ ⦇τₙ⦈ NOT NULL
  );

..
  TODO (SM): propagate requirement that fields cannot be named ``contract_id`` or
  whatever name is chosen in the end.

Every row in this table will be a contract instance. We choose
to only have one column per top-level record field to leave
the choice of how the table will look like to the user without
requiring any additional annotation in the surface language.

For example, if the user wants a special columnar representation
for its template that is then converted to a richer data type,
maybe because he is constrained by some primitive ORM system
that does not support our encoding of records / variants (see
next sections), he is free to do so by having a record as
the template argument that gets immediately converted
into the richer data type in DAML code.

Given this basic framework, in the next sections we'll give
a sketch on how to convert every serializable DAML type to
a SQL column type.

  * **Comment (SSM)**: How do you plan to support nested records? Perhaps simply
    by flattening the record - but how would you name the columns then?

  * **Comment (FM)**  The flattening is left up to the user. We could
    add some sort of annotation to automate the flattening, but I don't
    think we should do it for DAML 1.0.

Primitive types
~~~~~~~~~~~~~~~

* ``Bool``:

  - PostgreSQL: ``boolean``
  - MS SQL: ``bit``
  - Oracle DB: ``NUMBER(1)`` with ``0/1`` or ``NCHAR(1)`` with ``'F'/'T'``

* ``Int64``:

  - PostgreSQL and MS SQL: ``bigint``
  - Oracle DB: ``NUMBER(19)``

* ``Decimal``:

  - PostgreSQL and MS SQL: ``numeric(38, 10)``
  - Oracle DB: ``NUMBER(38, 10)``

* ``Char``:

  - PostreSQL: ``char(1)`` with ``UTF8`` server-side encoding
  - MS SQL: ``nchar(1)`` or ``nchar(2)``

    .. TODO (MH): Figure out the details of Unicode support (UTF-16 vs. UCS-2).
  - Oracle DB: ``NCHAR(1)`` with national character set ``UTF8``

* ``Text``:

  - PostgreSQL: ``text`` with ``UTF8`` server-side encoding (< 1 GB)
  - MS SQL: ``nvarchar(max)`` (< 1 GB or < 2 GB)

    .. TODO (MH): Figure out the details of Unicode support (UTF-16 vs. UCS-2).
  - Oracle DB: ``NCLOB`` with national character set ``UTF8``

* ``Date``:

  - PostgreSQL: ``date`` (4713 BC - 5874897 AD)
  - MS SQL: ``date`` (1 AD - 9999 AD)
  - Oracle DB: ``DATE`` (4713 BC - 9999 AD)

* ``Time``:

  - PostgreSQL: ``timestamp`` (4713 BC - 294276 AD, 1 μs resolution)
  - MS SQL: ``datetime2`` (1 AD - 9999 AD, 100 ns resolution)
  - Oracle DB: ``TIMESTAMP`` (4713 BC - 9999 AD, 1 ns resolution)

* ``Bool`` gets represented by booleans.

* ``Unit`` is represented as a small int, always 0.

* References:

  - PostgreSQL 9.3 (2013):
    https://www.postgresql.org/docs/9.3/static/datatype.html
  - MS SQL Server 2008 (2008):
    https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
  - Oracle DB 11gR2 (11.2, 2013):
    https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#SQLRF0021

..
  TODO (FM): We need to add Maybe to the prelude / builtin types eventually so that we can have NULL in SQL.

Defined types
~~~~~~~~~~~~~

For user-defined records and variants, we need to step outside
standard SQL. The best widely deployed format to represent
complex data in SQL columns is XML. XML is very well supported
by Postgres, Oracle, and Microsoft SQL server since the mid-2000s:

* Postgres 8.2. See
  https://www.postgresql.org/docs/8.3/static/datatype-xml.html
  and https://www.postgresql.org/docs/8.3/static/functions-xml.html .
  The support for storing and querying is quite thorough since
  its introduction, although it does not support XML schemas.
* At least as far 9.0.1 (2003-12): http://www.oracle.com/pls/db901/db901.show_toc?partno=a88894&remark=drilldown&word=xmltype
  The docs for that version are broken, but the support is very
  thorough including XPath queries and validation of content
  against XML schemas.
* MSSQL: SQL Server 2005, see
  https://technet.microsoft.com/en-us/library/ms345117(v=sql.90).aspx .
  The support is thorough, you can query XML and have columns
  that contained untyped ``xml`` or ``xml`` which is checked
  against a schema.
* MySQL: Not supported.

Note that the SQL language for these databases supports complex
XML queries, which means that people can easily build infrastructure
on it (indices, notification channels, etc.).

Also note that both Oracle and Microsoft SQL server support
XML schemas, which means that we can generate XML schemas for
tables containing columns with records and / or variants and have
the database ensure the consistency of our data.

For example, if we have record::

  data Dummy = {field1: Either Bool Text, field2: List Int64}

we will get postgres table::

  CREATE TABLE Dummy (
    contract_id uuid PRIMARY KEY,
    field1 xml NOT NULL,
    field2 xml NOT NULL
  );

where each of ``field1`` and ``field2`` will be filled in with the XML
representation of ``Either Bool Text`` and ``List Int64``,
respectively.

Similarly, we can use the JSON encoding for databases that support
it:

* Postgres 9.2 (2012-09) introduced the ``json`` data type, which
  is just a checked ``text``, 9.3 (2013-09) introduced accessing
  functions on json data, 9.4 (2014-12) introduced ``jsonb``
  which is a binary encoding of JSON data allowing much quicker
  retrieval of nested info. See
  https://www.postgresql.org/docs/10/static/datatype-json.html .
  Queries on JSON data are supported, see
  https://www.postgresql.org/docs/10/static/functions-json.html .
* Oracle Database 12c (2013-06) introduced
  https://docs.oracle.com/database/121/ADXDB/json.htm . JSON
  can be stored and queried using "path expressions", see
  https://docs.oracle.com/database/121/ADXDB/json.htm#ADXDB6253 .
* MSSQL: SQL Server 2016 (2016-06) supports JSON columns
  and querying, see https://docs.microsoft.com/en-us/sql/relational-databases/json/json-data-sql-server .
* MySQL: MySQL 5.7.8 (2017-08) supports JSON columns and querying,
  see https://dev.mysql.com/doc/relnotes/mysql/5.7/en/news-5-7-8.html#mysqld-5-7-8-json .

For databases that do not support any document format such as JSON or
XML, we have to fall back to ``text`` or ``bytea`` containing XML/JSON
to store user-defined formats.


Primitive types
^^^^^^^^^^^^^^^

* ``Int64``: Integers between ``-9,223,372,036,854,775,808`` and
  ``9,223,372,036,854,775,807`` (inclusive), i.e., 64-bit integers. Arithmetic
  operations raise an error on overflows and division by ``0``.

* ``Decimal``: Decimal numbers with precision 38 and scale 10, i.e., all numbers
  of the form ``x / 10^10`` where ``x`` is an integer with ``|x| < 10^38``.
  Arithmetic operations raise an error on overflows and division by ``0``.
  Multiplication and division might require rounding. All operations involving
  rounding use the rule "round to nearest, ties to even" (also knwon as
  *bankers' rounding*).

  .. note:: The precision of 38 is the maximal number of digits that fit into
            128 bits. Although we intend to make the scale variable in the
            future by having a type à la ``Decimal(38, s)`` for each ``0 ≤ s ≤
            38``, this is beyond scope for DAML-LF 1.0. Thus, we have chosen
            to go with a fixed scale for now. The exact value of the scale is a
            matter of debate. We propose a value of 10 after taking the
            following facts into account:

            * As of today (2017-11-10), a scale of 10 is necessary to express
              the exchange rate of the currency pair ``IRR/GBP`` (``1 IRR =
              0.0000215260 GBP``) with 6 significant digits. (The exchange rate
              of this currency pair is minimal, to my best knowledge.)

            * For Bitcoin, a scale of at least 8 is necessary.

            * For expressing time intervals in seconds and with a resolution of
              ``1 μ`` or ``1ns``, scales of at least 6 or 9, respectively, are
              necessary.

            * The gross world product (GWP) of 2016 expressed in ``IRR`` is in
              the order of ``3 * 10^18``. With a scale of 10, ``Decimal`` can
              accomodate numbers up to ``10^28``.

            The rule "round to nearest, ties to even" is what the IEEE 754
            standard defines as the default for floating-point and recommends as
            default for decimal. If required, we could parametrize all primitive
            operations involving rounding by a rounding mode rather than assume
            this default.

  * **Comment (SSM)**: I thought you were going for a .NET Decimal inspired
    type?

  * **Comment (MH)**: The .NET inspired type is neither compatible with MS SQL
    nor with Oracle DB. That's why we've decided to go for a fixed scale.
    However, the exact scale is definitely up to debate. See **Note** above.

  * **Comment (SSM)**: You could make the decimal type parametric with the
    scale. Regardsless it is simply not true that Oracle/MSSL can't represent any
    possible value of .NET Decimal. Oracles datatype NUMBER is for example
    more than sufficient.

  * **Comment (MH):** I have missed Oracle's ``NUMBER`` type (without precision
    or scale). I'm sorry about that. In MS SQL Server, ``numeric`` is just a
    synonym for ``numeric(18, 0)``, however. (I've double checked.) So, the
    problem persists.

    As I've mentioned above, we ultimately want to parametrize ``Decimal`` by
    it's precision and scale. We should probably put that back on the table for
    DAML-LF 1.0 again.

    @FM: Any thoughts on this?

  * **Comment (SSM)**: Ahh - OK. My bad. C#'s decimal is good being able to
    represent a wide variety of db types. But we need the logic to go the
    other way - a more narrow type that can be represented on a wide variety
    of databases. I think best bet is to make decimal parametric at least on
    scale.

    A much more narrow type is the MS MONEY type. Which is simply just a 64
    bit signed integer with an implicit scale of 10^-4.

* ``Char``: Unicode characters, including all code points, even those beyond the
  basic multilingual plane.

  .. note:: We might need to restrict the actual character set due to security
            concerns. In particular, some real world characters are assigned to
            multiple code points. This could be abused, e.g., for phishing.


* ``Text``: Unicode strings consisting of all characters admissible for
  ``Char``.

  .. note:: We do not give a bound on the length on purpose. The limiting factor
            will be the memory of the systems involved. We must address this
            issue when we design a model for bounding the resource consumption
            by DAML contracts.

            Note though that PostgreSQL and MS SQL Server impose a bound of
            approximately ``1 GB`` on UTF-8 encoded Unicode strings.

* ``Date``: Dates between ``0001-01-01`` and ``9999-12-31`` (both inclusive and
  AD) in the (proleptic) Gregorian calendar.

  .. note:: This choice follows the ISO 8601 standard. The upper and lower
            bounds are borrowed from MS SQL Server.

            Note that the ISO 8601 standard does not automatically allow the
            years ``0001`` to ``1582`` but only after mutual agreement.

* ``Time``: Timestamps between ``0001-01-01 00:00:00.000000Z`` and ``9999-12-31
  23:59:59.999999Z``. The date part is as in ``Date``. The resolution is ``1
  μs``. Time zones are not taken into account, all data is interpreted as UTC.
  Leap seconds are not taken taken into account, i.e., ``60`` is not a valid
  value for the second field. This issue must be resolved external to the DAML
  runtime, e.g., by using a "leap smear".

  .. note:: The upper and lower bound and the resolution are chosen such that we
            get the most expressive timestamp type which can natively be stored
            in PostgreSQL, MS SQL Server and Oracle DB.

            The MiFID II regulation, which starts to apply in Europe in 2018,
            requires timestamping with a resolution of ``100 μs``. We still need
            to investigate other regulations in other markets. However, in light
            of how recent MiFID II is, ``1 μs`` seems to be good enough from a
            regulatory perspective.

            To my best knowledge, HFT firms are currently able to take orders
            within about ``10 μs`` after the tick appeared on the tape. Light
            can travel ``300 m`` within ``1 μs``.

            The ASX has used "leap smears" in the past:
            http://www.asx.com.au/communications/notices/2015/0582.15.05.pdf
            Google's public NTP servers use it as well:
            https://developers.google.com/time/smear

* ``Party``: Unicode string representing a party.

  .. TODO(MH): Figure out which characters are admissible. The security concerns for ``Char`` apply as well.

Primitive Literals
^^^^^^^^^^^^^^^^^^

* ``Int64``: Decimal presentation without leading zeros and an optional ``-``
  sign.

* ``Decimal``: Decimal representation with 1 to 10 digits after the decimal
  point and no leading zeros, except for numbers between -1.0 and 1.0. Trailing
  zeros are allowed. Thus, ``Decimal`` literals are exactly those strings
  matching the regex::

    -?([1-9][0-9]{0,27}|0)\.[0-9]{1,10}

* ``Char``: TODO(MH) when it is clarified in the surface language.

* ``Text``: TODO(MH) when it is clarified in the surface language.

* ``Date``: Calendar dates in ISO 8601 extended format ``YYYY-MM-DD``. The
  dashes are mandatory. (Week dates and ordinal dates are not allowed.)

* ``Time``: Timestamps in ISO 8601 extended format
  ``YYYY-MM-DDThh:mm:ss.μμμμμμZ`` with the following restrictions: Seconds and
  fractional seconds can be omitted, minutes must be present. The number of
  fractional digits of the seconds part must not exceed 6. If there are no
  fractional seconds, the decimal point must be omitted as well. Fractional
  minutes are not allowed. The dashes, colons, the separator ``T`` and time zone
  designator ``Z`` are mandatory. The hour part ``hh`` must not be 24.

* ``Party``: TODO(MH) when it is clarified in the surface language.

* References:

  - ISO 8601 Standard: https://en.wikipedia.org/wiki/ISO_8601

Primitive Operations
^^^^^^^^^^^^^^^^^^^^

* ``Int64``:

  - ``int64Plus, int64Minus, int64Times : Int64 -> Int64 -> Int64``: Standard
    arithmetic operations, raise an error on overflow.

  - ``int64DivMod : Int64 -> Int64 -> Pair Int64``: Integer division with
    remainder. ``int64DivMod x y`` is the unique pair of integers ``(q, r)``
    such that ``x = q * y + r`` and ``0 ≤ r < |y|``. Raise an error if ``y =
    0``.

  - ``int64MinBound, int64MaxBound : Int64``: Minimal and maximal element of
    ``Int64``, i.e., ``-2⁶³`` and ``2⁶³-1``, respectively.


  - ``int64Cmp : Int64 -> Int64 -> Cmp``: Standard comparison.

  - ``int64ToText : Int64 -> Text``: Conversion to text.

* ``Decimal``:

  - ``decimalPlus, decimalMinus, decimalTimes, decimalDiv : Decimal -> Decimal ->
    Decimal``: Standard arithmetic operations, raise an error on overflow and
    division by zero. ``decimalTime`` and ``decimalDiv`` use the default
    rounding for ``Decimal`` (bankers' rounding).

  - ``decimalRound, decimalFloor, decimalCeil : Int64 -> Decimal -> Decimal``:
    For ``0 ≤ k ≤ 10``, ``decimal{Round|Floor|Ceil} k d`` round ``d`` to ``k``
    digits after the decimal point. All three functions raise and error on
    overflow or if ``k < 0`` or ``k > 10``. The following rounding modes are
    used:

    + ``decimalRound``: round to nearest, ties to even.

    + ``decimalFloor``: round towards minus infinity, i.e.::

        decimalFloor k d = ⌊10ᵏ * d⌋ / 10ᵏ

    + ``decimalCeil``: round towards plus infinity, i.e.::

        decimalCeil k d = ⌈10ᵏ * d⌉ / 10ᵏ

  - ``decimalMinBound, decimalMaxBound : Decimal``: Minimal and maximal element
    of ``Decimal``, i.e., ``-10²⁸ + 10⁻¹⁰`` and ``10²⁸ - 10⁻¹⁰``, respectively.

  - ``decimalCmp : Decimal -> Decimal -> Cmp``: Standard comparison.

  - ``decimalToText : Decimal -> Text``: Conversion to text with all 10 digits
    after the decimal point spelled out.

    .. note:: Conversions to text with a limited number of digits after the
              decimal point can be implemented in a library in terms of
              ``decimalRound`` and ``textSlice``.

* Conversion between ``Int64`` and ``Decimal``:

  - ``int64ToDecimal : Int64 -> Decimal``: Convert an integer into a decimal.
    Overflows cannot happen.

  - ``decimalToInt64 : Decimal -> Int64``: Round decimal to nearest integer
    using bankers' rounding and convert to an ``Int64``. Raise an error on
    overflow. Potential overflows can be detected by comparison with
    ``int64ToDecimal int64MixBound`` and ``int64ToDecimal int64MaxBound``.

* ``Char``:

  - ``charToCodePoint : Char -> Int64``: Convert character to its Unicode code
    point.

  - ``charFromCodePoint : Int64 -> Maybe Char``: Create a character from its
    Unicode code point.

  - ``charToText : Char -> Text``: Turn character into string of length 1.

  - ``charEq :: Char -> Char -> Bool``: Test for equality in terms of the code
    point.

  .. note:: Ordering of characters can depend on external reference data and is
            hence not provided here. A simple comparison of Unicode code points
            can be implemented in a library in terms of ``charToCodePoint`` and
            ``int64Cmp``.

* ``Text``:

  - ``textAppend : Text -> Text -> Text``: Append the two strings.

  - ``textLength :: Text -> Int64``: Length of the string in characters.

  - ``textAt :: Text -> Int64 -> Char``: Get the character at the given index.
    Indices are zero based. Raises an error if the index is out of bounds.

  - ``textSlice :: Text -> Int64 -> Int64 -> Text``: ``textSlice s i j`` returns
    the substring of ``s`` which starts at index ``i`` and has length ``j - i``.
    In particular, ``textSlice s 0 (textLength s) = s``. Raises an error if the
    condition ``0 ≤ i ≤ j ≤ textLength s`` is violated.

  - ``textToListOfChar : Text -> List Char``: Convert string to list of
    characters.

  - ``textFromListOfChar : List Char -> Text``: Create string from list of
    characters.

  - ``textEq : Text -> Text -> Bool``: Test if the strings are the same
    character by character in terms of the respective code points.

  - ``textCmp : (Char -> Char -> Cmp) -> Text -> Text -> Cmp``: Lexicographic
    comparison wrt. the given comparison function for characters.

    .. note:: The comparison of characters is a parameter as it might depend on
              external reference data.

* ``Date``:

  - ``dateToDaysSinceEpoch : Date -> Int64``: Convert a ``Date`` to the number
    of days since the Unix epoch date ``1970-01-01``. Overflows cannot happen.

  - ``dateFromDaysSinceEpoch : Int64 -> Date``: Create a ``Date`` from the
    number of days since the Unix epoch date ``1970-01-01``. Raises an error on
    overflows. Potential overflows can be detected by comparison with
    ``dateToDaysSinceEpoch dateMinBound`` and ``dateToDaysSinceEpoch``.

  - ``dateMinBound, dateMaxBound : Date``: Minimal and maximal element of
    ``Date``, i.e., ``0001-01-01`` and ``9999-12-31``, respectively.

  - ``dateToText : Date -> Text``: Conversion to text in ISO 8601 extended format.

  .. note:: Functions like ``datePlus : Date -> Int64 -> Date``, ``dateMinus :
            Date -> Date -> Int64`` and ``dateCmp : Date -> Date -> Cmp`` can be
            implemented in a library in terms of ``dateToDaysSinceEpoch`` and
            ``dateFromDaysSinceEpoch`` and operations on ``Int64``.

* ``Time``:

  - ``timeToSecondsSinceEpoch : Time -> Decimal``: Convert a ``Time`` to Unix
    time, i.e., the number of seconds since ``1970-01-01 00:00Z``. Overflows
    cannot happen.

  - ``timeFromSecondsSinceEpoch : Decimal -> Time``: Create a ``Time`` from Unix
    time, i.e., the number of seconds since ``1970-01-01 00:00Z``. Raises an
    error on overflows. Potential overflows can be detected by comparison with
    ``timeToSecondsSinceEpoch timeMinBound`` and ``timeToSecondsSinceEpoch
    timeMaxBound``.

  - ``timeMinBound, timeMaxBound : Time``: Minimal and maximal element of
    ``Time``, i.e., ``0001-01-01T00:00:00.000000Z`` and
    ``9999-31-12T23:59:59.999999Z``, respectively.

  - ``timeToText : Time -> Text``: Conversion to text in ISO 8601 extended format.

  .. note:: Functions like ``timePlus : Time -> Decimal -> Time``, ``timeMinus :
            Time -> Time -> Decimal``, where the ``Decimal`` represents a
            duration in terms of some fixed unit of time like one second, and
            ``timeCmp : Time -> Time -> Cmp`` can be implemented in a library in
            terms of ``timeToSecondsSinceEpoch``, ``timeFromSecondsSinceEpoch``
            and operations on ``Decimal``.

* ``Party``:

  - ``partyEq : Party -> Party -> Bool``: Equality character by character.

  - ``partyToText : Party -> Text``: Conversion to text.

* ``ContractId a``:

  - ``contractIdEq : ContractId a -> ContractId a -> Bool``: Equality.

    .. TODO(MH @GM): Do we want this?
