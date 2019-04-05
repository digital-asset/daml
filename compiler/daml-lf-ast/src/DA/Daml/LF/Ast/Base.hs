-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# LANGUAGE TypeFamilies #-}
-- | Types and pretty-printer for the AST of the DAML Ledger Fragment.
module DA.Daml.LF.Ast.Base where

import DA.Prelude

import           Control.DeepSeq
import           Control.Lens
import           Data.Aeson
import qualified Data.NameMap as NM
import qualified Data.Text          as T
import Data.Fixed

import           DA.Daml.LF.Ast.Version

infixr 1 `KArrow`

-- | Identifier for a package. Will be obtained by hashing the contents of the
-- package. Must match the regex
--
-- > [a-zA-Z0-9]+
type PackageId = Tagged PackageIdTag T.Text
data PackageIdTag
deriving instance Data PackageIdTag

-- | Name for a module. Must match the regex
--
-- > ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*
type ModuleName = Tagged ModuleNameTag [T.Text]
data ModuleNameTag
deriving instance Data ModuleNameTag

-- | Name for a type constructor. Must match the regex
--
-- > ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*
type TypeConName = Tagged TypeConNameTag [T.Text]
data TypeConNameTag
deriving instance Data TypeConNameTag

-- | Name for a record field. Must match the regex
--
-- > [a-z][a-zA-Z0-9_]*
type FieldName = Tagged FieldNameTag T.Text
data FieldNameTag
deriving instance Data FieldNameTag

-- | Name for a variant constructor. Must match the regex
--
-- > [A-Z][a-zA-Z0-9_]*
type VariantConName = Tagged VariantConNameTag T.Text
data VariantConNameTag
deriving instance Data VariantConNameTag

-- | Name for the choice of a contract. Must match the regex
--
-- > [A-Z][a-zA-Z0-9_]*
type ChoiceName = Tagged ChoiceNameTag T.Text
data ChoiceNameTag
deriving instance Data ChoiceNameTag

-- | Name for a type variable. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
type TypeVarName = Tagged TypeVarNameTag T.Text
data TypeVarNameTag
deriving instance Data TypeVarNameTag

-- | Name for a local expression variable, bound in an expression,
--   and used locally. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
type ExprVarName = Tagged ExprVarNameTag T.Text
data ExprVarNameTag
deriving instance Data ExprVarNameTag

-- | Name for an global expression variable, bound at the declaration level,
--   and used in this and other modules. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
type ExprValName = Tagged ExprValNameTag T.Text
data ExprValNameTag
deriving instance Data ExprValNameTag

-- | Identifier referring to a contract. Will be obtained by some mechanism for
-- generating unique identifiers, like content hashing. Must match the regex
--
-- > #[a-zA-Z0-9]+
type ContractIdLiteral = Tagged ContractIdLiteralTag T.Text
data ContractIdLiteralTag
deriving instance Data ContractIdLiteralTag

-- | Literal representing a party.
type PartyLiteral = Tagged PartyLiteralTag T.Text
data PartyLiteralTag
deriving instance Data PartyLiteralTag

-- | Reference to a package.
data PackageRef
  = PRSelf
    -- ^ Reference to the package being currently handled.
  | PRImport !PackageId
    -- ^ Reference to the package with the given id.
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Something qualified by a package and a module within that package.
data Qualified a = Qualified
  { qualPackage :: !PackageRef
  , qualModule  :: !ModuleName
  , qualObject  :: !a
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Source location annotation.
data SourceLoc = SourceLoc
  { slocModuleRef :: !(Maybe (PackageRef, ModuleName))
    -- ^ Optional reference to another module. Used when
    -- an expression is inlined from another module.
  , slocStartLine :: !Int
    -- ^ 0-indexed starting line of the source span.
  , slocStartCol :: !Int
  , slocEndLine :: !Int
  , slocEndCol :: !Int
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Kinds.
data Kind
  = KStar
  | KArrow Kind Kind
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Enumeration types like Bool and Unit.
data EnumType
  = ETUnit
  | ETBool
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Builtin type.
data BuiltinType
  = BTInt64
  | BTDecimal
  | BTText
  | BTTimestamp
  | BTDate
  | BTParty
  | BTEnum EnumType
  | BTList
  | BTUpdate
  | BTScenario
  | BTContractId
  | BTOptional
  | BTMap
  | BTArrow
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Type as used in typed binders.
data Type
  -- | Reference to a type variable.
  = TVar        !TypeVarName
  -- | Reference to a type constructor.
  | TCon        !(Qualified TypeConName)
  -- | Application of a type function to a type.
  | TApp        !Type !Type
  -- | Builtin type.
  | TBuiltin    !BuiltinType
  -- | Function type.
  -- | Universal quantified type. Is the result of a type abstraction.
  | TForall
    { forallBinder :: !(TypeVarName, Kind)
      -- ^ Type variable introduced by the type abstraction.
    , forallBody   :: !Type
      -- ^ Type of the body of the type abstraction.
    }
  -- | Type for tuples aka structural records. Parameterized by the names of the
  -- fields and their types.
  | TTuple      ![(FieldName, Type)]
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Fully applied qualified type constructor.
data TypeConApp = TypeConApp
  { tcaTypeCon :: !(Qualified TypeConName)
    -- ^ Qualified name of the type constructor.
  , tcaArgs    :: ![Type]
    -- ^ Type arguments which are applied to the type constructor.
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Constructors of builtin 'EnumType's.
data EnumCon
  = ECUnit   -- ∷ Unit
  | ECFalse  -- ∷ Bool
  | ECTrue   -- ∷ Bool
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

data E10
instance HasResolution E10 where
  resolution _ = 10000000000 -- 10^-10 resolution

-- | Builtin operation or literal.
data BuiltinExpr
  -- Literals
  = BEInt64      !Int64          -- :: Int64
  | BEDecimal    !(Fixed E10)    -- :: Decimal, precision 38, scale 10
  | BEText       !T.Text         -- :: Text
  | BETimestamp  !Int64          -- :: Timestamp, microseconds since unix epoch
  | BEParty      !PartyLiteral   -- :: Party
  | BEDate       !Int32          -- :: Date, days since unix epoch
  | BEEnumCon    !EnumCon        -- see 'EnumCon' above

  -- Polymorphic functions
  | BEError                      -- :: ∀a. Text -> a
  | BEEqual      !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BELess       !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BELessEq     !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEGreaterEq  !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEGreater    !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEToText     !BuiltinType    -- :: t -> Text, where t is one of the builtin types
                                 -- {Int64, Decimal, Text, Timestamp, Date, Party}
  | BEPartyFromText              -- :: Text -> Optional Party
  | BEPartyToQuotedText          -- :: Party -> Text

  -- Decimal arithmetic
  | BEAddDecimal                 -- :: Decimal -> Decimal -> Decimal, crashes on overflow
  | BESubDecimal                 -- :: Decimal -> Decimal -> Decimal, crashes on overflow
  | BEMulDecimal                 -- :: Decimal -> Decimal -> Decimal, crashes on overflow and underflow, automatically rounds to even (see <https://en.wikipedia.org/wiki/Rounding#Round_half_to_even>)
  | BEDivDecimal                 -- :: Decimal -> Decimal -> Decimal, automatically rounds to even, crashes on divisor = 0 and on overflow
  | BERoundDecimal               -- :: Int64 -> Decimal -> Decimal, the Int64 is the required scale. Note that this doesn't modify the scale of the type itself, it just zeroes things outside that scale out. Can be negative. Crashes if the scale is > 10 or < -27.

  -- Integer arithmetic
  | BEAddInt64                 -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BESubInt64                 -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BEMulInt64                 -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BEDivInt64                 -- :: Int64 -> Int64 -> Int64, crashes on divisor = 0
  | BEModInt64                 -- :: Int64 -> Int64 -> Int64, crashes on divisor = 0
  | BEExpInt64                 -- :: Int64 -> Int64 -> Int64, crashes on overflow

  -- Numerical conversion
  | BEInt64ToDecimal           -- :: Int64 -> Decimal, always succeeds since 10^28 > 2^63
  | BEDecimalToInt64           -- :: Decimal -> Int64, only converts the whole part, crashes if it doesn't fit

  -- Time conversion
  | BETimestampToUnixMicroseconds -- :: Timestamp -> Int64, in microseconds
  | BEUnixMicrosecondsToTimestamp -- :: Int64 -> Timestamp, in microseconds
  | BEDateToUnixDays              -- :: Date -> Int64, in microseconds
  | BEUnixDaysToDate              -- :: Int64 -> Date, in microseconds

  -- List operations
  | BEFoldl                      -- :: ∀a b. (b -> a -> b) -> b -> List a -> b
  | BEFoldr                      -- :: ∀a b. (a -> b -> b) -> b -> List a -> b
  | BEEqualList                  -- :: ∀a. (a -> a -> Bool) -> List a -> List a -> Bool

  | BEMapEmpty                    -- :: ∀ a. Map a
  | BEMapInsert                   -- :: ∀ a. Text -> a -> Map a -> Map a
  | BEMapLookup                   -- :: ∀ a. Text -> Map a -> Optional a
  | BEMapDelete                   -- :: ∀ a. Text -> Map a -> Map a
  | BEMapToList                   -- :: ∀ a. Map a -> List ⟨key: Text, value: a⟩
  | BEMapSize                     -- :: ∀ a. Map a -> Int64

  -- Text operations
  | BEExplodeText                -- :: Text -> List Text
  | BEAppendText                 -- :: Text -> Text -> Text
  | BEImplodeText                -- :: List Text -> Text
  | BESha256Text                 -- :: Text -> Text

  | BETrace                      -- :: forall a. Text -> a -> a
  | BEEqualContractId            -- :: forall a. ContractId a -> ContractId a -> Bool
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)


data Binding = Binding
  { bindingBinder :: !(ExprVarName, Type)
  , bindingBound  :: !Expr
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Expression.
data Expr
  -- | Reference to an expression variable.
  = EVar  !ExprVarName
  -- | Reference to a value definition.
  | EVal  !(Qualified ExprValName)
  -- | Builtin operation or literal.
  | EBuiltin !BuiltinExpr
  -- | Record construction.
  | ERecCon
    { recTypeCon :: !TypeConApp
      -- ^ Applied type constructor of the record type.
    , recFields  :: ![(FieldName, Expr)]
      -- ^ Fields togehter with the expressions to assign to them.
    }
  -- | Record projection.
  | ERecProj
    { recTypeCon :: !TypeConApp
      -- ^ Applied type constructor of the record type.
    , recField   :: !FieldName
      -- ^ Field to project to.
    , recExpr    :: !Expr
      -- ^ Expression to project from.
    }
  -- | Non-destructuve record update.
  | ERecUpd
    { recTypeCon :: !TypeConApp
      -- ^ Applied type constructor of the record type.
    , recField :: !FieldName
      -- ^ Field to update.
    , recExpr :: !Expr
      -- ^ Expression to update the field in.
    , recUpdate :: !Expr
      -- ^ Expression to update the field with.
    }
  -- | Variant construction.
  | EVariantCon
    { varTypeCon :: !TypeConApp
      -- ^ Applied type constructor of the variant type.
    , varVariant :: !VariantConName
      -- ^ Data constructor of the variant type.
    , varArg     :: !Expr
      -- ^ Argument to the data constructor.
    }
    -- TODO(MH): Move 'EEVariantCon' into 'BuiltinExpr' if we decide to allow
    -- using variant constructors as functions that can be around not applied.
  -- | Tuple construction.
  | ETupleCon
    { tupFields :: ![(FieldName, Expr)]
      -- ^ Fields together with the expressions to assign to them.
    }
  -- | Tuple projection.
  | ETupleProj
    { tupField :: !FieldName
      -- ^ Field to project to.
    , tupExpr  :: !Expr
      -- ^ Expression to project from.
    }
  -- | Non-destructive tuple update.
  | ETupleUpd
    { tupField :: !FieldName
      -- ^ Field to update.
    , tupExpr :: !Expr
      -- ^ Expression to update the field in.
    , tupUpdate :: !Expr
      -- ^ Expression to update the field with.
    }
  -- | (Expression) application.
  | ETmApp
    { tmappFun :: !Expr
      -- ^ Function to apply.
    , tmappArg :: !Expr
      -- ^ Argument to apply function on.
    }
  -- | Type application.
  | ETyApp
    { tyappExpr :: !Expr
      -- ^ Expression to apply.
    , tyappType :: !Type
      -- ^ Type to apply expression on.
    }
  -- | (Expression) abstraction (aka small lambda).
  | ETmLam
    { tmlamBinder :: !(ExprVarName, Type)
      -- ^ Variable to abstract.
    , tmlamBody   :: !Expr
      -- ^ Expression to abstract from.
    }
  -- | Type abstraction (aka capital lambda).
  | ETyLam
    { tylamBinder :: !(TypeVarName, Kind)
      -- ^ Type variable to abstract.
    , tylamBody   :: !Expr
      -- ^ Expression to abstract from.
    }
  -- | Pattern matching.
  | ECase
    { casScrutinee    :: !Expr
      -- ^ Expression to match on.

    -- TODO(MH): It would be nice to have this binder, but currently it's in
    -- the way of a quick translation from the renamer AST.
    -- , casBinder       :: !ExprVarName
    --  -- ^ Variable to bind the scrutinee to.
    , casAlternatives :: ![CaseAlternative]
      -- ^ Alternatives.
    }
  -- | Let binding.
  | ELet
    { letBinding :: !Binding
      -- ^ Binding.
    , letBody    :: !Expr
      -- ^ Expression to bind variable in.
    }
  -- | Concstruct empty list.
  | ENil
    -- TODO(MH): When we move 'ECons' to 'BuiltinExpr' or remove it entirely,
    -- do the same to 'ENil'.
    { nilType :: !Type
      -- ^ Element type of the list.
    }
  -- | Construct list from head and tail.s
  | ECons
    -- TODO(MH): Move 'ECons' into 'BuiltinExpr' if we decide to allow using
    -- it as a function that can be passed around not fully applied.
    -- OR: Remove 'ECons' entirely if we allow for recursive data types.
    { consType :: !Type
      -- ^ Element type of the list.
    , consHead :: !Expr
      -- ^ Head of the list.
    , consTail :: !Expr
      -- ^ Tail of the list.
    }
  | ESome
    { someType :: !Type
    , someBody :: !Expr
    }
  | ENone
    { noneType :: !Type
    }
  -- | Update expression.
  | EUpdate !Update
  -- | Scenario expression.
  | EScenario !Scenario
  -- | An expression annotated with a source location.
  | ELocation !SourceLoc !Expr
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Pattern matching alternative.
data CaseAlternative = CaseAlternative
  { altPattern :: !CasePattern
    -- ^ Pattern to match on.
  , altExpr    :: !Expr
    -- ^ Expression to evaluate in case of a match.
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

data CasePattern
  -- | Match on constructor of variant type.
  = CPVariant
    { patTypeCon :: !(Qualified TypeConName)
      -- ^ Type constructor of the type to match on.
    , patVariant :: !VariantConName
      -- ^ Variant constructor to match on.
    , patBinder  :: !ExprVarName
      -- ^ Variable to bind the variant constructor argument to.
    }
  | CPEnumCon !EnumCon
  -- | Match on empty list.
  | CPNil
  -- | Match on head and tail of non-empty list.
  | CPCons
    { patHeadBinder :: !ExprVarName
      -- ^ Variable to bind the head of the list to.
    , patTailBinder :: !ExprVarName
      -- ^ Variable to bind the tail of the list to.
    }
  | CPNone
  | CPSome
    { patBodyBinder :: !ExprVarName
    }
  -- | Match on anything. Should be the last alternative. Also note that 'ECase'
  -- bind the value of the scrutinee to a variable.
  | CPDefault
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Expression in the update monad.
data Update
  = UPure
    { pureType :: !Type
    , pureExpr :: !Expr
    }
    -- TODO(MH): Move 'UPure' to 'BuiltinExpr' when we decide to make it a
    -- proper function, potentially as part of an 'Applicative' or 'Monad' type
    -- class.
  -- | Bind in the update monad.
  | UBind
    { bindBinding :: !Binding
      -- ^ Variable and the expression to bind.
    , bindBody   :: !Expr
    }
    -- TODO(MH): Move 'UBind' to 'BuiltinExpr' when we defice to make it a
    -- proper function, porentially as part of a 'Monad' type class.
  -- | Create contract template instance.
  | UCreate
    { creTemplate :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , creArg      :: !Expr
      -- ^ Argument for the contract template.
    }
  -- | Exercise choice on a cotract template instance.
  | UExercise
    { exeTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeContractId :: !Expr
      -- ^ Contract id of the contract template instance to exercise choice on.
    , exeActors     :: !Expr
      -- ^ Parties exercising the choice.
    , exeArg        :: !Expr
      -- ^ Argument for the choice.
    }
  -- | Retrieve the argument of an existing contract template instance.
  | UFetch
    { fetTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , fetContractId :: !Expr
      -- ^ Contract id of the contract template instance whose argument shall be
      -- retrieved.
    }
  -- | Retrieve effective ledger time.
  | UGetTime
  -- | See comment for 'SEmbedExpr'
  | UEmbedExpr
    { updateEmbedType :: !Type
    , updateEmbedBody :: !Expr
    }
  | ULookupByKey !RetrieveByKey
  | UFetchByKey !RetrieveByKey
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Expression in the scenario monad
data Scenario
  = SPure
    { spureType :: !Type
    , spureExpr :: !Expr
    }
  -- Bind in the scenario monad
  | SBind
    { sbindBinding :: !Binding
      -- ^ Variable and the expression to bind.
    , sbindBody :: !Expr
    }
  -- | Commit an update action to the ledger.
  | SCommit
    { scommitType :: !Type
    -- ^ Type of the update to commit.
    , scommitParty :: !Expr
    -- ^ The committing party.
    , scommitExpr :: !Expr
    -- ^ The expression that yields the update action.
    }
  -- | A commit to the ledger that is expected to fail.
  | SMustFailAt
    { smustFailAtType :: !Type
    -- ^ Type of the update to commit.
    , smustFailAtParty :: !Expr
    -- ^ The committing party.
    , smustFailAtExpr :: !Expr
    -- ^ The expression that yields the update action.
    }
  -- | Move the time forward.
  | SPass
    { spassDelta :: !Expr
    -- ^ Amount of time to move forward.
    }
  | SGetTime
  -- Get a party given by its name. Given the same name twice it returns the
  -- same party.
  | SGetParty
    { sgetPartyName :: !Expr
    -- ^ Name of the party to get. This is an expression of type `Text`.
    }
  -- Wrap an expression of type Scenario. Operationally equivalent to:
  --
  -- sembed_expr x === do () <- return (); x
  --
  -- but the optimiser won't reduce it back to x.
  --
  -- Used to wrap top-level scenario values, ensuring that any expression
  -- generating the scenario value is also run on each scenario execution.
  -- e.g.
  --
  -- def test : Scenario Unit = if <blah> then <this> else <that>
  --
  -- Without the wrapping the `if` will run before the scenario. With the
  -- wrapping the `if` is run every execution -- as expected. Particularly
  -- useful for scenarios that call error.
  | SEmbedExpr
    { scenarioEmbedType :: !Type
    , scenarioEmbedExpr :: !Expr
    }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

data RetrieveByKey = RetrieveByKey
  { retrieveByKeyTemplate :: !(Qualified TypeConName)
  , retrieveByKeyKey :: !Expr
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

newtype IsSerializable = IsSerializable{getIsSerializable :: Bool}
  deriving stock (Eq, Generic, Ord, Show)
  deriving anyclass (NFData, ToJSON)

-- | Definition of a data type.
data DefDataType = DefDataType
  { dataLocation :: !(Maybe SourceLoc)
    -- ^ Location of the definition in the source file.
  , dataTypeCon :: !TypeConName
    -- ^ Name of the type constructor.
  , dataSerializable :: !IsSerializable
    -- ^ The data type preserves serializabillity.
  , dataParams  :: ![(TypeVarName, Kind)]
    -- ^ Type paramaters to the type constructor.
  , dataCons    :: !DataCons
    -- ^ Data constructor of the type.
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Data constructors for a data type definition.
data DataCons
  -- | A record type given by its field names and their types.
  = DataRecord  ![(FieldName, Type)]
  -- | A variant type given by its construtors and their payload types.
  | DataVariant ![(VariantConName, Type)]
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

newtype HasNoPartyLiterals = HasNoPartyLiterals{getHasNoPartyLiterals :: Bool}
  deriving stock (Eq, Generic, Ord, Show)
  deriving anyclass (NFData, ToJSON)

newtype IsTest = IsTest{getIsTest :: Bool}
  deriving stock (Eq, Generic, Ord, Show)
  deriving anyclass (NFData, ToJSON)

-- | Definition of a value.
data DefValue = DefValue
  { dvalLocation :: !(Maybe SourceLoc)
    -- ^ Location of the definition in the source file.
  , dvalBinder :: !(ExprValName, Type)
    -- ^ Name to bind the value to together with its type.
  , dvalNoPartyLiterals :: !HasNoPartyLiterals
    -- ^ If 'True', the value must not contain any party literals and not
    -- reference any value which contain party literals.
  , dvalIsTest :: !IsTest
    -- ^ Is the value maked as a test to be run as a scenario?
  , dvalBody   :: !Expr
    -- ^ Expression whose value to bind to the name.
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

data TemplateKey = TemplateKey
  { tplKeyType :: !Type
  , tplKeyBody :: !Expr
  -- ^ Note that the protobuf imposes strict restrictions on what this can be (see
  -- proto file). However the compiler produces things that are _not_ in that fragment,
  -- and thus we gradually simplify them to try to turn them into something as part
  -- of that fragment in DAML-LF directly.
  , tplKeyMaintainers :: !Expr
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Definition of a contract template.
data Template = Template
  { tplLocation :: !(Maybe SourceLoc)
    -- Location of the definition in the source file.
  , tplTypeCon         :: !TypeConName
    -- ^ Type constructor corresponding to the template.
  , tplParam           :: !ExprVarName
    -- ^ Variable to bind the template argument to.
  , tplPrecondition    :: !Expr
    -- ^ Precondition that needs to be satisfied by the argument to create an
    -- instance. It has type @Bool@ and the template parameter in scope.
  , tplSignatories     :: !Expr
    -- ^ Signatories of the contract. They have type @List Party@ and the
    -- template paramter in scope.
  , tplObservers       :: !Expr
    -- ^ Observers of the contract. They have type @List Party@ and the
    -- template paramter in scope.
  , tplAgreement       :: !Expr
    -- ^ Agreement text associated with the contract. It has type @Text@ and
    -- the template paramter in scope.
  , tplChoices         :: !(NM.NameMap TemplateChoice)
    -- ^ Choices of the template.
  , tplKey             :: !(Maybe TemplateKey)
    -- ^ Template key definition, if any.
  }
  deriving (Eq, Generic, NFData, Show, ToJSON)

-- | Single choice of a contract template.
data TemplateChoice = TemplateChoice
  { chcLocation :: !(Maybe SourceLoc)
    -- Location of the definition in the source file.
  , chcName       :: !ChoiceName
    -- ^ Name of the choice.
  , chcConsuming  :: !Bool
    -- ^ Flag determining whether the choice consumes the contract template
    -- instance or not.
  , chcControllers :: !Expr
    -- ^ The controllers of the choice. They have type @List Party@ and the
    -- template parameter in scope, but not the choice parameter.
  , chcSelfBinder :: !ExprVarName
    -- ^ Variable to bind the ContractId of the contract this choice is
    -- exercised on to.
  , chcArgBinder     :: !(ExprVarName, Type)
    -- ^ Variable to bind the choice argument to and its type.
  , chcReturnType :: !Type
    -- ^ Return type of the update triggered by exercising the choice.
  , chcUpdate     :: !Expr
    -- ^ Follow-up update of the choice. It has type @Update <ret_type>@ and
    -- both the template parameter and the choice parameter in scope.
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | Feature flags for a module.
data FeatureFlags = FeatureFlags
  { forbidPartyLiterals :: !Bool
  -- ^ If set to true, party literals are forbidden to appear in daml-lf packages.
  {-
  DAML-LF has these but our ecosystem does not support them anymore, see #157
  , dontDivulgeContractIdsInCreateArguments :: !Bool
  -- ^ If set to true, arguments to creates are not divulged. Instead target contract id's of
  -- exercises are divulged and fetch is checked for authorization.
  , dontDiscloseNonConsumingChoicesToObservers :: !Bool
  -- ^ If set to true, exercise nodes of non-consuming choices are only disclosed to the signatories
  -- and controllers of the target contract/choice and not to the observers of the target contract.
  -}
  }
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

defaultFeatureFlags :: FeatureFlags
defaultFeatureFlags = FeatureFlags
  { forbidPartyLiterals = False
  }

-- | Feature flags for DAML 1.2.
daml12FeatureFlags :: FeatureFlags
daml12FeatureFlags = FeatureFlags
  { forbidPartyLiterals = True
  }

-- | A module.
data Module = Module
  { moduleName        :: !ModuleName
    -- ^ Name of the module.
  , moduleSource :: !(Maybe FilePath)
    -- ^ Path to the source file, when known. This is not part of the
    -- protobuf serialization format.
  , moduleFeatureFlags :: !FeatureFlags
    -- ^ Feature flags of this module.
  , moduleDataTypes :: !(NM.NameMap DefDataType)
    -- ^ Data type definitions.
  , moduleValues :: !(NM.NameMap DefValue)
    -- ^ Top-level value definitions.
  , moduleTemplates :: !(NM.NameMap Template)
    -- ^ Template definitions.
  }
  deriving (Eq, Generic, NFData, Show, ToJSON)

-- | A package.
data Package = Package
    { packageLfVersion :: Version
    , packageModules :: NM.NameMap Module
    }
  deriving (Eq, Generic, NFData, Show, ToJSON)

-- | Type synonym for a reference to an LF value.
type ValueRef = Qualified ExprValName

deriving instance Foldable    Qualified
deriving instance Functor     Qualified
deriving instance Traversable Qualified

instance Hashable PackageRef
instance Hashable a => Hashable (Qualified a)

instance NM.Named TemplateChoice where
  type Name TemplateChoice = ChoiceName
  name = chcName

instance NM.Named DefDataType where
  type Name DefDataType = TypeConName
  name = dataTypeCon

instance NM.Named DefValue where
  type Name DefValue = ExprValName
  name = fst . dvalBinder

instance NM.Named Template where
  type Name Template = TypeConName
  name = tplTypeCon

instance NM.Named Module where
  type Name Module = ModuleName
  name = moduleName

concatSequenceA
  [ makePrisms ''Kind
  , makePrisms ''Type
  , makePrisms ''Expr
  , makePrisms ''Update
  , makePrisms ''Scenario
  , makePrisms ''DataCons
  , makePrisms ''Package
  , makeUnderscoreLenses ''DefDataType
  , makeUnderscoreLenses ''DefValue
  , makeUnderscoreLenses ''TemplateChoice
  , makeUnderscoreLenses ''Template
  , makeUnderscoreLenses ''Module
  , makeUnderscoreLenses ''Package
  , makeUnderscoreLenses ''TemplateKey
  ]
