-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# LANGUAGE TypeFamilies #-}
-- | Types and pretty-printer for the AST of the Daml Ledger Fragment.
module DA.Daml.LF.Ast.Base(
    module DA.Daml.LF.Ast.Base
    ) where

import Data.Aeson
import Data.Hashable
import Data.Data
import GHC.Generics(Generic)
import Data.Int
import Control.DeepSeq
import Control.Lens
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Set as S
import qualified "template-haskell" Language.Haskell.TH as TH
import qualified Control.Lens.TH as Lens.TH

import DA.Daml.LF.Ast.Version
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Ast.TypeLevelNat

infixr 1 `KArrow`

-- | Identifier for a package. Will be obtained by hashing the contents of the
-- package. Must match the regex
--
-- > [a-zA-Z0-9]+
newtype PackageId = PackageId{unPackageId :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData, ToJSON, ToJSONKey, FromJSON)

-- | Name for a module. Must match the regex
--
-- > ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*
newtype ModuleName = ModuleName{unModuleName :: [T.Text]}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData, ToJSON, FromJSON)

-- | Name for a type synonym. Must match the regex
--
-- > ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*
newtype TypeSynName = TypeSynName{unTypeSynName :: [T.Text]}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for a type constructor. Must match the regex
--
-- > ([A-Z][a-zA-Z0-9_]*)(\.[A-Z][a-zA-Z0-9_]*)*
newtype TypeConName = TypeConName{unTypeConName :: [T.Text]}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for a record field. Must match the regex
--
-- > [a-z][a-zA-Z0-9_]*
newtype FieldName = FieldName{unFieldName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for a variant constructor. Must match the regex
--
-- > [A-Z][a-zA-Z0-9_]*
newtype VariantConName = VariantConName{unVariantConName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for the choice of a contract. Must match the regex
--
-- > [A-Z][a-zA-Z0-9_]*
newtype ChoiceName = ChoiceName{unChoiceName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for an interface method.  Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
newtype MethodName = MethodName{unMethodName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for a type variable. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
newtype TypeVarName = TypeVarName{unTypeVarName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for a local expression variable, bound in an expression,
--   and used locally. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
newtype ExprVarName = ExprVarName{unExprVarName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Name for an global expression variable, bound at the declaration level,
--   and used in this and other modules. Must match the regex
--
-- > [a-z_][a-zA-Z0-9_]*
newtype ExprValName = ExprValName{unExprValName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData)

-- | Human-readable name of a package. Must match the regex
--
-- > [a-zA-Z0-9_-]+
newtype PackageName = PackageName{unPackageName :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData, ToJSON, FromJSON)

-- | Human-readable version of a package. Must match the regex
--
-- > (0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*
newtype PackageVersion = PackageVersion{unPackageVersion :: T.Text}
    deriving stock (Eq, Data, Generic, Ord, Show)
    deriving newtype (Hashable, NFData, ToJSON, FromJSON)

-- | Reference to a package.
data PackageRef
  = PRSelf
    -- ^ Reference to the package being currently handled.
  | PRImport !PackageId
    -- ^ Reference to the package with the given id.
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Something qualified by a package and a module within that package.
data Qualified a = Qualified
  { qualPackage :: !PackageRef
  , qualModule  :: !ModuleName
  , qualObject  :: !a
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

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
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Kinds.
data Kind
  = KStar
  | KNat
  | KArrow Kind Kind
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Builtin type.
data BuiltinType
  = BTInt64
  | BTNumeric
  | BTText
  | BTTimestamp
  | BTDate
  | BTParty
  | BTUnit
  | BTBool
  | BTList
  | BTUpdate
  | BTScenario
  | BTContractId
  | BTOptional
  | BTTextMap
  | BTGenMap
  | BTArrow
  | BTAny
  | BTTypeRep
  | BTRoundingMode
  | BTBigNumeric
  | BTAnyException
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Type as used in typed binders.
data Type
  -- | Reference to a type variable.
  = TVar        !TypeVarName
  -- | Reference to a type constructor.
  | TCon        !(Qualified TypeConName)
  -- | Fully-applied type synonym.
  | TSynApp     !(Qualified TypeSynName) ![Type]
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
  -- | Type for structs aka structural records. Parameterized by the names of the
  -- fields and their types.
  | TStruct      ![(FieldName, Type)]
  -- | Type-level natural numbers
  | TNat !TypeLevelNat
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Fully applied qualified type constructor.
data TypeConApp = TypeConApp
  { tcaTypeCon :: !(Qualified TypeConName)
    -- ^ Qualified name of the type constructor.
  , tcaArgs    :: ![Type]
    -- ^ Type arguments which are applied to the type constructor.
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data RoundingModeLiteral =
      LitRoundingUp
    | LitRoundingDown
    | LitRoundingCeiling
    | LitRoundingFloor
    | LitRoundingHalfUp
    | LitRoundingHalfDown
    | LitRoundingHalfEven
    | LitRoundingUnnecessary
    deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Builtin operation or literal.
data BuiltinExpr
  -- Literals
  = BEInt64      !Int64          -- :: Int64
  | BENumeric    !Numeric        -- :: Numeric, precision 38, scale 0 through 37
  | BEText       !T.Text         -- :: Text
  | BETimestamp  !Int64          -- :: Timestamp, microseconds since unix epoch
  | BEDate       !Int32          -- :: Date, days since unix epoch
  | BEUnit                       -- :: Unit
  | BEBool       !Bool           -- :: Bool
  | BERoundingMode !RoundingModeLiteral -- :: RoundingMode

  -- Exceptions
  | BEError                          -- :: ∀a. Text -> a
  | BEAnyExceptionMessage            -- :: AnyException -> Text

  -- Polymorphic functions
  | BEEqualGeneric               -- :: ∀t. t -> t -> Bool
  | BELessGeneric                -- :: ∀t. t -> t -> Bool
  | BELessEqGeneric              -- :: ∀t. t -> t -> Bool
  | BEGreaterGeneric             -- :: ∀t. t -> t -> Bool
  | BEGreaterEqGeneric           -- :: ∀t. t -> t -> Bool
  | BEEqual      !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BELess       !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BELessEq     !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEGreaterEq  !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEGreater    !BuiltinType    -- :: t -> t -> Bool, where t is the builtin type
  | BEToText     !BuiltinType    -- :: t -> Text, where t is one of the builtin types
                                 -- {Int64, Decimal, Text, Timestamp, Date, Party}
  | BEContractIdToText           -- :: forall t. ContractId t -> Option Text

  -- Numeric arithmetic and comparisons
  | BEEqualNumeric               -- :: ∀n. Numeric n -> Numeric n -> Bool, where t is the builtin type
  | BELessNumeric                -- :: ∀(s:nat). Numeric s -> Numeric s -> Bool
  | BELessEqNumeric              -- :: ∀(s:nat). Numeric s -> Numeric s -> Bool
  | BEGreaterEqNumeric           -- :: ∀(s:nat). Numeric s -> Numeric s -> Bool
  | BEGreaterNumeric             -- :: ∀(s:nat). Numeric s -> Numeric s -> Bool
  | BENumericToText              -- :: ∀(s:nat). Numeric s -> Text
  | BEAddNumeric                 -- :: ∀(s:nat). Numeric s -> Numeric s -> Numeric s, crashes on overflow
  | BESubNumeric                 -- :: ∀(s:nat). Numeric s -> Numeric s -> Numeric s, crashes on overflow
  | BEMulNumericLegacy           -- :: ∀(s1:nat). ∀(s2:nat). ∀(s3:nat). Numeric s1 -> Numeric s2 -> Numeric s3, crashes on overflow and underflow, automatically rounds to even (see <https://en.wikipedia.org/wiki/Rounding#Round_half_to_even>)
  | BEMulNumeric                 -- :: ∀(s1:nat). ∀(s2:nat). ∀(s3:nat). Numeric s3 -> Numeric s1 -> Numeric s2 -> Numeric s3, crashes on overflow and underflow, automatically rounds to even (see <https://en.wikipedia.org/wiki/Rounding#Round_half_to_even>)
  | BEDivNumericLegacy           -- :: ∀(s1:nat). ∀(s2:nat). ∀(s3:nat). Numeric s1 -> Numeric s2 -> Numeric s3, automatically rounds to even, crashes on divisor = 0 and on overflow
  | BEDivNumeric                 -- :: ∀(s1:nat). ∀(s2:nat). ∀(s3:nat). Numeric s3 -> Numeric s1 -> Numeric s2 -> Numeric s3, automatically rounds to even, crashes on divisor = 0 and on overflow
  | BERoundNumeric               -- :: ∀(s:nat). Int64 -> Numeric s -> Numeric s, the Int64 is the required scale. Note that this doesn't modify the scale of the type itself, it just zeroes things outside that scale out. Can be negative. Crashes if the scale is > 10 or < -27.
  | BECastNumericLegacy          -- :: ∀(s1:nat). ∀(s2:nat). Numeric s1 -> Numeric s2
  | BECastNumeric                -- :: ∀(s1:nat). ∀(s2:nat). Numeric s2 -> Numeric s1 -> Numeric s2
  | BEShiftNumericLegacy         -- :: ∀(s1:nat). ∀(s2:nat). Numeric s1 -> Numeric s2
  | BEShiftNumeric               -- :: ∀(s1:nat). ∀(s2:nat). Numeric s2 -> Numeric s1 -> Numeric s2

  -- Integer arithmetic
  | BEAddInt64                   -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BESubInt64                   -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BEMulInt64                   -- :: Int64 -> Int64 -> Int64, crashes on overflow
  | BEDivInt64                   -- :: Int64 -> Int64 -> Int64, crashes on divisor = 0
  | BEModInt64                   -- :: Int64 -> Int64 -> Int64, crashes on divisor = 0
  | BEExpInt64                   -- :: Int64 -> Int64 -> Int64, crashes on overflow

  -- Numerical conversion
  | BEInt64ToNumericLegacy       -- :: ∀(s:nat). Int64 -> Numeric s, crashes if it doesn't fit (TODO: verify?)
  | BEInt64ToNumeric             -- :: ∀(s:nat). Numeric s -> Int64 -> Numeric s, crashes if it doesn't fit (TODO: verify?)
  | BENumericToInt64             -- :: ∀(s:nat). Numeric s -> Int64, only converts the whole part, crashes if it doesn't fit

  -- Time conversion
  | BETimestampToUnixMicroseconds -- :: Timestamp -> Int64, in microseconds
  | BEUnixMicrosecondsToTimestamp -- :: Int64 -> Timestamp, in microseconds
  | BEDateToUnixDays              -- :: Date -> Int64, in microseconds
  | BEUnixDaysToDate              -- :: Int64 -> Date, in microseconds

  -- List operations
  | BEFoldl                      -- :: ∀a b. (b -> a -> b) -> b -> List a -> b
  | BEFoldr                      -- :: ∀a b. (a -> b -> b) -> b -> List a -> b
  | BEEqualList                  -- :: ∀a. (a -> a -> Bool) -> List a -> List a -> Bool

  -- Map operations
  | BETextMapEmpty               -- :: ∀ a. TextMap a
  | BETextMapInsert              -- :: ∀ a. Text -> a -> TextMap a -> TextMap a
  | BETextMapLookup              -- :: ∀ a. Text -> TextMap a -> Optional a
  | BETextMapDelete              -- :: ∀ a. Text -> TextMap a -> TextMap a
  | BETextMapToList              -- :: ∀ a. TextMap a -> List ⟨key: Text, value: a⟩
  | BETextMapSize                -- :: ∀ a. TextMap a -> Int64

  -- GenMap operations
  | BEGenMapEmpty                -- :: ∀ a b. GenMap a b
  | BEGenMapInsert               -- :: ∀ a b. a -> b -> GenMap a b -> GenMap a b
  | BEGenMapLookup               -- :: ∀ a b. a -> GenMap a b -> Optional b
  | BEGenMapDelete               -- :: ∀ a b. a -> GenMap a b -> GenMap a b
  | BEGenMapKeys                 -- :: ∀ a b. GenMap a b -> List a
  | BEGenMapValues               -- :: ∀ a b. GenMap a b -> List b
  | BEGenMapSize                 -- :: ∀ a b. GenMap a b -> Int64

  -- Text operations
  | BEExplodeText                -- :: Text -> List Text
  | BEAppendText                 -- :: Text -> Text -> Text
  | BEImplodeText                -- :: List Text -> Text
  | BESha256Text                 -- :: Text -> Text
  | BETextToParty                -- :: Text -> Optional Party
  | BETextToInt64                -- :: Text -> Optional Int64
  | BETextToNumericLegacy        -- :: ∀(s:nat). Text -> Optional (Numeric s)
  | BETextToNumeric              -- :: ∀(s:nat). Numeric s -> Text -> Optional (Numeric s)
  | BETextToCodePoints           -- :: Text -> List Int64
  | BECodePointsToText           -- :: List Int64 -> Text
  | BEPartyToQuotedText          -- :: Party -> Text

  -- BigNumeric operations
  | BEScaleBigNumeric            -- :: BigNumeric -> Int64
  | BEPrecisionBigNumeric        -- :: BigNumeric -> Int64
  | BEAddBigNumeric              -- :: BigNumeric -> BigNumeric -> BigNumeric
  | BESubBigNumeric              -- :: BigNumeric -> BigNumeric -> BigNumeric
  | BEMulBigNumeric              -- :: BigNumeric -> BigNumeric -> BigNumeric
  | BEDivBigNumeric              -- :: Int64 -> RoundingMode -> BigNumeric -> BigNumeric -> BigNumeric
  | BEShiftRightBigNumeric       -- :: Int64 -> BigNumeric -> BigNumeric
  | BEBigNumericToNumericLegacy  -- :: ∀(s:nat). BigNumeric -> Numeric s
  | BEBigNumericToNumeric        -- :: ∀(s:nat). Numeric s -> BigNumeric -> Numeric s
  | BENumericToBigNumeric        -- :: ∀(s:nat). Numeric s -> BigNumeric

  | BETrace                      -- :: forall a. Text -> a -> a
  | BEEqualContractId            -- :: forall a. ContractId a -> ContractId a -> Bool
  | BECoerceContractId           -- :: forall a b. ContractId a -> ContractId b

  -- TypeRep
  | BETypeRepTyConName           -- :: TypeRep -> Optional Text
  deriving (Eq, Data, Generic, NFData, Ord, Show)


data Binding = Binding
  { bindingBinder :: !(ExprVarName, Type)
  , bindingBound  :: !Expr
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

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
  -- | Enum construction.
  | EEnumCon
    { enumTypeCon :: !(Qualified TypeConName)
      -- ^ Type constructor of the enum type.
    , enumDataCon :: !VariantConName
      -- ^ Data constructor of the enum type.
    }
  -- | Struct construction.
  | EStructCon
    { structFields :: ![(FieldName, Expr)]
      -- ^ Fields together with the expressions to assign to them.
    }
  -- | Struct projection.
  | EStructProj
    { structField :: !FieldName
      -- ^ Field to project to.
    , structExpr  :: !Expr
      -- ^ Expression to project from.
    }
  -- | Non-destructive struct update.
  | EStructUpd
    { structField :: !FieldName
      -- ^ Field to update.
    , structExpr :: !Expr
      -- ^ Expression to update the field in.
    , structUpdate :: !Expr
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
  -- | Construct empty list.
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
  | EToAny
    { toAnyType :: !Type
    , toAnyBody :: !Expr
    }
  | EFromAny
    { fromAnyType :: !Type
    , fromAnyBody :: !Expr
    }
  | ETypeRep !Type
  -- | Construct an 'AnyException' value from a value of an exception type.
  | EToAnyException
    { toAnyExceptionType :: !Type
    , toAnyExceptionValue :: !Expr
    }
  -- | Convert 'AnyException' back to its underlying value, if possible.
  | EFromAnyException
    { fromAnyExceptionType :: !Type
    , fromAnyExceptionValue :: !Expr
    }
  -- | Throw an exception.
  | EThrow
    { throwReturnType :: !Type
    , throwExceptionType :: !Type
    , throwExceptionValue :: !Expr
    }
  -- | Convert template payload to interface type.
  | EToInterface
    { toInterfaceInterface :: !(Qualified TypeConName)
    , toInterfaceTemplate :: !(Qualified TypeConName)
    , toInterfaceExpr :: !Expr
    }
  -- | Convert interface type to template payload when possible.
  | EFromInterface
    { fromInterfaceInterface :: !(Qualified TypeConName)
    , fromInterfaceTemplate :: !(Qualified TypeConName)
    , fromInterfaceExpr :: !Expr
    }
  -- | Convert interface type to template payload or raise WronglyTypedContract error if not possible.
  | EUnsafeFromInterface
    { unsafeFromInterfaceInterface :: !(Qualified TypeConName)
    , unsafeFromInterfaceTemplate :: !(Qualified TypeConName)
    , unsafeFromInterfaceContractId :: !Expr
    , unsafeFromInterfaceExpr :: !Expr
    }
  -- | Invoke an interface method
  | ECallInterface
    { callInterfaceType :: !(Qualified TypeConName)
    , callInterfaceMethod :: !MethodName
    , callInterfaceExpr :: !Expr
    }
  -- | Upcast interface
  | EToRequiredInterface
    { triRequiredInterface :: !(Qualified TypeConName)
    , triRequiringInterface :: !(Qualified TypeConName)
    , triExpr :: !Expr
    }
  -- | Downcast interface
  | EFromRequiredInterface
    { friRequiredInterface :: !(Qualified TypeConName)
    , friRequiringInterface :: !(Qualified TypeConName)
    , friExpr :: !Expr
    }
  -- | Downcast interface or raise WronglyTypedContract error if not possible.
  | EUnsafeFromRequiredInterface
    { unsafeFromRequiredInterfaceInterface :: !(Qualified TypeConName)
    , unsafeFromRequiredInterfaceTemplate :: !(Qualified TypeConName)
    , unsafeFromRequiredInterfaceContractId :: !Expr
    , unsafeFromRequiredInterfaceExpr :: !Expr
    }
  -- | Obtain type representation of contract's template through an interface
  | EInterfaceTemplateTypeRep
    { ttrInterface :: !(Qualified TypeConName)
    , ttrExpr :: !Expr
    }
  -- | Obtain signatories of contract through an interface
  | ESignatoryInterface
    { rvsInterface :: !(Qualified TypeConName)
    , rvsExpr :: !Expr
    }
  -- | Obtain observers of contract through an interface
  | EObserverInterface
    { rvoInterface :: !(Qualified TypeConName)
    , rvoExpr :: !Expr
    }
  -- | Update expression.
  | EUpdate !Update
  -- | Scenario expression.
  | EScenario !Scenario
  -- | An expression annotated with a source location.
  | ELocation !SourceLoc !Expr
  -- | Obtain an interface view
  | EViewInterface
    { viewInterfaceInterface :: !(Qualified TypeConName)
    , viewInterfaceExpr :: !Expr
    }
  | EChoiceController
    { choiceControllerTemplate :: !(Qualified TypeConName)
    , choiceControllerChoice :: !ChoiceName
    , choiceControllerContract :: !Expr
    , choiceControllerChoiceArg :: !Expr
    }
  | EChoiceObserver
    { choiceObserverTemplate :: !(Qualified TypeConName)
    , choiceObserverChoice :: !ChoiceName
    , choiceObserverContract :: !Expr
    , choiceObserverChoiceArg :: !Expr
    }
  -- | Experimental Expression Hook
  | EExperimental !T.Text !Type
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Pattern matching alternative.
data CaseAlternative = CaseAlternative
  { altPattern :: !CasePattern
    -- ^ Pattern to match on.
  , altExpr    :: !Expr
    -- ^ Expression to evaluate in case of a match.
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data CasePattern
  -- | Match on a constructor of a variant type.
  = CPVariant
    { patTypeCon :: !(Qualified TypeConName)
      -- ^ Type constructor of the type to match on.
    , patVariant :: !VariantConName
      -- ^ Variant constructor to match on.
    , patBinder  :: !ExprVarName
      -- ^ Variable to bind the variant constructor argument to.
    }
  -- | Match on a constructor of an enum type.
  | CPEnum
    { patTypeCon :: !(Qualified TypeConName)
      -- ^ Type constructor of the type to match on.
    , patDataCon :: !VariantConName
      -- ^ Data constructor to match on.
    }
  -- | Match on the unit type.
  | CPUnit
  -- | Match on the bool type.
  | CPBool !Bool
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
  deriving (Eq, Data, Generic, NFData, Ord, Show)

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
  -- | Create contract instance based on interface payload.
  | UCreateInterface
    { creInterface :: !(Qualified TypeConName)
      -- ^ Interface type.
    , creArg :: !Expr
      -- ^ Payload expression.
    }

  -- | Exercise choice on a contract given a contract ID.
  | UExercise
    { exeTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeContractId :: !Expr
      -- ^ Contract id of the contract template instance to exercise choice on.
    , exeArg        :: !Expr
      -- ^ Argument for the choice.
    }

  -- | (Soft) Exercise choice on a contract given a contract ID.
  | USoftExercise
    { exeTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeContractId :: !Expr
      -- ^ Contract id of the contract template instance to exercise choice on.
    , exeArg        :: !Expr
      -- ^ Argument for the choice.
    }

  -- | Exercise choice on a contract given a contract ID, dynamically.
  | UDynamicExercise
    { exeTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeContractId :: !Expr
      -- ^ Contract id of the contract template instance to exercise choice on.
    , exeArg        :: !Expr
      -- ^ Argument for the choice.
    }
  -- | Exercise choice on a contract of an interface given a contract ID.
  | UExerciseInterface
    { exeInterface   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the interface.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeContractId :: !Expr
      -- ^ Contract id of the contract template instance to exercise choice on.
    , exeArg        :: !Expr
      -- ^ Argument for the choice.
    , exeGuard      :: !(Maybe Expr)
      -- ^ Exercise guard (Interface -> Bool) to abort the transaction eagerly
      -- if the payload does not satisfy the predicate. Nothing if the exercise
      -- was unguarded.
    }
  -- | Exercise a choice on a contract by key.
  | UExerciseByKey
    { exeTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , exeChoice     :: !ChoiceName
      -- ^ Choice to exercise.
    , exeKey        :: !Expr
      -- ^ Key of the contract template instance to exercise choice on.
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
  -- | Retrieve the argument of an existing contract instance of the given
  -- template type or one of its predecessors. If such a contract exists the
  -- result will be translated into terms of the given template type.
  | USoftFetch
    { fetTemplate   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , fetContractId :: !Expr
      -- ^ Contract id of the contract template instance whose argument shall be
      -- retrieved.
    }
  -- | Retrieve the argument of an existing contract interface instance.
  | UFetchInterface
    { fetInterface   :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the interface.
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
  | UTryCatch
    { tryCatchType :: !Type
    , tryCatchExpr :: !Expr
    , tryCatchVar :: !ExprVarName
    , tryCatchHandler :: !Expr
    }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

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
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data RetrieveByKey = RetrieveByKey
  { retrieveByKeyTemplate :: !(Qualified TypeConName)
  , retrieveByKeyKey :: !Expr
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

newtype IsSerializable = IsSerializable{getIsSerializable :: Bool}
  deriving stock (Eq, Data, Generic, Ord, Show)
  deriving anyclass (NFData)

-- | Definition of a type synonym.
data DefTypeSyn = DefTypeSyn
  { synLocation :: !(Maybe SourceLoc)
    -- ^ Location of the definition in the source file.
  , synName     :: !TypeSynName
    -- ^ Name of the synonym.
  , synParams   :: ![(TypeVarName, Kind)]
    -- ^ Type paramaters to the type synonym.
  , synType     :: !Type
    -- ^ Type synonomized.
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Definition of a data type.
data DefDataType = DefDataType
  { dataLocation :: !(Maybe SourceLoc)
    -- ^ Location of the definition in the source file.
  , dataTypeCon :: !TypeConName
    -- ^ Name of the type constructor.
  , dataSerializable :: !IsSerializable
    -- ^ The data type preserves serializabillity.
  , dataParams  :: ![(TypeVarName, Kind)]
    -- ^ Type paramaters to the type constructor. They must be empty when
    -- @dataCons@ is @DataEnum@.
  , dataCons    :: !DataCons
    -- ^ Data constructor of the type.
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Data constructors for a data type definition.
data DataCons
  -- | A record type given by its field names and their types.
  = DataRecord  ![(FieldName, Type)]
  -- | A variant type given by its construtors and their payload types.
  | DataVariant ![(VariantConName, Type)]
  -- | An enum type given by the name of its constructors.
  | DataEnum ![VariantConName]
  -- | An interface type, with accompanying interface definition in this module.
  | DataInterface
  deriving (Eq, Data, Generic, NFData, Ord, Show)

newtype IsTest = IsTest{getIsTest :: Bool}
  deriving stock (Eq, Data, Generic, Ord, Show)
  deriving anyclass (NFData)

-- | Definition of a value.
data DefValue = DefValue
  { dvalLocation :: !(Maybe SourceLoc)
    -- ^ Location of the definition in the source file.
  , dvalBinder :: !(ExprValName, Type)
    -- ^ Name to bind the value to together with its type.
  , dvalIsTest :: !IsTest
    -- ^ Is the value maked as a test to be run as a scenario?
  , dvalBody   :: !Expr
    -- ^ Expression whose value to bind to the name.
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data TemplateKey = TemplateKey
  { tplKeyType :: !Type
  , tplKeyBody :: !Expr
  -- ^ Note that the protobuf imposes strict restrictions on what this can be (see
  -- proto file). However the compiler produces things that are _not_ in that fragment,
  -- and thus we gradually simplify them to try to turn them into something as part
  -- of that fragment in Daml-LF directly.
  , tplKeyMaintainers :: !Expr
  }
  deriving (Eq, Data, Generic, NFData, Ord, Show)

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
  , tplImplements      :: !(NM.NameMap TemplateImplements)
    -- ^ The interfaces that this template implements.
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | Interface instance in the template declaration.
data TemplateImplements = TemplateImplements
  { tpiInterface :: !(Qualified TypeConName)
    -- ^ Interface name for implementation.
  , tpiBody :: !InterfaceInstanceBody
  , tpiLocation :: !(Maybe SourceLoc)
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | Contents of an interface instance.
data InterfaceInstanceBody = InterfaceInstanceBody
  { iiMethods :: !(NM.NameMap InterfaceInstanceMethod)
  , iiView :: !Expr
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | An implementation of an interface's method.
data InterfaceInstanceMethod = InterfaceInstanceMethod
  { iiMethodName :: !MethodName
    -- ^ Name of method.
  , iiMethodExpr :: !Expr
    -- ^ Method expression. Has type @mty@ (the method's type as defined in the interface)
    -- and the template parameter in scope with type @tpl@ (the type of the template).
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | The interface and template that identify an interface instance.
-- Currently not part of the AST.
data InterfaceInstanceHead = InterfaceInstanceHead
  { iiInterface :: !(Qualified TypeConName)
  , iiTemplate :: !(Qualified TypeConName)
  }
  deriving (Eq, Ord, Data, Generic, NFData, Show)

-- | Definition of an exception type.
data DefException = DefException
  { exnLocation :: !(Maybe SourceLoc)
  , exnName :: !TypeConName
  , exnMessage :: !Expr
  }
  deriving (Eq, Data, Generic, NFData, Show)

data DefInterface = DefInterface
  { intLocation :: !(Maybe SourceLoc)
  , intName :: !TypeConName
  , intRequires :: !(S.Set (Qualified TypeConName))
  , intParam :: !ExprVarName
  , intChoices :: !(NM.NameMap TemplateChoice)
  , intMethods :: !(NM.NameMap InterfaceMethod)
  , intCoImplements :: !(NM.NameMap InterfaceCoImplements)
  , intView :: !Type
  }
  deriving (Eq, Data, Generic, NFData, Show)

data InterfaceMethod = InterfaceMethod
  { ifmLocation :: !(Maybe SourceLoc)
  , ifmName :: !MethodName
  , ifmType :: !Type
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | Interface instance in the interface declaration.
data InterfaceCoImplements = InterfaceCoImplements
  { iciTemplate :: !(Qualified TypeConName)
  , iciBody :: !InterfaceInstanceBody
  , iciLocation :: !(Maybe SourceLoc)
  }
  deriving (Eq, Data, Generic, NFData, Show)

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
    -- template parameter in scope, and (since 1.2) also the choice parameter.
  , chcObservers :: !(Maybe Expr)
    -- ^ The observers of the choice. When they are present, they have type @List Party@.
  , chcAuthorizers :: !(Maybe Expr)
    -- ^ The authorizers of the choice. When they are present, they have type @List Party@.
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
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Feature flags for a module.
data FeatureFlags = FeatureFlags
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Feature flags for Daml 1.2.
daml12FeatureFlags :: FeatureFlags
daml12FeatureFlags = FeatureFlags

-- | A module.
data Module = Module
  { moduleName        :: !ModuleName
    -- ^ Name of the module.
  , moduleSource :: !(Maybe FilePath)
    -- ^ Path to the source file, when known. This is not part of the
    -- protobuf serialization format.
  , moduleFeatureFlags :: !FeatureFlags
    -- ^ Feature flags of this module.
  , moduleSynonyms :: !(NM.NameMap DefTypeSyn)
    -- ^ Type synonym definitions.
  , moduleDataTypes :: !(NM.NameMap DefDataType)
    -- ^ Data type definitions.
  , moduleValues :: !(NM.NameMap DefValue)
    -- ^ Top-level value definitions.
  , moduleTemplates :: !(NM.NameMap Template)
    -- ^ Template definitions.
  , moduleExceptions :: !(NM.NameMap DefException)
  , moduleInterfaces :: !(NM.NameMap DefInterface)
  }
  deriving (Eq, Data, Generic, NFData, Show)

-- | Package metadata.
-- In `damlc build` we are guaranteed to have a name and version
-- however, for `damlc package` (which should really die in a fire)
-- we might only have a name and for `damlc compile` we don’t even
-- have a package name <insert sad panda here>.
-- We require metadata to be present in newer LF versions,
-- so we set it to some arbitrarily chosen garbage.
data PackageMetadata = PackageMetadata
    { packageName :: PackageName
    , packageVersion :: PackageVersion
    , upgradedPackageId :: Maybe PackageId
    } deriving (Eq, Data, Generic, NFData, Show)

-- | A package.
data Package = Package
    { packageLfVersion :: Version
    , packageModules :: NM.NameMap Module
    , packageMetadata :: Maybe PackageMetadata
    }
  deriving (Eq, Data, Generic, NFData, Show)


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

instance NM.Named InterfaceMethod where
  type Name InterfaceMethod = MethodName
  name = ifmName

instance NM.Named DefTypeSyn where
  type Name DefTypeSyn = TypeSynName
  name = synName

instance NM.Named DefDataType where
  type Name DefDataType = TypeConName
  name = dataTypeCon

instance NM.Named DefValue where
  type Name DefValue = ExprValName
  name = fst . dvalBinder

instance NM.Named DefException where
  type Name DefException = TypeConName
  name = exnName

instance NM.Named DefInterface where
  type Name DefInterface = TypeConName
  name = intName

instance NM.Named InterfaceCoImplements where
  type Name InterfaceCoImplements = Qualified TypeConName
  name = iciTemplate

instance NM.Named Template where
  type Name Template = TypeConName
  name = tplTypeCon

instance NM.Named TemplateImplements where
  type Name TemplateImplements = Qualified TypeConName
  name = tpiInterface

instance NM.Named InterfaceInstanceMethod where
  type Name InterfaceInstanceMethod = MethodName
  name = iiMethodName

instance NM.Named Module where
  type Name Module = ModuleName
  name = moduleName


fmap concat $ sequenceA $
  let
    -- | Generate a lens for every field in a record. The name of the lens is the
    -- name of the field prefixed by an underscore. For instance, for
    --
    -- > data Foo = Foo{bar :: Int, _baz :: Bool}
    --
    -- it will generate
    --
    -- > _bar :: Lens' Foo Int
    -- > __baz :: Lens' Foo Bool
    makeUnderscoreLenses :: TH.Name -> TH.DecsQ
    makeUnderscoreLenses =
      Lens.TH.makeLensesWith (set Lens.TH.lensField noUnderscoreNoPrefixNamer Lens.TH.lensRules)
      where
        noUnderscoreNoPrefixNamer _ _ n = [Lens.TH.TopName (TH.mkName ('_':TH.nameBase n))]
  in
  [ makePrisms ''Kind
  , makePrisms ''Type
  , makePrisms ''Expr
  , makePrisms ''Update
  , makePrisms ''Scenario
  , makePrisms ''DataCons
  , makePrisms ''PackageRef
  , makeUnderscoreLenses ''DefValue
  , makeUnderscoreLenses ''Package
  ]
