import           LowLevelTests
import           LowLevelTests.Op
import           Test.Tasty
import           UnsafeTests

main :: IO ()
main = defaultMain $ testGroup "GRPC Unit Tests"
  [ unsafeTests
  , unsafeProperties
  , lowLevelOpTests
  , lowLevelTests
  ]
