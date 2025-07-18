import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.EncryptionPublicKey
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.util.{ErrorUtil, HexString, MonadUtil}
import com.digitalasset.canton.version.HasToByteString
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.google.protobuf.ByteString

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

// Wrapping object required due to https://github.com/com-lihaoyi/Ammonite/issues/534
object Main {

  case class TestMessage(str: String) extends HasToByteString {
    override def toByteString: ByteString = ByteString.copyFromUtf8(str)
  }

  object TestMessage {
    def fromByteString(bytes: ByteString): Either[DeserializationError, TestMessage] =
      Right(TestMessage(bytes.toStringUtf8))
  }

  val publicKeyStr =
    """0a6110041a5b3059301306072a8648ce3d020106082a8648ce3d03010703
      |4200049f36974eed2d06c9d2026b7ae4166ffeb0995d6b50f2af60c70cd8
      |4a67c0458a8d5cae3c4f5859ee2ff6aba23e92df3a8cef072b9a28611e1a
      |e60257212ca3932801101e
      |""".stripMargin.replaceAll("\n", "")

  val publicKeyBytes =
    HexString.parseToByteString(publicKeyStr).getOrElse(sys.error("Invalid public key hexstring"))

  val publicKey = EncryptionPublicKey
    .fromTrustedByteString(publicKeyBytes)
    .fold(err => sys.error(s"Invalid public key: $err"), identity)

  val testMessage = TestMessage("No bugs in Canton")

  val p1 = participant1.underlying.get
  val crypto = p1.cryptoPureApi

  val rounds = Option(System.getProperty("canton.encryption-test-rounds")).map(_.toInt).getOrElse(10)
  val parallelism = Option(System.getProperty("canton.encryption-test-parallelism")).map(_.toInt).getOrElse(5)

  def main() {

    logger.info(
      s"Run asymmetric encryption test with rounds=$rounds, algo=${crypto.defaultEncryptionAlgorithmSpec}"
    )

    val resultF = MonadUtil.parTraverseWithLimit_(PositiveInt.tryCreate(parallelism))(0 until rounds) { round =>
      Future {
        crypto.encryptWith(testMessage, publicKey) match {
          case Left(err) =>
            logger.error(s"[$round] Failed to encrypt: $err")
          case Right(encrypted) =>
            logger.debug(s"[$round] Produced ciphertext: ${HexString.toHexString(encrypted.ciphertext)}")
        }
      }
    }

    Await.ready(resultF, Duration.apply("60 second"))

    logger.info(s"Completed asymmetric encryption test")
    println("Encryption test completed")
  }
}

Main.main()
