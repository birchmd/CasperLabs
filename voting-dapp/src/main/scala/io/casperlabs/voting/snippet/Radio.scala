package io.casperlabs.voting.snippet

import cats.implicits._
import cats.effect.Sync

import com.google.protobuf.ByteString

import io.casperlabs.client.{DeployService, GrpcDeployService}
import io.casperlabs.crypto.codec.Base16
import io.casperlabs.casper.protocol._
import io.casperlabs.shared.UncaughtExceptionLogger

import java.io.File
import java.nio.file.Files

import monix.eval.Task
import monix.execution.Scheduler

import net.liftweb.common._
import net.liftweb.util.Helpers._
import net.liftweb.http.SHtml
import net.liftweb.http.SHtml.ChoiceHolder

object SatoshiIdentity extends Enumeration {
  type SatoshiIdentity = Value

  val hal       = Value("Hal Finney")
  val nick      = Value("Nick Szabo")
  val dave      = Value("Dave Kleiman")
  val gov       = Value("PsyOP directed by the U.S. Government")
  val craig     = Value("Craig Wright")
  val none      = Value("None of the above")
  val neverKnow = Value("The world will never know")
}

object Voting {
  import SatoshiIdentity._

  val deployService = new GrpcDeployService("3.16.244.129", 40401)
  val voteContract: File =
    new File(
      "/mnt/c/Users/mbirc/Documents/CasperLabs/CasperLabs/voting-dapp/rust/call/target/wasm32-unknown-unknown/release/vote.wasm"
    )

  val voteWasm: ByteString = ByteString.copyFrom(Files.readAllBytes(voteContract.toPath))
  val accountAddress: ByteString =
    ByteString.copyFrom(Base16.decode("3030303030303030303030303030303030303030"))

  def vote[F[_]](option: SatoshiIdentity)(implicit s: Sync[F], d: DeployService[F]): F[Unit] = {
    val i           = option.id
    val iBytes      = ByteString.copyFrom(Array(1, 0, 0, 0, 4, 0, 0, 0, i, 0, 0, 0).map(_.toByte))
    val sessionCode = DeployCode().withCode(voteWasm).withArgs(iBytes)
    val paymentCode = DeployCode().withCode(voteWasm)

    val deploy = DeployData()
      .withTimestamp(System.currentTimeMillis())
      .withSession(sessionCode)
      .withPayment(paymentCode)
      .withAddress(accountAddress)
      .withGasLimit(100000L)
      .withNonce(0L)

    for {
      _ <- DeployService[F].deploy(deploy)
      _ <- DeployService[F].createBlock()
    } yield ()
  }
}

object Radio {
  import SatoshiIdentity._

  implicit val scheduler: Scheduler = Scheduler.computation(
    Math.max(java.lang.Runtime.getRuntime.availableProcessors(), 2),
    "node-runner",
    reporter = UncaughtExceptionLogger
  )

  val options: Seq[SatoshiIdentity] = SatoshiIdentity.values.toSeq
  val default: Box[SatoshiIdentity] = Empty
  val radio: ChoiceHolder[SatoshiIdentity] =
    SHtml.radioElem(options, default) { selectedBox =>
      val selected   = selectedBox.openOr(hal)
      val votingTask = Voting.vote[Task](selected)(Sync[Task], Voting.deployService)
      votingTask.runSyncUnsafe()
    }

  def render = ".options" #> radio.toForm
}
