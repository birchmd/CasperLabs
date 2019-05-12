package io.casperlabs.voting.snippet

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Ref

import com.google.protobuf.ByteString

import io.casperlabs.client.{DeployService, GrpcDeployService}
import io.casperlabs.crypto.codec.Base16
import io.casperlabs.casper.protocol._
import io.casperlabs.shared.UncaughtExceptionLogger
import io.casperlabs.voting.Config

import java.io.File
import java.nio.file.Files

import monix.eval.Task
import monix.execution.Scheduler

import net.liftweb.common._
import net.liftweb.util.PassThru
import net.liftweb.util.Helpers._
import net.liftweb.http.{S, SHtml}
import net.liftweb.http.SHtml.ChoiceHolder

import scala.xml.{Elem, XML}

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

object Querying {

  private def message(block: String, id: Int): QueryStateRequest =
    QueryStateRequest(block, "address", Voting.accountAddressBase16, s"vote/$id")

  val latestBlock: Ref[Task, Option[Voting.BlockHash]] =
    Ref.unsafe[Task, Option[Voting.BlockHash]](None)

  def query[F[_]](block: String)(implicit s: Sync[F], d: DeployService[F]): F[Vector[Int]] =
    (0 until SatoshiIdentity.maxId).toVector.traverse { id =>
      d.queryState(message(block, id)).rethrow.flatMap { result =>
        if (result.startsWith("integer"))
          result.split(':')(1).trim.toInt.pure[F]
        else
          Sync[F].raiseError(new RuntimeException(s"Unexpected result from state query: $result"))
      }
    }

  def queryLatest(implicit d: DeployService[Task]): Task[(Voting.BlockHash, Vector[Int])] =
    latestBlock.get.flatMap {
      case Some(block) => query[Task](block).map(block -> _)
      case None        => Task.raiseError(new RuntimeException("No latest block to query!"))
    }

  def parsedResults(results: Vector[Int]): Vector[(Int, SatoshiIdentity.SatoshiIdentity, Int)] =
    results.zipWithIndex
      .map {
        case (votes, id) => (votes, SatoshiIdentity(id))
      }
      .sortBy(-_._1) // sort in decending order
      .zipWithIndex
      .map {
        case ((votes, id), rank) =>
          (rank + 1, id, votes)
      }

  def htmlResults(block: Voting.BlockHash, results: Vector[Int]): Elem = XML.loadString(
    s"""|<span>
        |  <h1>Results as of block: $block</h1>
        |  <table border="1">
        |    <tr>
        |      <th>Rank</th>
        |      <th>Candidate</th>
        |      <th>Votes</th>
        |    </tr>
        |    ${parsedResults(results)
         .map { case (rank, id, votes) => s"<tr><td>$rank</td><td>$id</td><td>$votes</td></tr>" }
         .mkString("\n")}
        |  </table>
        |</span> 
        |""".stripMargin
  )

  def render = {
    implicit val scheduler     = Radio.scheduler
    val (latestBlock, results) = queryLatest(Voting.deployService).runSyncUnsafe()

    "#theResults" #> htmlResults(latestBlock, results)
  }
}

object Voting {
  import SatoshiIdentity._
  type BlockHash = String

  val Config(host, wasm) = Config.fromResource()
  val deployService      = new GrpcDeployService(host, 40401)
  val voteContract: File = new File(wasm)

  val voteWasm: ByteString         = ByteString.copyFrom(Files.readAllBytes(voteContract.toPath))
  val accountAddressBase16: String = "3030303030303030303030303030303030303030"
  val accountAddress: ByteString =
    ByteString.copyFrom(Base16.decode(accountAddressBase16))

  def vote[F[_]](
      option: SatoshiIdentity
  )(implicit s: Sync[F], d: DeployService[F]): F[BlockHash] = {
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
      _      <- DeployService[F].deploy(deploy).rethrow
      result <- DeployService[F].createBlock().rethrow
      hash <- if (result.startsWith("Success"))
               result.split("Block")(1).trim.split('.').head.pure[F]
             else
               Sync[F].raiseError(
                 new RuntimeException(s"Unexpected response from block creation: $result")
               )
    } yield hash
  }
}

object Radio {
  implicit val scheduler: Scheduler = Scheduler.computation(
    Math.max(java.lang.Runtime.getRuntime.availableProcessors(), 2),
    "node-runner",
    reporter = UncaughtExceptionLogger
  )

  val idMapping: Map[String, Int] = Map(
    "hal"       -> 0,
    "nick"      -> 1,
    "dave"      -> 2,
    "gov"       -> 3,
    "craig"     -> 4,
    "none"      -> 5,
    "neverKnow" -> 6
  )

  def render = S.param("choice") match {
    // form correctly submitted
    case Full(selection) if idMapping.contains(selection) =>
      val selected = SatoshiIdentity(idMapping(selection))
      val votingTask = for {
        hash <- Voting.vote[Task](selected)(Sync[Task], Voting.deployService)
        _    <- Querying.latestBlock.set(Some(hash))
      } yield ()

      votingTask.runSyncUnsafe()
      S.redirectTo("/results")

    case _ => PassThru
  }
}
