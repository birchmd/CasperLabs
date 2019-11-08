package io.casperlabs.casper.util.execengine

import cats.Applicative
import cats.effect.Sync
import cats.implicits._
import com.google.protobuf.ByteString
import io.casperlabs.casper
import io.casperlabs.casper.CasperMetricsSource
import io.casperlabs.casper.consensus.state.{Unit => _, _}
import io.casperlabs.casper.consensus.{Block, Bond}
import io.casperlabs.casper.util.{CasperLabsProtocolVersions, ProtoUtil}
import io.casperlabs.casper.util.execengine.ExecEngineUtil.StateHash
import io.casperlabs.casper.validation.{Validation, ValidationImpl}
import io.casperlabs.crypto.Keys.PublicKey
import io.casperlabs.ipc._
import io.casperlabs.metrics.Metrics.MetricsNOP
import io.casperlabs.models.SmartContractEngineError
import io.casperlabs.shared.{Log, Time}
import io.casperlabs.smartcontracts.ExecutionEngineService
import io.casperlabs.storage.block._
import io.casperlabs.storage.dag._

import scala.concurrent.duration.FiniteDuration
import scala.util.Either

object ExecutionEngineServiceStub {
  type Bonds = Map[PublicKey, Long]

  implicit def functorRaiseInvalidBlock[F[_]: Sync] =
    casper.validation.raiseValidateErrorThroughApplicativeError[F]

  def validateBlockCheckpoint[F[_]: Sync: Log: BlockStorage: ExecutionEngineService: CasperLabsProtocolVersions](
      b: Block,
      dag: DagRepresentation[F]
  ): F[Either[Throwable, StateHash]] = {
    implicit val time = new Time[F] {
      override def currentMillis: F[Long]                   = 0L.pure[F]
      override def nanoTime: F[Long]                        = 0L.pure[F]
      override def sleep(duration: FiniteDuration): F[Unit] = Sync[F].unit
    }
    implicit val metrics    = new MetricsNOP[F]
    implicit val validation = new ValidationImpl[F]
    (for {
      parents <- ProtoUtil.unsafeGetParents[F](b)
      merged  <- ExecEngineUtil.merge[F](parents, dag)
      preStateHash <- ExecEngineUtil
                       .computePrestate[F](merged, rank = b.getHeader.rank, upgrades = Nil)
      effects <- ExecEngineUtil.effectsForBlock[F](b, preStateHash)
      _       <- Validation[F].transactions(b, preStateHash, effects)
    } yield ProtoUtil.postStateHash(b)).attempt
  }

  def mock[F[_]](
      runGenesisWithChainSpecFunc: (
          ChainSpec.GenesisConfig
      ) => F[Either[Throwable, GenesisResult]],
      upgradeFunc: (
          ByteString,
          ChainSpec.UpgradePoint,
          ProtocolVersion
      ) => F[Either[Throwable, UpgradeResult]],
      execFunc: (
          ByteString,
          Long,
          Seq[DeployItem],
          ProtocolVersion
      ) => F[Either[Throwable, Seq[DeployResult]]],
      commitFunc: (
          ByteString,
          Seq[TransformEntry]
      ) => F[Either[Throwable, ExecutionEngineService.CommitResult]],
      queryFunc: (ByteString, Key, Seq[String]) => F[Either[Throwable, Value]]
  ): ExecutionEngineService[F] = new ExecutionEngineService[F] {
    override def emptyStateHash: ByteString = ByteString.EMPTY
    override def runGenesis(
        genesisConfig: ChainSpec.GenesisConfig
    ): F[Either[Throwable, GenesisResult]] =
      runGenesisWithChainSpecFunc(genesisConfig)

    override def upgrade(
        prestate: ByteString,
        upgrade: ChainSpec.UpgradePoint,
        protocolVersion: ProtocolVersion
    ): F[Either[Throwable, UpgradeResult]] =
      upgradeFunc(prestate, upgrade, protocolVersion)

    override def exec(
        prestate: ByteString,
        blocktime: Long,
        deploys: Seq[DeployItem],
        protocolVersion: ProtocolVersion
    ): F[Either[Throwable, Seq[DeployResult]]] =
      execFunc(prestate, blocktime, deploys, protocolVersion)
    override def commit(
        prestate: ByteString,
        effects: Seq[TransformEntry],
        protocolVersion: ProtocolVersion
    ): F[Either[Throwable, ExecutionEngineService.CommitResult]] = commitFunc(prestate, effects)

    override def query(
        state: ByteString,
        baseKey: Key,
        path: Seq[String],
        protocolVersion: ProtocolVersion
    ): F[Either[Throwable, Value]] = queryFunc(state, baseKey, path)
  }

  def noOpApi[F[_]: Applicative](): ExecutionEngineService[F] =
    mock[F](
      (_) => GenesisResult().asRight[Throwable].pure[F],
      (_, _, _) => UpgradeResult().asRight[Throwable].pure[F],
      (_, _, _, _) => Seq.empty[DeployResult].asRight[Throwable].pure[F],
      (_, _) =>
        ExecutionEngineService
          .CommitResult(ByteString.EMPTY, Seq.empty[Bond])
          .asRight[Throwable]
          .pure[F],
      (_, _, _) =>
        Applicative[F]
          .pure[Either[Throwable, Value]](Left(new SmartContractEngineError("unimplemented")))
    )

}
