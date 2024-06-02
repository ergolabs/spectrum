package sim

import cats.Monad
import cats.effect.kernel.{Async, Clock}
import cats.effect.std.{AtomicCell, Queue}
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*

import scala.concurrent.duration.*

case class Addr(v: Long)

object Addr:
  given Ordering[Addr] = Ordering.Long.on(_.v)

sealed trait AgentOut[M]
case class SendMessage[M](m: M)     extends AgentOut[M]
case class Connect[M](handshake: M) extends AgentOut[M]
case class Disconnect[M]()          extends AgentOut[M]

case class Reject()

trait Agent[M, F[_]]:
  def getAddr: F[Addr]
  def injectMessage(srcAddr: Addr, m: M): F[Unit]
  def injectConnReq(peerAddr: Addr, handshake: M): F[Either[Reject, M]]
  def injectConnLost(peerAddr: Addr): F[Unit]
  def injectConnEstablished(peerAddr: Addr, handshake: M): F[Unit]
  def poll: F[Option[(Addr, AgentOut[M])]]

sealed trait PingMessage
object PingMessage:
  case class Ping(ts: Long) extends PingMessage
  case class Pong(ts: Long) extends PingMessage
  case class Handshake()    extends PingMessage

final class Ping[F[_]: Async: Clock](
    selfAddr: Addr,
    pingInterval: FiniteDuration,
    knownPeers: Set[Addr],
    pendingOutputs: Queue[F, (Addr, AgentOut[PingMessage])],
    pings: AtomicCell[F, Map[Addr, FiniteDuration]]
) extends Agent[PingMessage, F]:

  override def getAddr: F[Addr] = selfAddr.pure

  override def injectMessage(srcAddr: Addr, m: PingMessage): F[Unit] =
    for
      _ <- log("info", s"got message [$m] from peer [$srcAddr]")
      _ <- m match
        case PingMessage.Ping(ts) =>
          pendingOutputs.offer(srcAddr -> SendMessage(PingMessage.Pong(ts)))
        case PingMessage.Pong(_) | PingMessage.Handshake() => ().pure
    yield ()

  override def injectConnReq(
      peerAddr: Addr,
      handshake: PingMessage
  ): F[Either[Reject, PingMessage]] =
    Right(PingMessage.Handshake()).pure

  override def injectConnLost(peerAddr: Addr): F[Unit] = ().pure

  override def injectConnEstablished(peerAddr: Addr, handshake: PingMessage): F[Unit] =
    for
      _ <- log("info", s"conn established with peer [$peerAddr]")
      _ <- Clock[F].realTime.flatMap(now => pings.update(_.updated(peerAddr, now)))
    yield ()

  override def poll: F[Option[(Addr, AgentOut[PingMessage])]] =
    for
      selfAddr <- getAddr
      _ <- knownPeers.toList
        .filterNot(_ == selfAddr)
        .traverse_(addr =>
          pings.get
            .map(_.toList.map(_._1).contains(addr))
            .ifM(
              ().pure[F],
              log("info", s"going to connect to [$addr]") >> pendingOutputs.offer(
                addr -> Connect(PingMessage.Handshake())
              )
            )
        )
      _ <- pings.evalUpdate(ps =>
        ps.toList
          .traverse((addr, ts) =>
            for
              now <- Clock[F].realTime
              r <-
                if (now >= ts + pingInterval)
                  log("info", s"pinging [$addr]") >>
                  pendingOutputs
                    .offer(addr -> SendMessage(PingMessage.Ping(now.toMillis)))
                    .map(_ => addr -> now)
                else (addr, ts).pure
            yield r
          )
          .map(_.toMap)
      )
      out <- pendingOutputs.tryTake
    yield out

  def log(level: String, msg: String): F[Unit] =
    for
      selfAddr <- getAddr
      _        <- Async[F].delay(println(s"${level.toUpperCase()}: [$selfAddr] $msg"))
    yield ()

object Ping:
  def make[F[_]: Async: Clock](
      selfAddr: Addr,
      knownPeers: Set[Addr],
      pingInterval: FiniteDuration
  ): F[Ping[F]] =
    for
      outs  <- Queue.unbounded[F, (Addr, AgentOut[PingMessage])]
      pings <- AtomicCell[F].empty[Map[Addr, FiniteDuration]]
    yield new Ping(selfAddr, pingInterval, knownPeers, outs, pings)
