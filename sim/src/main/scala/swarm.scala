package sim

import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Random}
import cats.effect.syntax.spawn.*
import cats.effect.{Fiber, Spawn}
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import cats.{Applicative, Monad, Show}
import fs2.Stream

trait GenAddr[F[_]]:
  def newAddr: F[Addr]

object GenAddr:
  def apply[F[_]](using ev: GenAddr[F]): GenAddr[F] = ev

final class PeekAddrFromSet[F[_]](addrs: AtomicCell[F, Set[Addr]]) extends GenAddr[F]:
  override def newAddr: F[Addr] = addrs.modify(as =>
    val addr = as.max
    as - addr -> addr
  )

object PeekAddrFromSet:
  def make[F[_]: Async](addrs: Set[Addr]): F[PeekAddrFromSet[F]] =
    for addrsCell <- AtomicCell[F].of(addrs) yield new PeekAddrFromSet(addrsCell)

final case class NetworkStats(numConnections: Int, numMessagesTransmited: Int)
object NetworkStats:
  given Show[NetworkStats] = Show.show(s =>
    s"NetworkStats(numConnections=${s.numConnections}, numMessagesTransmited=${s.numMessagesTransmited})"
  )

trait Swarm[M, F[_]]:
  def spawn(agent: Agent[M, F]): F[Unit]
  def kill(addr: Addr): F[Boolean]
  def stats: F[NetworkStats]

object Swarm:
  def make[M, F[_]: Async: Spawn]: F[Swarm[M, F]] =
    for
      agents         <- AtomicCell[F].of(Map.empty[Addr, (Agent[M, F], Fiber[F, Throwable, Unit])])
      conns          <- AtomicCell[F].empty[Set[(Addr, Addr)]]
      msgTransmitted <- AtomicCell[F].of(0)
    yield new Live(agents, conns, msgTransmitted)

  class Live[M, F[_]: Async: Spawn](
      agents: AtomicCell[F, Map[Addr, (Agent[M, F], Fiber[F, Throwable, Unit])]],
      connections: AtomicCell[F, Set[(Addr, Addr)]],
      msgTransmitted: AtomicCell[F, Int]
  ) extends Swarm[M, F]:

    override def spawn(agent: Agent[M, F]): F[Unit] =
      for
        addr      <- agent.getAddr
        runAgentF <- runAgent(addr, agent).compile.drain.start
        _         <- agents.update(_.updated(addr, (agent, runAgentF)))
        _         <- Async[F].delay(println(s"INFO: [Swarm] spawned $addr"))
      yield ()

    override def kill(addr: Addr): F[Boolean] =
      agents.get.map(_.get(addr).map(_._2)).flatMap {
        case Some(fiber) => fiber.cancel.map(_ => true)
        case None        => false.pure[F]
      }

    override def stats: F[NetworkStats] =
      for
        conns <- connections.get.map(_.size)
        msgs  <- msgTransmitted.get
      yield NetworkStats(conns, msgs)

    private def route(srcAddr: Addr, destAddr: Addr, out: AgentOut[M]): F[Unit] =
      if (srcAddr != destAddr)
        agents.get.flatMap { nodes =>
          nodes.get(destAddr).map(_._1) match
            case Some(agent) =>
              out match
                case SendMessage(m) =>
                  connectionExists(srcAddr, destAddr).ifM(
                    countMessage() >> agent.injectMessage(srcAddr, m),
                    Async[F].delay(println("WARN: [Swarm] no route exists"))
                  )
                case Connect(handshake) =>
                  connectionExists(srcAddr, destAddr).ifM(
                    Async[F].delay(println("WARN: [Swarm] route already exists")),
                    countMessage() >> agent.injectConnReq(srcAddr, handshake).flatMap {
                      case Left(_) => ().pure[F]
                      case Right(handshake) =>
                        createConnection(srcAddr, destAddr) >>
                          nodes
                            .get(srcAddr)
                            .traverse((agent, _) =>
                              agent.injectConnEstablished(destAddr, handshake)
                            )
                            .void
                    }
                  )
                case Disconnect() =>
                  breakConnection(srcAddr, destAddr) >> agent.injectConnLost(srcAddr)
            case None => Async[F].delay(println("WARN: [Swarm] message cannot be delivered"))
        }
      else
        ().pure

    private def runAgent(addr: Addr, agent: Agent[M, F]): Stream[F, Unit] =
      Stream
        .repeatEval(agent.poll)
        .unNone
        .evalMap((destAddr, out) => route(addr, destAddr, out))

    private def connectionExists(a: Addr, b: Addr): F[Boolean] =
      connections.get.map(cs => cs.contains(a -> b) || cs.contains(b -> a))

    private def createConnection(a: Addr, b: Addr): F[Unit] =
      connections.update(_ + (a -> b))

    private def breakConnection(a: Addr, b: Addr): F[Unit] =
      connections.update(cs => cs - (a -> b)) >>
        connections.update(cs => cs - (b -> a))

    private def countMessage(): F[Unit] =
      msgTransmitted.update(_ + 1)
