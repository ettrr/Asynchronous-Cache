package cache

import cats.Monad
import cats.effect.std.{Console, Supervisor}
import cats.effect.{Async, Deferred, Ref, Resource}

import scala.concurrent.duration.FiniteDuration
import cats.syntax.all._

import scala.collection.immutable.HashMap

case class AsyncCache[F[_]: Monad: Console, K, V, E](
    maxSize: Long,
    ttl: FiniteDuration,
    rttl: FiniteDuration, // refreshAfterWriterTtl
    private val resourceManager: AsyncResourceManager[F, K, V, E],
    private val supervisor: Supervisor[F]

) extends Cache[F, K, V] {

  override def put(key: K, value: V): F[Unit] =
    Console[F].println(s"Inserting: ($key, $value)") >> resourceManager.put(key, value)

  private def getValue(pair: Option[(V, Long)], gettingTime: Long, key: K): F[Either[E, V]] = {
    pair match {
      case Some((v, time)) =>
        if (gettingTime - time > ttl.toMillis) asyncLoadValueAndReturn(key)
        else if (gettingTime - time > rttl.toMillis) asyncLoadValue(key) >> Monad[F].pure(Right(v))
        else resourceManager.put(key, v) >> Monad[F].pure(Right(v))
      case None => asyncLoadValueAndReturn(key)
    }
  }

  def getEither(key: K): F[Either[E, V]] = {
    val gettingTime = System.currentTimeMillis()
    for {
      pair <- resourceManager.get(key)
      v    <- getValue(pair, gettingTime, key)
    } yield v
  }

  override def get(key: K): F[Option[V]] = {
    val gettingTime = System.currentTimeMillis()
    for {
      pair    <- resourceManager.get(key)
      vEither <- getValue(pair, gettingTime, key)
      vOption = vEither match {
        case Right(v) => Some(v)
        case Left(_)  => None
      }
    } yield vOption
  }

  private def asyncLoadValueAndReturn(key: K): F[Either[E, V]] = {
    for {
      newVal <- resourceManager.asyncLoad(key)
      _ <- newVal match {
        case Right(v) => resourceManager.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield newVal
  }

  private def asyncLoadValue(key: K): F[Unit] = {
    for {
      _ <-
        supervisor.supervise {
          resourceManager.asyncLoad(key).flatMap {
            case Right(v) => resourceManager.put(key, v)
            case Left(_)  => Monad[F].unit
          }
        }.void
    } yield ()
  }
}

object AsyncCache {
  def apply[F[_]: Async: Console, K, V, E](
      maxSize: Long,
      ttl: FiniteDuration,
      rttl: FiniteDuration,
      load: K => F[Either[E, V]]
  ): Resource[F, AsyncCache[F, K, V, E]] = {

    type A = Either[E, V]

    Supervisor[F](await = true).flatMap { supervisor =>
      for {
        data      <- Resource.eval(Ref.of[F, HashMap[K, (V, Long)]](HashMap.empty))
        timeStamp <- Resource.eval(Ref.of[F, HashMap[Long, K]](HashMap.empty))
        keysForLoad <- Resource.eval(
          Ref.of[F, HashMap[K, (Boolean, Deferred[F, F[A]])]](HashMap.empty)
        )
        loader  = Loader(load, keysForLoad)
        storage = AsyncResourceManager(maxSize, data, timeStamp, loader)
      } yield new AsyncCache(maxSize, ttl, rttl, storage, supervisor)
    }
  }
}
