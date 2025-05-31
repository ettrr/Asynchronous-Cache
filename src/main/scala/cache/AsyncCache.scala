package cache

import cats.Monad
import cats.effect.std.{Console, Supervisor}
import cats.effect.{Async, Ref, Resource}

import scala.concurrent.duration.FiniteDuration
import cats.syntax.all._

import scala.collection.immutable.{HashMap, HashSet}

case class AsyncCache[F[_]: Ref.Make: Monad: Console, K, V, E](
    maxSize: Long,
    ttl: FiniteDuration,
    rttl: FiniteDuration, // refreshAfterWriterTtl
    load: K => F[Either[E, V]],
    private val storage: Storage[F, K, V],
    private val supervisor: Supervisor[F],
    private val loadLocks: Ref[F, Set[K]]
) extends Cache[F, K, V] {

  override def put(key: K, value: V): F[Unit] =
    Console[F].println(s"Inserting: ($key, $value)") >> storage.put(key, value)

  private def getValue(pair: Option[(V, Long)], gettingTime: Long, key: K): F[Either[E, V]] = {
    pair match {
      case Some((v, time)) =>
        if (gettingTime - time > ttl.toMillis) syncLoadValueAndReturn(key)
        else if (gettingTime - time > rttl.toMillis) asyncLoadValue(key) >> Monad[F].pure(Right(v))
        else storage.put(key, v) >> Monad[F].pure(Right(v))
      case None => load(key)
    }
  }

  def getEither(key: K): F[Either[E, V]] = {
    val gettingTime = System.currentTimeMillis()
    for {
      pair <- storage.get(key)
      v    <- getValue(pair, gettingTime, key)
    } yield v
  }

  override def get(key: K): F[Option[V]] = {
    val gettingTime = System.currentTimeMillis()
    for {
      pair    <- storage.get(key)
      vEither <- getValue(pair, gettingTime, key)
      vOption = vEither match {
        case Right(v) => Some(v)
        case Left(_)  => None
      }
    } yield vOption
  }

  private def syncLoadValueAndReturn(key: K): F[Either[E, V]] = {
    for {
      newVal <- load(key)
      _ <- newVal match {
        case Right(v) => storage.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield newVal
  }

  private def asyncLoadValue(key: K): F[Unit] = {
    for {
      locks <- loadLocks.get
      _ <-
        if (!locks.contains(key)) {
          supervisor.supervise {
            loadLocks.update(_ + key) >>
              load(key).flatMap {
                case Right(v) => storage.put(key, v)
                case Left(_)  => Monad[F].unit
              } >> loadLocks.update(_ - key)
          }.void
        } else Monad[F].unit
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
    Supervisor[F](await = true).flatMap { supervisor =>
      for {
        loadLocks <- Resource.eval(Ref.of[F, Set[K]](HashSet.empty))
        data      <- Resource.eval(Ref.of[F, HashMap[K, (V, Long)]](HashMap.empty))
        timeStamp <- Resource.eval(Ref.of[F, HashMap[Long, K]](HashMap.empty))
        storage = Storage(maxSize, data, timeStamp)
      } yield new AsyncCache(maxSize, ttl, rttl, load, storage, supervisor, loadLocks)
    }
  }
}
