package cache

import cats.Monad
import cats.effect.{Async, Deferred, Ref}
import cats.effect.std.Console

import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.HashMap
import cats.syntax.all._

trait Cache[F[_], K, V] {
  def put(key: K, value: V): F[Unit]
  def get(key: K): F[Option[V]]
}

case class SyncCache[F[_]: Monad: Console, K, V, E](
    maxSize: Long,
    ttl: FiniteDuration,
    rttl: FiniteDuration, // refreshAfterWriterTtl
    load: K => F[Either[E, V]],
    private val resourceManager: AsyncResourceManager[F, K, V, E]
) extends Cache[F, K, V] {

  override def put(key: K, value: V): F[Unit] =
    Console[F].println(s"Inserting: ($key, $value)") >> resourceManager.put(key, value)

  override def get(key: K): F[Option[V]] = {
    val getTime = System.currentTimeMillis()
    for {
      pair <- resourceManager.get(key)
      vLoad <- pair match {
        case Some((v, time)) =>
          if (getTime - time > ttl.toMillis) syncLoadValueAndReturn(key)
          else if (getTime - time > rttl.toMillis) syncLoadValue(key) >> Monad[F].pure(Right(v))
          else resourceManager.put(key, v) >> Monad[F].pure(Right(v))
        case None => load(key)
      }
      vOption = vLoad match {
        case Right(v) => Some(v)
        case Left(_)  => None
      }
    } yield vOption
  }

  private def syncLoadValueAndReturn(key: K): F[Either[E, V]] = {
    for {
      newVal <- load(key)
      _ <- newVal match {
        case Right(v) => resourceManager.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield newVal
  }

  private def syncLoadValue(key: K): F[Unit] = {
    for {
      newVal <- load(key)
      _ <- newVal match {
        case Right(v) => resourceManager.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield ()
  }
}

object SyncCache {
  def apply[F[_]: Async: Ref.Make: Console, K, V, E](
      maxSize: Long,
      ttl: FiniteDuration,
      rttl: FiniteDuration,
      load: K => F[Either[E, V]]
  ): F[SyncCache[F, K, V, E]] = {

    type A = Either[E, V]

    for {
      data      <- Ref.of[F, HashMap[K, (V, Long)]](HashMap.empty)
      timeStamp <- Ref.of[F, HashMap[Long, K]](HashMap.empty)
      keysForLoad <- Ref.of[F, HashMap[K, (Boolean, Deferred[F, F[A]])]](HashMap.empty)
      loader  = Loader(load, keysForLoad)
      storage = AsyncResourceManager(maxSize, data, timeStamp, loader)
    } yield new SyncCache(maxSize, ttl, rttl, load, storage)
  }
}
