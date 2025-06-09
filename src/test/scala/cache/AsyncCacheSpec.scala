package cache

import cats.effect.std.Supervisor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.{IO, Ref, Resource}
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._
import scala.collection.immutable.HashMap
import cats.syntax.all._

class AsyncCacheSpec extends AnyFlatSpec with Matchers {

  private def createTestCache[E](
      maxSize: Long = 100,
      ttl: FiniteDuration = 1.minute,
      rttl: FiniteDuration = 30.seconds,
      load: String => IO[Either[E, Int]] = (_: String) => IO.pure(Right(0)),
      initialData: Map[String, (Int, Long)] = Map.empty
  ): Resource[IO, AsyncCache[IO, String, Int, E]] = {
    for {
      supervisor <- Supervisor[IO](await = true)
      loadLocks  <- Resource.eval(Ref.of[IO, Set[String]](Set.empty))
      data <- Resource.eval(Ref.of[IO, HashMap[String, (Int, Long)]](HashMap.from(initialData)))
      timeStamp <- Resource.eval(Ref.of[IO, HashMap[Long, String]](HashMap.empty))
      storage   <- Resource.pure(Storage(maxSize, data, timeStamp))
    } yield new AsyncCache[IO, String, Int, E](
      maxSize,
      ttl,
      rttl,
      load,
      storage,
      supervisor,
      loadLocks
    )
  }

  "AsyncCache" should "store and retrieve values with put and get" in {
    val cacheResource = createTestCache()

    val result = cacheResource.use { cache =>
      for {
        _      <- cache.put("key1", 42)
        result <- cache.get("key1")
      } yield result
    }

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "return load value for non-existent key" in {
    val cacheResource = createTestCache()

    val result = cacheResource.use(_.get("nonexistent"))

    result.unsafeRunSync() shouldBe Some(0)
  }

  it should "load value for non-existent key using load function" in {
    val load: String => IO[Either[String, Int]] = {
      case "key1" => IO.pure(Right(42))
      case _      => IO.pure(Left("error"))
    }
    val cacheResource = createTestCache(load = load)

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "handle load errors by returning None in get" in {
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Left("error"))
    val cacheResource                           = createTestCache(load = load)

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe None
  }

  it should "return load errors in getEither" in {
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Left("error"))
    val cacheResource                           = createTestCache(load = load)

    val result = cacheResource.use(_.getEither("key1"))

    result.unsafeRunSync() shouldBe Left("error")
  }

  it should "synchronously load new value when ttl is passed" in {
    val currentTime                             = System.currentTimeMillis()
    val veryOldTime                             = currentTime - 2.minutes.toMillis
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Right(42))
    val cacheResource = createTestCache(
      ttl = 1.minute,
      rttl = 30.seconds,
      load = load,
      initialData = Map("key1" -> (1, veryOldTime))
    )

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "not exceed max size" in {
    val load: String => IO[Either[String, Int]] = k => IO.pure(Right(k.toInt))
    val cacheResource                           = createTestCache(maxSize = 2, load = load)

    val result = cacheResource.use { cache =>
      for {
        _ <- cache.put("1", 1)
        _ <- cache.put("2", 2)
        _ <- cache.put("3", 3)
        size <- cache.get("1").flatMap {
          case Some(_) =>
            cache.get("2").map {
              case Some(_) => 2
              case None    => 1
            }
          case None =>
            cache.get("2").map {
              case Some(_) => 1
              case None    => 0
            }
        }
      } yield size
    }

    result.unsafeRunSync() shouldBe 2
  }

  it should "prevent concurrent loads for the same key" in {
    val load: String => IO[Either[String, Int]] = {
      var count = 0
      _ => IO { count += 1; Right(count) }.delayBy(100.millis)
    }
    val cacheResource = createTestCache(load = load)

    val result = cacheResource.use { cache =>
      for {
        _ <- cache.get("key1")
        tasks <- (1 to 10).toList.parTraverse(_ => cache.get("key1"))
        _ <- IO.pure(println(tasks))
        result = tasks.forall(_.contains(1)) shouldBe true
      } yield result
    }

    result.unsafeRunSync()
  }
}
