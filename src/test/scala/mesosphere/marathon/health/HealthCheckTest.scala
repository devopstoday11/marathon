package mesosphere.marathon.health

import mesosphere.marathon.Protos
import Protos.HealthCheckDefinition.Protocol

import org.junit.Test
import org.junit.Assert._

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

import java.util.concurrent.TimeUnit.SECONDS
import javax.validation.Validation

class HealthCheckTest {

  @Test
  def testToProto() {
    val healthCheck = HealthCheck(
      path = Some("/health"),
      protocol = Protocol.HTTP,
      acceptableResponses = Some(Set(200)),
      portIndex = 0,
      initialDelay = FiniteDuration(10, SECONDS),
      interval = FiniteDuration(60, SECONDS)
    )

    val proto = healthCheck.toProto

    assertEquals("/health", proto.getPath)
    assertEquals(Protocol.HTTP, proto.getProtocol)
    assertEquals(Set(200), proto.getAcceptableResponsesList.asScala.toSet)
    assertEquals(0, proto.getPortIndex)
    assertEquals(10, proto.getInitialDelaySeconds)
    assertEquals(60, proto.getIntervalSeconds)
  }

  @Test
  def testMergeFromProto() {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setPath("/health")
      .setProtocol(Protocol.HTTP)
      .addAllAcceptableResponses(Set(200).map(i => i: Integer).asJava)
      .setPortIndex(0)
      .setInitialDelaySeconds(10)
      .setIntervalSeconds(60)
      .setTimeoutSeconds(10)
      .build

    val mergeResult = HealthCheck().mergeFromProto(proto)

    val expectedResult = HealthCheck(
      path = Some("/health"),
      protocol = Protocol.HTTP,
      acceptableResponses = Some(Set(200)),
      portIndex = 0,
      initialDelay = FiniteDuration(10, SECONDS),
      interval = FiniteDuration(60, SECONDS),
      timeout = FiniteDuration(10, SECONDS)
    )

    assertEquals(mergeResult, expectedResult)
  }

  @Test
  def testSerializationRoundtrip() {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import mesosphere.marathon.api.v2.json.MarathonModule

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(new MarathonModule)

    val original = HealthCheck()
    val json = mapper.writeValueAsString(original)
    val readResult = mapper.readValue(json, classOf[HealthCheck])

    println("original [%s]" format original)
    println("original JSON: %s" format json)
    println("readResult [%s]" format readResult)

    assertTrue(readResult == original)
  }

}