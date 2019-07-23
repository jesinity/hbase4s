package io.github.hbase4s

import io.github.hbase4s.config.HBaseExternalConfig
import io.github.hbase4s.utils.HBaseImplicitUtils._
import io.github.hbase4s.utils.HBaseTesting
import org.scalatest.{FlatSpec, Matchers}
import io.github.hbase4s.filter._
import org.apache.hadoop.hbase.HBaseTestingUtility

class HBaseOpsTest extends FlatSpec with Matchers {

  private[this] val utility: HBaseTestingUtility = HBaseTesting.hBaseServer
  private[this] val TestTable = "test_table"
  private[this] val Fam1 = "fox_family"
  private[this] val Fam2 = "dog_family"
  private[this] val Fam3 = "event"
  private[this] val Fam4 = "withopt"
  private[this] val F1 = "field1"
  private[this] val F2 = "field2"
  private[this] val Families: Array[Array[Byte]] = Array(
    Fam1.getBytes(),
    Fam2.getBytes(),
    Fam3.getBytes(),
    Fam4.getBytes()
  )

  utility.createTable(TestTable, Families, 1)

  case class Test2Field[T](key: T, field1: String, field2: String)

  val Max = 1000
  private val config = new HBaseExternalConfig(utility.getConfiguration)

  "It" should "perform put and scan with string key" in {

    import HBaseOps._

    withConnection(config) { connection =>
      withTable(connection, TestTable) { implicit table =>
        (1 to Max).foreach { i =>
          putFields(s"key_$i",
                    List(Field(Fam1, F1, s"value_$i"),
                         Field(Fam2, F2, "value_2")))
        }
        val res = scanAllAsStr
        res.size shouldBe Max

        val res2 = scan[String](s"($Fam1:$F1=value_20 )")
          .map(
            wr =>
              Test2Field(wr.key,
                         wr.asString(s"$Fam1:$F1"),
                         wr.asString(s"$Fam2:$F2")))
        res2.size shouldBe 1
        res2.headOption.map(_.field1 shouldBe "value_20")
      }
    }
  }
}
