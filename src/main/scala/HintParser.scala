import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import scala.io.Source
import java.io.{File, PrintWriter,FileWriter}

class HintParser(session: SparkSession, parser: ParserInterface) extends ParserInterface with Logging {
  override def parsePlan(sqlText: String): LogicalPlan = {
    session.conf.unset("spark.sql.optimizer.excludedRules")
    logInfo("Unset spark.sql.optimizer.excludedRules")
    logInfo("Parsing SQL text with HintParser.")

    // 读取文件内容
    val fileSource = Source.fromFile("/root/best_tpcds_sf100_0622.csv")
    val csvLines = fileSource.getLines().toList
    fileSource.close()
    var inputSql = sqlText.replaceAll("\\s+", " ").trim
    var flag = inputSql.matches("(?i).*\\bselect\\b.*\\bfrom\\b.*")
    logInfo(inputSql)
    if (flag) {
      // 遍历CSV文件内容，查找是否存在相同的SQL语句
      logInfo("This is a legal SQL, finding matches...")
      var found = false
      csvLines.foreach { line =>
        val cols = line.split(":")
        var sqlText1 = cols(1)
        if (sqlText1.startsWith("\"") && sqlText1.endsWith("\"")) {
          sqlText1 = sqlText1.substring(1, sqlText1.length - 1)
        }
        if (sqlText1 == inputSql) {
          logInfo("Match found")
          found = true
          var rewrite = cols(2)
          var knobs = cols(3)
          var schema = cols(4)
          if (rewrite.startsWith("\"") && rewrite.endsWith("\"")) {
            rewrite = rewrite.substring(1, rewrite.length - 1)
          }
          if (knobs.startsWith("\"") && knobs.endsWith("\"")) {
            knobs = knobs.substring(1, knobs.length - 1)
          }
          if (schema.startsWith("\"") && schema.endsWith("\"")) {
            schema = schema.substring(1, schema.length - 1)
          }
          if (rewrite != "None") {
            inputSql = rewrite
          }
          // 处理hint
          val hints = knobs.split(",")
          if (hints(0) != "None") {
            session.conf.set("spark.sql.optimizer.excludedRules", hints(0))
            var res = session.conf.get("spark.sql.optimizer.excludedRules")
            logInfo(res)
          }
          if (hints(1) != "None") {
            session.conf.set("spark.sql.optimizer.excludedRules", hints(1))
            var res = session.conf.get("spark.sql.optimizer.excludedRules")
            logInfo(res)
          }
          if (hints(2) != "None") {
            session.conf.set("spark.sql.optimizer.excludedRules", hints(2))
            var res = session.conf.get("spark.sql.optimizer.excludedRules")
            logInfo(res)
          }
          if (hints(3) != "None") {
            session.conf.set("spark.sql.optimizer.excludedRules", hints(3))
            var res = session.conf.get("spark.sql.optimizer.excludedRules")
            logInfo(res)
          }
          if (hints(4) != "None") {
            session.conf.set("spark.sql.optimizer.excludedRules", hints(4))
            var res = session.conf.get("spark.sql.optimizer.excludedRules")
            logInfo(res)
          }
          // for (i <- 0 until hints.length) {
          //  logInfo(hints(i))
          // session.conf.set("spark.sql.optimizer.excludedRules", hints(i))
          //  logInfo("asdjkcnj d")
          // }
          // hints.foreach { hint =>
          // logInfo(hint)
          // session.conf.set("spark.sql.optimizer.excludedRules", hint)
          // for (hint <- hints) {
          // logInfo(hint)
          // session.conf.set("spark.sql.optimizer.excludedRules", hint)
          // }
        }
      }

      if (!found) {
        // 如果未找到相同的SQL语句，则将该SQL语句添加到CSV文件中
        val newId = csvLines.length - 1
        val quotedSqlText = "\"" + inputSql + "\""
        val newLine =
          s"$newId:$quotedSqlText:\"None\":\"None,None,None,None,None\":\"None\""
        val pw = new PrintWriter(
          new FileWriter(new File("/root/best_tpcds_sf100_0622.csv"), true)
        )
        pw.println(newLine)
        pw.close()
        logInfo("No Optimization strategy for this SQl, added it to experience pool.")
      
    }
  }
    parser.parsePlan(inputSql)
  }

  override def parseExpression(sqlText: String): Expression =
    parser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    parser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    parser.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    parser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    parser.parseDataType(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    parser.parseMultipartIdentifier(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    parser.parseQuery(sqlText)
}

class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    val parserBuilder: ParserBuilder = (session, parser) =>
      new HintParser(session, parser)
    extensions.injectParser(parserBuilder)
  }
}

