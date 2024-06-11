import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.SparkSessionExtensions

import scala.util.{Failure, Success, Try}

class HintParser(session: SparkSession, parser: ParserInterface) extends ParserInterface with Logging {
  override def parsePlan(sqlText: String): LogicalPlan = {
    logInfo("Parsing SQL text with HintParser.")
    // 读取文件内容
    val fileSource = scala.io.Source.fromFile("path/to/file")
    val fileContent = fileSource.getLines().mkString("\n")
    fileSource.close()
    logInfo(fileContent)
    // 这里可以加Broadcast Hint
    var res = session.conf.get("spark.sql.optimizer.excludedRules")
    logInfo(res)
    session.conf.set("spark.sql.optimizer.excludedRules", "ReorderJoin")
    res = session.conf.get("spark.sql.optimizer.excludedRules")
    logInfo("Set ReorderJoin off.")
    logInfo(res)
    parser.parsePlan(sqlText)
  }
  
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)
  
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)
  
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)
  
  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)
  
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
  
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)
  
  override def parseQuery(sqlText: String): LogicalPlan = parser.parseQuery(sqlText)
}

class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    val parserBuilder: ParserBuilder = (session, parser) => new HintParser(session, parser)
    extensions.injectParser(parserBuilder)
  }
}
