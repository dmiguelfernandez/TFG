import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import scala.math.max
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat;
import java.util.Date;
import scala.util.control._
import java.util.concurrent.TimeUnit
import java.io.File
import java.io.PrintWriter

val rdd = sc.cassandraTable[Housekeeping]("upmsat2db", "housekeeping").collect()
var rdd2 = sc.cassandraTable[Hello]("upmsat2db", "hello").collect()
val rdd3 = sc.cassandraTable[EventError]("upmsat2db", "eventerror").collect()


val x = rdd.size
val xx = rdd2.size
val xxx = rdd3.size
