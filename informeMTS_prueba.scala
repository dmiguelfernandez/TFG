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
import java.util.concurrent.TimeUnit
import java.io.File
import java.io.PrintWriter


case class Mtsexperiment (tm_type: String,time_received: String,current_essay: String,current_step :Int,current_verification: String,is_at_essays :Boolean,mission_clock :Int,mts1: Boolean,mts2: Boolean,mts3: Boolean,mts_p1tts1: Int,mts_p1tts2: Int,mts_p1tts3: Int,mts_p1tts4: Int,mts_p1tts5: Int,mts_p1tts6: Int,sequencecount: Int,timestamps: Int,tm_id: BigInt)


val rdd = sc.cassandraTable[Mtsexperiment]("upmsat2db", "mtsexperiment").collect()

printf("\n\n\n")

if(rdd.size>0){printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>DATOS RECOGIDOS CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
}else {printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ERROR AL RECOGER LOS DATOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")}

val x = rdd.size

val informe = new File("C:/Users/chech/Desktop/TFG/VERSION_NUEVA/PROGRAMAS_SCALA/CODIGO/informe3.txt")

val writer = new PrintWriter(informe)
writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INICIO DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


val tiemposMTS = ArrayBuffer[String]()
for (y <- 0 until x){
    tiemposMTS += rdd(y).time_received
}


val indices = ArrayBuffer[Int]()


//------------------------------------------------------------------
val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

def convertStringToDate(s: String): Date = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.parse(s)
}
//-------------------------------------------------------------------

var tiemposMTSLength = tiemposMTS.length

//--------------------------------------------MEDIDOR DE DIFERENCIAS ENTRE TIEMPOS------------------------------------------------------
indices += 0
for (y <- 0 until tiemposMTSLength-1){

    var fff = Math.abs((convertStringToDate(tiemposMTS(y))).getTime() - (convertStringToDate(tiemposMTS(y+1))).getTime())
    var fz = TimeUnit.MILLISECONDS.toMinutes(fff)
    if (fz > 15) {

      indices += y
      indices += y+1
    }
}
indices += x-1
//--------------------------------------------------------------------------------------------------------------------------------------





var numeroExperimentos = 0;

for (y <- 0 until x-1){
        numeroExperimentos = numeroExperimentos + 1
}


printf("Fecha de inicio del experimento: %s\n",rdd(x-1).time_received)
printf("Fecha de finalización del experimento: %s\n\n",rdd(0).time_received)
printf("Se han contabilizado %s medidas de MTSExperiment\n\n\n",numeroExperimentos)

var minimo = rdd(0).mission_clock
var maximo = rdd(0).mission_clock

for (y <- 0 until x-1){
    if (rdd(y).mission_clock < minimo)
    minimo = rdd(y).mission_clock
}

for (y <- 0 until x-1){
    if (rdd(y).mission_clock > maximo)
    maximo = rdd(y).mission_clock
}




var duracion = (maximo - minimo)
var duracion2 = duracion / 4
var duracion3 = duracion2 / 60
var duracion4 = duracion3 / 60
printf("Duración del experimento: %s horas.\n", Math.round(duracion4))
printf("Nombre del ensayo: %s.\n", rdd(0).current_essay)
printf("Completado con éxito: %s.\n", rdd(0).is_at_essays)
///////////////////////////////////////////////////////////////////////////////////
/*
var nums: Map[String,String] = Map()
var nums2: Map[String,String] = Map()
var nums3: Map[String,String] = Map()
var nums4: Map[String,String] = Map()



for(y <- (0 until indices.length-1 by 2).reverse){
  printf("%s \n",y)
    var nums2: Map[String,String] = Map(rdd(indices(y+1)).time_received->rdd(indices(y+1)).time_received)
    nums = nums ++ nums2
}

//var t5 = z.select("INICIO HELLO",nums)



for(y <- (0 until indices.length-1 by 2).reverse){
  printf("%s \n",y)
    var nums4: Map[String,String] = Map(rdd(indices(y)).time_received->rdd(indices(y)).time_received)
    nums3 = nums3 ++ nums4
    printf("%s \n",nums3)
}
printf("%s \n",nums3)

var t6 = z.select("FINAL HELLO", nums3)

*/

printf("\n\n\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


var t5 = "2020-03-31 18:16:58.788881"
var t6 = "2020-04-09 11:51:48.289907"


writer.write("valorTiempo1 =  " + valorTiempo1 + "\n\n")
writer.write("valorTiempo2 =  "  + valorTiempo2 + "\n\n")



val t5String : String = t5 + ""
var t6String : String = t6 + ""






/*
for (y <- 1 until x-1){

            if (rdd(y).mts1 == true && rdd(y).mts2 == true && rdd(y).mts3 == true){
            printf("Entre   %s   y   %s  se pasa a combinación %s con éxito.\n",rdd(y).time_received.slice(0,19), rdd(y-1).time_received.slice(0,19), 5)
            printf("Current step: %s\n", rdd(y).current_step)
            printf("Tiempo transcurrido: %s milisegundos\n", Math.abs((convertStringToDate(tiemposMTS(y-1))).getTime() - (convertStringToDate(tiemposMTS(y))).getTime()) )
            val collection = sc.parallelize(Seq((rdd(y).mission_clock,183)))
            collection.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == false && rdd(y).mts2 == false && rdd(y).mts3 == false){

            val collection2 = sc.parallelize(Seq((rdd(y).mission_clock,0)))
            collection2.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == true && rdd(y).mts2 == false && rdd(y).mts3 == false){

            val collection3 = sc.parallelize(Seq((rdd(y).mission_clock,37)))
            collection3.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == false && rdd(y).mts2 == true && rdd(y).mts3 == false){

            val collection4 = sc.parallelize(Seq((rdd(y).mission_clock,73)))
            collection4.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == true && rdd(y).mts2 == true && rdd(y).mts3 == false){

            val collection5 = sc.parallelize(Seq((rdd(y).mission_clock,110)))
            collection5.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == false && rdd(y).mts2 == true && rdd(y).mts3 == true){

            val collection6 = sc.parallelize(Seq((rdd(y).mission_clock,146)))
            collection6.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))

        } else if (rdd(y).mts1 == false && rdd(y).mts2 == false && rdd(y).mts3 == true){

            val collection7 = sc.parallelize(Seq((rdd(y).mission_clock,73)))
            collection7.saveToCassandra("prototipodb", "pres2", SomeColumns("time_received","combinacion"))
        }

    }
*/







printf("\n\n\n")

for (y <- 0 until x){
    if ((convertStringToDate(tiemposMTS(y)).getTime() >= convertStringToDate(t5String).getTime()) && (convertStringToDate(tiemposMTS(y)).getTime() < convertStringToDate(t6String).getTime())){
        if ((rdd(y).mts_p1tts1 > 1850 || rdd(y).mts_p1tts2 > 1850 || rdd(y).mts_p1tts3 > 1850 || rdd(y).mts_p1tts4 > 1850 || rdd(y).mts_p1tts5 > 1850 || rdd(y).mts_p1tts6 > 1850 )){
            writer.write("Fallo por sobrecalentamiento del evaporador en       \n"  +    rdd(y).time_received + "\n")
            printf("Fallo por sobrecalentamiento del evaporador en       %s\n",rdd(y).time_received)}
        if ((rdd(y).mts_p1tts1 < 1332 || rdd(y).mts_p1tts2 < 1332 || rdd(y).mts_p1tts3 < 1332 || rdd(y).mts_p1tts4 < 1332 || rdd(y).mts_p1tts5 < 1332 || rdd(y).mts_p1tts6 < 1332 )){
            printf("Fallo por congelamiento del amoníaco en              %s\n", rdd(y).time_received)}
            writer.write("Fallo por congelamiento del amoníaco en       \n"  +    rdd(y).time_received + "\n")

          }}


printf("\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ANALISIS REALIZADO CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>FIN DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.close()
