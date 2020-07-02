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

case class EventError (tm_type: String,time_received: String,event :String,mission_time: Int,parameter_id: Int,parameter_value1: String,parameter_value2: String,sequencecount: Int,tm_id: BigInt);

case class Hello(tm_type: String,time_received: String,batt_t_ext_tm: Int,batt_t_int_tm : Int,batt_tbat1_tm : Int,batt_tbat2_tm : Int,batt_tbat3_tm : Int,batt_vbat_tm : Int,batt_vbus_tm : Int,battery_status: String,boom1_vbus : Boolean,boom2_vbus : Boolean,current_operating_mode: String,current_time : Int,das_n15v : Boolean,das_p15v : Boolean,das_p3v : Boolean,das_p5v : Boolean,ebox_t_ext_tm : Int,ebox_t_int_tm : Int,mgm1_p5v : Boolean,mgm1_t_tm : Int,mgm1_x_tm : Int,mgm1_y_tm : Int,mgm1_z_tm : Int,mgm2_p5v : Boolean,mgm2_t_tm : Int,mgm2_x_tm : Int,mgm2_y_tm : Int,mgm2_z_tm : Int,mgm3_n15v : Boolean,mgm3_p15v : Boolean,mgm3_t_tm : Int,mgm3_x_tm : Int,mgm3_y_tm : Int,mgm3_z_tm : Int,mgt_tx_tm : Int,mgt_x_vbus : Boolean,mission_time : Int,modem_t_tr_tm : Int,modem_vbus : Boolean,mts_p1tts1_tm : Int,mts_p1tts2_tm : Int,mts_p1tts3_tm : Int,mts_p1tts4_tm : Int,mts_p1tts5_tm : Int,mts_p1tts6_tm : Int,mts_vbus : Boolean,n15v_tm : Int,obc_t_tm : Int,p15v_tm : Int,p3v3_tm : Int, p5v_tm : Int,pdu_ivbus_tm : Int,pdu_p3v3 : Boolean,pdu_p5v : Boolean,psu_in15v_tm : Int,psu_ip15v_tm : Int,psu_ip3v3_tm : Int,psu_ip5v_tm : Int,psu_t_tm : Int,pv_ispxn_tm : Int,pv_ispxp_tm : Int,pv_ispyn_tm : Int,pv_ispyp_tm : Int,pv_ispzp_tm : Int,pv_tpsxn_tm : Int,pv_tpsxp_tm : Int,pv_tpsyn_tm : Int,pv_tpsyp_tm : Int,pv_tpszp_tm : Int,rw1_t_tm : Int,rw2_t_tm : Int,rw_p5v : Boolean,rw_vbus : Boolean,sequencecount : Int,sma_sb01 : Boolean,sma_sb02 : Boolean,ss6_xn_tm : Int,ss6_xp_tm : Int,ss6_yn_tm : Int,ss6_yp_tm : Int,ss6_zn_tm : Int,ss6_zp_tm : Int,temp_a_p5v : Boolean,temp_b_p5v : Boolean,tm_id : BigInt,ttc_stat : Boolean)





val rdd = sc.cassandraTable[Mtsexperiment]("upmsat2db", "mtsexperiment").collect()
val rdd2  = sc.cassandraTable[EventError]("upmsat2db", "eventerror").collect();
var rdd3 = sc.cassandraTable[Hello]("upmsat2db", "hello").collect()

printf("\n\n\n")

if(rdd.size>0){printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>DATOS RECOGIDOS CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
}else {printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ERROR AL RECOGER LOS DATOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")}

val x = rdd.size
val xx = rdd2.size

val informe = new File("C:/Users/chech/Desktop/TFG/VERSION_NUEVA/PROGRAMAS_SCALA/CODIGO/INFORME_MTS.txt")

val writer = new PrintWriter(informe)
writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INICIO DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

//------------------------------------------------------------------
val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

def convertStringToDate(s: String): Date = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.parse(s)
}

def convertLongToString(date: Long): String = {
       val d = new Date(date * 1000L)
       new SimpleDateFormat(DATE_FORMAT).format(d)
   }

def convertStringToLong(date: String): Long = {
      (convertStringToDate(date).getTime())/1000

  }

def diftiempos1(date1: Long, date2: Long): Long = {
      Math.abs(date1-date2)

  }

//-------------------------------------------------------------------

val tiemposMTS = ArrayBuffer[String]()
for (y <- 0 until x){
    tiemposMTS += rdd(y).time_received
}

val tiemposHello = ArrayBuffer[String]()
for (y <- 0 until xx){
    tiemposHello += rdd2(y).time_received
}

var tiemposMTSLength = tiemposMTS.length
var tiemposHelloLength = tiemposHello.length
val indices = ArrayBuffer[Int]()

indices += 0
for (y <- 0 until tiemposHelloLength - 1){
    if (((convertStringToDate(tiemposHello(y)).getTime() - convertStringToDate(tiemposHello(y+1)).getTime())/(60 * 60 * 1000)) > 1) {
    indices += y
    indices += y+1
    }
}
indices += xx-1
//--------------------------------------------------------------------------------------------------------------------------------------

printf("COBERTURAS DISPONIBLES PARA ANALISIS: %s \n\n", indices.length/2)

writer.write("COBERTURAS DISPONIBLES PARA ANALISIS:  " + indices.length/2 + "\n\n")



printf("HELLO:\n")
writer.write("HELLO: \n\n")
for (y <- (0 until indices.length-1 by 2).reverse){
    printf("%s   -   %s\n",rdd2(indices(y+1)).time_received, rdd2(indices(y)).time_received)
    writer.write( rdd2(indices(y+1)).time_received + "  -  " + rdd2(indices(y)).time_received + " \n\n ")}







printf("\n\n\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


var t1 = "2020-05-09 11:17:14.747724"
var t2 = "2020-05-09 14:00:58.604508"

var t5 = "2020-05-09 10:31:16.664029"
var t6 = "2020-05-09 14:44:09.060707"



var posUltimo = 0;
var reftimem = 0;
var reftimet = "";

for (y <- 0 until rdd2.length-1){
    if(rdd3(y).time_received == t6){
        posUltimo = y
        reftimem = rdd3(y).mission_time
        reftimet = rdd3(y).time_received

    }
}

def diftiempos(time : Int): String = {
        val f  = reftimem - time
        val f2 = ((convertStringToDate(reftimet).getTime())/1000) - ((f/4).toLong)
        convertLongToString(f2)


    }

writer.write("valorTiempo1HELLO =  " + t5 + "\n\n")
writer.write("valorTiempo2HELLO =  "  + t6 + "\n\n")

writer.write("valorTiempo1EVENTERROR =  " + t1 + "\n\n")
writer.write("valorTiempo2EVENTERROR =  "  + t2 + "\n\n")


val t1String : String = t1 + ""
var t2String : String = t2 + ""
val t5String : String = t5 + ""
var t6String : String = t6 + ""

var numeroExperimentos = 0;

for (y <- 0 until x-1){
        numeroExperimentos = numeroExperimentos + 1
}

printf("Se han contabilizado %s medidas de MTSExperiment\n\n\n",numeroExperimentos)

writer.write("Se han contabilizado "  + numeroExperimentos + " medidas de MTSExperiment \n\n")

val ensayos = ArrayBuffer[Int]()

ensayos += 0
for (y <- 1 until tiemposMTSLength-1){
    if (rdd(y).current_essay != rdd(y+1).current_essay) {
    ensayos += y
    ensayos += y+1
    }
}
ensayos += x-1



for (y <- (0 until ensayos.length by 2).reverse){


if((convertStringToDate(rdd(ensayos(y)).time_received).getTime() <= convertStringToDate(t6String).getTime()) && (convertStringToDate(rdd(ensayos(y)).time_received).getTime() >= convertStringToDate(t5String).getTime())){

if((convertStringToDate(rdd(ensayos(y)).time_received).getTime() <= convertStringToDate(t2String).getTime()) && (convertStringToDate(rdd(ensayos(y)).time_received).getTime() >= convertStringToDate(t1String).getTime())){



var numeroExperimentos2 = 0;

for (a <- ensayos(y) until ensayos(y+1)){
          numeroExperimentos2 = numeroExperimentos2 + 1
  }

  var minimo = rdd(ensayos(y)).mission_clock
  var maximo = rdd(ensayos(y)).mission_clock

  for (b <- ensayos(y) until ensayos(y+1)){
      if (rdd(b).mission_clock < minimo)
      minimo = rdd(b).mission_clock
  }

  for (c <- ensayos(y) until ensayos(y+1)){
      if (rdd(c).mission_clock > maximo)
      maximo = rdd(c).mission_clock
  }



  var duracion = maximo - minimo
  var duracion2 = duracion / 4
  var duracion3 = duracion2 / 60
  var duracion4 = duracion3 / 60

  printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INFORMACION GENERAL<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

  writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INFORMACION GENERAL<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

  printf("Nombre del ensayo: %s \n", rdd(ensayos(y)).current_essay)
  printf("Duración del experimento: %s horas\n", duracion4)
  printf("Completado con éxito: %s\n", rdd(ensayos(y+1)).is_at_essays)
  printf("Fecha de inicio del experimento: %s\n",diftiempos(rdd(ensayos(y+1)).mission_clock))
  printf("Fecha de finalización del experimento: %s\n\n",diftiempos(rdd(ensayos(y)).mission_clock))

  writer.write("Nombre del ensayo:  "  + rdd(ensayos(y)).current_essay + "\n\n")
  writer.write("Duración del experimento:  "  + duracion4 + " horas \n\n")
  writer.write("Completado con éxito:  "  + rdd(ensayos(y+1)).is_at_essays + "\n\n")
  writer.write("Fecha de inicio del experimento:  "  + diftiempos(rdd(ensayos(y+1)).mission_clock) + "\n\n")
  writer.write("Fecha de finalización del experimento:  "  + diftiempos(rdd(ensayos(y)).mission_clock) + "\n\n")

  printf("HEATERS \n\n")

  writer.write("HEATERS \n\n")

  if (rdd(y).mts1 == false && rdd(y).mts2 == false &&  rdd(y).mts3 == false){
      printf("Los tres heaters están apagados \n\n")
      writer.write("Los tres heaters están apagados \n\n")
  }else if (rdd(y).mts1 == false && rdd(y).mts2 == false && rdd(y).mts3 == true){
      printf("Heaters apagados: MTS1 y MTS2   ||   Heaters encendidos: MTS3 \n\n")
      writer.write("Heaters apagados: MTS1 y MTS2   ||   Heaters encendidos: MTS3 \n\n")
  }else if (rdd(y).mts1 == false &&  rdd(y).mts2 == true &&  rdd(y).mts3 == false){
      printf("Heaters apagados: MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
      writer.write("Heaters apagados: MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
  }else if (rdd(y).mts1 == false &&  rdd(y).mts2 == true &&  rdd(y).mts3 == true){
      printf("Heaters apagados: MTS1   ||   Heaters encendidos: MTS2 y MTS3 \n\n")
      writer.write("Heaters apagados: MTS1   ||   Heaters encendidos: MTS2 y MTS3 \n\n")
  }else if (rdd(y).mts1 == true &&  rdd(y).mts2 == false &&  rdd(y).mts3 == false){
      printf("Heaters apagados: MTS2 y MTS3  ||   Heaters encendidos: MTS1 \n\n")
      writer.write("Heaters apagados: MTS2 y MTS3  ||   Heaters encendidos: MTS1 \n\n")
  }else if (rdd(y).mts1 == true &&  rdd(y).mts2 == false &&  rdd(y).mts3 == true){
      printf("Heaters apagados:  MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
      writer.write("Heaters apagados:  MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
  }else if (rdd(y).mts1 == true &&  rdd(y).mts2 == true &&  rdd(y).mts3 == false){
      printf("Heaters apagados:  MTS3  ||   Heaters encendidos: MTS1 y MTS2 \n\n")
      writer.write("Heaters apagados:  MTS3  ||   Heaters encendidos: MTS1 y MTS2 \n\n")
  }else{ printf("Los tres heaters están encendidos \n\n")
      writer.write("Los tres heaters están encendidos \n\n")}


  for (y <- 0 until x){
      if ((convertStringToDate(tiemposMTS(y)).getTime() >= convertStringToDate(t5String).getTime()) && (convertStringToDate(tiemposMTS(y)).getTime() <= convertStringToDate(t6String).getTime())){
        if((convertStringToDate(tiemposMTS(y)).getTime() <= convertStringToDate(t2String).getTime()) && (convertStringToDate(tiemposMTS(y)).getTime() >= convertStringToDate(t1String).getTime())){
          if ((rdd(y).mts_p1tts1 > 1920 || rdd(y).mts_p1tts2 > 1920 || rdd(y).mts_p1tts3 > 1920 || rdd(y).mts_p1tts4 > 1920 || rdd(y).mts_p1tts5 > 1920 || rdd(y).mts_p1tts6 > 1920 )){
              printf("Fallo por sobrecalentamiento del evaporador en el MTS el %s recibido el %s\n", diftiempos(rdd(y).mission_clock), rdd(y).time_received)
              writer.write("Fallo por sobrecalentamiento del evaporador en el MTS  el       \n"  +    diftiempos(rdd(y).mission_clock)  + " recibido el " + rdd(y).time_received + " \n ")}
          if ((rdd(y).mts_p1tts1 < 1045 || rdd(y).mts_p1tts2 < 1045 || rdd(y).mts_p1tts3 < 1045 || rdd(y).mts_p1tts4 < 1045 || rdd(y).mts_p1tts5 < 1045 || rdd(y).mts_p1tts6 < 1045 )){
              printf("Fallo por congelamiento del amoníaco en el MTS  el %s recibido el %s\n",  diftiempos(rdd(y).mission_clock),rdd(y).time_received)
              writer.write("Fallo por congelamiento del amoníaco en el MTS  el \n"  +    diftiempos(rdd(y).mission_clock) + " recibido el " + rdd(y).time_received + "\n")}

            }}}



  printf("\n\n Verificación actual: %s\n\n",rdd(ensayos(y)).current_verification)
  printf("Paso actual: %s\n\n",rdd(ensayos(y+1)).current_step)
  printf("Se han contabilizado %s medidas de MTSExperiment entre los intervalos %s y %s\n\n\n",numeroExperimentos2,diftiempos(rdd(ensayos(y+1)).mission_clock),diftiempos(rdd(ensayos(y)).mission_clock))

  writer.write("\n\n Verificación actual: "  + rdd(ensayos(y)).current_verification + "\n\n")
  writer.write("Paso actual:  "  + rdd(ensayos(y+1)).current_step + "\n\n")
  writer.write("Se han contabilizad  "  + numeroExperimentos2 + " medidas de MTSExperiment entre los intervalos " + diftiempos(rdd(ensayos(y+1)).mission_clock) + " y " + diftiempos(rdd(ensayos(y)).mission_clock) + "\n\n")


  printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>PASOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

  writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>PASOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


  var comienzo = ensayos(y+1)
  var terminar = 0
  var duracion_pasos = 0

  for(d <- (ensayos(y) until ensayos(y+1)).reverse){

    if (rdd(d).current_step != rdd(d+1).current_step){

        terminar = d

      var fff = Math.abs(rdd(comienzo).mission_clock - rdd(terminar).mission_clock)
      duracion_pasos = (fff)/(4*60)

      printf("Paso actual: %s\n\n",rdd(terminar).current_step)
      printf("Comienzo: %s\n\n",diftiempos(rdd(comienzo).mission_clock))
      printf("Final: %s\n\n",diftiempos(rdd(terminar).mission_clock))
      printf("Duración: %s minutos\n\n",duracion_pasos)

      writer.write("Paso actual:  "  + rdd(comienzo).current_step + "\n\n")
      writer.write("Comienzo:  "  + diftiempos(rdd(comienzo).mission_clock) + "\n\n")
      writer.write("Final:  "  + diftiempos(rdd(terminar).mission_clock) + "\n\n")
      writer.write("Duración:  "  + duracion_pasos + " horas \n\n")

      printf("HEATERS \n\n")
      writer.write("HEATERS \n\n")

      if (rdd(comienzo).mts1 == false && rdd(comienzo).mts2 == false &&  rdd(comienzo).mts3 == false){
          printf("Los tres heaters están apagados \n\n")
          writer.write("Los tres heaters están apagados \n\n")
      }else if (rdd(comienzo).mts1 == false && rdd(comienzo).mts2 == false && rdd(comienzo).mts3 == true){
          printf("Heaters apagados: MTS1 y MTS2   ||   Heaters encendidos: MTS3 \n\n")
          writer.write("Heaters apagados: MTS1 y MTS2   ||   Heaters encendidos: MTS3 \n\n")
      }else if (rdd(comienzo).mts1 == false &&  rdd(comienzo).mts2 == true &&  rdd(comienzo).mts3 == false){
          printf("Heaters apagados: MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
          writer.write("Heaters apagados: MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
      }else if (rdd(comienzo).mts1 == false &&  rdd(comienzo).mts2 == true &&  rdd(comienzo).mts3 == true){
          printf("Heaters apagados: MTS1   ||   Heaters encendidos: MTS2 y MTS3 \n\n")
          writer.write("Heaters apagados: MTS1   ||   Heaters encendidos: MTS2 y MTS3 \n\n")
      }else if (rdd(comienzo).mts1 == true &&  rdd(comienzo).mts2 == false &&  rdd(comienzo).mts3 == false){
          printf("Heaters apagados: MTS2 y MTS3  ||   Heaters encendidos: MTS1 \n\n")
          writer.write("Heaters apagados: MTS2 y MTS3  ||   Heaters encendidos: MTS1 \n\n")
      }else if (rdd(comienzo).mts1 == true &&  rdd(comienzo).mts2 == false &&  rdd(comienzo).mts3 == true){
          printf("Heaters apagados:  MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
          writer.write("Heaters apagados:  MTS1 y MTS3  ||   Heaters encendidos: MTS2 \n\n")
      }else if (rdd(comienzo).mts1 == true &&  rdd(comienzo).mts2 == true &&  rdd(comienzo).mts3 == false){
          printf("Heaters apagados:  MTS3  ||   Heaters encendidos: MTS1 y MTS2 \n\n")
          writer.write("Heaters apagados:  MTS3  ||   Heaters encendidos: MTS1 y MTS2 \n\n")
      }else{ printf("Los tres heaters están encendidos \n\n")
          writer.write("Los tres heaters están encendidos \n\n")}

          comienzo = d + 1
    }

  }


} } }

printf("\n\n\n")


printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ANALISIS REALIZADO CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>FIN DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.close()
