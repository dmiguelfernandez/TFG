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


case class Housekeeping(tm_type: String,time_received: String,batt_t_ext_tm: Int,batt_t_int_tm : Int,batt_tbat1_tm : Int,batt_tbat2_tm : Int,batt_tbat3_tm : Int,batt_vbat_tm : Int,batt_vbus_tm : Int,battery_status: String,boom1_vbus : Boolean,boom2_vbus : Boolean,current_operating_mode: String,current_time : Int,das_n15v : Boolean,das_p15v : Boolean,das_p3v : Boolean,das_p5v : Boolean,ebox_t_ext_tm : Int,ebox_t_int_tm : Int,mgm1_p5v : Boolean,mgm1_t_tm : Int,mgm1_x_tm : Int,mgm1_y_tm : Int,mgm1_z_tm : Int,mgm2_p5v : Boolean,mgm2_t_tm : Int,mgm2_x_tm : Int,mgm2_y_tm : Int,mgm2_z_tm : Int,mgm3_n15v : Boolean,mgm3_p15v : Boolean,mgm3_t_tm : Int,mgm3_x_tm : Int,mgm3_y_tm : Int,mgm3_z_tm : Int,mgt_tx_tm : Int,mgt_x_vbus : Boolean,modem_t_tr_tm : Int,modem_vbus : Boolean,mts_p1tts1_tm : Int,mts_p1tts2_tm : Int,mts_p1tts3_tm : Int,mts_p1tts4_tm : Int,mts_p1tts5_tm : Int,mts_p1tts6_tm : Int,mts_vbus : Boolean,n15v_tm : Int,obc_t_tm : Int,p15v_tm : Int,p3v3_tm : Int, p5v_tm : Int,pdu_ivbus_tm : Int,pdu_p3v3 : Boolean,pdu_p5v : Boolean,psu_in15v_tm : Int,psu_ip15v_tm : Int,psu_ip3v3_tm : Int,psu_ip5v_tm : Int,psu_t_tm : Int,pv_ispxn_tm : Int,pv_ispxp_tm : Int,pv_ispyn_tm : Int,pv_ispyp_tm : Int,pv_ispzp_tm : Int,pv_tpsxn_tm : Int,pv_tpsxp_tm : Int,pv_tpsyn_tm : Int,pv_tpsyp_tm : Int,pv_tpszp_tm : Int,rw1_t_tm : Int,rw2_t_tm : Int,rw_p5v : Boolean,rw_vbus : Boolean,sequencecount : Int,sma_sb01 : Boolean,sma_sb02 : Boolean,ss6_xn_tm : Int,ss6_xp_tm : Int,ss6_yn_tm : Int,ss6_yp_tm : Int,ss6_zn_tm : Int,ss6_zp_tm : Int,temp_a_p5v : Boolean,temp_b_p5v : Boolean,tm_id : BigInt,ttc_stat : Boolean)

case class Hello(tm_type: String,time_received: String,batt_t_ext_tm: Int,batt_t_int_tm : Int,batt_tbat1_tm : Int,batt_tbat2_tm : Int,batt_tbat3_tm : Int,batt_vbat_tm : Int,batt_vbus_tm : Int,battery_status: String,boom1_vbus : Boolean,boom2_vbus : Boolean,current_operating_mode: String,current_time : Int,das_n15v : Boolean,das_p15v : Boolean,das_p3v : Boolean,das_p5v : Boolean,ebox_t_ext_tm : Int,ebox_t_int_tm : Int,mgm1_p5v : Boolean,mgm1_t_tm : Int,mgm1_x_tm : Int,mgm1_y_tm : Int,mgm1_z_tm : Int,mgm2_p5v : Boolean,mgm2_t_tm : Int,mgm2_x_tm : Int,mgm2_y_tm : Int,mgm2_z_tm : Int,mgm3_n15v : Boolean,mgm3_p15v : Boolean,mgm3_t_tm : Int,mgm3_x_tm : Int,mgm3_y_tm : Int,mgm3_z_tm : Int,mgt_tx_tm : Int,mgt_x_vbus : Boolean,mission_time : Int,modem_t_tr_tm : Int,modem_vbus : Boolean,mts_p1tts1_tm : Int,mts_p1tts2_tm : Int,mts_p1tts3_tm : Int,mts_p1tts4_tm : Int,mts_p1tts5_tm : Int,mts_p1tts6_tm : Int,mts_vbus : Boolean,n15v_tm : Int,obc_t_tm : Int,p15v_tm : Int,p3v3_tm : Int, p5v_tm : Int,pdu_ivbus_tm : Int,pdu_p3v3 : Boolean,pdu_p5v : Boolean,psu_in15v_tm : Int,psu_ip15v_tm : Int,psu_ip3v3_tm : Int,psu_ip5v_tm : Int,psu_t_tm : Int,pv_ispxn_tm : Int,pv_ispxp_tm : Int,pv_ispyn_tm : Int,pv_ispyp_tm : Int,pv_ispzp_tm : Int,pv_tpsxn_tm : Int,pv_tpsxp_tm : Int,pv_tpsyn_tm : Int,pv_tpsyp_tm : Int,pv_tpszp_tm : Int,rw1_t_tm : Int,rw2_t_tm : Int,rw_p5v : Boolean,rw_vbus : Boolean,sequencecount : Int,sma_sb01 : Boolean,sma_sb02 : Boolean,ss6_xn_tm : Int,ss6_xp_tm : Int,ss6_yn_tm : Int,ss6_yp_tm : Int,ss6_zn_tm : Int,ss6_zp_tm : Int,temp_a_p5v : Boolean,temp_b_p5v : Boolean,tm_id : BigInt,ttc_stat : Boolean)

case class EventError (tm_type: String,time_received: String,event :String,mission_time: Int,parameter_id: Int,parameter_value1: String,parameter_value2: String,sequencecount: Int,tm_id: BigInt);



val rdd = sc.cassandraTable[Housekeeping]("upmsat2db", "housekeeping").collect()
var rdd2 = sc.cassandraTable[Hello]("upmsat2db", "hello").collect()
val rdd3 = sc.cassandraTable[EventError]("upmsat2db", "eventerror").collect()


val x = rdd.size
val xx = rdd2.size
val xxx = rdd3.size

val informe = new File("C:/Users/chech/Desktop/TFG/VERSION_NUEVA/PROGRAMAS_SCALA/CODIGO/INFORME_GENERAL.txt")

val writer = new PrintWriter(informe)


var linf1 = xxx;
var lsup1 = xxx;
var linf2 = x;
var lsup2 = x;
var linf3 = xx;
var lsup3 = xx;
var x2 = 0;
var x21 = 0;
var x3 = 0;
var x31 = 0;
var x4 = 0;
var x41 = 0;
var x5 = 0;
var x51 = 0;
var x6 = 0;
var x61 = 0;
var x7 = 0;
var x71 = 0;

printf("------------------------------------------------------------------------------------------------------------------------------------------\n\n\n")

writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INICIO DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


val tiemposHello = ArrayBuffer[String]()
for (y <- 0 until xx){
    tiemposHello += rdd2(y).time_received
}

val tiemposHousekeeping = ArrayBuffer[String]()
for (y <- 0 until x){
    tiemposHousekeeping += rdd(y).time_received
}

val tiemposEvent = ArrayBuffer[String]()
for (y <- 0 until xxx){
    tiemposEvent += rdd3(y).time_received
}

val indices = ArrayBuffer[Int]()



//------------------------------------------------------------------------------------------------------------------------------------
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
//-------------------------------------------------------------------------------------------------------------------------------------



var tiemposHelloLength = tiemposHello.length
var tiemposHousekeepingLength = tiemposHousekeeping.length



//--------------------------------------------MEDIDOR DE DIFERENCIAS ENTRE TIEMPOS------------------------------------------------------
indices += 0
for (y <- 0 until tiemposHelloLength-1){
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
for (y <- (0 until indices.length-1 by 2).reverse){
    printf("%s   -   %s\n",rdd2(indices(y+1)).time_received, rdd2(indices(y)).time_received)


}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
var nums: Map[String,String] = Map()
var nums2: Map[String,String] = Map()
var nums3: Map[String,String] = Map()
var nums4: Map[String,String] = Map()




for(y <- (0 until indices.length-1 by 2).reverse){
    var nums2: Map[String,String] = Map(rdd2(indices(y+1)).time_received->rdd2(indices(y+1)).time_received)
    nums = nums ++ nums2
}

var t5 = z.select("INICIO HELLO",nums)




for(y <- (0 until indices.length-1 by 2).reverse){
    var nums4: Map[String,String] = Map(rdd2(indices(y)).time_received->rdd2(indices(y)).time_received)
    nums3 = nums3 ++ nums4
}

var t6 = z.select("FINAL HELLO", nums3)


*/


writer.write("\n------------------------------------------------------------------------------------------------------------------------------------------\nTIEMPOS ELEGIDOS\n\n")



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




val t5String : String = t5 + ""
var t6String : String = t6 + ""


writer.write("valorTiempo5 =  " + t5 + "\n")
writer.write("valorTiempo6 =  "  + t6 + "\n")

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




writer.write("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\nDATOS GENERALES\n\n")

for (y <- 0 until x) if (rdd(y).time_received > t5){ lsup2 = y;}
for (y <- (0 until x).reverse) if (rdd(y).time_received < t6){ linf2 = y;}
for (y <- 0 until xx) if (rdd2(y).time_received >= t5){ lsup3 = y;}
for (y <- (0 until xx).reverse) if (rdd2(y).time_received <= t6){ linf3 = y;}
for (y <- 0 until xxx) if (rdd3(y).time_received > t5){ lsup1 = y;}
for (y <- (0 until xxx).reverse) if (rdd3(y).time_received < t6){ linf1 = y;}


printf("%s a %s\n",linf1,lsup1)
printf("%s a %s\n",linf2,lsup2)
printf("%s a %s\n",linf3,lsup3)



var orbitas = ArrayBuffer[Int]()
var iteracion = 0;
printf("\n\n\n\n")
for (y <- (1 until indices.size by 2).reverse){
    var auxx = ((rdd2(indices(y-1)).mission_time - rdd2(indices(y)).mission_time) / (60 * 60 * 4) / 1.61)
    orbitas += (auxx).toInt
    printf("NUMERO DE ORBITAS ENTRE %s   -   %s: %s\n", rdd2(indices(y)).time_received, rdd2(indices(y-1)).time_received,orbitas(iteracion))
    writer.write("\n NUMERO DE ORBITAS ENTRE  " +    rdd2(indices(y)).time_received  + "-" + rdd2(indices(y-1)).time_received + ":" + orbitas(iteracion) + "/n/n")

    iteracion = iteracion + 1

}

printf("\n\n\n")







///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


for (y <- 0 until x)
    if (y >= linf2 && y <= lsup2 && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971)){
        x2 = x2 + 1
    }



for (y <- 0 until xx)
    if (y >= linf3 && y <= lsup3 && (rdd2(y).batt_t_ext_tm >= 866 || rdd2(y).batt_t_ext_tm <= 1971)){
        x21 = x21 + 1
    }


/*

val collection = sc.parallelize(Seq(("BATT_T_EXT_TM HOUSEKEEPING",x2,(x-x2),1)))
collection.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
val collection2 = sc.parallelize(Seq(("BATT_T_EXT_TM HELLO",x21,(xx-x21),2)))
collection2.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
*/

for (y <- 0 until x)
    if (y >= linf2 && y <= lsup2 && (rdd(y).batt_t_int_tm >= 866 || rdd(y).batt_t_int_tm <= 1971)){
        x3 = x3 + 1
    }

for (y <- 1 until xx)
    if (y >= linf3 && y <= lsup3 &&  (rdd2(y).batt_t_int_tm >= 866 || rdd2(y).batt_t_int_tm <= 1971)){
        x31 = x31 + 1
        }


/*
val collection3 = sc.parallelize(Seq(("BATT_T_INT_TM HOUSEKEEPING",x3,(x-x3),3)))
collection3.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
val collection4 = sc.parallelize(Seq(("BATT_T_INT_TM HELLO",x31,(xx-x31),4)))
collection4.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
*/

for (y <- 1 until x)
    if (y >= linf2 && y <= lsup2 &&  (rdd(y).batt_tbat1_tm <= 866 || rdd(y).batt_tbat1_tm >= 1971)){
        x4 = x4 + 1
        }

for (y <- 1 until xx)
    if (y >= linf3 && y <= lsup3 &&   (rdd2(y).batt_tbat1_tm <= 866 || rdd2(y).batt_tbat1_tm >= 1971)){
        x41 = x41 + 1
        }



/*
val collection5 = sc.parallelize(Seq(("BATT_TBAT1_TM HOUSEKEEPING",x4,(x-x4),5)))
collection5.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
val collection6 = sc.parallelize(Seq(("BATT_TBAT1_TM HELLO",x41,(xx-x41),6)))
collection6.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
*/

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


for (y <- 1 until x)
    if (y >= linf2 && y <= lsup2 && (rdd(y).batt_tbat2_tm <= 866 || rdd(y).batt_tbat2_tm >= 1971)){
        x5 = x5 + 1
    }

for (y <- 1 until xx)
    if (y >= linf3 && y <= lsup3 && (rdd2(y).batt_tbat2_tm <= 866 || rdd2(y).batt_tbat2_tm >= 1971)){
        x51 = x51 + 1
    }

/*
val collection7 = sc.parallelize(Seq(("BATT_TBAT2_TM HOUSEKEEPING",x5,(x-x5),7)))
collection7.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
val collection8 = sc.parallelize(Seq(("BATT_TBAT2_TM HELLO",x51,(xx-x51),8)))
collection8.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))

*/
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


for (y <- 1 until x)
    if (y >= linf2 && y <= lsup2 && (rdd(y).batt_tbat3_tm <= 866 || rdd(y).batt_tbat3_tm >= 1971)){
        x6 = x6 + 1
        }

for (y <- 1 until xx)
    if (y >= linf3 && y <= lsup3 && (rdd2(y).batt_tbat3_tm <= 866 || rdd2(y).batt_tbat3_tm >= 1971)){
        x61 = x61 + 1
    }



/*
val collection9 = sc.parallelize(Seq(("BATT_TBAT3_TM HOUSEKEEPING",x6,(x-x6),9)))
collection9.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))
val collection10 = sc.parallelize(Seq(("BATT_TBAT3_TM HELLO",x61,(xx-x61),10)))
collection10.saveToCassandra("prototipodb", "pres1", SomeColumns("tipo","fuerarango", "dentrorango","id"))

*/
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var contador1 = 0;
var contador2 = 0;
var contador3 = 0;
var contador4 = 0;
var contador5 = 0;
var contador6 = 0;
var contador7 = 0;
var contador8 = 0;
var contador9 = 0;
var contador10 = 0;
var contador11 = 0;
var contador12 = 0;
var contador13 = 0;
var contador14 = 0;
var contador15 = 0;
var contador16 = 0;
var contador17 = 0;
var contador18 = 0;
var contador19 = 0;

for (y <- 0 until (x-1)){
    if(y >= linf2 && y <= lsup2 && (rdd(y).current_operating_mode != rdd((y+1)).current_operating_mode)){
        x7 = x7 + 1}}

for (y <- 0 until (xx-1)) {
        if (y >= linf3 && y <= lsup3 && (rdd2(y).current_operating_mode != rdd2((y+1)).current_operating_mode)){
        x71 = x71 + 1}}



for (y <-0 until (xxx))
    if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "CHANGEMODE")){
        contador1 = contador1 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "SEPARATIONTIMEREXPIRED")){
        contador2 = contador2 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "RESET")){
        contador3 = contador3 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "INITIALIZATIONDONE")){
         contador4 = contador4 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "TCINVALID")){
         contador4 = contador5 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "INITIALIZATIONERROR")){
         contador4 = contador6 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "BATTERYLOWWARNING")){
         contador4 = contador7 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "BATTERYCRITICALWARNING")){
         contador4 = contador8 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "BATTERYHIGHWARNING")){
         contador4 = contador9 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "PROTECTIONERROR")){
         contador4 = contador10 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "COMMISSIONINGERROR")){
         contador4 = contador11 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "LOSTCOMM")){
         contador4 = contador12 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "TCRECEIVED")){
         contador4 = contador13 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "NONTRUSTEDSOURCE")){
         contador4 = contador14 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "TIMEDTCINVALID")){
         contador4 = contador15 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "EXPERIMENTDONE")){
         contador4 = contador16 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "EXPERIMENTABORTED")){
         contador4 = contador17 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "SW_ERROR")){
         contador4 = contador18 + 1
    }else if (y >= linf1 && y <= lsup1 && (rdd3(y).event == "RADIOHWERROR")){
         contador4 = contador19 + 1
    }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var aux3 = 0;
for (y <- 0 until (x))
    if (y >= linf2 && y <= lsup2 ){
        aux3 = aux3 + 1
        }

var aux2 = 0;
for (y <- 0 until (xx))
    if (y >= linf3 && y <= lsup3 ){
        aux2 = aux2 + 1
        }

var aux = 0;
for (y <- 0 until (xxx))
    if (y >= linf1 && y <= lsup1){
        aux = aux + 1
        }




printf("\n\nDESDE: %s           HASTA: %s\n",t5,t6)
printf("SE HAN CONTABILIZADO %s TELEMETRIAS DE HOUSEKEEPING\n",aux3)
printf("SE HAN CONTABILIZADO %s TELEMETRIAS DE HELLO\n",aux2)
printf("SE HAN CONTABILIZADO %s TELEMETRIAS DE EVENT ERROR\n\n",aux)

writer.write("\n\nDESDE  " +    t5  + " HASTA: " + t6 + "\n\n")

writer.write("SE HAN CONTABILIZADO  " +    aux3  + " TELEMETRIAS DE HOUSEKEEPING\n")
writer.write("SE HAN CONTABILIZADO  " +    aux2  + " TELEMETRIAS DE HELLO\n")
writer.write("SE HAN CONTABILIZADO  " +    aux  + " TELEMETRIAS DE EVENT ERROR\n")






printf("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\nHOUSEKEEPING\n\n")
printf(">>>SE HAN ALCANZADO %s VALORES FUERA DE RANGO DE BATT_T_EXT_TM DE HOUSEKEEPING\n",x2)
printf(">>>SE HAN ALCANZADO %s VALORES FUERA DE RANGO DE BATT_T_INT_TM DE HOUSEKEEPING\n",x3)
printf(">>>SE HAN ALCANZADO %s VALORES FUERA DE RANGO DE BATT_TBAT1_TM DE HOUSEKEEPING\n",x4)
printf(">>>SE HAN ALCANZADO %s VALORES FUERA DE RANGO DE BATT_TBAT2_TM DE HOUSEKEEPING\n",x5)
printf(">>>SE HAN ALCANZADO %s VALORES FUERA DE RANGO DE BATT_TBAT3_TM EN HOUSEKEEPING\n",x6)
printf("\n>>>SE HAN CONTABILIZADO %s CAMBIOS DE MODO DE OPERACION\n",x7)


writer.write("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\nHOUSEKEEPING\n\n")
writer.write(">>>SE HAN ALCANZADO  " +   x2  + " VALORES FUERA DE RANGO DE BATT_T_EXT_TM DE HOUSEKEEPING\n")
writer.write(">>>SE HAN ALCANZADO  " +   x3  + " VALORES FUERA DE RANGO DE BATT_T_INT_TM DE HOUSEKEEPING\n")
writer.write(">>>SE HAN ALCANZADO  " +   x4  + " VALORES FUERA DE RANGO DE BATT_TBAT1_TM DE HOUSEKEEPING\n")
writer.write(">>>SE HAN ALCANZADO  " +   x5  + " VALORES FUERA DE RANGO DE BATT_TBAT2_TM DE HOUSEKEEPING\n")
writer.write(">>>SE HAN ALCANZADO  " +   x6  + " VALORES FUERA DE RANGO DE BATT_TBAT3_TM DE HOUSEKEEPING\n")
writer.write("\n>>>SE HAN CONTABILIZAD  " +   x7  + " CAMBIOS DE MODO DE OPERACION\n")




printf(">>>>>>Fecha: %s || Último modo de operacion: %s\n\n\n", rdd2(linf3).time_received, rdd2(linf3).current_operating_mode)
writer.write(">>>>>>>>>Fecha:  " +   rdd2(linf3).time_received  + " || Último modo de operacion: " + rdd2(linf3).current_operating_mode + "\n\n\n")


printf("\n\n------------------------------------------------------------------------------------------------------------------------------------------\nHELLO\n\n")
printf(">>>SE HAN ALCANZADO %s VALORES LIMITE DE BATT_T_EXT_TM EN HELLO\n",x21)
printf(">>>SE HAN ALCANZADO %s VALORES LIMITE DE BATT_T_INT_TM EN HELLO\n",x31)
printf(">>>SE HAN ALCANZADO %s VALORES LIMITE DE BATT_TBAT1_TM EN HELLO\n",x41)
printf(">>>SE HAN ALCANZADO %s VALORES LIMITE DE BATT_TBAT2_TM EN HELLO\n",x51)
printf(">>>SE HAN ALCANZADO %s VALORES LIMITE DE BATT_TBAT3_TM EN HELLO\n",x61)
printf("\n>>>SE HAN CONTABILIZADO %s CAMBIOS DE MODO DE OPERACION\n",x71)



writer.write("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\nHELLO\n\n")
writer.write(">>>SE HAN ALCANZADO  " +   x21  + " VALORES FUERA DE RANGO DE BATT_T_EXT_TM DE HELLO\n")
writer.write(">>>SE HAN ALCANZADO  " +   x31  + " VALORES FUERA DE RANGO DE BATT_T_INT_TM DE HELLO\n")
writer.write(">>>SE HAN ALCANZADO  " +   x41  + " VALORES FUERA DE RANGO DE BATT_TBAT1_TM DE HELLO\n")
writer.write(">>>SE HAN ALCANZADO  " +   x51  + " VALORES FUERA DE RANGO DE BATT_TBAT2_TM DE HELLO\n")
writer.write(">>>SE HAN ALCANZADO  " +   x61  + " VALORES FUERA DE RANGO DE BATT_TBAT3_TM DE HELLO\n")
writer.write("\n>>>SE HAN CONTABILIZAD  " +   x71  + " CAMBIOS DE MODO DE OPERACION\n")






printf(">>>>>>Fecha: %s || Último modo de operacion: %s\n\n\n", rdd2(linf3).time_received, rdd2(linf3).current_operating_mode)
writer.write(">>>>>>>>>Fecha:  " +   rdd2(linf3).time_received  + " || Último modo de operacion: " + rdd2(linf3).current_operating_mode + "\n\n\n")







printf("\n\n------------------------------------------------------------------------------------------------------------------------------------------\nEVENTS\n\n")
printf(">>>SE HAN CONTABILIZADO %s ESTADOS CHANGEMODE\n",contador1)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS SEPARATIONTIMEREXPIRED\n",contador2)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS RESET\n",contador3)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS COMMISSIONINGDONE\n",contador4)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS TCINVALID\n",contador5)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS INITIALIZATIONERROR\n",contador6)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS BATTERYLOWWARNING\n",contador7)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS BATTERYCRITICALWARNING\n",contador8)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS BATTERYHIGHWARNING\n",contador9)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS PROTECTIONERROR\n",contador10)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS COMMISSIONINGERROR\n",contador11)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS LOSTCOMM\n",contador12)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS TCRECEIVED\n",contador13)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS NONTRUSTEDSOURCE\n",contador14)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS TIMEDTCINVALID\n",contador15)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS EXPERIMENTDONE\n",contador16)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS EXPERIMENTABORTED\n",contador17)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS SW_ERROR\n",contador18)
printf(">>>SE HAN CONTABILIZADO %s ESTADOS RADIOHWERROR\n",contador19)



writer.write("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\nEVENTS\n\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador1  + " ESTADOS CHANGEMODE\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador2  + " ESTADOS SEPARATIONTIMEREXPIRED\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador3  + " ESTADOS RESET\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador4  + " ESTADOS COMMISSIONINGDONE\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador5  + " ESTADOS TCINVALID\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador6  + " ESTADOS INITIALIZATIONERROR\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador7  + " ESTADOS BATTERYLOWWARNING\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador8  + " ESTADOS BATTERYCRITICALWARNING\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador9  + " ESTADOS BATTERYHIGHWARNING\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador10  + " ESTADOS PROTECTIONERROR\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador11 + " ESTADOS COMMISSIONINGERROR\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador12  + " ESTADOS LOSTCOMM\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador13  + " ESTADOS TCRECEIVED\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador14  + " ESTADOS NONTRUSTEDSOURCE\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador15  + " ESTADOS TIMEDTCINVALID\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador16  + " ESTADOS EXPERIMENTDONE\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador17  + " ESTADOS EXPERIMENTABORTED\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador18  + " ESTADOS SW_ERROR\n")
writer.write(">>>SE HAN CONTABILIZADO  " +   contador19  + " ESTADOS RADIOHWERROR\n")





printf("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\n")
printf("ULTIMO ESTADO DEL SATELITE:\n\n")
printf("HORA: %s,     BATT_T_EXT_TM: %s,     BATT_T_INT_TM: %s,    BATT_TBAT1_TM: %s,    BATT_TBAT2_TM: %s,   BATT_TBAT3_TM: %s\n", rdd2(posUltimo).time_received, rdd2(posUltimo).batt_t_ext_tm, rdd2(posUltimo).batt_t_int_tm, rdd2(posUltimo).batt_tbat1_tm, rdd2(posUltimo).batt_tbat2_tm, rdd2(posUltimo).batt_tbat3_tm)
printf("Modo de operacion: %s \n\n\n", rdd2(posUltimo).current_operating_mode)



writer.write("\n\n\n------------------------------------------------------------------------------------------------------------------------------------------\n")
writer.write("ULTIMO ESTADO DEL SATELITE:\n\n")
writer.write("HORA:  " +    rdd2(posUltimo).time_received  + " ||  BATT_T_EXT_TM: " + rdd2(posUltimo).batt_t_ext_tm + " ||  BATT_T_INT_TM: " + rdd2(posUltimo).batt_t_int_tm + " ||  BATT_TBAT1_TM: " + rdd2(posUltimo).batt_tbat1_tm +  " ||  BATT_TBAT2_TM: " + rdd2(posUltimo).batt_tbat2_tm + " ||  BATT_TBAT3_TM: " + rdd2(posUltimo).batt_tbat3_tm + "\n")
writer.write("Modo de operacion: " + rdd2(posUltimo).current_operating_mode + "\n\n\n")


writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>FIN DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.close()
