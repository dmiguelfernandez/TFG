
//HOUSEKEEPING
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import scala.math.max
import org.apache.spark.sql.functions._
import scala.collection.mutable
import java.io.File
import java.io.PrintWriter

case class Housekeeping(tm_type: String,time_received: String,batt_t_ext_tm: Int,batt_t_int_tm : Int,batt_tbat1_tm : Int,batt_tbat2_tm : Int,batt_tbat3_tm : Int,batt_vbat_tm : Int,batt_vbus_tm : Int,battery_status: String,boom1_vbus : Boolean,boom2_vbus : Boolean,current_operating_mode: String,current_time : Int,das_n15v : Boolean,das_p15v : Boolean,das_p3v : Boolean,das_p5v : Boolean,ebox_t_ext_tm : Int,ebox_t_int_tm : Int,mgm1_p5v : Boolean,mgm1_t_tm : Int,mgm1_x_tm : Int,mgm1_y_tm : Int,mgm1_z_tm : Int,mgm2_p5v : Boolean,mgm2_t_tm : Int,mgm2_x_tm : Int,mgm2_y_tm : Int,mgm2_z_tm : Int,mgm3_n15v : Boolean,mgm3_p15v : Boolean,mgm3_t_tm : Int,mgm3_x_tm : Int,mgm3_y_tm : Int,mgm3_z_tm : Int,mgt_tx_tm : Int,mgt_x_vbus : Boolean,modem_t_tr_tm : Int,modem_vbus : Boolean,mts_p1tts1_tm : Int,mts_p1tts2_tm : Int,mts_p1tts3_tm : Int,mts_p1tts4_tm : Int,mts_p1tts5_tm : Int,mts_p1tts6_tm : Int,mts_vbus : Boolean,n15v_tm : Int,obc_t_tm : Int,p15v_tm : Int,p3v3_tm : Int, p5v_tm : Int,pdu_ivbus_tm : Int,pdu_p3v3 : Boolean,pdu_p5v : Boolean,psu_in15v_tm : Int,psu_ip15v_tm : Int,psu_ip3v3_tm : Int,psu_ip5v_tm : Int,psu_t_tm : Int,pv_ispxn_tm : Int,pv_ispxp_tm : Int,pv_ispyn_tm : Int,pv_ispyp_tm : Int,pv_ispzp_tm : Int,pv_tpsxn_tm : Int,pv_tpsxp_tm : Int,pv_tpsyn_tm : Int,pv_tpsyp_tm : Int,pv_tpszp_tm : Int,rw1_t_tm : Int,rw2_t_tm : Int,rw_p5v : Boolean,rw_vbus : Boolean,sequencecount : Int,sma_sb01 : Boolean,sma_sb02 : Boolean,ss6_xn_tm : Int,ss6_xp_tm : Int,ss6_yn_tm : Int,ss6_yp_tm : Int,ss6_zn_tm : Int,ss6_zp_tm : Int,temp_a_p5v : Boolean,temp_b_p5v : Boolean,tm_id : BigInt,ttc_stat : Boolean)



val rdd = sc.cassandraTable[Housekeeping]("upmsat2db", "housekeeping").collect()


val informe = new File("C:/Users/chech/Desktop/TFG/VERSION_NUEVA/PROGRAMAS_SCALA/CODIGO/informe2.txt")

val writer = new PrintWriter(informe)
printf("\n\n\n")

if(rdd.size>0){printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>DATOS RECOGIDOS CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
}else {printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ERROR AL RECOGER LOS DATOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")}

  writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INICIO DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

val x = rdd.size

var x2 = 0;
var x3 = 0;
var x4 = 0;
var x5 = 0;
var x6 = 0;


var limiteInf = 0;
var limiteSup = 0;

printf("\n\n\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


var valorTiempo1 = "2020-04-10 19:03:43.023810"
var valorTiempo2 = "2020-04-10 19:03:25.841663"


writer.write("valorTiempo1 =  " + valorTiempo1 + "\n\n")
writer.write("valorTiempo2 =  "  + valorTiempo2 + "\n\n")



for (y <- 1 until x) if (rdd(y).time_received == valorTiempo1) limiteInf = y;
for (z <- 1 until x) if (rdd(z).time_received == valorTiempo2) limiteSup = z;






///////////////////////////////////////////////////////////////////////////////////
printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_T_EXT_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_T_EXT_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")



for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971)){
        if(rdd(y).batt_t_ext_tm <= 1428){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:  -40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  -40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1428 && rdd(y).batt_t_ext_tm <=1518){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:  -30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  -30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1518 && rdd(y).batt_t_ext_tm <=1592){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:  -20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  -20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1592 && rdd(y).batt_t_ext_tm <=1652){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:  -10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  -10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1652 && rdd(y).batt_t_ext_tm <=1701){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   0ªC           ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  0ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1701 && rdd(y).batt_t_ext_tm <=1741){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1741 && rdd(y).batt_t_ext_tm <=1774){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1774 && rdd(y).batt_t_ext_tm <=1803){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1803 && rdd(y).batt_t_ext_tm <=1830){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1830 && rdd(y).batt_t_ext_tm <=1859){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   50ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        }else if(rdd(y).batt_t_ext_tm >1859 && rdd(y).batt_t_ext_tm <=1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   60ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  60ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else if(rdd(y).batt_t_ext_tm >1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_ext_tm:   70ºC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " Valor fuera de rango de Batt_t_ext_tm:  70ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")
        }else{
        printf("ERROR")
        writer.write("ERROR")


        }
    }


///////////////////////////////////////////////////////////////////////////////////


printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_T_INT_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_T_INT_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")

for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971) ){
        if(rdd(y).batt_t_int_tm <= 1428){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:  -40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  -40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1428 && rdd(y).batt_t_int_tm <=1518){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:  -30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  -30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1518 && rdd(y).batt_t_int_tm <=1592){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:  -20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  -20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1592 && rdd(y).batt_t_int_tm <=1652){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:  -10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  -10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1652 && rdd(y).batt_t_int_tm <=1701){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   0ªC           ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  0ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1701 && rdd(y).batt_t_int_tm <=1741){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1741 && rdd(y).batt_t_int_tm <=1774){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1774 && rdd(y).batt_t_int_tm <=1803){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1803 && rdd(y).batt_t_int_tm <=1830){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1830 && rdd(y).batt_t_int_tm <=1859){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   50ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  50ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1859 && rdd(y).batt_t_int_tm <=1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   60ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  60ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_t_int_tm >1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_t_int_tm:   70ºC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_t_int_tm:  70ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else{
        printf("ERROR")
          writer.write("ERROR")

        }
    }



///////////////////////////////////////////////////////////////////////////////////

printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT1_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT1_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")

for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971) ){
        if(rdd(y).batt_tbat1_tm <=  1428){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:  -40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  -40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1428 && rdd(y).batt_tbat1_tm <=1518){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:  -30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  -30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1518 && rdd(y).batt_tbat1_tm <=1592){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:  -20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  -20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1592 && rdd(y).batt_tbat1_tm <=1652){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:  -10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  -10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1652 && rdd(y).batt_tbat1_tm <=1701){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   0ªC           ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  0ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1701 && rdd(y).batt_tbat1_tm <=1741){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1741 && rdd(y).batt_tbat1_tm <=1774){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1774 && rdd(y).batt_tbat1_tm <=1803){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1803 && rdd(y).batt_tbat1_tm <=1830){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1830 && rdd(y).batt_tbat1_tm <=1859){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   50ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  50ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1859 && rdd(y).batt_tbat1_tm <=1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   60ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  60ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat1_tm >1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat1_tm:   70ºC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat1_tm:  70ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else{
        printf("ERROR")
          writer.write("ERROR")

        }
    }



///////////////////////////////////////////////////////////////////////////////////

printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT2_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT2_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")

for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971) ){
        if(rdd(y).batt_tbat2_tm <= 1428){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:  -40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  -40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1428 && rdd(y).batt_tbat2_tm <=1518){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:  -30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  -30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1518 && rdd(y).batt_tbat2_tm <=1592){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:  -20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  -20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1592 && rdd(y).batt_tbat2_tm <=1652){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:  -10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  -10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1652 && rdd(y).batt_tbat2_tm <=1701){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   0ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  0ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1701 && rdd(y).batt_tbat2_tm <=1741){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1741 && rdd(y).batt_tbat2_tm <=1774){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1774 && rdd(y).batt_tbat2_tm <=1803){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1803 && rdd(y).batt_tbat2_tm <=1830){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1830 && rdd(y).batt_tbat2_tm <=1859){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   50ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  50ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1859 && rdd(y).batt_tbat2_tm <=1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   60ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  60ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat2_tm >1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat2_tm:   70ºC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat2_tm:  70ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else{
        printf("ERROR")
          writer.write("ERROR")

        }
    }

///////////////////////////////////////////////////////////////////////////////////
printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT3_TM  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BATT_TBAT3_TM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")

for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971)){
        if(rdd(y).batt_tbat3_tm <= 1428){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:  -40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  -40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1428 && rdd(y).batt_tbat3_tm <=1518){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:  -30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  -30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1518 && rdd(y).batt_tbat3_tm <=1592){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:  -20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  -20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1592 && rdd(y).batt_tbat3_tm <=1652){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:  -10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  -10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1652 && rdd(y).batt_tbat3_tm <=1701){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   0ªC           ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  0ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1701 && rdd(y).batt_tbat3_tm <=1741){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   10ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  10ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1741 && rdd(y).batt_tbat3_tm <=1774){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   20ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  20ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1774 && rdd(y).batt_tbat3_tm <=1803){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   30ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  30ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1803 && rdd(y).batt_tbat3_tm <=1830){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   40ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm: 40ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1830 && rdd(y).batt_tbat3_tm <=1859){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   50ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  50ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1859 && rdd(y).batt_tbat3_tm <=1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   60ªC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  60ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else if(rdd(y).batt_tbat3_tm >1920){
        printf("Fecha:   %s     ||     Valor fuera de rango de Batt_tbat3_tm:   70ºC          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||    Valor fuera de rango de Batt_tbat3_tm:  70ªC    ||  Hora del satélite:  " + rdd(y).current_time + "\n")

        }else{
        printf("ERROR")
          writer.write("ERROR")

        }
    }

///////////////////////////////////////////////////////////////////////////////////
printf("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>MODOS DE OPERACION <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
printf("DESDE: %s                               HASTA: %s\n\n",valorTiempo2,valorTiempo1)

writer.write("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>MODOS DE OPERACION <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
writer.write("DESDE : " + valorTiempo1    +    " HASTA :  " + valorTiempo2 + "\n\n")

for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).batt_t_ext_tm >= 866 || rdd(y).batt_t_ext_tm <= 1971) ){
        printf("Fecha:   %s     ||     Modo de operacion: %s          ||     Hora del satélite: %s\n" , rdd(y).time_received,rdd(y).current_time,rdd(y).current_operating_mode)
        writer.write("  Fecha:  " +    rdd(y).time_received  + " ||     Modo de operacion: " + rdd(y).current_operating_mode +  "         ||     Hora del satélite:  " + rdd(y).current_time + "\n")}



printf("\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ANALISIS REALIZADO CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>FIN DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.close()
