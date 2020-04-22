
import com.datastax.spark.connector._;
import org.apache.spark.sql.cassandra._;
import scala.math.max;
import org.apache.spark.sql.functions._;
import scala.collection.mutable;
import scala.io.StdIn._;
import java.io.File
import java.io.PrintWriter




case class EventError (tm_type: String,time_received: String,event :String,mission_time: Int,parameter_id: Int,parameter_value1: String,parameter_value2: String,sequencecount: Int,tm_id: BigInt);

val rdd = sc.cassandraTable[EventError]("upmsat2db", "eventerror").collect();

val informe = new File("C:/Users/chech/Desktop/TFG/VERSION_NUEVA/PROGRAMAS_SCALA/CODIGO/informe1.txt")

val writer = new PrintWriter(informe)


printf("\n\n\n")


  writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>INICIO DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

if(rdd.size>0){printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>DATOS RECOGIDOS CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

}else {printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ERROR AL RECOGER LOS DATOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")}

val x = rdd.size;
var limiteInf = 0;
var limiteSup = 0;

printf("\n\n\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TIEMPOS ELEGIDOS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")


var valorTiempo1 = "2020-04-10 16:56:25.764204";
var valorTiempo2 = "2020-04-09 11:05:52.080439";


writer.write("valorTiempo1 =  " + valorTiempo1 + "\n\n")
writer.write("valorTiempo2 =  "  + valorTiempo2 + "\n\n")



printf("\n\n\n")


//A LA ESPERA DEL SEQ QUE NO HE ENTENDIDO MUY BIEN


for (y <- 0 until x) if (rdd(y).time_received == valorTiempo1) limiteInf = y;
for (z <- 0 until x) if (rdd(z).time_received == valorTiempo2) limiteSup = z;




printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>EVENT ERROR<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
printf(">>>>>>>>>>>>>>>>>>>>>>EVOLUCION GENERAL DE EVENTOS : TELEMETRIA<<<<<<<<<<<<<<<<<<<<<<\n\n      DESDE : %s        HASTA : %s\n\n",valorTiempo2,valorTiempo1)
writer.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>EVENT ERROR<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")
writer.write(">>>>>>>>>>>>>>>>>>>>>>EVOLUCION GENERAL DE EVENTOS : TELEMETRIA<<<<<<<<<<<<<<<<<<<<<<\n\n      DESDE : " + valorTiempo1    +    "HASTA :  " + valorTiempo2 + "\n\n")


for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && rdd(y).event == "SW_ERROR"){
        printf("Fecha:   %s     ||     Evento:   %s                ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event + "  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")

    }
printf("\n\n\n")
for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).event == "CHANGEMODE")){
        if(rdd(y).parameter_value1 == "Nominal"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE NOMINAL A EXPERIMENT                ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +  "  -->DE NOMINAL A EXPERIMENT  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Experiment"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE EXPERIMENT a NOMINAL                ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +  "  -->DE EXPERIMENT a NOMINAL  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Safe"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE SAFE a NOMINAL                      ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  -->DE SAFE a NOMINAL    ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Initialization" && rdd(y).parameter_value2 == "Comissioning"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE INITIALIZATION a COMISSIONING       ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +  "  -->DE INITIALIZATION a COMISSIONING  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Initialization" && rdd(y).parameter_value2 == "Safe"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE INITIALIZATION a SAFE               ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +  "  -->DE INITIALIZATION a SAFE    ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Comissioning" && rdd(y).parameter_value2 == "Safe"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE COMMISSIONING a SAFE                ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  -->DE COMMISSIONING a SAFE  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Safe" && rdd(y).parameter_value2 == "Commissioning"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE SAFE a COMMISSIONING               ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  -->DE SAFE a COMMISSIONING  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
      }else if(rdd(y).parameter_value1 == "Safe" && rdd(y).parameter_value2 == "Beacon"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE SAFE a BEACON               ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  -->DE SAFE a BEACON  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
        }else if(rdd(y).parameter_value1 == "Beacon" && rdd(y).parameter_value2 == "Safe"){
        printf("Fecha:   %s     ||     Evento:   %s  -->DE BEACON a SAFE               ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  -->DE BEACON a SAFE  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
}}


printf("\n\n\n")
for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).event == "TCINVALID")){
        printf("Fecha:   %s     ||     Evento:   %s         ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +   "  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")
}

printf("\n\n\n")
for (y <- 0 until x)
    if (y >= limiteInf && y <= limiteSup && (rdd(y).event == "COMMISSIONINGERROR")){
        printf("Fecha:   %s     ||     Evento:   %s             EL SATELITE HA MODIFICADO SU ESTADO A OFF            ||     Hora del satélite: %s\n" , rdd(y).time_received, rdd(y).event, rdd(y).mission_time)
        writer.write("  Fecha:  " +    rdd(y).time_received  + "  ||  Evento:  "  +  rdd(y).event +  "  EL SATELITE HA MODIFICADO SU ESTADO A OFF  ||  Hora del satélite:  " + rdd(y).mission_time + "\n")

}
printf("\n")

printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ANALISIS REALIZADO CORRECTAMENTE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.write("\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>FIN DEL INFORME<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n")

writer.close()
