# TFG

El proyecto UPMSat-2 es un proyecto de la Universidad Politécnica de Madrid que consiste en el diseño, desarrollo, lanzamiento y comunicación de un microsatélite que será utilizado como plataforma de demostración tecnológica y validación de dispositivos y experimentos. El grupo IDR/UPM (Instituto de Microgravedad “Ignacio Da Riva”). El grupo STRAST se ha encargado del ámbito telemático del sistema, que conlleva el desarrollo de software para los segmentos de vuelo y tierra, así como un sistema de comunicación con el satélite.

El lanzamiento de UPMSat2 se planificó para el 24 de marzo. Sin embargo, se ha pospuesto por la crisis del COVID-19. Se espera que se lance en tres meses. El satélite se ha integrado en el lanzador Vega en el Guiana Space Centre en Kourou.

El segmento de tierra está operativo. Se han completado las funciones para gestionar los telecomandos a enviar al satélite, recibir y acceder a los datos de telemetría emitidos por el satélite y comunicar con el subsistema de gestión de la antena, gestionando fundamentalmente la base de datos Cassandra. El sistema resultante ha satisfecho los requisitos y funciones más importante, como la disponibilidad, tolerancia de fallos y transparencia.

La vida de vida de operativa del satélite es 3 años. El periodo de órbita es 97 minutos, con una conexión de 10 máxima de minutos, dos veces al día. Se espera recibir unas 1440 muestras cada día. La cantidad de datos es elevada, al igual que el esfuerzo necesario para procesarlos. En consecuencia, es necesario proporcionar herramientas que analicen eficiente y automáticamente la telemetría recibida.

El objetivo de este proyecto es desarrollar un subsistema que procese automáticamente la telemetría recibida del satélite y que genere los informes relevantes en cada órbita. En concreto, los objetivos son:
•	Estudio y análisis de la tecnología y las herramientas para en implementación de este proyecto.
•	Especificación de los informes requeridos de los investigadores y operadores del satélite y de la explotación de los datos recibidos.
•	Integración del subsistema en el segmento de tierra disponible.
•	Diseño, implementación, prueba y despliegue del subsistema de procesamiento de telemetría.
•	Documentación final del sistema.

La metodología usada y seguida para la realización del proyecto será:
•	Obtención de documentación sobre Cassandra, Scala, Spark, Data Analytics y Data Visualization y Zeppelin,
•	Estudio de las tecnologías, su interconexión y sus funciones básicas.
•	Especificación de los informes solicitados en el del subsistema de procesamiento automático de la telemetría.
•	Integración y configuración de las técnicas y herramientas del sistema.
•	Diseño, implementación y puesta en producción el subsistema.  
•	Exploración de herramientas de visualización de la telemetría.
•	Documentación del sistema software desarrollado y elaboración de la memoria final.

Bibliografía:

•	Programming in Scala, Third Edition – Martin Odersky, Lex Spoon, Bill Venners.
•	Cassandra, The Definitive Guide – Jeff Carpenter, Eben Hewitt.
•	Spark: The Definitive Guide –  By Bill Chambers and Matei Zaharia
•	https://medium.com/@alexbmeng/cassandra-query-language-cql-vs-sql-7f6ed7706b4c
•	https://www.upwork.com/hiring/data/scala-hybrid-functionalobject-oriented-language-big-data/
•	http://cassandra.apache.org/
•	https://zeppelin.apache.org/
