Esta aplicación es un pipeline ETL desarrollado en Python utilizando PySpark.
Construido en mi pc personal con estos requisitos:

- Requisitos del Sistema
Sistema Operativo: Windows 10.
Python: Versión 3.10 o superior.
Java: JDK 17: C:\Program Files\Java\jdk-17
Apache Spark: Versión 3.5.3: C:\spark-3.5.3-bin-hadoop3
Hadoop binarios: Para permitir lectura de archivos locales: C:\hadoop\binwinutils.exe

- Configuración de variables de entorno
JAVA_HOME: Ruta a la instalación de Java (ej: C:\Program Files\Java\jdk-17)
SPARK_HOME: Ruta a la instalación de Spark (ej: C:\spark-3.5.3-bin-hadoop3)
PYSPARK_HOME: Igual que SPARK_HOME.
HADOOP_HOME: Ruta al directorio de binarios Hadoop (ej: C:\hadoop).
Agregar SPARK_HOME\bin y HADOOP_HOME\bin al PATH del sistema

- Instalación de dependencias
Se encuentran en el archivo requirements.txt

- Configuración adicional
Usar el archivo Creación de tablas.sql para crear la estructura de tablas en postgres
Se uso el driver postgresql-42.7.4.jar, se ubica en el directorio raíz del proyecto

Ejecución del Pipeline:
Una vez hecha la creacion de las tablas en postgres (Creación de tablas.sql)
Abrir un cmd en \Prueba tecnica Ivonne Tapia\etl
ejecutar: py etl.py
