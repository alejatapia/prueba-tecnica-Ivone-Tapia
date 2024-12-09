from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import regexp_replace, col
import os
import shutil
from pyspark.sql import functions as F


class ETLPipeline:
    def __init__(self, spark, file_path):
        # Inicializa la sesión Spark y carga el archivo fuente.
        self.spark = spark
        self.file_path = file_path
        self.tables = {} 
    
    def extract(self, sheet_names):
        for sheet in sheet_names:
            self.tables[sheet] = self.spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("dataAddress", f"{sheet}!A1") \
                .load(self.file_path)
    
    def clean_column_names(self):
        for table_name, df in self.tables.items():
            clean_df = df.select([col(c).alias(c.strip()) for c in df.columns])
            self.tables[table_name] = clean_df
    
    def only_numbers(self,table_name,columns_name,type_):
        for column in columns_name:
            self.tables[table_name] = self.tables[table_name].withColumn(
                column,
                regexp_replace(col(column), "[^0-9.]", "")
            ).withColumn(
                column,
                col(column).cast(type_)
            )
    
    def castings(self,table_name,columns_name,type_):
        for column in columns_name:
            self.tables[table_name] = self.tables[table_name].withColumn(column, col(column).cast(type_))
            if type_ == 'boolean':
                self.tables[table_name] = self.tables[table_name].withColumn(column,
                when(col(column) == 1, True).when(col(column) == 0, False).otherwise(None)
                )

    def handle_nulls(self):
        for table_name, df in self.tables.items():
            for col_name, dtype in df.dtypes:
                if dtype in ['int', 'bigint', 'float', 'double', 'decimal']:
                    df = df.na.fill({col_name: 0})
                elif dtype in ['string', 'varchar', 'char']:
                    df = df.withColumn(
                        col_name,
                        F.when(F.trim(F.col(col_name)) == "NULL", "").otherwise(F.col(col_name))
                     )
                    df = df.na.fill({col_name: ""})         
                elif dtype in ['date', 'timestamp']:
                    df = df.na.fill({col_name: None})
            self.tables[table_name] = df
    
    def trim_string_columns(self):
        for table_name, df in self.tables.items():
            for col_name, dtype in df.dtypes:
                if dtype in ['string', 'varchar', 'char']:
                    df = df.withColumn(col_name, F.trim(F.col(col_name)))
            self.tables[table_name] = df
    
    def transform(self,columns_to_clean):
        self.clean_column_names()
        self.trim_string_columns()
        self.handle_nulls()
        for table_name in self.tables:
            self.tables[table_name] = self.tables[table_name].dropDuplicates()
        for item in columns_to_clean:
            table_name = item["table_name"]
            data_type = item["data_type"]
            columns = item["columns"]            
            if data_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                self.only_numbers(table_name, columns, data_type)
            else:
                self.castings(table_name, columns, data_type)
        
    def load(self, db_url, db_properties,tables_order,mode_):
        print('**************INICIO LOAD*****************')
        try:
            for table_name in tables_order:
                if table_name in self.tables:
                    df = self.tables[table_name]
                    df.write.jdbc(
                        url=db_url,
                        table=table_name,
                        mode=mode_,
                        properties=db_properties
                    )
                    print(f"Tabla {table_name} cargada exitosamente.")
                else:
                    print(f"La tabla {table_name} no está en los datos disponibles.")
        except Exception as e:
            print(f"Error al cargar las tablas a la base de datos: {e}")
            raise        
        print('**************FIN LOAD*****************')
    
    @staticmethod
    def create_spark_session(app_name="ETL Pipeline"):
        # Inicializa una sesión de Spark
        spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark


if __name__ == "__main__":
    # 1. Crear la sesión de Spark
    spark = ETLPipeline.create_spark_session()
    
    # 2. Definir el archivo y las hojas
    file_path = "Films_2 (4).xlsx"
    sheet_names = ["film", "inventory", "rental", "customer", "store"]
    
    # 3. Inicializar el pipeline
    etl = ETLPipeline(spark, file_path)
    
    # 4. Ejecutar ETL
    etl.extract(sheet_names)
    
    # Limpieza de datos: se ingresa las tablas, columnas y tipo de dato al que se quiere convetir las columnas
    columns_to_clean = [
        {
            "table_name": "film",
            "data_type": "int",
            "columns": ["film_id", "release_year", "num_voted_users", "language_id", "length","original_language_id","rental_duration"]
        },
        {
            "table_name": "film",
            "data_type": "double",
            "columns": ["rental_rate", "replacement_cost"]
        },
        {
            "table_name": "film",
            "data_type": "timestamp",
            "columns": ["last_update"]
        },
        {
            "table_name": "inventory",
            "data_type": "int",
            "columns": ["store_id"]
        },
        {
            "table_name": "store",
            "data_type": "timestamp",
            "columns": ["last_update"]
        },
        {
            "table_name": "store",
            "data_type": "int",
            "columns": ["store_id","manager_staff_id","address_id"]
        },
        {
            "table_name": "customer",
            "data_type": "boolean",
            "columns": ["active"]
        },
        {
            "table_name": "customer",
            "data_type": "timestamp",
            "columns": ["create_date","last_update"]
        },
        {
            "table_name": "customer",
            "data_type": "int",
            "columns": ["customer_id","store_id","address_id"]
        },
        {
            "table_name": "inventory",
            "data_type": "timestamp",
            "columns": ["last_update"]
        },
        {
            "table_name": "inventory",
            "data_type": "int",
            "columns": ["inventory_id","film_id"]
        },
        {
            "table_name": "rental",
            "data_type": "int",
            "columns": ["rental_id","inventory_id","customer_id","staff_id"]
        },
        {
            "table_name": "rental",
            "data_type": "timestamp",
            "columns": ["rental_date","return_date","last_update"]
        }
        
    ]
    etl.transform(columns_to_clean)
    
    # Configuración de la base de datos
    db_url = "jdbc:postgresql://database-1.cn8440ecs11q.us-east-2.rds.amazonaws.com:5432/postgres"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    # Cargar a PostgreSQL
    tables_order = ['store', 'film', 'customer', 'inventory', 'rental']
    mode_="append"
    etl.load(db_url, db_properties,tables_order,mode_)
    
    # 5. Detener la sesión
    spark.stop()

