import pymysql as sql
import pandas as pd
import numpy as np
from pathlib import Path 
import shutil
from os import remove


class Database:
    def __init__(self):
        self.connection = sql.connect(              # Conexión con MySQL
            host = 'localhost',
            user = 'root',
            password = '*****',
            db = 'pi_01b',
            port = 3306, 
            
        )
        
        self.cursor = self.connection.cursor()
        print ("MySQL Connection Established")
        
database = Database()
database    

output_path = Path ("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output")

sql_path = Path ("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b")

loaded_path = Path("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Loaded")  


for file in output_path.iterdir():        # Iteración por cada archivo en el directorio 
    shutil.copy(file, sql_path)           # Copiamos los archivos al directorio seguro de MySQL
    


# Carga a la tabla 'precios' de MySQL de los archivos que se encuentren en la carpeta 

for file in sql_path.iterdir():     # Para cada archivo de la carpeta, dependiendo de su nombre y por ende su extensión previa
    if file.stem == "db1":          # Se carga en SQL
        data_load = "LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b\\db1.csv' INTO TABLE precios FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        shutil.copy(file, loaded_path)  # Se traslada el archivo a la carpeta 'Loaded'
        remove(file)                    # Se elimina de la carpeta de sql
        remove("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db1.csv")  # Se elimina el archivo de la carpeta Output
    elif file.stem == "db2":
        data_load = "LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b\\db2.csv' INTO TABLE precios FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        shutil.copy(file, loaded_path)
        remove(file)
        remove("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db2.csv")   
    elif file.stem == "db3":
        data_load = "LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b\\db3.csv' INTO TABLE precios FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        shutil.copy(file, loaded_path) 
        remove(file)
        remove("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db3.csv")  
    elif file.stem == "db4":
        data_load = "LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b\\db4.csv' INTO TABLE precios FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        shutil.copy(file, loaded_path)
        remove(file)
        remove("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db4.csv")   
    elif file.stem == "db5":
        data_load = "LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pi_01b\\db5.csv' INTO TABLE precios FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        shutil.copy(file, loaded_path)
        remove(file)
        remove("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db5.csv")  
    else: print ("No data load available")
    