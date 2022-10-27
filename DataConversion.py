import pandas as pd
import numpy as np
from pathlib import Path 
import shutil
from os import remove

# Nombramos la ruta hacia nuestros archivos nuevos que serán incorporados a la base de datos

input_path = Path ("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Input")

output_path = Path ("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output")

backup_path = Path ('G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Backup')


# Realizamos la importación de los archivos, con un procedimiento distinto dependiendo de su extensión, para luego normalizarlo y exportarlo a un nuevo directorio.

for file in input_path.iterdir():        # Iteración por cada archivo en el directorio 
    shutil.copy(file, backup_path)       # Se realiza un backup de cada archivo
    if file.suffix == '.csv':
        db1=pd.read_csv(file, delimiter= ',', encoding='utf-16')
        db1 = db1[['precio', 'producto_id', 'sucursal_id']]             # Se ordenan las columnas en caso de que no estén en orden
        db1 = db1.replace('', np.nan, regex=True)                       # Se remplazan los valores vacios por NaN
        db1 = db1.dropna()                                              # Se quitan los valores NaN
        db1.drop_duplicates()                                           # Se eliminan filas con valores duplicados
        db1.to_csv("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db1.csv", index=False) # Se exporta el archivo resultante con la extensión .csv
        remove(file)                                                    # Se elimina el archivo de input
    elif file.suffix == '.txt':
        db2=pd.read_csv(file, delimiter= '|', encoding='utf-8')
        db2 = db2[['precio', 'producto_id', 'sucursal_id']]
        db2 = db2.replace('', np.nan, regex=True)
        db2 = db2.dropna()
        db2.drop_duplicates()
        db2.to_csv("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db2.csv", index=False)
        remove(file) 
    elif file.suffix == '.json':
        db3=pd.read_json(file)
        db3 = db3[['precio', 'producto_id', 'sucursal_id']]
        db3 = db3.replace('', np.nan, regex=True)
        db3 = db3.dropna()
        db3.drop_duplicates()
        db3.to_csv("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db3.csv", index=False)
        remove(file) 
    elif file.suffix == '.xlsx':
        db4=pd.read_excel(file)
        db4 = db4[['precio', 'producto_id', 'sucursal_id']]
        db4 = db4.replace('', np.nan, regex=True)
        db4 = db4.dropna()
        db4.drop_duplicates()
        db4.to_csv("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db4.csv", index=False)
        remove(file) 
    elif file.suffix == '.parquet':
        db5=pd.read_parquet(file)
        db5 = db5[['precio', 'producto_id', 'sucursal_id']]
        db5 = db5.replace('', np.nan, regex=True)
        db5 = db5.dropna()
        db5.drop_duplicates()
        db5.to_csv("G:\Mi unidad\Henry\Data Science\Henry Labs\PI 01 b\PI01_DATA_ENGINEERING\Test\Datasets\Output\db5.csv", index=False)
        remove(file) 
    else: print ('File extension not supported')