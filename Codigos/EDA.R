#install.packages("data.table")
#install.packages("arrow")

library(data.table)
library(arrow)

# Leer el archivo CSV de forma eficientE
df <- fread("Datos/eci_transactions.csv")  

write_parquet(df, "eci_transactions.parquet")

eci_transactions <- spark_read_parquet(sc, name = "eci_transacciones", path = "eci_transactions.parquet")

#Transformo la base para obtener lo que necesito


