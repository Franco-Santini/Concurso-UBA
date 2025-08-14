#install.packages("data.table")
#install.packages("arrow")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(data.table)
library(arrow)
library(readr)
library(kableExtra)

#Configuraciones de spark
config <- spark_config()
config$spark.driver.memory <- "8g"
config$spark.executor.instances <- 4
config$spark.executor.memory <- "4g"
config$spark.executor.cores <- 2
config$spark.storage.memoryFraction <- 0.8
config$spark.memory.fraction <- 0.7
config$spark.memory.storageFraction <- 0.6
config$`spark.network.timeout` <- "600s"
config$`spark.sql.shuffle.partitions` <- 200

# Establecemos la coneccion con spark
sc <- spark_connect(master = "local", config = config)

# Leer el archivo CSV de forma eficientE
#df <- fread("Datos/eci_transactions.csv")  

eci_stores <- read_csv("Datos/eci_stores.csv")

eci_product_groups <- read_csv("Datos/eci_product_groups.csv")

eci_product_master <- read_csv("Datos/eci_product_master.csv")

#write_parquet(df, "eci_transactions.parquet")

ds <- open_dataset("eci_transactions.parquet")

eci_transactions <- spark_read_parquet(sc, name = "eci_transacciones", path = "eci_transactions.parquet")

#Saco una muestra de transacciones

muestra <- eci_transactions %>%
  sdf_sample(fraction = 10000 / sdf_nrow(eci_transactions), replacement = FALSE) %>%
  collect()

#Agrego los costos y precios base.

eci_product_master <- eci_product_master |> rename(SKU = sku, SUBGROUP = subgroup)

eci_transactions_stores_prod <- muestra |> 
  left_join(eci_product_master, by = c("SKU", "SUBGROUP"))

#Agrupo por el día, subproducto y tienda.

datos_final <- eci_transactions_stores_prod |> 
  mutate(QUANTITY = round(TOTAL_SALES/PRICE)) |> 
  group_by(STORE_SUBGROUP_DATE_ID) |> 
  summarise(TOTAL_SALES_ = sum(TOTAL_SALES),
            QUANTITY_ = sum(QUANTITY),
            PRICE_ = sum(PRICE),
            BASE_PRICE_ = sum(base_price),
            INITIAL_TICKET_PRICE_ = sum(initial_ticket_price),
            COSTOS_ = sum(costos),
            SKU = SKU
  ) |> 
  ungroup() |>
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_")

#agrego los datos de la ciudad, cadena, etc

datos_final <- datos_final |> 
  left_join(eci_stores, by = "STORE_ID") |>
  select(!c(ADDRESS1, ADDRESS2, ZIP, OPENDATE, CLOSEDATE)) 

#Agrego los datos de categoría del producto, de grupo del producto y de precios

datos_final <- datos_final |> 
  left_join(eci_product_master, by = c("SKU", "SUBGROUP")) |>
  select(!c(product_name,brand,base_price,initial_ticket_price,costos)) 




