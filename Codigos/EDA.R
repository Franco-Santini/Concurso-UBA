#install.packages("data.table")
#install.packages("arrow")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(data.table)
library(arrow)
library(readr)
library(kableExtra)
library(plotly)

#Configuraciones de spark
config <- spark_config()

# Establecemos la coneccion con spark
sc <- spark_connect(master = "local", config = config)

#spark_disconnect_all() desconectar spark

# Leer el archivo CSV de forma eficientE
#df <- fread("Datos/eci_transactions.csv")  

eci_stores <- read_csv("Datos/eci_stores.csv")

eci_product_groups <- read_csv("Datos/eci_product_groups.csv")

eci_product_master <- read_csv("Datos/eci_product_master.csv")

#write_parquet(df, "eci_transactions.parquet")

ds <- open_dataset("eci_transactions.parquet")

eci_transactions <- spark_read_parquet(sc, name = "eci_transacciones", path = "eci_transactions.parquet")

#Para la base completa

# Copiar tabla de tiendas y productos a Spark

eci_product_master <- rename(eci_product_master, SKU = sku, SUBGROUP = subgroup)
eci_stores_tbl   <- copy_to(sc, eci_stores,        name = "eci_stores",        overwrite = TRUE)
eci_products_tbl <- copy_to(sc, eci_product_master, name = "eci_product_master", overwrite = TRUE)

#Agrego los costos y precios base

datos_tbl <- eci_transactions %>%
  left_join(eci_products_tbl, by = c("SKU", "SUBGROUP"))

#Agrupo por el día, subproducto y tienda.

print(colnames(datos_tbl))#Voy viendo que todas las variables esten

datos_tbl <- datos_tbl %>%
  mutate(QUANTITY = round(TOTAL_SALES / PRICE)) %>%
  group_by(STORE_SUBGROUP_DATE_ID) %>%
  summarise(
    TOTAL_SALES_        = sum(TOTAL_SALES),
    QUANTITY_           = sum(QUANTITY),
    PRICE_              = sum(PRICE),
    BASE_PRICE_         = sum(base_price),
    INITIAL_TICKET_PRICE_ = sum(initial_ticket_price),
    COSTOS_             = sum(costos)
  ) %>%
  ungroup() %>%
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) %>%
  tidyr::separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID","SUBGROUP","DATE_ID"), sep = "_") %>%
  left_join(eci_stores_tbl, by = "STORE_ID") 

print(colnames(datos_tbl))#Voy viendo que todas las variables esten

#Observo la cantidad de filas

datos_tbl %>% tally()

#Obtengo el archivo .parquet de la base que usaremos para el objetivo 1

spark_write_parquet(
  datos_tbl,
  path = "Datos/datos_final.parquet",
  mode = "overwrite"  # o "append" si querés agregar
)

#Leo el archivo final y empiezo a hacer consultas

ds_final <- open_dataset("Datos/datos_final.parquet")

print(colnames(ds_final))#Voy viendo que todas las variables esten

# Conteo por subgrupo
conteo_subgrupo <- ds_final %>%
  group_by(SUBGROUP) %>%
  summarise(total_ventas = sum(TOTAL_SALES_, na.rm = TRUE),
            cantidad = sum(QUANTITY_, na.rm = TRUE)) %>%
  collect()


#Gráfico de barras

plot_ly(
  data = conteo_subgrupo,
  x = ~total_ventas,
  y = ~reorder(SUBGROUP, total_ventas),
  type = "bar",
  orientation = "h",
  marker = list(color = "steelblue")
) %>%
  layout(
    title = "Demanda total (en $) por Subgrupo",
    xaxis = list(title = "Demanda"),
    yaxis = list(title = "Subgrupo")
  )























































#Para una muestra

#Saco una muestra de transacciones

muestra <- eci_transactions %>%
  sdf_sample(fraction = 100000 / sdf_nrow(eci_transactions), replacement = FALSE) %>%
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




