library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)
library(tidyr)
library(fpp3) # Modelos de series de tiempo

# Configuraciones de spark
config <- spark_config()
# Driver (R) -> Spark
config$spark.driver.memory <- "2g"
config$spark.driver.memoryOverhead <- "512m"
config$spark.executor.instances <- 2     # Máximo 2 instancias en local
config$spark.executor.memory <- "2g"
config$spark.executor.memoryOverhead <- "512m"
config$spark.executor.cores <- 2         # Aprovecha CPU sin saturar RAM
config$spark.sql.shuffle.partitions <- 50

# Establecemos la coneccion con spark
sc <- spark_connect(master = "local", config = config)

# Copiamos el formato de las tablas para no tener errores
customer_df <- read.csv("Datos/eci_customer_data.csv", nrows = 6)
customer_spec <- sapply(customer_df, class)
transactions_df <- read.csv("Datos/eci_transactions.csv", nrows = 6)
transactions_spec <- sapply(transactions_df, class)
product_groups_df <- read.csv("Datos/eci_product_groups.csv", nrows = 6)
product_groups_spec <- sapply(product_groups_df, class)
product_master_df <- read.csv("Datos/eci_product_master.csv", nrows = 6)
product_master_spec <- sapply(product_master_df, class)
stores_df <- read.csv("Datos/eci_stores.csv", nrows = 6)
stores_spec <- sapply(stores_df, class)
stores_cluster_df <- read.csv("Datos/eci_stores_clusters.csv", nrows = 6)
stores_cluster_spec <- sapply(stores_cluster_df, class)

# Leemos un dataframe de un csv
eci_customer <- spark_read_csv(sc, name = "eci_clientes", path = "Datos/eci_customer_data.csv", columns = customer_spec)
eci_transactions <- spark_read_csv(sc, name = "eci_transacciones", path = "Datos/eci_transactions.csv", columns = transactions_spec)
eci_product_groups <- spark_read_csv(sc, name = "eci_grupo_productos", path = "Datos/eci_product_groups.csv", columns = product_groups_spec)
eci_product_master <- spark_read_csv(sc, name = "eci_maestros_productos", path = "Datos/eci_product_master.csv", columns = product_master_spec)
eci_stores <- spark_read_csv(sc, name = "eci_tiendas", path = "Datos/eci_stores.csv", columns = stores_spec)
eci_stores_clusters <- spark_read_csv(sc, name = "eci_tiendas_cluster", path = "Datos/eci_stores_clusters.csv", columns = stores_cluster_spec)
eci_stores_totales <- eci_stores |> collect() |> mutate(CLOSEDATE = as_date(CLOSEDATE)) |> filter(CLOSEDATE > "2023-12-31" | is.na(CLOSEDATE)) |> filter(!is.na(STORE_TYPE))
eci_stores_clusters_totales <- eci_stores_clusters |> collect()
eci_product_master_totales <- eci_product_master |> collect() 

# Join de las tablas (con los datos totales de transacciones)
eci_product_master_totales <- eci_product_master 
eci_product_master_totales <- eci_product_master_totales |> rename(SKU = sku, SUBGROUP = subgroup, brand_prod = brand)
df_product_master <- eci_product_master_totales |> collect()

# Imposible diferenciar los subgrupos de Baseball y Basketball, una alternativa es suponer que tienen demanda similar
# e imputar los resultados del basket con los del baseball

eci_stores_totales <- eci_stores |> mutate(CLOSEDATE = as_date(CLOSEDATE)) |> filter(CLOSEDATE > "2023-12-31" | is.na(CLOSEDATE))
eci_stores_clusters_totales <- eci_stores_clusters
eci_stores_clusters_join <- eci_stores_totales |> 
  left_join(eci_stores_clusters_totales, by = "STORE_ID") |> 
  select(-c(BRAND_y, STORE_NAME_y)) |> 
  rename(BRAND = BRAND_x, STORE_NAME = STORE_NAME_x)

eci_transactions_stores <- eci_transactions |> 
  left_join(eci_stores_clusters_join, by = "STORE_ID") 

eci_transactions_stores <- eci_transactions_stores |>
  mutate(DATE = as_date(DATE),
         mes = month(DATE),
         año = year(DATE)) |> 
  mutate(año_mes = to_date(concat_ws("-", año, lpad(mes, 2, "0"), "01")))

eci_transactions_stores_prod <- eci_transactions_stores |> 
  left_join(eci_product_master_totales, by = c("SKU", "SUBGROUP"))


eci_transactions_stores_prod <- eci_transactions_stores_prod |> 
  mutate(
    margen = TOTAL_SALES - (TOTAL_SALES/PRICE)*costos,
    ganancia = (PRICE - costos) * TOTAL_SALES
  ) |> 
  filter(!is.na(BRAND))

# Otra forma de modelar los datos, no por transaccion individual, sino agrupada por tienda, subgrupo y dia
datos_otra_forma <- eci_transactions_stores_prod |> 
  mutate(QUANTITY = TOTAL_SALES/PRICE) |> 
  group_by(STORE_SUBGROUP_DATE_ID, category, group) |> 
  summarise(TOTAL_SALES_ = sum(TOTAL_SALES),
            QUANTITY_ = sum(QUANTITY),
            PRICE_ = sum(PRICE),
            base_price_ = sum(base_price),
            costos_ = sum(costos), 
            initial_ticket_price_ = sum(initial_ticket_price)
  ) |> 
  ungroup() |>
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_")

datos_otra_forma <- datos_otra_forma |> 
  left_join(eci_stores_clusters_join, by = "STORE_ID") |>
  select(!c(ADDRESS1, ADDRESS2, STATE, ZIP, OPENDATE, CLOSEDATE, STORE_TYPE, CLUSTER)) |> 
  mutate(mes = month(as_date(DATE_ID)),
         dia = day(as_date(DATE_ID)),
         año = year(as_date(DATE_ID)),
         año_mes = to_date(concat_ws("-", año, lpad(mes, 2, "0"), "01")))

# Preparamos los datos para aplicar series temporales
# La idea es separar cada serie temporal por tienda y subgrupo y para cada ella estimar el 
# precio mediano de enero x completo, se evita el precio promedio por que esta influenciado por precios muy caros o muy baratos

datos_series <- datos_otra_forma |> 
  group_by(STORE_ID, SUBGROUP, año_mes) |> 
  summarise(Median_price = dplyr::sql("percentile_approx(PRICE_, 0.5)")) |> 
  ungroup() |> 
  collect()

# Guardo los datos de las series, porque tarda mucho el collect
# write.csv(datos_series, "Datos/datos_series.csv", row.names = FALSE, quote = FALSE)

fechas_meses <- seq(ymd("2021-01-01"), ymd("2023-12-01"), by = "month")

df <- expand.grid(STORE_ID = unique(datos_series$STORE_ID),
                  SUBGROUP = unique(datos_series$SUBGROUP),
                  año_mes = fechas_meses)

datos_series_completo <- df |> 
  left_join(datos_series, by = c("STORE_ID", "SUBGROUP", "año_mes")) |> 
  mutate(Median_price = ifelse(is.na(Median_price), 0, Median_price))

datos_series_completo <- datos_series_completo |> 
  mutate(mes = as.numeric(month(año_mes)),
         date = yearmonth(año_mes)) |> 
  dplyr::select(-año_mes) |> 
  as_tsibble(
    key = c(STORE_ID, SUBGROUP),
    index = date,
    validate = T,
    regular = T)

# Pipeline para ejecutar series de tiempo
# Definimos un dataframe que sea el que contenga los precios
resultado_precio <- data.frame(STORE_ID = character(),
                               SUBGROUP = character(),
                               .model = character(),
                               date = double(),
                               Median_price = list(),
                               .mean = double())
contador <- 0
for(i in unique(datos_series_completo$STORE_ID)) {
  for(j in unique(datos_series_completo$SUBGROUP)) {
    contador <- contador + 1
    
    # Entrenamiento del modelo
    modelo <- datos_series_completo |>
      filter(STORE_ID == i & SUBGROUP == j) |>
      model(auto = ARIMA(Median_price))
    
    # Predicción del modelo
    resultado_precio <- rbind(resultado_precio, modelo |> forecast(h=1) |> filter(.model == "auto"))
    
    # Para ver en que serie va metemos un cat
    cat("Se ajustó la serie N°", contador, "\n")
  }
}

# Guardamos las predicciones de los precios usando modelos SARIMA
guardar <- data.frame(STORE_ID = resultado_precio$STORE_ID,
                      SUBGROUP = resultado_precio$SUBGROUP,
                      PRICE_ = resultado_precio$.mean)

write.csv(guardar, "Predicciones/precio_series.csv", row.names = FALSE, quote = FALSE)
