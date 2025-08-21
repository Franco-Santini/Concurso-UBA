library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)
library(tidyr)
library(fpp3)
library(zoo)
# library(tidymodels) 
# library(modeltime)
# library(timetk)

# Configuraciones de spark
config <- spark_config()
# Driver (R) -> Spark
config$spark.driver.memory <- "2g"
# config$spark.driver.memoryOverhead <- "512m"
config$spark.executor.instances <- 2     # Máximo 2 instancias en local
config$spark.executor.memory <- "2g"
# config$spark.executor.memoryOverhead <- "512m"
config$spark.executor.cores <- 2         # Aprovecha CPU sin saturar RAM

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
         año_mes = to_date(concat_ws("-", año, lpad(mes, 2, "0"), "01")),
         DATE_ID = as_date(DATE_ID))


# Preparamos los datos para aplicar series temporales
# La idea es separar cada serie temporal por tienda y subgrupo y para cada ella estimar el 
# precio mediano de enero x completo, se evita el precio promedio por que esta influenciado por precios muy caros o muy baratos

datos_series_semanal <- datos_otra_forma |> 
  mutate(semana = sql("weekofyear(DATE_ID)"),
         anio = sql("year(DATE_ID)")) |> 
  group_by(STORE_ID, SUBGROUP, semana, anio) |> 
  summarise(PRICE_ = dplyr::sql("percentile_approx(PRICE_, 0.5)")) |> 
  ungroup() |> 
  collect()

datos_series_semanal_ventas <- datos_otra_forma |>
  group_by(STORE_ID, SUBGROUP, DATE_ID) |> 
  summarise(TOTAL_SALES_ = sum(TOTAL_SALES_)) |> 
  ungroup() |> 
  collect()

datos_parte_1 <- datos_series_semanal_ventas |>
                      mutate(DATE_ID = as_date(DATE_ID)) |>
                      filter(DATE_ID <= "2021-12-31") |>
                      collect()

datos_parte_2 <- datos_series_semanal_ventas |>
  mutate(DATE_ID = as_date(DATE_ID)) |>
  filter(DATE_ID > "2021-12-31" & DATE_ID <= "2022-12-31")|>
  collect()

datos_parte_3 <- datos_series_semanal_ventas |>
  mutate(DATE_ID = as_date(DATE_ID)) |>
  filter(DATE_ID <= DATE_ID > "2022-12-31" & DATE_ID <= "2023-12-31") |>
  collect()

write.csv(rbind(datos_parte_1,datos_parte_2, datos_parte_3), "Datos/datos_series_diarios_ventas.csv")

# Guardamos los datos porque tarda mucho
# write.csv(datos_series_semanal, "Datos/datos_series_semanal.csv", row.names = FALSE, quote = FALSE)

# Lectura de los datos semanales
df_series_completo <- read.csv("Codigos/Datos/datos_series_semanal.csv")
df_series_completo <- df_series_completo |> 
  mutate(semanas_anio = paste(semana, anio, sep = "/"))

# Creamos el data frame de las series diarias
fechas_semanal <- seq(ymd("2021-01-01"), ymd("2023-12-31"), by = "week")
semanas_anio <- paste(week(fechas_semanal), year(fechas_semanal), sep = "/")

df <- expand.grid(STORE_ID = unique(df_series_completo$STORE_ID),
                  SUBGROUP = unique(df_series_completo$SUBGROUP),
                  semanas_anio = semanas_anio)

df_fechas <- data.frame(fecha = fechas_semanal,
           semanas_anio =  paste(week(fechas_semanal), year(fechas_semanal), sep = "/"))

# Precio mediano de la serie semana a semana
# datos_series_completo <- df |> 
#   left_join(df_series_completo, by = c("STORE_ID", "SUBGROUP", "semanas_anio")) |> 
#   select(-c(semana, anio)) |> 
#   mutate(Median_price = ifelse(is.na(PRICE_), 0, PRICE_))

datos_series_completo <- df |> 
  # unir con la serie de precios
  left_join(df_series_completo, by = c("STORE_ID", "SUBGROUP", "semanas_anio")) |> 
  select(-c(semana, anio)) |>    # quitar columnas que no necesitamos
  # separar semana y año para ordenar correctamente
  mutate(
    semana_num = as.numeric(sub("/.*", "", semanas_anio)),
    anio_num   = as.numeric(sub(".*/", "", semanas_anio)),
    semana_orden = anio_num * 100 + semana_num   # ej: 202101, 202102...
  ) |> 
  group_by(STORE_ID, SUBGROUP) |> 
  arrange(semana_orden) |> 
  # copiar la columna de precios original para trabajar
  mutate(Median_price = PRICE_) |> 
  # interpolación lineal de NA entre semanas
  mutate(Median_price = zoo::na.approx(Median_price, na.rm = FALSE)) |> 
  # rellenar NA al inicio y al final si no hay vecinos
  mutate(
    Median_price = ifelse(is.na(Median_price), zoo::na.locf(Median_price, na.rm = FALSE), Median_price),
    Median_price = ifelse(is.na(Median_price), zoo::na.locf(Median_price, fromLast = TRUE), Median_price)
  ) |> 
  ungroup() |> 
  select(-semana_num, -anio_num, -semana_orden)


datos_series_completo <- datos_series_completo |> 
  left_join(df_fechas, by = "semanas_anio")

#Guardo datos

#write.csv(datos_series_completo, "Datos/datos_series_semanal_nuevo.csv", row.names = FALSE, quote = FALSE)

# Lectura de los datos series semanales
datos_series_completo <- read.csv("Datos/datos_series_semanal_nuevo.csv")

# Datos de las series para trabajar con modelos SARIMA
datos_series_completo <- datos_series_completo |> 
  mutate(fecha = as_date(fecha)) |> 
  mutate(date = yearweek(fecha)) |> 
  dplyr::select(-c(PRICE_, fecha)) |> 
  as_tsibble(
    key = c(STORE_ID, SUBGROUP),
    index = date,
    validate = T,
    regular = T)

# Pipeline para ejecutar series de tiempo
# Definimos un dataframe que sea el que contenga los precios
library(doParallel)
num_cores <- parallel::detectCores() - 2
cl <- makePSOCKcluster(num_cores)
registerDoParallel(cl)

resultado_precio <- foreach(i = unique(datos_series_completo$STORE_ID), 
                            .combine = bind_rows,
                            .packages = c("fable", "fabletools", "dplyr")) %:%
  foreach(j = unique(datos_series_completo$SUBGROUP), 
          .combine = bind_rows,
          .packages = c("fable", "fabletools", "dplyr")) %dopar% {
            
            datos_filtro <- datos_series_completo |> 
              filter(STORE_ID == i, SUBGROUP == j)
            
            if (nrow(datos_filtro) > 5) { # evitar errores con series muy cortas
              modelo <- datos_filtro |> model(auto = ARIMA(Median_price))
              pred <- modelo |> forecast(h = 1) |> filter(.model == "auto")
              return(pred)
            } else {
              return(NULL)
            }
          }

stopCluster(cl)

# Guardamos las predicciones de los precios usando modelos SARIMA
guardar <- data.frame(STORE_ID = resultado_precio$STORE_ID,
                      SUBGROUP = resultado_precio$SUBGROUP,
                      PRICE_ = resultado_precio$.mean)

# write.csv(guardar, "precio_series_semanal_ultimo.csv", row.names = FALSE, quote = FALSE)

# Datos diarios
datos_series_diarios <- read.csv("Datos/datos_series_diarios.csv")
datos_series_diarios_ventas <- read.csv("Datos/datos_serie_diarios_ventas")
datos_series_diarios$DATE_ID <- as_date(datos_series_diarios$DATE_ID)

# DataFrame que contenga todas las combinaciones
fechas_dias <- seq(ymd("2021-01-01"), ymd("2023-12-31"), by = "day")
df <- expand.grid(STORE_ID = unique(datos_series_diarios$STORE_ID),
                  SUBGROUP = unique(datos_series_diarios$SUBGROUP),
                  DATE_ID = fechas_dias)

# Datos diarios completos
datos_series_diario_completo <- df |> 
  # unir con la serie de precios
  left_join(datos_series_diarios, by = c("STORE_ID", "SUBGROUP", "DATE_ID")) |> 
  # separar semana y año para ordenar correctamente
  mutate(
    dia_num = as.numeric(day(DATE_ID)),
    semana_num = as.numeric(week(DATE_ID)),
    anio_num   = as.numeric(year(DATE_ID)),
    orden = anio_num * 10000 + semana_num * 100 + dia_num
  ) |> 
  group_by(STORE_ID, SUBGROUP) |> 
  arrange(orden) |> 
  # copiar la columna de precios original para trabajar
  mutate(Median_price = PRICE_) |> 
  # interpolación lineal de NA entre semanas
  mutate(Median_price = zoo::na.approx(Median_price, na.rm = FALSE)) |> 
  # rellenar NA al inicio y al final si no hay vecinos
  mutate(
    Median_price = ifelse(is.na(Median_price), zoo::na.locf(Median_price, na.rm = FALSE), Median_price),
    Median_price = ifelse(is.na(Median_price), zoo::na.locf(Median_price, fromLast = TRUE), Median_price)
  ) |> 
  ungroup() |> 
  select(-semana_num, -anio_num, -orden, -dia_num)

# Guardo los datos diarios
# write.csv(datos_series_diario_completo, "Datos/precio_series_diarios_nuevo.csv", row.names = FALSE, quote = FALSE)

# Datos de las series para trabajar con modelos SARIMA
datos_series_diarios_completo <- datos_series_diario_completo |> 
  mutate(date = ymd(DATE_ID)) |> 
  dplyr::select(-c(PRICE_, DATE_ID)) |> 
  as_tsibble(
    key = c(STORE_ID, SUBGROUP),
    index = date,
    validate = T,
    regular = T)

# Pipeline para ejecutar series de tiempo
# Definimos un dataframe que sea el que contenga los precios
library(doParallel)
num_cores <- parallel::detectCores() - 2
cl <- makePSOCKcluster(num_cores)
registerDoParallel(cl)

resultado_precio_diario <- foreach(i = unique(datos_series_diarios_completo$STORE_ID), 
                            .combine = bind_rows,
                            .packages = c("fable", "fabletools", "dplyr")) %:%
  foreach(j = unique(datos_series_diarios_completo$SUBGROUP), 
          .combine = bind_rows,
          .packages = c("fable", "fabletools", "dplyr")) %dopar% {
            
            datos_filtro <- datos_series_diarios_completo |> 
              filter(STORE_ID == i, SUBGROUP == j)
            
            if (nrow(datos_filtro) > 5) { # evitar errores con series muy cortas
              modelo <- datos_filtro |> model(auto = ARIMA(Median_price))
              pred <- modelo |> forecast(h = 7) |> filter(.model == "auto")
              return(pred)
            } else {
              return(NULL)
            }
          }

stopCluster(cl)

# Guardamos las predicciones de los precios usando modelos SARIMA
guardar_diario <- data.frame(STORE_ID = resultado_precio_diario$STORE_ID,
                             SUBGROUP = resultado_precio_diario$SUBGROUP,
                             PRICE_ = resultado_precio_diario$.mean)

write.csv(guardar_diario, "precio_series_diario_ultimo.csv", row.names = FALSE, quote = FALSE)

###################### Optimizar el precio de la serie maximizando la ganancia


