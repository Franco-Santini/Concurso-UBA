library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)
library(tidyr)
library(sparkxgb)

# Configuraciones de spark
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
ids_test <- spark_read_csv(sc, name = "ids_test", path = "Prueba/ids_test.csv") |> collect()

# Join de las tablas (con los datos totales de transacciones)
eci_product_master_totales <- eci_product_master 
eci_product_master_totales <- eci_product_master_totales |> rename(SKU = sku, SUBGROUP = subgroup, brand_prod = brand)
df_product_master <- eci_product_master_totales |> collect()

# Imposible diferenciar los subgrupos de Baseball y Basketball, una alternativa es suponer que tienen demanda similar
# e imputar los resultados del basket con los del baseball
df_product_master |> 
  filter(SKU %in% c("SPOTEBA001", "SPOTEBA002", "SPOTEBA003", "SPOTEBA004", "SPOTEBA005", "SPOTEBA006", "SPOTEBA007")) |> 
  group_by(SUBGROUP) |> 
  summarise(base_price_avg = mean(base_price),
            initial_ticket_price_avg = mean(base_price),
            costos_avg = mean(costos))

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
  group_by(STORE_SUBGROUP_DATE_ID) |> 
  summarise(TOTAL_SALES_ = sum(TOTAL_SALES),
            QUANTITY_ = sum(QUANTITY),
            PRICE_ = sum(PRICE),
            base_price_ = sum(base_price)
            ) |> 
  ungroup() |>
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_")

datos_otra_forma <- datos_otra_forma |> 
  left_join(eci_stores_clusters_join, by = "STORE_ID") |>
  select(!c(ADDRESS1, ADDRESS2, CITY, STATE, ZIP, OPENDATE, CLOSEDATE, STORE_TYPE, CLUSTER)) |> 
  mutate(mes = month(as_date(DATE_ID)),
         dia = day(as_date(DATE_ID)))

prom_dia_mes <- datos_otra_forma |> 
  mutate(dia = day(as_date(DATE_ID))) |>
  filter(dia <= 7 & mes == 1) |> 
  group_by(dia, mes, STORE_ID, SUBGROUP) |> 
  summarise(PRICE_avg = mean(PRICE_),
            QUANTITY_avg = mean(QUANTITY_)) |> 
  collect()

# Division de la validacion de los datos pero agrupados por dia
# Definimos los 5 folds para la validacion cruzada
kfold_2 <- sdf_random_split(datos_otra_forma,
                            weights = purrr::set_names(rep(0.2, 5), paste0("fold", 1:5)),
                            seed = 115)

# Separacion entre train y test
train_set_2 <- do.call(rbind, kfold_2[2:5])
validation_set_2 <- kfold_2[[1]]

# # Entrenamiento de los modelos
# rf_model <- ml_random_forest_regressor(train_set_2, 
#                                        TOTAL_SALES_ ~ PRICE_ + base_price_ + QUANTITY_ + SUBGROUP + REGION + STORE_ID + mes +
#                                        SUBGROUP:PRICE_ + STORE_ID:PRICE_ + mes:PRICE_,
#                                        num_trees = 100, max_depth = 10)

reg_lin <- ml_linear_regression(train_set_2, 
                                TOTAL_SALES_ ~ PRICE_ + base_price_ + QUANTITY_ + SUBGROUP + REGION + STORE_ID + mes +
                                  SUBGROUP:PRICE_ + STORE_ID:PRICE_ + mes:PRICE_)

rf_model <- ml_random_forest_regressor(train_set_2,
                                       TOTAL_SALES_ ~ PRICE_ + QUANTITY_ + SUBGROUP + REGION + STORE_ID + mes + dia,
                                       num_trees = 300)

xgb_model <- xgboost_regressor(train_set_2,
                               TOTAL_SALES_ ~ PRICE_ + QUANTITY_ + SUBGROUP + REGION + STORE_ID + mes + dia,
                               eta = 0.3, max_depth = 5)

# Capacidad predictiva del modelo (Regresion lineal)
predicciones <- reg_lin |> 
  ml_predict(validation_set_2) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

predicciones_train <- reg_lin |> 
  ml_predict(train_set_2) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()


ml_regression_evaluator(predicciones, 
                        label_col = "TOTAL_SALES_", 
                        prediction_col = "prediction",
                        metric_name = "r2") # R^2: 0.8882013 conjunto de validacion

# Capacidad predictiva del modelo (Random Forest)
predicciones_rf <- rf_model |> 
  ml_predict(validation_set_2) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

predicciones_train_rf <- rf_model |> 
  ml_predict(train_set_2) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()


r2 <- ml_regression_evaluator(predicciones_rf, 
                              label_col = "TOTAL_SALES_", 
                              prediction_col = "prediction",
                              metric_name = "r2") # R^2: 0.8743325 conjunto de validacion (150 arboles)

mae <- ml_regression_evaluator(predicciones_rf, 
                               label_col = "TOTAL_SALES_", 
                               prediction_col = "prediction",
                               metric_name = "mape")

mape <- predicciones_rf |> 
  filter(TOTAL_SALES_ != 0) |>   # Avoid division by zero
  mutate(
    abs_pct_error = abs((TOTAL_SALES_ - prediction) / TOTAL_SALES_)
  ) |> 
  summarise(
    MAPE = mean(abs_pct_error)
  )


# Preparacion de los datos para el entrenamiento del modelo
datos_entrenamiento <- eci_transactions_stores_prod |> 
  # mutate(STORE_ID = factor(STORE_ID),
  #        SUBGROUP = factor(SUBGROUP),
  #        BRAND = factor(BRAND),
  #        STORE_TYPE = factor(STORE_TYPE),
  #        REGION = factor(REGION)) |> 
  select(c(STORE_ID, PRICE, TOTAL_SALES, SUBGROUP, BRAND, STORE_TYPE, REGION, mes, base_price))

# Definimos los 10 folds para la validacion cruzada
kfold <- sdf_random_split(datos_entrenamiento,
                          weights = purrr::set_names(rep(0.2, 5), paste0("fold", 1:5)),
                          seed = 115)

# Separacion entre train y test
train_set <- do.call(rbind, kfold[2:5])
validation_set <- kfold[[1]]

# Ajustamos xgboost a los datos
xgb_model <- xgboost_regressor(train_set, 
                               TOTAL_SALES ~ PRICE + base_price + SUBGROUP + REGION + BRAND + mes) # Parametro por defecto

reg_lineal <- ml_linear_regression(train_set, 
                                   TOTAL_SALES ~ PRICE + base_price + SUBGROUP + REGION + BRAND + mes)

rf_model <- ml_random_forest_regressor(train_set, 
                                       TOTAL_SALES ~ PRICE + base_price + SUBGROUP + REGION + STORE_ID + mes +
                                         SUBGROUP:PRICE + STORE_ID:PRICE,
                                       num_trees = 150, max_depth = 8)

predicciones <- reg_lineal |> 
  ml_predict(validation_set) |> 
  select(TOTAL_SALES, prediction, starts_with("probability_")) |> 
  glimpse()

predicciones_train <- reg_lineal |> 
  ml_predict(train_set) |> 
  select(TOTAL_SALES, prediction, starts_with("probability_")) |> 
  glimpse()

predicciones_rf <- rf_model |> 
  ml_predict(validation_set) |> 
  select(TOTAL_SALES, prediction, starts_with("probability_")) |> 
  glimpse()

# Capacidad predictiva del modelo
ml_regression_evaluator(predicciones_rf, 
                        label_col = "TOTAL_SALES", 
                        prediction_col = "prediction",
                        metric_name = "r2")





##########################################################

store_faltante <- eci_transactions_stores_prod |> 
  filter(!is.na(BRAND)) |> 
  select(STORE_ID, SUBGROUP) |> 
  mutate(st_sub = paste(STORE_ID, SUBGROUP, sep = "_")) |>
  distinct(st_sub) |> 
  collect()

store_faltante <- store_faltante |> 
  mutate(st_sub2 = st_sub) |> 
  separate(st_sub, into = c("STORE_ID", "SUBGROUP"), sep = "_") 

ids_test |> 
  distinct(STORE_SUBGROUP_DATE_ID) |> 
  summarise(n = n())


ids_test_sep |> 
  distinct(STORE_ID) |> 
  summarise(n = n())

ids_test_sep$st_sub <- paste(ids_test_sep$STORE_ID, ids_test_sep$SUBGROUP, sep = "_")

ids_test_sep |> 
  distinct(st_sub) |> 
  summarise(n = n())

ids_test_stores_join <- ids_test_sep |> 
  left_join(eci_stores |> collect(), by = "STORE_ID") |> 
  distinct(st_sub)

ids_test_stores_join <- ids_test_stores_join |> 
  mutate(st_sub2 = st_sub) |> 
  separate(st_sub, into = c("STORE_ID", "SUBGROUP"), sep = "_") 

x <- ids_test_stores_join |> 
  left_join(store_faltante, by = "st_sub2")

# Prueba de los modelos para subir a kaggle
ids_test_sep <- ids_test |> 
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_")

ids_test_sep <- ids_test_sep |> 
  left_join(eci_stores_clusters_join |> collect(), by = "STORE_ID") |> 
  select(!c(BRAND, STORE_NAME, ADDRESS1, ADDRESS2, CITY, STATE, ZIP, OPENDATE, CLOSEDATE, STORE_TYPE, CLUSTER)) |> 
  mutate(mes = month(as_date(DATE_ID)),
         dia = day(as_date(DATE_ID)))

ids_test_sep_bask <- ids_test_sep |> filter(SUBGROUP == "Basketball")
ids_test_sep_sin_bask <- ids_test_sep |> filter(SUBGROUP != "Basketball")

ids_test_sep_sin_bask <- ids_test_sep_sin_bask |> 
  left_join(prom_dia_mes, by = c("dia", "mes", "STORE_ID", "SUBGROUP")) |> 
  mutate(PRICE_avg = ifelse(is.na(PRICE_avg), 0, PRICE_avg),
         QUANTITY_avg = ifelse(is.na(QUANTITY_avg), 0, QUANTITY_avg))

