###################### Librerias ######################
library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)
library(tidyr)
library(arrow)
library(sparkxgb)
library(ParBayesianOptimization)

###################### Configuaración de SPARK ######################
config <- spark_config()
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
ids_test <- spark_read_csv(sc, name = "ids_test", path = "Prueba/ids_test.csv") |> collect()

###################### Join de tablas ######################
eci_product_master_totales <- eci_product_master 
eci_product_master_totales <- eci_product_master_totales |> rename(SKU = sku, SUBGROUP = subgroup, brand_prod = brand)
df_product_master <- eci_product_master_totales |> collect()

# Grupo de productos con maestro de productos
product_join <- eci_product_master |> 
  left_join(eci_product_groups, by = c("sku", "product_name")) |> 
  collect()

# Imposible diferenciar los subgrupos de Baseball y Basketball, una alternativa es suponer que tienen demanda similar
# e imputar los resultados del basket con los del baseball

df_product_master |> 
  filter(SKU %in% c("SPOTEBA001", "SPOTEBA002", "SPOTEBA003", "SPOTEBA004", "SPOTEBA005", "SPOTEBA006", "SPOTEBA007", "SPOTEBA008")) |> 
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
  select(!c(ADDRESS1, ADDRESS2, STATE, ZIP, OPENDATE, CLOSEDATE, CLUSTER)) |> 
  mutate(mes = month(as_date(DATE_ID)),
         dia = day(as_date(DATE_ID)),
         anio = year(as_date(DATE_ID)),
         STORE_TYPE = if_else(is.na(STORE_TYPE), "Unknown", STORE_TYPE))

# Obtenemos la media y el desvio de PRICE_
price_media_sd <- datos_otra_forma |> 
  summarise(
    price_mean = mean(PRICE_),
    price_sd = sd(PRICE_)
  ) |> 
  collect()

prom_dia_mes <- datos_otra_forma |> 
  mutate(dia = day(as_date(DATE_ID))) |>
  filter(dia <= 7 & mes == 1) |> 
  group_by(dia, mes, STORE_ID, SUBGROUP) |> 
  summarise(PRICE_avg = mean(PRICE_),
            QUANTITY_avg = mean(QUANTITY_)) |> 
  collect()

###################### Optimización de hiperparámetros ######################

# Definimos la división en datos de entrenamiento y testeo (85 - 15)
datos_entrenamiento <- sdf_random_split(datos_otra_forma, train = 0.85, test = 0.15, seed = 151213)

# Dentro de los datos de entrenamiento (train_df) hacemos la optimización de hiperparámetros del modelo
set.seed(202123) # Garantizar la reproducibilidad de los resultados

# Crear columna fold (del 1 al 4)
datos_cv <- datos_entrenamiento$train |> 
  mutate(fold = floor(rand() * 4) + 1)

scoring_function <- function(num_trees, min_instances) { 
  r2_scores <- c() 
  for (k in 1:4) { # Split en train/valid según fold 80% entrenamiento - 20% validacion 
    train_fold <- datos_cv %>% filter(fold != k) 
    valid_fold <- datos_cv %>% filter(fold == k) 
    
    # Entrenar RF 
    rf_model <- ml_random_forest_regressor( 
      train_fold, 
      TOTAL_SALES_ ~ PRICE_ + SUBGROUP + STORE_TYPE + REGION + STORE_ID + mes + dia + anio + category + group + BRAND, 
      num_trees = as.integer(num_trees), 
      max_depth = 5, 
      max_bins = 40, 
      min_instances_per_node = as.integer(min_instances), 
      subsampling_rate = 0.8, 
      feature_subset_strategy = "onethird")
    
    #Predicciones en validación 
    pred <- ml_predict(rf_model, valid_fold) 
    
    # Evaluar R2 
    r2 <- ml_regression_evaluator( 
      pred, 
      label_col = "TOTAL_SALES_", 
      prediction_col = "prediction", 
      metric_name = "r2" 
    ) 
    
    r2_scores <- c(r2_scores, r2) 
    
  } 
  
  # Promedio de los 4 folds 
  mean_r2 <- mean(r2_scores) 
  sd_r2 <- sd(r2_scores) 
  
  # ParBayesianOptimization espera lista con Score 
  list(Score = mean_r2, Score_sd = sd_r2, Pred = 0) 
  
} 

# Optimizacion 
bounds <- list(
  num_trees = c(70L, 225L), 
  min_instances = c(5L, 10L) 
) 

opt_res <- bayesOpt( 
  FUN = scoring_function, 
  bounds = bounds, 
  initPoints = 3, # puntos iniciales (exploración) 
  iters.n = 6 # iteraciones de optimización
)

# Obtencion de mejor parámetro
params <- getBestPars(opt_res)

# Resumen de los resultados
resumen <- opt_res$scoreSummary

###################### Entrenamiento de Random Forest con la totalidad de los datos ######################
rf_model_def <- ml_random_forest_regressor( 
  datos_otra_forma, 
  TOTAL_SALES_ ~ PRICE_ + SUBGROUP + STORE_TYPE + REGION + STORE_ID + mes + dia + anio + category + group + BRAND, 
  num_trees = as.integer(params$num_trees), 
  max_depth = 5, 
  max_bins = 40, 
  min_instances_per_node = as.integer(params$min_instances), 
  subsampling_rate = 0.8, 
  seed = 250,
  feature_subset_strategy = "onethird")

# Predicciones usando la totalidad de los datos
predicciones_rf <- rf_model_def |> 
  ml_predict(datos_otra_forma) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

# R2 del modelo
r2 <- ml_regression_evaluator(predicciones_rf, 
                              label_col = "TOTAL_SALES_", 
                              prediction_col = "prediction",
                              metric_name = "r2")

# Capacidad predictiva de Random Forest (Datos de prueba)
predicciones_rf_test <- rf_model_def |> 
  ml_predict(datos_entrenamiento$test) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

# R2 del modelo (Datos de prueba)
r2_test <- ml_regression_evaluator(predicciones_rf_test, 
                                   label_col = "TOTAL_SALES_", 
                                   prediction_col = "prediction",
                                   metric_name = "r2")

###################### Entrenamiento de GBM con la totalidad de los datos ######################
gbmodel_demanda <- ml_gradient_boosted_trees(datos_otra_forma,
                                             type = "regression",
                                             response = "TOTAL_SALES_",
                                             features = c("PRICE_", "STORE_ID", "REGION", "SUBGROUP", "mes", "anio", "category", "group", 
                                                          "BRAND", "STORE_TYPE", "dia"),
                                             max_iter = 20, seed = 300, step_size = 0.1)

# Predicciones usando la totalidad de los datos (Datos de entrenamiento)
predicciones_gbm <- gbmodel_demanda |> 
  ml_predict(datos_otra_forma) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

# R2 del modelo (datos de entrenamiento)
r2_gbm <- ml_regression_evaluator(predicciones_gbm, 
                                  label_col = "TOTAL_SALES_", 
                                  prediction_col = "prediction",
                                  metric_name = "r2")

# Capacidad predictiva de Gradient Boosting (Datos de prueba)
predicciones_gbm_test <- gbmodel_demanda |> 
  ml_predict(datos_entrenamiento$test) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()

# R2 del modelo (Datos de prueba)
r2_test <- ml_regression_evaluator(predicciones_gbm_test, 
                                   label_col = "TOTAL_SALES_", 
                                   prediction_col = "prediction",
                                   metric_name = "r2")

###################### Predicciones para subir a KAGGLE, serie precios semanal ######################
ids_test_sep <- ids_test |> 
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_") |> 
  mutate(DATE_ID = as_date(DATE_ID))

ids_test_sep <- ids_test_sep |> 
  left_join(eci_stores_clusters_join |> collect(), by = "STORE_ID") |> 
  select(!c(STORE_NAME, ADDRESS1, ADDRESS2, CITY, STATE, ZIP, OPENDATE, CLOSEDATE, STORE_TYPE, CLUSTER)) |> 
  mutate(mes = lubridate::month(DATE_ID),
         dia = lubridate::day(DATE_ID))

precio_promedio <- datos_otra_forma |> 
  mutate(DATE_ID = as_date(DATE_ID)) |> 
  filter(DATE_ID >= "2023-12-01") |> 
  group_by(SUBGROUP, STORE_ID, category, group) |> 
  summarise(PRICE_ = mean(PRICE_)) |> 
  ungroup() |> 
  collect()

precio_mediano <- datos_otra_forma |> 
  mutate(DATE_ID = as_date(DATE_ID)) |> 
  filter(DATE_ID >= "2023-12-01") |> 
  group_by(SUBGROUP, STORE_ID, category, group, STORE_TYPE) |> 
  summarise(PRICE_ = dplyr::sql("percentile_approx(PRICE_, 0.5)")) |> 
  ungroup() |> 
  collect()

# Lectura de los datos de los precios predichos
precio_series <- read.csv("Datos/precio_series_semanal_ultimo.csv")
  
precio_mediano <- precio_mediano |> 
  left_join(precio_series, by = c("STORE_ID", "SUBGROUP")) |> 
  rename(PRICE_ = PRICE_.y)

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball") |> 
  left_join(precio_mediano, by = c("STORE_ID", "SUBGROUP")) |> 
  mutate(dia = lubridate::day(DATE_ID),
         anio = lubridate::year(DATE_ID))

ids_test_sep_mismo_precio_bask <- ids_test_sep |> 
  filter(SUBGROUP == "Basketball") |> 
  left_join((precio_mediano |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x) |> 
  mutate(PRICE_ = PRICE_) |> 
  mutate(dia = lubridate::day(DATE_ID),
         anio = lubridate::year(DATE_ID))

ids_test_sep_mismo_precio_base <- ids_test_sep |> 
  filter(SUBGROUP == "Baseball") |> 
  left_join((precio_mediano |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x) |> 
  mutate(PRICE_ = PRICE_) |> 
  mutate(dia = lubridate::day(DATE_ID),
         anio = lubridate::year(DATE_ID))

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep_mismo_precio_sin_bask_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, mes.x, dia, PRICE_, PRICE_.x, QUANTITY_avg, category, group, BRAND, STORE_TYPE, anio) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |>
  rename(mes = mes.x)

ids_test_sep_mismo_precio_base <- ids_test_sep_mismo_precio_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, mes.x, dia, PRICE_, PRICE_.x, QUANTITY_avg, category, group, BRAND, STORE_TYPE, anio) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |>
  rename(mes = mes.x)

ids_test_sep_mismo_precio <- rbind(ids_test_sep_mismo_precio_sin_bask_base, ids_test_sep_mismo_precio_base)

ids_predicciones_sin_bask <- sparklyr::copy_to(sc, ids_test_sep_mismo_precio, overwrite = TRUE)

predicciones_ids_sin_bask <- gbmodel_demanda |> # Si usas Random Forest, cambiar gbmodel_demanda por rf_model_def
  ml_predict(ids_predicciones_sin_bask) |> 
  select(STORE_SUBGROUP_DATE_ID, SUBGROUP, STORE_ID, prediction, QUANTITY_, dia, starts_with("probability_")) |> 
  rename(TOTAL_SALES = prediction) |> 
  collect()

predicciones_ids_baseball <- predicciones_ids_sin_bask |> 
  filter(SUBGROUP == "Baseball") |> 
  mutate(TOTAL_SALES = TOTAL_SALES * 0.5)

predicciones_ids_bask <- ids_test_sep_mismo_precio_bask |> 
  left_join(predicciones_ids_baseball, by = c("STORE_ID", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID.x, STORE_ID, SUBGROUP.x, QUANTITY_, dia, TOTAL_SALES) |> 
  rename(STORE_SUBGROUP_DATE_ID = STORE_SUBGROUP_DATE_ID.x, SUBGROUP = SUBGROUP.x)

predicciones_ids_sin_bask_base <- predicciones_ids_sin_bask |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball")

predicciones_ids_completa <- rbind(predicciones_ids_sin_bask_base, predicciones_ids_baseball, predicciones_ids_bask)
predicciones_ids_completa <- predicciones_ids_completa |> 
  select(STORE_SUBGROUP_DATE_ID, TOTAL_SALES) |>
  mutate(TOTAL_SALES = TOTAL_SALES)

predicciones_finales <- ids_test |> left_join(predicciones_ids_completa, by = "STORE_SUBGROUP_DATE_ID")

### Guardamos las predicciones en un csv
# write.csv(predicciones_finales, "Predicciones/predicciones_finales_v32.csv", row.names = FALSE, quote = FALSE)

###################### Predicciones para subir a KAGGLE, serie precios diaria ######################

# Lectura de los datos de forma diaria
precio_series_diario <- read.csv("Datos/precio_series_diario_ultimo.csv")
fechas_dias <- seq(ymd("2024-01-01"), ymd("2024-01-07"), by = "day")
precio_series_diario$DATE_ID <- as_date(fechas_dias)

ids_test_sep <- ids_test |> 
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_") |> 
  mutate(DATE_ID = as_date(DATE_ID))

ids_test_sep <- ids_test_sep |> 
  left_join(eci_stores_clusters_join |> collect(), by = "STORE_ID") |> 
  select(!c(STORE_NAME, ADDRESS1, ADDRESS2, CITY, STATE, ZIP, OPENDATE, CLOSEDATE, CLUSTER)) |> 
  mutate(mes = lubridate::month(DATE_ID),
         dia = lubridate::day(DATE_ID))

precio_mediano <- datos_otra_forma |> 
  mutate(DATE_ID = as_date(DATE_ID)) |> 
  filter(DATE_ID >= "2023-12-01") |> 
  group_by(SUBGROUP, STORE_ID, category, group, STORE_TYPE) |> 
  summarise(PRICE_ = dplyr::sql("percentile_approx(PRICE_, 0.5)")) |> 
  ungroup() |> 
  collect()

precio_series_diario_join <- precio_series_diario |>
  left_join(precio_mediano |> select(-PRICE_), by = c("STORE_ID", "SUBGROUP"))

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball") |> 
  left_join(precio_series_diario_join, by = c("STORE_ID", "SUBGROUP", "DATE_ID")) |> 
  mutate(dia = lubridate::day(DATE_ID),
         anio = lubridate::year(DATE_ID))

ids_test_sep_mismo_precio_bask <- ids_test_sep |> 
  filter(SUBGROUP == "Basketball")

ids_test_sep_mismo_precio_base <- ids_test_sep |> 
  filter(SUBGROUP == "Baseball") |> 
  left_join((precio_series_diario_join |> filter(SUBGROUP == "Baseball")),  by = c("STORE_ID", "SUBGROUP", "DATE_ID")) |> 
  mutate(PRICE_ = PRICE_) |> 
  mutate(dia = lubridate::day(DATE_ID),
         anio = lubridate::year(DATE_ID))

ids_test_sep_mismo_precio_bask <- ids_test_sep_mismo_precio_bask |> 
  left_join(ids_test_sep_mismo_precio_base |> select(STORE_ID, DATE_ID, PRICE_, anio), by = c("STORE_ID", "DATE_ID"))

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep_mismo_precio_sin_bask_base |> 
  left_join(prom_dia_mes |> select(STORE_ID, SUBGROUP, dia, QUANTITY_avg), by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, STORE_TYPE.y, mes, dia, PRICE_, QUANTITY_avg, category, group, BRAND, anio) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |> 
  rename(STORE_TYPE = STORE_TYPE.y)

ids_test_sep_mismo_precio_base <- ids_test_sep_mismo_precio_base |> 
  left_join(prom_dia_mes |> select(STORE_ID, SUBGROUP, dia, QUANTITY_avg), by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, STORE_TYPE.y, mes, dia, PRICE_, QUANTITY_avg, category, group, BRAND, anio) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |> 
  rename(STORE_TYPE = STORE_TYPE.y)

ids_test_sep_mismo_precio <- rbind(ids_test_sep_mismo_precio_sin_bask_base, ids_test_sep_mismo_precio_base)

ids_predicciones_sin_bask <- sparklyr::copy_to(sc, ids_test_sep_mismo_precio, overwrite = TRUE)

predicciones_ids_sin_bask <- rf_model_def |> 
  ml_predict(ids_predicciones_sin_bask) |> 
  select(STORE_SUBGROUP_DATE_ID, SUBGROUP, STORE_ID, prediction, QUANTITY_, dia, starts_with("probability_")) |> 
  rename(TOTAL_SALES = prediction) |> 
  collect()

predicciones_ids_baseball <- predicciones_ids_sin_bask |> 
  filter(SUBGROUP == "Baseball") |> 
  mutate(TOTAL_SALES = TOTAL_SALES * 0.5)

predicciones_ids_bask <- ids_test_sep_mismo_precio_bask |> 
  left_join(predicciones_ids_baseball, by = c("STORE_ID", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID.x, STORE_ID, SUBGROUP.x, QUANTITY_, dia, TOTAL_SALES) |> 
  rename(STORE_SUBGROUP_DATE_ID = STORE_SUBGROUP_DATE_ID.x, SUBGROUP = SUBGROUP.x)

predicciones_ids_sin_bask_base <- predicciones_ids_sin_bask |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball")

predicciones_ids_completa <- rbind(predicciones_ids_sin_bask_base, predicciones_ids_baseball, predicciones_ids_bask)
predicciones_ids_completa <- predicciones_ids_completa |> 
  select(STORE_SUBGROUP_DATE_ID, TOTAL_SALES) |>
  mutate(TOTAL_SALES = TOTAL_SALES)

predicciones_finales <- ids_test |> left_join(predicciones_ids_completa, by = "STORE_SUBGROUP_DATE_ID")

### Guardamos las predicciones en un csv
# write.csv(predicciones_finales, "Predicciones/predicciones_finales_v26.csv", row.names = FALSE, quote = FALSE)