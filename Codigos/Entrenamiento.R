library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)
library(tidyr)
library(arrow)
library(sparkxgb)
library(ParBayesianOptimization) # Optimización de hiperparámetros tipo OPTUNA


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
ids_test <- spark_read_csv(sc, name = "ids_test", path = "Prueba/ids_test.csv") |> collect()

# Join de las tablas (con los datos totales de transacciones)
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

# Division de la validacion de los datos pero agrupados por dia
# Definimos la división en datos de entrenamiento y testeo (80 - 20)
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

# Entrenamiento del modelo con todos los datos y los mejores hiperparámetros
rf_model_def <- ml_random_forest_regressor( 
  datos_otra_forma, 
  TOTAL_SALES_ ~ PRICE_ + SUBGROUP + STORE_TYPE + REGION + STORE_ID + mes + dia + anio + category + group + BRAND, 
  num_trees = as.integer(params$num_trees), 
  max_depth = 5, 
  max_bins = 40, 
  min_instances_per_node = as.integer(params$min_instances), 
  subsampling_rate = 0.8, 
  feature_subset_strategy = "onethird")


#################

cant_stores_subg <- datos_otra_forma |> 
  group_by(STORE_ID, SUBGROUP) |> 
  summarise(quant = sum(QUANTITY_)) |> 
  collect()

# # Entrenamiento de los modelos
# rf_model <- ml_random_forest_regressor(train_set_2, 
#                                        TOTAL_SALES_ ~ PRICE_ + base_price_ + QUANTITY_ + SUBGROUP + REGION + STORE_ID + mes +
#                                        SUBGROUP:PRICE_ + STORE_ID:PRICE_ + mes:PRICE_,
#                                        num_trees = 100, max_depth = 10)

rf_model <- ml_random_forest_regressor(train_set_2,
                                       TOTAL_SALES_ ~ PRICE_ + SUBGROUP + STORE_TYPE + REGION + STORE_ID + mes + dia + category + group + BRAND,
                                       num_trees = 120, max_depth = 5, max_bins = 48, min_instances_per_node = 5, 
                                       seed = 200, subsampling_rate = 0.8, feature_subset_strategy = "onethird")

rf_model_definitivo <- ml_random_forest_regressor(datos_otra_forma,
                                                  TOTAL_SALES_ ~ PRICE_ + SUBGROUP + REGION + STORE_ID + mes + dia + category + group,
                                                  num_trees = 120, max_depth = 5, max_bins = 48, min_instances_per_node = 5, 
                                                  seed = 200, subsampling_rate = 0.8, feature_subset_strategy = "onethird")


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

predicciones_rf_def_train <- rf_model_definitivo |> 
  ml_predict(datos_otra_forma) |> 
  select(TOTAL_SALES_, prediction, starts_with("probability_")) |> 
  glimpse()


r2 <- ml_regression_evaluator(predicciones_rf, 
                              label_col = "TOTAL_SALES_", 
                              prediction_col = "prediction",
                              metric_name = "r2") # R^2: 0.8743325 conjunto de validacion (150 arboles)

# Tuneo de hiperparámetros de random forest (hacer un pipeline que te permita tunear hiperparámetros)


# Preparacion de los datos para el entrenamiento del modelo
datos_entrenamiento <- eci_transactions_stores_prod |> 
  mutate(mes = as.character(mes),
         dia = day(DATE),
         QUANTITY = TOTAL_SALES/PRICE) |> 
  select(c(STORE_ID, PRICE, TOTAL_SALES, QUANTITY, SUBGROUP, BRAND, STORE_TYPE, REGION, mes, dia, costos, initial_ticket_price))

# Definimos los 10 folds para la validacion cruzada
kfold <- sdf_random_split(datos_entrenamiento,
                          weights = purrr::set_names(rep(0.2, 5), paste0("fold", 1:5)),
                          seed = 115)

# Separacion entre train y test
train_set <- do.call(rbind, kfold[2:5])

validation_set <- kfold[[1]]

# Ajustamos xgboost a los datos
xgb_model <- xgboost_regressor(train_set, 
                               TOTAL_SALES ~ PRICE + SUBGROUP + REGION + BRAND + mes + dia,
                               num_workers = 4) # Parametro por defecto

reg_lineal <- ml_linear_regression(train_set, 
                                   TOTAL_SALES ~ PRICE + SUBGROUP + STORE_ID + REGION + BRAND + mes)

rf_model <- ml_random_forest_regressor(train_set, 
                                       TOTAL_SALES ~ PRICE + SUBGROUP + REGION + STORE_ID + costos + initial_ticket_price + mes,
                                       num_trees = 100, max_depth = 7, max_bins = 32, min_instances_per_node = 5, 
                                       seed = 200, subsampling_rate = 0.8, feature_subset_strategy = "onethird")

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


ids_test_sep_bask <- ids_test_sep |> filter(SUBGROUP == "Basketball")
ids_test_sep_sin_bask <- ids_test_sep |> filter(SUBGROUP != "Basketball")

# ids_test_sep_sin_bask <- ids_test_sep_sin_bask |> 
#   left_join(prom_dia_mes, by = c("dia", "mes", "STORE_ID", "SUBGROUP")) |> 
#   mutate(PRICE_avg = ifelse(is.na(PRICE_avg), 0, PRICE_avg),
#          QUANTITY_avg = ifelse(is.na(QUANTITY_avg), 0, QUANTITY_avg))

# Prueba de predicciones
ids_test_sep_sin_bask <- ids_test_sep_sin_bask |> 
  rename(PRICE_ = PRICE_avg, QUANTITY_ = QUANTITY_avg)
ids_test_sep_sin_bask <- sparklyr::copy_to(sc, ids_test_sep_sin_bask, overwrite = T)

predicciones_finales <- rf_model |> 
  ml_predict(ids_test_sep_sin_bask) |> 
  select(prediction, starts_with("probability_")) |> 
  collect()

ids_test_sep_sin_bask <- ids_test_sep_sin_bask |> 
  collect() |> 
  mutate(TOTAL_SALES_ = predicciones_finales$prediction)

basket <- ids_test_sep_bask |> 
  left_join(ids_test_sep_sin_bask |> 
              filter(SUBGROUP == "Baseball"), by = c("STORE_ID", "DATE_ID")) |> 
  select(STORE_SUBGROUP_DATE_ID.x, TOTAL_SALES_) |>
  rename(STORE_SUBGROUP_DATE_ID = STORE_SUBGROUP_DATE_ID.x,
         TOTAL_SALES = TOTAL_SALES_) |> 
  mutate(TOTAL_SALES = TOTAL_SALES * 0.5)

baseball <- ids_test_sep_sin_bask |> 
  filter(SUBGROUP == "Baseball") |> 
  mutate(TOTAL_SALES = TOTAL_SALES_ * 0.5) |> 
  select(STORE_SUBGROUP_DATE_ID, TOTAL_SALES)

ids_sin_bask_base <- ids_test_sep_sin_bask |> 
  filter(SUBGROUP != "Baseball") |> 
  filter(SUBGROUP != "Basketball") |> 
  rename(TOTAL_SALES = TOTAL_SALES_) |> 
  select(STORE_SUBGROUP_DATE_ID, TOTAL_SALES)

ids_test_prueba <- rbind(ids_sin_bask_base, basket, baseball)


predicciones_finales <- ids_test |> 
  left_join(ids_test_prueba, by = "STORE_SUBGROUP_DATE_ID") |> 
  mutate(TOTAL_SALES = round(TOTAL_SALES))

############# Predicciones del modelo, manteniendo el mismo comportamiento en el precio que el 2023
ids_test_sep <- ids_test |> 
  mutate(STORE_SUBGROUP_DATE_ID_2 = STORE_SUBGROUP_DATE_ID) |> 
  separate(STORE_SUBGROUP_DATE_ID_2, into = c("STORE_ID", "SUBGROUP", "DATE_ID"), sep = "_") |> 
  mutate(DATE_ID = as_date(DATE_ID))

ids_test_sep <- ids_test_sep |> 
  left_join(eci_stores_clusters_join |> collect(), by = "STORE_ID") |> 
  select(!c(BRAND, STORE_NAME, ADDRESS1, ADDRESS2, CITY, STATE, ZIP, OPENDATE, CLOSEDATE, STORE_TYPE, CLUSTER)) |> 
  mutate(mes = lubridate::month(DATE_ID),
         dia = lubridate::day(DATE_ID))

ultimo_precio <- datos_otra_forma |> 
  mutate(DATE_ID = as_date(DATE_ID)) |> 
  group_by(SUBGROUP, STORE_ID, category, group) |> 
  filter(DATE_ID == max(DATE_ID)) |> 
  select(SUBGROUP, STORE_ID, DATE_ID, BRAND, PRICE_, costos_, initial_ticket_price_) |> 
  ungroup() |> 
  collect()

ultimo_precio_est <- ultimo_precio |> 
  summarise(precio_mean = mean(PRICE_),
            precio_sd = sd(PRICE_))

ultimo_precio <- ultimo_precio |> 
  mutate(PRICE_est = (PRICE_ - !!ultimo_precio_est$precio_mean)/!!ultimo_precio_est$precio_sd)


ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball") |> 
  left_join(ultimo_precio, by = c("STORE_ID", "SUBGROUP")) |> 
  select(-c(DATE_ID.y)) |> 
  rename(DATE_ID = DATE_ID.x) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_bask <- ids_test_sep |> 
  filter(SUBGROUP == "Basketball") |> 
  left_join((ultimo_precio |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(DATE_ID.y, SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x, DATE_ID = DATE_ID.x) |> 
  mutate(PRICE_ = PRICE_ * 0.5) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_base <- ids_test_sep |> 
  filter(SUBGROUP == "Baseball") |> 
  left_join((ultimo_precio |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(DATE_ID.y, SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x, DATE_ID = DATE_ID.x) |> 
  mutate(PRICE_ = PRICE_ * 0.5) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep_mismo_precio_sin_bask_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, BRAND, REGION, mes.x, dia, PRICE_, QUANTITY_avg, costos_, initial_ticket_price_, PRICE_est, category, group) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |> 
  rename(mes = mes.x) |> 
  select(-c(QUANTITY_avg))

ids_test_sep_mismo_precio_base <- ids_test_sep_mismo_precio_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, BRAND, REGION, mes.x, dia, PRICE_, QUANTITY_avg, costos_, initial_ticket_price_, PRICE_est, category, group) |> 
  rename(mes = mes.x) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg/2))) |> 
  select(-c(QUANTITY_avg))

ids_test_sep_mismo_precio <- rbind(ids_test_sep_mismo_precio_sin_bask_base, ids_test_sep_mismo_precio_base)

ids_predicciones_sin_bask <- sparklyr::copy_to(sc, ids_test_sep_mismo_precio, overwrite = TRUE)

predicciones_ids_sin_bask <- rf_model |> 
  ml_predict(ids_predicciones_sin_bask) |> 
  select(STORE_SUBGROUP_DATE_ID, SUBGROUP, STORE_ID, prediction, dia, starts_with("probability_")) |> 
  rename(TOTAL_SALES = prediction) |> 
  collect()

predicciones_ids_baseball <- predicciones_ids_sin_bask |> 
  filter(SUBGROUP == "Baseball") |> 
  mutate(TOTAL_SALES = TOTAL_SALES)

predicciones_ids_bask <- ids_test_sep_mismo_precio_bask |> 
  left_join(predicciones_ids_baseball, by = c("STORE_ID", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID.x, STORE_ID, SUBGROUP.x, dia, TOTAL_SALES) |> 
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
write.csv(predicciones_finales, "predicciones_finales_v13.csv", row.names = FALSE, quote = FALSE)

######### Predicciones de Kaggle pero usando el precio promedio de cada producto en el ultimo mes

# Usar el precio promedio del ultimo mes
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
precio_series <- read.csv("Datos/precio")

precio_mediano <- precio_mediano |> 
  left_join(precio_series, by = c("STORE_ID", "SUBGROUP")) |> 
  rename(PRICE_ = PRICE_.y)

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep |> 
  filter(SUBGROUP != "Basketball") |> 
  filter(SUBGROUP != "Baseball") |> 
  left_join(precio_mediano, by = c("STORE_ID", "SUBGROUP")) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_bask <- ids_test_sep |> 
  filter(SUBGROUP == "Basketball") |> 
  left_join((precio_mediano |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x) |> 
  mutate(PRICE_ = PRICE_) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_base <- ids_test_sep |> 
  filter(SUBGROUP == "Baseball") |> 
  left_join((precio_mediano |> filter(SUBGROUP == "Baseball")), by = c("STORE_ID")) |> 
  select(-c(SUBGROUP.y)) |> 
  rename(SUBGROUP = SUBGROUP.x) |> 
  mutate(PRICE_ = PRICE_) |> 
  mutate(dia = lubridate::day(DATE_ID))

ids_test_sep_mismo_precio_sin_bask_base <- ids_test_sep_mismo_precio_sin_bask_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, mes.x, dia, PRICE_, PRICE_.x, QUANTITY_avg, category, group, BRAND, STORE_TYPE) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |>
  rename(mes = mes.x)

ids_test_sep_mismo_precio_base <- ids_test_sep_mismo_precio_base |> 
  left_join(prom_dia_mes, by = c("STORE_ID", "SUBGROUP", "dia")) |> 
  select(STORE_SUBGROUP_DATE_ID, STORE_ID, SUBGROUP, DATE_ID, REGION, mes.x, dia, PRICE_, PRICE_.x, QUANTITY_avg, category, group, BRAND, STORE_TYPE) |> 
  mutate(QUANTITY_ = ifelse(is.na(QUANTITY_avg), 0, round(QUANTITY_avg))) |>
  rename(mes = mes.x)

ids_test_sep_mismo_precio <- rbind(ids_test_sep_mismo_precio_sin_bask_base, ids_test_sep_mismo_precio_base)

ids_predicciones_sin_bask <- sparklyr::copy_to(sc, ids_test_sep_mismo_precio, overwrite = TRUE)

predicciones_ids_sin_bask <- rf_model |> 
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
write.csv(predicciones_finales, "Predicciones/predicciones_finales_v23.csv", row.names = FALSE, quote = FALSE)

