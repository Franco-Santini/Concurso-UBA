library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)
library(rlang)

# Configuraciones de spark
config <- spark_config()
config$spark.driver.memory <- "8g"
config$spark.executor.memory <- "4g"
config$spark.storage.memoryFraction <- 0.8

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

eci_stores_totales <- eci_stores |> mutate(CLOSEDATE = as_date(CLOSEDATE)) |> filter(CLOSEDATE > "2023-12-31" | is.na(CLOSEDATE)) |> filter(!is.na(STORE_TYPE))
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

eci_transactions_stores_prod_sample$margen <- eci_transactions_stores_prod_sample$TOTAL_SALES - eci_transactions_stores_prod_sample$TOTAL_SALES/eci_transactions_stores_prod_sample$PRICE*eci_transactions_stores_prod_sample$costos

eci_transactions_stores_prod <- eci_transactions_stores_prod |> 
  mutate(
    margen = TOTAL_SALES - (TOTAL_SALES/PRICE)*costos,
    ganancia = (PRICE - costos) * TOTAL_SALES
  ) |> 
  filter(!is.na(BRAND))

# Renombre de las columnas
# Tabla de tiendas
eci_stores_clusters_join <- eci_stores_totales |> 
  left_join(eci_stores_clusters_totales, by = "STORE_ID") |> 
  select(-c(BRAND.y, STORE_NAME.y)) |> 
  rename(BRAND = BRAND.x, STORE_NAME = STORE_NAME.x)

# Tabla del maestro de productos
eci_product_master_totales <- eci_product_master_totales |> rename(SKU = sku, SUBGROUP = subgroup)

# Filtrar los datos faltantes 

# Trabajamos con una muestra de todos los datos
eci_transactions_sample <- eci_transactions |> 
  sdf_sample(0.02) |> # Trabajamos con solo el 2% de los datos 
  collect()

# Intento unir las bases de transacciones y stores
eci_transactions_stores_sample <- eci_transactions_sample |> 
  left_join(eci_stores_clusters_join, by = "STORE_ID") |> 
  mutate(DATE = as_date(DATE)) |> 
  filter(!is.na(BRAND))

eci_transactions_stores_sample$mes <- month(eci_transactions_stores_sample$DATE)
eci_transactions_stores_sample$año <- year(eci_transactions_stores_sample$DATE)
eci_transactions_stores_sample$mes_año <- ym(paste(eci_transactions_stores_sample$año, eci_transactions_stores_sample$mes, sep = "/"))

# Join de transacciones con productos
eci_transactions_stores_prod_sample <- eci_transactions_stores_sample |> 
  left_join(eci_product_master_totales, by = c("SKU", "SUBGROUP"))

# Variable ganancia
eci_transactions_stores_prod_sample$margen <- eci_transactions_stores_prod_sample$TOTAL_SALES - eci_transactions_stores_prod_sample$TOTAL_SALES/eci_transactions_stores_prod_sample$PRICE*eci_transactions_stores_prod_sample$costos


# Deteccion de valores faltantes (Todos los data frames)
# Base completa de transacciones
eci_transactions %>%
  summarise_all(~sum(as.integer(is.na(.))))


# Base completa de grupo de productos
eci_product_groups |> 
  select(everything()) |> 
  collect() |>
  summarize(across(everything(), ~sum(is.na(.))))

# Base completa del maestro de productos
eci_product_master |> 
  select(everything()) |> 
  collect() |> 
  summarize(across(everything(), ~sum(is.na(.))))

# Base completa de las tiendas
eci_stores |> 
  select(everything()) |> 
  collect() |> 
  summarize(across(everything(), ~sum(is.na(.))))

# Base completa de los clusters de las tiendas
eci_stores_clusters |> 
  select(everything()) |> 
  collect() |> 
  summarize(across(everything(), ~sum(is.na(.))))

# Base completa de clientes (ni idea si se tiene que usar)
eci_customer |> 
  select(everything()) |> 
  collect() |> 
  summarize(across(everything(), ~sum(is.na(.))))

# Evolucion temporal de la demanda
eci_transactions_stores_prod |> 
  group_by(año_mes) |> 
  summarise(demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  ggplot() +
  aes(x = año_mes, y = demanda) +
  geom_line() +
  geom_point(color = "dodgerblue2") +
  labs(x = "Fecha", y = "Demanda ($)") +
  theme_bw() 

# Evolucion temporal de la demanda x subgrupos que más demanda tienen
eci_transactions_stores_prod |>
  group_by(año_mes, SUBGROUP) |> 
  summarise(demanda_x_subg = sum(TOTAL_SALES)) |> 
  filter(SUBGROUP %in% c("Headphones", "Wearables", "Speakers")) |> 
  ungroup() |> 
  ggplot() +
  aes(x = año_mes, y = demanda_x_subg, color = factor(SUBGROUP)) +
  geom_line() +
  geom_point() +
  theme_bw() +
  scale_color_manual(values = c("firebrick2", "dodgerblue2", "#982649")) +
  labs(x = "Fecha", y = "Demanda($)", color = "Subgrupo") +
  theme(legend.position = "top")

# Evolucion temporal de la demanada x 

# Analisis exploratorio (dividido en dos partes, agrupar los datos trabajando en spark y luego graficar)
# Grafico que va
transacciones_agrup <- eci_transactions |> 
  group_by(SUBGROUP) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  collect() |> 
  print()

transacciones_agrup |> 
  top_n(10, wt = Demanda) |>
  ggplot() +
  aes(x = reorder(SUBGROUP, Demanda), y = Demanda) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Demanda ($)", x = "Subgrupo") +
  theme_bw()

# Grafico que va
ganancias_tiendas_totales <- eci_transactions_stores_prod |> 
  group_by(SUBGROUP) |> 
  summarise(Margen = sum(margen)) |>
  collect() |> 
  print()

ganancias_tiendas_totales |> 
  top_n(10, wt = Margen) |>
  ggplot() +
  aes(x = reorder(SUBGROUP, Margen), y = Margen) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Margen ($)", x = "Subgrupo") +
  theme_bw()


# Grafico que va
transacciones_agrup_store_sample <- eci_transactions_stores_prod |>
  filter(BRAND == "AsterionHouse") |> 
  group_by(SUBGROUP) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  collect() |> 
  print()

transacciones_agrup_store_sample |> 
  top_n(10, wt = Demanda) |>
  ggplot() +
  aes(x = reorder(SUBGROUP, Demanda), y = Demanda) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Demanda ($)", x = "Subgrupo") +
  theme_bw()

# Grafico que va
ganancias_asterion <- eci_transactions_stores_prod |> 
  filter(BRAND == "AsterionHouse") |> 
  group_by(SUBGROUP) |> 
  summarise(Margen = sum(margen)) |> 
  collect() |> 
  print()

ganancias_asterion |> 
  top_n(10, wt = Margen) |>
  ggplot() +
  aes(x = reorder(SUBGROUP, Margen), y = Margen) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Margen ($)", x = "Subgrupo") +
  theme_bw()

# Grafico que va
tipo_tienda_demanda <- eci_transactions_stores_prod |>
  group_by(STORE_TYPE) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  collect() |> 
  print()

tipo_tienda_demanda |> 
  ggplot() +
  aes(x = reorder(STORE_TYPE, Demanda), y = Demanda) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Demanda ($)", x = "Tipo de tienda") +
  theme_bw()

# Grafico que va
tipo_tienda_ganancia <- eci_transactions_stores_prod |>
  group_by(STORE_ID, STORE_TYPE) |>
  summarise(MargenTienda = sum(margen, na.rm = TRUE), .groups = "drop") |>
  group_by(STORE_TYPE) |>
  summarise(MargenPromedio = mean(MargenTienda)) |> 
  ungroup() |> 
  collect()

tipo_tienda_ganancia |> 
  ggplot() +
  aes(x = reorder(STORE_TYPE, MargenPromedio), y = MargenPromedio) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(y = "Margen promedio ($)", x = "Tipo de tienda") +
  theme_bw()

# Grafico que va
categoria_demanda <- eci_transactions_stores_prod |>
  group_by(category) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  collect() |> 
  print()

categoria_demanda |> 
  ggplot() +
  aes(x = reorder(category, Demanda), y = Demanda) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(x = "Categoría", y = "Demanda ($)") +
  theme_bw()

# Grafico que va
categoria_ganancia <- eci_transactions_stores_prod |>
  group_by(category) |> 
  summarise(Margen = sum(margen)) |> 
  ungroup() |> 
  collect() |> 
  print()

categoria_ganancia |> 
  ggplot() +
  aes(x = reorder(category, Margen), y = Margen) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(x = "Categoría", y = "Margen ($)") +
  theme_bw()

# Grafico que va
grupo_demanda <- eci_transactions_stores_prod |>
  group_by(group) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  collect() |> 
  print()

grupo_demanda |> 
  top_n(n = 10, wt = Demanda) |> 
  ggplot() +
  aes(x = reorder(group, Demanda), y = Demanda) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue2") +
  coord_flip() +
  labs(x = "Grupo de productos", y = "Demanda ($)") +
  theme_bw()

# Desconectamos spark
spark_disconnect(sc)
