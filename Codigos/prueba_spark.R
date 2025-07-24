library(sparklyr)
library(dplyr)
library(ggplot2)
library(dbplot)
library(lubridate)

# Establecemos la coneccion con spark
sc <- spark_connect(master = "local")

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

# Renombre de las columnas
# Tabla de tiendas
eci_stores_clusters_join <- eci_stores_totales |> 
  left_join(eci_stores_clusters_totales, by = "STORE_ID") |> 
  select(-c(BRAND.y, STORE_NAME.y)) |> 
  rename(BRAND = BRAND.x, STORE_NAME = STORE_NAME.x)

# Tabla del maestro de productos
eci_product_master_totales <- eci_product_master_totales |> rename(SKU = sku, SUBGROUP = subgroup)

# Filtrar los datos faltantes 

# Intento unir las bases de transacciones y stores
eci_transactions_stores_sample <- eci_transactions_sample |> 
  left_join(eci_stores |> collect(), by = "STORE_ID") |> 
  mutate(DATE = as_date(DATE))

eci_transactions_stores_sample$mes <- month(eci_transactions_stores_sample$DATE)
eci_transactions_stores_sample$año <- year(eci_transactions_stores_sample$DATE)
eci_transactions_stores_sample$mes_año <- ym(paste(eci_transactions_stores_sample$año, eci_transactions_stores_sample$mes, sep = "/"))

# Join de transacciones con productos
eci_transactions_stores_prod_sample <- eci_transactions_stores_sample |> 
  left_join(eci_product_master_totales, by = c("SKU", "SUBGROUP"))


# Trabajamos con una muestra de todos los datos
eci_transactions_sample <- eci_transactions |> 
  sdf_sample(0.02) |> # Trabajamos con solo el 2% de los datos 
  collect()

# Deteccion de valores faltantes (Todos los data frames)
# Muestra de la base de datos de transacciones
eci_transactions_sample |> 
  select(everything()) |> 
  summarize(across(everything(), ~sum(is.na(.))))

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
eci_transactions_stores_sample |> 
  group_by(mes_año) |> 
  summarise(demanda = sum(TOTAL_SALES)) |> 
  ggplot() +
  aes(x = mes_año, y = demanda) +
  geom_line() +
  geom_point() +
  theme_bw() +
  theme(legend.position = "top")

# Evolucion temporal de la demanda x subgrupos que más demanda tienen
eci_transactions_stores_sample |> 
  group_by(mes_año, SUBGROUP) |> 
  summarise(demanda_x_subg = sum(TOTAL_SALES)) |> 
  filter(SUBGROUP %in% c("Headphones", "Wearables", "Speakers")) |> 
  ggplot() +
  aes(x = mes_año, y = demanda_x_subg, color = factor(SUBGROUP)) +
  geom_line() +
  geom_point() +
  theme_bw() +
  theme(legend.position = "top")


# Analisis exploratorio (dividido en dos partes, agrupar los datos trabajando en spark y luego graficar)

transacciones_agrup <- eci_transactions |> 
  group_by(SUBGROUP) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  collect() |> 
  print()

transacciones_agrup |> 
  top_n(10, wt = Demanda) |>  
  ggplot() +
  aes(x = SUBGROUP, y = Demanda) +
  geom_bar(stat = "identity") +
  coord_flip() +
  theme_bw()

transacciones_agrup_store_sample <- eci_transactions_stores_sample |> 
  group_by(STORE_ID, SUBGROUP) |> 
  summarise(Demanda = sum(TOTAL_SALES)) |> 
  ungroup() |> 
  collect() |> 
  print()

# Desconectamos spark
spark_disconnect(sc)
