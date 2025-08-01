install.packages("data.table")
install.packages("arrow")

library(data.table)
library(arrow)

# Leer el archivo CSV de forma eficientE
df <- fread("Datos/eci_transactions.csv")  # Reemplazá con la ruta a tu archivo

# Guardar como archivo .parquet (binario, comprimido y columnar)
write_parquet(df, "transactions.parquet")

ds <- open_dataset("transactions.parquet")


# Ver si hay valores faltantes
library(dplyr)

ds %>%
  filter(is.na(QUANTITY)) %>% 
  summarise(cantidad = n())%>%
  collect()   # Hay 95087 v.f.

ds %>%
  filter(is.na(PRICE)) %>% 
  summarise(cantidad = n())%>%
  collect()   # Hay 0 v.f.

ds %>%
  filter(is.na(TOTAL_SALES)) %>% 
  summarise(cantidad = n())%>%
  collect()   # Hay 0 v.f.


# Ver si hay outliers

