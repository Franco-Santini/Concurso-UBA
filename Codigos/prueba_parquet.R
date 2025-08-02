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
  summarise(cantidad = n()) %>%
  collect()   # Hay 95087 v.f., se pueden calcular haciendo, TOTAL_SALES/PRICE

ds %>%
  filter(is.na(PRICE)) %>% 
  summarise(cantidad = n()) %>%
  collect()   # Hay 0 v.f.

ds %>%
  filter(is.na(TOTAL_SALES)) %>% 
  summarise(cantidad = n()) %>%
  collect()   # Hay 0 v.f.



# Ver si hay outliers

stats <- ds %>%
  summarise(Q1 = quantile(QUANTITY, 0.01, na.rm = TRUE),
            Q3 = quantile(QUANTITY, 0.99, na.rm = TRUE),
            min = min(QUANTITY, na.rm = TRUE),
            max = max(QUANTITY, na.rm = TRUE)) %>%
  collect()

stats$Q3 
stats$Q1
stats$min
stats$max

ds %>%
  filter(QUANTITY < stats$Q1 | QUANTITY > stats$Q3) %>%
  summarise(cantidad = n()) %>%
  collect()  



stats <- ds %>%
  summarise(Q1 = quantile(PRICE, 0.01),
            Q3 = quantile(PRICE, 0.99),
            min = min(PRICE, na.rm = TRUE),
            max = max(PRICE, na.rm = TRUE)) %>%
  collect()

stats$Q3 
stats$Q1
stats$min 
stats$max

ds %>%
  filter(PRICE < stats$Q1 | PRICE > stats$Q3) %>%
  summarise(cantidad = n()) %>%
  collect()  



stats <- ds %>%
  summarise(Q1 = quantile(TOTAL_SALES, 0.01),
            Q3 = quantile(TOTAL_SALES, 0.99),
            min = min(TOTAL_SALES, na.rm = TRUE),
            max = max(TOTAL_SALES, na.rm = TRUE)) %>%
  collect()

stats$Q3 
stats$Q1
stats$min 
stats$max

ds %>%
  filter(TOTAL_SALES < stats$Q1 | TOTAL_SALES > stats$Q3) %>%
  summarise(cantidad = n()) %>%
  collect()  

# Al analizar valores mínimos, máximos y cuartiles, se puede decir que no hay outliers



# Errores en calculos

tolerancia = 0.1

casos_con_error = ds %>% filter(!is.na(QUANTITY)) %>%
  mutate(dif = abs(QUANTITY * PRICE - TOTAL_SALES)) %>%
  filter(dif > tolerancia) %>%
  select(QUANTITY, PRICE, TOTAL_SALES, dif) %>%
  collect()

casos_con_error # Luego de probar diferentes tolerancias, se llegó a la conclusión de que 
                # no hay errores de cálculo, los que hay son simplemente errores de redondeo.

