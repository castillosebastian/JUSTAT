# JUSTAT is a free software developed for official statistics of Supreme Court
# of Entre Ríos (STJER), Argentina, Office of Planification Management and
# Statistics (APGE)

# V.1.0
# 13-06-17

# Authors
# JUSTAT: Lic. Sebastián Castillo, Ing. Federico Caballaro y Lic. Paolo Orundes


############################ proc_CAMOV  ###################################

## Detalle del proceso

# Librerías
library(data.table)
library(lubridate)
library(stringr)
library(purrr)
library(stringdist)
library(dplyr)


# Abre bases de datos -----------------------------------------------

proc_CAMOV <- function() {
  BD_adm_ingresos <- fread("~/JUSTAT/BD/BD_adm_ingresos.csv")
  BD_CAMOV <- fread("~/JUSTAT/BD/BD_CAMOV.csv")
  ultimo_procesado <- BD_CAMOV$id_archivos[which.max(BD_CAMOV$id_archivos)]
  BD_adm_ingresos <- BD_adm_ingresos[BD_adm_ingresos$id_archivos >
                                    ultimo_procesado &
                                    BD_adm_ingresos$id_operacion == "CAMOV", ]

  lista_tbs_prim_CAMOV <- list()

  for (i in seq_along(BD_adm_ingresos$ruta)) {
    lista_tbs_prim_CAMOV[[i]] <- try(fread(BD_adm_ingresos$ruta[[i]]))
    lista_tbs_prim_CAMOV[[i]]
  }

  movimientos <- as.numeric(as.character(map(lista_tbs_prim_CAMOV, nrow)))


  # Arma BD_CAMOV-------------------------------------------------------

  # Recupera información de BD_adm_ingresos
  BD_CAMOV <- BD_adm_ingresos[ , c(1,2,10,9,11,14)]
  BD_CAMOV$movimientos <- movimientos

  # Guarda datos del Procesamiento-------------------------------------------


  # En BD_CAMOV
  write.table(BD_CAMOV, file= "~/JUSTAT/BD/BD_CAMOV.csv",
              sep = ",", row.names = F, col.names = F, append = T)

  # En BD/diseminacion
  BD_df_diseminacion <- BD_CAMOV[, -6]
  BD_CAMOV_encabezado <-
    c("nro_presentacion", "organismo", "anio", "mes", "materia",
      "causas_con_1o+_movivmientos_procesales"
      )
  colnames(BD_df_diseminacion) <- BD_CAMOV_encabezado

  write.table(BD_df_diseminacion, file= "~/JUSTAT/BD/diseminacion/BD_CAMOV.csv",
              sep = ",", row.names = F, col.names = F, append = T)
}


