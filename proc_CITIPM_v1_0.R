# JUSTAT is a free software developed for official statistics of Supreme Court
# of Entre Ríos (STJER), Argentina, Office of Planification Management and
# Statistics (APGE)

# V.1.0
# 13-06-17

# Authors
# JUSTAT: Lic. Sebastián Castillo, Ing. Federico Caballaro y Lic. Paolo Orundes


############################ proc_CITIPM  ###################################

# Librerías
library(data.table)
library(lubridate)
library(stringr)
library(purrr)
library(stringdist)
library(dplyr)
library(tidyr)
library(tibble)



proc_CITIPM <- function(id_archivo) {
  BD_adm_ingresos <- fread("~/JUSTAT/BD/BD_adm_ingresos.csv")
  BD_CITIPM <- fread("~/JUSTAT/BD/BD_CITIPM.csv")
  ultimo_procesado <- BD_CITIPM$id_archivos[which.max(BD_CITIPM$id_archivos)]
  BD_adm_ingresos <- BD_adm_ingresos[BD_adm_ingresos$id_archivos >
                                       ultimo_procesado &
                                       BD_adm_ingresos$id_operacion == "CITIPM" &
                                       BD_adm_ingresos$rec_estado == "admitido"
                                     , ]

  lista_tbs_prim_CITIPM <- map(BD_adm_ingresos$ruta,
                                 fread, encoding = "Latin-1", na.strings = "" )

  for (i in seq_along(lista_tbs_prim_CITIPM)) {
    if (length(lista_tbs_prim_CITIPM[[i]]) == 6) {
      colnames(lista_tbs_prim_CITIPM[[i]]) <-
        try(c("caratula", "tproc", "finic", "nro_receptoria",
              "radicacion", "origenOmedpriv"))
    } else {
      next()
    }
  }

  # filtar tablas de 6 variables
  indice_6col <- lengths(lista_tbs_prim_CITIPM) == 6

  lista_tbs_prim_CITIPM <- lista_tbs_prim_CITIPM[indice_6col]

  lista_inic_xorgano <- list()

  for (i in seq_along(lista_tbs_prim_CITIPM)) {
    lista_inic_xorgano[[i]] <- lista_tbs_prim_CITIPM[[i]] %>%
      group_by(radicacion, tproc) %>%
      summarise(cantidad_procesos = n())
    lista_inic_xorgano[[i]]$periodo <- BD_adm_ingresos$id_periodo[indice_6col][[i]]
    lista_inic_xorgano[[i]]$id_archivos <- BD_adm_ingresos$id_archivos[indice_6col][[i]]
    lista_inic_xorgano[[i]]$informante <- BD_adm_ingresos$id_organismo[indice_6col][[i]]
    lista_inic_xorgano[[i]]
  }

  BD_dfs <- bind_rows(lista_inic_xorgano)
  BD_dfs <- select(BD_dfs, id_archivos, informante, periodo, radicacion,
                   tproc, cantidad_procesos)

  write.table(BD_dfs, file= "~/JUSTAT/BD/BD_CITIPM.csv",
              sep = ",", row.names = F, col.names = F, append = T)

}

