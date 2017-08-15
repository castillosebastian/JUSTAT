# JUSTAT is a free software developed for official statistics of Supreme Court
# of Entre Ríos (STJER), Argentina, Office of Planification Management and
# Statistics (APGE)

# V.1.0
# 13-06-17

# Authors
# JUSTAT: Lic. Sebastián Castillo, Ing. Federico Caballaro y Lic. Paolo Orundes


############################ proc_CITIPJ  ###################################

## Detalle del proceso

# 1 Abrir conexión con BD_adm_ingresos
# 2 Listar tablas para procesar operacion
# 3 Procesar Indicadores Estadísticos
# 4 Guardar en BD_CITIPJ

## imput: BD_adm_ingresos
## output: BD_CITIPJ actualizada

# Librerías
library(data.table)
library(lubridate)
library(stringr)
library(purrr)
library(stringdist)
library(dplyr)
library(tidyr)
library(tibble)


# Abre bases de datos -----------------------------------------------


proc_CITIPJ <- function() {
  BD_adm_ingresos <- fread("~/JUSTAT/BD/BD_adm_ingresos.csv")
  BD_CITIPJ <- fread("~/JUSTAT/BD/BD_CITIPJ.csv")
  ultimo_procesado <- BD_CITIPJ$id_archivos[which.max(BD_CITIPJ$id_archivos)]

  BD_adm_ingresos <- BD_adm_ingresos[BD_adm_ingresos$id_archivos >
                                     ultimo_procesado &
                                       BD_adm_ingresos$id_operacion == "CITIPJ"
                                     # & BD_adm_ingresos$rec_estado == "admitido"
                                     , ]

  lista_tbs_prim_CITIPJ <- try(map(BD_adm_ingresos$ruta,
                               fread, encoding = "Latin-1",
                               quote = "", strip.white = F))

  # 1 er Bifurcación del flujo del script ------------------------------
  # Trabaja sobre lista con tablas de 2 columnas
  indice_tbs_no2col <- lengths(lista_tbs_prim_CITIPJ) != 2
  lista_tbs_prim_CITIPJ <- lista_tbs_prim_CITIPJ[!indice_tbs_no2col]


  # Arreglo de tbs
  for (i in seq_along(lista_tbs_prim_CITIPJ)) {
    # arreglo de columna cantidad
    lista_tbs_prim_CITIPJ[[i]][[2]] <-
      try(as.integer(gsub("\\D", "",
                      lista_tbs_prim_CITIPJ[[i]][[2]])))
    # asignando nombre a columnas
    colnames(lista_tbs_prim_CITIPJ[[i]]) <-
      try(c("Tipo_proceso", "Cantidad"))
  }


  # Procesamiento discriminado por: materia y proceso
  lista_materia <- list()
  lista_procesos <- list()

  # Asignar observaciones correspondientes a cada lista: Materia y Procesos
  for (i in seq_along(lista_tbs_prim_CITIPJ)) {
    # Controla condición de tbprim con/sin '"'
    if (length(grep('^"', lista_tbs_prim_CITIPJ[[i]][[1]])) == 0) {
      lista_materia[[i]] <-
        try(lista_tbs_prim_CITIPJ[[i]][grep('^[a-zA-Z].*',
                                        lista_tbs_prim_CITIPJ[[i]]$Tipo_proceso)])
      lista_materia[[i]]
    } else {
      lista_materia[[i]] <-
        try(lista_tbs_prim_CITIPJ[[i]][grep('^"[a-zA-Z].*',
                                        lista_tbs_prim_CITIPJ[[i]]$Tipo_proceso)])
      lista_materia[[i]]
    }
  }


  for (i in seq_along(lista_tbs_prim_CITIPJ)) {
    if (length(grep('^"', lista_tbs_prim_CITIPJ[[i]][[1]])) == 0) {
      lista_procesos[[i]] <-
        try(lista_tbs_prim_CITIPJ[[i]][grep('^     [a-zA-Z]',
                                        lista_tbs_prim_CITIPJ[[i]]$Tipo_proceso)])
      lista_procesos[[i]]
    } else {
      lista_procesos[[i]] <-
        try(lista_tbs_prim_CITIPJ[[i]][grep('^"     [a-zA-Z]',
                                        lista_tbs_prim_CITIPJ[[i]]$Tipo_proceso)])
      lista_procesos[[i]]
    }
  }

  # Unificar observaciones repetidas para "lista_procesos" en la
  #variable "Tipo_de_Procesos"
  # y suma cantidad

  lista_procesos_consolidados <- list()
  lista_materias_consolidadas <- list()

  # Limpiar listado
  for (i in seq_along(lista_materia)) {
    lista_materia[[i]][[1]] <- try(str_trim(gsub('"', "", lista_materia[[i]][[1]])))
  }

  for (i in seq_along(lista_procesos)) {
    lista_procesos[[i]][[1]] <- try(str_trim(gsub('"', "", lista_procesos[[i]][[1]])))
  }

  for (i in seq_along(lista_procesos)) {
    lista_procesos_consolidados[[i]] <- try(lista_procesos[[i]] %>%
      group_by(Tipo_proceso) %>%
      summarise(sum(Cantidad)))
    lista_procesos_consolidados[[i]]
  }

  for (i in seq_along(lista_materia)) {
    lista_materias_consolidadas[[i]] <- try(lista_materia[[i]] %>%
      group_by(Tipo_proceso) %>%
      summarise(sum(Cantidad)))
    lista_materias_consolidadas[[i]]
  }

  # Vectorización para armar BD
  materias <- vector()
  for (i in seq_along(lista_materias_consolidadas)) {
    materias[[i]] <- try(str_c(str_c(lista_materias_consolidadas[[i]][1][[1]],
                                 lista_materias_consolidadas[[i]][2][[1]],
                                 sep = "&"),
                           collapse = '#'))
    materias[[i]]
  }

  procesos <- vector()
  for (i in seq_along(lista_procesos_consolidados)) {
    procesos[[i]] <- try(str_c(str_c(lista_procesos_consolidados[[i]][1][[1]],
                                   lista_procesos_consolidados[[i]][2][[1]],
                                 sep = "&"),
                             collapse = '#'))
    procesos[[i]]
  }

  # Subselecciona de BD_adm_ingresos tablas procesadas y filtra columnas para df
  BD_CITIPJ <- BD_adm_ingresos[!indice_tbs_no2col, c(1,2,3,10,11)]
  BD_CITIPJ$mes <- month(BD_adm_ingresos$fecha_hora)
  BD_CITIPJ$calidad <- NA
  BD_CITIPJ$causas_inic_xmateria <- materias
  BD_CITIPJ$causas_inic_xproceso <- procesos

  # Asigna Calidad segun existencia de datos para materia y proceso
  for (i in seq_along(BD_CITIPJ$calidad)) {
    BD_CITIPJ$calidad[[i]] <- ((!is.na(BD_CITIPJ$causas_inic_xmateria[[i]])) +
                              (!is.na(BD_CITIPJ$causas_inic_xproceso[[i]])))
    BD_CITIPJ$calidad[[i]]
  }

  #2 Bifurcacion: recuperar tablas primaras de una columna si existen-------
  BD_CITIPJ2 <- BD_adm_ingresos[indice_tbs_no2col, c(1,2,3,10,11)]
  BD_CITIPJ2$mes <- month(BD_adm_ingresos$fecha_hora)
  BD_CITIPJ2$calidad <- NA
  BD_CITIPJ2$causas_inic_xmateria <- NA
  BD_CITIPJ2$causas_inic_xproceso <- NA

  BD_CITIPJ <- bind_rows(BD_CITIPJ, BD_CITIPJ2)

  write.table(BD_CITIPJ, file= "~/JUSTAT/BD/BD_CITIPJ.csv",
              sep = ",", row.names = F, col.names = F, append = T)
}


