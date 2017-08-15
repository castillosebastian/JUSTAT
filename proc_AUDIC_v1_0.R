# JUSTAT is a free software developed for official statistics of Supreme Court
# of Entre Ríos (STJER), Argentina, Office of Planification Management and
# Statistics (APGE)

# V.1.0
# 13-06-17

# Authors
# JUSTAT: Lic. Sebastián Castillo, Ing. Federico Caballaro y Lic. Paolo Orundes


############################ proc_AUDIC  ###################################

# Librerías
library(data.table)
library(lubridate)
library(stringr)
library(purrr)
library(stringdist)
library(dplyr)


# Abre bases de datos -----------------------------------------------

proc_AUDIC <- function() {
  BD_adm_ingresos <- fread("~/JUSTAT/BD/BD_adm_ingresos.csv")
  BD_AUDIC <- fread("~/JUSTAT/BD/BD_AUDIC.csv")
  ultimo_procesado <- BD_AUDIC$id_archivos[which.max(BD_AUDIC$id_archivos)]

  BD_adm_ingresos <- BD_adm_ingresos[BD_adm_ingresos$id_archivos >
                                     ultimo_procesado &
                                     BD_adm_ingresos$id_operacion == "AUDIC"
                                     & BD_adm_ingresos$rec_estado == "admitido"
                                     , ]

  lista_tbs_prim_AUDIC <- map(BD_adm_ingresos$ruta,
                              fread, encoding = "Latin-1", na.strings = "" )

  for (i in seq_along(lista_tbs_prim_AUDIC)) {
    if (colnames(lista_tbs_prim_AUDIC[[i]])[[1]] == "V1") {
      colnames(lista_tbs_prim_AUDIC[[i]]) <- try(c("nro", "caratula", "tproc",
                                                      "asis", "finicio", "duracm",
                                                      "duracb", "ffa", "fea",
                                                      "ta", "ra", "esta",
                                                      "jaud", "mat", "audvid",
                                                      "justiables"))
    } else {
      next()
    }
  }

  # Preparar tbs primarias: Asignar clase a columnas fechas, Agregar columna de
  # mes de dictado de resolución. Convertir a mayúsculas as
  for (i in seq_along(lista_tbs_prim_AUDIC)) {
    lista_tbs_prim_AUDIC[[i]]$finicio <-
      as.Date(lista_tbs_prim_AUDIC[[i]]$finicio, format="%d/%m/%Y")
    lista_tbs_prim_AUDIC[[i]]$ffa <-
      as.Date(lista_tbs_prim_AUDIC[[i]]$ffa, format="%d/%m/%Y")
    lista_tbs_prim_AUDIC[[i]]$fea <-
      as.Date(lista_tbs_prim_AUDIC[[i]]$fea, format="%d/%m/%Y")
    lista_tbs_prim_AUDIC[[i]]$mes_ffa <-
      month(lista_tbs_prim_AUDIC[[i]]$ffa)
    lista_tbs_prim_AUDIC[[i]]$mes_fea <-
      month(lista_tbs_prim_AUDIC[[i]]$fea)
    lista_tbs_prim_AUDIC[[i]]$ra <-
      toupper(lista_tbs_prim_AUDIC[[i]]$ra)
    lista_tbs_prim_AUDIC[[i]]$mat <-
      toupper(lista_tbs_prim_AUDIC[[i]]$mat)
    lista_tbs_prim_AUDIC[[i]]$duracm <-
      as.integer(gsub("[^0-9]", "", lista_tbs_prim_AUDIC[[i]]$duracm))
    lista_tbs_prim_AUDIC[[i]]$duracb <-
      as.integer(gsub("[^0-9]", "", lista_tbs_prim_AUDIC[[i]]$duracb))
  }


  # Creación de funciones

  p_aud_fijadas_total <- function() {
    # Cuenta audiencias informadas (total)
    aud_fijadas_total <- nrow(lista_tbs_prim_AUDIC[[i]])
    aud_fijadas_total
  }

  p_aud_fijadas_mes <- function() {
    # Cuenta audiencias fijadas en el mes
    aud_fijadas_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_ffa ==
                              BD_adm_ingresos$id_periodo[[i]], na.rm = T)
    aud_fijadas_mes

  }

  p_aud_realizadas_mes <- function() {
    # Cuenta audiencias realizadas en el mes
    aud_realizadas_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                             BD_adm_ingresos$id_periodo[[i]] &
                               lista_tbs_prim_AUDIC[[i]]$esta == 2, na.rm = T)
    aud_realizadas_mes

  }

  p_aud_norealizadas_mes <- function() {
    # Cuenta audiencias no realizadas en el mes
    aud_norealizadas_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 3, na.rm = T)
    aud_norealizadas_mes

  }

  p_aud_canceladas_mes <- function() {
    # Cuenta audiencias canceladas en el mes
    aud_canceladas_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 4, na.rm = T)
    aud_canceladas_mes

  }

  p_aud_reprogramadas_mes <- function() {
    # Cuenta audiencias reprogramadas en el mes
    aud_reprogramadas_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 5, na.rm = T)
    aud_reprogramadas_mes

  }


  p_aud_preliminar_mes <- function() {
    # Cuenta audiencias preliminares realizadas en el mes
    aud_preliminar_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                                lista_tbs_prim_AUDIC[[i]]$ta  == 1, na.rm = T)
    aud_preliminar_mes

  }

  p_aud_prueba_mes <- function() {
    # Cuenta audiencias de prueba realizadas en el mes
    aud_prueba_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                                lista_tbs_prim_AUDIC[[i]]$ta  == 2, na.rm = T)
    aud_prueba_mes

  }

  p_aud_conciliacion_mes <- function() {
    # Cuenta audiencias conciliación realizadas en el mes
    aud_conciliacion_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                                lista_tbs_prim_AUDIC[[i]]$ta  == 3, na.rm = T)
    aud_conciliacion_mes

  }

  p_aud_otras_mes <- function() {
    # Cuenta audiencias otras realizadas en el mes
    aud_otras_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                  BD_adm_ingresos$id_periodo[[i]] &
                                  lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                                  lista_tbs_prim_AUDIC[[i]]$ta  == 4, na.rm = T)
    aud_otras_mes

  }

  p_aud_conct_mes <- function() {
    # Cuenta audiencias con conciliación total
    aud_conct_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                           BD_adm_ingresos$id_periodo[[i]] &
                           lista_tbs_prim_AUDIC[[i]]$ra  == "T", na.rm = T)
    aud_conct_mes

  }

  p_aud_concp_mes <- function() {
    # Cuenta audiencias con conciliación parcial
    aud_concp_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                           BD_adm_ingresos$id_periodo[[i]] &
                           lista_tbs_prim_AUDIC[[i]]$ra  == "P", na.rm = T)
    aud_concp_mes

  }

  p_aud_sconc_mes <- function() {
    # Cuenta audiencias sin conciliacion
    aud_sconc_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                           BD_adm_ingresos$id_periodo[[i]] &
                           lista_tbs_prim_AUDIC[[i]]$ra  == "S", na.rm = T)
    aud_sconc_mes

  }

  p_prom_durac_ffa_frealizada <- function() {
    # Duración desde Fecha de Fijación a F.Realización
    indice_aud_realizadas <- lista_tbs_prim_AUDIC[[i]]$mes_fea ==
      BD_adm_ingresos$id_periodo[[i]] &
      lista_tbs_prim_AUDIC[[i]]$esta == 2
    durac_ffa_frealizada <-
      mean(as.integer(lista_tbs_prim_AUDIC[[i]]$fea[indice_aud_realizadas] -
                        lista_tbs_prim_AUDIC[[i]]$ffa[indice_aud_realizadas]),
           na.rm = T)
    durac_ffa_frealizada
  }

  p_prom_asis <- function() {
    # Promedio de asistentes por audiencia
    indice_aud_realizadas <- lista_tbs_prim_AUDIC[[i]]$mes_fea ==
      BD_adm_ingresos$id_periodo[[i]] &
      lista_tbs_prim_AUDIC[[i]]$esta == 2
    prom_asis <- mean(lista_tbs_prim_AUDIC[[i]]$asis[indice_aud_realizadas],
           na.rm = T)
    prom_asis
  }

  p_prom_duracm <- function() {
    # Promedio de duración en minutos por audiencia
    indice_aud_realizadas <- lista_tbs_prim_AUDIC[[i]]$mes_fea ==
      BD_adm_ingresos$id_periodo[[i]] &
      lista_tbs_prim_AUDIC[[i]]$esta == 2
    prom_duracm <- mean(lista_tbs_prim_AUDIC[[i]]$duracm[indice_aud_realizadas],
                      na.rm = T)
    prom_duracm
  }

  p_mediana_duracm <- function() {
    # Promedio de duración en minutos por audiencia
    indice_aud_realizadas <- lista_tbs_prim_AUDIC[[i]]$mes_fea ==
      BD_adm_ingresos$id_periodo[[i]] &
      lista_tbs_prim_AUDIC[[i]]$esta == 2
    mediana_duracm <- median(lista_tbs_prim_AUDIC[[i]]$duracm[indice_aud_realizadas],
                        na.rm = T)
    mediana_duracm
  }

  p_prom_duracb <- function() {
    # Promedio de duración en minutos por audiencia
    indice_aud_realizadas <- lista_tbs_prim_AUDIC[[i]]$mes_fea ==
      BD_adm_ingresos$id_periodo[[i]] &
      lista_tbs_prim_AUDIC[[i]]$esta == 2
    prom_duracb <- mean(lista_tbs_prim_AUDIC[[i]]$duracb[indice_aud_realizadas],
                        na.rm = T)
    prom_duracb
  }

  p_aud_videofilm_mes <- function() {
    # Cuenta audiencias preliminar en el mes
    aud_videofilm_mes <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                                lista_tbs_prim_AUDIC[[i]]$audvid == 1, na.rm = T)
    aud_videofilm_mes
  }

  p_aud_matfam <- function() {
    # Cuenta audiencias reprogramadas en el mes
    aud_matfam <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                           BD_adm_ingresos$id_periodo[[i]] &
                        lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                           lista_tbs_prim_AUDIC[[i]]$mat  == "F", na.rm = T)
    aud_matfam
  }

  p_aud_matciv <- function() {
    # Cuenta audiencias reprogramadas en el mes
    aud_matciv <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                        BD_adm_ingresos$id_periodo[[i]] &
                        lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                        lista_tbs_prim_AUDIC[[i]]$mat  == "C", na.rm = T)
    aud_matciv
  }

  p_aud_matlab <- function() {
    # Cuenta audiencias reprogramadas en el mes
    aud_matlab <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                        BD_adm_ingresos$id_periodo[[i]] &
                        lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                        lista_tbs_prim_AUDIC[[i]]$mat  == "L", na.rm = T)
    aud_matlab
  }

  p_aud_matpen <- function() {
    # Cuenta audiencias reprogramadas en el mes
    aud_matpen <- sum(lista_tbs_prim_AUDIC[[i]]$mes_fea ==
                        BD_adm_ingresos$id_periodo[[i]] &
                        lista_tbs_prim_AUDIC[[i]]$esta == 2 &
                        lista_tbs_prim_AUDIC[[i]]$mat  == "P", na.rm = T)
    aud_matpen
  }

  # DESARROLLO:
  # Audiencias y tipo de proceso!

  # crear vectores
  aud_fijadas_total <- vector()
  aud_fijadas_mes <- vector()
  aud_realizadas_mes <- vector()
  aud_norealizadas_mes <- vector()
  aud_canceladas_mes <- vector()
  aud_reprogramadas_mes <- vector()
  aud_preliminar_mes <- vector()
  aud_prueba_mes <- vector()
  aud_conciliacion_mes <- vector()
  aud_otras_mes <- vector()
  aud_conct_mes <- vector()
  aud_concp_mes <- vector()
  aud_sconc_mes <- vector()
  durac_ffa_frealizada <- vector()
  prom_asis <- vector()
  prom_duracm <- vector()
  mediana_duracm <- vector()
  prom_duracb <- vector()
  aud_videofilm_mes <- vector()
  aud_matfam <- vector()
  aud_matciv <- vector()
  aud_matlab <- vector()
  aud_matpen <- vector()


  # Extracción de indicadores estadísticos.Con "try" para devolver NA ante
  # falla
  for (i in seq_along(lista_tbs_prim_AUDIC)) {
    aud_fijadas_total[[i]] <- try(p_aud_fijadas_total(), silent = TRUE)
    aud_fijadas_total
    aud_fijadas_mes[[i]] <- try(p_aud_fijadas_mes(), silent = TRUE)
    aud_fijadas_mes
    aud_realizadas_mes[[i]] <- try(p_aud_realizadas_mes(), silent = TRUE)
    aud_realizadas_mes
    aud_norealizadas_mes[[i]] <- try(p_aud_norealizadas_mes(), silent = TRUE)
    aud_norealizadas_mes
    aud_canceladas_mes[[i]] <- try(p_aud_canceladas_mes(), silent = TRUE)
    aud_canceladas_mes
    aud_reprogramadas_mes[[i]] <- try(p_aud_reprogramadas_mes(), silent = TRUE)
    aud_reprogramadas_mes
    aud_preliminar_mes[[i]] <- try(p_aud_preliminar_mes(), silent = TRUE)
    aud_preliminar_mes
    aud_prueba_mes[[i]] <- try(p_aud_prueba_mes(), silent = TRUE)
    aud_prueba_mes
    aud_conciliacion_mes[[i]] <- try(p_aud_conciliacion_mes(), silent = TRUE)
    aud_conciliacion_mes
    aud_otras_mes[[i]] <- try(p_aud_otras_mes(), silent = TRUE)
    aud_otras_mes
    aud_conct_mes[[i]] <- try(p_aud_conct_mes(), silent = TRUE)
    aud_conct_mes
    aud_concp_mes[[i]] <- try(p_aud_concp_mes(), silent = TRUE)
    aud_concp_mes
    aud_sconc_mes[[i]] <- try(p_aud_sconc_mes(), silent = TRUE)
    aud_sconc_mes
    durac_ffa_frealizada[[i]] <- try(p_prom_durac_ffa_frealizada(), silent = TRUE)
    durac_ffa_frealizada
    prom_asis[[i]] <- try(p_prom_asis(), silent = TRUE)
    prom_asis
    prom_duracm[[i]] <- try(p_prom_duracm(), silent = TRUE)
    prom_duracm
    mediana_duracm[[i]] <- try(p_mediana_duracm(), silent = TRUE)
    mediana_duracm
    prom_duracb[[i]] <- try(p_prom_duracb(), silent = TRUE)
    prom_duracb
    aud_videofilm_mes[[i]] <- try(p_aud_videofilm_mes(), silent = TRUE)
    aud_videofilm_mes
    aud_matfam[[i]] <- try(p_aud_matfam(), silent = TRUE)
    aud_matfam
    aud_matciv[[i]] <- try(p_aud_matciv(), silent = TRUE)
    aud_matciv
    aud_matlab[[i]] <- try(p_aud_matlab(), silent = TRUE)
    aud_matlab
    aud_matpen[[i]] <- try(p_aud_matpen(), silent = TRUE)
    aud_matpen
  }


  # Arma BD_AUDIC-------------------------------------------------------

  # Recupera información de BD_adm_ingresos
  df_adm_ingresos <- BD_adm_ingresos[ , c(1,2,10,9,11,14)]
  # Asigna nombre y clase a columnas específicas

  # Agrega variables activo y calidad
  df_adm_ingresos$activo <- TRUE
  df_adm_ingresos$calidad <- NA

  # Arma df indicadores procesados
  df <- data.frame(
    aud_fijadas_total,
    aud_fijadas_mes,
    aud_realizadas_mes,
    aud_norealizadas_mes,
    aud_canceladas_mes,
    aud_reprogramadas_mes,
    aud_preliminar_mes,
    aud_prueba_mes,
    aud_conciliacion_mes,
    aud_otras_mes,
    aud_conct_mes,
    aud_concp_mes,
    aud_sconc_mes,
    durac_ffa_frealizada,
    prom_asis,
    prom_duracm,
    mediana_duracm,
    prom_duracb,
    aud_videofilm_mes,
    aud_matfam,
    aud_matciv,
    aud_matlab,
    aud_matpen)

  # Combina df para agregar a BD
  BD_dfs <- bind_cols(df_adm_ingresos, df)

  #### Desde acá-------------------------------------------------------

  # Evalúa calidad del informe presentado en base a consistencia de indicadores
  # y la presentación de datos. Sigue las siguientes reglas
    # 1: Causas resueltas <= Causas a despacho
    # 2: Causas resueltas == resueltas x S + x A + x otra
    # 3: Causas resoluestas == resueltas a termino + luego1v + luego2v
        # Como en estas columnas se presentan NA a veces no se puede evaluar la
        # la función de procesamiento
    # 4: 0 < Causas Iniciadas (ppales + no ppales) + causas archivadas
  # Máximo puntaje 4, mínimo 1.

  #for (i in seq_along(BD_dfs$calidad)) {
  #  regla1 <- BD_dfs$causas_resueltas[[i]] <= BD_dfs$causas_adespacho[[i]]
  #  regla2 <- BD_dfs$causas_resueltas[[i]] == (BD_dfs$res_xsentencia[[i]] +
  #                                          BD_dfs$res_xauto[[i]] +
  #                                          BD_dfs$res_xotra[[i]])
  #  regla3 <- BD_dfs$causas_resueltas[[i]] == (BD_dfs$res_atermino[[i]] +
  #                                          BD_dfs$res_luego1venc[[i]] +
  #                                          BD_dfs$res_luego2venc[[i]])
  #  regla4 <- 0 < (BD_dfs$causas_inic_ppales[[i]] +
  #                   BD_dfs$causas_inic_noppales[[i]] +
  #                   BD_dfs$causas_archivadas[[i]])
  #  BD_dfs$calidad[[i]] <- sum(regla1 + regla2 + regla3 + regla4, na.rm = T)
  #  BD_dfs$calidad[[i]]
  #}

  # Guarda datos del Procesamiento-------------------------------------------

  # En BD_AUDIC
  write.table(BD_dfs, file= "~/JUSTAT/BD/BD_AUDIC.csv",
                sep = ",", row.names = F, col.names = F, append = T)

  # En BD/diseminacion
  BD_df_diseminacion <- BD_dfs[, -c(6,7,8)]
  BD_AUDIC_encabezado <-
    c("nro_presentacion", "organismo", "anio", "mes", "materia",
      "audiencias_fijadas_total", "aud_fijadas_mes", "aud_realizadas_mes",
      "aud_norealizadas_mes", "aud_canceladas_mes", "aud_reprogramadas_mes",
      "aud_preliminar_mes", "aud_prueba_mes", "aud_conciliacion_mes",
      "aud_otras_mes", "aud_conciliacion_total", "aud_conciliacion_parcial",
      "aud_sin_conciliacion", "durac_fijada_realizada", "promedio_asistentes",
      "promedio_duracion_minutos", "mediana_duracion_minutos",
      "promedio_duracion_bloques", "aud_videofilm_mes", "aud_matfam",
      "aud_matciv", "aud_matlab", "aud_matpen")
  colnames(BD_df_diseminacion) <- BD_AUDIC_encabezado
  write.table(BD_df_diseminacion, file= "~/JUSTAT/BD/diseminacion/BD_AUDIC.csv",
              sep = ",", row.names = F, col.names = F, append = T)

}




