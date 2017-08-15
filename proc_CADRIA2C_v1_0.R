# JUSTAT is a free software developed for official statistics of Supreme Court
# of Entre Ríos (STJER), Argentina, Office of Planification Management and
# Statistics (APGE)

# V.1.0
# 13-06-17

# Authors
# JUSTAT: Lic. Sebastián Castillo, Ing. Federico Caballaro y Lic. Paolo Orundes


############################ proc_CADRIA2C  ###################################

## Detalle del proceso

# 1 Abrir conexión con BD_adm_ingresos
# 2 Listar tablas para procesar CADRIA1C
# 3 Procesar Indicadores Estadísticos
# 4 Guardar en BD_CADRIA1C

## imput: BD_adm_ingresos
## output: BD_CADRIA1C actualizada
## Periodicidad: Diaria

# Librerías
library(data.table)
library(lubridate)
library(stringr)
library(purrr)
library(stringdist)
library(dplyr)


# Abre bases de datos -----------------------------------------------

proc_CADRIA2C <- function() {
  BD_adm_ingresos <- fread("~/JUSTAT/BD/BD_adm_ingresos.csv")
  BD_CADRIA2C <- fread("~/JUSTAT/BD/BD_CADRIA2C.csv")
  ultimo_procesado <- BD_CADRIA2C$id_archivos[which.max(BD_CADRIA2C$id_archivos)]


  BD_adm_ingresos <- BD_adm_ingresos[BD_adm_ingresos$id_archivos >
                                     ultimo_procesado &
                                     BD_adm_ingresos$id_operacion == "CADRIA2C" &
                                     BD_adm_ingresos$rec_estado == "admitido", ]

  lista_tbs_prim_CADRIA2C <- map(BD_adm_ingresos$ruta,
                                 fread, encoding = "Latin-1", na.strings = "" )

  # Asigna nombre a las variables para las tablas sin encabezado (ERROR002)
  for (i in seq_along(lista_tbs_prim_CADRIA2C)) {
    if (colnames(lista_tbs_prim_CADRIA2C[[i]])[[1]] == "V1") {
      colnames(lista_tbs_prim_CADRIA2C[[i]]) <- try(c("nro", "caratula", "tproc",
                                                  "as", "ccon", "finicio",
                                                  "ordv", "fdesp1", "feg1",
                                                  "fdesp2", "feg2", "fdesp3",
                                                  "feg3", "fvenc1", "fvenc2",
                                                  "fres", "tres", "mat", "jact"))
      } else {
      next()
    }
  }

  # Preparar tbs primarias: Asignar clase a columnas fechas, Agregar columna de
  # mes de dictado de resolución. Convertir a mayúsculas as

  for (i in seq_along(lista_tbs_prim_CADRIA2C)) {
    lista_tbs_prim_CADRIA2C[[i]]$finicio <-
      as.Date(lista_tbs_prim_CADRIA2C[[i]]$finicio, format="%d/%m/%Y")
    lista_tbs_prim_CADRIA2C[[i]]$fdesp1 <-
      as.Date(lista_tbs_prim_CADRIA2C[[i]]$fdesp1, format="%d/%m/%Y")
    lista_tbs_prim_CADRIA2C[[i]]$fvenc1 <-
      as.Date(lista_tbs_prim_CADRIA2C[[i]]$fvenc1, format="%d/%m/%Y")
    lista_tbs_prim_CADRIA2C[[i]]$fvenc2 <-
      as.Date(lista_tbs_prim_CADRIA2C[[i]]$fvenc2, format="%d/%m/%Y")
    lista_tbs_prim_CADRIA2C[[i]]$fres <-
      as.Date(lista_tbs_prim_CADRIA2C[[i]]$fres, format="%d/%m/%Y")
    lista_tbs_prim_CADRIA2C[[i]]$mes_res <-
      month(lista_tbs_prim_CADRIA2C[[i]]$fres)
    lista_tbs_prim_CADRIA2C[[i]]$as <-
      toupper(lista_tbs_prim_CADRIA2C[[i]]$as)
    lista_tbs_prim_CADRIA2C[[i]]$jact <-
      toupper(lista_tbs_prim_CADRIA2C[[i]]$jact)
  }


  # Creación de funciones
  p_causasadespacho <- function() {
    # Suma causas con resolución en el mes que se informa y causas que
    # quedan desp
    causas_adespacho <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                              BD_adm_ingresos$id_periodo[[i]] |
                              is.na(lista_tbs_prim_CADRIA2C[[i]]$mes_res))
    causas_adespacho
  }

  p_causasresueltas <- function() {
    # Suma causas con resolución en el mes que se informa que no sean medidas
    # para mejor proveer
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    causas_resueltas <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                              BD_adm_ingresos$id_periodo[[i]] &
                            no_mmp, na.rm = T)
    causas_resueltas
  }

  p_resxsentencia <- function() {
    # Suma sentencias
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xsentencia <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                            BD_adm_ingresos$id_periodo[[i]] &
                            lista_tbs_prim_CADRIA2C[[i]]$as == "S" &
                            no_mmp, na.rm = T)
    res_xsentencia
  }

  p_resxauto <- function() {
    # Suma autos
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xauto <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                       BD_adm_ingresos$id_periodo[[i]] &
                       lista_tbs_prim_CADRIA2C[[i]]$as == "A" &
                       no_mmp, na.rm = T)
    res_xauto
  }

  p_resxindeter <- function() {
    # Suma resoluciones indeterminadas
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xindeter <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                          BD_adm_ingresos$id_periodo[[i]] &
                          !lista_tbs_prim_CADRIA2C[[i]]$as %in% c("A", "S") &
                          no_mmp, na.rm = T)
    res_xindeter
  }

  p_res_atermino <- function() {
    # Suma resoluciones con fecha menor o igual a la fecha de 1er. venc que
    # no sean medidas para mejor proveer
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_atermino <- sum(lista_tbs_prim_CADRIA2C[[i]]$fres <=
                          lista_tbs_prim_CADRIA2C[[i]]$fvenc1 &
                          lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                          BD_adm_ingresos$id_periodo[[i]] &
                          no_mmp, na.rm = T)
    res_atermino
  }

  p_res_luego1venc <- function() {
    # Suma resoluciones con fecha mayor a la de 1er. vencimiento y menor a la
    # del segundo sin medidas para mejor proveer
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_luego1venc <- sum(lista_tbs_prim_CADRIA2C[[i]]$fres >
                            lista_tbs_prim_CADRIA2C[[i]]$fvenc1 &
                            lista_tbs_prim_CADRIA2C[[i]]$fres <
                            lista_tbs_prim_CADRIA2C[[i]]$fvenc2 &
                            lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                            BD_adm_ingresos$id_periodo[[i]] &
                            no_mmp, na.rm = T)
    res_luego1venc
  }

  p_res_luego2venc <- function() {
    # Suma resoluciones con fecha mayor a la del 2do. vencimiento que no son
    # medida para mejor proveer
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_luego2venc <-sum(lista_tbs_prim_CADRIA2C[[i]]$fres >
                           lista_tbs_prim_CADRIA2C[[i]]$fvenc2 &
                           lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                           BD_adm_ingresos$id_periodo[[i]] &
                           no_mmp, na.rm = T)
    res_luego2venc
  }

  p_prom_durac_inic_sent <- function() {
    # Duración de Procesos desde Fecha de Inicio hasta Sentencia: PROMEDIO
    # en días corridos
    # ADVERTENCIA: dada esta medición quedan excluidos del indicador de
    # duración los procesos sucesorios y ejecutivos
    indice_sent <- lista_tbs_prim_CADRIA2C[[i]]$as == "S" &
      lista_tbs_prim_CADRIA2C[[i]]$mes_res == BD_adm_ingresos$id_periodo[[i]]
    durac_inic_sent <-
      mean(as.integer(lista_tbs_prim_CADRIA2C[[i]]$fres[indice_sent] -
                        lista_tbs_prim_CADRIA2C[[i]]$finicio[indice_sent]),
           na.rm = T)
    durac_inic_sent
  }

  p_durac_inic_sent_xproc <- function() {
    # Duración de Procesos desde Fecha de Inicio hasta Sentencia en días
    # corridos.
    # ADVERTENCIA: dada esta medición quedan excluidos del indicador de
    # duración los procesos sucesorios y ejecutivos
    indice_sent <- lista_tbs_prim_CADRIA2C[[i]]$as == "S" &
      lista_tbs_prim_CADRIA2C[[i]]$mes_res == BD_adm_ingresos$id_periodo[[i]]
    # Vector tipo de proceso
    vtproc <- lista_tbs_prim_CADRIA2C[[i]]$tproc[indice_sent]
    # Vector duracion
    vdurac <- lista_tbs_prim_CADRIA2C[[i]]$fres[indice_sent] -
      lista_tbs_prim_CADRIA2C[[i]]$finicio[indice_sent]
    # Vecto combinado
    vcomb <- str_c(vtproc, vdurac, sep = "&")
    vcomb <- noquote(vcomb[!is.na(vcomb)])
    durac_inic_sent_xproc <- str_c(vcomb, collapse = "#")
    if (is_empty(durac_inic_sent_xproc)) {
      durac_inic_sent_xproc <- NA
    } else {
      durac_inic_sent_xproc
    }
    durac_inic_sent_xproc
  }

  p_ccausa_ant <- function() {
    # Causa informada con fecha de despacho más antigua: Caratula
    if (length(lista_tbs_prim_CADRIA2C[[i]]$fdesp1 > 0)) {
      ccausa_ant <-
        lista_tbs_prim_CADRIA2C[[i]][[which.min(lista_tbs_prim_CADRIA2C[[i]]$fdesp1),
                                      "caratula"]]
      ccausa_ant
    } else {
      NA
    }
  }

  p_fcausa_ant <- function() {
    # Causa informada con fecha de despacho más antigua: Fecha
    if (length(lista_tbs_prim_CADRIA2C[[i]]$fdesp1 > 0)) {
      fcausa_ant <-
        lista_tbs_prim_CADRIA2C[[i]][[which.min(lista_tbs_prim_CADRIA2C[[i]]$fdesp1),
                                      "fdesp1"]]
      fcausa_ant <- as.Date(fcausa_ant)
      fcausa_ant
    } else {
      NA
    }
  }

  # Resolución por Tipo y Tipo+Juez

  p_tres_confirmacion <- function() {
    # Suma resoluciones
    tres_confirmacion <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                            BD_adm_ingresos$id_periodo[[i]] &
                            lista_tbs_prim_CADRIA2C[[i]]$tres == 1, na.rm = T)
    tres_confirmacion
  }

  p_tres_confirmacion_juez <- function() {
    indice_tres1 <- lista_tbs_prim_CADRIA2C[[i]]$tres == 1
    indice_jueztres1 <- lista_tbs_prim_CADRIA2C[[i]]$jact[indice_tres1]
    tres_confirmacion_juez <- str_c(indice_jueztres1[!is.na(indice_jueztres1)],
                                    collapse = "&")
    if (is_empty(tres_confirmacion_juez)) {
      tres_confirmacion_juez <- NA
    } else {
      tres_confirmacion_juez
    }
    tres_confirmacion_juez
  }

  p_tres_revocacion <- function() {
    # Suma resoluciones
    tres_revocacion <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                               BD_adm_ingresos$id_periodo[[i]] &
                               lista_tbs_prim_CADRIA2C[[i]]$tres == 2, na.rm = T)
    tres_revocacion
  }

  p_tres_revocacion_juez <- function() {
    indice_tres2 <- lista_tbs_prim_CADRIA2C[[i]]$tres == 2
    indice_jueztres2 <- lista_tbs_prim_CADRIA2C[[i]]$jact[indice_tres2]
    tres_revocacion_juez <- str_c(indice_jueztres2[!is.na(indice_jueztres2)],
                                    collapse = "&")
    if (is_empty(tres_revocacion_juez)) {
      tres_revocacion_juez <- NA
    } else {
      tres_revocacion_juez
    }

    tres_revocacion_juez
  }


  p_tres_revoc_parcial <- function() {
    # Suma resoluciones
    tres_revoc_parcial <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                             BD_adm_ingresos$id_periodo[[i]] &
                             lista_tbs_prim_CADRIA2C[[i]]$tres == 3, na.rm = T)
    tres_revoc_parcial
  }

  p_tres_revoc_parcial_juez <- function() {
    indice_tres3 <- lista_tbs_prim_CADRIA2C[[i]]$tres == 3
    indice_jueztres3 <- lista_tbs_prim_CADRIA2C[[i]]$jact[indice_tres3]
    tres_revoc_parcial_juez <- str_c(indice_jueztres3[!is.na(indice_jueztres3)],
                                  collapse = "&")
    if (is_empty(tres_revoc_parcial_juez)) {
      tres_revoc_parcial_juez <- NA
    } else {
      tres_revoc_parcial_juez
    }
    tres_revoc_parcial_juez
  }

  p_tres_nulidad <- function() {
    # Suma resoluciones
    tres_nulidad <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                                BD_adm_ingresos$id_periodo[[i]] &
                                lista_tbs_prim_CADRIA2C[[i]]$tres == 4, na.rm = T)
    tres_nulidad
  }

  p_tres_nulidad_juez <- function() {
    indice_tres4 <- lista_tbs_prim_CADRIA2C[[i]]$tres == 4
    indice_jueztres4 <- lista_tbs_prim_CADRIA2C[[i]]$jact[indice_tres4]
    tres_nulidad_juez <- str_c(indice_jueztres4[!is.na(indice_jueztres4)],
                                     collapse = "&")
    if (is_empty(tres_nulidad_juez)) {
      tres_nulidad_juez <- NA
    } else {
      tres_nulidad_juez
    }
    tres_nulidad_juez
  }

  # Resoluciones por Tipo (simples)
  p_tres_concedeRIL <- function() {
    # Suma resoluciones
    tres_concedeRIL <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                          BD_adm_ingresos$id_periodo[[i]] &
                          lista_tbs_prim_CADRIA2C[[i]]$tres == 5, na.rm = T)
    tres_concedeRIL
  }

  p_tres_deniegaRIL <- function() {
    # Suma resoluciones
    tres_deniegaRIL <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                             BD_adm_ingresos$id_periodo[[i]] &
                             lista_tbs_prim_CADRIA2C[[i]]$tres == 6, na.rm = T)
    tres_deniegaRIL
  }

  p_tres_concedeQJA <- function() {
    # Suma resoluciones
    tres_concedeQJA <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                             BD_adm_ingresos$id_periodo[[i]] &
                             lista_tbs_prim_CADRIA2C[[i]]$tres == 7, na.rm = T)
    tres_concedeQJA
  }

  p_tres_deniegaQJA <- function() {
    # Suma resoluciones
    tres_deniegaQJA <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                             BD_adm_ingresos$id_periodo[[i]] &
                             lista_tbs_prim_CADRIA2C[[i]]$tres == 8, na.rm = T)
    tres_deniegaQJA
  }

  p_tres_otra <- function() {
    # Suma resoluciones
    tres_otra <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                             BD_adm_ingresos$id_periodo[[i]] &
                             lista_tbs_prim_CADRIA2C[[i]]$tres == 9, na.rm = T)
    tres_otra
  }

  # A despacho y Resoluciones por Materia
  p_adespxmat_cco <- function() {
    # Suma causas con resolución en el mes que se informa y causas que
    # quedan desp
    causas_adespxmat_cco <- sum((lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                              BD_adm_ingresos$id_periodo[[i]] |
                              is.na(lista_tbs_prim_CADRIA2C[[i]]$mes_res)) &
                              lista_tbs_prim_CADRIA2C[[i]]$mat == 1, na.rm = T)
    causas_adespxmat_cco

  }

  p_resxmat_cco <- function() {
    # Suma sentencias
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xmat_cco <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                            BD_adm_ingresos$id_periodo[[i]] &
                            lista_tbs_prim_CADRIA2C[[i]]$mat == 1 &
                            no_mmp, na.rm = T)
    res_xmat_cco
  }


  p_adespxmat_fam <- function() {
    # Suma causas con resolución en el mes que se informa y causas que
    # quedan desp
    causas_adespxmat_fam <- sum((lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                                   BD_adm_ingresos$id_periodo[[i]] |
                                   is.na(lista_tbs_prim_CADRIA2C[[i]]$mes_res)) &
                                  lista_tbs_prim_CADRIA2C[[i]]$mat == 2, na.rm = T)
    causas_adespxmat_fam

  }

  p_resxmat_fam <- function() {
    # Suma sentencias
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xmat_fam <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                          BD_adm_ingresos$id_periodo[[i]] &
                          lista_tbs_prim_CADRIA2C[[i]]$mat == 2 &
                          no_mmp, na.rm = T)
    res_xmat_fam
  }

  p_adespxmat_ot <- function() {
    # Suma causas con resolución en el mes que se informa y causas que
    # quedan desp
    causas_adespxmat_ot <- sum((lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                                   BD_adm_ingresos$id_periodo[[i]] |
                                   is.na(lista_tbs_prim_CADRIA2C[[i]]$mes_res)) &
                                  lista_tbs_prim_CADRIA2C[[i]]$mat == 3, na.rm = T)
    causas_adespxmat_ot

  }

  p_resxmat_ot <- function() {
    # Suma sentencias
    no_mmp <- lista_tbs_prim_CADRIA2C[[i]]$tres != 0 |
      is.na(lista_tbs_prim_CADRIA2C[[i]]$tres)
    res_xmat_ot <- sum(lista_tbs_prim_CADRIA2C[[i]]$mes_res ==
                          BD_adm_ingresos$id_periodo[[i]] &
                          lista_tbs_prim_CADRIA2C[[i]]$mat == 3 &
                          no_mmp, na.rm = T)
    res_xmat_ot
  }

  # crear vectores
  causas_adespacho <- vector()
  causas_resueltas <- vector()
  res_xsentencia <- vector()
  res_xauto <- vector()
  res_xindeter <- vector()
  res_atermino <- vector()
  res_luego1venc <- vector()
  res_luego2venc <- vector()
  prom_durac_inic_sent <- vector()
  durac_inic_sent_xproc <- vector()
  ccausa_ant <- vector()
  fcausa_ant <- vector()
  tres_confirmacion <- vector()
  tres_confirmacion_juez <- vector()
  tres_revocacion <- vector()
  tres_revocacion_juez <- vector()
  tres_revoc_parcial <- vector()
  tres_revoc_parcial_juez <- vector()
  tres_nulidad <- vector()
  tres_nulidad_juez <- vector()
  tres_concedeRIL <- vector()
  tres_deniegaRIL <- vector()
  tres_concedeQJA <- vector()
  tres_deniegaQJA <- vector()
  tres_otra <- vector()
  causas_adespxmat_cco <- vector()
  res_xmat_cco <- vector()
  causas_adespxmat_fam <- vector()
  res_xmat_fam <- vector()
  causas_adespxmat_ot <- vector()
  res_xmat_ot <- vector()

  # Extracción de indicadores estadísticos.Con "try" para devolver NA ante
  # falla
  for (i in seq_along(lista_tbs_prim_CADRIA2C)) {
    causas_adespacho[[i]]  <- try(p_causasadespacho(), silent = TRUE)
    causas_adespacho
    causas_resueltas[[i]] <- try(p_causasresueltas(), silent = TRUE)
    causas_resueltas
    res_xsentencia[[i]] <- try(p_resxsentencia(), silent = TRUE)
    res_xsentencia
    res_xauto[[i]] <- try(p_resxauto(), silent = TRUE)
    res_xauto
    res_xindeter[[i]] <- try(p_resxindeter(), silent = TRUE)
    res_xindeter
    res_atermino[[i]] <- try(p_res_atermino(), silent = TRUE)
    res_atermino
    res_luego1venc[[i]] <- try(p_res_luego1venc(), silent = TRUE)
    res_luego1venc
    res_luego2venc[[i]] <- try(p_res_luego2venc(), silent = TRUE)
    res_luego2venc
    prom_durac_inic_sent[[i]] <- try(p_prom_durac_inic_sent(),
                                     silent = TRUE)
    prom_durac_inic_sent
    durac_inic_sent_xproc[[i]] <- try(p_durac_inic_sent_xproc(),
                                      silent = TRUE)
    durac_inic_sent_xproc
    ccausa_ant[[i]] <- try(p_ccausa_ant(), silent = TRUE)
    ccausa_ant
    fcausa_ant[[i]] <- try(p_fcausa_ant(), silent = TRUE)
    fcausa_ant
    tres_confirmacion[[i]] <- try(p_tres_confirmacion(), silent = TRUE)
    tres_confirmacion
    tres_confirmacion_juez[[i]] <- try(p_tres_confirmacion_juez(), silent = TRUE)
    tres_confirmacion_juez
    tres_revocacion[[i]] <- try(p_tres_revocacion(), silent = TRUE)
    tres_revocacion
    tres_revocacion_juez[[i]] <- try(p_tres_revocacion_juez(), silent = TRUE)
    tres_revocacion_juez
    tres_revoc_parcial[[i]] <- try(p_tres_revoc_parcial(), silent = TRUE)
    tres_revoc_parcial
    tres_revoc_parcial_juez[[i]] <- try(p_tres_revoc_parcial_juez(), silent = TRUE)
    tres_revoc_parcial_juez
    tres_nulidad[[i]] <- try(p_tres_nulidad(), silent = TRUE)
    tres_nulidad
    tres_nulidad_juez[[i]] <- try(p_tres_nulidad(), silent = TRUE)
    tres_nulidad_juez
    tres_concedeRIL[[i]] <- try(p_tres_concedeRIL(), silent = TRUE)
    tres_concedeRIL
    tres_deniegaRIL[[i]] <- try(p_tres_deniegaRIL(), silent = TRUE)
    tres_deniegaRIL
    tres_concedeQJA[[i]] <- try(p_tres_concedeQJA(), silent = TRUE)
    tres_concedeQJA
    tres_deniegaQJA[[i]] <- try(p_tres_deniegaQJA(), silent = TRUE)
    tres_deniegaQJA
    tres_otra[[i]] <- try(p_tres_otra(), silent = TRUE)
    tres_otra
    causas_adespxmat_cco[[i]] <- try(p_adespxmat_cco(), silent = TRUE)
    causas_adespxmat_cco
    res_xmat_cco[[i]] <- try(p_resxmat_cco(), silent = TRUE)
    res_xmat_cco
    causas_adespxmat_fam[[i]] <- try(p_adespxmat_fam(), silent = TRUE)
    causas_adespxmat_fam
    res_xmat_fam[[i]] <- try(p_resxmat_fam(), silent = TRUE)
    res_xmat_fam
    causas_adespxmat_ot[[i]] <- try(p_adespxmat_ot(), silent = TRUE)
    causas_adespxmat_ot
    res_xmat_ot[[i]] <- try(p_resxmat_ot(), silent = TRUE)
    res_xmat_ot
  }

  # Arma BD_CADRIA1C-------------------------------------------------------

  # Recupera información de BD_adm_ingresos
  df_adm_ingresos <- BD_adm_ingresos[ , c(1,2,10,9,11,14,15,16,17)]
  # Asigna nombre y clase a columnas específicas
  colnames(df_adm_ingresos)[7:9] <- c("causas_inic_ppales",
                                      "causas_inic_noppales",
                                      "causas_archivadas")
  df_adm_ingresos$causas_inic_ppales <-
    as.integer(df_adm_ingresos$causas_inic_ppales)
  df_adm_ingresos$causas_inic_noppales <-
    as.integer(df_adm_ingresos$causas_inic_noppales)
  df_adm_ingresos$causas_archivadas <-
    as.integer(df_adm_ingresos$causas_archivadas)

  # Agrega variables activo y calidad
  df_adm_ingresos$activo <- TRUE
  df_adm_ingresos$calidad <- NA

  # Arma df indicadores procesados
  df <- data.frame(
    causas_adespacho,
    causas_resueltas,
    res_xsentencia,
    res_xauto,
    res_xindeter,
    res_atermino,
    res_luego1venc,
    res_luego2venc,
    prom_durac_inic_sent,
    durac_inic_sent_xproc,
    ccausa_ant,
    fcausa_ant,
    tres_confirmacion,
    tres_confirmacion_juez,
    tres_revocacion,
    tres_revocacion_juez,
    tres_revoc_parcial,
    tres_revoc_parcial_juez,
    tres_nulidad,
    tres_nulidad_juez,
    tres_concedeRIL,
    tres_deniegaRIL,
    tres_concedeQJA,
    tres_deniegaQJA,
    tres_otra,
    causas_adespxmat_cco,
    res_xmat_cco,
    causas_adespxmat_fam,
    res_xmat_fam,
    causas_adespxmat_ot,
    res_xmat_ot)


  # formateando fecha de causa más antigua
  df$fcausa_ant <- as.Date(df$fcausa_ant, origin="1970-01-01")

  # Combina df para agregar a BD
  BD_dfs <- bind_cols(df_adm_ingresos, df)

  # Reordena
  BD_dfs <- BD_dfs[ , c(1:6,10,11,7,8,9,12,37,39,41,13,38,40,42,14:21,24:36,22,23)]

  # Evalúa calidad del informe presentado en base a consistencia de indicadores
  # y la presentación de datos. Sigue las siguientes reglas
    # 1: Causas resueltas <= Causas a despacho
    # 2: Causas resueltas == resueltas x S + x A + x indeter
    # 3: Causas resoluestas == resueltas a termino + luego1v + luego2v
        # Como en estas columnas se presentan NA a veces no se puede evaluar la
        # la función de procesamiento
    # 4: 0 < Causas Iniciadas (ppales + no ppales) + causas archivadas
  # Máximo puntaje 4, mínimo 1.

  for (i in seq_along(BD_dfs$calidad)) {
    regla1 <- BD_dfs$causas_resueltas[[i]] <= BD_dfs$causas_adespacho[[i]]
    regla2 <- BD_dfs$causas_resueltas[[i]] == (BD_dfs$res_xsentencia[[i]] +
                                            BD_dfs$res_xauto[[i]] +
                                            BD_dfs$res_xindeter[[i]])
    regla3 <- BD_dfs$causas_resueltas[[i]] == (BD_dfs$res_atermino[[i]] +
                                            BD_dfs$res_luego1venc[[i]] +
                                            BD_dfs$res_luego2venc[[i]])
    regla4 <- 0 < (BD_dfs$causas_inic_ppales[[i]] +
                     BD_dfs$causas_inic_noppales[[i]] +
                     BD_dfs$causas_archivadas[[i]])
    BD_dfs$calidad[[i]] <- sum(regla1 + regla2 + regla3 + regla4, na.rm = T)
    BD_dfs$calidad[[i]]
  }

  # Guarda datos del Procesamiento--------------------------------------
  # En BD_CADRIA2C
  write.table(BD_dfs, file= "~/JUSTAT/BD/BD_CADRIA2C.csv",
                sep = ",", row.names = F, col.names = F, append = T)

  # En BD/diseminacion
  BD_df_diseminacion <- BD_dfs[, -c(6,7,8)]
  BD_CADRIA2C_encabezado <-
    c("nro_registro", "organismo", "anio", "mes", "materia",
      "causas_inic_ppales", "causas_inic_noppales", "causas_archivadas",
      "causas_adespacho", "causas_adesp_civiles", "causas_adesp_fam",
      "causas_adesp_ot", "causas_resueltas", "res_mat_civil", "res_mat_fam",
      "res_mat_ot", "res_xsentencia", "res_xauto", "res_xotra", "res_antes1venc",
      "res_luego1venc", "res_luego2venc", "promedio_durac_inic_sent",
      "durac_inic_sent_procesos", "Confirmaciones", "Confirmacion_xjuez",
      "Revocaciones", "Revocaciones_xjuez", "Revoc_Parciales", "RevocP_xjuez",
      "Nulidades", "Nulidades_xjuez", "ConcedeRIL", "DeniegaRIL", "concedeQJA",
      "DeniegaQJA", "Otras_Res", "causa_masantigua_caratula",
      "causa_masantigua_fecgha")
  colnames(BD_df_diseminacion) <- BD_CADRIA2C_encabezado
  write.table(BD_df_diseminacion, file= "~/JUSTAT/BD/diseminacion/BD_CADRIA2C.csv",
              sep = ",", row.names = F, col.names = F, append = T)


  # Causas a despacho para el Boletin Oficial-----------------------------

  # Lista de causas a despacho

  lista_adespacho <- list()
  for (i in seq_along(lista_tbs_prim_CADRIA2C)) {
    lista_adespacho[[i]] <-
      lista_tbs_prim_CADRIA2C[[i]][is.na(lista_tbs_prim_CADRIA2C[[i]]$fres),
                                   c(2,8)]
    lista_adespacho[[i]]
  }

  for (i in seq_along(lista_adespacho)) {
    lista_adespacho[[i]]$mes_informado <- BD_adm_ingresos$id_periodo[[i]]
    lista_adespacho[[i]]
  }

  names(lista_adespacho) <-
    BD_adm_ingresos$id_organismo

  # Df de causas a despacho para envío al Boletin Oficial
  BD_adespacho <- bind_rows(lista_adespacho, .id = "id")


  # Registra en Base de datos
  write.table(BD_adespacho, file= "~/JUSTAT/BD/BD_adespacho.csv",
                sep = ",", row.names = F, col.names = F, append = T)
}





