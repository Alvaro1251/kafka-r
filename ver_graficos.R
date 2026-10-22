source("kafka_rest.R")
library(base64enc)

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 2) {
    stop("Uso: Rscript ver_graficos.R <nombre-del-visor> <ID-de-ejecución>")
  }
  visor <- args[[1]]
  run_id <- args[[2]]
  validar_id(visor, "Nombre del visor")
  validar_id(run_id, "ID de ejecución")

  # Cada visor tiene su propio grupo: ambos reciben el mismo mensaje.
  grupo <- paste0("visor_", visor)
  base <- crear_consumidor(grupo, "graficos")
  on.exit(try(cerrar_consumidor(base), silent = TRUE), add = TRUE)

  mensaje <- esperar_registro(base, run_id)
  bytes <- base64decode(mensaje$value)
  dir.create("salidas", showWarnings = FALSE)
  archivo <- file.path("salidas", paste0("grafico_", visor, "_", run_id, ".png"))
  writeBin(bytes, archivo)

  confirmar_registro(base, mensaje)
  cat("Visor", visor, "(grupo", grupo, ") guardó:", normalizePath(archivo), "\n")
}

main()
