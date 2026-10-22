source("kafka_rest.R")
library(ggplot2)
library(base64enc)

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 1) stop("Uso: Rscript generar_grafico.R <ID-de-ejecución>")
  run_id <- args[[1]]
  validar_id(run_id, "ID de ejecución")

  grupo <- "generador_graficos"
  base <- crear_consumidor(grupo, "datos-csv")
  on.exit(try(cerrar_consumidor(base), silent = TRUE), add = TRUE)

  mensaje <- esperar_registro(base, run_id)
  df <- jsonlite::fromJSON(mensaje$value)
  columnas <- c("flipper_length_mm", "body_mass_g", "species")
  if (!is.data.frame(df) || !all(columnas %in% names(df))) {
    stop("El mensaje de datos-csv no tiene las columnas necesarias para el gráfico.")
  }
  df <- df[complete.cases(df[columnas]), ]
  if (nrow(df) == 0) stop("No hay filas completas para graficar.")

  grafico <- ggplot(df, aes(x = flipper_length_mm, y = body_mass_g, color = species)) +
    geom_point(size = 3, alpha = 0.8) +
    theme_minimal(base_size = 12) +
    labs(
      title = "Largo de Aleta vs Masa Corporal",
      subtitle = "Generado en R a partir de datos recibidos por Kafka",
      x = "Largo de aleta (mm)",
      y = "Masa corporal (g)",
      color = "Especie"
    )

  archivo_temp <- tempfile(fileext = ".png")
  on.exit(unlink(archivo_temp), add = TRUE)
  ggsave(archivo_temp, plot = grafico, width = 7, height = 4.5, dpi = 150)
  bytes <- readBin(archivo_temp, "raw", file.info(archivo_temp)$size)

  publicar_registro("graficos", run_id, base64encode(bytes))
  confirmar_registro(base, mensaje)
  cat("Gráfico generado desde datos-csv y publicado en graficos. ID:", run_id, "\n")
}

main()
