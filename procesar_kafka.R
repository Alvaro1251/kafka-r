source("kafka_rest.R")

args <- commandArgs(trailingOnly = TRUE)
run_id <- if (length(args) >= 1) args[[1]] else format(Sys.time(), "%Y%m%d-%H%M%S")
validar_id(run_id, "ID de ejecución")

ruta_csv <- "datos/dataset.csv"
if (!file.exists(ruta_csv)) {
  stop("No se encuentra datos/dataset.csv. Ejecutá el script desde la carpeta del proyecto.")
}

df <- read.csv(ruta_csv)
if (nrow(df) == 0) stop("El CSV no contiene filas.")

# El productor solo lee y publica los datos. El gráfico se crea en otro proceso.
datos_json <- as.character(jsonlite::toJSON(df, dataframe = "rows", na = "null"))
publicar_registro("datos-csv", run_id, datos_json)

cat("Publicado", nrow(df), "filas en datos-csv. ID:", run_id, "\n")
