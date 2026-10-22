library(httr2)

REST_URL <- Sys.getenv("KAFKA_REST_URL", "http://localhost:8082")

validar_id <- function(valor, nombre) {
  if (length(valor) != 1 || is.na(valor) || !grepl("^[A-Za-z0-9_-]+$", valor)) {
    stop(nombre, " debe contener solo letras, números, guiones o guiones bajos.")
  }
}

publicar_registro <- function(topico, clave, valor) {
  respuesta <- request(paste0(REST_URL, "/topics/", topico)) |>
    req_headers("Content-Type" = "application/vnd.kafka.json.v2+json") |>
    req_body_json(list(records = list(list(key = clave, value = valor)))) |>
    req_perform()

  resultado <- resp_body_json(respuesta, simplifyVector = FALSE)
  if (length(resultado$offsets) != 1 || !is.null(resultado$offsets[[1]]$error_code)) {
    stop("Kafka no confirmó la publicación en ", topico, ".")
  }
  invisible(resultado$offsets[[1]])
}

crear_consumidor <- function(grupo, topico) {
  instancia <- paste0("lector_", Sys.getpid(), "_", as.integer(Sys.time()))
  respuesta <- request(paste0(REST_URL, "/consumers/", grupo)) |>
    req_headers("Content-Type" = "application/vnd.kafka.v2+json") |>
    req_body_json(list(
      name = instancia,
      format = "json",
      `auto.offset.reset` = "earliest",
      `auto.commit.enable` = "false"
    )) |>
    req_perform()

  instancia <- resp_body_json(respuesta)$instance_id
  base <- paste0(REST_URL, "/consumers/", grupo, "/instances/", instancia)
  tryCatch({
    request(paste0(base, "/subscription")) |>
      req_headers("Content-Type" = "application/vnd.kafka.v2+json") |>
      req_body_json(list(topics = list(topico))) |>
      req_perform()
  }, error = function(e) {
    try(cerrar_consumidor(base), silent = TRUE)
    stop(e)
  })
  base
}

esperar_registro <- function(base, clave, max_intentos = 20) {
  for (intento in seq_len(max_intentos)) {
    respuesta <- request(paste0(base, "/records?timeout=3000")) |>
      req_headers("Accept" = "application/vnd.kafka.json.v2+json") |>
      req_perform()
    registros <- resp_body_json(respuesta, simplifyVector = FALSE)
    for (registro in registros) {
      if (identical(registro$key, clave)) return(registro)
    }
  }
  stop("No se encontró un mensaje con ID '", clave, "'. Verificá el orden de ejecución.")
}

confirmar_registro <- function(base, registro) {
  # Kafka guarda la posición del próximo mensaje que debe leer este grupo.
  request(paste0(base, "/offsets")) |>
    req_headers("Content-Type" = "application/vnd.kafka.v2+json") |>
    req_body_json(list(offsets = list(list(
      topic = registro$topic,
      partition = registro$partition,
      offset = registro$offset + 1
    )))) |>
    req_perform()
  invisible(NULL)
}

cerrar_consumidor <- function(base) {
  request(base) |>
    req_method("DELETE") |>
    req_headers("Content-Type" = "application/vnd.kafka.v2+json") |>
    req_perform()
  invisible(NULL)
}
