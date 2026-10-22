# CSV → Kafka → gráfico en R → difusión

Este proyecto usa Kafka como intermediario entre tres tipos de procesos:

```text
procesar_kafka.R ──datos-csv──> generar_grafico.R ──graficos──> ver_graficos.R (visor A)
                                                    └──────────> ver_graficos.R (visor B)
```

`procesar_kafka.R` lee `datos/dataset.csv` y publica sus filas como un mensaje JSON en `datos-csv`. `generar_grafico.R` consume ese mensaje, crea el gráfico con `ggplot2` y publica la imagen PNG codificada en Base64 en `graficos`. Cada visor lee esa imagen y guarda su propia copia en `salidas/`. Los visores tienen **grupos de consumidores distintos**, por eso ambos reciben el mismo mensaje. Dos instancias dentro de un mismo grupo se repartirían las particiones y no demostrarían difusión.

## Ejecutar sin instalar R en Windows

Solo hace falta Docker Desktop en ejecución. Docker construye una imagen con R y los paquetes `httr2`, `jsonlite`, `ggplot2` y `base64enc`; la primera construcción descarga estas dependencias y puede tardar varios minutos. Abrir una terminal en esta carpeta (la que contiene `docker-compose.yml`) y ejecutar en este orden:

```powershell
docker compose up -d
docker compose build productor
docker compose run --rm productor demo-1
docker compose run --rm generador demo-1
docker compose run --rm visor A demo-1
docker compose run --rm visor B demo-1
```

Los servicios `productor`, `generador` y `visor` se ejecutan solo cuando se los invoca con `docker compose run`. Los archivos de `salidas/` quedan guardados en esta carpeta de Windows porque se comparte con el contenedor. Dentro de Docker, los scripts se conectan al REST Proxy por `http://rest-proxy:8082`.

## Alternativa: R instalado en Windows

Si preferís ejecutar los scripts directamente, instalá R y luego estos paquetes desde R:

```r
install.packages(c("httr2", "jsonlite", "ggplot2", "base64enc"))
```

Con Docker Desktop en ejecución, abrí una terminal en esta carpeta y ejecutá:

```powershell
docker compose up -d
Rscript procesar_kafka.R demo-1
Rscript generar_grafico.R demo-1
Rscript ver_graficos.R A demo-1
Rscript ver_graficos.R B demo-1
```

Se obtienen `salidas/grafico_A_demo-1.png` y `salidas/grafico_B_demo-1.png`. En PowerShell se puede comprobar que son iguales:

```powershell
Get-FileHash salidas/grafico_A_demo-1.png, salidas/grafico_B_demo-1.png
```

Para repetir la demostración, usar otro ID en **los cuatro comandos** (por ejemplo, `demo-2`). El ID viaja como clave de ambos mensajes y permite que cada proceso elija el mensaje correspondiente. Los grupos `generador_graficos`, `visor_A` y `visor_B` conservan sus posiciones de lectura por separado; por eso reutilizar el mismo ID no es una forma de volver a leerlo.

Los scripts se comunican con Kafka por el REST Proxy en `http://localhost:8082`; el broker Kafka escucha en el puerto `9092`. El archivo CSV solo lo lee el productor. La imagen se genera en el consumidor de `datos-csv`, no en el productor. Este ejemplo usa una sola partición, suficiente para difusión entre grupos, aunque no para repartir trabajo entre varios consumidores del mismo grupo.
