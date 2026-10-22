FROM rocker/r-ver:4.4.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libcurl4-openssl-dev libssl-dev libpng-dev fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

RUN Rscript -e 'paquetes <- c("httr2", "jsonlite", "ggplot2", "base64enc"); install.packages(paquetes, repos = "https://cloud.r-project.org"); stopifnot(all(vapply(paquetes, requireNamespace, logical(1), quietly = TRUE)))'

WORKDIR /app
