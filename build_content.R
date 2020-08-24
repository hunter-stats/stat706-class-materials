all_rmds <- list.files(pattern = "\\.Rmd$")
all_htmls <- list.files(pattern = "\\.html$")

purrr::walk(all_rmds, rmarkdown::render)
