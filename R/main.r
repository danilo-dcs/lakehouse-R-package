#' LakehouseClient Class
#'
#' This class allows users to interact with the lakehouse infrastructure via the lakehouse API and other functionalities.
#'
#' @import arrow
#' @import httr
#' @import jsonlite
#' @import readxl
#' @import rvest
#' @import R6
#' @import stringr
#' @import dplyr
#' @import readr
#' @export
LakehouseClient <- R6::R6Class("LakehouseClient",
    private = list(
        lakehouse_url = NULL,
        user_id = NULL,
        user_role = NULL,
        user_email = NULL,
        access_token = NULL,
        access_token_type = NULL,
        file_load_path = "/var/tmp",

        file_chunk_generator = function(file_path, chunk_size = 1 * 1024 * 1024) {
            file <- file(file_path, "rb")
            on.exit(close(file))
            repeat {
                chunk <- readBin(file, "raw", chunk_size)
                if (length(chunk) == 0) break
                yield(chunk)
            }
        },
        format_size = function(bytes) {
            kb <- bytes / 1024
            if (kb < 1024) {
                size <- sprintf("%.2f KB", kb)
                return(size)
            } else {
                mb <- kb / 1024
                size <- sprintf("%.2f MB", mb)
                return(size)
            }
        },
        parse_query_args = function(args) {
            pattern <- "^\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*(=|>=|<=|>|<|\\*)\\s*(.+?)\\s*$"
            parsed_args <- list()

            for (arg in args) {
                if (!is.character(arg)) {
                    stop(sprintf("Arguments must be a string, got: %s", typeof(arg)))
                }

                match <- str_match(arg, pattern)
                if (any(is.na(match))) {
                    stop(sprintf("Invalid format: '%s'. Expected format: (KEY)(OPERATOR)(VALUE). Ex: 'collection_name=lakehouse'", arg))
                }

                parsed_args <- append(parsed_args, list(list(key = match[2], op = match[3], value = match[4])))
            }

            return(parsed_args)
        },

        format_output_dataframe = function(dataset) {
            rows_list <- lapply(seq_along(dataset[[1]]), function(i) {
                lapply(dataset, `[[`, i)
            })

            df <- do.call(rbind, lapply(rows_list, function(x) {
                x[sapply(x, is.null)] <- NA
                as.data.frame(x, stringsAsFactors = FALSE)
            }))

            if ("id" %in% names(df)) {
                cols <- c("id", setdiff(names(df), "id"))
                df <- df[, cols]
            }
            return(df)
        }
    ),
  
    public = list(    
        #' Initializes the lakehouse client
        #' @param lakehouse_url The url to the lakehouse api host
        #' @return NULL
        #' @export
        initialize = function(lakehouse_url) {
            domain <- gsub("^https?://", "", lakehouse_url)
            private$lakehouse_url <- paste0("http://", domain)
        },

        #' Authenticates the lakehouse client
        #' @param email A string containing the user email
        #' @param password A string containing the user password
        #' @return NULL
        #' @export
        auth = function(
            email,
            password
        ) {
            auth_route_sulfix <- "auth/login"
            auth_payload <- list(email = email, password = password)

            url <- paste0(private$lakehouse_url, "/", auth_route_sulfix)
            
            response <- httr::POST(
                url, 
                body = jsonlite::toJSON(auth_payload, auto_unbox = TRUE), 
                encode = "json", 
                config = httr::config(ssl_verifypeer = 0)
            )


            if (httr::status_code(response) != 200) {
                stop("Error: Failed to authenticate. HTTP status: ", httr::status_code(response))
            }

            text_response <- httr::content(response, as="text", encoding="UTF-8")

            response_content <- jsonlite::fromJSON(text_response)
            
            if (!is.null(response_content)) {
                private$user_id <- response_content$user_id
                private$user_role <- response_content$user_role
                private$access_token <- response_content$access_token
                private$access_token_type <- response_content$token_type
                private$user_email <- email
                return("Session Authenticated!")
            } else {
                return("Unable to authenticate!")
            }
        },

        #' Downloads the a file from the lakehouse storage to the local host
        #' @param catalog_file_id A string with the catalog id for the file record to be downloaded
        #' @param output_file_dir A string containing the output file directory
        #' @return A string with the path to the output file
        #' @export
        download_file = function(
            catalog_file_id, 
            output_file_dir
        ) {
            visa_ids <- sapply(private$user_visas, function(item) item$id)
            
            headers <- c(Authorization = paste("Bearer", private$access_token))
            url <- paste0(private$lakehouse_url, "/catalog/file/id/", catalog_file_id)

            response <- httr::GET(url, httr::add_headers(.headers=headers), config = httr::config(ssl_verifypeer = 0))

            if (httr::status_code(response) != 200) {
                stop("Unable to get download url")
            }

            catalog_item <- jsonlite::fromJSON(httr::content(response, as="text", encoding="UTF-8"))

            if (is.null(catalog_item)) {
                stop("Unable to fetch catalog item")
            }

            payload <- list(
                catalog_file_id = catalog_file_id
            )

            download_url <- paste0(private$lakehouse_url, "/storage/files/download-request")

            response <- httr::POST(
                download_url, 
                body = jsonlite::toJSON(payload, auto_unbox = TRUE), 
                httr::add_headers(.headers=headers), 
                httr::content_type_json(), 
                config = httr::config(ssl_verifypeer = 0)
            )

            if (httr::status_code(response) != 200) {
                stop("Unable to get download url")
            }

            response_content <- jsonlite::fromJSON(httr::content(response, "text", encoding="UTF-8"))

            signed_url <- response_content$download_url

            output_file_path <- paste0(output_file_dir, "/", catalog_item$file_name)
            
            response <- httr::GET(signed_url, stream = TRUE, config = httr::config(ssl_verifypeer = 0))
            
            if (httr::status_code(response) == 200) {
                writeBin(httr::content(response, "raw"), output_file_path)
            } else {
                stop("Failed to download file")
            }
            
            return(output_file_path)
        },

        #' Create a new collection of files
        #' @param storage_type A string containing the storage type ("gcs" for google cloud storage, "s3" for amazon s3, "hdfs" for hadoop hdfs)
        #' @param collection_name A string containing the new collection's name
        #' @param namenode_address An optional string containing the ip address or URL to the Hadoop namenode
        #' @param bucket_name An optional string containing the cloud bucket name
        #' @param collection_description An optional string containing additional description for this collection
        #' @param public Optional boolean value indicating whether the collection will be public (TRUE or FALSE)
        #' @param secret Optional boolean value indicating whether the collection will be secret (TRUE or FALSE)
        #' @return A dataset containing the collection's information in the catalog 
        #' @export
        create_collection = function(
            storage_type,
            collection_name,
            bucket_name,
            collection_description = NULL,
            public = NULL,
            secret = NULL
        ) {
            if (is.null(bucket_name) && is.null(namenode_address)) {
                stop("Unable to find the storage location. namenode_address or bucket_name must be specified")
            }

            visa_ids <- sapply(private$user_visas, function(item) item$id)

            payload <- list(
                storage_type = storage_type,
                collection_name = collection_name
            )

            if(storage_type == "hdfs"){
                payload[["namenode_address"]] <- bucket_name
            } else {
                payload[["bucket_name"]] <- bucket_name
            }

            optional_params <- list(
                collection_description = collection_description,
                public = public,
                secret = secret
            )

            for (name in names(optional_params)) {
                if (!is.null(optional_params[[name]])) {
                    payload[[name]] <- optional_params[[name]]
                }
            }

            payload <- payload[!sapply(payload, is.null)]

            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )

            url <- paste0(private$lakehouse_url, "/storage/collections/create")
            
            response <- httr::POST(url, body = jsonlite::toJSON(payload, auto_unbox = TRUE), encode = "json", httr::add_headers(.headers = headers), config = httr::config(ssl_verifypeer = 0))

            if (httr::status_code(response) != 200) {
                stop("Error: Failed to add collection. HTTP status: ", httr::status_code(response))
            }

            text_response <- httr::content(response, as="text", encoding="UTF-8")

            return(jsonlite::fromJSON(text_response))
        },

        #' Get a sctructured file as a R doataframe
        #' Condition: the file must be CSV, XLSX, TSV, JSON, MD, HTML, TEX or PARQUET. If the file record's 'file_category' property is marked as 'structured' in the catalogue, the file is can be converted into a dataframe. 
        #' @param catalogue_file_id A string with the catalog id for the file record to be downloaded
        #' @return A dataframe representing the file selected 
        #' @export
        get_dataframe = function(catalogue_file_id) {
            downloaded_file_path <- self$download_file(
                catalog_file_id = catalogue_file_id,
                output_file_dir = private$file_load_path
            )
            
            df <- NULL
            
            if (grepl("\\.csv$", downloaded_file_path)) {
                df <- read.csv(downloaded_file_path)
            } else if (grepl("\\.xlsx$|\\.xls$", downloaded_file_path)) {
                df <- readxl::read_excel(downloaded_file_path)
            } else if (grepl("\\.tsv$", downloaded_file_path)) {
                df <- read.csv(downloaded_file_path, sep = "\t")
            } else if (grepl("\\.json$", downloaded_file_path)) {
                df <- jsonlite::fromJSON(downloaded_file_path)
            } else if (grepl("\\.md$", downloaded_file_path)) {
                df <- read.csv(downloaded_file_path, sep = "|", skip = TRUE)
            } else if (grepl("\\.html$", downloaded_file_path)) {
                df_list <- rvest::read_html(downloaded_file_path) %>%
                rvest::html_table(fill = TRUE)
                df <- if (length(df_list) > 0) df_list[[1]] else NULL
            } else if (grepl("\\.parquet$", downloaded_file_path)) {
                df <- arrow::read_parquet(downloaded_file_path)
            } else {
                file_content <- paste(readLines(downloaded_file_path, warn = FALSE), collapse = "")

                pattern <- "[^/\\\\]+$"
                match <- regexpr(pattern, downloaded_file_path)
               
               if (match == -1) {
                    filename <- NA
                } else {
                    filename <- regmatches(downloaded_file_path, match)
                }

                df <- data.frame(
                    dataset_name = filename,
                    content = file_content,
                    stringsAsFactors = FALSE
                )
            }

            if(file.exists(file=df_file_path)){
                file.remove(downloaded_file_path)
            }
            
            return(df)
        },

        #'
        #' @return Returns an R dataframe with the storage buckets in the system
        #' @export
        list_buckets = function() {            
            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            url <- paste0(private$lakehouse_url, "/storage/bucket-list")
            
            response <- httr::GET(
                url = url, 
                httr::add_headers(.headers = headers), 
                config = httr::config(ssl_verifypeer = 0)
            )

            respose_text <- httr::content(response, as = "text", encoding = "UTF-8")

            response_data <- jsonlite::fromJSON(respose_text)

            if (!is.null(response_data$error) || length(response_data$bucket_list) == 0) {
                return(data.frame())
            }

            buckets_df <- private$format_output_dataframe(response_data$bucket_list, output_format = "df")
  
            if (nrow(buckets_df) > 0) {
                buckets_df <- buckets_df[order(buckets_df$bucket_name), , drop = FALSE]
            }
            
            return(buckets_df)
        },

        #'
        #' @return Returns a json-formatted string with the storage buckets in the system
        #' @export
        list_buckets_json = function(){
            bucket_list <- self$list_buckets_df()
            return(jsonlite::toJSON(bucket_list, pretty = TRUE))
        },

        #' List collections catalog records
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return A R dataframe cotaining the collection records
        #' @export
        list_collections = function(
            sort_by_key = NULL, 
            sort_desc = FALSE
        ) {     
            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )
            
            response <- httr::GET(
                url = paste0(private$lakehouse_url, "/catalog/collections/all"), 
                httr::add_headers(.headers=headers), 
                config = httr::config(ssl_verifypeer = 0)
            )

            if (httr::status_code(response) != 200) {
                stop("Error: Failed to fetch collections. HTTP status: ", httr::status_code(response))
            }

            response_text <- httr::content(response, as = "text", encoding = "UTF-8")

            if (nchar(response_text) == 0) {
                return(list())
            }
            
            data <- jsonlite::fromJSON(response_text)

            records <- data$records

            if (length(records) == 0 || is.null(records)) {
                return(data.frame())
            }
            
            if (!is.null(sort_by_key) && sort_by_key %in% names(records)) {
                records <- records[order(records[[sort_by_key]], decreasing = sort_desc), ]
            }


            
            return(private$format_output_dataframe(records, output_format = "df"))
        },

        list_collections_json = function(
            sort_by_key = NULL, 
            sort_desc = FALSE
        ){
            collection_list <- self$list_collections_df(sort_by_key, sort_desc)
            json <- jsonlite::toJSON(collection_list, pretty = TRUE)
            return(json)
        },

        #' List files catalog records
        #' @param include_raw A Boolean value indicating if the raw files should be included in the response
        #' @param include_processed A Boolean value indicating if the processed files should be included in the response
        #' @param include_curated A Boolean value indicating if the curated files should be included in the response
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return A R dataframe cotaining the file records
        #' @export
        list_files = function(
            include_raw = TRUE, 
            include_processed = TRUE, 
            include_curated = TRUE, 
            sort_by_key = NULL, 
            sort_desc = FALSE
        ) {
           
            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )
            
            response <- httr::GET(url = paste0(private$lakehouse_url, "/catalog/files/all"), httr::add_headers(.headers=headers), config = httr::config(ssl_verifypeer = 0))

            if (httr::status_code(response) != 200) {
                stop("Error: Failed to fetch files. HTTP status: ", httr::status_code(response))
            }

            response_text <- httr::content(response, as = "text", encoding = "UTF-8")

            if (nchar(response_text) == 0) {
                return(list())
            }
            
            data <- jsonlite::fromJSON(response_text)
            
            records <- data$records

            if (length(records) == 0 || is.null(records)) {
                return(data.frame())
            }
            
            if (!is.null(sort_by_key) && sort_by_key %in% names(records)) {
                records <- records[order(records[[sort_by_key]], decreasing = sort_desc), ]
            }
            
            filter_options <- c()

            if (include_raw) filter_options <- c(filter_options, "raw")
            if (include_processed) filter_options <- c(filter_options, "processed")
            if (include_curated) filter_options <- c(filter_options, "curated")
            
            filtered_data <- records[records$processing_level %in% filter_options, ]
            
            return(private$format_output_dataframe(filtered_data, output_format = "df"))
        },


        #' List files catalog records
        #' @param include_raw A Boolean value indicating if the raw files should be included in the response
        #' @param include_processed A Boolean value indicating if the processed files should be included in the response
        #' @param include_curated A Boolean value indicating if the curated files should be included in the response
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return A json-formatted string cotaining the file records
        #' @export
        list_files_json = function(
            include_raw = TRUE, 
            include_processed = TRUE, 
            include_curated = TRUE, 
            sort_by_key = NULL, 
            sort_desc = FALSE
        ) {
            file_records <- self$list_files_df(include_raw, include_processed, include_curated, sort_by_key, sort_desc)
            json <- jsonlite::toJSON(file_records, pretty = TRUE)
            return(json)
        },

        #' Upload R dataframes function
        #' @param df: The R dataframe contaiing the data
        #' @param df_name: the dataframe name only, without the extension. By default the dataframe will be stored as a CSV file
        #' @param collection_catalog_id: the collection identifiyer, from the collection catalog, where the file will be placed in
        #' @param dataframe_description [Optional]: Additional description for the file
        #' @param file_category: the file class must indicate if the file is 'structured' or 'unstructured', by default the file is set to be 'unstructured'. Structured files can be Columnar or document files such as csv, tsv, excel, json, parquet. 
        #' @param dataframe_version [Optional, default 1]: The version of this dataframe in the system 
        #' @param public [Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog
        #' @param processing_level [Optional, default raw]: The processing level of this dataframe
        #' @return NULL
        #' @export
        upload_dataframe = function(
            df,
            df_name,
            collection_catalog_id,
            dataframe_description = "",
            file_category = "structured",
            dataframe_version = 1,
            public = FALSE,
            processing_level = "raw"
        ) {
            df_file_path <- paste0(getwd(), "/", df_name, ".csv") 
            write.csv(df, file = df_file_path, row.names = FALSE) 

            upload_response <-  self$upload_file(
                local_file_path = df_file_path,
                final_file_name = paste0(df_name, ".csv"),
                collection_catalog_id = collection_catalog_id,
                file_category="structured",
                file_description=dataframe_description,
                file_version=dataframe_version,
                public=public,
                processing_level=processing_level
            )
            
            if(file.exists(file=df_file_path)){
                file.remove(df_file_path)
            }

            return(upload_response)
        },

        #' Upload unstructured file funtion 
        #' @param local_file_path: the local path to the file to be uploaded
        #' @param final_file_name: the output file name in the storage
        #' @param collection_catalog_id: the collection identifiyer, from the collection catalog, where the file will be placed in
        #' @param file_category: the file class must indicate if the file is 'structured' or 'unstructured', by default the file is set to be 'unstructured'. Structured files can be Columnar or document files such as csv, tsv, excel, json, parquet. 
        #' @param file_description [Optional]: Additional description for the file
        #' @param file_version [Optional, default 1]: The version of the file you are uploading for version control
        #' @param file_size [Optional, default 0]: The size of the file you are uploading in bytes, the sizen will be taken by default from your system
        #' @param public [Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog
        #' @param processing_level [Optional]: A string containing the processing level of the file to be uploaded, e.g., ["raw", "processed", "curated"]
        #' @return Returns a dataframe containing the catalog record for the inserted file
        #' @export
        upload_file = function(
            local_file_path, 
            final_file_name, 
            collection_catalog_id, 
            file_category = "unstructured", 
            file_description = NULL,
            file_version = 1,
            public=FALSE,
            processing_level = NULL
        ) {  
            file_size <- file.info(local_file_path)$size
            
            payload <- list(
                collection_catalog_id=collection_catalog_id,
                file_name=final_file_name
            )

            optional_params <- list(
                file_category=file_category,
                file_version=file_version,
                file_size=file_size,
                public=public,
                processing_level=processing_level,
                file_description=file_description
            )

            for (name in names(optional_params)) {
                if (!is.null(optional_params[[name]])) {
                    payload[[name]] <- optional_params[[name]]
                }
            }

            payload <- payload[!sapply(payload, is.null)]
            
            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            url <- paste0(private$lakehouse_url, "/storage/files/upload-request")
            
            response <- httr::POST(
                url, 
                httr::add_headers(.headers=headers), 
                body = jsonlite::toJSON(payload, auto_unbox = TRUE), 
                config = httr::config(ssl_verifypeer = 0)
            )

            if (httr::status_code(response) != 200) {
                stop("Unable to get upload URL")
            }

            response_text <- httr::content(response, as = "text", encoding = "UTF-8")

            if (nchar(response_text) == 0) {
                stop("Unable to get upload URL -> No content")
            }
            
            response_content <- jsonlite::fromJSON(response_text)
            
            signed_url <- response_content$upload_url
            catalog_record_id <- response_content$catalog_record_id
            
            file_conn <- file(local_file_path, "rb")
            
            while (TRUE) {
                chunk <- readBin(file_conn, raw(), 1024 * 1024)
                
                if (length(chunk) == 0) {
                    break
                }
                
                upload_response <- httr::PUT(signed_url, body = chunk, httr::add_headers("Content-Type" = "application/octet-stream"))
                
                if (httr::status_code(upload_response) %in% c(200, 204)) {
                    next
                } else {
                    stop(paste("Failed to upload file:", httr::content(upload_response, "text")))
                }
            }
            
            close(file_conn)
            
            status_payload <- list(status = "ready")

            status_url <- paste0(private$lakehouse_url, "/catalog/set-file-status/", catalog_record_id)

            status_response <- httr::PUT(status_url, httr::add_headers(.headers=headers), body = jsonlite::toJSON(status_payload, auto_unbox = TRUE), config = httr::config(ssl_verifypeer = 0))

            parsed_response <- httr::content(status_response, as = "text", encoding = "UTF-8")
            
            return(jsonlite::fromJSON(parsed_response))
        },

        #' Search for collection names using a given keyword
        #' @param keyword A string containing the keyword to search for, the search will match the collection names to the keyword
        #' @return A list of collections in the given output_format
        #' @export
        search_collections_by_keyword = function(keyword) {      
            filters <- list(
                list(
                    property_name = "collection_name",
                    operator = "*",
                    property_value = keyword
                )
            )

            payload <- tryCatch({
                list(filters = filters)
            }, error = function(e) {
                stop("Incorrect filter format!")
            })

            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            url <- paste0(private$lakehouse_url,"/catalog", "/collections", "/search")

            response <- httr::POST(
                url, httr::add_headers(.headers=headers), 
                body = jsonlite::toJSON(payload, auto_unbox = TRUE), 
                config = httr::config(ssl_verifypeer = 0),
                encode = "json"
            )

            respose_text <- httr::content(response, as="text", encoding="UTF-8")
            
            response_data <- jsonlite::fromJSON(respose_text)

            records <- response_data$records %||% list()
            
            records <- private$format_output_dataframe(dataset = records)
            
            return(records)
        },

        #' Search for file names using a given keyword
        #' @param keyword A string containing the keyword to search for, the search will match the file names to the keyword
        #' @return A list of files in the given output_format
        #' @export
        search_files_by_keyword = function(keyword) {      

            filters <- list(
                list(
                    property_name = "file_name",
                    operator = "*",
                    property_value = keyword
                )
            )

            payload <- tryCatch({
                list(filters = filters)
            }, error = function(e) {
                stop("Incorrect filter format!")
            })

            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            url <- paste0(private$lakehouse_url,"/catalog", "/files", "/search")

            response <- httr::POST(
                url, httr::add_headers(.headers=headers), 
                body = jsonlite::toJSON(payload, auto_unbox = TRUE), 
                config = httr::config(ssl_verifypeer = 0),
                encode = "json"
            )

            respose_text <- httr::content(response, as="text", encoding="UTF-8")
            
            response_data <- jsonlite::fromJSON(respose_text)

            records <- response_data$records %||% list()
            
            records <- private$format_output_dataframe(dataset = records)
            
            return(records)
        },

        
        #' 
        #' @description Search files on the catalogue based on the given filters using query strings.
        #'
        #' @param ... Query strings containing search terms in KEY[OPERATOR]VALUE format
        #' @section Query String Structure:
        #' The query string should follow the pattern: \code{KEY[OPERATOR]VALUE}\cr
        #' Supported operators:
        #' \itemize{
        #'   \item \code{=} - Equal to
        #'   \item \code{>} - Greater than
        #'   \item \code{<} - Less than
        #'   \item \code{>=} - Greater than or equal to
        #'   \item \code{<=} - Less than or equal to
        #'   \item \code{*} - Substring match (wildcard)
        #' }
        #'
        #' @section Available Output Formats:
        #' \describe{
        #'   \item{\code{"df"}}{Returns results as a data.frame}
        #'   \item{\code{"json"}}{Returns results as JSON string}
        #'   \item{\code{"dict"}}{Returns results as named list (default)}
        #'   \item{\code{"table"}}{Returns results in pretty-printed table format}
        #' }
        #' #'
        #' @return The search results in the requested format. Returns an empty list if no results found or if error occurs.
        #' 
        #' @examples
        #' \dontrun{
        #' # Basic usage
        #' search_collections_query(self, 'collection_name*lake')
        #' 
        #' # Multiple conditions
        #' search_collections_query(self,
        #'                         'collection_name*lake',
        #'                         'inserted_by=user1@gmail.com',
        #'                         'inserted_at>1747934722',
        #'                         'public=TRUE',
        #'                         output_format='table')
        #' }
        #'
        #' @seealso \code{\link{parse_query_args}} for the query parsing implementation
        #' @export
        search_collections_query = function(
            ...
        ){
            query_args <- list(...)

            parsed_args <- tryCatch({
                private$parse_query_args(args = query_args)
            }, error = function(e) {
                stop("Failed to parse query arguments")
            })

            filters <- lapply(parsed_args, function(arg) {
                list(
                    property_name = arg$key,
                    operator = arg$op,
                    property_value = arg$value
                )
            })

            payload <- tryCatch({
                list(filters = filters)
            }, error = function(e) {
                stop("Incorrect filter format!")
            })

            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )

            response <- tryCatch({
                httr::POST(
                    url = paste0(private$lakehouse_url, "/catalog/collections/search"),
                    httr::add_headers(.headers = headers),
                    body = payload,
                    encode = "json"
                )
            }, error = function(e) {
                stop("Request failed: ", e$message)
            })

            if (httr::http_error(response) || !length(httr::content(response))) {
                return(list())
            }

            respose_text <- httr::content(response, as="text", encoding="UTF-8")
            
            response_data <- jsonlite::fromJSON(respose_text)

            records <- response_data$records %||% list()

            records <- private$format_output_dataframe(dataset = records)

            return(records)
        },


        #' 
        #' @description Search files on the catalogue based on the given filters using query strings.
        #'
        #' @param ... Query strings containing search terms in KEY[OPERATOR]VALUE format
        #' @section Query String Structure:
        #' The query string should follow the pattern: \code{KEY[OPERATOR]VALUE}\cr
        #' Supported operators:
        #' \itemize{
        #'   \item \code{=} - Equal to
        #'   \item \code{>} - Greater than
        #'   \item \code{<} - Less than
        #'   \item \code{>=} - Greater than or equal to
        #'   \item \code{<=} - Less than or equal to
        #'   \item \code{*} - Substring match (wildcard)
        #' }
        #'
        #' @section Available Output Formats:
        #' \describe{
        #'   \item{\code{"df"}}{Returns results as a data.frame}
        #'   \item{\code{"json"}}{Returns results as JSON string}
        #'   \item{\code{"dict"}}{Returns results as named list (default)}
        #'   \item{\code{"table"}}{Returns results in pretty-printed table format}
        #' }
        #' #'
        #' @return The search results in the requested format. Returns an empty list if no results found or if error occurs.
        #' 
        #' @examples
        #' \dontrun{
        #' # Basic usage
        #' search_files_query(self, 'file_name*lake')
        #' 
        #' # Multiple conditions
        #' search_files_query(self,
        #'                         'file_name*lake',
        #'                         'inserted_by=user1@gmail.com',
        #'                         'inserted_at>1747934722',
        #'                         'public=TRUE',
        #'                         output_format='table')
        #' }
        #'
        #' @seealso \code{\link{parse_query_args}} for the query parsing implementation
        #' @export
        search_files_query = function(
            ...
        ){
            query_args <- list(...)

            parsed_args <- tryCatch({
                private$parse_query_args(args = query_args)
            }, error = function(e) {
                stop("Failed to parse query arguments")
            })

            filters <- lapply(parsed_args, function(arg) {
                list(
                    property_name = arg$key,
                    operator = arg$op,
                    property_value = arg$value
                )
            })

            payload <- tryCatch({
                list(filters = filters)
            }, error = function(e) {
                stop("Incorrect filter format!")
            })

            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )

            response <- tryCatch({
                httr::POST(
                    url = paste0(private$lakehouse_url, "/catalog/files/search"),
                    httr::add_headers(.headers = headers),
                    body = payload,
                    encode = "json"
                )
            }, error = function(e) {
                stop("Request failed: ", e$message)
            })

            if (httr::http_error(response) || !length(httr::content(response))) {
                return(list())
            }

            respose_text <- httr::content(response, as="text", encoding="UTF-8")
            
            response_data <- jsonlite::fromJSON(respose_text)

            records <- response_data$records %||% list()

            records <- private$format_output_dataframe(dataset = records)

            return(records)
        }
    )
)
