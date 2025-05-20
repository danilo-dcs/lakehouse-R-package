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
        }
    ),
  
    public = list(    
        #' Initializes the lakehouse client
        #' @param lakehouse_url The url to the lakehouse api host
        #' @return NULL
        #' @export
        initialize = function(lakehouse_url) {
            private$lakehouse_url <- lakehouse_url
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
            namenode_address = NULL,
            bucket_name = NULL,
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

            optional_params <- list(
                namenode_address = namenode_address,
                bucket_name = bucket_name,
                collection_description = collection_description,
                public = public,
                secret = secret
            )

            for (name in names(optional_params)) {
                if (!is.null(optional_params[[name]])) {
                    payload[[name]] <- optional_params[[name]]
                }
            }

            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )

            url <- paste0(private$lakehouse_url, "/storage/collections/create")
            
            response <- httr::POST(url, body = jsonlite::toJSON(payload, auto_unbox = TRUE, null = "skip"), encode = "json", httr::add_headers(.headers = headers), config = httr::config(ssl_verifypeer = 0))

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

        #' List collections catalog records
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return A list cotaining collection entries
        #' @export
        list_collections = function(
            sort_by_key = NULL, 
            sort_desc = FALSE
        ) {

            headers <- c(
                "Authorization" = paste("Bearer", private$access_token)
            )
            
            response <- httr::GET(
                url = paste0(private$lakehouse_url, 
                "/catalog/collections/all"), 
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
            
            if (length(records) == 0) {
                return(list())
            }
            
            if (!is.null(sort_by_key)) {
                records <- records[order(records[[sort_by_key]], decreasing = sort_desc), ]
            }
            
            return(records)
        },

        #' List files catalog records
        #' @param include_raw A Boolean value indicating if the raw files should be included in the response
        #' @param include_processed A Boolean value indicating if the processed files should be included in the response
        #' @param include_curated A Boolean value indicating if the curated files should be included in the response
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return A list cotaining file entries
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
            
            if (length(records) == 0) {
                return(list())
            }
            
            if (!is.null(sort_by_key)) {
                records <- records[order(records[[sort_by_key]], decreasing = sort_desc), ]
            }
            
            filter_options <- c()
            if (include_raw) filter_options <- c(filter_options, "raw")
            if (include_processed) filter_options <- c(filter_options, "processed")
            if (include_curated) filter_options <- c(filter_options, "curated")
            
            filtered_data <- records[records$processing_level %in% filter_options, ]
            
            return(filtered_data)
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
            if (is.null(bucket_name) && is.null(namenode_address)) {
                stop("Unable to find the storage location. namenode_address or bucket_name must be specified")
            }

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
            file_size = 0,
            public=FALSE,
            processing_level = NULL
        ) {  
            if (is.null(bucket_name) && is.null(namenode_address)) {
                stop("Unable to find the storage location. namenode_address or bucket_name must be specified")
            }
            
            file_size <- file.info(local_file_path)$size

             payload = {
           
        }
            
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

        #' Search the catalogs using specified filters
        #' @param filters A list of filters to be applied to the query transaction. the filtes must follow the structure: list(list(property_name="file_name", operator="=", property_value="Marcel"),list(property_name="property2", operator="=", property_value="test@gmail.com")), Operators migh be ("=",">","<", ">=", "<=", "*") with "*" being the string matching operator fro string patterns
        #' @param target_catalog A string containing the target catalog for the search method ("files" or "collections")
        #' @param sort_by_key A string containing a valid catalog key to be user for sorting the response
        #' @param sort_desc A boolean indicating if the reponse list should be sorted descending
        #' @return Returns a list of catalog records
        #' @export
        search_catalogue = function(filters, target_catalog = "collections", sort_by_key = NULL, sort_desc = FALSE) {      

            if(length(filters) == 0) {
               stop("No filters were specified!")
            }

            if (
                !all(
                    sapply(
                        filters, function(x) is.list(x) && "property_name" %in% names(x) && 
                        "operator" %in% names(x) && "property_value" %in% names(x)
                    )
                )
            ) {
                stop("Incorrect filter format!")
            }

            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            payload <- list(filters = filters)
            
            url <- paste0(private$lakehouse_url,"/catalog/", target_catalog, "/search")

            response <- httr::POST(
                url, httr::add_headers(.headers=headers), 
                body = jsonlite::toJSON(payload, auto_unbox = TRUE), 
                config = httr::config(ssl_verifypeer = 0)
            )

            respose_text <- httr::content(response, as="text", encoding="UTF-8")
            
            response_data <- jsonlite::fromJSON(respose_text)
            
            if (!is.null(sort_by_key)) {
                response_data <- response_data[order(response_data[[sort_by_key]], decreasing = sort_desc), ]
            }
            
            return(response_data)
        },

        #'
        #' @return Returns a list of documents containing the storage bucket and the cloud environment
        #' @export
        list_available_buckets = function() {
            # Lists all the available storage buckets in the system where data can be uploaded
            
            headers <- c("Authorization" = paste("Bearer", private$access_token))
            
            url <- paste0(private$lakehouse_url, "/storage/bucket-list")
            
            response <- httr::POST(
                url, httr::add_headers(.headers = headers), 
                config = httr::config(ssl_verifypeer = 0)
            )
            
            respose_text <- httr::content(response, as="text", encoding="UTF-8")

            response_data <- jsonlite::fromJSON(respose_text)
            
            if (!is.null(response_data$error)) {
                return(list())
            }
            
            bucket_list <- response_data$bucket_list
            
            if (length(bucket_list) == 0) {
                return(list())
            }
            
            sorted_buckets <- bucket_list[order(sapply(bucket_list, function(item) item$bucket_name))]
            
            return(sorted_buckets)
        }
    )
)
