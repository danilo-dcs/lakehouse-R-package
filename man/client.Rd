\name{LakehouseClient}
\alias{client}
\title{Lakehouse Client Functions}

\section{Client Setup}{
  \code{client <- setup_client("lakehouse-url")}

  *To initialize the client and use its funtions to interact with the lakehouse system, you need to setup the client by specifying the lakehouse-api url.
}

\section{Methods}{

  \itemize{
    \item \href{#method-LakehouseClient-auth}{\code{client$auth()}}
    \item \href{#method-LakehouseClient-download_file}{\code{client$download_file()}}
    \item \href{#method-LakehouseClient-create_collection}{\code{client$create_collection()}}
    \item \href{#method-LakehouseClient-get_dataframe}{\code{client$get_dataframe()}}
    \item \href{#method-LakehouseClient-list_buckets}{\code{client$list_buckets()}}
    \item \href{#method-LakehouseClient-list_buckets_df}{\code{client$list_buckets_df()}}
    \item \href{#method-LakehouseClient-list_buckets_json}{\code{client$list_buckets_json()}}
    \item \href{#method-LakehouseClient-list_collections}{\code{client$list_collections()}}
    \item \href{#method-LakehouseClient-list_collections_df}{\code{client$list_collections_df()}}
    \item \href{#method-LakehouseClient-list_collections_json}{\code{client$list_collections_json()}}
    \item \href{#method-LakehouseClient-list_file}{\code{client$list_file()}}
    \item \href{#method-LakehouseClient-list_files_df}{\code{client$list_files_df()}}
    \item \href{#method-LakehouseClient-list_files_json}{\code{client$list_files_json()}}
    \item \href{#method-LakehouseClient-list_storage_data}{\code{client$list_storage_data()}}
    \item \href{#method-LakehouseClient-upload_dataframe}{\code{client$upload_dataframe()}}
    \item \href{#method-LakehouseClient-upload_file}{\code{client$upload_file()}}
    \item \href{#method-LakehouseClient-search_collections_by_keyword}{\code{client$search_collections_by_keyword()}}
    \item \href{#method-LakehouseClient-search_files_by_keyword}{\code{client$search_files_by_keyword()}}
    \item \href{#method-LakehouseClient-search_collections_query}{\code{client$search_collections_query()}}
    \item \href{#method-LakehouseClient-search_files_query}{\code{client$search_files_query()}}
  }
  

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-auth"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-auth}{}}}

  \subsection{Method \code{auth()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$auth("email", "password")}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{email}}{A string containing the user email}

      \item{\code{password}}{A string containing the user password}
      }
      \if{html}{\out{</div>}}
    }
    \subsection{Returns}{
      NULL
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-download_file"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-download_file}{}}}

  \subsection{Method \code{download_file()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$download_file("catalog_file_id", "output_file_dir")}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{catalog_file_id}}{A string with the catalog id for the file record to be downloaded}

      \item{\code{output_file_dir}}{A string containing the output file directory}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A string with the path to the output file
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-create_collection"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-create_collection}{}}}

  \subsection{Method \code{create_collection()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$create_collection(
        "storage_type",
        "collection_name",
        "bucket_name",
        "collection_description,"
        "public",
        "secret"
      )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{storage_type}}{A string containing the storage type ("gcs" for google cloud storage, "s3" for amazon s3, "hdfs" for hadoop hdfs)}

      \item{\code{collection_name}}{A string containing the new collection's name}

      \item{\code{bucket_name}}{A string containing the cloud bucket name}

      \item{\code{collection_description}}{An optional string containing additional description for this collection}

      \item{\code{public}}{Optional boolean value indicating whether the collection will be public (TRUE or FALSE)}

      \item{\code{secret}}{Optional boolean value indicating whether the collection will be secret (TRUE or FALSE)}
      }
      \if{html}{\out{</div>}}
    }
    \subsection{Returns}{
      A dataset containing the collection's information in the catalog
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-get_dataframe"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-get_dataframe}{}}}

  \subsection{Method \code{get_dataframe()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$get_dataframe("catalogue_file_id")}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{catalogue_file_id}}{A string with the catalog id for the file record to be downloaded}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A dataframe representing the file selected
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_buckets"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_buckets}{}}}

  \subsection{Method \code{list_buckets()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_buckets()}\if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      Returns a table-formatted string with the storage buckets in the system
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_buckets_df"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_buckets_df}{}}}

  \subsection{Method \code{list_buckets_df()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_buckets_df()}\if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      Returns an R dataframe with the storage buckets in the system
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_buckets_json"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_buckets_json}{}}}

  \subsection{Method \code{list_buckets_json()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_buckets_json()}\if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      Returns a json-formatted string with the storage buckets in the system
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_collections"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_collections}{}}}

  \subsection{Method \code{list_collections()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_collections("sort_by_key", sort_desc = FALSE)}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{sort_by_key}}{A string containing a valid catalog key to be user for sorting the response}

      \item{\code{sort_desc}}{A boolean indicating if the reponse list should be sorted descending}
      }
      \if{html}{\out{</div>}}
    }
    \subsection{Returns}{
      A R dataframe cotaining the collection records
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_collections_df"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_collections_df}{}}}

  \subsection{Method \code{list_collections_df()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_collections_df("sort_by_key", sort_desc = FALSE)}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{sort_by_key}}{A string containing a valid catalog key to be user for sorting the response}

      \item{\code{sort_desc}}{A boolean indicating if the reponse list should be sorted descending}
      }
      \if{html}{\out{</div>}}
    }
    \subsection{Returns}{
      A R dataframe cotaining the collection records
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_collections_json"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_collections_json}{}}}

  \subsection{Method \code{list_collections_json()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_collections_json("sort_by_key", sort_desc = FALSE)}\if{html}{\out{</div>}}
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_file"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_file}{}}}

  \subsection{Method \code{list_file()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_file(
        include_raw = TRUE,
        include_processed = TRUE,
        include_curated = TRUE,
        "sort_by_key",
        sort_desc = FALSE
      )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{include_raw}}{A Boolean value indicating if the raw files should be included in the response}

      \item{\code{include_processed}}{A Boolean value indicating if the processed files should be included in the response}

      \item{\code{include_curated}}{A Boolean value indicating if the curated files should be included in the response}

      \item{\code{sort_by_key}}{A string containing a valid catalog key to be user for sorting the response}

      \item{\code{sort_desc}}{A boolean indicating if the reponse list should be sorted descending}
      }
      \if{html}{\out{</div>}}
    }
    \subsection{Returns}{
      A table-formatted string cotaining the file records
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_files_df"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_files_df}{}}}

  \subsection{Method \code{list_files_df()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_files_df(
        include_raw = TRUE,
        include_processed = TRUE,
        include_curated = TRUE,
        "sort_by_key",
        sort_desc = FALSE
      )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{include_raw}}{A Boolean value indicating if the raw files should be included in the response}

      \item{\code{include_processed}}{A Boolean value indicating if the processed files should be included in the response}

      \item{\code{include_curated}}{A Boolean value indicating if the curated files should be included in the response}

      \item{\code{sort_by_key}}{A string containing a valid catalog key to be user for sorting the response}

      \item{\code{sort_desc}}{A boolean indicating if the reponse list should be sorted descending}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A R dataframe cotaining the file records
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_files_json"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_files_json}{}}}

  \subsection{Method \code{list_files_json()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_files_json(
        include_raw = TRUE,
        include_processed = TRUE,
        include_curated = TRUE,
        "sort_by_key",
        sort_desc = FALSE
      )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{include_raw}}{A Boolean value indicating if the raw files should be included in the response}

      \item{\code{include_processed}}{A Boolean value indicating if the processed files should be included in the response}

      \item{\code{include_curated}}{A Boolean value indicating if the curated files should be included in the response}

      \item{\code{sort_by_key}}{A string containing a valid catalog key to be user for sorting the response}

      \item{\code{sort_desc}}{A boolean indicating if the reponse list should be sorted descending}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A json-formatted string cotaining the file records
    }
  }

   \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-list_storage_data"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-list_storage_data}{}}}

  \subsection{Method \code{list_storage_data()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$list_storage_data()}
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A nested tibble with all storage data hierarchy buckets -> collections -> files
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-upload_dataframe"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-upload_dataframe}{}}}

  \subsection{Method \code{upload_dataframe()}}{
    \subsection{Usage}{
    \if{html}{\out{<div class="r">}}\preformatted{client$upload_dataframe(
      df,
      "df_name",
      "collection_catalog_id",
      "dataframe_description",
      "file_category",
      dataframe_version = 1,
      public = FALSE,
      "processing_level"
    )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{dataframe_description}}{[Optional]: Additional description for the file}

      \item{\code{dataframe_version}}{[Optional, default 1]: The version of this dataframe in the system}

      \item{\code{public}}{[Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog}

      \item{\code{processing_level}}{[Optional, default raw]: The processing level of this dataframe}

      \item{\code{df:}}{The R dataframe contaiing the data}

      \item{\code{df_name:}}{the dataframe name only, without the extension. By default the dataframe will be stored as a CSV file}

      \item{\code{collection_catalog_id:}}{the collection identifiyer, from the collection catalog, where the file will be placed in}

      \item{\code{file_category:}}{the file class must indicate if the file is 'structured' or 'unstructured', by default the file is set to be 'unstructured'. Structured files can be Columnar or document files such as csv, tsv, excel, json, parquet.}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      NULL
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-upload_file"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-upload_file}{}}}

  \subsection{Method \code{upload_file()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$upload_file(
        "local_file_path",
        "final_file_name",
        "collection_catalog_id",
        "file_category",
        "file_description",
        file_version = 1,
        public = FALSE,
        "processing_level"
      )}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{file_description}}{[Optional]: Additional description for the file}

      \item{\code{file_version}}{[Optional, default 1]: The version of the file you are uploading for version control}

      \item{\code{public}}{[Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog}

      \item{\code{processing_level}}{[Optional]: A string containing the processing level of the file to be uploaded, e.g., ["raw", "processed", "curated"]}

      \item{\code{local_file_path:}}{the local path to the file to be uploaded}

      \item{\code{final_file_name:}}{the output file name in the storage}

      \item{\code{collection_catalog_id:}}{the collection identifiyer, from the collection catalog, where the file will be placed in}

      \item{\code{file_category:}}{the file class must indicate if the file is 'structured' or 'unstructured', by default the file is set to be 'unstructured'. Structured files can be Columnar or document files such as csv, tsv, excel, json, parquet.}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      Returns a dataframe containing the catalog record for the inserted file
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-search_collections_by_keyword"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-search_collections_by_keyword}{}}}

  \subsection{Method \code{search_collections_by_keyword()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$search_collections_by_keyword("keyword")}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{keyword}}{A string containing the keyword to search for, the search will match the collection names to the keyword}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A list of collections in the given output_format
    }
  }

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-search_files_by_keyword"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-search_files_by_keyword}{}}}

  \subsection{Method \code{search_files_by_keyword()}}{
    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$search_files_by_keyword("keyword")}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
      \item{\code{keyword}}{A string containing the keyword to search for, the search will match the file names to the keyword}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      A list of files in the given output_format
    }
  }


  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-search_collections_query"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-search_collections_query}{}}}


  \subsection{Method \code{search_collections_query()}}{
    Search files on the catalogue based on the given filters using query strings.

    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$search_collections_query(...)}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
        \item{\code{...}}{Query strings containing search terms in "KEY[OPERATOR]VALUE" format}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      The search results in the requested format. Returns an empty list if no results found or if error occurs.
    }

    \subsection{Query String Structure}{
      The query string should follow the pattern: \code{"KEY[OPERATOR]VALUE"}\cr
      Supported operators:
      \itemize{
        \item \code{=} - Equal to
        \item \code{>} - Greater than
        \item \code{<} - Less than
        \item \code{>=} - Greater than or equal to
        \item \code{<=} - Less than or equal to
        \item \code{*} - Substring match (wildcard)
      }


      The query string should follow the pattern: \code{"KEY[OPERATOR]VALUE"}\cr
      Supported operators:
      \itemize{
        \item \code{=} - Equal to
        \item \code{>} - Greater than
        \item \code{<} - Less than
        \item \code{>=} - Greater than or equal to
        \item \code{<=} - Less than or equal to
        \item \code{*} - Substring match (wildcard)
      }
    }

    \subsection{Example}{
      \if{html}{\out{<div class="r example copy">}}
      \preformatted{
        # Basic usage
        client$search_collections_query('collection_name*lake')

        # Multiple conditions
        client$search_collections_query(
          "collection_name*lake",
          "inserted_by=user1@gmail.com",
          "inserted_at>1747934722",
          "public=TRUE"
        )
      }

      }
      \if{html}{\out{</div>}}
  }
  

  \if{html}{\out{<hr>}}
  \if{html}{\out{<a id="method-LakehouseClient-search_files_query"></a>}}
  \if{latex}{\out{\hypertarget{method-LakehouseClient-search_files_query}{}}}


  \subsection{Method \code{search_files_query()}}{
    Search files on the catalogue based on the given filters using query strings.

    \subsection{Usage}{
      \if{html}{\out{<div class="r">}}\preformatted{client$search_files_query(...)}\if{html}{\out{</div>}}
    }

    \subsection{Arguments}{
      \if{html}{\out{<div class="arguments">}}
      \describe{
        \item{\code{...}}{Query strings containing search terms in "KEY[OPERATOR]VALUE" format}
      }
      \if{html}{\out{</div>}}
    }

    \subsection{Returns}{
      The search results in the requested format. Returns an empty list if no results found or if error occurs.
    }


    \subsection{Query String Structure}{
      The query string should follow the pattern: \code{"KEY[OPERATOR]VALUE"}\cr
      Supported operators:
      \itemize{
        \item \code{=} - Equal to
        \item \code{>} - Greater than
        \item \code{<} - Less than
        \item \code{>=} - Greater than or equal to
        \item \code{<=} - Less than or equal to
        \item \code{*} - Substring match (wildcard)
      }


      The query string should follow the pattern: \code{"KEY[OPERATOR]VALUE"}\cr
      Supported operators:
      \itemize{
        \item \code{=} - Equal to
        \item \code{>} - Greater than
        \item \code{<} - Less than
        \item \code{>=} - Greater than or equal to
        \item \code{<=} - Less than or equal to
        \item \code{*} - Substring match (wildcard)
      }
    }

    \subsection{Example}{
      \if{html}{\out{<div class="r example copy">}}
      \preformatted{
        # Basic usage
        client$search_files_query('file_name*lake')

        # Multiple conditions
        client$search_files_query(
          "file_name*lake",
          "inserted_by=user1@gmail.com",
          "inserted_at>1747934722",
          "public=TRUE"
        )
      }
    }
  }
}
