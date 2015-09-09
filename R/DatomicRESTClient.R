require(RCurl)

DatomicRESTClient <- setClass(  
  "DatomicRESTClient",
  slots = c(
    URI = "character",
    storage = "character",
    options = "list"        
  ),
  prototype=list(
    options = list(style = "post")  #for urlencoded
  ))

DatomicRESTClient <- function(URI, storage, ...) {
  require(RCurl)
  new("DatomicRESTClient", URI = URI, storage = storage,  ...)
}

setGeneric(name="createDatabase",
           def=function(object, dbname, ...)  standardGeneric("createDatabase"))

setGeneric(name="listDatabases",
           def=function(object) standardGeneric("listDatabases"))

setGeneric(name="transact",
           def=function(object, dbname, txData, ...) standardGeneric("transact"))

setGeneric(name="rootURL",
           def=function(object)  standardGeneric("rootURL"))

setGeneric(name="getDatabaseInfo",
           def=function(object, dbname, basisT="-") standardGeneric("getDatabaseInfo"))

setGeneric(name="datoms",
           def=function(object, dbname, basisT, index, e = NULL, a = NULL, v = NULL, start = NULL,
                        end = NULL, limit = NULL, offset = NULL, asOfT = NULL, sinceT = NULL, history = NULL) {
             standardGeneric("datoms")} )

setGeneric(name="entity",
           def=function(object, dbname, e, basisT = "-" , asOfT = NULL, sinceT = NULL)
             standardGeneric("entity"))

setGeneric(name="query",
           def=function(object, dbname, basisT="-", asOfT = NULL,
                        sinceT = NULL, limit = NULL, offset = NULL) standardGeneric("query"))

#####


setMethod(f="createDatabase",
          signature="DatomicRESTClient",
          definition=function(object, dbname, options = list(
            Header="Content-Type: application/x-www-form-urlencoded")            
          )
          {           
            gatherer <- basicHeaderGatherer()
            options$headerfunction <- gatherer$update
            result <- postForm(rootURL(object),
                               .opts = options,
                               `db-name` = dbname,
                               style = object@options$style
            )         
            if (is201(gatherer))
              result
            else
              stop(paste0("Http status: ", gatherer$value()['status']))
          }
)

setMethod(f="listDatabases",
          signature="DatomicRESTClient",
          definition=function(object) 
          {
            getURL( rootURL(object) )
          }
)

setMethod(f="getDatabaseInfo",
          signature="DatomicRESTClient",
          definition=function(object, dbname, basisT="-") 
          {
            URI <- paste0(
              paste0(
                c(paste0(rootURL(object), dbname), basisT), collapse="/"
              ), 
              "/")
            getURL(URI)
          }
)

setMethod(f="transact",
          signature="DatomicRESTClient",
          definition=function(object, dbname, txData, options = list(
            header = "Content-Type: application/x-www-form-urlencoded")
          )
          {
            gatherer <- basicHeaderGatherer()
            options$headerfunction <- gatherer$update
            URI <- paste0(rootURL(object), paste0(dbname, "/"))
            postForm(URI, style="post", .opts = options, `tx-data` = txData)
            if (is201(gatherer))
              result
            else
              stop(paste0("Http status: ", gatherer$value()['status']))
          }
)

setMethod(f="entity",
          signature="DatomicRESTClient",
          definition=function(object, dbname, e, basisT="-", asOfT = NULL, sinceT = NULL) 
          {
            gatherer <- basicHeaderGatherer()
            URI <- paste0(
              paste0(
                c(paste0(rootURL(object), dbname), basisT), collapse="/"
              ), 
              "/")            
            URI <- paste0(URI, sprintf("entity?e=%s&as-of=%s&since=%s", e, asOfT, sinceT))
            response <- getURL(URI, headerfunction=gatherer$update)
            
            if (gatherer$value()['status'] == '200')
              response
            else
              stop(sprintf("HTTP Status: %s, %s", gatherer$value()['status'], gatherer$value()['statusMessage']))
          }
)

setMethod(f="datoms",
          definition=function(object, dbname, basisT, index, e = NULL, a = NULL, v = NULL, start = NULL,
                              end = NULL, limit = NULL, offset = NULL, asOfT = NULL, sinceT = NULL, history = NULL) {           
            #GET /data/<storage-alias>/<db-name>/<basis-t>/datoms?index=<index>
            #[&e=<e>][&a=<a>][&v=<v>][&start=<start>][&end=<end>][&offset=<offset>]
            #[&limit=<limit>][&as-of=<as-of-t>][&since=<since-t>][&history=<history>]
            gatherer <- basicHeaderGatherer()
            URI <- paste0(
              paste0(
                c(paste0(rootURL(object), dbname), basisT), collapse="/"
              ), 
              "/")
            URI <- paste0(URI, sprintf("datomic?index=%s[&e=%s][&a=%s][&v=%s]
                                       [&start=%s][&end=%s][&offset=%s]
                                       [&limit=%s][&as-of=%s][&since=%s]
                                       [&history=%s]", index, a, v,
                                       start, end, offset, limit, asOfT, sinceT, history))
            response <- getURL(URI, headerfunction=gatherer$update)
          } 
            )

setMethod(f="query",
          definition=function(object, dbname, basisT="-", asOfT = NULL,
                              sinceT = NULL, limit = NULL, offset = NULL) {
            URI <- paste0(rootURL(object), dbname)
          }
)



setMethod(f="rootURL",
          signature="DatomicRESTClient",
          definition=function(object) {
            paste0(paste(object@URI, object@storage, sep="/"), "/")
          }
)

is201 <- function(gatherer) gatherer$value()['status'] == "201"