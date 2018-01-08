
#Install the libraries
library(httr)
library(data.table)
library(xml2)
library(purrr)
library(doParallel)
library(dplyr)
library(jsonlite)

#=======BASIC INFO ABOUT THE Fingera EXTRACTOR========#

# Using fingera API documentation v.5.4.0.
# Main Changes are defining offsets for users

#=======CONFIGURATION========#
## initialize application
library('keboola.r.docker.application')
app <- DockerApplication$new('/data/')

app$readConfig()

## access the supplied value of 'myParameter'
password<-app$getParameters()$`#password`
user<-app$getParameters()$user
# start_date<-app$getParameters()$from
# end_date<-app$getParameters()$to
api<-app$getParameters()$api_url


# devel -------------------------------------------------------------------

source("devel.R")

##Catch config errors

if(is.null(user)) stop("enter your username in the user config field")
if(is.null(password)) stop("enter your password in the #password config field")
if(is.null(api)) stop("enter the API url")

## List of possible endpoints
endpoint_list<-c(
  "day_schedules", 
  "event_categories",
  "fingera_stations",
  "groups",
  "local_settings",
  "settings",
  "terminals",
  "time_logs",
  "user_accounts",
  "users")

##Functions 

getStats <- function( endpoint, api, ids = FALSE ) {
  url <- paste0(api, endpoint)
  
  r <-
    RETRY(
      "GET",
      url,
      config = authenticate(user, password),
      times = 3,
      pause_base = 3,
      pause_cap = 10
    )
  
  if (as.numeric(r$headers$`x-total`) > as.numeric(r$headers$`x-max-limit`)) {
    #get the size of the list
    size <- as.numeric(r$headers$`x-total`)
    limit <- as.numeric(r$headers$`x-max-limit`)
    sequence <- if(ids == FALSE) seq(0, size, by = limit)
    
    #register cores on the machine for the parallel loop - tohle nefunguje, zatím používám klasickou smyčku.
    if(length(sequence)>1000) registerDoParallel(cores=detectCores()-1)
    
    res <-
      foreach(
        i = sequence,
        .combine = bind_rows,
        .multicombine = TRUE,
        .init = NULL,
        .errorhandling = "remove"
      ) %dopar% {
        
        r <-
          GET(
            url,
            config = authenticate(user, password),
            query = list("offset" = i, "limit" = limit)
          )
        res <- content(r, "text", encoding = "UTF-8") %>% fromJSON
        
      }
    
  } else{ res <- content(r, "text", encoding = "UTF-8") %>% fromJSON }
  
  result<-res%>%distinct
  
}


results<-map(endpoint_list,getStats,api=api)

##Write the tables in the output bucket

map2(results,endpoint_list,function(x,y){fwrite(x,paste0("out/tables/",y,".csv"))})
