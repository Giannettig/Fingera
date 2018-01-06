
#Install the libraries
library(httr)
library(data.table)
library(xml2)
library(purrr)
library(doParallel)
library(dplyr)

#=======BASIC INFO ABOUT THE Fingera EXTRACTOR========#

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

##Catch config errors

if(is.null(user)) stop("enter your username in the user config field")
if(is.null(password)) stop("enter your password in the #password config field")


##Now process the request

getStats<-function(api,endpoint){
  
  url<-paste0(api,endpoint)
  
  r <-
    RETRY(
      "GET",
      url,
      config = authenticate(user,password),
      times = 3,
      pause_base = 3,
      pause_cap = 10
    )
  
  res<-content(r,"text",encoding = "UTF-8")%>%fromJSON
  
}

getTimelogs<-function(api,user_id){
  
  url<-paste0(api,"time_logs")
  
  args<-list(
    "user_id"=user_id
  )
  
r<-RETRY(
      "GET",
      url,
      config = authenticate(user,password),
      times = 3,
      pause_base = 3,
      pause_cap = 10,
      query = args
    )

  res<-content(r,"text",encoding = "UTF-8")%>%fromJSON
  
}

#iterate over users
users<-getStats(api,"users")

user_ids<-users%>%select(id)%>%.[,1]

registerDoParallel(cores=detectCores()-1)

res<-foreach(i=user_ids,.combine=bind_rows,.multicombine = TRUE, .errorhandling="remove") %dopar% {

getTimelogs(api,i)

}

write_csv(res,"out/tables/time_logs.csv")
write_csv(res,"out/tables/users.csv")
