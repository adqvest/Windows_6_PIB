start1=Sys.time()
# tryCatch({
## ----setup, include=FALSE-------------
# rm(list = ls())
# knitr::opts_chunk$set(echo = FALSE)
# path="C:\\Users\\Santonu\\Desktop\\ADQvest\\Error files\\Modified(corr)\\R_PPT\\NIIF_R_PPT_new_widgets_verson_second_B_NOV.Rmd"
# knitr::purl(path)

## ----include=FALSE--------------------

r_file = 'A001_NIIF_R_PPT'
scheduler = '9_30_AM_WINDOWS_SERVER_NIIF_PPT'
run_by ='Adqvest_Bot'
Schedular_Start_Time=Sys.time()
start_time = Sys.time()
log_rel_date = Sys.Date()
end_time = Sys.time()



## ----include=FALSE--------------------
library(pacman)
library(officedown)
library(officer)
library(ggplot2)

p_load(XML,downloader,varhandle,gdata,mFilter,Matrix,dplyr,igraph,reshape,reshape2,
       stringr,DBI,lubridate,RMySQL,zoo,lattice,pracma,plotrix,timeDate,neverhpfilter,
       png,jpeg,RCurl,pracma,ggplot2,scales,RPostgres,jsonlite,glue,plotly,sf,
       ggnewscale,ggrepel,extrafont,showtext,flextable,grid,gtable,tibble,data.table,scales,
       paletteer,padr,tidyr,epitools,lubridate,maps,mapdata,ggmap,stringr,reprex,ggfittext,
       ggalluvial,lubridate,timeDate,english,RClickhouse,officer,officedown,ggthemes,
       openxlsx,readxl,ggpattern,hms,
       install = TRUE)
default_start_date ="2013-01-01"#Properties_FILE


## --------------------------------------#MYSQL
# MYSQL
# Adqvest_Properties = read.delim("C:\\000_Adqvest\\R Scripts\\AdQvest_properties.txt", sep = " ", header = T)
# Adqvest_Properties = read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\AdQvest_properties.txt",sep = " ", header = T)

# Adqvest_Properties = read.delim("C:\\Users\\Santonu\\Desktop\\ADQvest\\ADQ_data_base\\AdQvest_properties.txt",
#                                 sep = " ", header = T)
#
# #CLICK
# # Adqvest_Clickhouse=read.delim("C:\\000_Adqvest\\R Scripts\\Adqvest_ClickHouse_properties.txt", sep = "", header = T)
# # Adqvest_Clickhouse=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Adqvest_ClickHouse_properties.txt",
# #                               sep = "", header = T)
# Adqvest_Clickhouse=read.delim("C:\\Users\\Santonu\\Desktop\\ADQvest\\ADQ_data_base\\Adqvest_ClickHouse_properties.txt",
#                               sep = "", header = T)
#
# #PG_ADMIN
# # Adqvest_Test_Thurro=read.delim("C:\\000_Adqvest\\R Scripts\\Postgress_properties_test.txt",  sep = "", header = T)
# # Adqvest_Test_Thurro=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Postgress_properties_test.txt",                                                      sep = "", header = T)
# Adqvest_Test_Thurro=read.delim("C:\\Users\\Santonu\\Desktop\\ADQvest\\ADQ_data_base\\Postgress_properties_test.txt",                                                 sep = "", header = T)
#
#
# # Adqvest_Test_Thurro=read.delim("C:\\Users\\Santonu\\Desktop\\ADQvest\\ADQ_data_base\\Postgress_properties_prod.txt",                                                 sep = "", header = T)
#
# #PPT_TEMPLATE
# # f="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\template_presentation_Santonu_Adqvest_NIIF.pptx"
# # f="C:\\000_Adqvest\\R Scripts\\template_presentation_Santonu_Adqvest_NIIF.pptx"
# f="C:\\Users\\Santonu\\Desktop\\ADQvest\\Error files\\Modified(corr)\\R_PPT\\template_presentation_Santonu_Adqvest_NIIF.pptx"
#
#
#
# #MAP_TEMPLATE
# map_template="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\R_PPT_INDIA\\India_updated_ST_UT_boundary_Updated.shp"
# map_template="C:\\Users\\Santonu\\Downloads\\India\\India_updated_ST_UT_boundary_Updated.shp"
# excel_dir="C:\\Users\\Santonu\\Documents"
# ppt_dir="C:\\Users\\Santonu\\Documents"

## --------------------------------------#MYSQL
# Adqvest_Properties = read.delim("/home/ubuntu/AdQvestDir/AdQvest_properties.txt", sep = " ", header = T)
# Adqvest_Properties = read.delim("C:\\000_Adqvest\\R Scripts\\AdQvest_properties.txt", sep = " ", header = T)
Adqvest_Properties = read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\AdQvest_properties.txt",
                                sep = " ", header = T)

#CLICK
# Adqvest_Clickhouse=read.delim("/home/ubuntu/AdQvestDir/Adqvest_ClickHouse_properties.txt", sep = "", header = T)
# Adqvest_Clickhouse=read.delim("C:\\000_Adqvest\\R Scripts\\Adqvest_ClickHouse_properties.txt", sep = "", header = T)
Adqvest_Clickhouse=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Adqvest_ClickHouse_properties.txt",
                              sep = "", header = T)

#PG_ADMIN
# Adqvest_Test_Thurro=read.delim("/home/ubuntu/AdQvestDir/Postgress_properties_test.txt",sep = "", header = T)
# Adqvest_Test_Thurro=read.delim("C:\\000_Adqvest\\R Scripts\\Postgress_properties_test.txt",  sep = "", header = T)
Adqvest_Test_Thurro=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Postgress_properties_test.txt",                                               sep = "", header = T)

#PPT_TEMPLATE
f="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\template_presentation_Santonu_Adqvest_NIIF.pptx"
# f="C:\\000_Adqvest\\R Scripts\\template_presentation_Santonu_Adqvest_NIIF.pptx"

#MAP_TEMPLATE
map_template="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\R_PPT_INDIA\\India_updated_ST_UT_boundary_Updated.shp"
excel_dir="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT\\PPT_EXCEL"
ppt_dir="C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT"


## ----include=FALSE--------------------
#-----DB Deatail----------------------------
username = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[1]
password = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[2]
hostname = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[3]
portnum  = as.numeric(as.character(Adqvest_Properties[2,2]))
DBName   = as.character(Adqvest_Properties[3,2])


## -------------------------------------
host_cl     = unlist(as.character(Adqvest_Clickhouse[1,2]))
port_cl     = as.numeric(as.character(Adqvest_Clickhouse[2,2]))
DB_cl       = as.character(Adqvest_Clickhouse[3,2])
user_cl     = unlist(strsplit(as.character(Adqvest_Clickhouse[4,2]),':|@'))[1]
password_cl = unlist(as.character(Adqvest_Clickhouse[5,2]))


## ----include=FALSE--------------------

host_pg     = unlist(as.character(Adqvest_Test_Thurro[1,2]))
port_pg     = as.numeric(as.character(Adqvest_Test_Thurro[2,2]))
DB_pg       = as.character(Adqvest_Test_Thurro[3,2])
user_pg     = unlist(strsplit(as.character(Adqvest_Test_Thurro[4,2]),':|@'))[1]
password_pg = unlist(as.character(Adqvest_Test_Thurro[5,2]))



## -------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)

query="desc TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA"
table1 <- dbGetQuery(mysql, query)
dbDisconnect(mysql)


val1=paste0("VALUES ('NIIF_R_PPT',",
           paste0("'",r_file,"',"),
           paste0("'",scheduler,"',"),
           paste0("'",run_by,"',"),
           paste0("'",Schedular_Start_Time,"',"),
           paste0("'",start_time,"',"))

q1=paste0("INSERT INTO TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA ",
         "(", paste0(table1$Field, collapse = ", "), ") ")

log_generated_error=function(error_ty){
  error_type = class(error_ty)[1][[1]]
  error_msg =paste0(conditionMessage(error_ty))
  end_time = Sys.time()
  exe_time_sec = end_time - start_time
  val2=paste0(paste0("'",end_time,"',"),
                paste0(exe_time_sec,","),
                paste0(0,","),
                paste0('"',error_type,'",'),
                paste0('"',error_msg,'",'),
                paste0(rep("'NA'",2), collapse = ", "),
                paste0(","),
                paste0(rep("''", 3), collapse = ", "),
                paste0(", '",log_rel_date,"',"),
                paste0("'",Sys.time(),"'"))
  print(val2)
  query  = paste0(q1,val1,val2,");")
  query <- gsub("'NA'", "NULL", query)
  print(query)
  #execute the query
  con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                    user = username, password = password)

  dbExecute(con, query)
  dbDisconnect(con)

}

generate_excel=function(excel_dir,delete=FALSE){
       xlsx_files <- list.files(excel_dir, pattern = "df_niif_.*\\.xlsx$", full.names = TRUE)
       if (delete==TRUE){
         for (file in xlsx_files) {
                    file.remove(file)
         }
       }else{
         df_excel <- data.frame()
         for (file in xlsx_files) {
            df <- read_excel(file)
            df_excel <- bind_rows(df_excel, df)
         }
         write.xlsx(df_excel,file.path(getwd(), paste0('NIIF_ID_mv_.xlsx')))
         for (file in xlsx_files) {
                      file.remove(file)
           }
         }
}
generate_excel(excel_dir,delete=TRUE)
#
# remove_ppt=function(dir,doc){
#   doc=paste0('.',doc,'$')
#   ppt_files<- list.files(ppt_dir, pattern =doc, full.names = TRUE)
#   for (file in ppt_files) {file.remove(file)}
# }

remove_ppt=function(dir){
  ppt_files<- list.files(ppt_dir, pattern = "NIIF_PPT.*\\.pptx$", full.names = TRUE)
  for (file in ppt_files) {file.remove(file)}
}


remove_ppt(ppt_dir)


## -------------------------------------
Four_Chart=function(title,c1,c2,c3,c4,
                    c1_t,c1_t1,c2_t,c2_t1,c3_t,c3_t1,c4_t,c4_t1,
                    c1_s,c2_s,c3_s,c4_s,c1_n,c2_n,c3_n,c4_n,title1="TITLE"){

  my_ppt=add_slide(my_ppt,layout ="4 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
                 ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                 ph_with(value = c1, location =ph_location_type(type ="chart",id=1)) %>%
                 ph_with(value =c2, location =ph_location_type(type ="chart",id=2)) %>%
                 ph_with(value =c3, location =ph_location_type(type ="chart",id=3)) %>%
                 ph_with(value =c4, location =ph_location_type(type ="chart",id=4)) %>%
                 # "Subheading"
                 ph_with(value =title, location =ph_location_type(type ="body",id=1)) %>%
                 #Source1,2
                 ph_with(value =c1_s, location =ph_location_type(type ="body",id=4)) %>%
                 ph_with(value =c2_s, location =ph_location_type(type ="body",id=5)) %>%
                 #Source3,4
                 ph_with(value =c3_s, location =ph_location_type(type ="body",id=6)) %>%
                 ph_with(value =c4_s, location =ph_location_type(type ="body",id=7)) %>%

                 ph_with(value =c1_t1, location =ph_location_type(type ="body",id=8)) %>%
                 ph_with(value =c1_t, location =ph_location_type(type ="body",id=9)) %>%

                 ph_with(value =c2_t1, location =ph_location_type(type ="body",id=10)) %>%
                 ph_with(value =c2_t, location =ph_location_type(type ="body",id=11)) %>%

                 ph_with(value =c3_t1, location =ph_location_type(type ="body",id=14)) %>%
                 ph_with(value =c3_t, location =ph_location_type(type ="body",id=12)) %>%

                 ph_with(value =c4_t1, location =ph_location_type(type ="body",id=15)) %>%
                 ph_with(value =c4_t, location =ph_location_type(type ="body",id=13)) %>%

                 ph_with(value =c1_n, location =ph_location_type(type ="body",id=16)) %>%
                 ph_with(value =c2_n, location =ph_location_type(type ="body",id=17)) %>%
                 ph_with(value =c3_n, location =ph_location_type(type ="body",id=18)) %>%
                 ph_with(value =c4_n, location =ph_location_type(type ="body",id=19)) %>%


                 ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))

}



## -------------------------------------
Three_Chart_A=function(title,c1,c2,c3,
                       c1_t,c1_t1,c2_t,c2_t1,c3_t,c3_t1,
                       c1_s,c2_s,c3_s,c1_n,c2_n,c3_n,title1="TITLE",com1=''){

    my_ppt=add_slide(my_ppt,layout ="3 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                     ph_with(value =title,location =ph_location_type(type ="body",id=12)) %>%
                     ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
                     ph_with(value =c2, location =ph_location_type(type ="chart",id=2)) %>%
                     ph_with(value =c3, location =ph_location_type(type ="chart",id=3)) %>%
                     #Source1,2
                     ph_with(value=c1_s,location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value =c2_s, location =ph_location_type(type ="body",id=4)) %>%
                     ph_with(value =c3_s, location =ph_location_type(type ="body",id=5)) %>%
                     #Source3,4
                     ph_with(value =c1_t1, location =ph_location_type(type ="body",id=8)) %>%
                     ph_with(value =c1_t, location =ph_location_type(type ="body",id=9)) %>%
                     ph_with(value =c2_t1, location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value =c2_t, location =ph_location_type(type ="body",id=7)) %>%
                     ph_with(value =c3_t1, location =ph_location_type(type ="body",id=10)) %>%
                     ph_with(value =c3_t, location =ph_location_type(type ="body",id=11)) %>%
                     ph_with(value =c1_n, location =ph_location_type(type ="body",id=13)) %>%
                     ph_with(value =c3_n, location =ph_location_type(type ="body",id=15)) %>%
                     ph_with(value =c2_n, location =ph_location_type(type ="body",id=14)) %>%
                     ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Three_Chart_B=function(title,c1,c2,c3,
                       c1_t,c1_t1,c2_t,c2_t1,c3_t,c3_t1,
                       c1_s,c2_s,c3_s,title1="TITLE",com1=''){

    my_ppt=add_slide(my_ppt,layout ="THREE_CHART_NIIF_NEW",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                     ph_with(value =title,location =ph_location_type(type ="body",id=1)) %>%
                     ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
                     ph_with(value =c2, location =ph_location_type(type ="chart",id=2)) %>%
                     ph_with(value =c3, location =ph_location_type(type ="chart",id=3)) %>%

                     #Header1,2,3
                     ph_with(value=c1_t1,location =ph_location_type(type ="body",id=2)) %>%
                     ph_with(value =c1_t, location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value =c2_t1, location =ph_location_type(type ="body",id=8)) %>%
                     ph_with(value=c2_t, location =ph_location_type(type ="body",id=9)) %>%
                     ph_with(value =c3_t1, location =ph_location_type(type ="body",id=13)) %>%
                     ph_with(value =c3_t, location =ph_location_type(type ="body",id=14)) %>%

                     #Source1,2,3
                     ph_with(value =c1_s, location =ph_location_type(type ="body",id=4)) %>%
                     ph_with(value =c2_s, location =ph_location_type(type ="body",id=7)) %>%
                     ph_with(value =c3_s, location =ph_location_type(type ="body",id=12)) %>%
                     #Source3,4
                     ph_with(value ="Note:", location =ph_location_type(type ="body",id=5)) %>%
                     ph_with(value =com1, location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Two_Chart_big_comment=function(title,c1,c2,c1_t,c1_t1="Chart header1",
                               c2_t,c2_t2="Chart header2",
                               c1_s,c2_s,note1="Note: 1",note2="Note: 2",title1="TITLE",com1=''){

    my_ppt=add_slide(my_ppt,layout ="2 CHARTS TEXTBOX",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                     ph_with(value =c1,location =ph_location_type(type ="chart",id=1)) %>%
                     ph_with(value =c2,location =ph_location_type(type ="chart",id=2)) %>%

                     ph_with(value =title, location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value =c1_t1, location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value =c1_t, location =ph_location_type(type ="body",id=7)) %>%
                     ph_with(value =c2_t2, location =ph_location_type(type ="body",id=8)) %>%
                     ph_with(value =c2_t, location =ph_location_type(type ="body",id=9)) %>%
                     # Source1
                     ph_with(value =c1_s, location =ph_location_type(type ="body",id=1)) %>%
                     ph_with(value =c2_s, location =ph_location_type(type ="body",id=2)) %>%


                     ph_with(value =com1, location =ph_location_type(type ="body",id=5)) %>%
                     ph_with(value =note1, location =ph_location_type(type ="body",id=10)) %>%
                     ph_with(value =note2, location =ph_location_type(type ="body",id=11))%>%

                     ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Two_Chart=function(title,c1,c2,c1_t,c1_t1="Chart header1",
                   c2_t,c2_t2="Chart header2",
                   c1_s,c2_s,title1="TITLE",com1='',com2=''){

        my_ppt=add_slide(my_ppt,layout ="WITH NAME 2 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
                       ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                       ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
                       ph_with(value =c2,location =ph_location_type(type ="chart",id=2)) %>%

                       ph_with(value =title, location =ph_location_type(type ="body",id=1)) %>%
                       ph_with(value =c1_t1, location =ph_location_type(type ="body",id=8)) %>%
                       ph_with(value =c1_t, location =ph_location_type(type ="body",id=9)) %>%
                       ph_with(value =c2_t2, location =ph_location_type(type ="body",id=2)) %>%
                       ph_with(value =c2_t, location =ph_location_type(type ="body",id=7)) %>%
                       # Source1
                       ph_with(value =c1_s, location =ph_location_type(type ="body",id=3)) %>%
                       ph_with(value ="Note:", location =ph_location_type(type ="body",id=6)) %>%
                       ph_with(value =com1, location =ph_location_type(type ="body",id=10)) %>%
                      # Source2
                       ph_with(value =c2_s, location =ph_location_type(type ="body",id=4)) %>%
                       ph_with(value ="Note:", location =ph_location_type(type ="body",id=11)) %>%
                       ph_with(value =com2, location =ph_location_type(type ="body",id=12)) %>%

                       ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Two_Chart_map=function(title,c1,c2,c1_t2,c1_t1,c2_t2,c2_t1,c1_s,c2_s,title1='TITLE'){

        my_ppt=add_slide(my_ppt,layout ="1_WITH NAME 2 CHARTS_MAP",master ="NIIF MONTHLY REPORTS" ) %>%
                         ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                         ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
                         ph_with(value =c2, location =ph_location_type(type ="chart",id=2)) %>%
                         ph_with(value =title, location =ph_location_type(type ="body",id=1)) %>%
                         ph_with(value =c1_t1, location =ph_location_type(type ="body",id=6)) %>%
                         ph_with(value =c1_t2, location =ph_location_type(type ="body",id=7)) %>%
                         ph_with(value =c2_t1, location =ph_location_type(type ="body",id=8)) %>%
                         ph_with(value =c2_t2, location =ph_location_type(type ="body",id=5)) %>%
                         # Source1
                         ph_with(value =c1_s, location =ph_location_type(type ="body",id=2)) %>%
                        # Source2
                         ph_with(value =c2_s, location =ph_location_type(type ="body",id=3)) %>%
                         ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Chart_Medium_Table=function(title,c1,t1,c1_t,c1_t1,c1_s,title1="TITLE",com1=""){

      my_ppt=add_slide(my_ppt,layout ="WITH NAME CHART/ MEDIUM TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
                      ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                      ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
                      ph_with(value =t1, location =ph_location_type(type ="tbl",id=1)) %>%
                      ph_with(value =title, location =ph_location_type(type ="body",id=1)) %>%
                      ph_with(value =c1_t1, location =ph_location_type(type ="body",id=2)) %>%
                      ph_with(value =c1_t, location =ph_location_type(type ="body",id=3)) %>%
                      # Source1
                      ph_with(value =c1_s, location =ph_location_type(type ="body",id=4)) %>%
                      ph_with(value ="Note:", location =ph_location_type(type ="body",id=5)) %>%
                      ph_with(value =com1, location =ph_location_type(type ="body",id=6)) %>%

                      ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Chart_Small_Table=function(title,c1,t1,c1_t,c1_t1,c1_s,title1,com1=""){

    my_ppt=add_slide(my_ppt,layout ="WITH NAME CHART/ SMALL TABLE",master ="NIIF MONTHLY REPORTS") %>%
                      ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                      ph_with(value =c1,location =ph_location_type(type ="chart",id=1)) %>%

                      ph_with(value =t1,location =ph_location_template(type ="tbl",id=1, left = 10, top = 2.5)) %>%
                      ph_with(value =title, location =ph_location_type(type ="body",id=1)) %>%
                      ph_with(value =c1_t1, location =ph_location_type(type ="body",id=2)) %>%
                      ph_with(value =c1_t, location =ph_location_type(type ="body",id=3)) %>%
                      # Source1
                      ph_with(value =c1_s, location =ph_location_type(type ="body",id=4)) %>%
                      ph_with(value ="Note:", location =ph_location_type(type ="body",id=5)) %>%
                      ph_with(value =com1, location =ph_location_type(type ="body",id=6)) %>%
                      ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}


## -------------------------------------
Single_Chart=function(title2,c1,c1_t,c1_t1="Chart header1",c1_s,title1='TITLE',com=""){

    my_ppt=add_slide(my_ppt,layout ="WITH NOTE BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                     ph_with(value =c1, location =ph_location_type(type ="tbl",id=1)) %>%
                     ph_with(value =title2, location =ph_location_type(type ="body",id=1)) %>%
                     ph_with(value =c1_t1, location =ph_location_type(type ="body",id=4))%>%
                     ph_with(value =c1_t, location =ph_location_type(type ="body",id=5)) %>%
                     ph_with(value =c1_s, location =ph_location_type(type ="body",id=2)) %>%
                     ph_with(value ="Note:", location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value =com, location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}

#BLANK CHART
Blank_chart=function(title){

      my_ppt=add_slide(my_ppt,layout ="1_BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
                      ph_with(value =title, location =ph_location_type(type ="body",id=4))}

#TABLE oF CONTENT
tabe_content=function(S1,C1,S2,C2,S3,C3,S4,C4,S5,C5){

    my_ppt=add_slide(my_ppt,layout ="1_TABLE OF CONTENTS",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value ="Table of contents",location=ph_location_type(type ="body",id=1)) %>%

                     ph_with(value ="04", location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value ="19", location =ph_location_type(type ="body",id=4)) %>%
                     ph_with(value ="35", location =ph_location_type(type ="body",id=5)) %>%
                     ph_with(value="40", location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value ="47", location =ph_location_type(type ="body",id=7)) %>%

                     ph_with(value =S1,location =ph_location_type(type ="body",id=2)) %>%
                     ph_with(value =C1, location =ph_location_type(type ="body",id=8)) %>%

                     ph_with(value =S2, location =ph_location_type(type ="body",id=9)) %>%
                     ph_with(value =C2, location =ph_location_type(type ="body",id=10)) %>%
                     ph_with(value =S3, location =ph_location_type(type ="body",id=11)) %>%
                     ph_with(value =C3, location =ph_location_type(type ="body",id=12)) %>%
                     ph_with(value =S4, location =ph_location_type(type ="body",id=13)) %>%
                     ph_with(value =C4, location =ph_location_type(type ="body",id=14)) %>%
                     ph_with(value =S5, location =ph_location_type(type ="body",id=15)) %>%
                     ph_with(value =C5, location =ph_location_type(type ="body",id=16))}

#SECTION_BREAKER
section_breaker=function(title,Comment){

  my_ppt=add_slide(my_ppt,layout ="SECTION BREAKER",master ="NIIF MONTHLY REPORTS" ) %>%
                   ph_with(value =title,location=ph_location_type(type ="body",id=1)) %>%
                   ph_with(value =Comment,location =ph_location_type(type ="body",id=2))}

executive_summary=function(title,summary,t1,t2,t3,t4,t5,t6,t7,t8){

  my_ppt=add_slide(my_ppt,layout ="1_8 EXECUTIVE SUMMARY",master ="NIIF MONTHLY REPORTS" ) %>%
                  ph_with(value =title,location =ph_location_type(type ="title",id=1)) %>%
                  ph_with(value =summary,location =ph_location_type(type ="body",id=1)) %>%
                  ph_with(value ="Key Trends: September/October 2022",
                          location =ph_location_type(type ="body",id=2)) %>%

                  ph_with(value =t1, location =ph_location_type(type ="body",id=5)) %>%
                  ph_with(value =t2, location =ph_location_type(type ="body",id=20)) %>%
                  ph_with(value =t3, location =ph_location_type(type ="body",id=26)) %>%
                  ph_with(value =t4, location =ph_location_type(type ="body",id=21)) %>%
                  ph_with(value =t5, location =ph_location_type(type ="body",id=22)) %>%
                  ph_with(value =t6, location =ph_location_type(type ="body",id=23)) %>%
                  ph_with(value =t7, location =ph_location_type(type ="body",id=24)) %>%
                  ph_with(value =t8, location =ph_location_type(type ="body",id=25))}

contact=function(Published_Date){

my_ppt=add_slide(my_ppt,layout ="CONTACTS",master ="NIIF MONTHLY REPORTS" ) %>%
                 ph_with(value ="Contact",location =ph_location_type(type ="body",id=1)) %>%
                 ph_with(value =paste0('volume 16\nPublished on ',Published_Date),
                         location =ph_location_type(type ="body",id=2)) %>%

                 ph_with(value ="PHOTO", location =ph_location_type(type ="body",id=3)) %>%
                 ph_with(value ="PHOTO", location =ph_location_type(type ="body",id=4)) %>%

                 ph_with(value="Name1",location =ph_location_type(type ="body",id=6)) %>%
                 ph_with(value ="Email1:", location =ph_location_type(type ="body",id=7)) %>%

                 ph_with(value ="Name2", location =ph_location_type(type ="body",id=8)) %>%
                 ph_with(value ="Email2:", location =ph_location_type(type ="body",id=9))
}


## -------------------------------------
abdul_table_type1=function(title,s_t1,table,ch1,csh1,src,
                           l1,t1,l2,t2,l3,t3,l4,t4,
                           l5,t5,w2,n1){

  my_ppt=add_slide(my_ppt,layout ="WITH NOTE BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
      ph_with(value =title,location=ph_location_type(type ="title")) %>%
      ph_with(value =table,location =ph_location_template(type ="tbl",id=1,
                                                          left =l1, top =t1)) %>%

      ph_with(value =s_t1,location =ph_location_type(type ="body",id=1)) %>%
      ph_with(value =ch1,location=ph_location_template(type="body",id=4,
                                                       left =l2,top =t2,width =w2)) %>%

      ph_with(value =csh1,location =ph_location_template(type ="body",id=5,
                                                         left =l3,top =t3,width =w2)) %>%
      ph_with(value =src,location =ph_location_template(type ="body",id=2, left = l4, top = t4, width = w2)) %>%

      ph_with(value =n1,location =ph_location_template(type ="body",id=6,
                                                        left = l5, top = t5,width =11.98,height =0.3
                                                           )) %>%

      ph_with(value ="Note:",location =ph_location_template(type ="body",id=3,
                                                        left = 1.35, top =7.15)) %>%
      ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}

abdul_table_type2=function(title,s_t1,table,ch1,csh1,src,l1,t1,l2,t2,w=8.27){

  my_ppt=add_slide(my_ppt,layout ="1_BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
      ph_with(value =title,location=ph_location_type(type ="title")) %>%
      ph_with(value =table,location =ph_location_template(type ="tbl",id=1,left =l1,top=t1)) %>%

      ph_with(value =s_t1, location =ph_location_type(type ="body",id=1)) %>%
      ph_with(value =ch1,location =ph_location_type(type ="body",id=3)) %>%
      ph_with(value =csh1, location =ph_location_type(type ="body",id=4)) %>%

      ph_with(value =src,location =ph_location_template(type ="body",id=2, left = l2, top = t2,width =w,height =0.1)) %>%
      ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))

}



## -------------------------------------
roundexcel = function(x,digits=0){
  posneg = sign(x)
  z = abs(x)*10^digits
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^digits
  z*posneg
}


table_preprocessing <- function(data,frequency_normalizer = '',unit= 'NA',variable = 'NA',sector = 'NA',calculate_gr = FALSE,divisor = 0,rounder = 0,month_num=13){

  tryCatch({


  last_date <- today()
  start_date <- timeFirstDayInMonth(last_date %m-% months(month_num))
  end_date <- timeFirstDayInMonth(last_date %m-% months(1))

  if (frequency_normalizer == 'Monthly'){
    all_dates <- format(seq(start_date,end_date,by="month"), "%Y-%m-%d")
    dates_df <- data.frame(Relevant_Date = c(all_dates))
    dates_df$Relevant_Date <- as.Date(timeLastDayInMonth(dates_df$Relevant_Date))
    dates_df['Value'] <- NA
    max_date <- max(dates_df$Relevant_Date)
    min_date <- min(dates_df$Relevant_Date)
    min_date <- min_date %m-% months(1)
    data$Value <- data$Value + 1
    data <- rbind(data,dates_df)
    data <- data[order(as.Date(data$Relevant_Date, format="%Y-%m-%d")), ]
    rownames(data) = seq(length=nrow(data))
    data[is.na(data)] <- 0


    data <- aggregate(data['Value'], by=list(data$Relevant_Date), FUN=sum)
    names(data)[1]<-"Relevant_Date"
    names(data)[2]<-"Value"

    data <- data[data$Relevant_Date >= min_date &  data$Relevant_Date <= max_date, ]

    data$Value <- data$Value - 1

    data$Value = ifelse(data$Value == -1 ,NA, data$Value)
  }


  data$Relevant_Date = format(as.Date(data$Relevant_Date),"%b-%y")

  if (calculate_gr == TRUE){
    growth_rate = roundexcel(((data$Value[length(data$Value)]/data$Value[1]) - 1)*100, 1)
  }
  # data["Value"] = roundexcel(data["Value"])

  if (divisor != 0){
    data["Value"] =data["Value"]/divisor

    data["Value"] = roundexcel(data["Value"], rounder)
  }
  else{
    data["Value"] = roundexcel(data["Value"], rounder)
  }


  if (calculate_gr == TRUE){
    data[nrow(data) + 1,] <- c('Growth (% yoy)', growth_rate)
  }



  data <- transpose(data)


  names(data) <- data[1,]

  data <- data[-1,]


  if (unit != 'NA'){
    data$Units <- unit
    data <- data %>%
      select(Units, everything())
  }

  if (variable != 'NA'){
    data$Variable <- variable
    data <- data %>%
      select(Variable, everything())
  }

  if (sector != 'NA'){
    data$Sector <- sector
    data <- data %>%
      select(Sector, everything())
  }

  return (data)
},
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)

      })

}

economic_indicator_table <- function(data, has_main_sector = FALSE,main_sector_bg = "GRAY 96",has_units = FALSE,rename_unit_col = 'Units',set_header_bg = 'white',padding_vals = list(),make_bold = c(),make_col_bold=c(),hlines = c(),vlines = c(),background_vals = list(),color_coding = FALSE,alpha = 0.5,median_change = list(),invert_map = c(),rounder_exeptions = c(), replace_null = '', caption = '',font_size = 9, var_col_width = 2,unit_col_width = 1,other_col_width = 0.61,line_space=0,row_height=0.05,yoy=FALSE){

  tryCatch({
  all_variables <- data$Variable[!duplicated(data$Variable)]

  all_df = data.frame()

  if (has_main_sector == TRUE){
    all_sectors <- data$Sector[!duplicated(data$Sector)]
    for (i in all_sectors){
      if (i == ''){
        temp_df <- subset(data,data$Sector == i)
        result <- temp_df[ , !(names(temp_df) %in% c("Sector","Variable","Units"))]
        row.names(result) <- temp_df$Variable
        all_df <- rbind(all_df,result)
      }
      else {
        temp_df <- subset(data,data$Sector == i)
        temp_df[nrow(temp_df) + 1,] <- i
        temp_df <- subset(temp_df, select = -c(Sector))
        row.names(temp_df) <- NULL
        temp_df <- temp_df %>% slice(nrow(temp_df), 1:nrow(temp_df)-1)
        result <- temp_df[-1]
        row.names(result) <- temp_df$Variable
        result[i,] <- ''
        all_df <- rbind(all_df,result)
      }

    }
    all_df <- tibble::rownames_to_column(all_df, "Variable")
  }
  else{
    all_df <- data
  }

  # print(all_df)
  if (has_units == TRUE){
    date_cols <- colnames(all_df)[c(3:length(colnames(all_df)))]
  }
  else{
    date_cols <- colnames(all_df)[c(2:length(colnames(all_df)))]
  }

  all_df[date_cols] <- sapply(all_df[date_cols],as.numeric)

  set_flextable_defaults(font.family = 'Calibri')
  print(all_df)
  ft <- flextable(all_df)
  # print(ft)


  ft <- theme_alafoli(ft)


  if (caption != ''){
    ft <- add_header_lines(ft,values = caption)
    ft <- border_remove(ft)
    ft <- theme_alafoli(ft)
    ft <- merge_at(ft, i = 1, part = 'header')
    ft <- align(ft, i = 1, align = "center", part = "header")
  }
  print(ft)
  ### Heat map
  if (color_coding == TRUE){
    #paletteer_c("ggthemes::Red-Green-Gold Diverging", 10000)

    for (m in all_variables){
      if (m %in% invert_map){
        mypal <- colorRampPalette(c('#22763F', '#69A760', '#73AC60', '#B0C162', '#D4C964', '#EACF65', '#F8A956', '#D44D44'))(10000)


        pospal <- as.character(paletteer_c("ggthemes::Red-Gold", 100))
        negpal <- rev(as.character(paletteer_c("ggthemes::Green-Gold", 100)))
      }
      else{
        mypal <- colorRampPalette(c("#D44D44", "#F8A956","#EACF65","#D4C964","#B0C162","#73AC60","#69A760","#22763F"))(10000)
        # negpal <- colorRampPalette(c("#BE2A3E","#D44D44", "#E36148","#EC754A","#F5894C","#F8A254","#F5CA63"))(30)
        # pospal <- colorRampPalette(c("#F5CA63", "#C2C563","#87B560","#5DA25F","#3C8D53","#22763F"))(30)

        negpal <- rev(as.character(paletteer_c("ggthemes::Red-Gold", 100)))
        pospal <- as.character(paletteer_c("ggthemes::Green-Gold", 100))
      }

      if (m %in% ls(median_change)){
        med = median_change[m][[1]]
      }
      else{
        med = 0
      }


      temp_df = subset(all_df,all_df$Variable == m)
      temp_df = temp_df[ , !(names(temp_df) %in% c("Variable","Units"))]
      temp_df = transpose(temp_df)
      temp_df = na.omit(temp_df)
      max_val = max(temp_df[,1])
      min_val = min(temp_df[,1])
      row_formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
      neg_df = subset(all_df,all_df["Variable"] == m)
      neg_df <- neg_df %>%
        gather(var, val, all_of(date_cols)) %>%
        group_by(var) %>%
        summarise(res = val[!is.na(val)] < med)
      neg_cols = subset(neg_df,neg_df$res == TRUE)$var
      pos_cols = subset(neg_df,neg_df$res == FALSE)$var

      neg_cols = date_cols[! date_cols %in% pos_cols]
      pos_cols = date_cols[! date_cols %in% neg_cols]
      if (length(neg_cols) == 0){
        lower_limit = med#min_val - (abs(min_val)*0.1)
        upper_limit = max_val + (abs(max_val)*alpha)
        colourer_pos <- scales::col_numeric(palette = pospal,domain = c(lower_limit, upper_limit))
        ft <- ft %>% bg(bg = colourer_pos,i = row_formula,j = pos_cols , part = "body")
      }
      else if (length(pos_cols) == 0){
        lower_limit = min_val - (abs(min_val)*alpha)
        upper_limit = med#max_val + (abs(max_val)*0.1)
        colourer_neg <- scales::col_numeric(palette = negpal,domain = c(lower_limit, upper_limit))
        ft <- ft %>% bg(bg = colourer_neg,i = row_formula,j = neg_cols , part = "body")
      }
      else{
        # abs_min_val = abs(min_val)
        # abs_max_val = abs(max_val)
        # lower_limit = max(c(abs_min_val,abs_max_val))*-1 - (max(c(abs_min_val,abs_max_val))*0.1)
        # upper_limit = max(c(abs_min_val,abs_max_val)) + (max(c(abs_min_val,abs_max_val))*0.1)
#
        lower_limit = min_val - (abs(min_val)*alpha)
        upper_limit = max_val + (abs(max_val)*alpha)
#
        colourer_neg <- scales::col_numeric(palette = negpal,domain = c(lower_limit, med))
        colourer_pos <- scales::col_numeric(palette = pospal,domain = c(med, upper_limit))
        ft <- ft %>% bg(bg = colourer_neg,i = row_formula,j = neg_cols , part = "body")
        ft <- ft %>% bg(bg = colourer_pos,i = row_formula,j = pos_cols , part = "body")
      }


    }

    if (yoy ==TRUE){for (l in date_cols){
        ft <- ft %>% bg(bg = '#D3D3D3',j = c(l) ,i = is.na(all_df[l]), part = "body")
      }
    }
    else{
      for (l in date_cols){
          ft <- ft %>% bg(bg = 'white',j = c(l) ,i = is.na(all_df[l]), part = "body")
        }
    }


  }

  #Padding if specified  : Please mention padding value as "pad_" and followed by the pad value
  ###  Example : if you want to pad by 10 then your list input should be pad_10
  if (length(padding_vals) != 0){
    for (m in names(padding_vals)){
      val = as.integer(sub("pad_","",m))
      for (i in padding_vals[[m]]){
        row_formula = as.formula(paste("~ Variable %in% '",i,"'",sep = ''))
        ft <- padding(ft,i = row_formula,j = 1, padding.left = val , part = "body")
      }
    }
  }


  ## Background Color if specified : Please mention color value as "col_" and followed by the color hex value
  ###  Example : if you want to color of #D44D44 then your list input should be col_D44D44
  if (length(background_vals) != 0){
    for (m in names(background_vals)){
      val = sub("col_","#",m)
      for (i in background_vals[[m]]){
        row_formula = as.formula(paste("~ Variable %in% '",i,"'",sep = ''))
        ft <- bg(ft,bg = val,i = row_formula , part = "body")
      }
    }
  }

  ## Make specified rows as bold
  if (length(make_bold) != 0){
    for (m in make_bold){
      formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
      ft <- ft %>% bold(i = formula, part = 'body')
    }
  }
  ## Make specified columns as bold
  if (length(make_col_bold) != 0){
    for (m in make_col_bold){
      # formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
      ft <- ft %>% bold(j = m, part = 'body')
    }
  }




  ft <- set_header_labels(ft, Variable = " ")
  ft <- ft %>% color(color = 'black', part = 'all')
  ft <- ft %>% bold(part = 'header')

  ## Makes main sectors bold and gives background color
  if (has_main_sector == TRUE){
    for (m in all_sectors){
      formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
      ft <- ft %>% bold(i = formula, part = 'body') %>% bg(i = formula, bg = main_sector_bg, part = 'body')
    }
  }


  ft <- ft %>% fontsize(size = font_size, part = 'all')


  ## Number Formatting
  for (m in all_variables){
    row_formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
    if (m %in% rounder_exeptions){
      ft <- ft %>% colformat_double(i = row_formula, big.mark = ",", digits = 1)
    }
    else{
      ft <- ft %>% colformat_double(i = row_formula, big.mark = ",", digits = 0)
    }
  }


  if (has_units == TRUE){index <- 3}else{index <- 2}
  ## Formats the negative values to in brackets,, example: -45 ------> (45)
  for (l in index:length(colnames(all_df))){
    null_vals = which(all_df[l] < 0)
    if (length(null_vals) != 0){
      for (m in null_vals){
        neg = paste("(",sub('-','',as.character(all_df[,l][m])),")",sep='')
        if (grepl('.', neg, fixed = TRUE) == FALSE){
          neg = paste("(",sub('-','',as.character(all_df[,l][m])),".0)",sep='')
        }
        ft <- compose(ft, i = m, j = l, as_paragraph(as_chunk(neg)))
      }
    }
  }



  ## Changes all null and empty strings to '  ', Required due to some random error
  for (l in 1:length(colnames(all_df))){
    null_vals = which((all_df[l] == '') | (is.na(all_df[l])))
    if (length(null_vals) != 0){
      for (m in null_vals){
        neg = '  '
        ft <- compose(ft, i = m, j = l, as_paragraph(as_chunk(neg)))
      }
    }
  }

  ## Replace Null values
  if (replace_null != ''){
    for (l in index:length(colnames(all_df))){
      null_vals = which((all_df[l] == '') | (is.na(all_df[l])))
      if (length(null_vals) != 0){
        for (m in null_vals){
          neg = replace_null
          ft <- compose(ft, i = m, j = l, as_paragraph(as_chunk(neg)))
        }
      }
    }
  }


  if (has_units == TRUE){
    ft <- width(ft, j = c('Variable') ,width = var_col_width)
    ft <- width(ft, j = c('Units') ,width = unit_col_width)
    ft <- width(ft, j = date_cols, width = other_col_width)
  } else {
    ft <- width(ft, j = c('Variable') ,width = var_col_width)
    ft <- width(ft, j = date_cols, width = other_col_width)
  }

  # if (has_units == TRUE){
  #   ft <- width(ft, j = c('Variable') ,width = 2)
  #   ft <- width(ft, j = c('Units') ,width = 1)
  #   ft <- width(ft, j = date_cols, width = 0.61)
  # } else {
  #   ft <- width(ft, j = c('Variable') ,width = 2.2)
  #   ft <- width(ft, j = date_cols, width = 0.65)
  # }

  ft <- height_all(ft,height =row_height)
  ft <- hrule(ft, rule = 'exact', part = 'body')
  ft <- fit_to_width(ft, max_width = 12, unit = 'in')
  # ft <- fit_to_width(ft, max_width = 20, unit = 'in')
  # ft <- set_table_properties(ft, layout = "fixed")


  big_border = fp_border(color="black", width = 1)
  # big_border <- fix_border_issues(big_border)  ##########Santonu
  ft <- hline_bottom(ft, part="body", border = big_border)

  ## Adding horizontal lines below the specified rows
  small_border = fp_border(color="black", width = 0.5)
  if (length(hlines) != 0){
    for (m in hlines){
      formula = as.formula(paste("~ Variable %in% '",m,"'",sep = ''))
      ft <- ft %>% hline(i = formula, part = 'body', border = small_border)
    }
  }

  ## Adding Vertical lines below the specified rows
  dotted_border = fp_border(color = "black", style = "dotted", width = 0.5)
  if (length(vlines) != 0){
    for (m in vlines){
      ft <- ft %>% vline(j = m, part = 'all', border = dotted_border)
    }
  }

  if (has_units == TRUE){ft <- set_header_labels(ft, Units = rename_unit_col)}

  ft <- ft %>% bg(bg = set_header_bg, part = 'header')
  if (line_space != 0){
    ft <- ft %>% line_spacing(space = line_space, part = 'body')
  }

  return (ft)
},
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0("'",error_type,"',"),
                  paste0("'",error_msg,"',"),
                  paste0(rep("''", 5), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)

      })
}


read_query <- function(widget_id, p, frequency = '',quarter=FALSE,convert_na=FALSE){

  tryCatch({
      clk  <- DBI::dbConnect(RClickhouse::clickhouse(), dbname =DB_cl,host = host_cl,
                             port = port_cl,user = user_cl,password =password_cl)

      pg  <- DBI::dbConnect(RPostgres::Postgres(), dbname =DB_pg,host = host_pg,
                            port = port_pg,user = user_pg,password =password_pg)

      chart_id_num=widget_id
      period=p

      qstring='select * from widgets_new where widget_id=chart_id'
      query_by_id=str_replace(qstring,"chart_id",glue({chart_id_num}))

      res =dbGetQuery(pg,query_by_id)
      d1=res[,"query_param"]
      query_1=fromJSON(d1, flatten=TRUE)
      y_lab=query_1[7][[1]][[1]]
      plot_type=query_1[6][[1]][[1]]
      if (quarter==TRUE){
            final_query=str_replace(query_1[1],"@@periodQtr@@", glue({period}))

      }else{final_query=str_replace(query_1[1], "@@period@@", glue({period}))}

      print(final_query)
      data = dbGetQuery(clk,final_query)

      if (frequency == 'daily'){
        print('daily data')
      }else {
        data$Relevant_Date <- as.Date(timeLastDayInMonth(data$Relevant_Date))
      }

      dbDisconnect(clk)
      dbDisconnect(pg)

      d1=tolower(final_query)
      str_remove(d1,'where')
      s1=str_split(d1,'where')[[1]][1]
      s2=str_split(s1,'from')[[1]][2]
      mv_table=str_trim(s2, side="both")
      df1=data.frame(ID=c(widget_id),MV_TABLE=c(mv_table))
      write.xlsx(df1,file.path(excel_dir, paste0('df_niif_',widget_id,'_.xlsx')))

      data=data[!duplicated(data[c("Relevant_Date")]), ]
      if (convert_na==TRUE){
        data=replace(data,is.na(data),0.0)
      }

      return (data)
    },
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      # dbExecute(con, query)
      # dbDisconnect(con)

      })

}

growth_and_market_cap_calc <- function(df,variable,cap_df = data.frame(),cap_divisor = 10^9){

  tryCatch({
  last_date <- as.Date(timeFirstDayInMonth(max(df$Relevant_Date))) %m-% days(1)
  #For last day of month
  if (as.Date(max(df$Relevant_Date))==as.Date(timeLastDayInMonth(max(df$Relevant_Date)))){
      last_date <- as.Date(max(df$Relevant_Date))
  }


  print(last_date)

  dates = list(One_Week = last_date %m-% weeks(1), One_Month = last_date %m-% months(1), Three_Month = last_date %m-% months(3), Six_Month = last_date %m-% months(6), Twelve_Month = last_date %m-% months(12))

  main_df = data.frame(Variable = variable)

  # Market Cap Calculation
  ##Changed by Santonu
  if (length(cap_df) != 0){
    main_df$Current = roundexcel(subset(cap_df,cap_df$Relevant_Date == last_date)$Value/cap_divisor, 1)
    main_df$Peak = roundexcel(max(cap_df$Value)/cap_divisor, 1)
  }


  # for (date in names(dates)){
  #   limit = 0
  #   while (length(subset(df,df$Relevant_Date == dates[date][[1]])$Value) == 0 & limit < 4){
  #     dates[date] = as.Date(ymd(dates[date][[1]]) - days(1) , format = '%Y-%m-%d')
  #     limit = limit + 1
  #   }
  #   if (limit == 4){
  #     main_df[date] = NA
  #   }
  #   else{
  #     main_df[date] = roundexcel(((subset(df,df$Relevant_Date == last_date)$Value/subset(df,df$Relevant_Date == dates[date][[1]])$Value) - 1)*100, 1)
  #   }
  # }

  ## Growth Calculation
  df <- padr::pad(df)
  df <- df[order(as.Date(df$Relevant_Date, format="%Y-%m-%d")),]
  # df <- tibble::as_tibble(df)
  # df %>% tidyr::fill(Value, .direction = 'updown')
  # df <- as.data.frame(df)
  df <- zoo::na.locf(zoo::na.locf(df), fromLast = FALSE)

  for (date in names(dates)){
    if (length(subset(df,df$Relevant_Date == dates[date][[1]])$Value) == 0){
      main_df[date] = NA
    }
    else{
      main_df[date] = roundexcel(((subset(df,df$Relevant_Date == last_date)$Value/subset(df,df$Relevant_Date == dates[date][[1]])$Value) - 1)*100, 1)

      print(subset(df,df$Relevant_Date == last_date)$Value)
      print(subset(df,df$Relevant_Date == dates[date][[1]])$Value)
      print(dates[date][[1]])
    }
  }

  return (main_df)
},
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)

      })
}

table_preprocessing_annual <- function(data,frequency_normalizer = '',period=3,unit= 'NA',
                                       variable = 'NA',sector = 'NA',calculate_gr = FALSE,
                                       divisor = 0,rounder = 0,fy_format=FALSE){

  tryCatch({
  data$Relevant_Date=as.Date(timeFirstDayInMonth(data$Relevant_Date))

  max_date <- max(data$Relevant_Date)
  min_date_org <- min(data$Relevant_Date)

  # start_date <- (max_date %m-% years(as.numeric(period)))
  if (frequency_normalizer=='quarter'){
    start_date <- (max_date %m-% months((period-1)*3))
  }else{
    start_date <- (max_date %m-% years(as.numeric(period)))
  }

  end_date <- (max_date)
  all_dates <- format(seq(start_date,end_date,by=frequency_normalizer), "%Y-%m-%d")
  dates_df <- data.frame(Relevant_Date = c(all_dates))
  dates_df$Relevant_Date <- as.Date((dates_df$Relevant_Date))

  max_date <- max(dates_df$Relevant_Date)
  min_date <- min(dates_df$Relevant_Date)

  if ((as.Date(start_date)!=as.Date(min_date_org))==TRUE){
        dates_df['Value'] <- NA
        dates_df=dates_df[(dates_df$Relevant_Date)<as.Date(min_date_org),]
        data <- rbind(data,dates_df)
        data <- data[order(as.Date(data$Relevant_Date, format="%Y-%m-%d")), ]
        rownames(data) = seq(length=nrow(data))
        data <- data[data$Relevant_Date >= min_date &  data$Relevant_Date <= max_date, ]
        data[is.na(data)] <- 0

  }else{
        data=merge(dates_df,data,by="Relevant_Date")
        dates_df['Value'] <- NA
        data$Value <- data$Value + 1

        data <- rbind(data,dates_df)
        data <- data[order(as.Date(data$Relevant_Date, format="%Y-%m-%d")), ]

        rownames(data) = seq(length=nrow(data))
        data[is.na(data)] <- 0

        data <- aggregate(data['Value'], by=list(data$Relevant_Date), FUN=sum)
        names(data)[1]<-"Relevant_Date"
        names(data)[2]<-"Value"
        data <- data[data$Relevant_Date >= min_date &  data$Relevant_Date <= max_date, ]
        data$Value <- data$Value - 1
        data$Value = ifelse(data$Value == -1 ,NA, data$Value)
    }



  data <- data[order(as.Date(data$Relevant_Date, format="%Y-%m-%d")), ]

  f1=function(x){

      max_date=as.Date(x[1])
      current_month=format(max_date,format="%b")
      current_month_num=format(max_date,"%m")

      if (as.numeric(current_month_num)>=4) {
              current_f_year=format(max_date %m+% years(1))
      }else {
          current_f_year=format(max_date) }
      }



  f2=function(x){

      max_date=as.Date(x[1])
      current_month=format(max_date,format="%b")
      current_month_num=format(max_date,"%m")
      current_f_year=format(max_date %m-% months(3))
  }
  f3=function(x){
     max_date=as.Date(x[1])
     current_month_num=format(max_date,"%m")
     print(max_date)
     print(current_month_num)


  if (as.numeric(current_month_num)>=4){
                current_f_year=format(max_date+years(1),format="%Y")
                current_f_year=paste0(as.character(current_f_year),'-03-31')
                 print(current_f_year)



  }
  else{

      current_f_year=format(max_date,format="%Y" )
      current_f_year=paste0(as.character(current_f_year),'-03-31')
      print(current_f_year)}
}
  if (frequency_normalizer=='quarter'){
     data["Fy_year"]=apply(data,1,f1)
     data["Qtr_mon"]=apply(data,1,f2)

     data$qtr=quarters(as.Date(data$Qtr_mon))
     data$year= format(as.Date(data$Fy_year),format ="%y")
     data=data  %>% mutate(Relevant_Date=paste0(qtr,"FY",year))
     data=data[,c('Relevant_Date','Value')]

     #
     # data$qtr=quarters(as.Date(data$Relevant_Date))
     # data$year= format(as.Date(data$Relevant_Date),format ="%y")
     # data=data  %>% mutate(Relevant_Date=paste0(qtr,"FY",year))
     # data=data[,c('Relevant_Date','Value')]

  }else if(frequency_normalizer=='month'){
      data$Relevant_Date = format(as.Date(data$Relevant_Date),"%b-%y")

  }else if(fy_format==TRUE){
      data["Fy_year"]=apply(data,1,f3)
      data <- data %>%
              group_by(Fy_year) %>%
              summarize(Value = sum(Value))
      names(data)[1]='Relevant_Date'
      data$year= format(as.Date(data$Relevant_Date),format ="%y")
      data=data  %>% mutate(Relevant_Date=paste0("FY",year,' '))
      data=data[,c('Relevant_Date','Value')]


  }else{
     data$Relevant_Date = format(as.Date(data$Relevant_Date),"%Y")
 }

  print(data)
  # data2=data[!duplicated(data[c("Relevant_Date")]), ]

  if (calculate_gr == TRUE){
    growth_rate = roundexcel(((data$Value[length(data$Value)]/data$Value[1]) - 1)*100, 1)
  }
  # data["Value"] = roundexcel(data["Value"])

  if (divisor != 0){
    data["Value"] = data["Value"]/divisor

    data["Value"] = roundexcel(data["Value"], rounder)
  }
  else{
    data["Value"] = roundexcel(data["Value"], rounder)
  }


  if (calculate_gr == TRUE){
    data[nrow(data) + 1,] <- c('Growth (% yoy)', growth_rate)
  }


  print(data)
  data <- transpose(data)
  names(data) <- data[1,]
  data <- data[-1,]


  if (unit != 'NA'){
    data$Units <- unit
    data <- data %>%
      select(Units, everything())
  }

  if (variable != 'NA'){
    data$Variable <- variable
    data <- data %>%
      select(Variable, everything())
  }

  if (sector != 'NA'){
    data$Sector <- sector
    data <- data %>%
      select(Sector, everything())
  }

  return (data)
},
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)

      })

}
new_row_insertion=function(data,new_row_name="Others",sub_row_name="Total"){
  tryCatch({
  df1=data
  df1 <- df1 %>% mutate_at(c(2:ncol(df1)), as.numeric)
  df2=df1 %>%
    bind_rows(df1 %>%
              filter(Variable %in% c(df1$Variable[!df1$Variable %in% c("Total")])) %>%
              summarise_if(is.numeric, funs(sum))) %>%
    mutate(Variable = ifelse(is.na(Variable), "Others_1", Variable))

  idx1=c(which(df2$Variable==sub_row_name))
  idx2=c(which(df2$Variable=="Others_1"))

  df <- rbind(df2, (as.numeric(df2[idx1, ]) - as.numeric(df2[idx2, ])))
  df[nrow(df),'Variable']=new_row_name
  df=df[-idx2,]
},
  error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =e[[1]]
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)

      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)

      })
}

heading=function(data,title=''){
    data_ends=data
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_ends$Relevant_Date)

    current_month=format(current_date,format="%b")
    current_day=format(current_date,format="%d")
    current_month_num=format(current_date,"%m")
    first_mon_mum=format(first_date,"%m")

    if (as.numeric(current_month_num)>=4){
                current_f_year=format(current_date+years(1),format="%Y")


    }else{current_f_year=format(current_date,format="%Y" )}

    if (as.numeric(first_mon_mum)>=4){
                first_f_year=format(first_date+years(1),format="%Y" )


    }else{first_f_year=format(first_date,format="%Y" )
    }
    if (title=='month'){
      sub_h=paste0(format(as.Date(timeFirstDayInMonth(first_date)),"%b '%y")," - ",
                                   format(current_date,"%b '%y"), sep = '')
    }else{
      sub_h=paste0("FY",first_f_year,"-","FY",
                    current_f_year," (",current_month," '",
                    format(current_date,format="%y"),")")

    }

}

f1=function(x){
     max_date=as.Date(x[1])
     current_month_num=format(max_date,"%m")
     print(max_date)
     print(current_month_num)


  if (as.numeric(current_month_num)>=4){
                current_f_year=format(max_date+years(1),format="%Y")
                current_f_year=paste0(as.character(current_f_year),'-03-31')
                 print(current_f_year)
  }
  else{
      current_f_year=format(max_date,format="%Y" )
      current_f_year=paste0(as.character(current_f_year),'-03-31')
      print(current_f_year)}
}


## -------------------------------------
clean_state_names = function(df,flag=1){

  df[df$State=='Pondicherry',"State"]<-"Puducherry"

  if (flag==1) {

    a=df[df$State=='Daman And Diu','total']
    b=df[df$State=='Dadra And Nagar Haveli','total']
    t=as.numeric(sum(a+b))

    data_3=data.frame(total=c(0,t),
                      State=c('Andaman & Nicobar','Daman and Diu and Dadra and Nagar Haveli'),
                      Relevant_Date=max(df$Relevant_Date))

    df=rbind(df,data_3)
  }

  df=df[,c("State","total","Relevant_Date")]

  return(df)
}
State_clean=function(df,flag=1){

    df[df$State=='Pondicherry',"State"]<-"Puducherry"
    df[df$State=='Mp',"State"]<-"Madhya Pradesh"
    df[df$State=='Up',"State"]<-"Uttar Pradesh"
    df[df$State=='Hp',"State"]<-"Himachal Pradesh"
    df[df$State=='J&K(UT) & Ladakh(UT)',"State"]<-"Jammu and Kashmir"
    df[df$State=='Chattisgarh',"State"]<-"Chhattisgarh"
    df[df$State=='Tamil Nadu',"State"]<-"Tamil Nadu"
    df[df$State=='Orissa',"State"]<-"Odisha"
    df[df$State=='New Delhi',"State"]<-"Delhi"
    df[df$State=='Maharashtra',"State"]<-"Maharashtra"
    df[df$State=='Nct Of Delhi',"State"]<-"Delhi"
    df[df$State=='Union Territories - Delhi',"State"]<-"Delhi"
    df[df$State=='Gujrat',"State"]<-"Gujarat"
    df[df$State=='The dadar and nagar haveli and daman and diu',"State"]<-"Daman and Diu and Dadra and Nagar Haveli"



    #For Railway Zones
    df[df$State=='Northern Frontier',"State"]<-"Northern Eastern  Frontier"
    df[df$State=='Northern Eastern Frontier',"State"]<-"Northern Eastern  Frontier"


    # df$value_1 <- ifelse(is.infinite(df$value_1), NA, df$value_1)

    if (flag==1) {

      a=df[df$State=='Daman And Diu','value_1']
      b=df[df$State=='Dadra And Nagar Haveli','value_1']
      t=as.numeric(sum(a+b))

      data_3=data.frame(value_1=c(0,t),
                        State=c('Andaman & Nicobar','Daman and Diu and Dadra and Nagar Haveli'))

      df=rbind(df,data_3)
    }

    return (df)
}
column_correction=function(df,flag=1,bubble_map=FALSE){
    df$State=str_to_title(df$State)
    df$State=df[1:nrow(df),which(names(df)=="State")]
    df$value_1=df[1:nrow(df),which(names(df)=="Var")]
    if (bubble_map==TRUE){
      df$value_a=df[1:nrow(df),which(names(df)=="value_a")]
      df=df[,c('State','value_1','value_a')]

    }else{
      df=df[,c('State','value_1')]
    }
    df=State_clean(df,flag)

    df[c('value_1')][sapply(df[c('value_1')], is.infinite)] <- NA
    return (df)
}
table_preprocessing_state_tbl <- function(data,unit= 'NA',variable = 'NA',
                                       sector = 'NA',divisor = 0,rounder = 0){

  # data=gst_yoy
  names(data)[which(names(data)=='value_1')]='Value'
  if (divisor != 0){
    data["Value"] = data["Value"]/divisor
    data["Value"] = roundexcel(data["Value"], rounder)
    print(data)
  }
  else{
    data["Value"] = roundexcel(data["Value"], rounder)
  }

  print(data)
  data <- transpose(data)
  names(data) <- data[1,]
  data <- data[-c(1,3),]


  if (unit != 'NA'){
    data$Units <- unit
    data <- data %>%
      select(Units, everything())
  }

  if (variable != 'NA'){
    data$Variable <- variable
    data <- data %>%
      select(Variable, everything())
  }

  if (sector != 'NA'){
    data$Sector <- sector
    data <- data %>%
      select(Sector, everything())
  }
  return (data)


}
get_sorted_state=function(df1,df2){
    df1=column_correction(df1,flag=0)
    df1=df1[order(-df1$value_1), ]
    names(df1)[which(names(df1)=='value_1')]='gdp'

    df2=column_correction(df2,flag=0)
    df2 <- subset(df2, !duplicated(df2))


    res <- left_join(df1, df2, by = "State")
    names(res)[which(names(res)=='Var')]='value_1'
    res=res[,c('State','value_1')]
    return (res)

}
get_data=function(mv_table,variable,act_amt='',Show_Date=FALSE,railway=FALSE,pow=FALSE,electricity = FALSE,bubble_map=FALSE,epfo=FALSE,vh_cat = '',fuel = '',toll=FALSE){


      clk  <- DBI::dbConnect(RClickhouse::clickhouse(),
                               dbname =DB_cl,host = host_cl,
                               port = port_cl,user = user_cl,password =password_cl)

      prev_month<-as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
      p1=paste0("'",prev_month,"'")



      q1=paste0("select State,GDP as Var,Relevant_Date from GDP_STATEWISE_TEMP")
      gst_state=df =dbGetQuery(clk,q1)


      if(electricity==TRUE){
        qu="select States as State,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"
      }
      else if(pow==TRUE){
        qu="select Region as State,variable as Var,Relevant_Date from mv_table where Relevant_Date in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"
      }
      else if(toll==TRUE){
        qu="select State,variable as Var,Relevant_Date from mv_table where Relevant_Date in (select max(Relevant_Date) from mv_table)"
      }
      else if(epfo==TRUE){
        qu="select State, SUM(variable) as Var,Relevant_Date from mv_table group by State,Relevant_Date HAVING Relevant_Date in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month) "
      }

      else if (railway==TRUE){
        qu="select Region as State,variable as Var,Relevant_Date from mv_table where Relevant_Date in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"
      }
      else if(bubble_map==TRUE){
        qu="select Latitude,Longitude,variable as Var,act_amt as value_a,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"

      }
      else if(vh_cat != ''){
        if(fuel == ''){qu="select State,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month) and Vehicle_Segment = 'vh_cat'"}
        else{
          qu="select State,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month) and Vehicle_Segment = 'vh_cat' and Fuel = 'ELECTRIC'"
        }

      }
      else{
         qu="select State,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"
      }

      if (bubble_map==TRUE){
        f_qu=str_replace_all(qu,'mv_table',mv_table)
        f_qu=str_replace_all(f_qu,"variable",variable)
        f_qu=str_replace_all(f_qu,"act_amt",act_amt)
        f_qu=str_replace_all(f_qu,"prev_month",p1)
        print(f_qu)
      }
      else if(vh_cat!=''){
        f_qu=str_replace_all(qu,'mv_table',mv_table)
        f_qu=str_replace_all(f_qu,"variable",variable)
        f_qu=str_replace_all(f_qu,"vh_cat",vh_cat)
        f_qu=str_replace_all(f_qu,"prev_month",p1)
        print(f_qu)
      }
      else{
        f_qu=str_replace_all(qu,'mv_table',mv_table)
        f_qu=str_replace_all(f_qu,"variable",variable)
        f_qu=str_replace_all(f_qu,"prev_month",p1)
        print(f_qu)
      }

      df =dbGetQuery(clk,f_qu)
      dbDisconnect(clk)
      rd = df$Relevant_Date

      df=df[df$Relevant_Date<=prev_month,]
      df=get_sorted_state(gst_state,df)

      df=df[1:10,1:ncol(df)]
      #df=df[order(df$State), ]
      df[df == -100] <- NA
      return(list(df,rd))
}


get_percent_data=function(mv_table,var1='State',variable,act_amt='',Show_Date=FALSE,railway=FALSE,
                          electricity = FALSE,bubble_map=FALSE,epfo=FALSE,vh_cat = ''){

       clk  <- DBI::dbConnect(RClickhouse::clickhouse(),
                                dbname =DB_cl,host = host_cl,
                                port = port_cl,user = user_cl,password =password_cl)

       prev_month<-as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
       p1=paste0("'",prev_month,"'")

       q1=paste0("select State,GDP as Var,Relevant_Date from GDP_STATEWISE_TEMP")
       gst_state=df =dbGetQuery(clk,q1)

     if(vh_cat!=''){

        qu="select States,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month) and Vehicle_Segment = 'vh_cat'"

       f_qu=str_replace_all(qu,'mv_table',mv_table)
       f_qu=str_replace_all(f_qu,"variable",variable)
       f_qu=str_replace_all(f_qu,"prev_month",p1)
       f_qu=str_replace_all(f_qu,"vh_cat",vh_cat)
       print(f_qu)

       x =dbGetQuery(clk,f_qu)

       qu=paste0("select States,variable as Var,Relevant_Date from mv_table where Relevant_Date in (select         max(Relevant_Date) - INTERVAL 12 MONTH from mv_table) and Vehicle_Segment = 'vh_cat'")

       f_qu=str_replace_all(qu,'mv_table',mv_table)
       f_qu=str_replace_all(f_qu,"variable",variable)
       f_qu=str_replace_all(f_qu,"vh_cat",vh_cat)
       print(f_qu)

       y =dbGetQuery(clk,f_qu)

       library(dplyr)

       df <- x %>%
         left_join(y, by = c("States"), suffix = c(".x", ".y")) %>%
         mutate(Var = 100 * (Var.x - Var.y) / Var.y) %>%
         select(States, Var)
       df$Relevant_Date <- x$Relevant_Date
       rd = df$Relevant_Date

       df=df[df$Relevant_Date<=prev_month,]
             df=get_sorted_state(gst_state,df)

             df=df[1:10,1:ncol(df)]
             #df=df[order(df$States), ]
             df[df == -100] <- NA
             return(list(df,rd))
            }

       else{
          qu="select var1 as States,variable as Var,Relevant_Date from mv_table where Relevant_Date  in (select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month)"

       f_qu=str_replace_all(qu,'mv_table',mv_table)
       f_qu=str_replace_all(f_qu,"variable",variable)
       f_qu=str_replace_all(f_qu,"var1",var1)
       f_qu=str_replace_all(f_qu,"prev_month",p1)
       print(f_qu)

       x =dbGetQuery(clk,f_qu)

       qu2=paste0("select var1 as States,variable as Var,Relevant_Date from mv_table where Relevant_Date in ((select max(Relevant_Date) from mv_table where Relevant_Date<=prev_month) - INTERVAL 12 MONTH) ")

         f_qu2=str_replace_all(qu2,'mv_table',mv_table)
         f_qu2=str_replace_all(f_qu2,"variable",variable)
         f_qu2 =str_replace_all(f_qu2,"var1",var1)
         f_qu2=str_replace_all(f_qu2,"prev_month",p1)
         print(f_qu2)

       y =dbGetQuery(clk,f_qu2)

       library(dplyr)

      df <- x %>%
       left_join(y, by = c("States"), suffix = c(".x", ".y")) %>%
       mutate(Var = 100 * ( (Var.x / Var.y) -1)) %>%
       select(States, Var)
     df$Relevant_Date <- x$Relevant_Date
     rd = df$Relevant_Date

     df=df[df$Relevant_Date<=prev_month,]
           df=get_sorted_state(gst_state,df)

           df=df[1:10,1:ncol(df)]
           #df=df[order(df$States), ]
           df[df == -100] <- NA
           return(list(df,rd))

       }


}


## -------------------------------------
clk  <- DBI::dbConnect(RClickhouse::clickhouse(), dbname =DB_cl,host = host_cl, port = port_cl,
                       user = user_cl,password =password_cl)
pg  <- DBI::dbConnect(RPostgres::Postgres(), dbname =DB_pg,host = host_pg, port = port_pg,
                      user = user_pg,password =password_pg)



## -------------------------------------
## GoI expenditure
goi = read_query(725985,14)
goi = goi[,c("Relevant_Date","Total")]
names(goi)[1]<-"Relevant_Date"
names(goi)[2]<-"Value"
## GoI expenditure growth
goi_gr = read_query(725967,14)
goi_gr = goi_gr[,c("Relevant_Date","growth")]
names(goi_gr)[1]<-"Relevant_Date"
names(goi_gr)[2]<-"Value"

##Gross tax revenue
gross_tax_rev = read_query(1498583,14)
names(gross_tax_rev)[1]<-"Relevant_Date"
names(gross_tax_rev)[2]<-"Value"

##Gross tax growth
gross_tax_rev_gr = read_query(1498598,14)
gross_tax_rev_gr = gross_tax_rev_gr[,c("Relevant_Date","growth")]
names(gross_tax_rev_gr)[1]<-"Relevant_Date"
names(gross_tax_rev_gr)[2]<-"Value"

## GST Collection
gst_coll= read_query(522953,14)
gst_coll=gst_coll[,c("Relevant_Date","Value")]
## GST Collection growth
gst_coll_gr= read_query(522952,14)
gst_coll_gr=gst_coll_gr[,c("Relevant_Date","growth")]
names(gst_coll_gr)[1]<-"Relevant_Date"
names(gst_coll_gr)[2]<-"Value"


## Index of industrial production
iip = read_query(318934,14)
iip = iip[,c("Relevant_Date","Index")]
names(iip)[2]<-"Value"

# Index of industrial production growth
iip_gr = read_query(1288704,14)
iip_gr = iip_gr[,c("Relevant_Date","growth")]
names(iip_gr)[1]<-"Relevant_Date"
names(iip_gr)[2]<-"Value"


## Index of Eight core industries
core_8 = read_query(318936,14)
core_8 = core_8[,c("Relevant_Date","Index")]
names(core_8)[2]<-"Value"
# names(core_8)[1]<-"Relevant_Date"
# names(core_8)[2]<-"Value"
## Index of Eight core industries growth
core_8_gr = read_query(724219,14)
core_8_gr = core_8_gr[,c("Relevant_Date","growth")]
names(core_8_gr)[1]<-"Relevant_Date"
names(core_8_gr)[2]<-"Value"

## Electricity Generation
elec_gen = read_query(2046376,14)
elec_gen=elec_gen[,c("Relevant_Date","Total")]
elec_gen$days = as.numeric(format(as.Date(elec_gen$Relevant_Date,format="%Y-%m-%d"), format = "%d"))
elec_gen$Total = elec_gen$Total*elec_gen$days
elec_gen=elec_gen[,c("Relevant_Date","Total")]
names(elec_gen)[1]<-"Relevant_Date"
names(elec_gen)[2]<-"Value"
# Electricity generation growth
elec_gen_gr = read_query(2046283,14)
elec_gen_gr=elec_gen_gr[,c("Relevant_Date","growth")]
names(elec_gen_gr)[1]<-"Relevant_Date"
names(elec_gen_gr)[2]<-"Value"


##Steel Production
steel = read_query(2041910,14)
names(steel)[1]<-"Relevant_Date"
names(steel)[2]<-"Value"

##Steel Production Growth
steel_gr = read_query(2041915,14)
names(steel_gr)[1]<-"Relevant_Date"
names(steel_gr)[2]<-"Value"

##Cement Production
cement = read_query(1525990,14)
names(cement)[1]<-"Relevant_Date"
names(cement)[2]<-"Value"

##Cement Production Growth
cement_gr = read_query(738892,14)
cement_gr = cement_gr[,c("Relevant_Date","Total1")]
names(cement_gr)[1]<-"Relevant_Date"
names(cement_gr)[2]<-"Value"

##Coal Production
coal = read_query(723037,14)
coal = coal[,c("Relevant_Date","Total")]
names(coal)[1]<-"Relevant_Date"
names(coal)[2]<-"Value"

##Coal Production Growth
coal_gr = read_query(273532,14)
coal_gr = coal_gr[,c("Relevant_Date","Growth")]
names(coal_gr)[1]<-"Relevant_Date"
names(coal_gr)[2]<-"Value"



##WPI Index
wpi_indx = read_query(1623468,14)
names(wpi_indx)[1]<-"Relevant_Date"
names(wpi_indx)[2]<-"Value"

##WPI Index Growth
wpi_indx_gr = read_query(725962,14)
names(wpi_indx_gr)[1]<-"Relevant_Date"
names(wpi_indx_gr)[2]<-"Value"

### Rail freight
rail_freight = read_query(284813,14)
rail_freight=rail_freight[,c("Relevant_Date","Volume")]
names(rail_freight)[2]<-"Value"

## Rail freight Growth
rail_freight_gr = read_query(284801,14)
rail_freight_gr=rail_freight_gr[,c("Relevant_Date","growth")]
names(rail_freight_gr)[1]<-"Relevant_Date"
names(rail_freight_gr)[2]<-"Value"

##Air Cargo
air_cargo= read_query(295110,14)
air_cargo=air_cargo[,c("Relevant_Date","Volume")]
names(air_cargo)[2]<-"Value"
## Air Cargo growth
air_cargo_gr= read_query(295115,14)
air_cargo_gr=air_cargo_gr[,c("Relevant_Date","Growth")]
names(air_cargo_gr)[1]<-"Relevant_Date"
names(air_cargo_gr)[2]<-"Value"

## Port Cargo
port_cargo = read_query(318937,14)
port_cargo=port_cargo[,c("Relevant_Date","Volume")]
names(port_cargo)[2]<-"Value"

# Port Cargo growth
port_cargo_gr = read_query(318907,14)
port_cargo_gr=port_cargo_gr[,c("Relevant_Date","Growth")]
names(port_cargo_gr)[1]<-"Relevant_Date"
names(port_cargo_gr)[2]<-"Value"


# E-way bills
e_way_bills = read_query(523012,14)
e_way_bills=e_way_bills[,c("Relevant_Date","Total")]
names(e_way_bills)[2]<-"Value"

# E-way bills growth
e_way_bills_gr = read_query(523035,14)
e_way_bills_gr=e_way_bills_gr[,c("Relevant_Date","growth")]
names(e_way_bills_gr)[1]<-"Relevant_Date"
names(e_way_bills_gr)[2]<-"Value"

## Merchandise Export
export = read_query(1502373,14)
export=export[,c("Relevant_Date","Value")]
names(export)[1]<-"Relevant_Date"
names(export)[2]<-"Value"

## Merchandise Export growth
export_gr = read_query(1502361,14)
export_gr=export_gr[,c("Relevant_Date","Growth")]
names(export_gr)[1]<-"Relevant_Date"
names(export_gr)[2]<-"Value"

## Merchandise Import
import = read_query(1502371,14)
import=import[,c("Relevant_Date","Value")]
names(import)[1]<-"Relevant_Date"
names(import)[2]<-"Value"

## Merchandise Import growth
import_gr = read_query(1502368,14)
import_gr=import_gr[,c("Relevant_Date","Growth")]
names(import_gr)[1]<-"Relevant_Date"
names(import_gr)[2]<-"Value"


##Services Exports
ser_exp = read_query(1502367,14)
ser_exp=ser_exp[,c("Relevant_Date","Value")]
names(ser_exp)[1]<-"Relevant_Date"
names(ser_exp)[2]<-"Value"

##Services exports growth
ser_exp_gr = read_query(1502352,14)
ser_exp_gr=ser_exp_gr[,c("Relevant_Date","Growth")]
names(ser_exp_gr)[1]<-"Relevant_Date"
names(ser_exp_gr)[2]<-"Value"

##Services Imports
ser_imp = read_query(1502360,14)
ser_imp=ser_imp[,c("Relevant_Date","Value")]
names(ser_imp)[1]<-"Relevant_Date"
names(ser_imp)[2]<-"Value"

##Services Imports growth
ser_imp_gr = read_query(1502356,14)
ser_imp_gr=ser_imp_gr[,c("Relevant_Date","Growth")]
names(ser_imp_gr)[1]<-"Relevant_Date"
names(ser_imp_gr)[2]<-"Value"

##Non-oil merchandize exports
n_oil_mer_exp = read_query(1502354,14)
names(n_oil_mer_exp)[1]<-"Relevant_Date"
names(n_oil_mer_exp)[2]<-"Value"

##Non-oil merchandize exports growth
n_oil_mer_exp_gr = read_query(1502355,14)
names(n_oil_mer_exp_gr)[1]<-"Relevant_Date"
names(n_oil_mer_exp_gr)[2]<-"Value"


##Non-oil merchandize Imports
n_oil_mer_imp = read_query(1502362,14)
names(n_oil_mer_imp)[1]<-"Relevant_Date"
names(n_oil_mer_imp)[2]<-"Value"

##Non-oil merchandize Imports growth
n_oil_mer_imp_gr = read_query(1502359,14)
names(n_oil_mer_imp_gr)[1]<-"Relevant_Date"
names(n_oil_mer_imp_gr)[2]<-"Value"


## -------------------------------------
goi_gr = table_preprocessing(goi_gr,frequency_normalizer = 'Monthly', '% yoy','Central government expenditure','Fiscal', rounder = 1)
gross_tax_rev_gr = table_preprocessing(gross_tax_rev_gr,frequency_normalizer = 'Monthly', '% yoy','Gross tax revenue','Fiscal', rounder = 1)
gst_coll_gr = table_preprocessing(gst_coll_gr,frequency_normalizer = 'Monthly', '% yoy','GST collection','Fiscal', rounder = 1)

iip_gr = table_preprocessing(iip_gr,frequency_normalizer = 'Monthly', '% yoy','Index of industrial production','Industry', rounder = 1)
core_8_gr = table_preprocessing(core_8_gr,frequency_normalizer = 'Monthly', '% yoy','Index of eight core industries','Industry', rounder = 1)
elec_gen_gr = table_preprocessing(elec_gen_gr,frequency_normalizer = 'Monthly', '% yoy','Electricity generation','Industry', rounder = 1)

steel_gr = table_preprocessing(steel_gr,frequency_normalizer = 'Monthly', '% yoy','Steel production','Industry', rounder = 1)
cement_gr = table_preprocessing(cement_gr,frequency_normalizer = 'Monthly', '% yoy','Cement production','Industry', rounder = 1)
coal_gr = table_preprocessing(coal_gr,frequency_normalizer = 'Monthly', '% yoy','Coal production','Industry', rounder = 1)

wpi_indx_gr = table_preprocessing(wpi_indx_gr,frequency_normalizer = 'Monthly', '% yoy','Wholesale price index','Industry', rounder = 1)



air_cargo_gr = table_preprocessing(air_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Air cargo','Logistics', rounder = 1)
rail_freight_gr = table_preprocessing(rail_freight_gr,frequency_normalizer = 'Monthly', '% yoy','Rail freight','Logistics', rounder = 1)
port_cargo_gr = table_preprocessing(port_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Port cargo','Logistics', rounder = 1)
e_way_bills_gr = table_preprocessing(e_way_bills_gr,frequency_normalizer = 'Monthly', '% yoy','E-way bills (volume)','Logistics', rounder = 1)


export_gr = table_preprocessing(export_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize exports','Trade', rounder = 1)
import_gr = table_preprocessing(import_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize imports','Trade', rounder = 1)
n_oil_mer_exp_gr = table_preprocessing(n_oil_mer_exp_gr,frequency_normalizer = 'Monthly', '% yoy','Non-oil merchandize exports','Trade', rounder = 1)
n_oil_mer_imp_gr = table_preprocessing(n_oil_mer_imp_gr,frequency_normalizer = 'Monthly', '% yoy','Non-oil merchandize imports','Trade', rounder = 1)
ser_exp_gr = table_preprocessing(ser_exp_gr,frequency_normalizer = 'Monthly', '% yoy','Services exports','Trade', rounder = 1)
ser_imp_gr = table_preprocessing(ser_imp_gr,frequency_normalizer = 'Monthly', '% yoy','Services imports','Trade', rounder = 1)

growth_df_1 = rbind(goi_gr,gross_tax_rev_gr,gst_coll_gr,iip_gr,core_8_gr,elec_gen_gr,steel_gr,
                    cement_gr,coal_gr,wpi_indx_gr,rail_freight_gr,port_cargo_gr,air_cargo_gr,
                    e_way_bills_gr,export_gr,import_gr,n_oil_mer_exp_gr,n_oil_mer_imp_gr,
                    ser_exp_gr,ser_imp_gr)



economic_indicator_pg1_gr=economic_indicator_table(growth_df_1,has_main_sector = TRUE,
                                        has_units = TRUE,color_coding = TRUE,
                                        invert_map = c("Wholesale price index"),
                                        median_change =
                                         list("Wholesale price index" = 6),

                                        rounder_exeptions = growth_df_1$Variable,
                                        font_size = 7.7,row_height=0.03,
                                        var_col_width = 2,unit_col_width = 0.8,
                                        other_col_width = 0.58)


economic_indicator_pg1_gr_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y"),
                                      " - ",
                                      format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"),
                                      sep = '')

economic_indicator_pg1_gr



goi = table_preprocessing(goi,frequency_normalizer = 'Monthly', 'INR trillion','Central government expenditure','Fiscal', divisor = 10^12, rounder = 1)
gross_tax_rev = table_preprocessing(gross_tax_rev,frequency_normalizer = 'Monthly', 'INR trillion','Gross tax revenue','Fiscal',divisor = 10^12, rounder = 1)
gst_coll = table_preprocessing(gst_coll,frequency_normalizer = 'Monthly', 'INR trillion','GST collection','Fiscal', divisor = 10^12, rounder = 1)


rail_freight = table_preprocessing(rail_freight,frequency_normalizer = 'Monthly', 'mn tonnes','Rail freight','Logistics', rounder = 1)
air_cargo = table_preprocessing(air_cargo,frequency_normalizer = 'Monthly', "'000 ton",'Air cargo','Logistics', divisor = 10^3, rounder = 1)#
port_cargo = table_preprocessing(port_cargo,frequency_normalizer = 'Monthly', 'mn tonnes','Port cargo','Logistics', rounder = 1)
e_way_bills = table_preprocessing(e_way_bills,frequency_normalizer = 'Monthly', 'million','E-way bills (volume)','Logistics', divisor = 10^6, rounder = 1)




iip = table_preprocessing(iip,frequency_normalizer = 'Monthly', 'Index','Index of industrial production','Industry', rounder = 1)
core_8 = table_preprocessing(core_8,frequency_normalizer = 'Monthly', 'Index','Index of eight core industries','Industry', rounder = 1)
elec_gen = table_preprocessing(elec_gen,frequency_normalizer = 'Monthly', 'billion kWh','Electricity generation','Industry', divisor = 10^3, rounder = 1)



steel = table_preprocessing(steel,frequency_normalizer = 'Monthly', 'mn tonnes','Steel production','Industry',divisor = 1, rounder = 1)
cement = table_preprocessing(cement,frequency_normalizer = 'Monthly', 'mn tonnes','Cement production','Industry',divisor = 1, rounder = 1)
coal = table_preprocessing(coal,frequency_normalizer = 'Monthly', 'mn tonnes','Coal production','Industry',divisor = 1, rounder = 1)
wpi_indx = table_preprocessing(wpi_indx,frequency_normalizer = 'Monthly', 'Index','Wholesale price index','Industry', rounder = 1)





export = table_preprocessing(export,frequency_normalizer = 'Monthly', 'USD billion','Merchandize exports','Trade', divisor = 10^9, rounder = 1)
import = table_preprocessing(import,frequency_normalizer = 'Monthly', 'USD billion','Merchandize imports','Trade', divisor = 10^9, rounder = 1)

n_oil_mer_exp = table_preprocessing(n_oil_mer_exp,frequency_normalizer = 'Monthly', 'USD billion','Non-oil merchandize exports','Trade',divisor = 10^9, rounder = 1)
n_oil_mer_imp = table_preprocessing(n_oil_mer_imp,frequency_normalizer = 'Monthly', 'USD billion','Non-oil merchandize imports','Trade',divisor = 10^9,rounder = 1)

ser_import = table_preprocessing(ser_imp,frequency_normalizer = 'Monthly', 'USD billion','Services imports','Trade', divisor = 10^9, rounder = 1)
ser_export = table_preprocessing(ser_exp,frequency_normalizer = 'Monthly', 'USD billion','Services exports','Trade', divisor = 10^9, rounder = 1)

indicator_df_1 = rbind(goi,gross_tax_rev,gst_coll,
                       iip,core_8,elec_gen,steel,cement,coal,wpi_indx,
                       rail_freight,port_cargo,air_cargo,e_way_bills,
                       export,import,n_oil_mer_exp,
                       n_oil_mer_imp,ser_export,ser_import)


economic_indicator_pg1 = economic_indicator_table(indicator_df_1,has_main_sector = TRUE,
                             has_units = TRUE,color_coding = FALSE,
                             rounder_exeptions = indicator_df_1$Variable,
                             font_size =8, var_col_width = 2.5,
                             unit_col_width = 0.8,line_space=0,row_height=0.03,
                             other_col_width = 0.58) #
economic_indicator_pg1

economic_indicator_pg1_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y"),
                                      " - ",
                                      format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"),
                                      sep = '')


## -------------------------------------

## Consumer Price Index
cpi = read_query(1445956,14)
cpi = cpi[,c("Relevant_Date","Total")]
names(cpi)[1]<-"Relevant_Date"
names(cpi)[2]<-"Value"
# CPI growth
cpi_gr = read_query(1445961,14)
cpi_gr = cpi_gr[,c("Relevant_Date","Inflation")]
names(cpi_gr)[1]<-"Relevant_Date"
names(cpi_gr)[2]<-"Value"

## Aggregate deposits
deposits = read_query(1443675 ,14)
deposits = deposits[,c("Relevant_Date","Value")]
# names(deposits)[1]<-"Relevant_Date"
# names(deposits)[2]<-"Value"
## Aggregate deposits growth

deposits_gr = read_query(1443676 ,14)
deposits_gr = deposits_gr[,c("Relevant_Date","growth")]
names(deposits_gr)[1]<-"Relevant_Date"
names(deposits_gr)[2]<-"Value"

## Outstanding Credit
credits = read_query(1443674 ,14)
credits = credits[,c("Relevant_Date","Value")]
# names(credits)[1]<-"Relevant_Date"
# names(credits)[2]<-"Value"

## Outstanding Credit growth
credits_gr = read_query(1443677 ,14)
credits_gr = credits_gr[,c("Relevant_Date","growth")]
names(credits_gr)[1]<-"Relevant_Date"
names(credits_gr)[2]<-"Value"


#ENERGY SECTION
##Electricity demand
elec_demand = read_query(725975,14)
elec_demand = elec_demand[,c("Relevant_Date","Total")]
names(elec_demand)[1]<-"Relevant_Date"
names(elec_demand)[2]<-"Value"

##Electricity demand growth
elec_demand_gr = read_query(725975,14)
elec_demand_gr = elec_demand_gr[,c("Relevant_Date","Total_Growth")]
names(elec_demand_gr)[1]<-"Relevant_Date"
names(elec_demand_gr)[2]<-"Value"

##Petrol consumtion
petrol_con = read_query(269930,14)
petrol_con = petrol_con[,c("Relevant_Date","Quantity_KMT")]
names(petrol_con)[1]<-"Relevant_Date"
names(petrol_con)[2]<-"Value"

##Petrol consumtion growth
petrol_con_gr = read_query(269835,14)
names(petrol_con_gr)[1]<-"Relevant_Date"
names(petrol_con_gr)[2]<-"Value"

#Automobile registrations

##Passenger vehicle
pv= read_query(526271,14)
names(pv)[1]<-"Relevant_Date"
names(pv)[2]<-"Value"


##Passenger vehicle
pv_gr = read_query(526302,14)
names(pv_gr)[1]<-"Relevant_Date"
names(pv_gr)[2]<-"Value"

##Three wheeler
three_w= read_query(526220,14)
names(three_w)[1]<-"Relevant_Date"
names(three_w)[2]<-"Value"

##Three wheeler growth
three_w_gr = read_query(526270,14)
names(three_w_gr)[1]<-"Relevant_Date"
names(three_w_gr)[2]<-"Value"

##Two wheeler
two_w= read_query(526281,14)
names(two_w)[1]<-"Relevant_Date"
names(two_w)[2]<-"Value"

##Two wheeler growth
two_w_gr = read_query(526384,14)
names(two_w_gr)[1]<-"Relevant_Date"
names(two_w_gr)[2]<-"Value"

##Commercial Vehicles
com_vehi= read_query(724271,14)
names(com_vehi)[1]<-"Relevant_Date"
names(com_vehi)[2]<-"Value"

##Commercial Vehicles growth
com_vehi_gr = read_query(724270,14)
names(com_vehi_gr)[1]<-"Relevant_Date"
names(com_vehi_gr)[2]<-"Value"


##Passenger vehicle Electric
pv_elec= read_query(527263,14)
names(pv_elec)[1]<-"Relevant_Date"
names(pv_elec)[2]<-"Value"

##Passenger vehicle Electric
pv_elec_gr = read_query(526992,14)
names(pv_elec_gr)[1]<-"Relevant_Date"
names(pv_elec_gr)[2]<-"Value"

##Three wheeler Electric
three_w_elec= read_query(526402,14)
names(three_w_elec)[1]<-"Relevant_Date"
names(three_w_elec)[2]<-"Value"

##Three wheeler growth Electric
three_w_elec_gr = read_query(526513,14)
names(three_w_elec_gr)[1]<-"Relevant_Date"
names(three_w_elec_gr)[2]<-"Value"

##Two wheeler Electric
two_w_elec= read_query(526965,14)
names(two_w_elec)[1]<-"Relevant_Date"
names(two_w_elec)[2]<-"Value"

##Two wheeler growth Electric
two_w_elec_gr = read_query(526638,14)
names(two_w_elec_gr)[1]<-"Relevant_Date"
names(two_w_elec_gr)[2]<-"Value"

##Commercial Vehicles Electric
com_vehi_elec= read_query(2214729,14)
names(com_vehi_elec)[1]<-"Relevant_Date"
names(com_vehi_elec)[2]<-"Value"

##Commercial Vehicles Electric
com_vehi_elec_gr = read_query(2314936,14)
names(com_vehi_elec_gr)[1]<-"Relevant_Date"
names(com_vehi_elec_gr)[2]<-"Value"

#Services

##Air passenger domestic
air_psng_dom= read_query(515783,14)
names(air_psng_dom)[1]<-"Relevant_Date"
names(air_psng_dom)[2]<-"Value"

##Air passenger domestic growth
air_psng_dom_gr = read_query(515846,14)
names(air_psng_dom_gr)[1]<-"Relevant_Date"
names(air_psng_dom_gr)[2]<-"Value"

##Air passenger International
air_psng_intl= read_query(1526662,14)
names(air_psng_intl)[1]<-"Relevant_Date"
names(air_psng_intl)[2]<-"Value"

##Air passenger International growth
air_psng_intl_gr = read_query(2316999,14)
names(air_psng_intl_gr)[1]<-"Relevant_Date"
names(air_psng_intl_gr)[2]<-"Value"

##Rail passenger
rail_psng= read_query(284859,14)
names(rail_psng)[1]<-"Relevant_Date"
names(rail_psng)[2]<-"Value"

##Rail passenger  growth
rail_psng_gr = read_query(284860,14)
names(rail_psng_gr)[1]<-"Relevant_Date"
names(rail_psng_gr)[2]<-"Value"


## FastTag Transaction
fast_tag = read_query(745423,14)
fast_tag=fast_tag[,c("Relevant_Date","Volume")]
fast_tag$days = as.numeric(format(as.Date(fast_tag$Relevant_Date,format="%Y-%m-%d"), format = "%d"))
fast_tag$Volume = fast_tag$Volume*fast_tag$days
fast_tag=fast_tag[,c("Relevant_Date","Volume")]
names(fast_tag)[1]<-"Relevant_Date"
names(fast_tag)[2]<-"Value"
# Fasttag Transaction growth
fast_tag_gr = read_query(318897,14)
fast_tag_gr=fast_tag_gr[,c("Relevant_Date","growth")]
names(fast_tag_gr)[1]<-"Relevant_Date"
names(fast_tag_gr)[2]<-"Value"


##Fastag colection value
fastag_col_val= read_query(745421,14)
fastag_col_val$days = as.numeric(format(as.Date(fastag_col_val$Relevant_Date,format="%Y-%m-%d"), format = "%d"))
fastag_col_val$Value  = fastag_col_val$Value*fastag_col_val$days
fastag_col_val=fastag_col_val[,c("Relevant_Date","Value")]
names(fastag_col_val)[1]<-"Relevant_Date"
names(fastag_col_val)[2]<-"Value"

##Fastag colection growth
fastag_col_val_gr = read_query(318930,14)
names(fastag_col_val_gr)[1]<-"Relevant_Date"
names(fastag_col_val_gr)[2]<-"Value"

##UPI Transaction
upi_trans= read_query(739144,14)
upi_trans=upi_trans[,c("Relevant_Date","Total_Volume")]
names(upi_trans)[1]<-"Relevant_Date"
names(upi_trans)[2]<-"Value"
## UPI Transaction growth
upi_trans_gr= read_query(318931,14)
upi_trans_gr=upi_trans_gr[,c("Relevant_Date","growth")]
names(upi_trans_gr)[1]<-"Relevant_Date"
names(upi_trans_gr)[2]<-"Value"

##UPI value
upi_val= read_query(739144,14)
upi_val=upi_val[,c("Relevant_Date","Total_Value")]
names(upi_val)[1]<-"Relevant_Date"
names(upi_val)[2]<-"Value"

##UPI value growth
upi_val_gr = read_query(2317000,14)
names(upi_val_gr)[1]<-"Relevant_Date"
names(upi_val_gr)[2]<-"Value"



## -------------------------------------
elec_demand_gr = table_preprocessing(elec_demand_gr,frequency_normalizer = 'Monthly', '% yoy','Electricity demand','Energy', rounder = 1)
petrol_con_gr = table_preprocessing(petrol_con_gr,frequency_normalizer = 'Monthly', '% yoy','Petrol consumption','Energy', rounder = 1)


pv_gr = table_preprocessing(pv_gr,frequency_normalizer = 'Monthly', '% yoy','Passenger vehicles ','Automobile registrations', rounder = 1)
three_w_gr = table_preprocessing(three_w_gr,frequency_normalizer = 'Monthly', '% yoy','Three-wheeler','Automobile registrations', rounder = 1)
two_w_gr = table_preprocessing(two_w_gr,frequency_normalizer = 'Monthly', '% yoy','Two-wheeler','Automobile registrations', rounder = 1)
com_vehi_gr = table_preprocessing(com_vehi_gr,frequency_normalizer = 'Monthly', '% yoy','Commercial vehicles ','Automobile registrations', rounder = 1)

pv_elec_gr = table_preprocessing(pv_elec_gr,frequency_normalizer = 'Monthly', '% yoy','Passenger vehicles-electric','Automobile registrations', rounder = 1)
three_w_elec_gr = table_preprocessing(three_w_elec_gr,frequency_normalizer = 'Monthly', '% yoy','Three-wheeler-electric','Automobile registrations', rounder = 1)
two_w_elec_gr = table_preprocessing(two_w_elec_gr,frequency_normalizer = 'Monthly', '% yoy','Two-wheeler-electric','Automobile registrations', rounder = 1)
com_vehi_elec_gr = table_preprocessing(com_vehi_elec_gr,frequency_normalizer = 'Monthly', '% yoy','Commercial vehicles-electric ','Automobile registrations', rounder = 1)



air_psng_dom_gr = table_preprocessing(air_psng_dom_gr,frequency_normalizer = 'Monthly', '% yoy','Air passenger (domestic)','Services', rounder = 1)
air_psng_intl_gr = table_preprocessing(air_psng_intl_gr,frequency_normalizer = 'Monthly', '% yoy','Air passenger (international)','Services', rounder = 1)
rail_psng_gr = table_preprocessing(rail_psng_gr,frequency_normalizer = 'Monthly', '% yoy','Rail passenger','Services', rounder = 1)

fast_tag_gr = table_preprocessing(fast_tag_gr,frequency_normalizer = 'Monthly', '% yoy','FASTag collection (volume)','Services', rounder = 1)
fastag_col_val_gr = table_preprocessing(fastag_col_val_gr,frequency_normalizer = 'Monthly', '% yoy','FASTag collection (value)','Services', rounder = 1)

upi_trans_gr = table_preprocessing(upi_trans_gr,frequency_normalizer = 'Monthly', '% yoy','UPI transactions (volume)','Services', rounder = 1)
upi_val_gr = table_preprocessing(upi_val_gr,frequency_normalizer = 'Monthly', '% yoy','UPI transactions (value)','Services', rounder = 1)
cpi_gr = table_preprocessing(cpi_gr,frequency_normalizer = 'Monthly', '% yoy','Consumer price index','Services', rounder = 1)

deposits_gr = table_preprocessing(deposits_gr,frequency_normalizer = 'Monthly', '% yoy','Aggregate deposits','Banking', rounder = 1)
credits_gr = table_preprocessing(credits_gr,frequency_normalizer = 'Monthly', '% yoy','Outstanding credit','Banking', rounder = 1)

growth_df_2 = rbind(elec_demand_gr,petrol_con_gr,pv_gr,three_w_gr,two_w_gr,com_vehi_gr,
                    pv_elec_gr,three_w_elec_gr,two_w_elec_gr,com_vehi_elec_gr,air_psng_dom_gr,
                    air_psng_intl_gr,rail_psng_gr,fast_tag_gr,fastag_col_val_gr,upi_trans_gr,
                    upi_val_gr,cpi_gr,deposits_gr,credits_gr)




economic_indicator_pg2_gr=economic_indicator_table(growth_df_2,has_main_sector = TRUE,
                                        has_units = TRUE,color_coding = TRUE,
                                        invert_map = c("Consumer price index"),
                                        median_change =
                                         list("Consumer price index" = 6),

                                        rounder_exeptions = growth_df_2$Variable,
                                        font_size = 7.7,row_height=0.03,
                                        var_col_width = 2,unit_col_width = 0.8,
                                        other_col_width = 0.58)


economic_indicator_pg2_gr_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y"),
                                      " - ",
                                      format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"),
                                      sep = '')

economic_indicator_pg2_gr


elec_demand = table_preprocessing(elec_demand,frequency_normalizer = 'Monthly', 'billion kWh','Electricity demand','Energy', rounder = 1,divisor =10^9)
petrol_con = table_preprocessing(petrol_con,frequency_normalizer = 'Monthly', 'mn tonnes','Petrol consumption','Energy', rounder = 1,divisor = 10^6)


pv = table_preprocessing(pv,frequency_normalizer = 'Monthly',"'000s",'Passenger vehicles ','Automobile registrations', rounder = 1,divisor = 10^3)
three_w = table_preprocessing(three_w,frequency_normalizer = 'Monthly',"'000s",'Three-wheeler','Automobile registrations', rounder = 1,divisor = 10^3)
two_w = table_preprocessing(two_w,frequency_normalizer = 'Monthly',"'000s",'Two-wheeler','Automobile registrations', rounder = 1,divisor = 10^3)
com_vehi = table_preprocessing(com_vehi,frequency_normalizer = 'Monthly',"'000s",'Commercial vehicles ','Automobile registrations', rounder = 1,divisor = 10^3)



pv_elec = table_preprocessing(pv_elec,frequency_normalizer = 'Monthly', "'000s",'Passenger vehicles-electric','Automobile registrations', rounder = 1,divisor = 10^3)
three_w_elec = table_preprocessing(three_w_elec,frequency_normalizer = 'Monthly', "'000s",'Three-wheeler-electric','Automobile registrations', rounder = 1,divisor = 10^3)
two_w_elec = table_preprocessing(two_w_elec,frequency_normalizer = 'Monthly', "'000s",'Two-wheeler-electric','Automobile registrations', rounder = 1,divisor = 10^3)
com_vehi_elec = table_preprocessing(com_vehi_elec,frequency_normalizer = 'Monthly', "'000s",'Commercial vehicles-electric ','Automobile registrations', rounder = 1,divisor = 10^3)



air_psng_dom = table_preprocessing(air_psng_dom,frequency_normalizer = 'Monthly', 'million','Air passenger (domestic)','Services', rounder = 1,divisor = 10^6)
air_psng_intl = table_preprocessing(air_psng_intl,frequency_normalizer = 'Monthly', 'million','Air passenger (international)','Services', rounder = 1,divisor = 10^6)
rail_psng = table_preprocessing(rail_psng,frequency_normalizer = 'Monthly', 'million','Rail passenger','Services', rounder = 1,divisor = 10^6)


fast_tag = table_preprocessing(fast_tag,frequency_normalizer = 'Monthly', 'million','FASTag collection (volume)','Services', divisor = 10^6, rounder = 1)
fastag_col_val= table_preprocessing(fastag_col_val,frequency_normalizer = 'Monthly', 'INR billion','FASTag collection (value)','Services', divisor = 10^9, rounder = 1)

upi_trans = table_preprocessing(upi_trans,frequency_normalizer = 'Monthly', 'billion','UPI transactions (volume)','Services', divisor = 10^9, rounder = 1)
upi_val= table_preprocessing(upi_val,frequency_normalizer = 'Monthly', 'INR trillion','UPI transactions (value)','Services', divisor = 10^12, rounder = 1)
cpi = table_preprocessing(cpi,frequency_normalizer = 'Monthly', 'Index','Consumer price index','Services', rounder = 1)


deposits = table_preprocessing(deposits,frequency_normalizer = 'Monthly', 'INR trillion','Aggregate deposits','Banking', divisor = 10^12, rounder = 1)
credits = table_preprocessing(credits,frequency_normalizer = 'Monthly', 'INR trillion','Outstanding credit','Banking', divisor = 10^12, rounder = 1)


indicator_df_2 = rbind(elec_demand,petrol_con,pv,three_w,two_w,com_vehi,pv_elec,three_w_elec,
                       two_w_elec,com_vehi_elec,air_psng_dom,air_psng_intl,rail_psng,fast_tag,
                       fastag_col_val,upi_trans,upi_val,cpi,deposits,credits)



economic_indicator_pg2 = economic_indicator_table(indicator_df_2,has_main_sector = TRUE,
                             has_units = TRUE,color_coding = FALSE,
                             rounder_exeptions = indicator_df_2$Variable,
                             font_size =8, var_col_width = 2.5,
                             unit_col_width = 0.8,line_space=0,row_height=0.03,
                             other_col_width = 0.58)
economic_indicator_pg2

economic_indicator_pg2_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y")," - ",
                                   format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"), sep = '')




## ----eval=FALSE, include=FALSE--------
## goi_gr = table_preprocessing(goi_gr,frequency_normalizer = 'Monthly', '% yoy','Central government expenditure','Fiscal', rounder = 1)
## gst_coll_gr = table_preprocessing(gst_coll_gr,frequency_normalizer = 'Monthly', '% yoy','GST collection','Fiscal', rounder = 1)
## rail_freight_gr = table_preprocessing(rail_freight_gr,frequency_normalizer = 'Monthly', '% yoy','Rail freight','Logistics', rounder = 1)
## elec_gen_gr = table_preprocessing(elec_gen_gr,frequency_normalizer = 'Monthly', '% yoy','Electricity generation','Industry', rounder = 1)
## air_cargo_gr = table_preprocessing(air_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Air cargo','Logistics', rounder = 1)
## upi_trans_gr = table_preprocessing(upi_trans_gr,frequency_normalizer = 'Monthly', '% yoy','UPI transactions (volume)','Consumption', rounder = 1)
## export_gr = table_preprocessing(export_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize exports','Trade', rounder = 1)
## import_gr = table_preprocessing(import_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize imports','Trade', rounder = 1)
## fast_tag_gr = table_preprocessing(fast_tag_gr,frequency_normalizer = 'Monthly', '% yoy','FASTag collection (volume)','Consumption', rounder = 1)
## port_cargo_gr = table_preprocessing(port_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Port cargo','Logistics', rounder = 1)
## e_way_bills_gr = table_preprocessing(e_way_bills_gr,frequency_normalizer = 'Monthly', '% yoy','E-way bills (volume)','Logistics', rounder = 1)
## cpi_gr = table_preprocessing(cpi_gr,frequency_normalizer = 'Monthly', '% yoy','Consumer Price Index','Consumption', rounder = 1)
## iip_gr = table_preprocessing(iip_gr,frequency_normalizer = 'Monthly', '% yoy','Index of industrial production','Industry', rounder = 1)
## core_8_gr = table_preprocessing(core_8_gr,frequency_normalizer = 'Monthly', '% yoy','Index of eight core industries','Industry', rounder = 1)
## deposits_gr = table_preprocessing(deposits_gr,frequency_normalizer = 'Monthly', '% yoy','Aggregate deposits  (excluding merger)','Banking', rounder = 1)
## credits_gr = table_preprocessing(credits_gr,frequency_normalizer = 'Monthly', '% yoy','Outstanding credit  (excluding merger)','Banking', rounder = 1)
##
## ser_exp_gr = table_preprocessing(ser_exp_gr,frequency_normalizer = 'Monthly', '% yoy','Services exports','Trade', rounder = 1)
##
## ser_imp_gr = table_preprocessing(ser_imp_gr,frequency_normalizer = 'Monthly', '% yoy','Services imports','Trade', rounder = 1)
##
##
##
##
## growth_df = rbind(goi_gr,gst_coll_gr,iip_gr,core_8_gr,elec_gen_gr,rail_freight_gr,
##                   port_cargo_gr,air_cargo_gr,e_way_bills_gr,export_gr,import_gr,ser_exp_gr,
##                   ser_imp_gr,deposits_gr,credits_gr,upi_trans_gr,fast_tag_gr,cpi_gr)
##
##
##
## economic_indicator_gr_54 =economic_indicator_table(growth_df,has_main_sector = TRUE,
##                                         has_units = TRUE,color_coding = TRUE,
##                                         invert_map = c("Consumer Price Index"),
##                                         median_change =
##                                          list("Consumer Price Index" = 6),
##
##                                         rounder_exeptions = growth_df$Variable,
##                                         font_size = 7.7,row_height=0.03,
##                                         var_col_width = 2,unit_col_width = 0.8,
##                                         other_col_width = 0.58)
##
##
## economic_indicator_gr_54_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y"),
##                                       " - ",
##                                       format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"),
##                                       sep = '')
##
## economic_indicator_gr_54
##
##
## goi = table_preprocessing(goi,frequency_normalizer = 'Monthly', 'INR trillion','Central government expenditure','Fiscal', divisor = 10^12, rounder = 1)
## gst_coll = table_preprocessing(gst_coll,frequency_normalizer = 'Monthly', 'INR trillion','GST collection','Fiscal', divisor = 10^12, rounder = 1)
## rail_freight = table_preprocessing(rail_freight,frequency_normalizer = 'Monthly', 'mn tonnes','Rail freight','Logistics', rounder = 1)
## air_cargo = table_preprocessing(air_cargo,frequency_normalizer = 'Monthly', "'000 ton",'Air cargo','Logistics', divisor = 10^3, rounder = 1)#
## port_cargo = table_preprocessing(port_cargo,frequency_normalizer = 'Monthly', 'mn tonnes','Port cargo','Logistics', rounder = 1)
## e_way_bills = table_preprocessing(e_way_bills,frequency_normalizer = 'Monthly', 'million','E-way bills (volume)','Logistics', divisor = 10^6, rounder = 1)
## upi_trans = table_preprocessing(upi_trans,frequency_normalizer = 'Monthly', 'billion','UPI transactions (volume)','Consumption', divisor = 10^9, rounder = 1)
## elec_gen = table_preprocessing(elec_gen,frequency_normalizer = 'Monthly', 'billion kWh','Electricity generation','Industry', divisor = 10^3, rounder = 1)
## export = table_preprocessing(export,frequency_normalizer = 'Monthly', 'USD billion','Merchandize exports','Trade', divisor = 10^9, rounder = 1)
## import = table_preprocessing(import,frequency_normalizer = 'Monthly', 'USD billion','Merchandize imports','Trade', divisor = 10^9, rounder = 1)
##
## ser_import = table_preprocessing(ser_imp,frequency_normalizer = 'Monthly', 'USD billion','Services imports','Trade', divisor = 10^9, rounder = 1)
## ser_export = table_preprocessing(ser_exp,frequency_normalizer = 'Monthly', 'USD billion','Services exports','Trade', divisor = 10^9, rounder = 1)
##
##
##
## fast_tag = table_preprocessing(fast_tag,frequency_normalizer = 'Monthly', 'million','FASTag collection (volume)','Consumption', divisor = 10^6, rounder = 1)
## cpi = table_preprocessing(cpi,frequency_normalizer = 'Monthly', 'Index','Consumer Price Index','Consumption', rounder = 1)
## iip = table_preprocessing(iip,frequency_normalizer = 'Monthly', 'Index','Index of industrial production','Industry', rounder = 1)
## core_8 = table_preprocessing(core_8,frequency_normalizer = 'Monthly', 'Index','Index of eight core industries','Industry', rounder = 1)
## deposits = table_preprocessing(deposits,frequency_normalizer = 'Monthly', 'INR trillion','Aggregate deposits  (excluding merger)','Banking', divisor = 10^12, rounder = 1)
## credits = table_preprocessing(credits,frequency_normalizer = 'Monthly', 'INR trillion','Outstanding credit  (excluding merger)','Banking', divisor = 10^12, rounder = 1)
##
##
##
##
##
## indicator_df = rbind(goi,gst_coll,iip,core_8,elec_gen,rail_freight,port_cargo,air_cargo,
##                      e_way_bills,export,import,ser_export,ser_import,deposits,credits,
##                      upi_trans,fast_tag,cpi)
##
## economic_indicator_55 = economic_indicator_table(indicator_df,has_main_sector = TRUE,
##                              has_units = TRUE,color_coding = FALSE,
##                              rounder_exeptions = indicator_df$Variable,
##                              font_size =8, var_col_width = 2.5,
##                              unit_col_width = 0.8,line_space=0,row_height=0.03,
##                              other_col_width = 0.58) #
## economic_indicator_55
##
## economic_indicator_55_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y")," - ",
##                                    format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"), sep = '')
##
##


## -------------------------------------
gdp = get_data("GDP_STATEWISE_TEMP",'GDP')
gdp_rd =  gdp[2][[1]][1]
gdp = data.frame(gdp[1])
gdp=table_preprocessing_state_tbl(gdp,unit= 'INR tn',
                                  variable = 'State GSDP (FY2022)',sector='NA'
                                   ,divisor = 1000000,rounder = 1)
gst_yoy=get_data("mv_gst_tax_state_india_growth",'growth')
gst_rd =  gst_yoy[2][[1]][1]
gst_yoy = data.frame(gst_yoy[1])
gst_yoy=table_preprocessing_state_tbl(gst_yoy,unit = '% yoy',variable = 'GST collections',
                                   sector = 'Industry',divisor = 0,rounder = 1)

elec_dmd_yoy=get_percent_data("mv_state_electricity_demand_deficit",var1='States',variable = 'Total_Energy_Met+Total_Shortage')
elec_dmd_yoy_rd =  elec_dmd_yoy[2][[1]][1]
elec_dmd_yoy = data.frame(elec_dmd_yoy[1])
elec_dmd_yoy=table_preprocessing_state_tbl(elec_dmd_yoy,unit = '% yoy',variable = 'Electricity demand',
                                        sector = 'Industry',divisor = 0,rounder = 1)

elec_gen_yoy=get_percent_data("mv_power_states_total_generation_cea",var1 = 'Region',variable='Total')
elec_gen_yoy_rd =  elec_gen_yoy[2][[1]][1]
elec_gen_yoy = data.frame(elec_gen_yoy[1])
elec_gen_yoy=table_preprocessing_state_tbl(elec_gen_yoy,unit = '% yoy',variable = 'Electricity generation',
                                   sector = 'Industry',divisor = 0,rounder = 1)

toll_traffic_yoy=get_data("mv_ihmcl_statewise_new_collection_traffic_growth",'growth',toll=TRUE)
toll_traffic_yoy_rd =  toll_traffic_yoy[2][[1]][1]
toll_traffic_yoy = data.frame(toll_traffic_yoy[1])
toll_traffic_yoy=table_preprocessing_state_tbl(toll_traffic_yoy,unit = '% yoy',variable = 'Toll Traffic',sector = 'Industry',divisor = 0,rounder = 1)

toll_revenue_yoy=get_data("mv_ihmcl_statewise_new_collection_revenue_growth",'growth',toll=TRUE)
toll_revenue_yoy_rd =  toll_revenue_yoy[2][[1]][1]
toll_revenue_yoy = data.frame(toll_revenue_yoy[1])
toll_revenue_yoy=table_preprocessing_state_tbl(toll_revenue_yoy,unit = '% yoy',variable = 'Toll Revenue',
                                   sector = 'Industry',divisor = 0,rounder = 1)

epfo_yoy=get_data("mv_epfo_state_growth",'growth')
epfo_yoy_rd =  epfo_yoy[2][[1]][1]
epfo_yoy = data.frame(epfo_yoy[1])
epfo_yoy=table_preprocessing_state_tbl(epfo_yoy,unit = '% yoy',variable = 'EPFO net additions',
                                    sector = 'Employment',divisor = 0,rounder = 1)

mnrega_hh_yoy=get_data("mv_househould_emp_volume_growth",'growth')
mnrega_hh_yoy_rd =  mnrega_hh_yoy[2][[1]][1]
mnrega_hh_yoy = data.frame(mnrega_hh_yoy[1])
mnrega_hh_yoy=table_preprocessing_state_tbl(mnrega_hh_yoy,unit = '% yoy',variable = 'MNREGA (hh)',
                                         sector = 'Employment',divisor = 0,rounder = 1)

mnrega_per_yoy=get_data("mv_persondays_emp_volume_growth",'growth')
mnrega_per_yoy_rd =  mnrega_per_yoy[2][[1]][1]
mnrega_per_yoy = data.frame(mnrega_per_yoy[1])
mnrega_per_yoy=table_preprocessing_state_tbl(mnrega_per_yoy,unit = '% yoy',variable = 'MNREGA (persons)',
                                          sector = 'Employment',divisor = 0,rounder = 1)

naukri_yoy=get_data("mv_naukri_statewise_avg_growth",variable= 'growth')
naukri_yoy_rd =  naukri_yoy[2][[1]][1]
naukri_yoy = data.frame(naukri_yoy[1])
naukri_yoy=table_preprocessing_state_tbl(naukri_yoy,unit = '% yoy',variable ='Job Postings',
                                   sector = 'Employment',divisor = 0,rounder = 1)
two_wh_total=get_data("mv_vr_gr_2w_state",'growth')
two_wh_total_rd =  two_wh_total[2][[1]][1]
two_wh_total = data.frame(two_wh_total[1])
two_wh_total=table_preprocessing_state_tbl(two_wh_total,unit = '% yoy',variable ='2W Registrations',sector = 'Automobiles',divisor = 0,rounder = 1)

two_wh_elec_total=get_data("mv_vr_gr_2w_elec_state",'growth')
two_wh_elec_total_rd =  two_wh_elec_total[2][[1]][1]
two_wh_elec_total = data.frame(two_wh_elec_total[1])
two_wh_elec_total=table_preprocessing_state_tbl(two_wh_elec_total,unit = '% yoy',variable ='2W Electric Registrations',sector = 'Automobiles',divisor = 0,rounder = 1)

four_wh_total=get_data("mv_vr_gr_4w_state",'growth')
four_wh_total_rd =  four_wh_total[2][[1]][1]
four_wh_total = data.frame(four_wh_total[1])
four_wh_total=table_preprocessing_state_tbl(four_wh_total,unit = '% yoy',variable ='4W Registrations',sector = 'Automobiles',divisor = 0,rounder = 1)

four_wh_elec_total=get_data("mv_vr_gr_4w_elec_state",'growth')
four_wh_elec_total_rd =  four_wh_elec_total[2][[1]][1]
four_wh_elec_total = data.frame(four_wh_elec_total[1])
four_wh_elec_total=table_preprocessing_state_tbl(four_wh_elec_total,unit = '% yoy',variable ='4W Electric Registrations',sector = 'Automobiles',divisor = 0,rounder = 1)

yoy_hm=rbind(gst_yoy,elec_dmd_yoy,elec_gen_yoy,toll_traffic_yoy,toll_revenue_yoy,epfo_yoy,mnrega_hh_yoy,mnrega_per_yoy,naukri_yoy,two_wh_total,two_wh_elec_total,four_wh_total,four_wh_elec_total)


economic_indicator_gr_st_54 =economic_indicator_table(yoy_hm,has_main_sector = TRUE,
                                  has_units = TRUE,color_coding = TRUE,
                                  rounder_exeptions = yoy_hm$Variable,
                                  font_size = 12,row_height=1,
                                  var_col_width = 3,unit_col_width = 1,
                                  other_col_width = 0.951,yoy=TRUE)
economic_indicator_gr_st_54 <- add_body(
  x = economic_indicator_gr_st_54, values=gdp,top=TRUE
)
economic_indicator_gr_st_54 <- bg(economic_indicator_gr_st_54,i=1, bg = "white",part = "body")
economic_indicator_gr_st_54 <- italic(economic_indicator_gr_st_54,i=1, italic = TRUE,part = "body")
economic_indicator_gr_st_54 <- color(economic_indicator_gr_st_54,i=1, color="grey48",part = "body")
economic_indicator_gr_st_54

max_date_yoy = max(four_wh_elec_total_rd, four_wh_total_rd,two_wh_elec_total_rd,two_wh_total_rd,naukri_yoy_rd,mnrega_per_yoy_rd,mnrega_hh_yoy_rd,epfo_yoy_rd,toll_revenue_yoy_rd,toll_traffic_yoy_rd,elec_gen_yoy_rd,elec_dmd_yoy_rd,gst_rd)

data_yoy <- data.frame(Variable = c(four_wh_elec_total$Variable[1], four_wh_total$Variable[1],two_wh_elec_total$Variable[1],two_wh_total$Variable[1],naukri_yoy$Variable[1],mnrega_per_yoy$Variable[1],mnrega_hh_yoy$Variable[1],epfo_yoy$Variable[1],toll_revenue_yoy$Variable[1],toll_traffic_yoy$Variable[1],elec_gen_yoy$Variable[1],elec_dmd_yoy$Variable[1],gst_yoy$Variable[1]),Date = as.Date(c(four_wh_elec_total_rd, four_wh_total_rd,two_wh_elec_total_rd,two_wh_total_rd,naukri_yoy_rd,mnrega_per_yoy_rd,mnrega_hh_yoy_rd,epfo_yoy_rd,toll_revenue_yoy_rd,toll_traffic_yoy_rd,elec_gen_yoy_rd,elec_dmd_yoy_rd,gst_rd))
)

given_date_yoy <- as.Date(max_date_yoy)
filtered_df_yoy <- data_yoy %>% filter(Date < given_date_yoy)
note_yoy <- ""
for (i in 1:nrow(filtered_df_yoy)) {note_yoy <- paste0(note_yoy, filtered_df_yoy$Variable[i], "-", format(filtered_df_yoy$Date[i],'%b %Y'), ", ")}



## -------------------------------------
gdp = get_data("GDP_STATEWISE_TEMP",'GDP')
gdp_rd =  gdp[2][[1]][1]
gdp = data.frame(gdp[1])
gdp=table_preprocessing_state_tbl(gdp,unit= 'INR tn',variable = 'State GSDP (FY2022)',sector='NA'
                                   ,divisor = 1000000,rounder = 1)
#DEMAND
gst_yoy=get_data("mv_gst_tax_monthly_state",'Value')
gst_rd =  gst_yoy[2][[1]][1]
gst_yoy = data.frame(gst_yoy[1])
gst_yoy=table_preprocessing_state_tbl(gst_yoy,unit= 'INR Bn',variable = 'GST collections',
                                   sector = 'Industry',divisor = 1000000000,rounder = 1)

elec_dmd_yoy=get_data("mv_state_electricity_demand_deficit",'(Total_Energy_Met+Total_Shortage)*30',electricity=TRUE)
elec_dmd_yoy_rd =  elec_dmd_yoy[2][[1]][1]
elec_dmd_yoy = data.frame(elec_dmd_yoy[1])
elec_dmd_yoy=table_preprocessing_state_tbl(elec_dmd_yoy,unit= 'BU',variable = 'Electricity demand',
                                        sector = 'Industry',divisor = 1000,rounder = 1)

elec_gen_yoy=get_data("mv_power_states_total_generation_cea",'Total',pow=TRUE)
elec_gen_yoy_rd =  elec_gen_yoy[2][[1]][1]
elec_gen_yoy = data.frame(elec_gen_yoy[1])
elec_gen_yoy=table_preprocessing_state_tbl(elec_gen_yoy,unit= 'BU',variable = 'Electricity generation',
                                   sector = 'Industry',divisor = 1000,rounder = 1)

toll_traffic_yoy=get_data("mv_ihmcl_new_statewise_collection",'Traffic')
toll_traffic_yoy_rd =  toll_traffic_yoy[2][[1]][1]
toll_traffic_yoy = data.frame(toll_traffic_yoy[1])
toll_traffic_yoy=table_preprocessing_state_tbl(toll_traffic_yoy,unit= 'Mn',variable = 'Toll Traffic',
                                   sector = 'Industry',divisor = 1000000,rounder = 1)

toll_revenue_yoy=get_data("mv_ihmcl_new_statewise_collection",'Revenue')
toll_revenue_yoy_rd =  toll_revenue_yoy[2][[1]][1]
toll_revenue_yoy = data.frame(toll_revenue_yoy[1])
toll_revenue_yoy=table_preprocessing_state_tbl(toll_revenue_yoy,unit= 'INR Bn',variable = 'Toll Revenue',
                                   sector = 'Industry',divisor = 1000000000,rounder = 1)

#Employment
epfo_yoy=get_data("mv_epfo_state",'Value',epfo=TRUE)
epfo_yoy_rd =  epfo_yoy[2][[1]][1]
epfo_yoy = data.frame(epfo_yoy[1])
epfo_yoy=table_preprocessing_state_tbl(epfo_yoy,unit= "'000s",variable = 'EPFO net additions',
                                    sector = 'Employment',divisor = 1000,rounder = 1)

mnrega_hh_yoy=get_data("mv_househould_emp_volume",'Total')
mnrega_hh_yoy_rd =  mnrega_hh_yoy[2][[1]][1]
mnrega_hh_yoy = data.frame(mnrega_hh_yoy[1])
mnrega_hh_yoy=table_preprocessing_state_tbl(mnrega_hh_yoy,unit= "'000s",variable = 'MNREGA (hh)',
                                         sector = 'Employment',divisor = 1000,rounder = 1)

mnrega_per_yoy=get_data("mv_persondays_emp_volume",'Total')
mnrega_per_yoy_rd =  mnrega_per_yoy[2][[1]][1]
mnrega_per_yoy = data.frame(mnrega_per_yoy[1])
mnrega_per_yoy=table_preprocessing_state_tbl(mnrega_per_yoy,unit= 'Mn',variable = 'MNREGA (persons)',
                                          sector = 'Employment',divisor = 1000000,rounder = 1)

naukri_yoy=get_data("mv_naukri_statewise_avg",'Value')
naukri_yoy_rd =  naukri_yoy[2][[1]][1]
naukri_yoy = data.frame(naukri_yoy[1])
naukri_yoy=table_preprocessing_state_tbl(naukri_yoy,unit= "'000s",variable ='Job Postings',
                                   sector = 'Employment',divisor = 1000,rounder = 1)

two_wh_total=get_data("mv_vr_category_state",'pr_total',vh_cat='2W')
two_wh_total_rd =  two_wh_total[2][[1]][1]
two_wh_total = data.frame(two_wh_total[1])
two_wh_total=table_preprocessing_state_tbl(two_wh_total,unit = "'000s",variable ='2W Registrations',sector = 'Automobiles',divisor = 1000,rounder = 1)

two_wh_elec_total=get_data("mv_vr_category_state_fuel",'pr_total',vh_cat='2W',fuel = 'ELECTRIC(BOV)')
two_wh_elec_total_rd =  two_wh_elec_total[2][[1]][1]
two_wh_elec_total = data.frame(two_wh_elec_total[1])
two_wh_elec_total=table_preprocessing_state_tbl(two_wh_elec_total,unit= "'000s",variable ='2W Electric Registrations',sector = 'Automobiles',divisor = 1000,rounder = 1)

four_wh_total=get_data("mv_vr_category_state",'pr_total',vh_cat='4W')
four_wh_total_rd =  four_wh_total[2][[1]][1]
four_wh_total = data.frame(four_wh_total[1])
four_wh_total=table_preprocessing_state_tbl(four_wh_total,unit = "'000s",variable ='4W Registrations',sector = 'Automobiles',divisor = 1000,rounder = 1)

four_wh_elec_total=get_data("mv_vr_category_state_fuel",'pr_total',vh_cat='4W',fuel = 'ELECTRIC(BOV)')
four_wh_elec_total_rd =  four_wh_elec_total[2][[1]][1]
four_wh_elec_total = data.frame(four_wh_elec_total[1])
four_wh_elec_total=table_preprocessing_state_tbl(four_wh_elec_total,unit= "'000s",variable ='4W Electric Registrations',sector = 'Automobiles',divisor = 1000,rounder = 1)


yoy_hm_abs=rbind(gst_yoy,elec_dmd_yoy,elec_gen_yoy,toll_traffic_yoy,toll_revenue_yoy,epfo_yoy,mnrega_hh_yoy,mnrega_per_yoy,naukri_yoy,two_wh_total,two_wh_elec_total,four_wh_total,four_wh_elec_total)

economic_indicator_abs_st_54 =economic_indicator_table(yoy_hm_abs,has_main_sector = TRUE,
                                  has_units = TRUE,color_coding = FALSE,
                                  rounder_exeptions = yoy_hm$Variable,
                                  font_size = 12,row_height=1,
                                  var_col_width = 3,unit_col_width = 1,
                                  other_col_width = 0.951)
economic_indicator_abs_st_54 <- add_body(
  x = economic_indicator_abs_st_54, values=gdp,top=TRUE
)
economic_indicator_abs_st_54 <- bg(economic_indicator_abs_st_54,i=1, bg = "white",part = "body")
economic_indicator_abs_st_54 <- italic(economic_indicator_abs_st_54,i=1, italic = TRUE,part = "body")
economic_indicator_abs_st_54 <- color(economic_indicator_abs_st_54,i=1, color="grey48",part = "body")
economic_indicator_abs_st_54

max_date_abs = max(four_wh_elec_total_rd, four_wh_total_rd,two_wh_elec_total_rd,two_wh_total_rd,naukri_yoy_rd,mnrega_per_yoy_rd,mnrega_hh_yoy_rd,epfo_yoy_rd,toll_revenue_yoy_rd,toll_traffic_yoy_rd,elec_gen_yoy_rd,elec_dmd_yoy_rd,gst_rd)

data_abs <- data.frame(Variable = c(four_wh_elec_total$Variable[1], four_wh_total$Variable[1],two_wh_elec_total$Variable[1],two_wh_total$Variable[1],naukri_yoy$Variable[1],mnrega_per_yoy$Variable[1],mnrega_hh_yoy$Variable[1],epfo_yoy$Variable[1],toll_revenue_yoy$Variable[1],toll_traffic_yoy$Variable[1],elec_gen_yoy$Variable[1],elec_dmd_yoy$Variable[1],gst_yoy$Variable[1]),Date = as.Date(c(four_wh_elec_total_rd, four_wh_total_rd,two_wh_elec_total_rd,two_wh_total_rd,naukri_yoy_rd,mnrega_per_yoy_rd,mnrega_hh_yoy_rd,epfo_yoy_rd,toll_revenue_yoy_rd,toll_traffic_yoy_rd,elec_gen_yoy_rd,elec_dmd_yoy_rd,gst_rd))
)

given_date_abs <- as.Date(max_date_abs)
filtered_df_abs <- data_abs %>% filter(Date < given_date_abs)
note_abs <- ""
for (i in 1:nrow(filtered_df_abs)) {note_abs <- paste0(note_abs, filtered_df_abs$Variable[i], "-", format(filtered_df_abs$Date[i],'%b %Y'), ", ")}



## -------------------------------------
## IIP
iip_total_gr = read_query(1288704,12)
iip_total_gr = iip_total_gr[,c("Relevant_Date","growth")]
names(iip_total_gr)[1]<-"Relevant_Date"
names(iip_total_gr)[2]<-"Value"
iip_total_gr = table_preprocessing(iip_total_gr, sector = '',
                                   variable = 'IIP', rounder = 1)


## IIP Mining
iip_mining_gr = read_query(318913,12)
iip_mining_gr = iip_mining_gr[,c("Relevant_Date","growth")]
names(iip_mining_gr)[1]<-"Relevant_Date"
names(iip_mining_gr)[2]<-"Value"
iip_mining_gr = table_preprocessing(iip_mining_gr, sector = 'Sector-based classification', variable = 'Mining', rounder = 1)
str(iip_mining_gr)

## IIP Manufacturing
iip_manufacture_gr = read_query(318906,12)
iip_manufacture_gr = iip_manufacture_gr[,c("Relevant_Date","growth")]
names(iip_manufacture_gr)[1]<-"Relevant_Date"
names(iip_manufacture_gr)[2]<-"Value"
iip_manufacture_gr = table_preprocessing(iip_manufacture_gr, sector = 'Sector-based classification', variable = 'Manufacturing', rounder = 1)
str(iip_manufacture_gr)

## IIP Electricity
iip_electricity_gr = read_query(318902,12)
iip_electricity_gr = iip_electricity_gr[,c("Relevant_Date","growth")]
names(iip_electricity_gr)[1]<-"Relevant_Date"
names(iip_electricity_gr)[2]<-"Value"
iip_electricity_gr = table_preprocessing(iip_electricity_gr, sector = 'Sector-based classification', variable = 'Electricity', rounder = 1)
str(iip_electricity_gr)

## IIP Primary goods
iip_primary_gr = read_query(318918,12)
iip_primary_gr = iip_primary_gr[,c("Relevant_Date","growth")]
names(iip_primary_gr)[1]<-"Relevant_Date"
names(iip_primary_gr)[2]<-"Value"
iip_primary_gr = table_preprocessing(iip_primary_gr, sector = 'Use-based classification', variable = 'Primary goods', rounder = 1)
str(iip_primary_gr)

## IIP Capital goods
iip_capital_gr = read_query(318884,12)
iip_capital_gr = iip_capital_gr[,c("Relevant_Date","growth")]
names(iip_capital_gr)[1]<-"Relevant_Date"
names(iip_capital_gr)[2]<-"Value"
iip_capital_gr = table_preprocessing(iip_capital_gr, sector = 'Use-based classification', variable = 'Capital goods', rounder = 1)
str(iip_capital_gr)
## IIP Intermediate Goods
iip_intermediate_gr = read_query(318911,12)
iip_intermediate_gr = iip_intermediate_gr[,c("Relevant_Date","growth")]
names(iip_intermediate_gr)[1]<-"Relevant_Date"
names(iip_intermediate_gr)[2]<-"Value"
iip_intermediate_gr = table_preprocessing(iip_intermediate_gr, sector = 'Use-based classification', variable = 'Intermediate goods', rounder = 1)
str(iip_intermediate_gr)

## IIP Infrastructure and Construction Goods
iip_infrastructure_gr = read_query(318905,12)
iip_infrastructure_gr = iip_infrastructure_gr[,c("Relevant_Date","growth")]
names(iip_infrastructure_gr)[1]<-"Relevant_Date"
names(iip_infrastructure_gr)[2]<-"Value"
iip_infrastructure_gr = table_preprocessing(iip_infrastructure_gr, sector = 'Use-based classification',
                                            variable = 'Infrastructure and construction goods', rounder = 1)


## IIP Consumer Durable Goods
iip_durable_gr = read_query(318891,12)
iip_durable_gr = iip_durable_gr[,c("Relevant_Date","growth")]
names(iip_durable_gr)[1]<-"Relevant_Date"
names(iip_durable_gr)[2]<-"Value"
iip_durable_gr = table_preprocessing(iip_durable_gr, sector = 'Use-based classification', variable = 'Consumer durable goods', rounder = 1)


## IIP Consumer Non-Durable Goods
iip_non_durable_gr = read_query(318898,12)
iip_non_durable_gr = iip_non_durable_gr[,c("Relevant_Date","growth")]
names(iip_non_durable_gr)[1]<-"Relevant_Date"
names(iip_non_durable_gr)[2]<-"Value"
iip_non_durable_gr = table_preprocessing(iip_non_durable_gr,
                                         sector ='Use-based classification',                                              variable='Consumer non-durable goods',rounder = 1)




iip_df = rbind(iip_total_gr,iip_mining_gr,iip_manufacture_gr,iip_electricity_gr,
               iip_primary_gr,iip_capital_gr,iip_intermediate_gr,iip_infrastructure_gr,
               iip_durable_gr,iip_non_durable_gr)

bold = c("IIP")
background = list(col_FDECDA = c("IIP"))
borders = c("IIP", "Electricity")
pad = list(pad_10 = c("Mining","Manufacturing","Electricity","Primary goods",'Capital goods',
                      'Intermediate goods','Infrastructure and construction goods',
                      'Consumer durable goods',
                      'Consumer non-durable goods'))

industrial_Prod_56 = economic_indicator_table(iip_df,has_main_sector = TRUE,
                            main_sector_bg = "GRAY 96",rounder_exeptions = iip_df$Variable,
                            make_bold =bold,background_vals =  background,
                            hlines = borders, padding_vals = pad, font_size = 12,
                            var_col_width = 3,other_col_width = 0.6862)

industrial_Prod_56_title=paste0(format(min(read_query(1288704,12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                 format(max(read_query(1288704,12)[,c("Relevant_Date")]),"%b '%y"))

industrial_Prod_56



## -------------------------------------
# Agriculture Credit
agri_credit = read_query(525664, 12)
agri_credit=agri_credit[,c("Relevant_Date","Value")]

agri_credit = table_preprocessing(agri_credit, variable = "Agriculture", calculate_gr = TRUE,divisor = 10^12, rounder = 1)


# Industry Credit
industry_credit = read_query(1662939,12)
industry_credit=industry_credit[,c("Relevant_Date","Value")]
industry_credit = table_preprocessing(industry_credit, variable = "Industry", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Construction Credit
construction_credit = read_query(525709,12)
construction_credit=construction_credit[,c("Relevant_Date","Value")]
names(construction_credit)[1]<-"Relevant_Date"
names(construction_credit)[2]<-"Value"
construction_credit = table_preprocessing(construction_credit, variable = "Construction", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Infrastructure Credit
infrastucture_credit = read_query(525749,12)
infrastucture_credit=infrastucture_credit[,c("Relevant_Date","Value")]

infrastucture_credit = table_preprocessing(infrastucture_credit, variable = "Infrastructure", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Power Credit
power_credit = read_query(525764,12)
power_credit=power_credit[,c("Relevant_Date","Value")]
power_credit = table_preprocessing(power_credit, variable = "Power", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Telecom Credit
telecom_credit = read_query(525779,12)
telecom_credit=telecom_credit[,c("Relevant_Date","Value")]
names(telecom_credit)[1]<-"Relevant_Date"
names(telecom_credit)[2]<-"Value"
telecom_credit = table_preprocessing(telecom_credit, variable = "Telecom", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Roads Credit
road_credit = read_query(525774,12)
road_credit=road_credit[,c("Relevant_Date","Value")]
names(road_credit)[1]<-"Relevant_Date"
names(road_credit)[2]<-"Value"
road_credit = table_preprocessing(road_credit, variable = "Roads", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Airports Credit
airport_credit = read_query(525754,12)
airport_credit=airport_credit[,c("Relevant_Date","Value")]
names(airport_credit)[1]<-"Relevant_Date"
names(airport_credit)[2]<-"Value"
airport_credit = table_preprocessing(airport_credit, variable = "Airports", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Ports Credit
port_credit = read_query(525759,12)
port_credit=port_credit[,c("Relevant_Date","Value")]
names(port_credit)[1]<-"Relevant_Date"
names(port_credit)[2]<-"Value"
port_credit = table_preprocessing(port_credit, variable = "Ports", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Railways Credit
rail_credit = read_query(525769,12)
rail_credit=rail_credit[,c("Relevant_Date","Value")]
names(rail_credit)[1]<-"Relevant_Date"
names(rail_credit)[2]<-"Value"
rail_credit = table_preprocessing(rail_credit, variable = "Railways (other than Indian Railways)", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Services Credit
service_credit = read_query(1662965,12)
service_credit=service_credit[,c("Relevant_Date","Value")]
names(service_credit)[1]<-"Relevant_Date"
names(service_credit)[2]<-"Value"
service_credit = table_preprocessing(service_credit, variable ="Services", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# # Services Shipping Credit
# service_shipping_credit = read_query(525815,12)
# service_shipping_credit=service_shipping_credit[,c("Relevant_Date","Total")]
# names(service_shipping_credit)[1]<-"Relevant_Date"
# names(service_shipping_credit)[2]<-"Value"
# service_shipping_credit = table_preprocessing(service_shipping_credit, variable = "Services - Shipping", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# # Services Aviation Credit
# service_aviation_credit = read_query(525785,12)
# service_aviation_credit=service_aviation_credit[,c("Relevant_Date","Total")]
# names(service_aviation_credit)[1]<-"Relevant_Date"
# names(service_aviation_credit)[2]<-"Value"
# service_aviation_credit = table_preprocessing(service_aviation_credit, variable = "Services - Aviation", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

#Santonu
# Services Trade
service_shipping_trade = read_query(1662975,12)
service_shipping_trade=service_shipping_trade[,c("Relevant_Date","Value")]
names(service_shipping_trade)[1]<-"Relevant_Date"
names(service_shipping_trade)[2]<-"Value"
service_shipping_trade = table_preprocessing(service_shipping_trade, variable = "Services - Trade", calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# Services Real Estate
service_shipping_realest = read_query(1662967,12)
service_shipping_realest=service_shipping_realest[,c("Relevant_Date","Value")]
names(service_shipping_realest)[1]<-"Relevant_Date"
names(service_shipping_realest)[2]<-"Value"
service_shipping_realest = table_preprocessing(service_shipping_realest, variable ="Services- Commercial Real Estate", calculate_gr = TRUE, divisor = 10^12, rounder = 1)


# Services NBFC Credit
service_nbfc_credit = read_query(1662969,12)
service_nbfc_credit=service_nbfc_credit[,c("Relevant_Date","Value")]
names(service_nbfc_credit)[1]<-"Relevant_Date"
names(service_nbfc_credit)[2]<-"Value"
service_nbfc_credit = table_preprocessing(service_nbfc_credit, variable = "Services - NBFC", calculate_gr = TRUE, divisor = 10^12, rounder = 1)


# Retail loans
retail_loans_credit = read_query(1662943,12)
retail_loans_credit=retail_loans_credit[,c("Relevant_Date","Value")]
names(retail_loans_credit)[1]<-"Relevant_Date"
names(retail_loans_credit)[2]<-"Value"
retail_loans_credit = table_preprocessing(retail_loans_credit, variable ="Retail loans",calculate_gr = TRUE, divisor = 10^12, rounder = 1)


# Other non-food loans
other_non_food_loans = read_query(525673,12)
other_non_food_loans=other_non_food_loans[,c("Relevant_Date","Value")]
names(other_non_food_loans)[1]<-"Relevant_Date"
names(other_non_food_loans)[2]<-"Value"
other_non_food_loans = table_preprocessing(other_non_food_loans, variable ="Other non-food loans", calculate_gr = TRUE, divisor = 10^12, rounder = 1)


#Non-Food Credit
non_food_credit = read_query(525706,12)
non_food_credit=non_food_credit[,c("Relevant_Date","Value")]
names(non_food_credit)[1]<-"Relevant_Date"
names(non_food_credit)[2]<-"Value"
non_food_credit = table_preprocessing(non_food_credit, variable ="Non-food Credit",
                                      calculate_gr = TRUE, divisor = 10^12, rounder = 1)

#Total Credit Outstanding (without hdfc merger)
total_cre_wt_hdfc = read_query(2169972,15)
total_cre_wt_hdfc=total_cre_wt_hdfc[,c("Relevant_Date","Value")]
##HARD CODE
df1=read_query(525706,12)
total_cre_wt_hdfc=total_cre_wt_hdfc[(total_cre_wt_hdfc$Relevant_Date<=max(df1$Relevant_Date) & (total_cre_wt_hdfc$Relevant_Date>=min(df1$Relevant_Date))) ,]
names(total_cre_wt_hdfc)[1]<-"Relevant_Date"
names(total_cre_wt_hdfc)[2]<-"Value"
total_cre_wt_hdfc = table_preprocessing(total_cre_wt_hdfc,
                                   variable ="Total Credit Outstanding (without HDFC merger)",
                                   calculate_gr = TRUE, divisor = 10^12, rounder = 1)

#Total Credit Outstanding
total_credit = read_query(525690,12)
total_credit=total_credit[,c("Relevant_Date","Value")]
names(total_credit)[1]<-"Relevant_Date"
names(total_credit)[2]<-"Value"
total_credit = table_preprocessing(total_credit, variable ="Total Credit Outstanding",
                                   calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# credit_df = rbind(agri_credit,industry_credit,construction_credit,infrastucture_credit,power_credit,telecom_credit,road_credit,airport_credit,port_credit,rail_credit,service_credit,service_nbfc_credit,service_shipping_trade,service_shipping_realest,retail_loans_credit,other_non_food_loans,non_food_credit,total_credit)

credit_df = rbind(agri_credit,industry_credit,construction_credit,infrastucture_credit,power_credit,telecom_credit,road_credit,service_credit,service_nbfc_credit,service_shipping_trade,service_shipping_realest,retail_loans_credit,other_non_food_loans,non_food_credit,total_cre_wt_hdfc,total_credit)

# padding = list(pad_10 = c("Construction","Infrastructure","Services - NBFC","Services - Trade","Services- Commercial Real Estate"), pad_20 = c("Power","Telecom","Roads","Airports","Ports","Railways (other than Indian Railways)"))

padding = list(pad_10 = c("Construction","Infrastructure","Services - NBFC","Services - Trade","Services- Commercial Real Estate"), pad_20 = c("Power","Telecom","Roads"))

bg = list(col_D9D9D9 = c("Industry","Services"), col_F2F2F2 = c("Infrastructure"),col_FDECDA = c("Total Credit Outstanding","Total Credit Outstanding (without HDFC merger)"))
border = c("Services- Commercial Real Estate","Retail loans","Other non-food loans","Non-food Credit","Total Credit Outstanding (without HDFC merger)")
bold = c("Total Credit Outstanding","Total Credit Outstanding (without HDFC merger)")
vborder = c(14)

credit_outstanding_57 = economic_indicator_table(credit_df,
                         rounder_exeptions = credit_df$Variable,
                         padding_vals = padding, background_vals = bg,hlines = border,
                         vlines = vborder, make_bold = bold,font_size = 12,
                         var_col_width = 3.5,other_col_width = 0.75)

credit_outstanding_57 <- credit_outstanding_57 %>% italic( j = 15, italic = TRUE, part = "all")
credit_outstanding_57 <- credit_outstanding_57 %>% italic( j = 15, italic = TRUE, part = "all")

credit_outstanding_57_title=paste0(format(min(read_query(525664, 12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                    format(max(read_query(525664, 12)[,c("Relevant_Date")]),"%b '%y"))

credit_outstanding_57


## -------------------------------------
# Rail Cargo | Cement & Clinker
rail_cmnt = read_query(284821, 12,convert_na=TRUE)
names(rail_cmnt)[2]<-"Value"
rail_cmnt = table_preprocessing(rail_cmnt, variable = "Cement and clinker",
                                calculate_gr = TRUE,divisor = 1,
                                rounder = 1)

# Rail Cargo | Coal
rail_coal = read_query(284836, 12,convert_na=TRUE)
names(rail_coal)[2]<-"Value"
rail_coal = table_preprocessing(rail_coal, variable = "Coal",
                                calculate_gr = TRUE,divisor = 1,
                                rounder = 1)

#Rail Cargo | Container Service
rail_cnt = read_query(284807, 12,convert_na=TRUE)
names(rail_cnt)[2]<-"Value"
rail_cnt = table_preprocessing(rail_cnt, variable = "Container service",
                                calculate_gr = TRUE,divisor = 1,
                                rounder = 1)

# Rail Cargo | Fertilizers
rail_ferti = read_query(284815, 12,convert_na=TRUE)
names(rail_ferti)[2]<-"Value"
rail_ferti = table_preprocessing(rail_ferti, variable = "Fertilizers",
                                calculate_gr = TRUE,divisor = 1,
                                rounder = 1)

# Rail Cargo | Foodgrains
rail_fg = read_query(284818, 12,convert_na=TRUE)
names(rail_fg )[2]<-"Value"
rail_fg  = table_preprocessing(rail_fg,variable = "Foodgrains",
                              calculate_gr = TRUE,divisor = 1,
                              rounder = 1)

#Rail Cargo | Iron Ore
rail_iron = read_query(284826, 12,convert_na=TRUE)
names(rail_iron)[2]<-"Value"
rail_iron  = table_preprocessing(rail_iron,variable = "Iron ore",
                              calculate_gr = TRUE,divisor = 1,
                              rounder = 1)



#Rail Cargo | Mineral Oil
rail_mn_oil = read_query(284811, 12,convert_na=TRUE)
names(rail_mn_oil)[2]<-"Value"
rail_mn_oil  = table_preprocessing(rail_mn_oil,variable = "Mineral oil",
                              calculate_gr = TRUE,divisor = 1,
                              rounder = 1)



# Rail Cargo | Pig Iron and Finished Steel
rail_ir_ft = read_query(284831, 12,convert_na=TRUE)
names(rail_ir_ft)[2]<-"Value"
rail_ir_ft  = table_preprocessing(rail_ir_ft,
                              variable = "Pig iron and finished steel",
                              calculate_gr = TRUE,divisor = 1,
                              rounder = 1)


#Raw Material
rail_raw = read_query(2084624, 15,convert_na=TRUE)
names(rail_raw)[2]<-"Value"
rail_raw  = table_preprocessing(rail_raw,
                    frequency_normalizer = 'Monthly',
                    variable = "Raw material for steel plants (except iron ore)",
                    calculate_gr = TRUE,divisor = 1,
                    rounder = 1,month_num = 15)
rail_raw=replace(rail_raw,is.na(rail_raw),0.0)
rail_raw <- subset(rail_raw, select = names(rail_raw) %in%names(rail_ir_ft))


#Others
rail_oth = read_query(284804, 12,convert_na=TRUE)
names(rail_oth)[2]<-"Value"
rail_oth  = table_preprocessing(rail_oth,
                              variable = "Others",
                              calculate_gr = TRUE,divisor = 1,
                              rounder = 1)


#Rail Cargo
rail_total = read_query(284813, 12,convert_na=TRUE)
names(rail_total)[2]<-"Value"
rail_total  = table_preprocessing(rail_total,
                          variable = "Total",
                          calculate_gr = TRUE,divisor = 1,
                          rounder = 1)







rail_df = rbind(rail_cmnt,rail_coal,rail_cnt,rail_ferti,rail_fg,rail_iron,
                rail_mn_oil,rail_ir_ft,rail_raw)

##HARD CODE
rail_df <- rail_df[order(as.numeric(rail_df$`May-24`),decreasing = TRUE),]
rail_df = rbind(rail_df,rail_oth,rail_total)
rail_df[rail_df$`Growth (% yoy)`== -100, "Growth (% yoy)"] <- 0


bold = c("Total")
bg = list(col_FDECDA =c("Total"))

rail_cargo_85 = economic_indicator_table(rail_df,
                         rounder_exeptions = rail_df$Variable,
                         background_vals = bg,
                         make_bold = bold,
                         vlines = c(14),font_size = 12,
                         var_col_width = 3.5,other_col_width = 0.75)

rail_cargo_85 <- rail_cargo_85 %>% italic( j = 15, italic = TRUE, part = "all")
rail_cargo_85 <- rail_cargo_85 %>% italic( j = 15, italic = TRUE, part = "all")

rail_cargo_85_title=paste0(format(min(read_query(284821, 12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                    format(max(read_query(284821, 12)[,c("Relevant_Date")]),"%b '%y"))

rail_cargo_85


## -------------------------------------
# Personal Loans
retail_per = read_query(1662943, 12)
names(retail_per)[2]<-"Value"
retail_per = table_preprocessing(retail_per, variable = "Personal loans",

                                calculate_gr = TRUE,divisor = 10^12,
                                rounder = 1)

#Credit | Personal Loans | Advances against Fixed Deposits
reatail_fd = read_query(1662944, 12)
names(reatail_fd)[2]<-"Value"
reatail_fd = table_preprocessing(reatail_fd, variable = "Advances against fixed deposits",

                                 calculate_gr = TRUE,divisor = 10^12,
                                 rounder = 1)

#Credit | Personal Loans | Advances to Individuals against share, bonds, etc
retail_sb = read_query(1662945, 12)
names(retail_sb)[2]<-"Value"
retail_sb = table_preprocessing(retail_sb,
                                variable = "Advances to individuals against share, bonds, etc",
                                calculate_gr = TRUE,divisor = 10^12,
                                rounder = 1)

#Credit | Personal Loans | Consumer Durables
retail_cd = read_query(1662946, 12)
names(retail_cd)[2]<-"Value"
retail_cd = table_preprocessing(retail_cd, variable = "Consumer durables",

                                calculate_gr = TRUE,divisor = 10^12,
                                rounder = 1)

#Credit | Personal Loans | Credit Card Outstanding
retail_crcd = read_query(1662947, 12)
names(retail_crcd )[2]<-"Value"
retail_crcd  = table_preprocessing(retail_crcd,variable = "Credit card outstanding",

                              calculate_gr = TRUE,divisor = 10^12,
                              rounder = 1)


#Credit | Personal Loans | Education
retail_edu = read_query(1662948, 12)
names(retail_edu)[2]<-"Value"
retail_edu  = table_preprocessing(retail_edu,variable = "Education",

                              calculate_gr = TRUE,divisor = 10^12,
                              rounder = 1)



#Credit | Personal Loans | Housing
retail_hu = read_query(1662949, 12)
names(retail_hu)[2]<-"Value"
retail_hu  = table_preprocessing(retail_hu,variable = "Housing",

                              calculate_gr = TRUE,divisor = 10^12,
                              rounder = 1)

# Credit | Personal Loans | Loans against gold jewellery
retail_gold = read_query(1662950, 12)
names(retail_gold)[2]<-"Value"
retail_gold  = table_preprocessing(retail_gold,
                              variable = "Loans against gold jewellery",

                              calculate_gr = TRUE,divisor = 10^12,
                              rounder = 1)


#Credit | Personal Loans | Vehicle Loans
retail_vl = read_query(1662951, 12)
names(retail_vl)[2]<-"Value"
retail_vl  = table_preprocessing(retail_vl,
                    variable = "Vehicle loans",

                    calculate_gr = TRUE,divisor = 10^12,
                    rounder = 1)

#Others
retail_oth = read_query(2084775, 12)
names(retail_oth)[2]<-"Value"
retail_oth  = table_preprocessing(retail_oth,
                              variable = "Others",
                              calculate_gr = TRUE,divisor = 10^12,
                              rounder = 1)


retail_loan_df = rbind(retail_per,reatail_fd,retail_cd,retail_crcd,retail_edu,
                       retail_hu,retail_gold,retail_vl)

##HARD CODE
retail_loan_df <- retail_loan_df[order(as.numeric(retail_loan_df$"Sep-23"),
                                       decreasing = TRUE),]
retail_loan_df=rbind(retail_loan_df,retail_oth)


bold = c("Personal loans")
bg = list(col_FDECDA = c("Personal loans"))

padding = list(pad_10 = c("Advances against fixed deposits",
                          "Advances to individuals against share, bonds, etc",
                          "Consumer durables","Credit card outstanding",
                          "Education","Housing",
                          "Loans against gold jewellery",
                          "Vehicle loans","Others"))


retail_loan_86 = economic_indicator_table(retail_loan_df,
                         rounder_exeptions = retail_loan_df$Variable,
                         background_vals = bg,
                         padding_vals = padding,
                         make_bold = bold,
                         vlines = c(14),font_size = 12,
                         var_col_width = 3.5,other_col_width = 0.75)

retail_loan_86 <- retail_loan_86 %>% italic( j = 15, italic = TRUE, part = "all")
retail_loan_86_title=paste0(format(min(read_query(1662943, 12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                    format(max(read_query(1662943, 12)[,c("Relevant_Date")]),"%b '%y"))

retail_loan_86


## -------------------------------------
# CPI
cpi_total_gr = read_query(1445961, 12)
cpi_total_gr=cpi_total_gr[,c("Relevant_Date","Inflation")]
names(cpi_total_gr)[1]<-"Relevant_Date"
names(cpi_total_gr)[2]<-"Value"

weight_query = "select Weight from PIB_MOSPI_ALL_INDIA_CPI_CFPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Variable = 'CPI'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
cpi_total_gr = table_preprocessing(cpi_total_gr, unit = weight, variable = "Consumer Price Index", rounder = 1)

# CPI Core
cpi_core_gr = read_query(725986, 12)
cpi_core_gr=cpi_core_gr[,c("Relevant_Date","Total")]
names(cpi_core_gr)[1]<-"Relevant_Date"
names(cpi_core_gr)[2]<-"Value"
weight_query = "select (100 - sum(Weight_Value)) as Weight from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description in ('Food and beverages','Fuel and light')"
weight = dbGetQuery(clk,weight_query)$Weight[1]
cpi_core_gr = table_preprocessing(cpi_core_gr, unit = weight, variable = "Consumer Price Index - Core", rounder = 1)

# CPI Food and beverages
cpi_food_gr = read_query(726007, 12)
cpi_food_gr=cpi_food_gr[,c("Relevant_Date","Inflation")]
names(cpi_food_gr)[1]<-"Relevant_Date"
names(cpi_food_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Food and beverages'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_food_gr = table_preprocessing(cpi_food_gr, unit = weight,variable = "Food and beverages", rounder = 1)

# CPI Pan, tobacco and intoxicants
cpi_intoxicants_gr = read_query(726029, 12)
cpi_intoxicants_gr=cpi_intoxicants_gr[,c("Relevant_Date","Inflation")]
names(cpi_intoxicants_gr)[1]<-"Relevant_Date"
names(cpi_intoxicants_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Pan, tobacco and intoxicants'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_intoxicants_gr = table_preprocessing(cpi_intoxicants_gr, unit = weight, variable = "Pan, tobacco and intoxicants", rounder = 1)

# CPI Clothing and footwear
cpi_clothing_gr = read_query(725998, 12)
cpi_clothing_gr=cpi_clothing_gr[,c("Relevant_Date","Inflation")]
names(cpi_clothing_gr)[1]<-"Relevant_Date"
names(cpi_clothing_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Clothing and footwear'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_clothing_gr = table_preprocessing(cpi_clothing_gr, unit = weight, variable = "Clothing and footwear", rounder = 1)

# CPI Housing
cpi_housing_gr = read_query(726015, 12)
cpi_housing_gr=cpi_housing_gr[,c("Relevant_Date","Inflation")]
names(cpi_housing_gr)[1]<-"Relevant_Date"
names(cpi_housing_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Housing'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_housing_gr = table_preprocessing(cpi_housing_gr, unit = weight, variable = "Housing", rounder = 1)

# CPI Fuel and light
cpi_fuel_gr = read_query(726010, 12)
cpi_fuel_gr=cpi_fuel_gr[,c("Relevant_Date","Inflation")]
names(cpi_fuel_gr)[1]<-"Relevant_Date"
names(cpi_fuel_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Fuel and light'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_fuel_gr = table_preprocessing(cpi_fuel_gr, unit = weight, variable = "Fuel and light", rounder = 1)

# CPI Miscellaneous
cpi_misc_gr = read_query(726021, 12)
cpi_misc_gr=cpi_misc_gr[,c("Relevant_Date","Inflation")]
names(cpi_misc_gr)[1]<-"Relevant_Date"
names(cpi_misc_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Miscellaneous'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_misc_gr = table_preprocessing(cpi_misc_gr, unit = weight, variable = "Miscellaneous", rounder = 1)

# CPI Household goods and services
cpi_household_gr = read_query(726008, 12)
cpi_household_gr=cpi_household_gr[,c("Relevant_Date","Inflation")]
names(cpi_household_gr)[1]<-"Relevant_Date"
names(cpi_household_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Household goods and services'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_household_gr = table_preprocessing(cpi_household_gr, unit = weight, variable = "Household goods and services", rounder = 1)

# CPI Health
cpi_health_gr = read_query(726001, 12)
cpi_health_gr=cpi_health_gr[,c("Relevant_Date","Inflation")]
names(cpi_health_gr)[1]<-"Relevant_Date"
names(cpi_health_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Health'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_health_gr = table_preprocessing(cpi_health_gr, unit = weight, variable = "Health", rounder = 1)

# CPI Transportation and communication
cpi_transport_gr = read_query(726026, 12)
cpi_transport_gr=cpi_transport_gr[,c("Relevant_Date","Inflation")]
names(cpi_transport_gr)[1]<-"Relevant_Date"
names(cpi_transport_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Transport and communication'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_transport_gr = table_preprocessing(cpi_transport_gr, unit = weight, variable = "Transportation and communication", rounder = 1)

# CPI Recreation and amusement
cpi_recreation_gr = read_query(726023, 12)
cpi_recreation_gr=cpi_recreation_gr[,c("Relevant_Date","Inflation")]
names(cpi_recreation_gr)[1]<-"Relevant_Date"
names(cpi_recreation_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Recreation and amusement'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_recreation_gr = table_preprocessing(cpi_recreation_gr, unit = weight, variable = "Recreation and amusement", rounder = 1)

# CPI Education
cpi_education_gr = read_query(725991, 12)
cpi_education_gr=cpi_education_gr[,c("Relevant_Date","Inflation")]
names(cpi_education_gr)[1]<-"Relevant_Date"
names(cpi_education_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Education'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_education_gr = table_preprocessing(cpi_education_gr, unit = weight, variable = "Education", rounder = 1)

# CPI Personal Care and effects
cpi_personal_gr = read_query(726016, 12)
cpi_personal_gr=cpi_personal_gr[,c("Relevant_Date","Inflation")]
names(cpi_personal_gr)[1]<-"Relevant_Date"
names(cpi_personal_gr)[2]<-"Value"
weight_query = "select Weight_Value from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Type = 'Combined' and Description = 'Personal care and effects'"
weight = dbGetQuery(clk,weight_query)$Weight_Value[1]
cpi_personal_gr = table_preprocessing(cpi_personal_gr, unit = weight, variable = "Personal Care and effects", rounder = 1)



cpi_df = rbind(cpi_total_gr,cpi_core_gr,cpi_food_gr,cpi_intoxicants_gr,cpi_clothing_gr,cpi_housing_gr,
      cpi_fuel_gr,cpi_misc_gr,cpi_household_gr,cpi_health_gr,cpi_transport_gr,cpi_recreation_gr,
      cpi_education_gr,cpi_personal_gr)

bg = list(col_FDECDA = c("Consumer Price Index","Consumer Price Index - Core"), col_F2F2F2 = c("Miscellaneous","Weights"))
bold = c("Consumer Price Index","Consumer Price Index - Core")
borders = c("Consumer Price Index","Consumer Price Index - Core","Fuel and light")

pad = list(pad_10 = c("Food and beverages","Pan, tobacco and intoxicants","Clothing and footwear","Housing",
                      "Fuel and light","Miscellaneous"),
           pad_20 = c("Household goods and services","Health","Transportation and communication","Recreation and amusement",
                      "Education","Personal Care and effects"))


consumer_inflation_58 = economic_indicator_table(cpi_df, has_units = TRUE,
                             rename_unit_col = 'Weights',
                             rounder_exeptions = cpi_df$Variable, background_vals = bg,
                             make_bold = bold, hlines = borders, padding_vals = pad,
                             font_size = 12,var_col_width = 2.3,unit_col_width = 0.7,
                             other_col_width =0.6093)


consumer_inflation_58 <- consumer_inflation_58 %>% italic( j = 2, italic = TRUE, part = "all")


consumer_inflation_58_title=paste0(format(min(read_query(725955, 12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                   format(max(read_query(725955, 12)[,c("Relevant_Date")]),"%b '%y"))

consumer_inflation_58



## -------------------------------------
# WPI
wpi_total_gr = read_query(725962,12)
wpi_total_gr=wpi_total_gr[,c("Relevant_Date","Inflation")]
names(wpi_total_gr)[1]<-"Relevant_Date"
names(wpi_total_gr)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = 'All commodities'"

weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_total_gr = table_preprocessing(wpi_total_gr, unit = weight, variable = "WPI", rounder = 1)

# WPI Primary articles
wpi_primary = read_query(726030, 12)
wpi_primary=wpi_primary[,c("Relevant_Date","Inflation")]
names(wpi_primary)[1]<-"Relevant_Date"
names(wpi_primary)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = 'I    PRIMARY ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_primary = table_preprocessing(wpi_primary, unit = weight, variable = "Primary articles", rounder = 1)

# WPI Food articles
wpi_food = read_query(726000, 12)
wpi_food=wpi_food[,c("Relevant_Date","Inflation")]
names(wpi_food)[1]<-"Relevant_Date"
names(wpi_food)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(A).  FOOD ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_food = table_preprocessing(wpi_food, unit = weight, variable = "Food articles", rounder = 1)

# WPI Non-food articles
wpi_non_food = read_query(726025, 12)
wpi_non_food=wpi_non_food[,c("Relevant_Date","Inflation")]
names(wpi_non_food)[1]<-"Relevant_Date"
names(wpi_non_food)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(B).  NON-FOOD ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_non_food = table_preprocessing(wpi_non_food, unit = weight, variable = "Non-food articles", rounder = 1)

# WPI Minerals
wpi_minerals = read_query(726022, 12)
wpi_minerals=wpi_minerals[,c("Relevant_Date","Inflation")]
names(wpi_minerals)[1]<-"Relevant_Date"
names(wpi_minerals)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(C).  MINERALS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_minerals = table_preprocessing(wpi_minerals, unit = weight, variable = "Minerals", rounder = 1)

# WPI Crude oil, petroleum and natural gas
wpi_fossil_fuel = read_query(725988, 12)
wpi_fossil_fuel=wpi_fossil_fuel[,c("Relevant_Date","Inflation")]
names(wpi_fossil_fuel)[1]<-"Relevant_Date"
names(wpi_fossil_fuel)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(D). CRUDE PETROLEUM & NATURAL GAS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_fossil_fuel = table_preprocessing(wpi_fossil_fuel, unit = weight, variable = "Crude oil, petroleum and natural gas", rounder = 1)

# WPI Fuel and power
wpi_fuel_power = read_query(726006, 12)
wpi_fuel_power=wpi_fuel_power[,c("Relevant_Date","Inflation")]
names(wpi_fuel_power)[1]<-"Relevant_Date"
names(wpi_fuel_power)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = 'II FUEL & POWER'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_fuel_power = table_preprocessing(wpi_fuel_power, unit = weight, variable = "Fuel and power", rounder = 1)

# WPI Coal
wpi_coal = read_query(725959, 12)
wpi_coal=wpi_coal[,c("Relevant_Date","Inflation")]
names(wpi_coal)[1]<-"Relevant_Date"
names(wpi_coal)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(A). COAL'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_coal = table_preprocessing(wpi_coal, unit = weight, variable = "Coal", rounder = 1)

# WPI Mineral oils
wpi_mineral_oils = read_query(726017, 12)
wpi_mineral_oils=wpi_mineral_oils[,c("Relevant_Date","Inflation")]
names(wpi_mineral_oils)[1]<-"Relevant_Date"
names(wpi_mineral_oils)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(B). MINERAL OILS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_mineral_oils = table_preprocessing(wpi_mineral_oils, unit = weight, variable = "Mineral oils", rounder = 1)

# WPI Electricity
wpi_electricity = read_query(725989, 12)
wpi_electricity=wpi_electricity[,c("Relevant_Date","Inflation")]
names(wpi_electricity)[1]<-"Relevant_Date"
names(wpi_electricity)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = '(C). ELECTRICITY'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_electricity = table_preprocessing(wpi_electricity, unit = weight, variable = "Electricity", rounder = 1)

# WPI Manufactured products
wpi_mfg_pdt = read_query(726011, 12)
wpi_mfg_pdt=wpi_mfg_pdt[,c("Relevant_Date","Inflation")]
names(wpi_mfg_pdt)[1]<-"Relevant_Date"
names(wpi_mfg_pdt)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA) and Commodity = 'III   MANUFACTURED PRODUCTS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_mfg_pdt = table_preprocessing(wpi_mfg_pdt, unit = weight, variable = "Manufactured products", rounder = 1)


wpi_df = rbind(wpi_total_gr,wpi_primary,wpi_food,wpi_non_food,wpi_minerals,wpi_fossil_fuel,wpi_fuel_power,wpi_coal,wpi_mineral_oils,wpi_electricity,wpi_mfg_pdt)


bg = list(col_FDECDA = c("WPI"), col_F2F2F2 = c("Primary articles","Fuel and power","Manufactured products"))
bold = c("WPI","Primary articles","Fuel and power","Manufactured products")
borders = c("WPI")
pad = list(pad_10 = c("Food articles","Non-food articles","Minerals","Crude oil,
                      petroleum and natural gas","Coal","Mineral oils","Electricity"))

wholesale_inflation_59 = economic_indicator_table(wpi_df, has_units = TRUE,
                                  rename_unit_col = 'Weights',
                                  rounder_exeptions = wpi_df$Variable, background_vals = bg,
                                  make_bold = bold, hlines = borders, padding_vals = pad,
                                  font_size = 12,var_col_width = 2,unit_col_width = 0.7,
                                  other_col_width = 0.6324)

wholesale_inflation_59 <- wholesale_inflation_59 %>% italic( j = 2, italic = TRUE, part = "all")

wholesale_inflation_59_title=paste0(format(min(read_query(725962, 12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                   format(max(read_query(725962, 12)[,c("Relevant_Date")]),"%b '%y"))


wholesale_inflation_59



## ----eval=FALSE, include=FALSE--------
## #NOTE: This format is depricated now
## #Eco-Indicators | Current Account
## qtr_bop_ca=read_query(1640367,15,quarter=TRUE)
## names(qtr_bop_ca)[1]<-"Relevant_Date"
## names(qtr_bop_ca)[2]<-"Value"
## qtr_bop_ca = table_preprocessing_annual(qtr_bop_ca,frequency_normalizer='quarter',period=2,
##                                     variable = "Current account",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
## #Eco-Indicators | Current Account | Goods and Services
## qtr_bop_gs=read_query(1633403,15,quarter=TRUE)
## names(qtr_bop_gs)[1]<-"Relevant_Date"
## names(qtr_bop_gs)[2]<-"Value"
## qtr_bop_gs = table_preprocessing_annual(qtr_bop_gs,frequency_normalizer='quarter',period=2,
##                                     variable = "Goods and services",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
##
## #Eco-Indicators | Current Account | Goods
## qtr_bop_g=read_query(1633411,15,quarter=TRUE)
## names(qtr_bop_g)[1]<-"Relevant_Date"
## names(qtr_bop_g)[2]<-"Value"
## qtr_bop_g = table_preprocessing_annual(qtr_bop_g,frequency_normalizer='quarter',period=2,
##                                     variable = "Goods", calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
## #Eco-Indicators | Current Account | Services
## qtr_bop_s=read_query(1633412,15,quarter=TRUE)
## names(qtr_bop_s)[1]<-"Relevant_Date"
## names(qtr_bop_s)[2]<-"Value"
## qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='quarter',period=2,
##                                     variable = "Services", calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
##
## #Eco-Indicators | Current Account | Primary Income
## qtr_bop_pi=read_query(1633404 ,15,quarter=TRUE)
## names(qtr_bop_pi)[1]<-"Relevant_Date"
## names(qtr_bop_pi)[2]<-"Value"
## qtr_bop_pi = table_preprocessing_annual(qtr_bop_pi,frequency_normalizer='quarter',period=2,
##                                     variable = "Primary income",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
## #Eco-Indicators | Current Account | Secondary Income
## qtr_bop_si=read_query(1633405 ,15,quarter=TRUE)
## names(qtr_bop_si)[1]<-"Relevant_Date"
## names(qtr_bop_si)[2]<-"Value"
## qtr_bop_si = table_preprocessing_annual(qtr_bop_si,frequency_normalizer='quarter',period=2,
##                                     variable = "Secondary income",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
##
## #Eco-Indicators | Capital Account
## qtr_bop_caa=read_query(1640368   ,15,quarter=TRUE)
## names(qtr_bop_caa)[1]<-"Relevant_Date"
## names(qtr_bop_caa)[2]<-"Value"
## qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='quarter',period=2,
##                                     variable = "Capital account",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Financial Account
## qtr_bop_fa=read_query(1640369 ,15,quarter=TRUE)
## names(qtr_bop_fa)[1]<-"Relevant_Date"
## names(qtr_bop_fa)[2]<-"Value"
## qtr_bop_fa = table_preprocessing_annual(qtr_bop_fa,frequency_normalizer='quarter',period=2,
##                                     variable = "Financial accounts", calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Financial Account | Direct Investment
## qtr_bop_di=read_query(1633406 ,15,quarter=TRUE)
## names(qtr_bop_di)[1]<-"Relevant_Date"
## names(qtr_bop_di)[2]<-"Value"
## qtr_bop_di = table_preprocessing_annual(qtr_bop_di,frequency_normalizer='quarter',period=2,
##                                     variable = "Direct investments",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Financial Account | Portfolio Investment
## qtr_bop_poi=read_query(1633407 ,15,quarter=TRUE)
## names(qtr_bop_poi)[1]<-"Relevant_Date"
## names(qtr_bop_poi)[2]<-"Value"
## qtr_bop_poi = table_preprocessing_annual(qtr_bop_poi,frequency_normalizer='quarter',period=2,
##                                     variable = "Portfolio investments",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Financial Account | Financial derivatives
## qtr_bop_fd=read_query(1633408 ,15,quarter=TRUE)
## names(qtr_bop_fd)[1]<-"Relevant_Date"
## names(qtr_bop_fd)[2]<-"Value"
## qtr_bop_fd = table_preprocessing_annual(qtr_bop_fd,frequency_normalizer='quarter',period=2,
##                                     variable = "Financial derivatives (other than reserves)",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Financial Account | Other investment
## qtr_bop_oi=read_query(1633409 ,15,quarter=TRUE)
## names(qtr_bop_oi)[1]<-"Relevant_Date"
## names(qtr_bop_oi)[2]<-"Value"
## qtr_bop_oi = table_preprocessing_annual(qtr_bop_oi,frequency_normalizer='quarter',period=2,
##                                     variable = "Other investments",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
##
## #Eco-Indicators | Financial Account | Reserve assets
## qtr_bop_ra=read_query(1633410 ,15,quarter=TRUE)
## names(qtr_bop_ra)[1]<-"Relevant_Date"
## names(qtr_bop_ra)[2]<-"Value"
## qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='quarter',period=2,
##                                     variable = "Reserve assets",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
## #Eco-Indicators | Net errors and omissions
## qtr_bop_eo=read_query(1632782 ,15,quarter=TRUE)
## names(qtr_bop_eo)[1]<-"Relevant_Date"
## names(qtr_bop_eo)[2]<-"Value"
## qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='quarter',period=2,
##                                     variable = "Net errors and omissions",
##                                     calculate_gr = FALSE,
##                                     divisor =10^9, rounder = 1)
##
##
## bop=rbind(qtr_bop_ca,qtr_bop_gs,qtr_bop_g,qtr_bop_s,qtr_bop_pi,qtr_bop_si,qtr_bop_caa,
##           qtr_bop_fa,qtr_bop_di,qtr_bop_poi,qtr_bop_fd,qtr_bop_oi,qtr_bop_ra,qtr_bop_eo)
##
##
## bg = list(col_FDECDA = c("Current account","Capital account","Financial accounts",
##                          "Net errors and omissions"))
##
## padding = list(pad_10 = c("Goods and services","Primary income","Secondary income",
##                           "Direct investments","Portfolio investments",
##                           "Financial derivatives (other than reserves)",
##                           "Other investments","Reserve assets"),
##                pad_20 = c("Goods","Services"))
##
##
## border = c("Capital account","Financial accounts","Net errors and omissions")
## bold = c("Current account","Capital account","Financial accounts","Net errors and omissions")
## vborder =c(14)
##
## quarterly_bop_74_chart = economic_indicator_table(bop,rounder_exeptions = bop$Variable,
##                                                  padding_vals = padding,
##                                                  background_vals = bg,
##                                                  hlines = border, make_bold = bold,
##                                                  font_size = 12,
##                                                  var_col_width =2,other_col_width =0.9912)
##
##
##
## quarterly_bop_74_chart
## data=read_query(1632782,15,quarter=TRUE)
## names(data)[1]<-"Relevant_Date"
## names(data)[2]<-"Value"
## quarterly_bop_74_title=heading(data)
##


## -------------------------------------
#Eco-Indicators | Current Account
qtr_bop_mt=read_query(1633411,15,quarter=TRUE)
names(qtr_bop_mt)[1]<-"Relevant_Date"
names(qtr_bop_mt)[2]<-"Value"
qtr_bop_mt = table_preprocessing_annual(qtr_bop_mt,
                                    frequency_normalizer='quarter',
                                    period=5,
                                    variable = "a.  Merchandize trade",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account
qtr_bop_mt_gdp=read_query(1690708,15,quarter=TRUE)
names(qtr_bop_mt_gdp)[1]<-"Relevant_Date"
names(qtr_bop_mt_gdp)[2]<-"Value"
qtr_bop_mt_gdp = table_preprocessing_annual(qtr_bop_mt_gdp,frequency_normalizer='quarter',
                                    period=5,
                                    variable = "(as % of GDP)",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)



#Eco-Indicators | Current Account | Goods and Services
qtr_bop_exp=read_query(2303192,15,quarter=TRUE)
names(qtr_bop_exp)[1]<-"Relevant_Date"
names(qtr_bop_exp)[2]<-"Value"
qtr_bop_exp = table_preprocessing_annual(qtr_bop_exp,frequency_normalizer='quarter',
                                    period=5,
                                    variable = "b.  Exports",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Goods
qtr_bop_imp=read_query(2303193,15,quarter=TRUE)
names(qtr_bop_imp)[1]<-"Relevant_Date"
names(qtr_bop_imp)[2]<-"Value"
qtr_bop_imp = table_preprocessing_annual(qtr_bop_imp,frequency_normalizer='quarter',
                                    period=5, variable = "c.  Imports",
                                    calculate_gr = FALSE,divisor =10^9, rounder = 1)



#Eco-Indicators | Current Account | Services
qtr_bop_s=read_query(1633412,15,quarter=TRUE)
names(qtr_bop_s)[1]<-"Relevant_Date"
names(qtr_bop_s)[2]<-"Value"
qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='quarter',period=5,
                                    variable = "d.  Services trade", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Primary Income
qtr_bop_i=read_query(1633404 ,15,quarter=TRUE)
names(qtr_bop_i)[1]<-"Relevant_Date"
names(qtr_bop_i)[2]<-"Value"
qtr_bop_i = table_preprocessing_annual(qtr_bop_i,frequency_normalizer='quarter',period=5,
                                    variable = "e.  Income",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Secondary Income
qtr_bop_t=read_query(1633405 ,15,quarter=TRUE)
names(qtr_bop_t)[1]<-"Relevant_Date"
names(qtr_bop_t)[2]<-"Value"
qtr_bop_t = table_preprocessing_annual(qtr_bop_t,frequency_normalizer='quarter',period=5,
                                    variable = "f.  Transfers",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Capital Account
qtr_bop_caa=read_query(1640367   ,15,quarter=TRUE)
names(qtr_bop_caa)[1]<-"Relevant_Date"
names(qtr_bop_caa)[2]<-"Value"
qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='quarter',period=5,
                                    variable = "g.  Current account (a + d + e + f)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Capital Account
qtr_bop_ca_gdp=read_query(1783070,15,quarter=TRUE)
names(qtr_bop_ca_gdp)[1]<-"Relevant_Date"
names(qtr_bop_ca_gdp)[2]<-"Value"
qtr_bop_ca_gdp = table_preprocessing_annual(qtr_bop_ca_gdp,
                                    frequency_normalizer='quarter',period=5,
                                    variable = "(as % of GDP) ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)



#Eco-Indicators | Financial Account | Direct Investment
qtr_bop_fdi=read_query(1633406 ,15,quarter=TRUE)
names(qtr_bop_fdi)[1]<-"Relevant_Date"
names(qtr_bop_fdi)[2]<-"Value"
qtr_bop_fdi = table_preprocessing_annual(qtr_bop_fdi,frequency_normalizer='quarter',
                                    period=5, variable = "h.  Foreign direct investment",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Financial Account | Portfolio Investment
qtr_bop_pf=read_query(1633407 ,15,quarter=TRUE)
names(qtr_bop_pf)[1]<-"Relevant_Date"
names(qtr_bop_pf)[2]<-"Value"
qtr_bop_pf = table_preprocessing_annual(qtr_bop_pf,frequency_normalizer='quarter',period=5,
                                    variable = "i.  Portfolio",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



qtr_bop_lo=read_query(2303195 ,15,quarter=TRUE)
names(qtr_bop_lo)[1]<-"Relevant_Date"
names(qtr_bop_lo)[2]<-"Value"
qtr_bop_lo = table_preprocessing_annual(qtr_bop_lo,
                                    frequency_normalizer='quarter',period=5,
                                    variable = "j.  Loans (external assistance,commercial borrowings, short term credit to india)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Financial Account | Financial derivatives
qtr_bop_bc=read_query(2303196 ,15,quarter=TRUE)
names(qtr_bop_bc)[1]<-"Relevant_Date"
names(qtr_bop_bc)[2]<-"Value"
qtr_bop_bc = table_preprocessing_annual(qtr_bop_bc,frequency_normalizer='quarter',
                                    period=5, variable = "k.  Banking capital",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Financial Account | Other investment
qtr_bop_oth=read_query(2303197 ,15,quarter=TRUE)
names(qtr_bop_oth)[1]<-"Relevant_Date"
names(qtr_bop_oth)[2]<-"Value"
qtr_bop_oth = table_preprocessing_annual(qtr_bop_oth,frequency_normalizer='quarter',period=5,
                                    variable = "l.  Others",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Financial Account | Reserve assets
qtr_bop_ra=read_query(2303194 ,15,quarter=TRUE)
names(qtr_bop_ra)[1]<-"Relevant_Date"
names(qtr_bop_ra)[2]<-"Value"
qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='quarter',period=5,
                                    variable = "m.  Capital account (h+ i + j + k + l)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account
qtr_bop_ra_gdp=read_query(2303202,15,quarter=TRUE)
names(qtr_bop_ra_gdp)[1]<-"Relevant_Date"
names(qtr_bop_ra_gdp)[2]<-"Value"
qtr_bop_ra_gdp = table_preprocessing_annual(qtr_bop_ra_gdp,
                                    frequency_normalizer='quarter',period=5,
                                    variable = "(as % of GDP)  ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)

#Eco-Indicators | Net errors and omissions
qtr_bop_eo=read_query(1632782 ,15,quarter=TRUE)
names(qtr_bop_eo)[1]<-"Relevant_Date"
names(qtr_bop_eo)[2]<-"Value"
qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='quarter',period=5,
                                    variable = "n.  Net errors and omissions",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Net errors and omissions
qtr_bop=read_query(2303203 ,15,quarter=TRUE)
names(qtr_bop)[1]<-"Relevant_Date"
names(qtr_bop)[2]<-"Value"
qtr_bop = table_preprocessing_annual(qtr_bop,frequency_normalizer='quarter',
                                    period=5,
                                    variable = "o.  Balance of payment (g + m + n)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Current Account
qtr_bop_bop_gdp=read_query(2303204,15,quarter=TRUE)
names(qtr_bop_bop_gdp)[1]<-"Relevant_Date"
names(qtr_bop_bop_gdp)[2]<-"Value"
qtr_bop_bop_gdp = table_preprocessing_annual(qtr_bop_bop_gdp,
                                    frequency_normalizer='quarter',period=5,
                                    variable = "(as % of GDP)   ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)



bop=bind_rows(qtr_bop_mt,qtr_bop_mt_gdp,qtr_bop_exp,qtr_bop_imp,qtr_bop_s,qtr_bop_i,
          qtr_bop_t,qtr_bop_caa,qtr_bop_ca_gdp,qtr_bop_fdi,qtr_bop_pf,qtr_bop_lo,
          qtr_bop_bc,
          qtr_bop_oth,qtr_bop_ra,qtr_bop_ra_gdp,qtr_bop_eo,qtr_bop,qtr_bop_bop_gdp)


bg = list(col_FDECDA = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
                         "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  "),

          col_D9D9D9=c("o.  Balance of payment (g + m + n)","(as % of GDP)   "))



border = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
           "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  ",
           "o.  Balance of payment (g + m + n)","(as % of GDP)   ")


bold = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
         "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  ",
         "o.  Balance of payment (g + m + n)","(as % of GDP)   ")


quarterly_bop_74_chart = economic_indicator_table(bop,
                                                 rounder_exeptions = bop$Variable,
                                                 background_vals = bg,
                                                 hlines = border, make_bold = bold,
                                                 font_size = 12,vlines=c(4),
                                                 var_col_width =2.9,other_col_width =1.25)



quarterly_bop_74_chart <- italic(quarterly_bop_74_chart,i=2,italic = TRUE,
                                 part = "body")
quarterly_bop_74_chart <- italic(quarterly_bop_74_chart,i=9,italic = TRUE,
                                 part = "body")
quarterly_bop_74_chart <- italic(quarterly_bop_74_chart,i=15,italic = TRUE,
                                 part = "body")
quarterly_bop_74_chart <- italic(quarterly_bop_74_chart,i=18,italic = TRUE,
                                 part = "body")

quarterly_bop_74_chart
data=read_query(1632782,15,quarter=TRUE)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_bop_74_title=heading(data)




## -------------------------------------
#Eco-Indicators | Current Account
# qtr_bop_ca=read_query(1640367,162,quarter=TRUE)
# names(qtr_bop_ca)[1]<-"Relevant_Date"
# names(qtr_bop_ca)[2]<-"Value"
# qtr_bop_ca = table_preprocessing_annual(qtr_bop_ca,frequency_normalizer='year',period=9,
#                                     variable = "Current account",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)


# #Eco-Indicators | Current Account | Goods and Services
# qtr_bop_gs=read_query(1633403,162,quarter=TRUE)
# names(qtr_bop_gs)[1]<-"Relevant_Date"
# names(qtr_bop_gs)[2]<-"Value"
# qtr_bop_gs = table_preprocessing_annual(qtr_bop_gs,frequency_normalizer='year',period=9,
#                                     variable = "Goods and services",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)



# #Eco-Indicators | Current Account | Goods
# qtr_bop_g=read_query(1633411,162,quarter=TRUE)
# names(qtr_bop_g)[1]<-"Relevant_Date"
# names(qtr_bop_g)[2]<-"Value"
# qtr_bop_g = table_preprocessing_annual(qtr_bop_g,frequency_normalizer='year',period=9,
#                                     variable = "Goods", calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)


# #Eco-Indicators | Current Account | Services
# qtr_bop_s=read_query(1633412,162,quarter=TRUE)
# names(qtr_bop_s)[1]<-"Relevant_Date"
# names(qtr_bop_s)[2]<-"Value"
# qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='year',period=9,
#                                     variable = "Services", calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)



# #Eco-Indicators | Current Account | Primary Income
# qtr_bop_pi=read_query(1633404 ,162,quarter=TRUE)
# names(qtr_bop_pi)[1]<-"Relevant_Date"
# names(qtr_bop_pi)[2]<-"Value"
# qtr_bop_pi = table_preprocessing_annual(qtr_bop_pi,frequency_normalizer='year',period=9,
#                                     variable = "Primary income",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)


# #Eco-Indicators | Current Account | Secondary Income
# qtr_bop_si=read_query(1633405,162,quarter=TRUE)
# names(qtr_bop_si)[1]<-"Relevant_Date"
# names(qtr_bop_si)[2]<-"Value"
# qtr_bop_si = table_preprocessing_annual(qtr_bop_si,frequency_normalizer='year',period=9,
#                                     variable = "Secondary income",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)



# #Eco-Indicators | Capital Account
# qtr_bop_caa=read_query(1640368,162,quarter=TRUE)
# names(qtr_bop_caa)[1]<-"Relevant_Date"
# names(qtr_bop_caa)[2]<-"Value"
# qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='year',period=9,
#                                     variable = "Capital account",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Financial Account
# qtr_bop_fa=read_query(1640369 ,162,quarter=TRUE)
# names(qtr_bop_fa)[1]<-"Relevant_Date"
# names(qtr_bop_fa)[2]<-"Value"
# qtr_bop_fa = table_preprocessing_annual(qtr_bop_fa,frequency_normalizer='year',period=9,
#                                     variable = "Financial accounts", calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Financial Account | Direct Investment
# qtr_bop_di=read_query(1633406 ,162,quarter=TRUE)
# names(qtr_bop_di)[1]<-"Relevant_Date"
# names(qtr_bop_di)[2]<-"Value"
# qtr_bop_di = table_preprocessing_annual(qtr_bop_di,frequency_normalizer='year',period=9,
#                                     variable = "Direct investments",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Financial Account | Portfolio Investment
# qtr_bop_poi=read_query(1633407 ,162,quarter=TRUE)
# names(qtr_bop_poi)[1]<-"Relevant_Date"
# names(qtr_bop_poi)[2]<-"Value"
# qtr_bop_poi = table_preprocessing_annual(qtr_bop_poi,frequency_normalizer='year',period=9,
#                                     variable = "Portfolio investments",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Financial Account | Financial derivatives
# qtr_bop_fd=read_query(1633408 ,162,quarter=TRUE)
# names(qtr_bop_fd)[1]<-"Relevant_Date"
# names(qtr_bop_fd)[2]<-"Value"
# qtr_bop_fd = table_preprocessing_annual(qtr_bop_fd,frequency_normalizer='year',period=9,
#                                     variable = "Financial derivatives (other than reserves)",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Financial Account | Other investment
# qtr_bop_oi=read_query(1633409 ,162,quarter=TRUE)
# names(qtr_bop_oi)[1]<-"Relevant_Date"
# names(qtr_bop_oi)[2]<-"Value"
# qtr_bop_oi = table_preprocessing_annual(qtr_bop_oi,frequency_normalizer='year',period=9,
#                                     variable = "Other investments",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)



# #Eco-Indicators | Financial Account | Reserve assets
# qtr_bop_ra=read_query(1633410 ,162,quarter=TRUE)
# names(qtr_bop_ra)[1]<-"Relevant_Date"
# names(qtr_bop_ra)[2]<-"Value"
# qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='year',period=9,
#                                     variable = "Reserve assets",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)

# #Eco-Indicators | Net errors and omissions
# qtr_bop_eo=read_query(1632782 ,162,quarter=TRUE)
# names(qtr_bop_eo)[1]<-"Relevant_Date"
# names(qtr_bop_eo)[2]<-"Value"
# qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='year',period=9,
#                                     variable = "Net errors and omissions",
#                                     calculate_gr = FALSE,
#                                     divisor =10^9, rounder = 1,fy_format=TRUE)


# bop=rbind(qtr_bop_ca,qtr_bop_gs,qtr_bop_g,qtr_bop_s,qtr_bop_pi,qtr_bop_si,qtr_bop_caa,
#           qtr_bop_fa,qtr_bop_di,qtr_bop_poi,qtr_bop_fd,qtr_bop_oi,qtr_bop_ra,qtr_bop_eo)


# bg = list(col_FDECDA = c("Current account","Capital account","Financial accounts",
#                          "Net errors and omissions"))

# padding = list(pad_10 = c("Goods and services","Primary income","Secondary income",
#                           "Direct investments","Portfolio investments",
#                           "Financial derivatives (other than reserves)",
#                           "Other investments","Reserve assets"),
#                pad_20 = c("Goods","Services"))


# border = c("Capital account","Financial accounts","Net errors and omissions")
# bold = c("Current account","Capital account","Financial accounts","Net errors and omissions")
# vborder =c(14)

# quarterly_bop_annual_chart = economic_indicator_table(bop,rounder_exeptions = bop$Variable,
#                                                  padding_vals = padding,
#                                                  background_vals = bg,
#                                                  hlines = border, make_bold = bold,
#                                                  font_size = 12,
#                                                  var_col_width =2,other_col_width =0.81)



# quarterly_bop_annual_chart
# data=read_query(1632782,162,quarter=TRUE)
# names(data)[1]<-"Relevant_Date"
# names(data)[2]<-"Value"
# quarterly_bop_annual_title=heading(data)

#Eco-Indicators | Current Account
qtr_bop_mt=read_query(1633411,108,quarter=TRUE)
names(qtr_bop_mt)[1]<-"Relevant_Date"
names(qtr_bop_mt)[2]<-"Value"
qtr_bop_mt = table_preprocessing_annual(qtr_bop_mt,
                                    frequency_normalizer='year',
                                    period=9,
                                    variable = "a.  Merchandize trade",
                                    calculate_gr = FALSE,fy_format=TRUE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account
qtr_bop_mt_gdp=read_query(2523352,108,quarter=TRUE)
names(qtr_bop_mt_gdp)[1]<-"Relevant_Date"
names(qtr_bop_mt_gdp)[2]<-"Value"
qtr_bop_mt_gdp = table_preprocessing_annual(qtr_bop_mt_gdp,frequency_normalizer='year',
                                    period=9,
                                    variable = "(as % of GDP)",
                                    calculate_gr = FALSE,fy_format=TRUE,
                                    divisor =1, rounder = 1)



#Eco-Indicators | Current Account | Goods and Services
qtr_bop_exp=read_query(2303192,108,quarter=TRUE)
names(qtr_bop_exp)[1]<-"Relevant_Date"
names(qtr_bop_exp)[2]<-"Value"
qtr_bop_exp = table_preprocessing_annual(qtr_bop_exp,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "b.  Exports",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Goods
qtr_bop_imp=read_query(2303193,108,quarter=TRUE)
names(qtr_bop_imp)[1]<-"Relevant_Date"
names(qtr_bop_imp)[2]<-"Value"
qtr_bop_imp = table_preprocessing_annual(qtr_bop_imp,frequency_normalizer='year',
                                    period=9,fy_format=TRUE, variable = "c.  Imports",
                                    calculate_gr = FALSE,divisor =10^9, rounder = 1)



#Eco-Indicators | Current Account | Services
qtr_bop_s=read_query(1633412,108,quarter=TRUE)
names(qtr_bop_s)[1]<-"Relevant_Date"
names(qtr_bop_s)[2]<-"Value"
qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "d.  Services trade",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Primary Income
qtr_bop_i=read_query(1633404 ,108,quarter=TRUE)
names(qtr_bop_i)[1]<-"Relevant_Date"
names(qtr_bop_i)[2]<-"Value"
qtr_bop_i = table_preprocessing_annual(qtr_bop_i,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "e.  Income",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Current Account | Secondary Income
qtr_bop_t=read_query(1633405 ,108,quarter=TRUE)
names(qtr_bop_t)[1]<-"Relevant_Date"
names(qtr_bop_t)[2]<-"Value"
qtr_bop_t = table_preprocessing_annual(qtr_bop_t,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "f.  Transfers",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Capital Account
qtr_bop_caa=read_query(1640367   ,108,quarter=TRUE)
names(qtr_bop_caa)[1]<-"Relevant_Date"
names(qtr_bop_caa)[2]<-"Value"
qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "g.  Current account (a + d + e + f)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)




#Eco-Indicators | Capital Account
qtr_bop_ca_gdp=read_query(2523348,108,quarter=TRUE)
names(qtr_bop_ca_gdp)[1]<-"Relevant_Date"
names(qtr_bop_ca_gdp)[2]<-"Value"
qtr_bop_ca_gdp = table_preprocessing_annual(qtr_bop_ca_gdp,
                                    frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "(as % of GDP) ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)



#Eco-Indicators | Financial Account | Direct Investment
qtr_bop_fdi=read_query(1633406 ,108,quarter=TRUE)
names(qtr_bop_fdi)[1]<-"Relevant_Date"
names(qtr_bop_fdi)[2]<-"Value"
qtr_bop_fdi = table_preprocessing_annual(qtr_bop_fdi,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "h.  Foreign direct investment",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Financial Account | Portfolio Investment
qtr_bop_pf=read_query(1633407,108,quarter=TRUE)
names(qtr_bop_pf)[1]<-"Relevant_Date"
names(qtr_bop_pf)[2]<-"Value"
qtr_bop_pf = table_preprocessing_annual(qtr_bop_pf,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "i.  Portfolio",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



qtr_bop_lo=read_query(2303195,108,quarter=TRUE)
names(qtr_bop_lo)[1]<-"Relevant_Date"
names(qtr_bop_lo)[2]<-"Value"
qtr_bop_lo = table_preprocessing_annual(qtr_bop_lo,
                  frequency_normalizer='year',
                  period=9,fy_format=TRUE,
                  variable = "j.  Loans (external assistance,commercial borrowings, short term credit to india)",
                  calculate_gr = FALSE,
                  divisor =10^9, rounder = 1)


#Eco-Indicators | Financial Account | Financial derivatives
qtr_bop_bc=read_query(2303196 ,108,quarter=TRUE)
names(qtr_bop_bc)[1]<-"Relevant_Date"
names(qtr_bop_bc)[2]<-"Value"
qtr_bop_bc = table_preprocessing_annual(qtr_bop_bc,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "k.  Banking capital",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Financial Account | Other investment
qtr_bop_oth=read_query(2303197 ,108,quarter=TRUE)
names(qtr_bop_oth)[1]<-"Relevant_Date"
names(qtr_bop_oth)[2]<-"Value"
qtr_bop_oth = table_preprocessing_annual(qtr_bop_oth,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "l.  Others",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Financial Account | Reserve assets
qtr_bop_ra=read_query(2303194 ,108,quarter=TRUE)
names(qtr_bop_ra)[1]<-"Relevant_Date"
names(qtr_bop_ra)[2]<-"Value"
qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "m.  Capital account (h+ i + j + k + l)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account
qtr_bop_ra_gdp=read_query(2523349,108,quarter=TRUE)
names(qtr_bop_ra_gdp)[1]<-"Relevant_Date"
names(qtr_bop_ra_gdp)[2]<-"Value"
qtr_bop_ra_gdp = table_preprocessing_annual(qtr_bop_ra_gdp,
                                    frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "(as % of GDP)  ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)

#Eco-Indicators | Net errors and omissions
qtr_bop_eo=read_query(1632782 ,108,quarter=TRUE)
names(qtr_bop_eo)[1]<-"Relevant_Date"
names(qtr_bop_eo)[2]<-"Value"
qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "n.  Net errors and omissions",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Net errors and omissions
qtr_bop=read_query(2303203 ,108,quarter=TRUE)
names(qtr_bop)[1]<-"Relevant_Date"
names(qtr_bop)[2]<-"Value"
qtr_bop = table_preprocessing_annual(qtr_bop,frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "o.  Balance of payment (g + m + n)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Current Account
qtr_bop_bop_gdp=read_query(2523350,108,quarter=TRUE)
names(qtr_bop_bop_gdp)[1]<-"Relevant_Date"
names(qtr_bop_bop_gdp)[2]<-"Value"
qtr_bop_bop_gdp = table_preprocessing_annual(qtr_bop_bop_gdp,
                                    frequency_normalizer='year',
                                    period=9,fy_format=TRUE,
                                    variable = "(as % of GDP)   ",
                                    calculate_gr = FALSE,
                                    divisor =1, rounder = 1)



bop=bind_rows(qtr_bop_mt,qtr_bop_mt_gdp,qtr_bop_exp,qtr_bop_imp,qtr_bop_s,qtr_bop_i,
          qtr_bop_t,qtr_bop_caa,qtr_bop_ca_gdp,qtr_bop_fdi,qtr_bop_pf,qtr_bop_lo,
          qtr_bop_bc,
          qtr_bop_oth,qtr_bop_ra,qtr_bop_ra_gdp,qtr_bop_eo,qtr_bop,qtr_bop_bop_gdp)

# bop=rbind(qtr_bop_mt,qtr_bop_mt_gdp,qtr_bop_exp,qtr_bop_imp,qtr_bop_s,qtr_bop_i,
#           qtr_bop_t,qtr_bop_caa,qtr_bop_ca_gdp,qtr_bop_fdi,qtr_bop_pf,qtr_bop_lo,
#           qtr_bop_bc,
#           qtr_bop_oth,qtr_bop_ra,qtr_bop_ra_gdp,qtr_bop_eo,qtr_bop,qtr_bop_bop_gdp)

bg = list(col_FDECDA = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
                         "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  "),

          col_D9D9D9=c("o.  Balance of payment (g + m + n)","(as % of GDP)   "))



border = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
           "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  ",
           "o.  Balance of payment (g + m + n)","(as % of GDP)   ")


bold = c("g.  Current account (a + d + e + f)","(as % of GDP) ",
         "m.  Capital account (h+ i + j + k + l)","(as % of GDP)  ",
         "o.  Balance of payment (g + m + n)","(as % of GDP)   ")


quarterly_bop_annual_chart = economic_indicator_table(bop,
                                                 rounder_exeptions = bop$Variable,
                                                 background_vals = bg,
                                                 hlines = border, make_bold = bold,
                                                 font_size = 12,
                                                 var_col_width =3,other_col_width =0.81)



quarterly_bop_annual_chart <- italic(quarterly_bop_annual_chart,i=2,italic = TRUE,
                                 part = "body")
quarterly_bop_annual_chart <- italic(quarterly_bop_annual_chart,i=9,italic = TRUE,
                                 part = "body")
quarterly_bop_annual_chart <- italic(quarterly_bop_annual_chart,i=15,italic = TRUE,
                                 part = "body")
quarterly_bop_annual_chart <- italic(quarterly_bop_annual_chart,i=18,italic = TRUE,
                                 part = "body")

quarterly_bop_annual_chart
data=read_query(1632782,36,quarter=TRUE)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_bop_annual_title=heading(data)




## -------------------------------------
#MF | AUTO
qtr_mfh_au=read_query(1633786,36,quarter=FALSE)
names(qtr_mfh_au)[1]<-"Relevant_Date"
names(qtr_mfh_au)[2]<-"Value"
qtr_mfh_auto = table_preprocessing_annual(qtr_mfh_au,frequency_normalizer='month',period=1,
                                    variable = "Auto",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | AUTO ANCILLARIES
qtr_mfh_au_an=read_query(1633756,36,quarter=FALSE)
names(qtr_mfh_au_an)[1]<-"Relevant_Date"
names(qtr_mfh_au_an)[2]<-"Value"
qtr_mfh_au_an = table_preprocessing_annual(qtr_mfh_au_an,frequency_normalizer='month',period=1,
                                    variable = "Auto ancillaries",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | BANKS
qtr_mfh_bnk=read_query(1633807,36,quarter=FALSE)
names(qtr_mfh_bnk)[1]<-"Relevant_Date"
names(qtr_mfh_bnk)[2]<-"Value"
qtr_mfh_bnk = table_preprocessing_annual(qtr_mfh_bnk,frequency_normalizer='month',period=1,
                                    variable = "Banks",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | CEMENT
qtr_mfh_cmt=read_query(1633828,36,quarter=FALSE)
names(qtr_mfh_cmt)[1]<-"Relevant_Date"
names(qtr_mfh_cmt)[2]<-"Value"
qtr_mfh_cmt = table_preprocessing_annual(qtr_mfh_cmt,frequency_normalizer='month',period=1,
                                    variable = "Cement",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



#MF | CHEMICALS
qtr_mfh_ch=read_query(1633753,36,quarter=FALSE)
names(qtr_mfh_ch)[1]<-"Relevant_Date"
names(qtr_mfh_ch)[2]<-"Value"
qtr_mfh_ch = table_preprocessing_annual(qtr_mfh_ch,frequency_normalizer='month',period=1,
                                    variable = "Chemicals",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



#MF | CONSTRUCTION PROJECT
qtr_mfh_conp=read_query(1633783,36,quarter=FALSE)
names(qtr_mfh_conp)[1]<-"Relevant_Date"
names(qtr_mfh_conp)[2]<-"Value"
qtr_mfh_conp = table_preprocessing_annual(qtr_mfh_conp,frequency_normalizer='month',period=1,
                                    variable = "Construction project",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | CONSUMER DURABLES
qtr_mfh_con_du=read_query(1633846,36,quarter=FALSE)
names(qtr_mfh_con_du)[1]<-"Relevant_Date"
names(qtr_mfh_con_du)[2]<-"Value"
qtr_mfh_con_du = table_preprocessing_annual(qtr_mfh_con_du,frequency_normalizer='month',period=1,
                                    variable = "Consumer durables",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)


#MF | CONSUMER NON DURABLES
qtr_mfh_con_n_du=read_query(1633822,36,quarter=FALSE)
names(qtr_mfh_con_n_du)[1]<-"Relevant_Date"
names(qtr_mfh_con_n_du)[2]<-"Value"
qtr_mfh_con_n_du = table_preprocessing_annual(qtr_mfh_con_n_du,frequency_normalizer='month',period=1,
                                    variable = "Consumer non durables",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | FERROUS METALS
qtr_mfh_fm=read_query(1633852,36,quarter=FALSE)
names(qtr_mfh_fm)[1]<-"Relevant_Date"
names(qtr_mfh_fm)[2]<-"Value"
qtr_mfh_fm = table_preprocessing_annual(qtr_mfh_fm,frequency_normalizer='month',period=1,
                                    variable = "Ferrous metals",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | FINANCE
qtr_mfh_fi=read_query(1633765,36,quarter=FALSE)
names(qtr_mfh_fi)[1]<-"Relevant_Date"
names(qtr_mfh_fi)[2]<-"Value"
qtr_mfh_fi = table_preprocessing_annual(qtr_mfh_fi,frequency_normalizer='month',period=1,
                                    variable = "Finance",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)


#MF | PETROLEUM PRODUCTS
qtr_mfh_pp=read_query(1633834,36,quarter=FALSE)
names(qtr_mfh_pp)[1]<-"Relevant_Date"
names(qtr_mfh_pp)[2]<-"Value"
qtr_mfh_pp = table_preprocessing_annual(qtr_mfh_pp,frequency_normalizer='month',period=1,
                                    variable = "Petroleum products",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)


#MF | PHARMACEUTICALS
qtr_mfh_phar=read_query(1633792,36,quarter=FALSE)
names(qtr_mfh_phar)[1]<-"Relevant_Date"
names(qtr_mfh_phar)[2]<-"Value"
qtr_mfh_phar = table_preprocessing_annual(qtr_mfh_phar,frequency_normalizer='month',period=1,
                                    variable = "Pharmaceuticals",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)


#MF | POWER
qtr_mfh_pwr=read_query(1633741,36,quarter=FALSE)
names(qtr_mfh_pwr)[1]<-"Relevant_Date"
names(qtr_mfh_pwr)[2]<-"Value"
qtr_mfh_pwr = table_preprocessing_annual(qtr_mfh_pwr,frequency_normalizer='month',period=1,
                                    variable = "Power",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



#MF | RETAILING
qtr_mfh_rtl=read_query(1633810,36,quarter=FALSE)
names(qtr_mfh_rtl)[1]<-"Relevant_Date"
names(qtr_mfh_rtl)[2]<-"Value"
qtr_mfh_rtl = table_preprocessing_annual(qtr_mfh_rtl,frequency_normalizer='month',period=1,
                                    variable = "Retailing",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



#MF | SOFTWARE
qtr_mfh_sof=read_query(1633813,36,quarter=FALSE)
names(qtr_mfh_sof)[1]<-"Relevant_Date"
names(qtr_mfh_sof)[2]<-"Value"
qtr_mfh_sof = table_preprocessing_annual(qtr_mfh_sof,frequency_normalizer='month',period=1,
                                    variable = "Software",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | Equity Holdings | TELECOM - SERVICES
qtr_mfh_tele=read_query(1633798,36,quarter=FALSE)
names(qtr_mfh_tele)[1]<-"Relevant_Date"
names(qtr_mfh_tele)[2]<-"Value"
qtr_mfh_tele = table_preprocessing_annual(qtr_mfh_tele,frequency_normalizer='month',period=1,
                                    variable = "Telecom services",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)

#MF | Equity Holdings | OTHERS
qtr_mfh_oth=read_query(1640371,36,quarter=FALSE)
names(qtr_mfh_oth)[1]<-"Relevant_Date"
names(qtr_mfh_oth)[2]<-"Value"
qtr_mfh_oth = table_preprocessing_annual(qtr_mfh_oth,frequency_normalizer='month',period=1,
                                    variable = "Others",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



#MF | Equity
qtr_mfh_t=read_query(1633862,36,quarter=FALSE)
names(qtr_mfh_t)[1]<-"Relevant_Date"
names(qtr_mfh_t)[2]<-"Value"
qtr_mfh_t = table_preprocessing_annual(qtr_mfh_t,frequency_normalizer='month',period=1,
                                    variable = "Total",
                                    calculate_gr = TRUE,
                                    divisor =10^12, rounder = 1)



qtr_mf_hold2=rbind(qtr_mfh_t,qtr_mfh_auto,qtr_mfh_au_an,qtr_mfh_bnk,qtr_mfh_cmt,qtr_mfh_ch,
                  qtr_mfh_conp,
                  qtr_mfh_con_du,qtr_mfh_con_n_du,qtr_mfh_fm,qtr_mfh_fi,qtr_mfh_pp,qtr_mfh_phar,
                  qtr_mfh_pwr,qtr_mfh_rtl,qtr_mfh_sof,qtr_mfh_tele)


##HARD CODE
qtr_mf_hold2 <- qtr_mf_hold2[order(as.numeric(qtr_mf_hold2$"Sep-23"),
                                       decreasing = TRUE),]
qtr_mf_hold2=rbind(qtr_mf_hold2,qtr_mfh_oth)


# qtr_mf_hold2=new_row_insertion(qtr_mf_hold,new_row_name="Others",sub_row_name="Total")

border = c("Total","Auto")
bold = c("Total")
bg = list(col_FDECDA =c("Total"))
vborder = c(14)
quarterly_mfh_75_chart = economic_indicator_table(qtr_mf_hold2,
                        rounder_exeptions =qtr_mf_hold2$Variable,
                        font_size = 12,
                        background_vals = bg,
                        make_bold = bold,vlines =vborder,
                        var_col_width =3.5,other_col_width =0.75)

quarterly_mfh_75_chart <- quarterly_mfh_75_chart %>% italic( j = 15,italic = TRUE, part = "all")
quarterly_mfh_75_chart
data=read_query(1633862,12)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_mfh_75_title=heading(data,title='month')



## -------------------------------------
#Capital Flows | FDI | COMPUTER SOFTWARE AND HARDWARE
fdi_computer = read_query(1633154,12)
names(fdi_computer)[1]<-"Relevant_Date"
names(fdi_computer)[2]<-"Value"
fdi_computer = table_preprocessing_annual(fdi_computer,
                              frequency_normalizer='year',
                              variable ="Computer software and hardware",
                              sector ='Sector',
                              calculate_gr = FALSE,period=10,
                              divisor =10^9,rounder = 1)


#Capital Flows | FDI | CONSTRUCTION INFRASTRUCTURE ACTIVITIES
fdi_const = read_query(1633178,12)
names(fdi_const)[1]<-"Relevant_Date"
names(fdi_const)[2]<-"Value"
fdi_const = table_preprocessing_annual(fdi_const,frequency_normalizer='year',
                               variable = "Construction and infrastructure",
                               sector ='Sector',
                               calculate_gr = FALSE,period=10,
                               divisor =10^9,rounder = 1)

#Capital Flows | FDI | SERVICES SECTOR
fdi_service = read_query(1633144,12)
names(fdi_service)[1]<-"Relevant_Date"
names(fdi_service)[2]<-"Value"
fdi_service = table_preprocessing_annual(fdi_service,frequency_normalizer='year',
                                    variable = "Services",sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | AUTOMOBILE INDUSTRY
fdi_automobile = read_query(1633113,12)
names(fdi_automobile)[1]<-"Relevant_Date"
names(fdi_automobile)[2]<-"Value"
fdi_automobile = table_preprocessing_annual(fdi_automobile,frequency_normalizer='year',
                                    variable = "Automobile",sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)


#Capital Flows | FDI | TRADING
fdi_trading = read_query(1633174,12)
names(fdi_trading)[1]<-"Relevant_Date"
names(fdi_trading)[2]<-"Value"
fdi_trading = table_preprocessing_annual(fdi_trading,frequency_normalizer='year',
                                    variable = "Trading",sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | DRUGS AND PHARMACEUTICALS
fdi_drugs = read_query(1633165,12)
names(fdi_drugs)[1]<-"Relevant_Date"
names(fdi_drugs)[2]<-"Value"
fdi_drugs = table_preprocessing_annual(fdi_drugs,frequency_normalizer='year',
                                    variable = "Drugs and pharmaceuticals",
                                    sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CHEMICALS OTHER THAN FERTILIZER
fdi_chem = read_query(1633169,12)
names(fdi_chem)[1]<-"Relevant_Date"
names(fdi_chem)[2]<-"Value"
fdi_chem = table_preprocessing_annual(fdi_chem,frequency_normalizer='year',
                                    variable = "Chemicals,excl. Fertilizers",
                                    sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | TELECOMMUNICATIONS
fdi_tele = read_query(1633160,12)
names(fdi_tele)[1]<-"Relevant_Date"
names(fdi_tele)[2]<-"Value"
fdi_tele = table_preprocessing_annual(fdi_tele,frequency_normalizer='year',
                                    variable = "Telecommunications",
                                    sector ='Sector',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)



fdi_Sector = rbind(fdi_computer,fdi_const,fdi_service,fdi_trading,fdi_automobile,fdi_drugs,
                   fdi_chem,
                   fdi_tele)

fdi_Sector <- fdi_Sector[order(as.numeric(fdi_Sector$'2024'), decreasing = TRUE),]


#Capital Flows | FDI
fdi_gross = read_query(1681734,12)
names(fdi_gross)[1]<-"Relevant_Date"
names(fdi_gross)[2]<-"Value"
fdi_gross=subset(fdi_gross,(Relevant_Date>=min(read_query(1633160,12)$Relevant_Date) &Relevant_Date<=max( read_query(1633160,12)$Relevant_Date)))

fdi_gross = table_preprocessing_annual(fdi_gross,frequency_normalizer='year',
                                variable = "Gross FDI inflows",sector ='Sector',
                                calculate_gr = FALSE,period=10,
                                divisor =10^9, rounder = 1)


#Capital Flows | FDI
#Capital Flows | FDI
fdi_total_1 = read_query(1681734,10)
names(fdi_total_1)[1]<-"Relevant_Date"
fdi_total_1=subset(fdi_total_1,(Relevant_Date>=min(read_query(1633160,12)$Relevant_Date) &Relevant_Date<=max( read_query(1633160,12)$Relevant_Date)))

fdi_Sector[, 3:ncol(fdi_Sector)] <- sapply(fdi_Sector[, 3:3:ncol(fdi_Sector)], as.numeric)
fdi_mjr_sectors=as.data.frame(colSums(fdi_Sector[, c(3,4,5,6,7,8,9,10,11,12,13)], na.rm=TRUE))
fdi_mjr_sectors['Relevant_Date']=as.Date(fdi_total_1$Relevant_Date)
names(fdi_mjr_sectors)[1]<-"Value"
fdi_mjr_sectors=fdi_mjr_sectors[,c('Relevant_Date',"Value")]

fdi_mjr_sectors = table_preprocessing_annual(fdi_mjr_sectors,frequency_normalizer='year',
                                  variable = "FDI inflows across major sectors",
                                  sector ='Sector',
                                  calculate_gr = FALSE,period=10,
                                  rounder = 1)


fdi_Sector=rbind(fdi_Sector,fdi_mjr_sectors,fdi_gross)




bg = list(col_FDECDA = c("Gross FDI inflows"),
          col_D9D9D9 = c("FDI inflows across major sectors"))
bold = c("FDI inflows across major sectors","Gross FDI inflows")
borders = c("FDI inflows across major sectors","Gross FDI inflows")

Annual_FDI_inflows_by_Sector_76_chart  =economic_indicator_table(fdi_Sector,
                                                                 has_main_sector = TRUE,
                                                has_units = FALSE,
                                                color_coding = FALSE,rounder_exeptions =
                                                fdi_Sector$Variable,  background_vals = bg,
                                                font_size = 12, var_col_width = 2,
                                                make_bold = bold,hlines = borders,
                                                other_col_width = 0.8921)





data=read_query(1633154,9)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
Annual_FDI_inflows_by_Sector_76_title=heading(data)
Annual_FDI_inflows_by_Sector_76_chart


## -------------------------------------

#Capital Flows | FDI | SINGAPORE
fdi_singapore = read_query(1633159,12)
names(fdi_singapore)[1]<-"Relevant_Date"
names(fdi_singapore)[2]<-"Value"
fdi_singapore = table_preprocessing_annual(fdi_singapore,frequency_normalizer='year',
                                    variable = "Singapore",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9,rounder = 1)


#Capital Flows | FDI | U.S.A.
fdi_usa = read_query(1633128,12)
names(fdi_usa)[1]<-"Relevant_Date"
names(fdi_usa)[2]<-"Value"
fdi_usa = table_preprocessing_annual(fdi_usa,frequency_normalizer='year',
                               variable = "USA",sector ='Country',
                               calculate_gr = FALSE,period=10,
                               divisor =10^9,rounder = 1)

#Capital Flows | FDI | MAURITIUS
fdi_mauritius = read_query(1633175,12)
names(fdi_mauritius)[1]<-"Relevant_Date"
names(fdi_mauritius)[2]<-"Value"
fdi_mauritius = table_preprocessing_annual(fdi_mauritius,frequency_normalizer='year',
                                    variable = "Mauritius",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | UAE
fdi_uae = read_query(1633152,12)
names(fdi_uae)[1]<-"Relevant_Date"
names(fdi_uae)[2]<-"Value"
fdi_uae = table_preprocessing_annual(fdi_uae,frequency_normalizer='year',
                                    variable = "UAE",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)


#Capital Flows | FDI | NETHERLAND
fdi_netherlands = read_query(1633145,12)
names(fdi_netherlands)[1]<-"Relevant_Date"
names(fdi_netherlands)[2]<-"Value"
fdi_netherlands = table_preprocessing_annual(fdi_netherlands,frequency_normalizer='year',
                                    variable = "Netherlands",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CAYMAN ISLANDS
fdi_Cayman = read_query(1633179,12)
names(fdi_Cayman)[1]<-"Relevant_Date"
names(fdi_Cayman)[2]<-"Value"
fdi_Cayman = table_preprocessing_annual(fdi_Cayman,frequency_normalizer='year',
                                    variable = "Cayman islands",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | UNITED KINGDOM
fdi_uk = read_query(1633171,12)
names(fdi_uk)[1]<-"Relevant_Date"
names(fdi_uk)[2]<-"Value"
fdi_uk = table_preprocessing_annual(fdi_uk,frequency_normalizer='year',
                                    variable = "UK",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | JAPAN
fdi_japan = read_query(1633112,12)
names(fdi_japan)[1]<-"Relevant_Date"
names(fdi_japan)[2]<-"Value"
fdi_japan = table_preprocessing_annual(fdi_japan,frequency_normalizer='year',
                                    variable = "Japan",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | GERMANY
fdi_germany = read_query(1633168,12)
names(fdi_germany)[1]<-"Relevant_Date"
names(fdi_germany)[2]<-"Value"
fdi_germany = table_preprocessing_annual(fdi_germany,frequency_normalizer='year',
                                    variable = "Germany",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CYPRUS
fdi_cyprus = read_query(1633164,12)
names(fdi_cyprus)[1]<-"Relevant_Date"
names(fdi_cyprus)[2]<-"Value"
fdi_cyprus = table_preprocessing_annual(fdi_cyprus,frequency_normalizer='year',
                                    variable = "Cyprus",sector ='Country',
                                    calculate_gr = FALSE,period=10,
                                    divisor =10^9, rounder = 1)

fdi_country = rbind(fdi_singapore,fdi_cyprus,fdi_usa,fdi_mauritius,fdi_uae,fdi_netherlands,fdi_Cayman,
                    fdi_uk,fdi_japan,fdi_germany)

fdi_country <- fdi_country[order(as.numeric(fdi_country$'2024'),decreasing = TRUE),]


#Capital Flows | FDI
fdi_total = read_query(1681734,12)
names(fdi_total)[1]<-"Relevant_Date"
names(fdi_total)[2]<-"Value"
fdi_total=subset(fdi_total,(Relevant_Date>=min(read_query(1633164,12)$Relevant_Date) &Relevant_Date<=max( read_query(1633164,12)$Relevant_Date)))
fdi_total = table_preprocessing_annual(fdi_total,frequency_normalizer='year',
                                variable = "Total FDI Inflows",sector ='Country',
                                calculate_gr = FALSE,period=10,
                                divisor =10^9, rounder = 1)


#Capital Flows | FDI
fdi_total_1 = read_query(1681734,10)
names(fdi_total_1)[1]<-"Relevant_Date"
fdi_total_1=subset(fdi_total_1,(Relevant_Date>=min(read_query(1633164,12)$Relevant_Date) &Relevant_Date<=max( read_query(1633164,12)$Relevant_Date)))

fdi_country[, 3:ncol(fdi_country)] <- sapply(fdi_country[, 3:ncol(fdi_country)], as.numeric)
fdi_mjr_inv=as.data.frame(colSums(fdi_country[, c(3,4,5,6,7,8,9,10,11,12,13)], na.rm=TRUE))
fdi_mjr_inv['Relevant_Date']=as.Date(fdi_total_1$Relevant_Date)
names(fdi_mjr_inv)[1]<-"Value"
fdi_mjr_inv=fdi_mjr_inv[,c('Relevant_Date',"Value")]

fdi_mjr_inv = table_preprocessing_annual(fdi_mjr_inv,frequency_normalizer='year',
                                  variable = "FDI inflows from major investors",
                                  sector ='Country',period=10,
                                  calculate_gr = FALSE,
                                  rounder = 1)


fdi_country=rbind(fdi_country,fdi_mjr_inv,fdi_total)

bg = list(col_FDECDA = c("Total FDI Inflows"),col_D9D9D9 = c("FDI inflows from major investors"))
bold = c("FDI inflows from major investors","Total FDI Inflows")
borders = c("FDI inflows from major investors","Total FDI Inflows")




Annual_FDI_inflows_by_country_77_chart  =economic_indicator_table(fdi_country,has_main_sector = TRUE,
                                                has_units = FALSE,
                                                color_coding = FALSE,rounder_exeptions =
                                                fdi_country$Variable,  background_vals = bg,
                                                make_bold = bold,hlines = borders,
                                                font_size = 12,var_col_width = 2,
                                                other_col_width = 0.8921)



Annual_FDI_inflows_by_country_77_chart
data=read_query(1633164,9)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
Annual_FDI_inflows_by_country_77_title=heading(data)



## -------------------------------------
req_date <- as.Date(timeLastDayInMonth(today() %m-% months(1)))

## BSE Sensex
bse_sensex = read_query(2314934,2)
bse_sensex=bse_sensex[,c("Relevant_Date","growth")]
names(bse_sensex)[1]<-"Relevant_Date"
names(bse_sensex)[2]<-"Units"
bse_sensex$Variable = 'BSE Sensex'
bse_sensex <- bse_sensex %>%
  select(Variable, everything())
bse_sensex = subset(bse_sensex, bse_sensex$Relevant_Date <= req_date)
bse_sensex <- tail(bse_sensex, 1)
rownames(bse_sensex) <- NULL
bse_sensex=bse_sensex[,c("Variable","Units")]


## NSE Nifty 50
nse_nifty_50 = read_query(318890,2)
nse_nifty_50=nse_nifty_50[,c("Relevant_Date","growth")]
names(nse_nifty_50)[1]<-"Relevant_Date"
names(nse_nifty_50)[2]<-"Units"
nse_nifty_50$Variable = 'NSE Nifty 50'
nse_nifty_50 <- nse_nifty_50 %>%
  select(Variable, everything())
nse_nifty_50 = subset(nse_nifty_50, nse_nifty_50$Relevant_Date <= req_date)
nse_nifty_50 <- tail(nse_nifty_50, 1)
rownames(nse_nifty_50) <- NULL
nse_nifty_50=nse_nifty_50[,c("Variable","Units")]

## FTSE 100
# 318896
ftse_100 = read_query(1525121,2)
ftse_100=ftse_100[,c("Relevant_Date","growth")]
names(ftse_100)[1]<-"Relevant_Date"
names(ftse_100)[2]<-"Units"
ftse_100$Variable = 'FTSE 100'
ftse_100 <- ftse_100 %>%
  select(Variable, everything())
ftse_100 = subset(ftse_100, ftse_100$Relevant_Date <= req_date)
ftse_100 <- tail(ftse_100, 1)
rownames(ftse_100) <- NULL
ftse_100=ftse_100[,c("Variable","Units")]

## NIKKEI 225
nikkei_225 = read_query(1525122,2)
nikkei_225=nikkei_225[,c("Relevant_Date","growth")]
names(nikkei_225)[1]<-"Relevant_Date"
names(nikkei_225)[2]<-"Units"
nikkei_225$Variable = 'NIKKEI 225'
nikkei_225 <- nikkei_225 %>%
  select(Variable, everything())
nikkei_225 = subset(nikkei_225, nikkei_225$Relevant_Date <= req_date)
nikkei_225 <- tail(nikkei_225, 1)
rownames(nikkei_225) <- NULL
nikkei_225=nikkei_225[,c("Variable","Units")]

## Singapore
singapore = read_query(1525128,2)
singapore=singapore[,c("Relevant_Date","growth")]
names(singapore)[1]<-"Relevant_Date"
names(singapore)[2]<-"Units"
singapore$Variable = 'Singapore'
singapore <- singapore %>%
  select(Variable, everything())
singapore = subset(singapore, singapore$Relevant_Date <= req_date)
singapore <- tail(singapore, 1)
rownames(singapore) <- NULL
singapore=singapore[,c("Variable","Units")]

## Dow Jones
dow_jones = read_query(1525124,2)
dow_jones=dow_jones[,c("Relevant_Date","growth")]
names(dow_jones)[1]<-"Relevant_Date"
names(dow_jones)[2]<-"Units"
dow_jones$Variable = 'Dow Jones'
dow_jones <- dow_jones %>%
  select(Variable, everything())
dow_jones = subset(dow_jones, dow_jones$Relevant_Date <= req_date)
dow_jones <- tail(dow_jones, 1)
rownames(dow_jones) <- NULL
dow_jones=dow_jones[,c("Variable","Units")]

## CAC40
cac_40 = read_query(1525120,2)
cac_40=cac_40[,c("Relevant_Date","growth")]
names(cac_40)[1]<-"Relevant_Date"
names(cac_40)[2]<-"Units"
cac_40$Variable = 'CAC40'
cac_40 <- cac_40 %>%
  select(Variable, everything())
cac_40 = subset(cac_40, cac_40$Relevant_Date <= req_date)
cac_40 <- tail(cac_40, 1)
rownames(cac_40) <- NULL
cac_40=cac_40[,c("Variable","Units")]

## S&P 500
s_p_500 = read_query(1525127,2)
s_p_500=s_p_500[,c("Relevant_Date","growth")]
names(s_p_500)[1]<-"Relevant_Date"
names(s_p_500)[2]<-"Units"
s_p_500$Variable = 'S&P 500'
s_p_500 <- s_p_500 %>%
  select(Variable, everything())
s_p_500 = subset(s_p_500, s_p_500$Relevant_Date <= req_date)
s_p_500 <- tail(s_p_500, 1)
rownames(s_p_500) <- NULL
s_p_500=s_p_500[,c("Variable","Units")]

## Hang Seng
hang_seng = read_query(1525125,2)
hang_seng=hang_seng[,c("Relevant_Date","growth")]
names(hang_seng)[1]<-"Relevant_Date"
names(hang_seng)[2]<-"Units"
hang_seng$Variable = 'Hang Seng'
hang_seng <- hang_seng %>%
  select(Variable, everything())
hang_seng = subset(hang_seng, hang_seng$Relevant_Date <= req_date)
hang_seng <- tail(hang_seng, 1)
rownames(hang_seng) <- NULL
hang_seng=hang_seng[,c("Variable","Units")]

## KOSPI
kospi = read_query(1525126,2)
kospi=kospi[,c("Relevant_Date","growth")]
names(kospi)[1]<-"Relevant_Date"
names(kospi)[2]<-"Units"
kospi$Variable = 'KOSPI'
kospi <- kospi %>%
  select(Variable, everything())
kospi = subset(kospi, kospi$Relevant_Date <= req_date)
kospi <- tail(kospi, 1)
rownames(kospi) <- NULL
kospi=kospi[,c("Variable","Units")]



equity_df = rbind(bse_sensex,nse_nifty_50,ftse_100,nikkei_225,singapore,dow_jones,cac_40,s_p_500,hang_seng,kospi)
equity_df <- equity_df[order(equity_df$Units, decreasing = TRUE),]
equity_df$Units <- roundexcel(equity_df$Units ,digit=1)
# bg = list(col_D9D9D9 = head(equity_df,2)$Variable)

equity_df_2=subset(equity_df,(Variable=="BSE Sensex" | Variable=="NSE Nifty 50"))
bg = list(col_D9D9D9 = head(equity_df_2)$Variable)


equity_markets_34 = economic_indicator_table(equity_df, background_vals = bg,
                                             rounder_exeptions = equity_df$Variable,
                                             caption =paste0("Returns as on ",
                                                             format(req_date,'%B %d, %Y')," (% yoy)",
                                                             sep = ''),line_space =1)

equity_markets_34 <- merge_at(equity_markets_34, i = 2, part = 'header')
equity_markets_34<- border_remove(equity_markets_34)
big_border = fp_border(color="black", width = 1)
equity_markets_34<- hline_bottom(equity_markets_34, part="body", border = big_border )
#equity_markets_34<- hline_top(equity_markets_34, part="body", border = big_border )

equity_markets_34



## ----eval=FALSE, include=FALSE--------
## # Zomato
## zomato = read_query(724563,1, frequency = 'daily')
## zomato_cap = read_query(724554,1,frequency = 'daily')
## zomato = growth_and_market_cap_calc(zomato,"Zomato")
##
## # Policybazaar
## policybazaar = read_query(724568,1, frequency = 'daily')
## policybazaar_cap = read_query(724560,1,frequency = 'daily')
## policybazaar = growth_and_market_cap_calc(policybazaar,"Policybazaar")
##
## # RateGain
## rategain = read_query(724570,1, frequency = 'daily')
## rategain_cap = read_query(724564,1,frequency = 'daily')
## rategain = growth_and_market_cap_calc(rategain,"RateGain")
##
## # CarTrade
## cartrade = read_query(724565,1, frequency = 'daily')
## cartrade_cap = read_query(724556,1,frequency = 'daily')
## cartrade = growth_and_market_cap_calc(cartrade,"CarTrade")
##
## # Map My India
## map_my_india = read_query(724571,1, frequency = 'daily')
## map_my_india_cap = read_query(724566,1,frequency = 'daily')
## map_my_india = growth_and_market_cap_calc(map_my_india,"Map My India")
##
## # Nykaa
## nykaa = read_query(724567,1, frequency = 'daily')
## nykaa_cap = read_query(724558,1,frequency = 'daily')
## nykaa = growth_and_market_cap_calc(nykaa,"Nykaa")
##
## # PayTM
## paytm = read_query(724569,1, frequency = 'daily')
## paytm_cap = read_query(724562,1,frequency = 'daily')
## paytm = growth_and_market_cap_calc(paytm,"PayTM")
##
##
## new_age_df = rbind(zomato,policybazaar,rategain,cartrade,map_my_india,nykaa,paytm)
## new_age_df <- new_age_df[order(new_age_df$One_Week, decreasing = TRUE),]
## # new_age_df=new_age_df[,c(1,4,5,6,7,8)]
##
## returns_new_age_35 = economic_indicator_table(new_age_df,rounder_exeptions = new_age_df$Variable,
##                                               var_col_width = 1,other_col_width = 0.8, font_size = 10,
##                                               replace_null = '-',set_header_bg = 'gray',line_space =2,
##                                               make_col_bold=c(1))
##
##
## returns_new_age_35<- set_header_labels(returns_new_age_35, One_Week = "1-Week", One_Month = "1-Month",
##                                        Three_Month = "3-Month", Six_Month = "6-Month", Twelve_Month = "12-Month")
##
##
## returns_new_age_35 <- add_header_row(returns_new_age_35, values = c(" ","Returns (%)"),colwidths = c(1,5))
## returns_new_age_35 <- align(returns_new_age_35, i = 1, align ="center", part = "header")
## returns_new_age_35 <- bg(returns_new_age_35, i =1, bg = 'white', part = "header")
## returns_new_age_35<- border_remove(returns_new_age_35)
## big_border = fp_border(color="black", width = 1)
## returns_new_age_35<- hline_bottom(returns_new_age_35, part="body", border = big_border )
## returns_new_age_35<- hline_top(returns_new_age_35, part="body", border = big_border )
## returns_new_age_35


## -------------------------------------
##Embassy_REIT
embassy = read_query(724555,13, frequency = 'daily')
names(embassy)[2]<-"Value"
embassy = growth_and_market_cap_calc(embassy,"Embassy REIT")

## Brookfield REIT
brookfield = read_query(724559,13, frequency = 'daily')
names(brookfield)[2]<-"Value"
brookfield = growth_and_market_cap_calc(brookfield,"Brookfield REIT")

## Mindspace REIT
mindspace = read_query(724557,13, frequency = 'daily')
names(mindspace)[2]<-"Value"
mindspace = growth_and_market_cap_calc(mindspace,"Mindspace REIT")

## IRB InvIT
irb_invit = read_query(724552,13, frequency = 'daily')
names(irb_invit)[2]<-"Value"
irb_invit = growth_and_market_cap_calc(irb_invit,"IRB InvIT")

## India Grid
india_grid = read_query(724553,13, frequency = 'daily')
names(india_grid)[2]<-"Value"
india_grid = growth_and_market_cap_calc(india_grid,"India Grid")


#NHIT InvIT
nhit_invit = read_query(1684508,13, frequency = 'daily')
names(nhit_invit)[2]<-"Value"
nhit_invit = growth_and_market_cap_calc(nhit_invit,"NHIT InvIT")

#SHREM InvIT
# shrem_invit = read_query(1684506,13, frequency = 'daily')
# names(shrem_invit)[2]<-"Value"
# shrem_invit = growth_and_market_cap_calc(shrem_invit,"SHREM InvIT")

## PGCIL InvIT
pgcil_invit = read_query(724561,13, frequency = 'daily')
names(pgcil_invit)[2]<-"Value"
pgcil_invit = growth_and_market_cap_calc(pgcil_invit,"PGCIL InvIT")


reit_df = rbind(embassy,brookfield,mindspace)
invit_df = rbind(irb_invit,india_grid,pgcil_invit)
reit_df <- reit_df[order(reit_df$One_Week, decreasing = TRUE),]
invit_df <- invit_df[order(invit_df$One_Week, decreasing = TRUE),]
reit_invit_df <- rbind(reit_df,invit_df)
border = tail(reit_df$Variable,n = 1)

reit_returns_39 = economic_indicator_table(reit_invit_df,rounder_exeptions = reit_invit_df$Variable,
                                           caption = "Returns (%)",hlines = border,line_space =2.5,make_col_bold=c(1))

reit_returns_39 <- set_header_labels(reit_returns_39, One_Week = "1W", One_Month = "1M",Three_Month = "3M",
                                     Six_Month = "6M", Twelve_Month = "12M")
reit_returns_39


## ----eval=FALSE, include=FALSE--------
## ## Bitcoin
## bitcoin = dbGetQuery(clk,"Select Relevant_Date, Close_USD as Value from YAHOO_FINANCE_BITCOIN_TRADE_DAILY_DATA order by Relevant_Date")
## bitcoin = growth_and_market_cap_calc(bitcoin,"Bitcoin")
##
## ## Ethereum
## ethereum = dbGetQuery(clk,"Select Relevant_Date, Close_USD as Value from YAHOO_FINANCE_ETHERIUM_TRADE_DAILY_DATA order by Relevant_Date")
## ethereum = growth_and_market_cap_calc(ethereum,"Ethereum")
##
## crypto_df = rbind(bitcoin,ethereum)
## crypto_df <- crypto_df[order(crypto_df$One_Week, decreasing = TRUE),]
##
##
## crypto_returns_40 = economic_indicator_table(crypto_df,rounder_exeptions = crypto_df$Variable, caption = "Returns (%)",line_space =5,make_col_bold=c(1))
##
## crypto_returns_40 <- set_header_labels(crypto_returns_40, One_Week = "1W", One_Month = "1M",Three_Month = "3M",
##                                        Six_Month = "6M", Twelve_Month = "12M")
## crypto_returns_40
## dbDisconnect(clk)
## dbDisconnect(pg)


## ----include=FALSE--------------------
font_paths()
t=font_files() %>%tibble()
font_add(family='Calibri_Regular',regular ='calibril.ttf')


## -------------------------------------
common_theme=function(x_angle=0,leg_plc="top",legend_key_sp_y=0.05,
                      Position='center'){

  theme_bw()+
  theme (legend.position =leg_plc,
         legend.direction="horizontal",
         legend.justification=Position,
         legend.title = element_blank(),
         legend.title.align = 0,
         legend.key.size = unit(legend_key_size, "cm"),    #Length of key
         legend.key.width= unit(legend_key_width, 'cm'),
         legend.key.height = NULL,                         # key height (unit)
         legend.spacing.x = unit(0.05, 'cm'),
         legend.spacing.y = unit(legend_key_sp_y, 'cm'),
         legend.box = NULL,
         legend.box.margin= margin(0, 0, 0, 0, "cm"),
         legend.box.spacing=unit(0.20, 'cm'),
         legend.margin = margin(0, 0, 0, 0, "cm"),
         legend.text = element_text(size =text_size,
                                     color = "black",
                                     family="Calibri_Regular",
                                     margin = margin(r =key_spacing, unit="cm")))+

  theme(axis.title.x=element_blank(),
        axis.title.y = element_blank())+

  theme(axis.text.x=element_text(angle =x_angle,
                                 color = "black",
                                 size =text_size,
                                 family="Calibri_Regular",
                                 margin = margin(t = .25, unit = "cm")),

        axis.text.y.left=element_text(angle =0,
                                      color = "black",
                                      size =text_size,
                                      family="Calibri_Regular"),

        axis.text.y.right=element_text(angle =0,
                                       color = "black",
                                       size =text_size,
                                       family="Calibri_Regular"))+


theme(axis.ticks.x = element_line(size = 0.25),
      axis.ticks.y.right=element_blank(),
      axis.ticks.y.left=element_blank(),
      axis.ticks.length.y = unit(0.5,"cm"))
}



cutom_theme=function(){
  theme( panel.border=element_blank(),
            panel.grid=element_blank(),
            panel.grid.major=element_line(size = 0.2,linetype =1,colour = "grey"),
            panel.grid.major.x =element_blank(),
            axis.line.x.bottom=element_line(size =0.5,linetype =1,colour = "grey"),
            plot.margin=unit(c(0,0,0.25,0), 'cm'))
}

cutom_theme_1=function(){

  theme(panel.border=element_blank(),
        panel.grid=element_blank(),
        panel.grid.major=element_line(size = 0.2,linetype =1,colour = "grey"),
        panel.grid.major.x =element_blank(),
        axis.line.x.bottom=element_line(size =0.5,linetype =1,colour = "grey"))

}

cutom_theme_map=function(){
    theme_bw()+
    #Legends height is very important for its placing
    theme (legend.position = "none",legend.direction="horizontal",
          legend.justification="left",
          legend.title = element_blank(),
          legend.title.align = 0,
          legend.key.size = unit(legend_key_size, "cm"),       #Length of key
          legend.key.width= unit(legend_key_width, 'cm'),
          legend.key.height = NULL,                            # key height (unit)
          legend.spacing.x = unit(0.10, 'cm'),
          legend.box = NULL,
          legend.box.margin= margin(0, 0, 0, 0, "cm"),
          legend.box.spacing=unit(0.20, 'cm'),
          legend.margin = margin(0, 0, 0, 0, "cm"),
          legend.text = element_text(size =text_size,family="Calibri_Regular",
                                     margin = margin(r =0.10, unit="cm")))+
    theme(axis.title.x=element_blank(),
          axis.title.y = element_blank())+

    theme(axis.text.x =element_blank(),
          axis.text.y = element_blank())+

    theme(axis.ticks.x = element_blank(),
          axis.ticks.y.right=element_blank(),
          axis.ticks.y.left=element_blank())+

   theme(panel.border=element_blank(),
         panel.grid=element_blank(),
         panel.grid.major=element_blank(),
         panel.grid.major.x =element_blank(),
         axis.line.x.bottom=element_blank())
}
cutom_theme_pmi=function(){

  theme(panel.border=element_blank(),
        panel.grid=element_blank(),
        panel.grid.major=element_blank(),
        panel.grid.major.x =element_blank(),
        axis.line.x.bottom=element_line(size =0.5,linetype =1,colour = "grey"))


}

cutom_theme_no_grid_y_axis=function(){
  theme(panel.border=element_blank(),
        panel.grid=element_blank(),
        panel.grid.major=element_blank(),
        panel.grid.major.x =element_blank(),
        axis.line.x.bottom=element_line(size =0.5,linetype =1,colour = "grey"))


}

prev_month<-as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
max_rel_date_replacor=function(df,data_upto){
  df[df$Relevant_Date==max(df$Relevant_Date),"Relevant_Date"]<-data_upto
  return(df)

}
add_proxy_dates=function(df_t,df){
  df2=df_t[df_t$Relevant_Date<min(df$Relevant_Date),]
  if (nrow(df2)> 0){
    df2['Volume']=0
    data <- rbind(df,df2)
  }else{
    data=df
  }
  df3=merge(df_t,data,by="Relevant_Date",all.x = TRUE)
  return(df3)

}
mon_year_df_creator=function(df,keep_col=c("Relevant_Date"),Sum_date=FALSE,
                             match_month='-03-31',ffill=FALSE){

  prev_month=as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
  df=df[df$Relevant_Date<=prev_month,]
  max_date=as.Date(max(df$Relevant_Date))


  col=keep_col[2]

  df1=copy(df)
  df3 <-copy(df)

  df3=df3[df3$Relevant_Date>=max(df$Relevant_Date),]
  df3$Relevant_Date=as.Date(timeLastDayInMonth(df3$Relevant_Date))

  f1=function(x){
     max_date=as.Date(x[1])
     current_month_num=format(max_date,"%m")
     print(max_date)
     print(current_month_num)


  if (as.numeric(current_month_num)>=4){
                current_f_year=format(max_date+years(1),format="%Y")
                current_f_year=paste0(as.character(current_f_year),match_month)
                 print(current_f_year)



  }
  else{

      current_f_year=format(max_date,format="%Y" )
      current_f_year=paste0(as.character(current_f_year),match_month)
      print(current_f_year)}
}



  df["year_mon"]=apply(df,1,f1)
  df_2 <- df %>% mutate_at(c(which(names(df)=="year_mon")), as.Date)
  df_2 <- df_2 %>% mutate_at(c(which(names(df_2)=="Relevant_Date")), as.Date)
  if(ffill==TRUE){
      df_2 <- df_2 %>%
              mutate(
                col = na.locf(col, na.rm = F))
                }

  if (Sum_date==TRUE){
    df_2 <- df_2 %>%
              group_by(year_mon) %>%
              summarize(Sum_Value = sum(Value))
     names(df_2)[c(which(names(df_2)==paste0('Sum_Value')))]=col


  }else{
    df1 <- df1 %>% mutate_at(c(which(names(df1)=="Relevant_Date")), as.Date)
    names(df1)[1]='year_mon'
    print(names(df1))
    df_2=merge(df_2,df1,by="year_mon")
    df_2=df_2[!duplicated(df_2[c("year_mon")]), ]
    names(df_2)[c(which(names(df_2)==paste0(col,'.x')))]=col
  }

  names(df_2)[1]='Relevant_Date'
  df_2=df_2[,c("Relevant_Date",col)]
  df_2=df_2[,keep_col]
  # df_2=rbind(df_2,df3)
  print(max_date)
  if(Sum_date==TRUE){
      df_2[df_2$Relevant_Date==max(df_2$Relevant_Date),"Relevant_Date"]<-max_date
  }else{
    df3=df3[,keep_col]
    df_2=rbind(df_2,df3)
  }
  df_2=df_2[df_2$Relevant_Date<=prev_month,]

  return (df_2)
}



## -------------------------------------
create_df_for_chart=function(df1,df2=as.data.frame(),bar_line=FALSE,Stack_Bar_Line=FALSE,
                             Stack_bar=FALSE,mtlti_line=FALSE,line=FALSE,Show_Older=FALSE,SHOW_FU_DATE=FALSE,
                             Order_Stack=FALSE,top_column1='',hour_Chart=FALSE){

  prev_month=as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
  print(prev_month)
  if (bar_line==TRUE){

      df2=Reduce(function(x, y) merge(x, y, all=TRUE),df2)
      df2=melt(df2,id=c("Relevant_Date"))
      df2=na.omit(df2)
      df2$value_y_right=df2[1:nrow(df2),3]
      df2$category=df2[1:nrow(df2),2]
      df2=df2[,c("Relevant_Date","value_y_right","category")]


      df1$value_y_left=df1[1:nrow(df1),2]
      # df2$value_y_right=df2[1:nrow(df2),2]

      df1$Month <- as.Date(df1$Relevant_Date, format = "%Y-%m-%d")
      df1$Month =as.Date(df1$Month)

      #special_case:For Dual axis line Chart
      #special_case_2:When no of data in left is lesser than right
      if(nrow(df1)!=nrow(df2)){
            df_final=merge(df1, df2, by="Relevant_Date",all=T)
            df_final=do.call(data.frame,
                               lapply(df_final,function(x) replace(x, is.infinite(x), NA)))



      }else{
           # df_final=cbind(df1[,c("Relevant_Date","value_y_left","Month")],
           #                value_y_right=df2$value_y_right)

          df_final=cbind(df1,df2,by="Relevant_Date")

          df_final=df_final[,c("Relevant_Date","value_y_left","Month",
                               'value_y_right','category')]


           df_final=do.call(data.frame,
                              lapply(df_final,function(x) replace(x, is.infinite(x), NA)))
           df_final=na.omit(df_final)
           }
  }else if(Stack_Bar_Line==TRUE){

    df1$Relevant_Date=as.Date(timeLastDayInMonth(df1$Relevant_Date))
    df2$Relevant_Date=as.Date(timeLastDayInMonth(df2$Relevant_Date))

    df_final=merge(df1, df2, by="Relevant_Date")
    df_final= reshape2::melt(df_final,id=c("Relevant_Date","growth"))

    df_final$Month <-as.Date(df_final$Relevant_Date)
    df_final=df_final %>%
                rename(value_y_right=growth,
                       category=variable,
                       value_y_left=value)

  }else if(Stack_bar==TRUE){
    df1=Reduce(function(x, y) merge(x, y, all=TRUE),df1)
    if (Order_Stack==TRUE){
        my_chart_col=re_arrange_columns(df1,sort_type ='',
                                          top_col =top_column1)[[2]]
        df1=df1[,re_arrange_columns(df1,
                                  sort_type ='',
                                  top_col =top_column1)[[1]]]
        }
    df1=melt(df1,id=c("Relevant_Date"))
    df1=na.omit(df1)

    df1$Relevant_Date <- as.Date(df1$Relevant_Date)
    df1 <- df1[order(df1$Relevant_Date),]



    df1$Month <- as.Date(df1$Relevant_Date, format = "%Y-%m-%d")
    max_overlap=10


    df1$value_y_left=df1[1:nrow(df1),3]
    df1$category=df1[1:nrow(df1),2]


    df_final=df1[,c("Relevant_Date","value_y_left","Month","category")]


  }else if(mtlti_line==TRUE){
      # df1=data_s
      # hour_Chart=TRUE
      df1=Reduce(function(x, y) merge(x, y, all=TRUE),df1)
      df1=melt(df1,id=c("Relevant_Date"))
      df1=na.omit(df1)

      #For Every multi line chart provide data as a  list of dataframe
      if (hour_Chart==TRUE){

        df1$Month <-as.POSIXct(df1$Relevant_Date,
                               format = "%Y-%m-%d %H:%M:%S")

        df1$Relevant_Date <- as.Date(df1$Relevant_Date)
        df1 <- df1[order(df1$Relevant_Date),]

      }else{
        df1$Relevant_Date <- as.Date(df1$Relevant_Date)
        df1$Month <- as.Date(df1$Relevant_Date, format = "%Y-%m-%d")
      }
      max_overlap=10
      df1$value_y_left=df1[1:nrow(df1),3]
      df1$category=df1[1:nrow(df1),2]

      df_final=df1[,c("Relevant_Date","value_y_left","Month","category")]

  }else if(line==TRUE){
    df1$Relevant_Date <- as.Date(df1$Relevant_Date)
    df1 <- df1[order(df1$Relevant_Date),]
    df1$Month <- as.Date(df1$Relevant_Date,format = "%Y-%m-%d")
    df1$value_y_left=df1[1:nrow(df1),2]
    df_final=cbind(df1[,c("Relevant_Date","value_y_left","Month")])

  }

    if (bar_line==FALSE){
      df_final=do.call(data.frame,
                     lapply(df_final,function(x) replace(x,is.infinite(x), NA)))
      df_final=na.omit(df_final)

    }
    if (Show_Older==FALSE){
          df_final<- df_final[df_final$Relevant_Date>=default_start_date,]
    }


    df_final=df_final[order(df_final$Relevant_Date),]
    if (SHOW_FU_DATE==FALSE){
      df_final=df_final[df_final$Relevant_Date<=prev_month,]
      print(df_final)
    }



    if (Order_Stack==TRUE){
       return(list('df'=df_final,"chart_col"=my_chart_col))

    }else{
      return(list('df'=df_final))
    }



}
get_sub_title=function(df1,df,show_calender_date=FALSE,
                       show_DATE_HEADER=FALSE,exculde_fy=FALSE){

  current_date=max(df$Relevant_Date)
  first_date=min(df1$Relevant_Date)

  current_day=format(df$Relevant_Date,format="%d")
  current_month=format(df$Relevant_Date,format="%b")
  current_month_num=format(df$Relevant_Date,"%m")[1]
  first_mon_mum=format(first_date,"%m")


  if (show_calender_date==TRUE){
        current_f_year=format(current_date,format="%Y")
        first_f_year=format(first_date,format="%Y")
        sub_h=paste0("CY",first_f_year,"-","CY",current_f_year," (",current_month," '",
                  format(df$Relevant_Date,format="%y"),")")

      }else{
        if (as.numeric(current_month_num)>=4){
            current_f_year=format(current_date+years(1),format="%Y" )

        }else{current_f_year=format(current_date,format="%Y")}

        if (as.numeric(first_mon_mum)>=4){
            first_f_year=format(first_date+years(1),format="%Y" )
        }else{first_f_year=format(first_date,format="%Y" )}

        if (show_DATE_HEADER==TRUE){
          sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_day,' ',current_month," '",
                               format(max(df$Relevant_Date),format="%y"),")")

        }else if(exculde_fy==TRUE){
          sub_h=paste0(" (",current_month," '",
                         format(df$Relevant_Date,format="%y"),")")

        }else{
          sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                  format(df$Relevant_Date,format="%y"),")")}


      }
}
generate_data_labels=function(df,col_name,decimal_plc=1,Negative=TRUE){
  df$value=df[1:nrow(df),which(names(df)==col_name)]
  if(Negative==TRUE){

                  v1=c(df$value)
                  neg_indx=c(which(v1<0))
                  v1=format(roundexcel((v1),decimal_plc),nsmall=decimal_plc,big.mark=",")
                  v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),
                                       nsmall=decimal_plc),")")
                  v1[neg_indx]=v3
                  line_lab=v1

  }else{line_lab=format(roundexcel(df$value,decimal_plc),
                                 nsmall=decimal_plc,big.mark=",")}

  return(line_lab)


}


## -------------------------------------
line_bar_chart_niif <- function(data1,data2,sales_heading,
                                x_axis_interval="24 month",round_integer=FALSE,
                                special_case=FALSE,
                                graph_lim=30,data_unit='',
                                WHITE_BACK=FALSE,
                                Position="center",
                                show_older=FALSE,
                                format_date='',x_angle1=0,
                                key_spacing=0.01,DATE_HEADER=FALSE,
                                show_fu_dt=FALSE,
                                led_position='center',bar_thick=8){

  tryCatch({
    my_chart_col=c("GOLDEN ROD 1","GRAY 48")
    my_legends_col=c("GOLDEN ROD 1","GRAY 48")
    legend_key_width=0.27
    Position=led_position


   #############################Data_formatting##############################
    showtext_auto()
    data_final=create_df_for_chart(data1,data2,bar_line  =TRUE,SHOW_FU_DATE=show_fu_dt)[[1]]

    print(paste("Current working rows:",nrow(data_final)))

    if(DATE_HEADER==TRUE){
                        prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))

    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}


    print(prev_month)

    #This is specially for dual axis line Chart
    data_final=data_final[order(data_final$Relevant_Date),]
    if (show_older==TRUE){
          data_final<- data_final
    }else{
         data_final<- data_final[data_final$Relevant_Date>=default_start_date,]

    }
    data_final$Relevant_Date=as.Date(data_final$Relevant_Date)
    data_final<- data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)

    if (is.numeric(data_final[max(data_final$Relevant_Date),'value_y_right'])==FALSE){

                                      one_mon_gap=as.Date(max(data_final$Relevant_Date)-duration(1,"month"))
                                      data_final<- data_final[data_final$Relevant_Date<=one_mon_gap,]
    }

    if (is.numeric(data_final[max(data_final$Relevant_Date),'value_y_left'])==FALSE){
                                      one_mon_gap=as.Date(max(data_final$Relevant_Date)-duration(1,"month"))
                                      data_final<- data_final[data_final$Relevant_Date<=one_mon_gap,]
    }

    data_ends <- data_final %>% filter(Month == Month[length(Month)])

    print(data_ends)

    #Showing Data labels in chart
    line_lab=generate_data_labels(data_ends,'value_y_right')
    line_lab_l=generate_data_labels(data_ends[1,],'value_y_left')



   #Primary and secondary axis interval related work
    if (max(data_final$value_y_left)==0){max_factor=0

    }else{max_factor=max_pri_y/max(data_final$value_y_left)}

    if (min(data_final$value_y_left)==0){min_factor=0

    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}


    if (round_integer==TRUE){
                  y_max=ceiling(max_factor*max(data_final$value_y_left))
                  y_min=ceiling(min_factor*min(data_final$value_y_left))

    }else{
                  y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
                  y_min=round(min_factor*min(data_final$value_y_left),digits = -1)}

    if (y_max<=0){
                  y_max=max_factor*max(data_final$value_y_left)

    }


    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))

    x_min=min(data_final$Month)
    x_max=max(data_final$Month)
    if (special_case==TRUE){

            LEFT_CHART= geom_line(aes(x=Month,y=value_y_left,
                                  color=paste0(sales_heading)),
                                  size=line_thick,group=1)

            RIGHT_CHART=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                          color=category),
                          stat="identity",
                          size=line_thick,
                          linetype =1,
                          group=1)

            scale_col= scale_color_manual(name = NULL,
                                          values = c("GOLDEN ROD 1","Gray 48"))
      }
      else{
            LEFT_CHART=geom_bar(aes(x=Month,y=(value_y_left),
                                fill=paste0(sales_heading)),
                                stat="identity",
                                position='identity',
                                width = bar_thick)

            RIGHT_CHART=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                      color=category,group=category,linetype =category),
                      stat="identity",
                      size=line_thick
                      )

            scale_col=scale_colour_manual(values=my_leg_col)
                       #
      }

     if(WHITE_BACK==TRUE){
           theme1=cutom_theme_pmi

     }else{theme1=cutom_theme
     }
    normalizer <-max_sec_y/y_max
     if (format_date==''){
      df_f="%b-%y"
    }else{
      df_f=format_date
    }



   ###################################HEADER_SUB_HEADER##################
    sub_h=get_sub_title(data_final,data_ends,show_DATE_HEADER =DATE_HEADER )
   #############################Graph#######################################

  line=ggplot(data=data_final)+
      LEFT_CHART+
      geom_text_repel(aes(x=Month,y=value_y_left,
                      label=line_lab_l),data=data_ends[1,],
                      direction="y",
                      max.overlaps=max_overlap,
                      font="bold",
                      min.segment.length = Inf,
                      nudge_x =nug_x,
                      na.rm=TRUE,hjust=h_just,vjust=v_just,
                      size =chart_label,family="Calibri")+

      RIGHT_CHART+

      geom_text_repel(aes(x=Month,y=(value_y_right)/normalizer,
                      label=line_lab,sep=","),
                      data=data_ends,
                      direction="y",
                      max.overlaps=max_overlap,
                      font="bold",
                      nudge_x =nug_x,
                      min.segment.length = Inf,
                      na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                      size =chart_label,family="Calibri")+

      scale_fill_manual(name = NULL, values = c("GOLDEN ROD 1")) +
      scale_col+
      scale_linetype_manual(values=my_line_type)+
      scale_y_continuous(expand = expansion(mult=c(0,0.04)),
                         breaks=pretty_breaks(n=num_brek),
                         labels =number_format(big.mark = ",",
                                               style_positive = c("none"),
                                               style_negative = c("parens")),

                         sec.axis =sec_axis(~.*normalizer,
                                           breaks=pretty_breaks(n=num_brek),
                                           labels=number_format(big.mark = ",",
                                                                style_positive = c("none"),
                                                                style_negative = c("parens")
                                                                 )))+
      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format(df_f),
                   breaks =seq.Date(min(data_final$Month),
                                    max(data_final$Month),
                                    by=x_axis_interval),
                   expand =c(0,0))+
      #
      # guides(color =guide_legend(order =1),
      #        fill =guide_legend(order =2))+

      # guides(colour=guide_legend(ncol=2,nrow=1,byrow=TRUE,order =1),
      #        fill =guide_legend(order =2,reverse=FALSE))+

      guides(colour=guide_legend(ncol=3,nrow=1,byrow=TRUE),
             fill =guide_legend(order =1,reverse=FALSE))+

      common_theme(x_angle=x_angle1,Position = led_position)+
      theme1()


   ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))
  },
  error =function(e){
    log_generated_error(e)
  }
  )



}

#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.



## -------------------------------------
stacked_bar_line_chart_niif=function(data1,data2,growth_heading,
                       x_axis_interval="24 month",
                       data_unit='',graph_lim=30,round_integer=FALSE,
                       Exception=FALSE,
                       SIDE_BAR=FALSE,negative=FALSE,GST_SPECIAL=FALSE,
                       DUAL_AXIS=TRUE,ptrn_den=0.2,ptrn_spc=0.01,
                       key_spacing=0.10,JNPT=FALSE,
                       legends_break=FALSE,legends_reverse=FALSE,
                       YTD=FALSE,show_shaded=FALSE,order_stack=FALSE,
                       top_column='',line_type=1,
                       ptrn_size=0.5,
                       my_pattern=c('stripe'),
                       led_position='center',legend_placing="top",
                       legend_ver_sp=0.05,add_std_col=FALSE,
                       format_date='',custom_y_min=FALSE,
                       bar_thick=8,expand_x=c(0,0),single_bar_width=0.2){

  tryCatch({
    showtext_auto()
    Position=led_position
  #NEW_MODIFIED
    data2=data2[!duplicated(data2[c("Relevant_Date")]), ]

    ##########################DATA_PROCESSING###########################
    if (order_stack==TRUE){
       if (add_std_col==TRUE){
         my_chart_col=re_arrange_columns(data1,sort_type ='',
                                          top_col =top_column)[[2]]
         my_legends_col=my_chart_col

       }
      data1=data1[,re_arrange_columns(data1,sort_type ='',
                                              top_col =top_column)[[1]]]
    }

    data_final=create_df_for_chart(data1,data2,Stack_Bar_Line=TRUE,
                                   Order_Stack = order_stack)[[1]]



    print(paste("Current working rows:",nrow(data_final)))

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)


    max_factor=max_pri_y/max(data_final$value_y_left)
    min_factor=min_pri_y/abs(min(data_final$value_y_left))
    # if (min(data_final$value_y_left)==0){
    #                                     min_factor=0
    # }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}


    if (Exception==TRUE) {
      y_max = max_factor * max(data_final$value_y_left)
      y_min = min_factor * min(data_final$value_y_left)

    }else if (round_integer == TRUE) {
        y_max = ceiling(max_factor * max(data_final$value_y_left))


        if (custom_y_min == TRUE) {
          print('-------------------------------------')
          y_min =min_pri_y
        }else{
          y_min = ceiling(min_factor * min(data_final$value_y_left))
        }

    }else{
      y_max = round(max_factor * max(data_final$value_y_left), digits = -1)
      y_min = round(min_factor * min(data_final$value_y_left), digits = -1)
    }

    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))

    normalizer <-max_sec_y/y_max
    print( normalizer)
    max_date_actual=as.Date(max(data_final$Month))
    if (YTD==TRUE){
      data_final['Month']=apply(data_final,1,f1)
      data_final$Month <-as.Date(data_final$Month)
      data_final_org=data_final

    }else{
      data_final_org=data_final
    }
    if (show_shaded==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
      data_final1=data_final[data_final$Relevant_Date<max(data_ends$Relevant_Date),]
      data_ends_line=data_final[data_final$Relevant_Date>=max(data_final1$Relevant_Date),]
      data_final=data_final1
    }else{
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
    }


    print(data_ends)
    if (legends_break==TRUE){
      if (legends_reverse==TRUE){
         guides_type=guides(color =guide_legend(order =1,reverse=TRUE),
                            fill =guide_legend(nrow=n_row,byrow=TRUE,order =2))

      }else{
        guides_type=guides(fill =guide_legend(nrow=n_row,byrow=TRUE,
                                              reverse=TRUE,order =1))

      }
    }else{
      guides_type=guides(color =guide_legend(order =2),
                          fill =guide_legend(order =1,reverse=TRUE))

    }

    if (SIDE_BAR == TRUE) {
      bar_position = "dodge2"
      # bar_position = position_dodge(width = single_bar_width,preserve="total")
      # bar_position=position_dodge(0.5)
      bar_position=position_dodge2(width=single_bar_width,preserve="single",
                                   padding = 0)


    } else{bar_position = "stack"
    }

    if (negative==TRUE){
                        v1=c(data_ends$value_y_left)
                        neg_indx=c(which(v1<0))
                        v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
                        v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
                        v1[neg_indx]=v3
                        line_lab_1=v1

    }else{line_lab_1=format(roundexcel(data_ends$value_y_left,1),nsmall=1, big.mark=",")}
    print(line_lab_1)

    if (growth_heading==""){
      line_1=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                    color=paste0(growth_heading)),show.legend = FALSE,
                    size=line_thick,linetype =1,group=1)
      shaded=geom_line(data=data_ends,
                       aes(x=Month,y=(value_y_right/normalizer),
                          color=paste0(growth_heading)),
                          show.legend = FALSE,
                          size=line_thick,linetype =0,group=1)

      line_2=geom_line(data=data_final,
                       aes(x=Month,y=(value_y_right/normalizer),
                          color=paste0(growth_heading)),
                          show.legend = FALSE,
                          size=line_thick,linetype =0,group=1)




    }else {
      if (show_shaded==FALSE){

        line_1=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                      color=paste0(growth_heading)),
                      size=line_thick,linetype =line_type,group=1)


        shaded=geom_line(data=data_ends,
                       aes(x=Month,y=(value_y_right/normalizer),
                          color=paste0(growth_heading)),
                          show.legend = FALSE,
                          size=line_thick,linetype =0,group=1)

        line_2=geom_line(data=data_final,
                       aes(x=Month,y=(value_y_right/normalizer),
                          color=paste0(growth_heading)),
                          show.legend = FALSE,
                          size=line_thick,linetype =0,group=1)




      }else{
         line_1=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                          color=paste0(growth_heading)),
                          size=line_thick,linetype =1,group=1)


          line_2=geom_line(data=data_ends_line,
                       aes(x=Month,y=(value_y_right/normalizer),
                          color=paste0(growth_heading)),
                          size=line_thick,linetype =2,group=1)
          shaded=geom_col_pattern(data=data_ends,
                aes(x=Month,y=value_y_left,
                       pattern_type=category,
                       pattern_fill = category,
                  pattern_filename = category),

                width = bar_thick,
                show.legend = FALSE,
                position = bar_position,

                pattern         = my_pattern,
                pattern_type    = 'none',
                fill            = my_chart_col,
                colour          ='white',

                pattern_scale   = 2,
                pattern_filter  = 'line',
                pattern_gravity = 'east',
                pattern_size=ptrn_size,
                pattern_colour='white',
                pattern_density=ptrn_den,
                pattern_spacing=ptrn_spc,
                pattern_angle=45,
                pattern_frequency=1,
                pattern_key_scale_factor = 1)

      # scale_pattern_filename_discrete(choices= c("stripe", "wave"))+
      # scale_pattern_fill_manual(values=c("white"))+
      # scale_linetype_manual(values=c("solid",'dashed'))+
      # scale_colour_manual(values=c("GRAY 48"),
      #                     guide = guide_legend(override.aes = list(linetype = c('dashed'))))

      }

    }

    if (GST_SPECIAL==TRUE){

           data_ends <- data_final %>% filter(Month == Month[length(Month)])
           data_ends$value_y_left=sum(data_ends$value_y_left)
           data_ends=data_ends[1,1:ncol(data_ends)]

           line_lab_1=format(roundexcel(data_ends$value_y_left,1),nsmall=1,big.mark=",")

           print(line_lab_1)
           text_label_right=geom_text_repel(mapping=aes(x=Month,y=(value_y_left),
                                           #Here Fill is extremly_important
                                            label=line_lab_1),data=data_ends,
                                            stat = "identity",
                                            position =bar_position,
                                            direction="y",
                                            font="bold",
                                            # force=force_bar,
                                            # point.padding=unit(pad_b,'lines'),
                                            min.segment.length = Inf,
                                            na.rm=TRUE,hjust=h_just,vjust=v_just,
                                            size =chart_label,family="Calibri")


      }else{text_label_right=geom_text_repel(mapping=aes(x=Month,y=(value_y_left),fill=category,

                                            #Here Fill is extremly_important
                                            label=line_lab_1),data=data_ends,
                                            stat = "identity",
                                            position =bar_position,
                                            # max_overlaps =max_overlap,
                                            direction="y",
                                            font="bold",
                                            # force=force_bar,
                                            # point.padding=unit(pad_b,'lines'),
                                            min.segment.length = Inf,
                                            na.rm=TRUE,hjust=h_just,vjust=v_just,
                                            size =chart_label,family="Calibri")


      }
      if (JNPT==TRUE){
            data_ends <- data_final %>% filter(Month == Month[length(Month)])
            # idx2=c(which(data_ends$category=="Ports cargo traffic (LHS, mn tonnes)"))
            idx2=1
            data_ends[idx2,'value_y_left']=sum(data_ends$value_y_left)
            print(data_ends)
            line_lab_1=format(roundexcel(data_ends$value_y_left,1),nsmall=1,big.mark=",")


      }

    #Special case means line chart with double axis
    if (DUAL_AXIS==TRUE){
      RIGHT_CHART_scale=scale_y_continuous(expand = expansion(mult=c(0,0.04)),
                                           breaks=pretty_breaks(n=num_brek),
                                           labels =number_format(
                                                                big.mark = ",",
                                                                style_positive = c("none"),
                                                                style_negative = c("parens")),
                                           sec.axis =sec_axis(~.*normalizer,
                                                             breaks=pretty_breaks(n=num_brek),
                                                             labels =number_format(big.mark =",",
                                                                                  style_positive = c("none"),
                                                                                  style_negative = c("parens")
                                                                                   )))

      }else{
        RIGHT_CHART_scale=scale_y_continuous(expand=expansion(mult=c(0,0.04)),
                                           breaks=pretty_breaks(n=num_brek),
                                           labels =number_format(big.mark =",",
                                                                style_positive = c("none"),
                                                                style_negative = c("parens")))

      }



    if (data_ends$value_y_right[1]<0){

         line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_right[1]),1),nsmall=1,big.mark=","),")")


    }else{line_lab=format(roundexcel(abs(data_ends$value_y_right[1]),1),nsmall=1,big.mark=",")}

    if (format_date==''){
      df_f="%b-%y"
    }else{
      df_f=format_date
    }
    ###################################HEADER_SUB_HEADER##################
    if (YTD==TRUE){
      data_ends[data_ends$Month==max(data_ends$Month),"Month"]<-max_date_actual
    }
    sub_h=get_sub_title(data_final,data_ends)

    ##################################################GRAPH#####################
    stacked_bar=ggplot(data=data_final)+
      geom_bar(aes(x=Month,y=(value_y_left),fill=category),
                        stat="identity",width = bar_thick,
                        position=bar_position) +


      line_1+
      RIGHT_CHART_scale+
      scale_fill_manual(values=my_chart_col)+

      line_2+
      shaded+
      text_label_right+
      geom_text_repel(aes(x=Month,y=(value_y_right)/normalizer,
               label=line_lab),
               data=data_ends[1,],
               direction="y",           #All changes will be effect on verticle axis.
               min.segment.length = Inf,#Reomes line
               nudge_x =nug_x,
               font="bold",
               na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
               size =chart_label,family="Calibri")+


      coord_cartesian(ylim =c(y_min,y_max))+

      scale_pattern_filename_discrete(choices= c("stripe", "wave"))+
      scale_pattern_fill_manual(values=c('white','white','white'))+
      # scale_pattern_fill_manual(values=my_chart_col)+


      # scale_pattern_colour_manual(values=my_chart_col)+
      # scale_linetype_manual(values=c("solid",'dashed'))+
      # scale_colour_manual(values=c("GRAY 48"),
      #                     guide = guide_legend(override.aes = list(linetype = c('dashed'))))+


      scale_colour_manual(values=c("GRAY 48"),
                          guide = guide_legend(override.aes = list(linetype = c("solid"))))+

      scale_linetype_manual(values=c("solid"))+




      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format(df_f),
                   breaks =seq.Date(min(data_final_org$Month),
                                    max(data_final_org$Month),
                                    by=x_axis_interval),
                   expand =expand_x)+

      #Fill--->for bar chart
      #Order=1 means it will place that type at top
      #color--->line Chart
      guides_type+
      # guides(fill =guide_legend(nrow=n_row,byrow=TRUE,order =2))+

      # guides(color =guide_legend(order =2),
      #         fill =guide_legend(order =1))+

      # guides(fill=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE,order=1))+
      # guides(color =guide_legend(ncol=n_col1,nrow=n_col1,byrow=TRUE,order=1))+

      common_theme(leg_plc=legend_placing,legend_key_sp_y=legend_ver_sp,
                   Position = led_position)+
      cutom_theme()
    ########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))

  },
  error =function(e){
    log_generated_error(e)
  }

  )

}

# NOTE:-if u see bar are surround with another colour that means u have done something wrong while arranging colors.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.
#Scale fill manual represents what will be the color of your graph.


## -------------------------------------
stacked_bar_chart_niif <-  function(data1,x_axis_interval="24 month",
                    data_unit="",
                    graph_lim=30,negative=FALSE,show_all=FALSE,show_older=FALSE,
                    SIDE_BAR=FALSE,DATE_HEADER=FALSE,
                    surplus=FALSE,round_integer=FALSE,YTD=FALSE,format_date='',
                    order_stack=FALSE,top_column='',single_bar_width=4,
                    led_position='center',bar_thick=8,ptrn_size=0.5,
                    add_std_col=FALSE,legend_reverse=FALSE,
                    legends_break=FALSE,show_grid=TRUE,
                    legend_placing="top",legend_ver_sp=0.05,
                    show_fu_date=FALSE,
                    show_shaded=FALSE,ptrn_den=0.2,ptrn_spc=0.01){
  tryCatch({

    ##########################DATA_PROCESSING###########################
    showtext_auto()
    output=create_df_for_chart(data1,Stack_bar=TRUE,
                                   Order_Stack=order_stack,
                                   Show_Older=show_older,
                                   top_column1=top_column,
                                   SHOW_FU_DATE=show_fu_date)
    if (order_stack==TRUE){
      if (add_std_col==TRUE){
         my_chart_col=output[[2]]
         my_legends_col=my_chart_col

      }


    }
    data_final=output[[1]]


    # data_final$value_y_left=as.numeric(data_final$value_y_left)

    if(DATE_HEADER==TRUE){
                         prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))

    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}

    if (show_fu_date==FALSE){
      print(prev_month)
      data_final<- data_final[data_final$Relevant_Date<=prev_month,]
      print(data_final)
    }


    x_max=max(data_final$Month)
    x_min=min(data_final$Month)

    if (SIDE_BAR==TRUE){
        # bar_position="dodge2"
        # bar_position=position_dodge(width = bar_thick)
        bar_position = position_dodge(width = bar_thick,preserve="total")


    }else{
      bar_position ="stack"
      # bar_position =position_stack()
     }

    #Primary and secondary axis interval related work
    max_factor=max_pri_y/max(data_final$value_y_left)

    if (min(data_final$value_y_left)==0){
                                        min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}

    print(paste("Min factor:",min_factor))

    if (round_integer==TRUE){
                  y_max=ceiling(max_factor*max(data_final$value_y_left))
                  y_min=ceiling(min_factor*min(data_final$value_y_left))

    }else{
                  y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
                  y_min=round(min_factor*min(data_final$value_y_left),digits = -1)}

    if (y_max<=0){
                  y_max=max_factor*max(data_final$value_y_left)

    }


#     y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
#     y_min=round(min_factor*min(data_final$value_y_left),digits = -1)
#
#     if (y_max<=0){
#                   y_max=max_factor*max(data_final$value_y_left)
# }
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    max_date_actual=as.Date(max(data_final$Month))
    if (YTD==TRUE)
    {
      data_final['Month']=apply(data_final,1,f1)
      data_final$Month <-as.Date(data_final$Month)
      print(data_final)
      print(names(data_final))
      data_final_org=data_final
    }else{
      data_final_org=data_final
    }


    if (show_all==TRUE){
      data_ends <- data_final
    }else{
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
    }

    if (surplus==TRUE){
                    if  (negative==TRUE){
                                        v1=c(data_ends$value_y_left)
                                        neg_indx=c(which(v1<0))
                                        v1=format(roundexcel((v1),2),nsmall=2,big.mark=",")
                                        v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=2),")")
                                        v1[neg_indx]=v3
                                        label_1=v1

                    }else{label_1=format(roundexcel((data_ends$value_y_left),2),nsmall=2,big.mark=",") }

    }else{
          if  (negative==TRUE){
                              v1=c(data_ends$value_y_left)
                              neg_indx=c(which(v1<0))
                              v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
                              v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
                              v1[neg_indx]=v3
                              label_1=v1

          }else{label_1=format(roundexcel((data_ends$value_y_left),1),nsmall=1, big.mark=",")
                # label_1=generate_data_labels(data_ends,'value_y_left')
                }
    }

    print(data_ends)
    print(label_1)
    if (format_date==''){
      df_f="%b-%y"
    }else{
      df_f=format_date
    }
    if (show_shaded==TRUE){
      shaded=geom_col_pattern(data=data_ends,
                aes(x=Month,y=value_y_left,
                   pattern_type=category,
                   pattern_fill = category,
                   pattern_filename = category),

                width = bar_thick,
                show.legend = FALSE,
                position = bar_position,

                pattern         = 'stripe',
                pattern_type    = 'none',
                fill            = my_chart_col,
                colour          ='white',
                pattern_scale   = 2,
                pattern_size=ptrn_size,
                pattern_filter  = 'line',
                pattern_gravity = 'east',
                pattern_colour='white',
                pattern_density=ptrn_den,
                pattern_spacing=ptrn_spc,
                pattern_angle=45,
                pattern_frequency=1,
                pattern_key_scale_factor = 1)
    }else{
      shaded=geom_line(data=data_ends,
                       aes(x=Month,y=(value_y_left)),
                          show.legend = FALSE,
                          size=line_thick,linetype =0,group=1)
    }
     print(data_ends)
    if (legends_break==TRUE){
      if (legend_reverse==TRUE){
         guides_type=guides(color =guide_legend(order =1,reverse=TRUE),
                            fill =guide_legend(nrow=n_row,byrow=TRUE,order =2))

      }else{
        guides_type=guides(fill =guide_legend(nrow=n_row,byrow=TRUE,
                                              reverse=TRUE,order =1))

      }
    }else{
      # guides_type=guides(color =guide_legend(order =2),
      #                     fill =guide_legend(order =1,reverse=TRUE))

       guides_type= guides(fill =guide_legend(order =1,reverse=legend_reverse))

    }

     if(show_grid==FALSE){
           theme1=cutom_theme_no_grid_y_axis

     }else{theme1=cutom_theme
     }

     ###################################HEADER_SUB_HEADER##################
    if (YTD==TRUE){
      # data_ends <- data_final %>% filter(Month == Month[length(Month)])
      data_ends[data_ends$Month==max(data_ends$Month),"Month"]<-max_date_actual
    }
    sub_h=get_sub_title(data_final,data_ends,show_DATE_HEADER =DATE_HEADER)



     ######################################################GRAPH###################
    print(data_ends)
    print(label_1)
    stacked_bar=ggplot(data=data_final)+
      geom_bar(aes(x=as.Date(Month),y=(value_y_left),fill=category),
                stat="identity",
                width = bar_thick,
                position=bar_position) +

      geom_text_repel(mapping=aes(x=Month,y=(value_y_left),fill=category,
                      #Here Fill is extremly_important
                      label=label_1),data=data_ends,
                      stat = "identity",
                      position =bar_position,
                      max_overlaps=max_overlap,
                      direction="y",
                      font="bold",
                      min.segment.length = Inf,
                      na.rm=TRUE,hjust=h_just,vjust=v_just,
                      size =chart_label,family="Calibri")+
      scale_fill_manual(values=my_chart_col)+
      scale_colour_manual(values=my_legends_col)+

      shaded+
      scale_y_continuous(expand=expansion(mult = c(0,0.04)),
                        breaks=pretty_breaks(n=num_brek),
                        labels =number_format(big.mark = ",",
                                              style_positive = c("none"),
                                              style_negative = c("parens")))+
      coord_cartesian(ylim =c(y_min,y_max))+
      scale_pattern_filename_discrete(choices= c("stripe", "wave"))+
      # scale_pattern_fill_manual(values=c('white','white','white'))+
      scale_pattern_fill_manual(values=my_chart_col)+

      scale_x_date(limits=as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format(df_f),
                   breaks =seq.Date(min(data_final_org$Month),
                                    max(data_final_org$Month),
                                    by=x_axis_interval),
                   expand =c(0,0))+

      # guides(fill =guide_legend(order =1,reverse=legend_reverse))+

      guides_type+
      # common_theme(Position = led_position)+
      common_theme(leg_plc=legend_placing,legend_key_sp_y=legend_ver_sp,
                   Position = led_position)+

      # cutom_theme()
      theme1()


     ##########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))

  },
  error = function(e){
   log_generated_error(e)

    }
  )
}

#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## -------------------------------------
stacked_bar_chart_niif_exp <-  function(data1,x_axis_interval="24 month",data_unit="",
                                    graph_lim=30,negative=FALSE,show_all=FALSE,show_older=FALSE,
                                    SIDE_BAR=FALSE,DATE_HEADER=FALSE,
                                    surplus=FALSE,round_integer=FALSE,YTD=FALSE,format_date='',
                                    order_stack=FALSE,top_column='',led_position='center',bar_thick=8,add_std_col=FALSE){
  tryCatch({

    ##########################DATA_PROCESSING###########################
    showtext_auto()
    output=create_df_for_chart(data1,Stack_bar=TRUE,
                                   Order_Stack=order_stack,
                                   Show_Older=show_older,
                                   top_column1=top_column)
    if (order_stack==TRUE){
      if (add_std_col==TRUE){
         my_chart_col=output[[2]]
         my_legends_col=my_chart_col

      }


    }
    data_final=output[[1]]


    # data_final$value_y_left=as.numeric(data_final$value_y_left)

    if(DATE_HEADER==TRUE){
                         prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))

    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}


    print(prev_month)
    data_final<- data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)

    if (SIDE_BAR==TRUE){
                        bar_position="dodge2"
                        bar_position=position_dodge(width = bar_thick)

     }else{bar_position ="stack"}

    #Primary and secondary axis interval related work
    max_factor=max_pri_y/max(data_final$value_y_left)

    if (min(data_final$value_y_left)==0){
                                        min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}

    print(paste("Min factor:",min_factor))

    if (round_integer==TRUE){
                  y_max=ceiling(max_factor*max(data_final$value_y_left))
                  y_min=ceiling(min_factor*min(data_final$value_y_left))

    }else{
                  y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
                  y_min=round(min_factor*min(data_final$value_y_left),digits = -1)}

    if (y_max<=0){
                  y_max=max_factor*max(data_final$value_y_left)

    }


#     y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
#     y_min=round(min_factor*min(data_final$value_y_left),digits = -1)
#
#     if (y_max<=0){
#                   y_max=max_factor*max(data_final$value_y_left)
# }
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    max_date_actual=as.Date(max(data_final$Month))
    if (YTD==TRUE)
    {
      data_final['Month']=apply(data_final,1,f1)
      data_final$Month <-as.Date(data_final$Month)
      print(data_final)
      print(names(data_final))
    }
    if (show_shaded==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
      data_final1=data_final[data_final$Relevant_Date<max(data_ends$Relevant_Date),]
      data_ends1=data_final[data_final$Relevant_Date>=max(data_final1$Relevant_Date),]
      data_final=data_final1
    }else{
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
    }


    if (show_all==TRUE){
      data_ends <- data_final
    }else{
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
    }


    if (surplus==TRUE){
                    if  (negative==TRUE){
                                        v1=c(data_ends$value_y_left)
                                        neg_indx=c(which(v1<0))
                                        v1=format(roundexcel((v1),2),nsmall=2,big.mark=",")
                                        v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=2),")")
                                        v1[neg_indx]=v3
                                        label_1=v1

                    }else{label_1=format(roundexcel((data_ends$value_y_left),2),nsmall=2,big.mark=",") }

    }else{
          if  (negative==TRUE){
                              v1=c(data_ends$value_y_left)
                              neg_indx=c(which(v1<0))
                              v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
                              v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
                              v1[neg_indx]=v3
                              label_1=v1

         }else{label_1=format(roundexcel((data_ends$value_y_left),1),nsmall=1, big.mark=",")}
    }
    print(data_ends)
    print(label_1)
    if (format_date==''){
      df_f="%b-%y"
    }else{
      df_f=format_date
    }

     ###################################HEADER_SUB_HEADER##################
    if (YTD==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
      data_ends[data_ends$Month==max(data_ends$Month),"Month"]<-max_date_actual
    }
    sub_h=get_sub_title(data_final,data_ends,show_DATE_HEADER =DATE_HEADER)

    if (YTD==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])

    }

     ######################################################GRAPH###################
    stacked_bar=ggplot(data=data_final)+
      geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
                position=bar_position,
                width = bar_thick) +

      geom_text_repel(mapping=aes(x=Month,y=(value_y_left),fill=category,
                      #Here Fill is extremly_important
                      label=label_1),data=data_ends,
                      stat = "identity",
                      position =bar_position,
                      max_overlaps=max_overlap,
                      direction="y",
                      font="bold",
                      min.segment.length = Inf,
                      na.rm=TRUE,hjust=h_just,vjust=v_just,
                      size =chart_label,family="Calibri")+

      scale_fill_manual(values=my_chart_col)+
      scale_colour_manual(values=my_legends_col)+

      scale_y_continuous(expand=expansion(mult = c(0,0.04)),
                        breaks=pretty_breaks(n=num_brek),
                        labels =number_format(big.mark = ",",
                                              style_positive = c("none"),
                                              style_negative = c("parens")))+
      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x_date(limits=as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format(df_f),
                   breaks =function(x) seq.Date(from = min(x),to = max(x) ,by=x_axis_interval),
                   expand =c(0,0))+

      guides(fill =guide_legend(order =1,reverse=TRUE))+


      common_theme(Position = led_position)+
      cutom_theme()


     ##########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))

  },
  error = function(e){
    log_generated_error(e)
    }
  )
}

#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## -------------------------------------
multi_line_chart_trend_niif <- function(data1,
                                      x_axis_interval="24 month",
                                graph_lim=30,negative=TRUE,
                                PMI_reference=FALSE,BSE_Index=FALSE,
                                legend_key_width=0.27,led_position="center",
                                CPI_reference=FALSE,show_actual_lbls=TRUE,
                                round_integer=FALSE,Repo=FALSE,DATE_HEADER=FALSE,
                                trend='',key_spacing = 0.25){

  tryCatch({

  # Note: Always put key_spacing before max_pri_y order of the legends_key
  ##########################DATA_PROCESSING###########################
    showtext_auto()
    # Position=led_position
    data1=Reduce(function(x, y) merge(x, y, all=TRUE),data1)
    data1=melt(data1,id=c("Relevant_Date"))
    data1=na.omit(data1)

    #For Every multi line chart provide data as a  list of dataframe


    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data1$Month <- as.Date(data1$Relevant_Date, format = "%Y-%m-%d")
    max_overlap=10


    data1$value_y_left=data1[1:nrow(data1),3]
    data1$value_y_left1=data1[1:nrow(data1),3]
    data1$category=data1[1:nrow(data1),2]
    data1$category2=as.character(data1$category)

    # values=c('Post pandemic trend','Pre pandemic trend',v4)
    # keys=c(v1,v2,v3)
    # my_dict <- setNames(values, keys)
    # for (k in keys){
    #   print(k)
    #   for (i in which(data1$category2==k)){
    #       data1[i,8]=my_dict[[which(names(my_dict)==k)]]
    #
    # }
    #
    # }


    data_final=data1[,c("Relevant_Date","value_y_left","Month","category",
                        "category2","value_y_left1")]

    # print(data_final)
    data_final2=data_final[data_final$category==trend,]
    print(data_final2)
    data_final=data_final[data_final$category!=trend,]

    data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))



    data_final=data_final[order(data_final$Relevant_Date),]
    # data_final<- data_final[data_final$Relevant_Date>=default_start_date,]



    # prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
    # print(prev_month)
    # print(data_ends)


    if(DATE_HEADER==TRUE){
                        prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))

    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}

    data_final<- data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)


     x_ref=data_final %>%subset(format(as.Date(Relevant_Date),"%Y")==2016)
     x_ref=min(x_ref$Month)

    #RELATED TO Y AXIS LIMIT
    if (max(data_final$value_y_left)==0){
             max_factor=0
       }else{max_factor=max_pri_y/max(data_final$value_y_left)}

    if (min(data_final$value_y_left)==0){
          min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}


    if (round_integer==TRUE){
           y_max=ceiling(max_factor*max(data_final$value_y_left))
           y_min=ceiling(min_factor*min(data_final$value_y_left))

    }else{
          y_max=max_factor*max(data_final$value_y_left)
          y_min=min_factor*min(data_final$value_y_left)
        }

    if (y_max<=0){
             y_max=max_factor*max(data_final$value_y_left)
    }
    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))

    if (show_actual_lbls==TRUE){
      data_ends <- data_final2 %>% filter(Month == Month[length(Month)])
      data_ends=data_ends[!duplicated(data_ends[c("category")]), ]
      data_ends$value_y_left=roundexcel((data_ends$value_y_left),1)
      print(data_ends)

    }else{
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
      # data_ends <- data_final2 %>% filter(Month == Month[length(Month)])
      print(data_ends)
      data_ends=data_ends[!duplicated(data_ends[c("category")]), ]
      data_ends$value_y_left=roundexcel((data_ends$value_y_left1),1)
      print(data_ends)
    }



    if (any(data_ends$value_y_left)<0){
         line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1, big.mark=","),")")


    }else{
         line_lab=format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=",")
    }
    if (BSE_Index==TRUE){
      text_label=geom_text_repel(aes(x=Month,y=value_y_left,
                                label=""),data=data_ends,
                                max.overlaps=max_overlap,
                                show.legend =FALSE,
                                direction="y",
                                font="bold",
                                min.segment.length = Inf,  #Reomes_line
                                na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                                size =chart_label,family="Calibri")
    }else{
      text_label=geom_text_repel(aes(x=Month,y=value_y_left,
                                label=line_lab),data=data_ends,
                                max.overlaps=max_overlap,
                                direction="y",
                                font="bold",
                                min.segment.length = Inf,  #Reomes_line
                                na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                                size =chart_label,family="Calibri")
    }

    if (PMI_reference==TRUE){

              theme1=cutom_theme_pmi
              reference_line_1=geom_segment(x =x_min, y =50,
                                           xend =x_max, yend =50,
                                           color ="GRAY 32",size=0.25,
                                           show.legend =FALSE,
                                           linetype =1)

               a1=annotate("text",x =x_min,y =2.25,label="", angle=0, size=15, family="Calibri_Regular")
               reference_line_2=geom_hline(yintercept =0,color ="black",linetype =0)
               a2=annotate("text",x =x_ref,y = 0, label="", angle=0, size=14, color="black")


      }else if (CPI_reference==TRUE){
              theme1=cutom_theme
              reference_line_1=geom_segment(x =x_ref, y = 2,
                                           xend =x_max, yend = 2,
                                           color = "black",size=0.75,
                                           show.legend =FALSE,
                                           linetype =2)
               a1=annotate("text",x =x_ref+10,y =3,label="2", angle=0, size=10.5,family="Calibri_Regular")

               reference_line_2=geom_segment(x =x_ref, y =6,
                                           xend =x_max, yend = 6,
                                           color = "black",size=0.75,
                                           show.legend =FALSE,
                                           linetype =2)

               a2=annotate("text",x =x_ref+10,y = 7,label="6",angle=0, size=10.5, family="Calibri_Regular")


      }else{
             theme1=cutom_theme
             reference_line_1=geom_hline(yintercept =0,color ="black",linetype =0)
             a1=annotate("text",x =x_min,y = 0, label="", angle=0, size=14, color="black")
             reference_line_2=geom_hline(yintercept =0,color ="black", linetype =0)
             a2=annotate("text",x =x_ref,y = 0, label="", angle=0, size=14, color="black")

      }



    if (Repo==TRUE){
           if  (negative==TRUE){
                              v1=c(data_ends$value_y_left)
                              neg_indx=c(which(v1<0))
                              v1=format(roundexcel((v1),2),nsmall=2,big.mark=",")
                              v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=2),")")
                              v1[neg_indx]=v3
                              line_lab=v1

           }else{line_lab=format(roundexcel(data_ends$value_y_left,2),nsmall=2,big.mark=",")}

    }else{
           if  (negative==TRUE){
                              v1=c(data_ends$value_y_left)
                              neg_indx=c(which(v1<0))
                              v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
                              v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
                              v1[neg_indx]=v3
                              line_lab=v1

            }else{line_lab=format(roundexcel(data_ends$value_y_left,1),nsmall=1,big.mark=",")}
    }


    print(line_lab)
    ###################################HEADER_SUB_HEADER##################
    sub_h=get_sub_title(data_final,data_ends)

    ########################################################GRAPH#####################

    line=ggplot()+


        geom_line(data=data_final,
                aes(x=Month,y=(value_y_left1),
                    color=category2,group=category,
                    linetype =category2),
                    size=line_thick,
                    show.legend=FALSE,key_glyph =draw_key_smooth)+

        geom_smooth(data=data_final,method=lm,
                    aes(x=Month,y=(value_y_left1),
                        colour=category2,group=category2),
                    fullrange=FALSE, linetype=2,show.legend = TRUE,se=FALSE)+

        geom_line(data=data_final2,
                  aes(x=Month,y=(value_y_left),
                      color=category,group=category,
                      linetype =category),
                      size=line_thick,
                      show.legend=TRUE)+

      geom_text_repel(aes(x=Month,y=value_y_left,
                                      label=line_lab),data=data_ends,
                                      max.overlaps=max_overlap,
                                      direction="y",
                                      font="bold",
                                      min.segment.length = Inf,  #Reomes_line
                                      na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                                      size =chart_label,family="Calibri")+


      scale_colour_manual(values=my_legends_col)+
      scale_linetype_manual(values=my_line_type)+

      scale_y_continuous(expand=expansion(mult=c(0,0.04)),
                     breaks=pretty_breaks(n=num_brek),
                     labels =number_format(big.mark =",",
                                          style_positive = c("none"),
                                          style_negative = c("parens") ))+



      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =seq.Date(min(data_final$Month),
                                    max(data_final$Month),
                                    by=x_axis_interval),
                   expand =c(0,0))+


      reference_line_1+
      a1+
      reference_line_2+
      a2+


      #To exclude that legend that i don't want.
      guides(fill=FALSE)+
      guides(colour=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE))+
      common_theme(Position = led_position)+
      theme1()

    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))



  },
  error = function(e){
    log_generated_error(e)

    }
  )
}


#Scale fill manual represents what will be the color of your graph
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## -------------------------------------
multi_line_chart_niif <- function(data1,x_axis_interval="24 month",
                                  graph_lim=30,negative=TRUE,
                                  PMI_reference=FALSE,
                                  BSE_Index=FALSE,calender_date=FALSE,
                                  legend_key_width=0.27,
                                  led_position="center",
                                  CPI_reference=FALSE,
                                  show_older=FALSE,
                                  Hour_minute=FALSE,
                                  exculde_FY=FALSE,
                                  round_integer=FALSE,Repo=FALSE,
                                  DATE_HEADER=FALSE,format_date='',
                                  x_angle1=0,key_spacing=0.10){

  tryCatch({

  # Note: Always put key_spacing before max_pri_y order of the legends_key
  ##########################DATA_PROCESSING###########################
    showtext_auto()
    data_final=create_df_for_chart(data1,mtlti_line=TRUE,
                                   hour_Chart=Hour_minute,
                                   Show_Older=show_older)[[1]]

    print(data_final)
    if(DATE_HEADER==TRUE){
                  prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))

    }else{
      prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
      }

    data_final<- data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)
    print(x_max)
    print(x_min)


     x_ref=data_final %>%subset(format(as.Date(Relevant_Date),"%Y")==2016)
     x_ref=min(x_ref$Month)

    #RELATED TO Y AXIS LIMIT
    if (max(data_final$value_y_left)==0){
             max_factor=0
       }else{max_factor=max_pri_y/max(data_final$value_y_left)}

    if (min(data_final$value_y_left)==0){
          min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}


    if (round_integer==TRUE){
           y_max=ceiling(max_factor*max(data_final$value_y_left))
           y_min=ceiling(min_factor*min(data_final$value_y_left))

    }else{
          y_max=max_factor*max(data_final$value_y_left)
          y_min=min_factor*min(data_final$value_y_left)
        }

    if (y_max<=0){
             y_max=max_factor*max(data_final$value_y_left)
    }
    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))

    data_ends <- data_final %>% filter(Month == Month[length(Month)])
    data_ends=data_ends[!duplicated(data_ends[c("category")]), ]
    data_ends$value_y_left=roundexcel((data_ends$value_y_left),1)
    print(data_ends)


    if (any(data_ends$value_y_left)<0){
         line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_left),1),
                                    nsmall=1, big.mark=","),")")


    }else{
         line_lab=format(roundexcel(abs(data_ends$value_y_left),1),
                         nsmall=1,big.mark=",")
    }
    if (BSE_Index==TRUE){
      text_label=geom_text_repel(aes(x=Month,y=value_y_left,
                                label=""),data=data_ends,
                                max.overlaps=max_overlap,
                                show.legend =FALSE,
                                direction="y",
                                font="bold",
                                min.segment.length = Inf,  #Reomes_line
                                na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                                size =chart_label,family="Calibri")


    }else{
      text_label=geom_text_repel(aes(x=Month,y=value_y_left,
                                label=line_lab),data=data_ends,
                                max.overlaps=max_overlap,
                                direction="y",
                                font="bold",
                                min.segment.length = Inf,  #Reomes_line
                                na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                                size =chart_label,family="Calibri")
    }


    if (PMI_reference==TRUE){

              theme1=cutom_theme_pmi
              reference_line_1=geom_segment(x =x_min, y =50,
                                           xend =x_max, yend =50,
                                           color ="GRAY 32",size=0.25,
                                           show.legend =FALSE,
                                           linetype =1)

               a1=annotate("text",x =x_min,y =2.25,
                           label="",angle=0,
                           size=15, family="Calibri_Regular")

               reference_line_2=geom_hline(yintercept =0,
                                           color ="black",
                                           linetype =0)

               a2=annotate("text",x =x_ref,y = 0,
                           label="", angle=0,
                           size=14, color="black")


      }else if (CPI_reference==TRUE){
              theme1=cutom_theme
              reference_line_1=geom_segment(x =x_ref, y = 2,
                                           xend =x_max, yend = 2,
                                           color = "black",size=0.75,
                                           show.legend =FALSE,
                                           linetype =2)
               a1=annotate("text",x =x_ref+10,y =3,
                           label="2", angle=0, size=10.5,
                           family="Calibri_Regular")

               reference_line_2=geom_segment(x =x_ref, y =6,
                                           xend =x_max, yend = 6,
                                           color = "black",size=0.75,
                                           show.legend =FALSE,
                                           linetype =2)

               a2=annotate("text",x =x_ref+10,y = 7,
                           label="6",angle=0, size=10.5,
                           family="Calibri_Regular")


      }else{
             theme1=cutom_theme
             reference_line_1=geom_hline(yintercept =0,
                                         color ="black",
                                         linetype =0)
             a1=annotate("text",x =x_min,y = 0,
                         label="", angle=0, size=14, color="black")

             reference_line_2=geom_hline(yintercept =0,
                                         color ="black", linetype =0)
             a2=annotate("text",x =x_ref,y = 0,
                         label="", angle=0, size=14, color="black")

      }

    if (Repo==TRUE){
           if  (negative==TRUE){
                    v1=c(data_ends$value_y_left)
                    neg_indx=c(which(v1<0))
                    v1=format(roundexcel((v1),2),nsmall=2,big.mark=",")
                    v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=2),")")
                    v1[neg_indx]=v3
                    line_lab=v1

           }else{line_lab=format(roundexcel(data_ends$value_y_left,2),
                                 nsmall=2,big.mark=",")}

    }else{
           if  (negative==TRUE){
              v1=c(data_ends$value_y_left)
              neg_indx=c(which(v1<0))
              v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
              v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
              v1[neg_indx]=v3
              line_lab=v1

           }else{line_lab=format(roundexcel(data_ends$value_y_left,1),
                                  nsmall=1,big.mark=",")}
    }


    if (format_date==''){
      df_f="%b-%y"
    }else{
      df_f=format_date
    }

    ###################################HEADER_SUB_HEADER##################
    sub_h=get_sub_title(data_final,data_ends,
                        exculde_fy = exculde_FY,
                        show_DATE_HEADER =DATE_HEADER,
                        show_calender_date=calender_date)



    if(Hour_minute==TRUE){
         scale_x=scale_x_datetime(
           limits=c(as.POSIXct(x_min),as.POSIXct(x_max+graph_lim)),
           date_breaks =x_axis_interval,
           expand = c(0, 0),
           date_labels =df_f)


      }else{
         scale_x=scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                              labels =date_format(df_f),
                              breaks =seq.Date(min(data_final$Month),
                                    max(data_final$Month),
                                    by=x_axis_interval))

      }

    ########################################################GRAPH#####################
    line=ggplot(data=data_final)+
      geom_line(aes(x=Month,y=(value_y_left),
                    color=category,group=category,
                    linetype =category),
                    size=line_thick,
                    show.legend=TRUE,key_glyph =draw_key_smooth)+

      text_label+
      scale_colour_manual(values=my_legends_col)+
      scale_linetype_manual(values=my_line_type)+

      scale_y_continuous(expand=expansion(mult=c(0,0.04)),
                         breaks=pretty_breaks(n=num_brek),
                         labels =number_format(big.mark =",",
                         style_positive = c("none"),
                         style_negative = c("parens")))+



      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x+
      reference_line_1+
      a1+
      reference_line_2+
      a2+

      #To exclude that legend that i don't want.
      guides(fill=FALSE)+
      guides(colour=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE))+
      common_theme(x_angle=x_angle1,Position = led_position)+
      theme1()

    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))



  },
  error = function(e){
    log_generated_error(e)

    }
  )
}

#Scale fill manual represents what will be the color of your graph
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## -------------------------------------
multi_line_chart_rainfall_niif <- function(data1, x_axis_interval="1 month",num_brek=8,key_spacing=0.10,led_position='center'){
    legend_key_width=0.25
    Position=led_position
    ##########################DATA_PROCESSING###########################
    showtext_auto()
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data_f1=data1
    lubridate::year(data1$Relevant_Date)=2023




    # data1$Month <- as.yearmon(data1$Relevant_Date,format="%b %y")
    # data1$Month =as.Date(data1$Month)

    # class(data1$Month)

    data1$value_y_left=data1[1:nrow(data1),3]
    data1$category=data1[1:nrow(data1),2]


    data_final=data1[,c("Relevant_Date","value_y_left","category")]
                     # value_y_right=data2$value_y_right)
    data_final=na.omit(data_final)
    data_final=data_final[order(data_final$Relevant_Date),]
    data_final=data_final[!duplicated(data_final[c("category","Relevant_Date")]), ]
    print(data_final)

    x_max=max(data_final$Relevant_Date)
    x_min=min(data_final$Relevant_Date)

    if (max(data_final$value_y_left)==0){
         max_factor=0
       }else{max_factor=max_pri_y/max(data_final$value_y_left)}

    if (min(data_final$value_y_left)==0){
          min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}

    print(paste("Min factor:",min_factor))

    y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
    y_min=round(min_factor*min(data_final$value_y_left),digits = -1)

    if (y_max<=0){
             y_max=max_factor*max(data_final$value_y_left)
}
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))


    data_ends <- data_final %>% filter(Relevant_Date == Relevant_Date[length(Relevant_Date)])

    if (any(data_ends$value_y_left)<0){
         line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=","),")")

    }else{
        line_lab=format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=",")
    }

    ###################################HEADER_SUB_HEADER##################
    current_date=max(data_f1$Relevant_Date)
    first_date=min(data_f1$Relevant_Date)
    #
    current_month=format(max(data_f1$Relevant_Date),format="%b")
    current_month_num=format(max(data_f1$Relevant_Date),"%m")[1]
    current_day=format(max(data_f1$Relevant_Date),format="%d")

    first_mon_mum=format(first_date,"%m")[1]

    current_f_year=format(current_date,format="%Y" )
    first_f_year=format(first_date,format="%Y" )

    sub_h=paste0("CY",first_f_year,"-","CY",current_f_year," (",current_day,' ',current_month," '",
                  format(max(data_f1$Relevant_Date),format="%y"),")")

    ########################################################GRAPH#####################
    line=ggplot(data=data_final)+
      geom_line(aes(x=Relevant_Date,y=(value_y_left),
                    group=category,
                    color=category,linetype =category ),
                    size=line_thick)+
      geom_point(aes(x=Relevant_Date,y=(value_y_left),
                        colour = category, shape = category),
                        group=1,size=2)+
      geom_text_repel(aes(x=Relevant_Date,y=value_y_left,
                      label=line_lab),data=data_ends,
                      max.overlaps=max_overlap,
                      direction="y",
                      font="bold",
                      min.segment.length = Inf,  #Reomes_line
                      na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                      size =chart_label,family="Calibri")+
      #
      # scale_colour_manual(values=my_legends_col)+
      # scale_linetype_manual(values=my_line_type)+


     scale_linetype_manual(values=c("dashed","solid","solid","solid","solid"))+
     scale_colour_manual(values=c("BLACK","BURLYWOOD 1","GRAY 32","DARK ORANGE 2","SADDLE BROWN"),
          guide = guide_legend(override.aes =list(linetype = c("dashed","solid","solid","solid","solid"))))+


      scale_shape_manual(values=c(32,32,32,32,15)) +

      scale_y_continuous(expand = expansion(mult=c(0,0.04)),breaks=pretty_breaks(n=num_brek),
                         limits = c(y_min,y_max),
                         labels =number_format( big.mark =",",
                                                style_positive = c("none"),
                                                style_negative = c("parens")))+

      scale_x_date(limits = as.Date(c(x_min,NA)),
                   labels =date_format("%B"),
                   date_breaks=x_axis_interval,expand =c(0,15))+

      common_theme(Position = led_position)+
      cutom_theme()
      # guides(fill=TRUE)+
      # guides(colour=guide_legend(ncol=n_col,nrow=n_row,byrow=FALSE,
      # reverse=FALSE,order =1))
      # guides(colour =guide_legend(nrow=1,byrow=FALSE))


    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))


}

#Scale fill manual represents what will be the color of your graph.
# scale_fill_manual(values=my_chart_col)+
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.



## -------------------------------------
line_chart_niif <- function(data1,x_axis_interval="24 month",sales_heading,
                            graph_lim=10,SPECIAL_LINE=FALSE,
                            graph_lim1=0,
                            led_position='center',
                            legend_key_width=0.27,Reference=FALSE,
                            DATE_HEADER=FALSE,Repo=FALSE,y_dot_rng=18,
                            show_legend=TRUE,reverse_y=FALSE,
                            show_fu_dt=FALSE,
                            exculde_FY=FALSE,round_integer=FALSE,
                            key_spacing=0.10){

  tryCatch({

    my_chart_col=c("GOLDEN ROD 1","GRAY 48")
    Position=led_position
    ##########################DATA_PROCESSING###########################
    showtext_auto()
    # data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    # data1 <- data1[order(data1$Relevant_Date),]
    # data1$Month <- as.Date(data1$Relevant_Date,format = "%Y-%m-%d")
    # data1$value_y_left=data1[1:nrow(data1),2]
    #
    # data_final=cbind(data1[,c("Relevant_Date","value_y_left","Month")])
    # data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))
    #
    #
    # data_final=na.omit(data_final)
    # data_final=data_final[order(data_final$Relevant_Date),]
    # data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
    # data1=RBI_Repo_Rates
    data_final=create_df_for_chart(data1,line=TRUE,SHOW_FU_DATE=show_fu_dt)[[1]]
    if(DATE_HEADER==TRUE){
         prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))
    }else{
        prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}

    print(prev_month)
    data_final<- data_final[data_final$Relevant_Date<=prev_month,]

    x_min=min(data_final$Month)
    x_max=max(data_final$Month)


    if (SPECIAL_LINE==TRUE){
                         max_factor=max_pri_y
                         y_max=max_factor*max(data_final$value_y_left)
                         min_factor=min_pri_y/abs(min(data_final$value_y_left))
                         y_min=min_factor*min(data_final$value_y_left)

    }else{
         max_factor=max_pri_y/max(data_final$value_y_left)
         y_max=round(max_factor*max(data_final$value_y_left),digits =-1)

         if (min(data_final$value_y_left)==0){
                                              min_factor=0
                                              y_min=round(min_factor*min(data_final$value_y_left),digits = -1)

         }else {min_factor=min_pri_y/abs(min(data_final$value_y_left))
                y_min=round(min_factor*min(data_final$value_y_left),digits = -1)}

    }
    if (round_integer==TRUE){
                  y_max=ceiling(max_factor*max(data_final$value_y_left))
                  y_min=ceiling(min_factor*min(data_final$value_y_left))

    }

    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    print(data_final)
    data_ends <- data_final %>% filter(Month == Month[length(Month)])

    print(data_ends)

    if (Repo==TRUE){
        if (data_ends$value_y_left<0){
               line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_left),2),nsmall=2,big.mark=","),")")
               print(line_lab)

        }else{line_lab=format(roundexcel(abs(data_ends$value_y_left),2),nsmall=2,big.mark=",")}

    }else{
          if (data_ends$value_y_left<0){
               line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=","),")")
               print(line_lab)

         }else{line_lab=format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=",")}
    }

    if (Reference==TRUE){
              reference_line_1=geom_segment(x =x_min, y = y_dot_rng,
                                            xend =x_max, yend =y_dot_rng,
                                            color = "black",size=0.75,
                                            show.legend =FALSE,
                                            linetype =2)

              a1=annotate("text",x =x_max-20,y =20,
                          label=as.character(y_dot_rng),
                          angle=0, size=10.5,family="Calibri_Regular")



      }else{
           reference_line_1=geom_segment(x =x_min, y =0,
                                         xend =x_max, yend =0,
                                         color = "GRAY 32",size=0.25,
                                         show.legend =FALSE,
                                         linetype =1)
            a1=annotate("text",x =x_min,y = 0, label="", angle=0, size=14, color="black")

      }

    if (reverse_y==TRUE){
      cordi=coord_cartesian(ylim =c(y_max,y_min))
      trans_formation='reverse'

    }else{
      cordi=coord_cartesian(ylim =c(y_min,y_max))
      trans_formation='identity'

    }

    ###################################HEADER_SUB_HEADER##################
    sub_h=get_sub_title(data_final,data_ends,
                        show_DATE_HEADER =DATE_HEADER,
                        exculde_fy =exculde_FY)

    ##################################################GRAPH#####################

    line=ggplot(data=data_final)+
      geom_line(aes(x=Month,y=value_y_left,color=paste0(sales_heading)),
                size=line_thick,group=1,
                show.legend = show_legend,
                linetype =1)+

      geom_text_repel(aes(x=Month,y=value_y_left,
                    label=line_lab),
                    max.overlaps=max_overlaps,data=data_ends,
                    direction="y",
                    font="bold",
                    min.segment.length = Inf,  #Reomes line
                    na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                    size =chart_label,family="Calibri")+

      scale_colour_manual(values=my_chart_col)+
      scale_y_continuous(trans =trans_formation,
                         expand = expansion(mult = c(0,0.04)),breaks=pretty_breaks(n=num_brek),
                         labels =number_format(big.mark =",",
                                               style_positive = c("none"),
                                               style_negative = c("parens")))+
      cordi+
      scale_x_date(limits =as.Date(c(x_min-graph_lim1,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =seq.Date(min(data_final$Month),
                                    max(data_final$Month),
                                    by=x_axis_interval),
                   expand =c(0,0))+

      reference_line_1+
      a1+
      common_theme(Position = led_position)+
      cutom_theme()

    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))

},
  error = function(e){
    log_generated_error(e)

    }
  )
}


## -------------------------------------
stacked_bar_line_chart_special_niif <-  function(data1,data2,data3,x_axis_interval,
                                                 growth_heading,graph_lim=30,
                                                 negative=TRUE,
                                                 DUAL_AXIS=TRUE,bar_thick=8,key_spacing=0.10,led_position='center'){

  tryCatch({

    showtext_auto()
    legend_key_width=0.27
    Position=led_position


    #NEW_MODIFIED
    ##########################DATA_PROCESSING###########################
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data_final=merge(data1, data2,by="Relevant_Date")
    data_final= reshape2::melt(data_final,id=c("Relevant_Date","growth"))
    data_final$Month=as.Date((data_final$Relevant_Date))


    data_final=data_final %>%
                rename(value_y_right=growth,
                       category=variable,
                       value_y_left=value
                       )
    data_final=do.call(data.frame,lapply(data_final,

                          function(x) replace(x, is.infinite(x), NA)))
    data_final=merge(data_final,data3,by="Relevant_Date")


    data_final=data_final[order(data_final$Relevant_Date),]
    data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
    prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
    print(prev_month)
    data_final<- data_final[data_final$Relevant_Date<=prev_month,]

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)

    data_ends <- data_final %>% filter(Month == Month[length(Month)])

     if  (negative==TRUE){
          v1=c(data_ends$value_y_left)
          neg_indx=c(which(v1<0))
          v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
          v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
          v1[neg_indx]=v3
          label_1=v1


    }else{
      label_1=format(roundexcel(data_ends$value_y_left,1),nsmall=1,
                                           big.mark=",")
    }

    #FOR LABELS OF single Line Chart
    if (data_ends$value_y_right[1]<0){
         line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_right[1]),1),nsmall=1,big.mark=","),")")

    }else{
        line_lab=format(roundexcel(abs(data_ends$value_y_right[1]),1),nsmall=1)
    }

    # FOR LABELS OF DOTTED LINE CHART
    if (data_ends$cumulative_rolling[1]<0){
         line_lab_rolling=paste0("(",format(roundexcel(abs(data_ends$cumulative_rolling[1]),1),nsmall=1,
                                            big.mark=","),")")
    }else{
    line_lab_rolling=format(roundexcel(abs(data_ends$cumulative_rolling[1]),1),nsmall=1,big.mark=",")
}

    #FOR Y SCALE LIMIT
    max_factor=max_pri_y/max(data_final$value_y_left)
    min_factor=min_pri_y/abs(min(data_final$value_y_left))


    y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
    y_min=round(min_factor*min(data_final$value_y_left),digits = -1)

    if (y_max<=0){
             y_max=max_factor*max(data_final$value_y_left)
    }
    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    normalizer <-max_sec_y/y_max

    if (DUAL_AXIS==TRUE){
      RIGHT_CHART_scale=scale_y_continuous(expand = expansion(mult=c(0,0.04)),breaks=pretty_breaks(n=num_brek),
                                  labels =number_format(
                                              big.mark = ",",
                                              style_positive = c("none"),
                                              style_negative = c("parens")
                                               ),
                         sec.axis =sec_axis(~.*normalizer,
                         breaks=pretty_breaks(n=num_brek),
                         labels =number_format(
                                              big.mark =",",
                                              style_positive = c("none"),
                                              style_negative = c("parens")
                                               )))

      }else{
        RIGHT_CHART_scale=scale_y_continuous(expand=expansion(mult=c(0,0.04)),
                                           breaks=pretty_breaks(n=num_brek),
                                           # limits =c(y_min,y_max),
                                           labels =number_format(
                                              big.mark =",",
                                              style_positive = c("none"),
                                              style_negative = c("parens")
                                               ))

      }


    ###################################HEADER_SUB_HEADER##################

    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)

    current_month=format(data_ends$Month,format="%b")
    current_month_num=format(data_ends$Month,"%m")
    first_mon_mum=format(first_date,"%m")


    if (as.numeric(current_month_num[1])>=4){
       current_f_year=format(current_date+years(1),format="%Y" )

    }else{current_f_year=format(current_date,format="%Y" )}

    if (as.numeric(first_mon_mum[1])>=4){
       first_f_year=format(first_date+years(1),format="%Y" )
       print(first_f_year)

    }else{first_f_year=format(first_date,format="%Y" )
    }

    sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                format(data_ends$Month,format="%y"),")")

#############################################GRAPH############
     stacked_bar=ggplot(data=data_final)+
               #NOTE: Please dont give show.legend = TRUE/False
               geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
                        width = bar_thick) +

               geom_text_repel(mapping=aes(x=Month,y=(value_y_left),fill=category,
                              #Here Fill is extremly_important
                              # label_1
                              label=label_1),data=data_ends,
                              stat ="identity",
                              position = "stack",
                              max_overlaps =max_overlap,
                              direction="y",
                              font="bold",
                              # force=force_bar,
                              # point.padding=unit(pad_b,'lines'),
                              min.segment.length = Inf,#Reomes line
                              na.rm=TRUE,hjust=h_just,vjust=v_just,
                              size =chart_label,family="Calibri")+


       geom_line(aes(x=Month,y=(value_y_right),
                          color=paste0(growth_heading)),
                          size=line_thick,linetype =1,group=1)+

       geom_text_repel(aes(x=Month,y=value_y_right,
                              label=line_lab),
                              data=data_ends[1,],
                              direction="y",#All changes will be effect on verticle axis.
                              min.segment.length = Inf,#Reomes line
                              nudge_x =nug_x,
                              font="bold",
                              max_overlaps =max_overlap,
                              na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,#Standard one dont change it.
                              size =chart_label,family="Calibri")+

        geom_line(aes(x=Month,y=(cumulative_rolling )/normalizer,
        color=paste0(cumulative_rolling_heading),show.legend = FALSE),
        size=0.50,linetype ="dashed",group=1)+

        geom_text_repel(aes(x=Month,y=(cumulative_rolling)/normalizer,
        label=line_lab_rolling),
        data=data_ends[1,],
        direction="y",#All changes will be effect on verticle axis.
        min.segment.length = Inf,#Reomes line
        nudge_x =nug_x,
        font="bold",
        max_overlaps =max_overlap,
        na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,#Standard one dont change it.
        size =chart_label,family="Calibri")+



               # NOTE:--if u see bar are surround with another colour that means u have done something wrong                while arranging colors.

               #Scale fill manual represents what will be the color of your graph.
               scale_fill_manual(values=my_chart_col)+
               # scale_linetype_manual(values=c("dashed","solid"))+
               scale_linetype_manual(values=c("solid"))+

               # scale_colour_manual(values=c("GRAY 88","GRAY 48"),
               #           guide = guide_legend(override.aes = list(
               #           linetype = c("twodash","solid"))))+

               scale_colour_manual(values=c("GRAY 48"),
                         guide = guide_legend(override.aes = list(
                         linetype = c("solid"))))+

               #Scale color manual represents what will be the color of your corresponding legends
               #Legends always placed with alphabatical order.
               #
              RIGHT_CHART_scale+
              coord_cartesian(ylim =c(y_min,y_max))+

              scale_x_date(limits =as.Date(c(x_min,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =seq.Date(min(data_final$Month),
                                    max(data_final$Month),
                                    by=x_axis_interval),
                   expand =c(0,0))+

                #To exclude that legend that i don't want.
                # guides( "none")+
                # guides(fill=FALSE)+
                #Fill--->for bar chart  #Order=1 means it will place that type at top
                #color--->line Chart
                guides(fill=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE,order=1))+
                guides(color =guide_legend(ncol=2,nrow=1,byrow=TRUE,order=2))+

                # guides(fill=guide_legend(reverse=T))+
                 common_theme(Position = led_position)+

                cutom_theme_1()
      # labs(caption ="Source: Bloomberg, NIIF Research")+
      # theme(plot.caption =element_text(size =35,
      #           family="Calibri_Light",
      #           hjust=-0.15,
      #           vjust=-2.5
      #          ))

    ########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))

},
  error = function(e){
   log_generated_error(e)

    }
  )
}


## -------------------------------------
side_bar_chart_niif_rupee <-  function(data1,graph_lim=30,negative=TRUE,
                                       Position="left",legends_break=FALSE,
                                       legends_reverse=FALSE,x_angle1=0,
                                       show_legend=TRUE,SIDE_BAR=TRUE,
                                       DATE_HEADER=FALSE,pos_d=0.3,pos_lb=0.1,bar_thick=8,key_spacing=0.10,
                                       led_position='center'){

  tryCatch({

    Position=led_position
    ##########################DATA_PROCESSING###########################
    # side_width=0.5
    # bar_thick=0.5

    showtext_auto()
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1$Month <- as.Date(data1$Relevant_Date)

    data1$x_axis=data1[1:nrow(data1),2]
    data1$category=data1[1:nrow(data1),3]
    data1$value_y_left=data1[1:nrow(data1),4]


    data_final=data1[,c("Relevant_Date","x_axis","category","value_y_left","Month")]
    data_final=data_final[order(data_final$Relevant_Date),]

    x_max=max(data_final$Month)
    x_min=min(data_final$Month)

    print(data_final)
    # data_ends <- data_final %>% filter(Month == Month[length(Month)],category=="USD")
    data_ends <- data_final %>% filter(Month == Month[length(Month)])

    print(data_ends)

    if  (negative==TRUE){
                        v1=c(data_ends$value_y_left)
                        neg_indx=c(which(v1<0))
                        v1=format(roundexcel((v1),1),nsmall=1,big.mark=",")
                        v3=paste0("(",format(abs(as.numeric(v1[neg_indx])),nsmall=1),")")
                        v1[neg_indx]=v3
                        label_1=v1


    }else{
         label_1=format(roundexcel(as.numeric(data_ends$value_y_left),1),nsmall=1,big.mark=",")}

    #Primary and secondary axis interval related work
    if (max(data_final$value_y_left)==0){
                                        max_factor=0
    }else{max_factor=max_pri_y/max(data_final$value_y_left)}

    if (min(data_final$value_y_left)==0){
                                      min_factor=0
    }else{min_factor=min_pri_y/abs(min(data_final$value_y_left))}

    print(paste("Min factor:",min_factor))

    # y_max=round(max_factor*max(data_final$value_y_left),digits =-1)
    # y_min=round(min_factor*min(data_final$value_y_left),digits = -1)

    y_max=round(max_factor*max(data_final$value_y_left))
    y_min=round(min_factor*min(data_final$value_y_left))

    if (y_max<=0){
                  y_max=max_factor*max(data_final$value_y_left)
}
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
     # print(data_ends)
    if (legends_break==TRUE){
      guides_type=guides(fill =guide_legend(nrow=n_row,byrow=TRUE,reverse=TRUE,order =1))

    }else if(legends_reverse==TRUE){
       guides_type=guides(fill =guide_legend(order =1,reverse=TRUE))

      # guides_type=guides(color =guide_legend(order =2),
      #                     fill =guide_legend(order =1,reverse=TRUE))



    }else{
      guides_type=guides(color =guide_legend(order =2),
                          fill =guide_legend(order =1,reverse=FALSE))

    }

    if (SIDE_BAR==TRUE){
                        bar_position="dodge2"
                        bar_position=position_dodge(width = bar_thick)
                        bar_position=position_dodge(pos_d)

     }else{bar_position ="stack"}

########################################HEADER_SUB_HEADER##################
    sub_h=get_sub_title(data_final,data_ends,show_DATE_HEADER =DATE_HEADER)

##################################################GRAPH#####################
   stacked_bar=ggplot(data=data_final,
                       aes(x=factor(x_axis,
                             level=custom_lev),

                            y=value_y_left,
                            fill=category)) +

                geom_bar(stat="identity",
                         width =bar_thick,
                         show.legend = show_legend,

                         position=bar_position)+

                geom_text(aes(label=label_1),
                          data=data_ends,
                          vjust =v_just,hjust=h_just,,
                          color="black",
                          # position = position_dodge(pos_lb),
                          position =bar_position,
                          size =chart_label)+

    #--------------------old Logic---------------------------------------

    # stacked_bar=ggplot(data=data_final)+
    #
    #   geom_bar(aes(x=factor(x_axis,
    #                         level=c("1-month","3-month","6-month","1-year","3-year","5-year","10-year")),
    #
    #                  y=(value_y_left),
    #                  group=category,
    #                  fill=category),
    #
    #            stat = "identity",
    #            width =bar_thick,
    #            position=position_dodge2(width=side_width,preserve="single"),
    #            show.legend=TRUE,key_glyph =draw_key_rect)+
    #
    #  geom_text_repel(mapping=aes(x=as.factor(x_axis),
    #                             y=value_y_left,
    #                             fill=category,label=label_1),
    #
    #                 data=data_ends,
    #                 min.segment.length = Inf,
    #                 na.rm=TRUE,hjust=h_just,vjust=v_just,
    #                 direction="y",
    #                 font="bold",
    #                 size =chart_label,family="Calibri",
    #                 stat = "identity")+

    #------------------------------------------------------------------------------

      scale_fill_manual(values=my_chart_col)+
      scale_colour_manual(values=my_legends_col)+

      scale_y_continuous(expand=expansion(mult = c(0,0.04)),
                         breaks=pretty_breaks(n=num_brek),
                         labels =number_format(big.mark =",",
                                               style_positive = c("none"),
                                               style_negative = c("parens")))+
      coord_cartesian(ylim =c(y_min,y_max))+
      guides_type+
      common_theme(x_angle=x_angle1,Position = led_position)+
      cutom_theme()

    ########################################################RETURN######################
    return(list("chart"=stacked_bar,"s_header"=sub_h[1]))
},
  error = function(e){
   log_generated_error(e)

    }
  )
}
#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## -------------------------------------
map_rainfall_niif <- function(data1){

  tryCatch({
    ##########################DATA_PROCESSING###########################
    showtext_auto()
    India<- read_sf(map_template)
    India=India[,c("Name","geometry")]
    colnames(India)=c("State","geometry")

    # Som times sahape fines names are not same
    data1=data1[data1$State!="India",]
    data_final1=India %>%
                left_join(data1,by="State")

    data_final1$Start_Date=data1[1:nrow(data1),4]

    ###################################HEADER_SUB_HEADER##################
    current_date=max(data1$Relevant_Date)
    first_date=min(data1$Start_Date)

    current_month=format(as.Date(current_date),format="%b")
    prev_month=format(as.Date(first_date),format="%b")


    current_day=format(as.Date(current_date),format="%d")
    prev_day=format(as.Date(first_date),format="%d")

    sub_h=paste0(prev_month," ",prev_day," to ",current_month," ",current_day,", ",format(as.Date(current_date),format="%Y"))

    ########################################################GRAPH#####################
    line=ggplot(data=data_final1,aes(fill=Rainfall,
                        color=Rainfall))+
         ggplot2::geom_sf(colour="white")+
                  coord_sf(xlim = c(68.35859, 97.46212),
                           ylim = c(6.733651, 37.03473),
                           expand = FALSE)+

         scale_fill_manual(values=my_legends_col)+
         # scale_color_manual(values = my_legends_col)+

         # guides(fill =guide_legend(order =1,reverse=TRUE))+

         guides(fill =guide_legend(nrow=n_row,byrow=TRUE,reverse=FALSE,order =1))+


         common_theme()+
         cutom_theme_map()
         # cutom_theme()



    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))

},
  error = function(e){
    log_generated_error(e)

    }
  )
}



## -------------------------------------
data_query_clk_pg=function(id,exception=FALSE,surplus=FALSE,
                           Water_Reservior=FALSE,VAHAN=FALSE,year=FALSE,set_perid=10){

   tryCatch({

    clk  <- DBI::dbConnect(RClickhouse::clickhouse(),
                               dbname =DB_cl,host = host_cl,
                               port = port_cl,user = user_cl,password =password_cl)

    pg  <- DBI::dbConnect(RPostgres::Postgres(),
                          dbname =DB_pg,host = host_pg,
                          port = port_pg,user = user_pg,
                          password =password_pg)

    if (surplus==TRUE){
              period=2500
    }else if(year==TRUE){
      period=set_perid
    }
    else{period=162}

    qstring='select * from widgets_new where widget_id=chart_id'
    quary_by_id=str_replace(qstring,"chart_id",glue({id}))

    res =dbGetQuery(pg,quary_by_id)
    d1=res[,"query_param"]
    descrip=res[,"title"]
    data_upto=res[,"data_upto"]
    source=paste0("Source: ","Thurro, ",res[,"source"][1],", NIIF Research")
    quary_1=fromJSON(d1, flatten=TRUE)
    y_lab=quary_1[7][[1]][[1]]
    plot_type=quary_1[6][[1]][[1]]

    if(exception==TRUE){
              final_quary=str_replace(quary_1[1],"@@periodQtr@@", glue({period}))
    }else if(Water_Reservior==TRUE){
              final_quary=str_replace(quary_1[9], "@@period@@", glue({period}))

    }else if(VAHAN==TRUE){
              final_quary=str_replace_all(quary_1[1],
                                          c("@@period@@"=glue({period}),'2021-04-30'='2000-01-01',
                                                                        '2021-05-31'='2000-01-01'))
    }else{
         final_quary=str_replace(quary_1[1], "@@period@@", glue({period}))
         final_quary=str_replace_all(quary_1[1], "@@period@@", glue({period}))
    }

    print(final_quary)
    final_data_frame = dbGetQuery(clk,final_quary)

    result = list("data_frame" = final_data_frame,
                  "description" =  descrip,
                  "source"=source,
                  "data_upto"=data_upto)

    dbDisconnect(clk)
    dbDisconnect(pg)

    d1=tolower(final_quary)
    str_remove(d1,'where')
    s1=str_split(d1,'where')[[1]][1]
    s2=str_split(s1,'from')[[1]][2]
    mv_table=str_trim(s2, side="both")
    df1=data.frame(ID=c(id),MV_TABLE=c(mv_table))
    write.xlsx(df1,file.path(excel_dir, paste0('df_niif_',id,'_.xlsx')))

    return(result)
   },
   error = function(e){
      error_type = class(e)[1][[1]]
      error_msg =paste0(e[[1]],"-id-",id)
      end_time = Sys.time()
      exe_time_sec = end_time - start_time
      val2=paste0(paste0("'",end_time,"',"),
                  paste0(exe_time_sec,","),
                  paste0(0,","),
                  paste0('"',error_type,'",'),
                  paste0('"',error_msg,'",'),
                  paste0(rep("'NA'",2), collapse = ", "),
                  paste0(","),
                  paste0(rep("''", 3), collapse = ", "),
                  paste0(", '",log_rel_date,"',"),
                  paste0("'",Sys.time(),"'"))
      print(val2)
      query  = paste0(q1,val1,val2,");")
      query <- gsub("'NA'", "NULL", query)
      print(query)
      #execute the query
      con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                        user = username, password = password)
      dbExecute(con, query)
      dbDisconnect(con)
      print(class(error_msg))


      }
  )

}

mon_qtr_df_creator=function(df,keep_col=c("Relevant_Date","Equity")){
    f2=function(x){
          max_date=as.Date(x[1])
          current_month=format(max_date,format="%b")
          current_month_num=format(max_date,"%m")
          current_f_year=format(max_date %m-% months(0))}

    df["Qtr_mon"]=apply(df,1,f2)
    df_2 <- df %>% mutate_at(c(which(names(df)=="Qtr_mon")), as.Date)
    df_2["Relevant_Date"]=as.Date(timeLastDayInQuarter(df_2$Qtr_mon))
    df_2=df_2[1:nrow(df_2),c(1,3)]

    df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))

    df1=merge(df_2,df,by="Relevant_Date")
    df1=df1[!duplicated(df1[c("Relevant_Date")]), ]
    df1=df1[,keep_col]

}

re_arrange_columns=function(df,sort_type="",top_col=''){
  df<- df[df$Relevant_Date<=prev_month,]
  df1=df[df$Relevant_Date==max(df$Relevant_Date),]
  df1= reshape2::melt(df1,id=c("Relevant_Date"))
  if (sort_type=='desc'){
    ordered_df <- df1 %>%arrange(desc(value))
  }else{
    ordered_df <- df1 %>%arrange((value))
    }

  new_col=unique(c(ordered_df$variable))
  if (top_col!=''){
    new_col <- subset(new_col, new_col != top_col)
    col_n=c('Relevant_Date',top_col)
  }else{
    col_n=c('Relevant_Date')
  }

  for (i in new_col){
    print(i)
    col_n=c(col_n,i)
  }
  my_chart_col=c("#8B6914","#8B2500","#FEECDA","GRAY 88","TAN 1",
                 "BURLYWOOD 1","#EE9572","GRAY 48","#BF6E00",
                 "DARK ORANGE 2","GOLDEN ROD 1")

  my_chart_col=rev(my_chart_col)[1:length(col_n)-1]
  org_col <- subset(col_n, col_n != 'Relevant_Date')
  my_shorted_col=setNames(my_chart_col, rev(org_col))

return(list("columns"= col_n,"colours"=my_shorted_col))

}


## -------------------------------------
line_thick=0.60
bar_thick=8
max_overlap=10
num_brek=4
#####legends
legend_key_size=0
legend_key_width=0.27
legend_key_height=0.25
key_spacing=0.10
legend_key_sp_y=0.05
Position="center"

####Fonts
text_size=30
chart_label=8
v_just=0
x_v=0
nug_x=0.1
nug_y=0.3



## -------------------------------------
#Monthly Production & Growth
Cement_Prod1 =data_query_clk_pg(738892)
Cement_Prod1_title=Cement_Prod1[2][[1]]
Cement_Prod1_src=Cement_Prod1 [3][[1]]
Cement_Prod1=Cement_Prod1 [1][[1]]
Cement_Prod=Cement_Prod1[,c("Relevant_Date","Total")]

######################################################
#3W GROWTH RATE
Cement_Prod_gr=Cement_Prod1[,c("Relevant_Date","Total1")]
names(Cement_Prod_gr)[2]="Cement production growth (RHS, % yoy)"



## -------------------------------------
data_s=Cement_Prod
data_g=list(Cement_Prod_gr)

text_size1=35
chart_label=8
# l2=-1
# l1=-2
l2=-2.5
l1=-0.15

num_brek=5
max_pri_y=50
min_pri_y=-10
max_sec_y=50

h_just=-0.50
v_just=0.75
h_just_line=0.40
v_just_line=0.40

my_line_type=c("Cement production growth (RHS, % yoy)"="solid")
my_leg_col=c("Cement production growth (RHS, % yoy)"="Gray 48")

Cement_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
          sales_heading=paste0("Cement production (LHS, mn tonnes)"))


Cement_Prod_7_title=Cement_Prod_7_chart[2][[1]]
Cement_Prod_7_chart=Cement_Prod_7_chart[1][[1]]
Cement_Prod_7_src=Cement_Prod1_src
Cement_Prod_7_chart


## -------------------------------------
Steel_Prod1 =data_query_clk_pg(2041910)
Steel_Prod1_title=Steel_Prod1[2][[1]]
Steel_Prod1_src=Steel_Prod1[3][[1]]
Steel_Prod1=Steel_Prod1[1][[1]]
Steel_Prod=Steel_Prod1[,c("Relevant_Date","Total")]

######################################################
#GROWTH RATE
Steel_Prod_gr =data_query_clk_pg(2041915)[1][[1]]
names(Steel_Prod_gr)[2]="Crude steel production (RHS, % yoy)"


## -------------------------------------
data_s=Steel_Prod
data_g=list(Steel_Prod_gr)

h_just=-1
v_just=2
h_just_line=0
v_just_line=0.85

num_brek=5
max_pri_y=20
min_pri_y=-10
max_sec_y=20

my_line_type=c("Crude steel production (RHS, % yoy)"="solid")
my_leg_col=c("Crude steel production (RHS, % yoy)"="Gray 48")

Steel_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
               sales_heading="Crude steel production (LHS, mn tonnes)")

Steel_Prod_7_title=Steel_Prod_7_chart[2][[1]]
Steel_Prod_7_chart=Steel_Prod_7_chart[1][[1]]
#HARD_CODE:: JPC WAS ADDEd
Steel_Prod_7_src='Source: Thurro, Ministry of Steel, JPC, NIIF Research'
Steel_Prod_7_chart


## -------------------------------------
Coal_Prod1=data_query_clk_pg(738941)
Coal_Prod_title=Coal_Prod1[2][[1]]
Coal_Prod1=Coal_Prod1[1][[1]]

Coal_Prod=Coal_Prod1[,c("Relevant_Date","Total")]

#HARD Code ::Due to data issue
Coal_Prod=Coal_Prod[Coal_Prod$Relevant_Date<='2020-03-31',]


Coal_Prod_new=data_query_clk_pg(723037)[1][[1]]
Coal_Prod_src=data_query_clk_pg(723037)[3][[1]]
Coal_Prod_new=Coal_Prod_new[,c("Relevant_Date","Total")]
Coal_Prod_new=Coal_Prod_new[Coal_Prod_new$Relevant_Date>max(Coal_Prod$Relevant_Date),]
Coal_Prod_new=rbind(Coal_Prod,Coal_Prod_new)

#GROWTH
Coal_Prod_gr=Coal_Prod1[,c("Relevant_Date","Total1")]

#HARD Code ::Due to data issue
Coal_Prod_gr=Coal_Prod_gr[Coal_Prod_gr$Relevant_Date<='2021-03-31',]
Coal_Prod_gr_new=data_query_clk_pg(273532)[1][[1]]
colnames(Coal_Prod_gr_new)=c("Relevant_Date","Total1")
Coal_Prod_gr_new=Coal_Prod_gr_new[Coal_Prod_gr_new$Relevant_Date>max(Coal_Prod_gr$Relevant_Date),]
Coal_Prod_gr_new=rbind(Coal_Prod_gr,Coal_Prod_gr_new)
names(Coal_Prod_gr_new)[2]="Coal production growth (RHS, % yoy)"



## -------------------------------------
data_s=Coal_Prod_new
data_g=list(Coal_Prod_gr_new)

h_just=0.25
v_just=0.0
h_just_line=0.60
v_just_line=0.60

num_brek=8
max_pri_y=120
min_pri_y=-40
max_sec_y=30

my_line_type=c("Coal production growth (RHS, % yoy)"="solid")
my_leg_col=c("Coal production growth (RHS, % yoy)"="Gray 48")

Coal_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="Coal production (LHS, mn tonnes)")

Coal_Prod_7_title=Coal_Prod_7_chart[2][[1]]
Coal_Prod_7_chart=Coal_Prod_7_chart[1][[1]]
Coal_Prod_7_src=Coal_Prod_src
Coal_Prod_7_chart


## -------------------------------------
e_way_bills=data_query_clk_pg(id=523012)
e_way_bills_title=e_way_bills[2][[1]]
e_way_bills_src=e_way_bills[3][[1]]
e_way_bills=e_way_bills[1][[1]]

e_way_bills=e_way_bills[,c("Relevant_Date","Total")]
names(e_way_bills)[2]='Value'

e_way_bills$Value=e_way_bills$Value/10^6

#E_WAY_gr
e_way_bills_gr= data_query_clk_pg(id=523035,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)
e_way_bills_gr=e_way_bills_gr[1][[1]]
colnames(e_way_bills_gr)=c("Relevant_Date","Total1")
names(e_way_bills_gr)[2]="e-way bills growth (RHS, % yoy)"

#HARD Code ::NIIF taken from April-2019
e_way_bills=e_way_bills[e_way_bills$Relevant_Date>="2019-04-01",]



## -------------------------------------
data_s=e_way_bills
data_g=list(e_way_bills_gr)

x_axis_interval="6 month"

num_brek=8
max_pri_y=120
min_pri_y=-20
max_sec_y=120

my_line_type=c("e-way bills growth (RHS, % yoy)"="solid")
my_leg_col=c("e-way bills growth (RHS, % yoy)"="Gray 48")

e_way_bills_chart=line_bar_chart_niif(data_s,data_g,
                                      graph_lim =90,
                                      sales_heading="e-way bills (LHS, million)",
                                      x_axis_interval="12 month")

e_way_bills_8_title=e_way_bills_chart[2][[1]]
e_way_bills_chart=e_way_bills_chart[1][[1]]
e_way_bills_8_src=e_way_bills_src

e_way_bills_chart


## -------------------------------------
Four_W_reg = data_query_clk_pg(id=526271)
Four_W_reg_title=Four_W_reg[2][[1]]
Four_W_reg_src=Four_W_reg[3][[1]]
Four_W_reg=Four_W_reg[1][[1]]
Four_W_reg$Total=Four_W_reg$Total/1000


#4W | ELECTRIC(BOV)
four_el =data_query_clk_pg(id=527263)[1][[1]]
names(four_el)[2]='Electric'
four_el$Electric=four_el$Electric/1000
Four_W_reg=add_proxy_dates(Four_W_reg,four_el)
Four_W_reg$Electric[is.na(Four_W_reg$Electric)] <- 0

Four_W_reg=Four_W_reg %>%
           mutate(Total = ifelse(four_el$Electric==0,Total, Total-Electric))


colnames(Four_W_reg)=c("Relevant_Date","PV non-EV registrations (LHS, '000s)",
                      "PV EV registrations (LHS, '000s)")


#######################################################3W GROWTH RATE
Four_W_gr = data_query_clk_pg(id=526302,exception=FALSE,surplus=FALSE,
                              Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(Four_W_gr)[2]='growth'

#HARD Code ::id 526302 having NA value upto March-2014

Four_W_reg=Four_W_reg[Four_W_reg$Relevant_Date>="2014-01-31",]
Four_W_gr=Four_W_gr[Four_W_gr$Relevant_Date>="2014-01-31",]
# Four_W_gr$growth=movavg(Four_W_gr$growth,3,t="s")


## -------------------------------------
data_s=Four_W_reg
data_g=Four_W_gr


# BURLYWOOD 1
my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY GRAY 96")

h_just=0
v_just=0.40
h_just_line=0
v_just_line=0.75


n_row=3
n_col=1
chart_label=8

num_brek=5
max_pri_y=400
min_pri_y=-100
max_sec_y=80

passenger_vehicle_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="PV total growth registrations (RHS, % yoy)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=60,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break =TRUE,order_stack=TRUE,add_std_col = FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,line_type = 1)


passenger_vehicle_title=passenger_vehicle_chart[2][[1]]
passenger_vehicle_chart=passenger_vehicle_chart[1][[1]]
passenger_vehicle_src=Four_W_reg_src

#HARD CODE::Long source not getting enough space in PPT
passenger_vehicle_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"
passenger_vehicle_chart


## -------------------------------------
TWO_Sales =data_query_clk_pg(id=526281)
TWO_Sales_title=TWO_Sales[2][[1]]
TWO_Sales_src=TWO_Sales[3][[1]]
TWO_Sales=TWO_Sales[1][[1]]
TWO_Sales$Total=TWO_Sales$Total/1000

#2W | ELECTRIC(BOV)
Two_el =data_query_clk_pg(id=526965)[1][[1]]
names(Two_el)[2]='Electric'
Two_el$Electric=Two_el$Electric/1000
TWO_Sales=add_proxy_dates(TWO_Sales,Two_el)
TWO_Sales$Electric[is.na(TWO_Sales$Electric)] <- 0
TWO_Sales=TWO_Sales %>%
           mutate(Total = ifelse(TWO_Sales$Electric==0,Total, Total-Electric))


colnames(TWO_Sales)=c("Relevant_Date","2W non-EV registrations (LHS, '000s)",
                      "2W EV registrations (LHS, '000s)")

#######################################################3W GROWTH RATE
TWO_gr =data_query_clk_pg(id=526384,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(TWO_gr)[2]='growth'

#HARD Code ::id 526384 having NA value upto March-2014

TWO_Sales=TWO_Sales[TWO_Sales$Relevant_Date>="2014-01-01",]
TWO_gr=TWO_gr[TWO_gr$Relevant_Date>="2014-01-01",]
# TWO_gr$growth=movavg(TWO_gr$growth,3,"t")



## -------------------------------------
data_s=TWO_Sales
data_g=TWO_gr

# BURLYWOOD 1
my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY GRAY 96")

h_just=0
v_just=0
h_just_line=2
v_just_line=0.25

n_row=2
n_col=3
chart_label=8

num_brek=5
max_pri_y=2500
min_pri_y=-500
max_sec_y=50

TWO_W_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="2W total growth registrations (RHS, % yoy)",
               x_axis_interval="24 month",legends_break =TRUE,
               data_unit='',graph_lim=60,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
               order_stack = TRUE,add_std_col = FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE)


TWO_W_title=TWO_W_chart[2][[1]]
TWO_W_chart=TWO_W_chart[1][[1]]
TWO_W_src=TWO_Sales_src
#HARD CODE::Long source not getting enough space in PPT
TWO_W_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"



## -------------------------------------
CV_Sales =data_query_clk_pg(id=724271)
CV_Sales_title=CV_Sales[2][[1]]
CV_Sales_src=CV_Sales[3][[1]]
CV_Sales=CV_Sales[1][[1]]
CV_Sales$Total=CV_Sales$Total/1000

# #CV | ELECTRIC(BOV)
cv_el =data_query_clk_pg(id=2214729)[1][[1]]
names(cv_el)[2]='Electric'
cv_el$Electric=cv_el$Electric/1000
CV_Sales=add_proxy_dates(CV_Sales,cv_el)
CV_Sales$Electric[is.na(CV_Sales$Electric)] <- 0

CV_Sales=CV_Sales %>%
           mutate(Total = ifelse(CV_Sales$Electric==0,Total, Total-Electric))


colnames(CV_Sales)=c("Relevant_Date","CV non-EV registrations (LHS, '000s)",
                      "CV EV registrations (LHS, '000s)")


#######################################################3W GROWTH RATE
CV_gr =data_query_clk_pg(id=724270,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(CV_gr)[2]='growth'

#HARD Code ::id 724270 having NA value upto Dec-2013
CV_gr=CV_gr[CV_gr$Relevant_Date>='2014-01-31',]
CV_Sales=CV_Sales[CV_Sales$Relevant_Date>=min(CV_gr$Relevant_Date),]
# CV_gr$growth=movavg(CV_gr$growth,3,t="s")



## -------------------------------------
data_s=CV_Sales
data_g=CV_gr


#
# num_brek=8
# max_pri_y=150
# min_pri_y=-20
# max_sec_y=150

num_brek=9
max_pri_y=125
min_pri_y=-40
max_sec_y=125

Com_Vehicle_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="CV registrations growth (RHS, % yoy)",
               x_axis_interval="24 month",custom_y_min = TRUE,
               data_unit='',graph_lim=60,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
               legends_break =TRUE,order_stack=TRUE,add_std_col = FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,line_type = 1)



Com_Vehicle_title=Com_Vehicle_chart[2][[1]]
Com_Vehicle_chart=Com_Vehicle_chart[1][[1]]
Com_Vehicle_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"
Com_Vehicle_chart


## -------------------------------------
TW_Sales =data_query_clk_pg(id=526220)
TW_Sales_title=TW_Sales[2][[1]]
TW_Sales_src=TW_Sales[3][[1]]
TW_Sales=TW_Sales[1][[1]]
TW_Sales$Total=TW_Sales$Total/1000

#CV | ELECTRIC(BOV)
tw_el =data_query_clk_pg(id=526402)[1][[1]]
names(tw_el)[2]='Electric'
tw_el$Electric=tw_el$Electric/1000
TW_Sales=add_proxy_dates(TW_Sales,tw_el)
TW_Sales$Electric[is.na(TW_Sales$Electric)] <- 0
TW_Sales=TW_Sales %>%
           mutate(Total = ifelse(TW_Sales$Electric==0,Total, Total-Electric))


colnames(TW_Sales)=c("Relevant_Date","3W non-EV registrations (LHS, '000s)",
                      "3W EV registrations (LHS, '000s)")

#######################################################3W GROWTH RATE
TW_gr =data_query_clk_pg(id=526270,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(TW_gr)[2]='growth'
#HARD Code ::id 526270 having NA value upto Dec-2013
TW_Sales=TW_Sales[TW_Sales$Relevant_Date>="2014-01-31",]
TW_gr=TW_gr[TW_gr$Relevant_Date>="2014-01-31",]
# TW_gr$growth=movavg(TW_gr$growth,3,t="s")



## -------------------------------------
data_s=TW_Sales
data_g=TW_gr

h_just=1.25
v_just=1.25
h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=120
min_pri_y=-40
max_sec_y=120

TW_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="3W registrations growth (RHS, % yoy)",
               x_axis_interval="24 month",custom_y_min = TRUE,
               data_unit='',graph_lim=25,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break =TRUE,order_stack=TRUE,add_std_col = FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,line_type = 1)


TW_title=TW_chart[2][[1]]
TW_chart=TW_chart[1][[1]]
#HARD CODE::Long source not getting enough space in PPT
TW_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"
TW_chart


## -------------------------------------
elec_demand=data_query_clk_pg(id=725975)
elec_demand_title=elec_demand[2][[1]]
elec_demand_src=elec_demand[3][[1]]
elec_demand1=elec_demand[1][[1]]
elec_demand1=elec_demand1[,c("Relevant_Date","Total")]

elec_demand_niif=data_query_clk_pg(817610,150)[1][[1]]
elec_demand_niif=elec_demand_niif[elec_demand_niif$Relevant_Date<min(elec_demand1$Relevant_Date),]
colnames(elec_demand_niif)=c("Relevant_Date","Total")
elec_demand1=rbind(elec_demand_niif,elec_demand1)
elec_demand1$Total=elec_demand1$Total/10^9

#E_WAY_gr
elec_demand_gr= data_query_clk_pg(id=725975)[1][[1]]
elec_demand_gr=elec_demand_gr[,c("Relevant_Date","Total_Growth")]
elec_demand_gr=elec_demand_gr[elec_demand_gr$Relevant_Date>(min(elec_demand_gr$Relevant_Date)+duration(1,"year")),]

elec_demand_gr_niif=data_query_clk_pg(817611,150)[1][[1]]
elec_demand_gr_niif=elec_demand_gr_niif[elec_demand_gr_niif$Relevant_Date<min(elec_demand_gr$Relevant_Date),]
colnames(elec_demand_gr_niif)=c("Relevant_Date","Total_Growth")
elec_demand_gr=rbind(elec_demand_gr_niif,elec_demand_gr)
names(elec_demand_gr)[2]="Electricity demand growth (RHS, % yoy)"

elec_demand1$Relevant_Date=as.Date(timeLastDayInMonth(elec_demand1$Relevant_Date))
elec_demand_gr$Relevant_Date=as.Date(timeLastDayInMonth(elec_demand_gr$Relevant_Date))



## -------------------------------------

data_s=elec_demand1
data_g=list(elec_demand_gr)

v_just=0.60
h_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=4
max_pri_y=150
min_pri_y=-50
max_sec_y=30

my_line_type=c("Electricity demand growth (RHS, % yoy)"="solid")
my_leg_col=c("Electricity demand growth (RHS, % yoy)"="Gray 48")

Mon_Elec_Demand_chart=line_bar_chart_niif(data_s,data_g,
                        sales_heading="Electricity demand (LHS, billion kWh)")

Mon_Elec_Demand_title=Mon_Elec_Demand_chart[2][[1]]
Mon_Elec_Demand_chart=Mon_Elec_Demand_chart[1][[1]]
Mon_Elec_Demand_src=elec_demand_src



## -------------------------------------
# Petroleum Products-Consumption
Petroleum_Con= data_query_clk_pg(id=318939)
Petroleum_Con_title=Petroleum_Con[2][[1]]
Petroleum_Con_src=Petroleum_Con[3][[1]]
Petroleum_Con=Petroleum_Con[1][[1]]
Petroleum_Con$Quantity_KMT=Petroleum_Con$Quantity_KMT/10^6

#Consumption Growth Rate
Petroleum_Con_gr=data_query_clk_pg(id=318921)[1][[1]]
names(Petroleum_Con_gr)[2]="Petroleum consumption (RHS, % yoy)"



## -------------------------------------
data_s=Petroleum_Con
data_g=list(Petroleum_Con_gr)

v_just=0.60
h_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=20
min_pri_y=-6
max_sec_y=20

my_line_type=c("Petroleum consumption (RHS, % yoy)"="solid")
my_leg_col=c("Petroleum consumption (RHS, % yoy)"="Gray 48")

Petroleum_Con_chart=line_bar_chart_niif(data_s,data_g,
                            sales_heading="Petroleum consumption (LHS, mn tonnes)")

Petroleum_Con_title=Petroleum_Con_chart[2][[1]]
Petroleum_Con_chart=Petroleum_Con_chart[1][[1]]
Petroleum_Con_src=Petroleum_Con_src



## -------------------------------------
##Consumption (Volume)
Diesel_Con= data_query_clk_pg(id=269911)
Diesel_Con_title=Diesel_Con[2][[1]]
Diesel_Con_src=Diesel_Con[3][[1]]
Diesel_Con=Diesel_Con[1][[1]]
Diesel_Con$Quantity_KMT=as.numeric(Diesel_Con$Quantity_KMT)/10^6


#Diesel Price
Diesel_price=data_query_clk_pg(id=725979)[1][[1]]
Diesel_price$Relevant_Date=as.Date(timeFirstDayInMonth(Diesel_price$Relevant_Date))


# Diesel_Con
Diesel_price=Diesel_price %>%
             tidyr::complete(Relevant_Date= seq(min(Relevant_Date), max(Relevant_Date), "1 month"))

Diesel_price=as.data.frame(Diesel_price)
Diesel_price$Price[is.na(Diesel_price$Price)] <- mean(Diesel_price$Price, na.rm = TRUE)
Diesel_price$Relevant_Date=as.Date(timeLastDayInMonth(Diesel_price$Relevant_Date))
d1=data.frame(date1=c(max(Diesel_price$Relevant_Date),max(Diesel_Con$Relevant_Date)))
common_min=min(as.Date(d1$date1))

Diesel_price=Diesel_price[Diesel_price$Relevant_Date<=common_min,]
Diesel_Con=Diesel_Con[Diesel_Con$Relevant_Date<=common_min,]
names(Diesel_price)[2]="Diesel prices-Delhi (RHS, INR/ltr)"


## -------------------------------------
data_s=Diesel_Con
data_g=list(Diesel_price)

h_just=0
v_just=0.20
h_just_line=0.50
v_just_line=0

num_brek=6
max_pri_y=12
min_pri_y=0
max_sec_y=100

my_line_type=c("Diesel prices-Delhi (RHS, INR/ltr)"="solid")
my_leg_col=c("Diesel prices-Delhi (RHS, INR/ltr)"="Gray 48")

Diesel_Con_price_chart=line_bar_chart_niif(data_s,data_g,
                             sales_heading="Diesel consumption (LHS, mn tonnes)")

Diesel_Con_price_title=Diesel_Con_price_chart[2][[1]]
Diesel_Con_price_chart=Diesel_Con_price_chart[1][[1]]
Diesel_Con_price_src=Diesel_Con_src
Diesel_Con_price_title
Diesel_Con_price_chart



## -------------------------------------
#Petroleum Products | Motor Spirit
Petrol_Consumption=data_query_clk_pg(id=269930)
Petrol_Consumption_title=Petrol_Consumption[2][[1]]
Petrol_Consumption_src=Petrol_Consumption[3][[1]]
Petrol_Consumption=Petrol_Consumption[1][[1]]
Petrol_Consumption$Quantity_KMT=Petrol_Consumption$Quantity_KMT/10^6


#Petrol Price | Delhi
Petrol_Pric_Delhi=data_query_clk_pg(id=725976)[1][[1]]
Petrol_Pric_Delhi$Relevant_Date=as.Date(timeFirstDayInMonth(Petrol_Pric_Delhi$Relevant_Date))
##HEARD_CODE
Petrol_Pric_Delhi=Petrol_Pric_Delhi%>%
        tidyr::complete(Relevant_Date= seq(min(Relevant_Date), max(Relevant_Date), "1 month"))

Petrol_Pric_Delhi=as.data.frame(Petrol_Pric_Delhi)
Petrol_Pric_Delhi$Price[is.na(Petrol_Pric_Delhi$Price)] <- mean(Petrol_Pric_Delhi$Price, na.rm = TRUE)
Petrol_Pric_Delhi$Relevant_Date=as.Date(timeLastDayInMonth(Petrol_Pric_Delhi$Relevant_Date))




d1=data.frame(date1=c(max(Petrol_Pric_Delhi$Relevant_Date),
                      max(Petrol_Consumption$Relevant_Date)))

common_min=min(as.Date(d1$date1))

Petrol_Pric_Delhi=Petrol_Pric_Delhi[Petrol_Pric_Delhi$Relevant_Date<=common_min,]
Petrol_Consumption=Petrol_Consumption[Petrol_Consumption$Relevant_Date<=common_min,]
names(Petrol_Pric_Delhi)[2]="Petrol prices-Delhi (RHS, INR/ltr)"



## -------------------------------------
data_s=Petrol_Consumption
data_g=list(Petrol_Pric_Delhi)

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.50

num_brek=5
max_pri_y=10
min_pri_y=0
max_sec_y=100

my_line_type=c("Petrol prices-Delhi (RHS, INR/ltr)"="solid")
my_leg_col=c("Petrol prices-Delhi (RHS, INR/ltr)"="Gray 48")

Petrol_Consumption_price_chart=line_bar_chart_niif(data_s,data_g,
                               sales_heading="Petrol consumption (LHS, mn tonnes)",
                               x_axis_interval="24 month",
                               round_integer=FALSE,
                               special_case=FALSE,
                               graph_lim=30,data_unit='',
                               WHITE_BACK=FALSE)

Petrol_Consumption_price_title=Petrol_Consumption_price_chart[2][[1]]
Petrol_Consumption_price_chart=Petrol_Consumption_price_chart[1][[1]]
Petrol_Consumption_price_src=Petrol_Consumption_src




## -------------------------------------
Water_Reservoir_Level= data_query_clk_pg(id=523077,exception=FALSE,surplus=FALSE,Water_Reservior=TRUE)
Water_Reservoir_Level_title=Water_Reservoir_Level[2][[1]]
Water_Reservoir_Level_src=Water_Reservoir_Level[3][[1]]
Water_Reservoir_Level=Water_Reservoir_Level[1][[1]]

#Reservoir Volume
Water_Reservoir_Level_gr= data_query_clk_pg(id=318929)[1][[1]]
names(Water_Reservoir_Level_gr)[2]="Water reservoir volume (RHS, % yoy)"



## -------------------------------------
data_s=Water_Reservoir_Level
data_g=list(Water_Reservoir_Level_gr)


h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75
text_size=30

num_brek=5
max_pri_y=200
min_pri_y=-20
max_sec_y=80

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1","GRAY 48")

my_line_type=c("Water reservoir volume (RHS, % yoy)"="solid")
my_leg_col=c("Water reservoir volume (RHS, % yoy)"="Gray 48")

Water_Reservoir_Level_chart=line_bar_chart_niif(data_s,data_g,
                  sales_heading="Current reservoir volume - All India (LHS, BCM)",
                  x_axis_interval="24 month")

Water_Reservoir_Level_title=Water_Reservoir_Level_chart[2][[1]]
Water_Reservoir_Level_chart=Water_Reservoir_Level_chart[1][[1]]


## -------------------------------------
#tractors registrationss
Domestic_Tractor_reg= data_query_clk_pg(id=526256)
Domestic_Tractor_reg_title=Domestic_Tractor_reg[2][[1]]
Domestic_Tractor_reg_src=Domestic_Tractor_reg[3][[1]]
Domestic_Tractor_reg=Domestic_Tractor_reg[1][[1]]
Domestic_Tractor_reg$Total=Domestic_Tractor_reg$Total/1000
#Registration Growth

Domestic_Tractor_gr= data_query_clk_pg(id=526344,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,
                                       VAHAN=TRUE)[1][[1]]

#HARD Code ::id 526344 having NA value upto Dec-2013
Domestic_Tractor_gr=Domestic_Tractor_gr[Domestic_Tractor_gr$Relevant_Date>="2014-01-01",]
Domestic_Tractor_reg=Domestic_Tractor_reg[Domestic_Tractor_reg$Relevant_Date>="2014-01-01",]

names(Domestic_Tractor_gr)[2]="Growth (RHS, % yoy)"



## -------------------------------------
data_s=Domestic_Tractor_reg
data_g=list(Domestic_Tractor_gr)



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=5
max_pri_y=100
min_pri_y=-50
max_sec_y=100

my_line_type=c("Growth (RHS, % yoy)"="solid")
my_leg_col=c("Growth (RHS, % yoy)"="Gray 48")

Domestic_Tractor_reg_chart=line_bar_chart_niif(data_s,data_g,
                   sales_heading="Domestic tractor registrations (LHS, '000 unit)")

Domestic_Tractor_reg_title=Domestic_Tractor_reg_chart[2][[1]]
Domestic_Tractor_reg_chart=Domestic_Tractor_reg_chart[1][[1]]
# Domestic_Tractor_reg_src=Domestic_Tractor_reg_src
#HARD CODE::Long source not getting enough space in PPT
Domestic_Tractor_reg_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"



## -------------------------------------
#All Scheduled Commercial Bank Deposits
SCB_Deposit= data_query_clk_pg(id=1443675)
SCB_Deposit_title=tolower(SCB_Deposit[2][[1]])
SCB_Deposit_src=SCB_Deposit[3][[1]]
SCB_Deposit=SCB_Deposit[1][[1]]
SCB_Deposit=SCB_Deposit[,c("Relevant_Date","Value")]


older_niif=data_query_clk_pg(1384198,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Value")
older_niif=older_niif[older_niif$Relevant_Date<min(SCB_Deposit$Relevant_Date),]
SCB_Deposit=rbind(older_niif,SCB_Deposit)
SCB_Deposit$Value=SCB_Deposit$Value/10^12

#GROWTH
SCB_Deposit_gr= data_query_clk_pg(id=1443676)[1][[1]]
older_niif=data_query_clk_pg(1384200,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth")
older_niif=older_niif[older_niif$Relevant_Date<min(SCB_Deposit_gr$Relevant_Date),]
SCB_Deposit_gr=rbind(older_niif,SCB_Deposit_gr)

SCB_Deposit_gr$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Deposit_gr$Relevant_Date))
names(SCB_Deposit_gr)[2]='Growth (RHS, % yoy)'
SCB_Deposit$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Deposit$Relevant_Date))

# SCB_Depo_gr_exc_hdfc=data_query_clk_pg(id=2232824)[1][[1]]
# SCB_Depo_gr_exc_hdfc$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Depo_gr_exc_hdfc$Relevant_Date))

# older_niif=data_query_clk_pg(1384200,150)[1][[1]]
# colnames(older_niif)=c("Relevant_Date", "growth")
# older_niif=older_niif[older_niif$Relevant_Date<min(SCB_Depo_gr_exc_hdfc$Relevant_Date),]
# SCB_Depo_gr_exc_hdfc=rbind(older_niif,SCB_Depo_gr_exc_hdfc)
# names(SCB_Depo_gr_exc_hdfc)[2]='growth excluding merger (RHS, % yoy)'


## -------------------------------------
data_s=SCB_Deposit
# data_g=list(SCB_Deposit_gr,SCB_Depo_gr_exc_hdfc)
data_g=list(SCB_Deposit_gr)


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.50

num_brek=8
max_pri_y=250
min_pri_y=0
max_sec_y=25

# my_line_type=c("Growth including merger (RHS, % yoy)"="dotted",
#                'Growth (RHS, % yoy)'='solid')
# my_leg_col=c("Growth including merger (RHS, % yoy)"="Gray 48",
#              'Growth (RHS, % yoy)'="Gray 48")

my_line_type=c('Growth (RHS, % yoy)'='solid')
my_leg_col=c('Growth (RHS, % yoy)'="Gray 48")


SCB_Deposit_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Bank deposit (INR, trillion)")
SCB_Deposit_title=SCB_Deposit_chart[2][[1]]
SCB_Deposit_chart=SCB_Deposit_chart[1][[1]]
SCB_Deposit_src=SCB_Deposit_src
SCB_Deposit_chart


## -------------------------------------
#All Scheduled Commercial Bank Credit
SCB_Credit= data_query_clk_pg(id=1443674)
SCB_Credit_title=tolower(SCB_Credit[2][[1]])
SCB_Credit_src=SCB_Credit[3][[1]]
SCB_Credit=SCB_Credit[1][[1]]
SCB_Credit_1=SCB_Credit[,c("Relevant_Date","Value")]


older_niif=data_query_clk_pg(1384197,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Value")

older_niif=older_niif[older_niif$Relevant_Date<min(SCB_Credit_1$Relevant_Date),]
SCB_Credit_1=rbind(older_niif,SCB_Credit_1)
SCB_Credit_1$Value=SCB_Credit_1$Value/10^12


#Growth
SCB_Credit_gr= data_query_clk_pg(id=1443677)
SCB_Credit_gr=SCB_Credit_gr[1][[1]]
SCB_Credit_gr=SCB_Credit_gr[,c("Relevant_Date","growth")]


older_niif=data_query_clk_pg(1384199,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date", "growth")
older_niif=older_niif[older_niif$Relevant_Date<min(SCB_Credit_gr$Relevant_Date),]
SCB_Credit_gr=rbind(older_niif,SCB_Credit_gr)

SCB_Credit_gr$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Credit_gr$Relevant_Date))
names(SCB_Credit_gr)[2]='Growth (RHS, % yoy)'

SCB_Credit_1$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Credit_1$Relevant_Date))
SCB_Credit_1=SCB_Credit_1[!duplicated(SCB_Credit_1[c("Relevant_Date")]), ]


# SCB_cre_gr_exc_hdfc=data_query_clk_pg(id=2232823)[1][[1]]
# SCB_cre_gr_exc_hdfc$Relevant_Date=as.Date(timeLastDayInMonth(SCB_cre_gr_exc_hdfc$Relevant_Date))
# older_niif=data_query_clk_pg(1384199,150)[1][[1]]
# colnames(older_niif)=c("Relevant_Date", "growth")
# older_niif=older_niif[older_niif$Relevant_Date<min(SCB_cre_gr_exc_hdfc$Relevant_Date),]
# SCB_cre_gr_exc_hdfc=rbind(older_niif,SCB_cre_gr_exc_hdfc)
# names(SCB_cre_gr_exc_hdfc)[2]='Growth excluding merger (RHS, % yoy)'



## -------------------------------------
data_s=SCB_Credit_1
# data_g=list(SCB_Credit_gr,SCB_cre_gr_exc_hdfc)
data_g=list(SCB_Credit_gr)

h_just=0.51
v_just=0.51
h_just_line=-1
v_just_line=1.5

num_brek=8
max_pri_y=250
min_pri_y=0
max_sec_y=25

# my_line_type=c("Growth including merger (RHS, % yoy)"="dotted",
#                'Growth (RHS, % yoy)'='solid')

# my_leg_col=c("Growth including merger (RHS, % yoy)"="Gray 48",
#              'Growth (RHS, % yoy)'="Gray 48")

my_line_type=c('Growth (RHS, % yoy)'='solid')
my_leg_col=c('Growth (RHS, % yoy)'="Gray 48")

SCB_Credit_chart=line_bar_chart_niif(data_s,data_g,
                 sales_heading="Bank credit (INR  trillion)",
                 round_integer = TRUE,
                 graph_lim = 100)


SCB_Credit_title=SCB_Credit_chart[2][[1]]
SCB_Credit_chart=SCB_Credit_chart[1][[1]]
SCB_Credit_src=SCB_Credit_src
SCB_Credit_chart
SCB_Credit_title
draw_key_line=draw_key_smooth


## -------------------------------------
# Monthly Tran
UPI_Trx_Volumes =data_query_clk_pg(id=739144)
UPI_Trx_Volumes_title=tolower(UPI_Trx_Volumes[2][[1]])
UPI_Trx_Volumes_src=UPI_Trx_Volumes[3][[1]]
UPI_Trx_Volumes=UPI_Trx_Volumes[1][[1]]

UPI_Trx_Volumes=UPI_Trx_Volumes[,c("Relevant_Date","Total_Volume")]
UPI_Trx_Volumes$Total_Volume=UPI_Trx_Volumes$Total_Volume/10^9

# Trx Values
UPI_Trx_values =data_query_clk_pg(id=739144)[1][[1]]
UPI_Trx_values=UPI_Trx_values[,c("Relevant_Date","Total_Value")]
UPI_Trx_values$Total_Value=UPI_Trx_values$Total_Value/10^12
names(UPI_Trx_values)[2]="Value (RHS, INR trillion)"


## -------------------------------------
data_s=UPI_Trx_Volumes
data_g=list(UPI_Trx_values)



h_just=1.5
v_just=0
h_just_line=0
v_just_line=0


num_brek=4
max_pri_y=15
min_pri_y=0
max_sec_y=25
my_line_type=c("Value (RHS, INR trillion)"="solid")
my_leg_col=c("Value (RHS, INR trillion)"="Gray 48")


UPI_Tran_chart=line_bar_chart_niif(data_s,data_g,
                         sales_heading="Volume (LHS, billion)",
                         x_axis_interval="12 month",round_integer=TRUE,
                         special_case=FALSE,graph_lim=30,data_unit='')


UPI_Tran_title=UPI_Tran_chart[2][[1]]
UPI_Tran_chart=UPI_Tran_chart[1][[1]]
UPI_Tran_src=UPI_Trx_Volumes_src
UPI_Tran_chart


## -------------------------------------
#In Circulation
Cash_Cir_org=data_query_clk_pg(id=2475763)
Cash_Cir_org_title=tolower(Cash_Cir_org[2][[1]])
Cash_Cir_org_src=Cash_Cir_org[3][[1]]
Cash_Cir_org=Cash_Cir_org[1][[1]]
Cash_Cir=Cash_Cir_org[,c("Relevant_Date","Total")]


older_niif=data_query_clk_pg(869227,137)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Cash_Cir$Relevant_Date),]
Cash_Cir=rbind(older_niif,Cash_Cir)
Cash_Cir$Total=Cash_Cir$Total/10^12

#HARD CODE ::NIIF taken from Feb-2020
Cash_Cir_gr=data_query_clk_pg(726003,137)[1][[1]]
Cash_Cir_gr=Cash_Cir_gr[Cash_Cir_gr$Relevant_Date>="2020-02-01",]

older_niif=data_query_clk_pg(878190,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth_Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Cash_Cir_gr$Relevant_Date),]
Cash_Cir_gr=rbind(older_niif,Cash_Cir_gr)
names(Cash_Cir_gr)[2]="Growth (RHS, % yoy)"



## -------------------------------------
data_s=Cash_Cir
data_g=list(Cash_Cir_gr)

special_case_2=TRUE


h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=40
min_pri_y=-20
max_sec_y=40

my_line_type=c("Growth (RHS, % yoy)"="solid")
my_leg_col=c("Growth (RHS, % yoy)"="Gray 48")

Cash_Cir_chart=line_bar_chart_niif(data_s,data_g,
                     sales_heading="Currency in circulation (LHS, INR trillion)")

Cash_Cir_title=Cash_Cir_chart[2][[1]]
Cash_Cir_chart=Cash_Cir_chart[1][[1]]
Cash_Cir_src=Cash_Cir_org_src
Cash_Cir_title
Cash_Cir_chart


## -------------------------------------
#RTGS_Tran_volume-RTGS Txn Volume
RTGS_Tran_vol=data_query_clk_pg(id=731129)
RTGS_Tran_vol_title=tolower(RTGS_Tran_vol[2][[1]])
RTGS_Tran_vol_src=RTGS_Tran_vol[3][[1]]
RTGS_Tran_vol=RTGS_Tran_vol[1][[1]]
RTGS_Tran_vol$Volume=RTGS_Tran_vol$Volume/10^6

#RTGS_Tran_value-RTGS Flow
RTGS_Tran_value=data_query_clk_pg(id=731134)[1][[1]]
RTGS_Tran_value$Value=RTGS_Tran_value$Value/10^12
names(RTGS_Tran_value)[2]="Value (RHS, INR trillion)"


## -------------------------------------
data_s=RTGS_Tran_vol
data_g=list(RTGS_Tran_value)



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=25
min_pri_y=1
max_sec_y=300

my_line_type=c("Value (RHS, INR trillion)"="solid")
my_leg_col=c("Value (RHS, INR trillion)"="Gray 48")

RTGS_Tran_chart=line_bar_chart_niif(data_s,data_g,
                                    sales_heading="Volume (LHS, mn)",
                                    round_integer = TRUE)

RTGS_Tran_title=RTGS_Tran_chart[2][[1]]
RTGS_Tran_chart=RTGS_Tran_chart[1][[1]]

RTGS_Tran_src=RTGS_Tran_vol_src




## -------------------------------------
# Credit_Card_Tran_volume-Credit Card Tran
Credit_Crd_Tran_vol=data_query_clk_pg(id=731108)
Credit_Crd_Tran_vol_title=tolower(Credit_Crd_Tran_vol[2][[1]])
Credit_Crd_Tran_vol_src=Credit_Crd_Tran_vol[3][[1]]
Credit_Crd_Tran_vol=Credit_Crd_Tran_vol[1][[1]]
Credit_Crd_Tran_vol$Volume=Credit_Crd_Tran_vol$Volume/10^6


#Credit_Card_Tran_value-Credit Card Spends
Credit_Card_Tran_value=data_query_clk_pg(id=731107)[1][[1]]
Credit_Card_Tran_value$Value=Credit_Card_Tran_value$Value/10^9
names(Credit_Card_Tran_value)[2]="Value (RHS, INR billion)"


## -------------------------------------
data_s=Credit_Crd_Tran_vol
data_g=list(Credit_Card_Tran_value)



h_just=0
v_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=7
max_pri_y=350
min_pri_y=0
max_sec_y=3500

my_line_type=c("Value (RHS, INR billion)"="solid")
my_leg_col=c("Value (RHS, INR billion)"="Gray 48")

Credit_Card_Tran_chart=line_bar_chart_niif(data_s,data_g,
                                           sales_heading="Volume (LHS, mn)")

Credit_Card_Tran_title=Credit_Card_Tran_chart[2][[1]]
Credit_Card_Tran_chart=Credit_Card_Tran_chart[1][[1]]

Credit_Card_Tran_src=Credit_Crd_Tran_vol_src



## -------------------------------------
#Net Purchase/Sale#US Dollar
Mon_net_pur_sale = data_query_clk_pg(id=808880)
Mon_net_pur_sale_title=Mon_net_pur_sale[2][[1]]
Mon_net_pur_sale_src=Mon_net_pur_sale[3][[1]]
Mon_net_pur_sale=Mon_net_pur_sale[1][[1]]

Mon_net_pur_sale=Mon_net_pur_sale[,c("Relevant_Date","Value")]
Mon_net_pur_sale$Value=Mon_net_pur_sale$Value/10^9
colnames(Mon_net_pur_sale)=c("Relevant_Date","Net purchase/(sale) of USD (LHS, USD billion)")
my_chart_col=re_arrange_columns(Mon_net_pur_sale)[[2]]
my_legends_col=my_chart_col
# Mon_net_pur_sale=Mon_net_pur_sale[,re_arrange_columns(Mon_net_pur_sale)[[1]]]

#######################################################
#average foreign exchange rate
Avg_Foreign_Exc_Rate_gr = data_query_clk_pg(id=808879)[1][[1]]




## -------------------------------------
data_s=list(Mon_net_pur_sale)
data_g=Avg_Foreign_Exc_Rate_gr

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

h_just=0.60
v_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=4
max_pri_y=20
min_pri_y=20
max_sec_y=100

Mon_purchase_sale_dollar_chart=stacked_bar_chart_niif(data_s,
                                                      x_axis_interval="12 month",
                                                      data_unit="",
                                                      graph_lim=90,bar_thick = 20,
                                                      negative=TRUE,SIDE_BAR=FALSE)

Mon_purchase_sale_dollar_title=Mon_purchase_sale_dollar_chart[2][[1]]

Mon_purchase_sale_dollar_chart=Mon_purchase_sale_dollar_chart[1][[1]]
Mon_purchase_sale_dollar_src=Mon_net_pur_sale_src
Mon_purchase_sale_dollar_chart


## -------------------------------------
#Monthly Performance
Mon_BSE_Sen = data_query_clk_pg(id=2303266)
Mon_BSE_Sen_src=Mon_BSE_Sen[3][[1]]
Mon_BSE_Sen=Mon_BSE_Sen[1][[1]]
names(Mon_BSE_Sen)[2]='Index'


#######################################################3W GROWTH RATE
Mon_BSE_Sen_gr = data_query_clk_pg(id=2314934)[1][[1]]
Mon_BSE_Sen_gr=Mon_BSE_Sen_gr[,c("Relevant_Date","growth")]
names(Mon_BSE_Sen_gr)[2]="BSE Sensex TTM returns (RHS, %)"


## -------------------------------------
data_s=Mon_BSE_Sen
data_g=list(Mon_BSE_Sen_gr)



h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=80000
min_pri_y=-20000
max_sec_y=80

my_line_type=c("BSE Sensex TTM returns (RHS, %)"="solid")
my_leg_col=c("BSE Sensex TTM returns (RHS, %)"="Gray 48")

Mon_BSE_Sen_perform_chart=line_bar_chart_niif(data_s,data_g,
                                              sales_heading="BSE Sensex (LHS)")

Mon_BSE_Sen_perform_title=Mon_BSE_Sen_perform_chart[2][[1]]
Mon_BSE_Sen_perform_chart=Mon_BSE_Sen_perform_chart[1][[1]]
Mon_BSE_Sen_perform_src=Mon_BSE_Sen_src



## -------------------------------------
#Monthly Performance
Mon_NSE_Ni = data_query_clk_pg(id=318865)
Mon_NSE_Ni_title=Mon_NSE_Ni[2][[1]]
Mon_NSE_Ni_src=Mon_NSE_Ni[3][[1]]
Mon_NSE_Ni=Mon_NSE_Ni[1][[1]]
Mon_NSE_Ni=Mon_NSE_Ni[,c("Relevant_Date","Value")]

#######################################################3W GROWTH RATE
Mon_NSE_Ni_gr = data_query_clk_pg(id=318890)[1][[1]]
Mon_NSE_Ni_gr=Mon_NSE_Ni_gr[,c("Relevant_Date","growth")]
names(Mon_NSE_Ni_gr)[2]="NSE Nifty 50 TTM returns (RHS, %)"


## -------------------------------------
data_s=Mon_NSE_Ni
data_g=list(Mon_NSE_Ni_gr)



num_brek=5
max_pri_y=25000
min_pri_y=-5000
max_sec_y=100

my_line_type=c("NSE Nifty 50 TTM returns (RHS, %)"="solid")
my_leg_col=c("NSE Nifty 50 TTM returns (RHS, %)"="Gray 48")

Mon_NSE_Nifty_perform_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="NSE Nifty 50 (LHS)")

Mon_NSE_Nifty_perform_title=Mon_NSE_Nifty_perform_chart[2][[1]]
Mon_NSE_Nifty_perform_chart=Mon_NSE_Nifty_perform_chart[1][[1]]
Mon_NSE_Nifty_perform_src=Mon_NSE_Ni_src



## -------------------------------------
#Avg.Daily Transaction Volume
etc_road_toll_col=data_query_clk_pg(id=745423)
etc_road_toll_col_title=etc_road_toll_col[2][[1]]
etc_road_toll_col_src=etc_road_toll_col[3][[1]]
etc_road_toll_col=etc_road_toll_col[1][[1]]
etc_road_toll_col$days=days_in_month(etc_road_toll_col$Relevant_Date)
etc_road_toll_col=etc_road_toll_col %>% mutate(Volume  =Volume*days)
etc_road_toll_col$Volume=etc_road_toll_col$Volume/10^6
etc_road_toll_col=etc_road_toll_col[,c("Relevant_Date","Volume")]


older_niif=data_query_clk_pg(817616,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Volume")
older_niif$Volume=older_niif$Volume/10^6
older_niif=older_niif[older_niif$Relevant_Date<min(etc_road_toll_col$Relevant_Date),]
etc_road_toll_col=rbind(older_niif,etc_road_toll_col)


#Avg. Daily Transaction Value-Settlements | Fastag
etc_road_toll_col_gr=data_query_clk_pg(id=745421)[1][[1]]
etc_road_toll_col_gr$days=days_in_month(etc_road_toll_col_gr$Relevant_Date)
etc_road_toll_col_gr=etc_road_toll_col_gr %>% mutate(Value =Value*days)
etc_road_toll_col_gr$Value =etc_road_toll_col_gr$Value/10^9
etc_road_toll_col_gr=etc_road_toll_col_gr[,c("Relevant_Date","Value")]

older_niif=data_query_clk_pg(817617,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Value")
older_niif$Value=older_niif$Value/10^9

older_niif=older_niif[older_niif$Relevant_Date<min(etc_road_toll_col_gr$Relevant_Date),]
etc_road_toll_col_gr=rbind(older_niif,etc_road_toll_col_gr)
names(etc_road_toll_col_gr)[2]="Value (RHS, INR billion)"



## -------------------------------------
data_s=etc_road_toll_col
data_g=list(etc_road_toll_col_gr)



h_just=0.55
v_just=0.55
h_just_line=1
v_just_line=0.50

num_brek=6
max_pri_y=350
min_pri_y=0
max_sec_y=70

my_line_type=c("Value (RHS, INR billion)"="solid")
my_leg_col=c("Value (RHS, INR billion)"="Gray 48")

Mon_etc_road_toll_col_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Volume (LHS, mn) ",
                                      x_axis_interval="12 month")

Mon_etc_road_toll_col_title=Mon_etc_road_toll_col_chart[2][[1]]
Mon_etc_road_toll_col_chart=Mon_etc_road_toll_col_chart[1][[1]]
Mon_etc_road_toll_col_src=etc_road_toll_col_src



## -------------------------------------
#Revenue Collected
ihmcl_toll_col=data_query_clk_pg(1836371)
ihmcl_toll_col_title=ihmcl_toll_col[2][[1]]
ihmcl_toll_col_src=ihmcl_toll_col[3][[1]]
ihmcl_toll_col_vol=ihmcl_toll_col[1][[1]]
ihmcl_toll_col_vol$Value=ihmcl_toll_col_vol$Value/10^6

##HARD CODE: july and Aug 2022 missing
# df1=data.frame(Relevant_Date=c('2022-07-31','2022-08-31'),
#                Value=c(0,0))

# ihmcl_toll_col_vol=rbind(ihmcl_toll_col_vol,df1)
#Traffic
ihmcl_toll_col_val=data_query_clk_pg(1836370)[1][[1]]
ihmcl_toll_col_val$Value=ihmcl_toll_col_val$Value/10^9
names(ihmcl_toll_col_val)[2]="Value (RHS, INR billion)"


## -------------------------------------
data_s=ihmcl_toll_col_vol
data_g=list(ihmcl_toll_col_val)

h_just=0.55
v_just=0.55
h_just_line=1
v_just_line=0.50

num_brek=6
max_pri_y=300
min_pri_y=0
max_sec_y=60

my_line_type=c("Value (RHS, INR billion)"="solid")
my_leg_col=c("Value (RHS, INR billion)"="Gray 48")


Mon_ihmcl_col_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="Volume (LHS, mn)",
                                x_axis_interval="6 month")

Mon_ihmcl_col_title=Mon_ihmcl_col_chart[2][[1]]
Mon_ihmcl_col_chart=Mon_ihmcl_col_chart[1][[1]]
Mon_ihmcl_col_src=ihmcl_toll_col_src
Mon_ihmcl_col_chart


## -------------------------------------
#Major Ports
crg_tfc_mjr_port=data_query_clk_pg(id=318937)
crg_tfc_mjr_port_title=crg_tfc_mjr_port[2][[1]]
crg_tfc_mjr_port_src=crg_tfc_mjr_port[3][[1]]
crg_tfc_mjr_port=crg_tfc_mjr_port[1][[1]]

crg_tfc_mjr_port_niif=data_query_clk_pg(817613,150)[1][[1]]
crg_tfc_mjr_port_niif=crg_tfc_mjr_port_niif[crg_tfc_mjr_port_niif$Relevant_Date<min(crg_tfc_mjr_port$Relevant_Date),]
names(crg_tfc_mjr_port_niif)[2]='Volume'

crg_tfc_mjr_port=rbind(crg_tfc_mjr_port_niif,crg_tfc_mjr_port)
crg_tfc_mjr_port$Relevant_Date=as.Date(timeLastDayInMonth(crg_tfc_mjr_port$Relevant_Date))


#Maritime Traffic | Minor Ports | J N P T
jnpt_port=data_query_clk_pg(id=1682023)[1][[1]]
jnpt_port$Relevant_Date=as.Date(timeLastDayInMonth(jnpt_port$Relevant_Date))
names(jnpt_port)[2]='Volume'
jnpt_port2=data_query_clk_pg(id=1526011)[1][[1]]
jnpt_port2=jnpt_port2[jnpt_port2$Relevant_Date>max(jnpt_port$Relevant_Date),]
names(jnpt_port2)[2]='Volume'
jnpt_port=rbind(jnpt_port,jnpt_port2)
df=crg_tfc_mjr_port[crg_tfc_mjr_port$Relevant_Date<min(jnpt_port$Relevant_Date),]
df['Volume']=0


data <- rbind(jnpt_port,df)
colnames(data)=c("Relevant_Date","JNPT")

crg_tfc_mjr_port1=merge(crg_tfc_mjr_port,data,by="Relevant_Date")
crg_tfc_mjr_port1=crg_tfc_mjr_port1 %>%
                  mutate(Volume = ifelse(crg_tfc_mjr_port1$JNPT==0,Volume, Volume-JNPT))

colnames(crg_tfc_mjr_port1)=c("Relevant_Date","Ports cargo traffic excl. JNPT (LHS, mn tonnes)","JNPT (LHS, mn tonnes)")


#Passenger Growth
crg_tfc_mjr_port_gr=data_query_clk_pg(id=318907)[1][[1]]
crg_tfc_mjr_port_gr=crg_tfc_mjr_port_gr[order(as.Date(crg_tfc_mjr_port_gr$Relevant_Date, format="%Y-%m-%d")), ]

crg_tfc_mjr_port_gr_niif=data_query_clk_pg(817614,150)[1][[1]]
crg_tfc_mjr_port_gr_niif=crg_tfc_mjr_port_gr_niif[crg_tfc_mjr_port_gr_niif$Relevant_Date<min(crg_tfc_mjr_port_gr$Relevant_Date),]
names(crg_tfc_mjr_port_gr_niif)[2]='Growth'

crg_tfc_mjr_port_gr=rbind(crg_tfc_mjr_port_gr_niif,crg_tfc_mjr_port_gr)
names(crg_tfc_mjr_port_gr)[2]='growth'
# crg_tfc_mjr_port_gr$growth=movavg(crg_tfc_mjr_port_gr$growth,3,t="s")
crg_tfc_mjr_port_gr$Relevant_Date=as.Date(timeLastDayInMonth(crg_tfc_mjr_port_gr$Relevant_Date))


## -------------------------------------
data_s=crg_tfc_mjr_port1
data_g=crg_tfc_mjr_port_gr
# BURLYWOOD 1
my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")

h_just=0
v_just=0.40
h_just_line=0
v_just_line=0.75

n_row=2
n_col=3
chart_label=8

num_brek=6
max_pri_y=80
min_pri_y=-20
max_sec_y=40

Mon_crg_tfc_mjr_port_chart=stacked_bar_line_chart_niif(data_s,data_g,
                             growth_heading="Ports cargo traffic (RHS, % yoy)",
                             x_axis_interval="24 month",
                             data_unit='',graph_lim=30,round_integer=TRUE,
                             Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
                             order_stack = TRUE,add_std_col = FALSE,
                             legends_break = TRUE,
                             GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE)


Mon_crg_tfc_mjr_port_title=Mon_crg_tfc_mjr_port_chart[2][[1]]
Mon_crg_tfc_mjr_port_chart=Mon_crg_tfc_mjr_port_chart[1][[1]]
Mon_crg_tfc_mjr_port_src=crg_tfc_mjr_port_src
Mon_crg_tfc_mjr_port_chart


## -------------------------------------
#Passengers Travelled
Mon_psgr_rail_tfc=data_query_clk_pg(1836374)
Mon_psgr_rail_tfc_title=Mon_psgr_rail_tfc[2][[1]]
Mon_psgr_rail_tfc_src=Mon_psgr_rail_tfc[3][[1]]
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[1][[1]]

#HARD CODE ::id 284860 is from Aug-2014
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[Mon_psgr_rail_tfc$Relevant_Date>="2014-07-31",]
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[!duplicated(Mon_psgr_rail_tfc[c("Relevant_Date")]), ]
colnames(Mon_psgr_rail_tfc)=c("Relevant_Date","Sleeper (LHS, million)","3AC (LHS, million)","Others (LHS, million)")
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[,c("Relevant_Date","Others (LHS, million)","3AC (LHS, million)","Sleeper (LHS, million)")]
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc%>%mutate(across(c(2:4), .fns = ~./10^6))


# #Railways | Sleeper Class
# rail_psn_slpr=data_query_clk_pg(id=284864)[1][[1]]
# names(rail_psn_slpr)[2]='Sleeper'
#
# #Railways | Second Class
# rail_psn_2nd_cls=data_query_clk_pg(id=284876)[1][[1]]
# names(rail_psn_2nd_cls)[2]='Second class'
#
# #Railways | Third Class AC
# rail_psn_3ac=data_query_clk_pg(id=284872)[1][[1]]
# names(rail_psn_3ac)[2]='3AC'
#
# #Railways | Second Class AC
# rail_psn_2ac=data_query_clk_pg(id=284870)[1][[1]]
# names(rail_psn_2ac)[2]='2AC'
#
# #Railways | Chair Car AC
# rail_psn_cc=data_query_clk_pg(id=284862)[1][[1]]
# names(rail_psn_cc)[2]='CC'
#
# rail_psn_cc=data_query_clk_pg(id=1836374)[1][[1]]


#

# rail_total=cbind(rail_psn_slpr,rail_psn_2nd_cls,rail_psn_3ac,
#               rail_psn_2ac,rail_psn_cc,by="Relevant_Date")
# rail_total=rail_total[,c("Relevant_Date","Sleeper","Second class","3AC","2AC","CC")]
#
# rail_total=rail_total %>%
#            mutate(Others=rowSums(.[setdiff(names(.),"Relevant_Date")]))
#
# rail_total=rail_total[rail_total$Relevant_Date>=min(Mon_psgr_rail_tfc$Relevant_Date),]
#
#
# Mon_psgr_rail_tfc=merge(Mon_psgr_rail_tfc,rail_total,by="Relevant_Date")
# Mon_psgr_rail_tfc=Mon_psgr_rail_tfc%>%mutate(across(c(2:8), .fns = ~./10^6))
# Mon_psgr_rail_tfc=Mon_psgr_rail_tfc %>%
#                  mutate(Others=Mon_psgr_rail_tfc$Total-Others)
#                  # mutate(Others=Mon_psgr_rail_tfc$Total-Mon_psgr_rail_tfc$'3AC'-Sleeper)
#
# Mon_psgr_rail_tfc <- subset(Mon_psgr_rail_tfc, select = -Total)
#
# Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[,c("Relevant_Date","Others","CC","2AC","3AC",
#                          "Second class","Sleeper")]
#

#Passenger Growth
Mon_psgr_rail_tfc_gr=data_query_clk_pg(id=284860)[1][[1]]
Mon_psgr_rail_tfc_gr=Mon_psgr_rail_tfc_gr[!duplicated(Mon_psgr_rail_tfc_gr[c("growth","Relevant_Date")]), ]
# Mon_psgr_rail_tfc_gr$growth=movavg(Mon_psgr_rail_tfc_gr$growth,3,t='s')



## -------------------------------------
data_s=Mon_psgr_rail_tfc
data_g=Mon_psgr_rail_tfc_gr


my_chart_col=c("Others (LHS, million)"="GRAY 48",
"3AC (LHS, million)"="DARK ORANGE 2",
"Sleeper (LHS, million)"="GOLDEN ROD 1")

my_legends_col=c("Others (LHS, million)"="GRAY 48",
"3AC (LHS, million)"="DARK ORANGE 2",
"Sleeper (LHS, million)"="GOLDEN ROD 1")




h_just=0
v_just=0.40
h_just_line=2.25
v_just_line=0.50

Position="left"
n_row=2
n_col=2
chart_label=8

num_brek=5
max_pri_y=100
min_pri_y=50
max_sec_y=100

Mon_psgr_rail_tfc=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="Passenger rail traffic (RHS, % yoy)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=30,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break = TRUE,top_column ="Others (LHS, million)",
               order_stack = TRUE,add_std_col = FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.75,JNPT=FALSE)


Mon_psgr_rail_tfc_title=Mon_psgr_rail_tfc[2][[1]]
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[1][[1]]
Mon_psgr_rail_tfc_src=Mon_psgr_rail_tfc_src
Mon_psgr_rail_tfc


## -------------------------------------
#Monthly Volume
Mon_rail_freight_tfc=data_query_clk_pg(id=284813)
Mon_rail_freight_tfc_title=Mon_rail_freight_tfc[2][[1]]
Mon_rail_freight_tfc_src=Mon_rail_freight_tfc[3][[1]]
Mon_rail_freight_to=Mon_rail_freight_tfc[1][[1]]



#Monthly Volume of Top 3 Categories
Mon_rail_freight_tfc=data_query_clk_pg(id=1836375)[1][[1]]
Mon_rail_freight_tfc=Mon_rail_freight_tfc %>%
              mutate(Others=Mon_rail_freight_to$Volume-Coal-Iron_Ore-Cement_Clinker)

colnames(Mon_rail_freight_tfc)=c("Relevant_Date",
                                 "Coal (LHS, mn tonnes)",
                                 "Iron ore (LHS, mn tonnes)",
                                 "Cement clinker (LHS, mn tonnes)","Others (LHS, mn tonnes)")

Mon_rail_freight_tfc=Mon_rail_freight_tfc[,c("Relevant_Date",
                                  "Others (LHS, mn tonnes)",
                                  "Cement clinker (LHS, mn tonnes)",
                                  "Iron ore (LHS, mn tonnes)",
                                  "Coal (LHS, mn tonnes)")]

# Mon_rail_freight_tfc=Mon_rail_freight_tfc[,re_arrange_columns(Mon_rail_freight_tfc,top_col ="Others (LHS, mn tonnes)")]


#growth by volume
Mon_rail_freight_tfc_gr= data_query_clk_pg(id=284801)[1][[1]]
# Mon_rail_freight_tfc_gr$growth=movavg(Mon_rail_freight_tfc_gr$growth,3,t="s")


## -------------------------------------
data_s=Mon_rail_freight_tfc
data_g=Mon_rail_freight_tfc_gr

h_just=0
v_just=0.40
h_just_line=2.25
v_just_line=1.50



my_chart_col=c("Others (LHS, mn tonnes)"="GRAY 48",
"Coal (LHS, mn tonnes)"="GOLDEN ROD 1",
"Iron ore (LHS, mn tonnes)"="DARK ORANGE 2",
"Cement clinker (LHS, mn tonnes)"="BURLYWOOD 1")

my_legends_col=c("Others (LHS, mn tonnes)"="GRAY 48",
"Coal (LHS, mn tonnes)"="GOLDEN ROD 1",
"Iron ore (LHS, mn tonnes)"="DARK ORANGE 2",
"Cement clinker (LHS, mn tonnes)"="BURLYWOOD 1")


n_row=4
n_col=1

num_brek=5
max_pri_y=150
min_pri_y=-50
max_sec_y=60
Position="left"
Mon_rail_freight_tfc_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="Rail freight traffic (RHS, % yoy)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=30,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break = TRUE,top_column = "Others (LHS, mn tonnes)",
               order_stack = TRUE,add_std_col = FALSE,
               led_position = "left",
               GST_SPECIAL=FALSE,key_spacing=0.75,JNPT=FALSE)


Mon_rail_freight_tfc_title=Mon_rail_freight_tfc_chart[2][[1]]
Mon_rail_freight_tfc_chart=Mon_rail_freight_tfc_chart[1][[1]]
Mon_rail_freight_tfc_src=Mon_rail_freight_tfc_src
Mon_rail_freight_tfc_chart



## -------------------------------------
#Avg Daily Passengers
Mon_dom_air_psn= data_query_clk_pg(id=515783)
Mon_dom_air_psn_title=Mon_dom_air_psn[2][[1]]
Mon_dom_air_psn_src=Mon_dom_air_psn[3][[1]]
Mon_dom_air_psn=Mon_dom_air_psn[1][[1]]
Mon_dom_air_psn$day=as.numeric(format(as.Date(timeLastDayInMonth(Mon_dom_air_psn$Relevant_Date),
                                              format="%Y-%m-%d"), format = "%d"))

# Mon_dom_air_psn$Total=Mon_dom_air_psn$Total*Mon_dom_air_psn$day
Mon_dom_air_psn=Mon_dom_air_psn[,c("Relevant_Date","Total")]
older_niif=data_query_clk_pg(894687,150)[1][[1]]
names(older_niif)[2]='Total'
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_dom_air_psn$Relevant_Date),]
Mon_dom_air_psn=rbind(older_niif,Mon_dom_air_psn)
Mon_dom_air_psn=Mon_dom_air_psn[Mon_dom_air_psn$Relevant_Date>=default_start_date,]
Mon_dom_air_psn$Total=Mon_dom_air_psn$Total/10^6


#Airlines | Domestic-Growth In Passengers
Mon_dom_air_psn_gr=data_query_clk_pg(id=515846)[1][[1]]

#HARD CODE ::NA value is there upto Dec-2015
Mon_dom_air_psn_gr=Mon_dom_air_psn_gr[Mon_dom_air_psn_gr$Relevant_Date>="2016-01-01",]
older_niif=data_query_clk_pg(894688,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Growth")


older_niif=older_niif[older_niif$Relevant_Date<min(Mon_dom_air_psn_gr$Relevant_Date),]
Mon_dom_air_psn_gr=rbind(older_niif,Mon_dom_air_psn_gr)

Mon_dom_air_psn_gr$Relevant_Date=as.Date(Mon_dom_air_psn_gr$Relevant_Date)
# Mon_dom_air_psn_gr$Growth=movavg(Mon_dom_air_psn_gr$Growth,3,t="s")
names(Mon_dom_air_psn_gr)[2]="Domestic air passenger (RHS, % yoy)"


## -------------------------------------
data_s=Mon_dom_air_psn
data_g=list(Mon_dom_air_psn_gr)



h_just=0.75
v_just=0.75
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=15
min_pri_y=-10
max_sec_y=40

my_line_type=c("Domestic air passenger (RHS, % yoy)"="solid")
my_leg_col=c("Domestic air passenger (RHS, % yoy)"="Gray 48")

Mon_dom_air_psns_chart=line_bar_chart_niif(data_s,data_g,
                         sales_heading="Domestic air passenger (LHS, million)")

Mon_dom_air_psns_title=Mon_dom_air_psns_chart[2][[1]]

Mon_dom_air_psns_chart=Mon_dom_air_psns_chart[1][[1]]

#Hard Coded
Mon_dom_air_psns_src=gsub('Current Month is Extrapolated','',gsub("\\(|\\)", '', Mon_dom_air_psn_src))
Mon_dom_air_psns_src="Source: Thurro, DGCA, Ministry of Civil Aviation, NIIF Research"
Mon_dom_air_psns_chart


## -------------------------------------
Mon_air_cargo_tfc= data_query_clk_pg(id=295110)
Mon_air_cargo_tfc_title=Mon_air_cargo_tfc[2][[1]]
Mon_air_cargo_tfc_src=Mon_air_cargo_tfc[3][[1]]
Mon_air_cargo_tfc=Mon_air_cargo_tfc[1][[1]]
# Mon_air_cargo_tfc$Value=Mon_air_cargo_tfc$Value/1000

older_niif=data_query_clk_pg(894691,150)[1][[1]]
names(older_niif)[2]='Volume'
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_air_cargo_tfc$Relevant_Date),]
Mon_air_cargo_tfc=rbind(older_niif,Mon_air_cargo_tfc)


Mon_air_cargo_tfc=Mon_air_cargo_tfc[Mon_air_cargo_tfc$Relevant_Date>=default_start_date,]
Mon_air_cargo_tfc$Relevant_Date=as.Date(Mon_air_cargo_tfc$Relevant_Date)
Mon_air_cargo_tfc$Volume=Mon_air_cargo_tfc$Volume/1000

#Growth in Inbound Volume
Mon_air_cargo_tfc_gr=data_query_clk_pg(id=295115)[1][[1]]

#HARD CODE ::NA is there upto Dec-2017
Mon_air_cargo_tfc_gr=Mon_air_cargo_tfc_gr[Mon_air_cargo_tfc_gr$Relevant_Date>="2018-01-31",]

older_niif=data_query_clk_pg(894689,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Growth")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_air_cargo_tfc_gr$Relevant_Date),]
Mon_air_cargo_tfc_gr=rbind(older_niif,Mon_air_cargo_tfc_gr)
Mon_air_cargo_tfc_gr$Relevant_Date=as.Date(Mon_air_cargo_tfc_gr$Relevant_Date)
# Mon_air_cargo_tfc_gr$Growth=movavg(Mon_air_cargo_tfc_gr$Growth,3,t="s")
names(Mon_air_cargo_tfc_gr)[2]="Air cargo traffic growth (RHS, % yoy)"



## -------------------------------------
data_s=Mon_air_cargo_tfc
data_g=list(Mon_air_cargo_tfc_gr)

h_just=0.60
v_just=0.60
h_just_line=0.60
v_just_line=0.60


num_brek=5
max_pri_y=400
min_pri_y=-100
max_sec_y=80

my_line_type=c("Air cargo traffic growth (RHS, % yoy)"="solid")
my_leg_col=c("Air cargo traffic growth (RHS, % yoy)"="Gray 48")

Mon_air_cargo_tfc_chart=line_bar_chart_niif(data_s,data_g,
                            sales_heading="Air cargo traffic (LHS, '000 tonnes)")


Mon_air_cargo_tfc_title=Mon_air_cargo_tfc_chart[2][[1]]
Mon_air_cargo_tfc_chart=Mon_air_cargo_tfc_chart[1][[1]]
Mon_air_cargo_tfc_src=Mon_air_cargo_tfc_src
Mon_air_cargo_tfc_chart



## -------------------------------------
#Average Daily Traded Volume
DAM_Traded_vol=data_query_clk_pg(id=273694)
DAM_Traded_vol_title=DAM_Traded_vol[2][[1]]
DAM_Traded_vol_src=DAM_Traded_vol[3][[1]]
DAM_Traded_vol=DAM_Traded_vol[1][[1]]

DAM_Traded_vol$days=days_in_month(DAM_Traded_vol$Relevant_Date)
DAM_Traded_vol=DAM_Traded_vol %>% mutate(Volume =Volume*days)
DAM_Traded_vol$Volume=DAM_Traded_vol$Volume/1000
DAM_Traded_vol=DAM_Traded_vol[,c("Relevant_Date","Volume")]

#Volume For NIIF
older_niif=data_query_clk_pg(1073311,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Volume")
older_niif=older_niif[older_niif$Relevant_Date<min(DAM_Traded_vol$Relevant_Date),]
older_niif$Volume=older_niif$Volume/10^9

DAM_Traded_vol=rbind(older_niif,DAM_Traded_vol)
DAM_Traded_vol$Relevant_Date=as.Date(DAM_Traded_vol$Relevant_Date)

#Clearing Price
DAM_Clearing_Price=data_query_clk_pg(id=273702)
DAM_Clearing_Price_title=DAM_Clearing_Price[2][[1]]
DAM_Clearing_Price_src=DAM_Clearing_Price[3][[1]]
DAM_Clearing_Price=DAM_Clearing_Price[1][[1]]
# DAM_Clearing_Price$Price=DAM_Clearing_Price$Price/1000

#Price for NIIF
older_niif=data_query_clk_pg(1073310,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Price")
older_niif=older_niif[older_niif$Relevant_Date<min(DAM_Clearing_Price$Relevant_Date),]
DAM_Clearing_Price=rbind(older_niif,DAM_Clearing_Price)
DAM_Clearing_Price$Relevant_Date=as.Date(DAM_Clearing_Price$Relevant_Date)
names(DAM_Clearing_Price)[2]="IEX prices (RHS, INR per kWh)"


## -------------------------------------
data_s=DAM_Traded_vol
data_g=list(DAM_Clearing_Price)

h_just=0
v_just=0
h_just_line=0
v_just_line=1

num_brek=5
max_pri_y=10
min_pri_y=0
max_sec_y=10

my_line_type=c("IEX prices (RHS, INR per kWh)"="solid")
my_leg_col=c("IEX prices (RHS, INR per kWh)"="Gray 48")

Mon_DAM_Clearing_Price_chart=line_bar_chart_niif(data_s,data_g,
                               sales_heading="IEX volume (LHS, billion kWh)")

Mon_DAM_Clearing_Price_title=Mon_DAM_Clearing_Price_chart[2][[1]]
Mon_DAM_Clearing_Price_chart=Mon_DAM_Clearing_Price_chart[1][[1]]
Mon_DAM_Clearing_Price_src=DAM_Clearing_Price_src
Mon_DAM_Clearing_Price_chart


## -------------------------------------
#Avg Daily Generation
Mon_elec_gen1=data_query_clk_pg(id=2046376)
Mon_elec_gen_title=Mon_elec_gen1[2][[1]]
Mon_elec_gen_src=Mon_elec_gen1[3][[1]]
Mon_elec_gen=Mon_elec_gen1[1][[1]]

Mon_elec_gen$day=as.numeric(format(as.Date(timeLastDayInMonth(Mon_elec_gen$Relevant_Date),
                                           format="%Y-%m-%d"),format = "%d"))

Mon_elec_gen$Total=Mon_elec_gen$Total*Mon_elec_gen$day
Mon_elec_gen$Total=Mon_elec_gen$Total/1000
Mon_elec_gen=Mon_elec_gen[,c("Relevant_Date","Total")]


older_niif=data_query_clk_pg(894693,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_elec_gen$Relevant_Date),]
older_niif$Total=older_niif$Total/10^9
Mon_elec_gen=rbind(older_niif,Mon_elec_gen)
Mon_elec_gen$Relevant_Date=as.Date(Mon_elec_gen$Relevant_Date)
Mon_elec_gen$Relevant_Date=as.Date(timeLastDayInMonth(Mon_elec_gen$Relevant_Date))

#HARD CODE: 270333 have missing data on 2018-11-30 so we are doing interpolation
# Mon_elec_gen=Mon_elec_gen %>%
#              tidyr::complete(Relevant_Date= seq(min(Relevant_Date), max(Relevant_Date), "1 month"))
#
# Mon_elec_gen=as.data.frame(Mon_elec_gen)
# Mon_elec_gen$Total[is.na(Mon_elec_gen$Total)] <- mean(Mon_elec_gen$Total, na.rm = TRUE)
# Mon_elec_gen$Relevant_Date=as.Date(timeLastDayInMonth(Mon_elec_gen$Relevant_Date))


#Avg Daily Growth
Mon_elec_gen_gr=data_query_clk_pg(id=2046283)[1][[1]]

#HARD CODE :: For id 1998768 we have taken from Nov-2019
Mon_elec_gen_gr=Mon_elec_gen_gr[Mon_elec_gen_gr$Relevant_Date>"2019-11-01",]

older_niif=data_query_clk_pg(894695,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_elec_gen_gr$Relevant_Date),]
Mon_elec_gen_gr=rbind(older_niif,Mon_elec_gen_gr)
Mon_elec_gen_gr$Relevant_Date=as.Date(timeLastDayInMonth(Mon_elec_gen_gr$Relevant_Date))
names(Mon_elec_gen_gr)[2]="Growth (RHS, yoy %)"


## -------------------------------------
data_s=Mon_elec_gen
data_g=list(Mon_elec_gen_gr)

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=175
min_pri_y=-50
max_sec_y=75

my_line_type=c("Growth (RHS, yoy %)"="solid")
my_leg_col=c("Growth (RHS, yoy %)"="Gray 48")

Mon_electricity_gen_chart=line_bar_chart_niif(data_s,data_g,
                        sales_heading="Electricity generation (LHS, billion kWh)")

Mon_electricity_gen_title=Mon_electricity_gen_chart[2][[1]]
Mon_electricity_gen_chart=Mon_electricity_gen_chart[1][[1]]
Mon_electricity_gen_src=Mon_elec_gen_src
Mon_electricity_gen_chart


## -------------------------------------
#Avg Daily Generation
gdp_const_pri=data_query_clk_pg(1502377)
gdp_const_pri_title=gdp_const_pri[2][[1]]
gdp_const_pri_src=gdp_const_pri[3][[1]]
gdp_const_pri=gdp_const_pri[1][[1]]
gdp_const_pri$Value=gdp_const_pri$Value/10^12

#Growth
gdp_const_pri_gr=data_query_clk_pg(1502376)[1][[1]]
gdp_const_pri_gr=gdp_const_pri_gr[gdp_const_pri_gr$Relevant_Date>default_start_date,]
names(gdp_const_pri_gr)[2]="Growth (RHS, % yoy)"


## -------------------------------------
data_s=gdp_const_pri
data_g=list(gdp_const_pri_gr)

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=4
max_pri_y=60
min_pri_y=-20
max_sec_y=15

my_line_type=c("Growth (RHS, % yoy)"="solid")
my_leg_col=c("Growth (RHS, % yoy)"="Gray 48")

Qtr_real_GDP_growth_chart=line_bar_chart_niif(data_s,data_g,
                                 sales_heading=paste0("GDP (LHS, INR trillion)"))

Qtr_real_GDP_growth_title=Qtr_real_GDP_growth_chart[2][[1]]
Qtr_real_GDP_growth_chart=Qtr_real_GDP_growth_chart[1][[1]]
Qtr_real_GDP_growth_src=gdp_const_pri_src
Qtr_real_GDP_growth_chart


## -------------------------------------
#Avg Daily Generation
services_trd=data_query_clk_pg(1662975)
services_trd_title=services_trd[2][[1]]
services_trd_src=services_trd[3][[1]]
services_trd=services_trd[1][[1]]
# services_trd$Total=services_trd$Total/10^12
services_trd$Relevant_Date=as.Date(timeLastDayInMonth(services_trd$Relevant_Date))

#HARD CODE :: id 525840 having data from Nov-2016
services_trd=services_trd[services_trd$Relevant_Date>="2016-10-30",]
#Growth
services_trd_gr=data_query_clk_pg(525840)[1][[1]]
services_trd_gr$Relevant_Date=as.Date(timeLastDayInMonth(services_trd_gr$Relevant_Date))
services_trd_gr=services_trd_gr[services_trd_gr$Relevant_Date>="2016-10-30",]
names(services_trd_gr)[2]="Growth (RHS, % yoy)"


## -------------------------------------
data_s=services_trd
data_g=list(services_trd_gr)

legend_key_width=0.27
h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=8
min_pri_y=-2
max_sec_y=40

my_line_type=c("Growth (RHS, % yoy)"="solid")
my_leg_col=c("Growth (RHS, % yoy)"="Gray 48")

Services_trd_chart=line_bar_chart_niif(data_s,data_g,
                                       sales_heading="Services trade (LHS, INR trillion)",
                                       x_axis_interval="24 month",round_integer=TRUE,
                                       special_case=FALSE,
                                       graph_lim=30,data_unit='',WHITE_BACK=FALSE,
                                       key_spacing=0)

Services_trd_title=Services_trd_chart[2][[1]]
Services_trd_chart=Services_trd_chart[1][[1]]
Services_trd_src=services_trd_src



## -------------------------------------
#Services | Commercial Real Estate
services_real_est=data_query_clk_pg(1662967)
services_real_est_title=services_real_est[2][[1]]
services_real_est_src=services_real_est[3][[1]]
services_real_est=services_real_est[1][[1]]
services_real_est$Relevant_Date=as.Date(timeLastDayInMonth(services_real_est$Relevant_Date))

#HARD CODE :: id 525832 having data from Nov-2016
services_real_est=services_real_est[services_real_est$Relevant_Date>="2016-10-01",]
services_real_est$Value=services_real_est$Value/10^12
#Growth
services_real_est_gr=data_query_clk_pg(525832)[1][[1]]
services_real_est_gr$Relevant_Date=as.Date(timeLastDayInMonth(services_real_est_gr$Relevant_Date))
names(services_real_est_gr)[2]="Growth (RHS, % yoy)"


## -------------------------------------
data_s=services_real_est
data_g=list(services_real_est_gr)



h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=4
min_pri_y=-1
max_sec_y=40
my_line_type=c("Growth (RHS, % yoy)"="solid")
my_leg_col=c("Growth (RHS, % yoy)"="Gray 48")
services_real_est_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="Services commercial real estate (LHS, INR trillion)",
                                x_axis_interval="24 month",round_integer=TRUE,
                                special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE)

services_real_est_title=services_real_est_chart[2][[1]]
services_real_est_chart=services_real_est_chart[1][[1]]
services_real_est_src=services_real_est_src




## -------------------------------------
#Agriculture,forestry and fishing
GVA_act= data_query_clk_pg(1502378)
GVA_act_title=GVA_act[2][[1]]
GVA_act_src=GVA_act[3][[1]]
GVA_act=GVA_act[1][[1]]
GVA_act$Value=GVA_act$Value/10^12
#GVA growth
GVA_total= data_query_clk_pg(1502379)[1][[1]]
colnames(GVA_total) = c("Relevant_Date","GVA")
GVA_total=GVA_total[GVA_total$Relevant_Date>default_start_date,]
names(GVA_total)[2]="GVA (RHS,  % yoy)"


## -------------------------------------
data_s=GVA_act
data_g=list(GVA_total)

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=60
min_pri_y=-20
max_sec_y=15
my_line_type=c("GVA (RHS,  % yoy)"="solid")
my_leg_col=c("GVA (RHS,  % yoy)"="Gray 48")

Qtr_real_GVA_gr_chart=line_bar_chart_niif(data_s,data_g,
                                    sales_heading="GVA (LHS, INR trillion)",
                                    x_axis_interval="24 month",round_integer=TRUE,
                                          special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE)

Qtr_real_GVA_gr_title=Qtr_real_GVA_gr_chart[2][[1]]
Qtr_real_GVA_gr_chart=Qtr_real_GVA_gr_chart[1][[1]]
Qtr_real_GVA_gr_src=GVA_act_src


## -------------------------------------
#Telecom (Wireless + Wireline)
#Total Subscribers
telecom_t= data_query_clk_pg(1669458,exception=TRUE)
telecom_t_src=telecom_t[3][[1]]
telecom_t=telecom_t[1][[1]]



telecom_t=telecom_t[telecom_t$Relevant_Date>=default_start_date,]
telecom_t$Value=telecom_t$Value/10^6
colnames(telecom_t) = c("Relevant_Date","telecom")

#Telecom | Internet
tele_int= data_query_clk_pg(2447234,exception=TRUE)[1][[1]]

tele_int=tele_int[tele_int$Relevant_Date>=default_start_date,]
tele_int$Total=tele_int$Total/10^6
colnames(tele_int) = c("Relevant_Date","internet")
names(tele_int)[2]="Internet subscribers"


## -------------------------------------
data_s=telecom_t
data_g=list(tele_int)


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=1500
min_pri_y=-20
max_sec_y=1500

my_line_type=c("Internet subscribers"="solid")
my_leg_col=c("Internet subscribers"="Gray 48")

Qtr_tele_sub_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Telecom subscribers",
                                      x_axis_interval="24 month",round_integer=TRUE,
                                      special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE,bar_thick=40)

Qtr_tele_sub_title=Qtr_tele_sub_chart[2][[1]]
Qtr_tele_sub_chart=Qtr_tele_sub_chart[1][[1]]

Qtr_tele_sub_src=telecom_t_src


## ----eval=FALSE, include=FALSE--------
## #World | Exports | Services
## gbl_ser= data_query_clk_pg(2158074,year=TRUE,set_perid=40)
## gbl_ser_src=gbl_ser[3][[1]]
## gbl_ser=gbl_ser[1][[1]]
## gbl_ser$Value=gbl_ser$Value/10^12
##
##
##
## #World | Exports | Modern Services
## gbl_mdrn_ser= data_query_clk_pg(2158073,year= TRUE,set_perid=40)[1][[1]]
## colnames(gbl_mdrn_ser) = c("Relevant_Date","growth")
## names(gbl_mdrn_ser)[2]="Modern services share (RHS, %)"


## ----eval=FALSE, include=FALSE--------
## data_s=gbl_ser
## data_g=list(gbl_mdrn_ser)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=10
## min_pri_y=0
## max_sec_y=100
##
## my_line_type=c("Modern services share (RHS, %)"="solid")
## my_leg_col=c("Modern services share (RHS, %)"="Gray 48")
##
## global_ser_chart=line_bar_chart_niif(data_s,data_g,
##                                       sales_heading="Global services (LHS, trillion)",
##                                       x_axis_interval="12 month",round_integer=TRUE,
##                                       format_date =paste0("%Y"),
##                                       show_older = TRUE,x_angle1=0,
##                                       special_case=FALSE,graph_lim=300,
##                                       data_unit='',WHITE_BACK=FALSE,
##                                       bar_thick=200)
##
## global_ser_title=global_ser_chart[2][[1]]
## global_ser_chart=global_ser_chart[1][[1]]
## global_ser_src=gbl_ser_src
## global_ser_chart


## ----eval=FALSE, include=FALSE--------
## #World | Exports | Services
## india_ser= data_query_clk_pg(2158079,year=TRUE,set_perid=40)
## india_ser_src=india_ser[3][[1]]
## india_ser=india_ser[1][[1]]
## india_ser$Value=india_ser$Value/10^12
##
## #World | Exports | Modern Services
## india_mdrn_ser= data_query_clk_pg(2158080,year= TRUE,set_perid=40)[1][[1]]
## colnames(india_mdrn_ser) = c("Relevant_Date","growth")
## names(india_mdrn_ser)[2]="Modern services share (RHS, %)"


## ----eval=FALSE, include=FALSE--------
## data_s=india_ser
## data_g=list(india_mdrn_ser)
##
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=0.5
## min_pri_y=0
## max_sec_y=100
##
## my_line_type=c("Modern services share (RHS, %)"="solid")
## my_leg_col=c("Modern services share (RHS, %)"="Gray 48")
##
## india_ser_chart=line_bar_chart_niif(data_s,data_g,
##                                       sales_heading="India services (LHS, trillion)",
##                                       x_axis_interval="12 month",round_integer=FALSE,
##                                       special_case=FALSE,graph_lim=300,
##                                       format_date =paste0("%Y"),
##
##                                       x_angle=0,
##                                       show_older = TRUE,
##                                       data_unit='',WHITE_BACK=FALSE,bar_thick=200)
##
## india_ser_title=india_ser_chart[2][[1]]
## india_ser_chart=india_ser_chart[1][[1]]
##
## india_ser_src=india_ser_src
## india_ser_chart


## ----eval=FALSE, include=FALSE--------
## # This chart is no longer required
## # #Bit coin
## # id=1525072
## # Bitcoin= data_query_clk_pg(id)
## # Bitcoin_title=Bitcoin[2][[1]]
## # Bitcoin_src=Bitcoin[3][[1]]
## # Bitcoin=Bitcoin[1][[1]]
## # Bitcoin$Value=Bitcoin$Value/1000
##
## #
## # #Etherium
## # id=1525074
## # Etherium= data_query_clk_pg(id)[1][[1]]
## # Etherium$Value=Etherium$Value/1000
## # #HARD_CODE
## # Etherium=Etherium[Etherium$Relevant_Date>="2018-01-01",]
## # Bitcoin=Bitcoin[Bitcoin$Relevant_Date>"2018-01-01",]


## ----eval=FALSE, include=FALSE--------
## # data_s=Bitcoin
## # data_g=Etherium
## #
## # sales_heading="Bitcoin (LHS)"
## # growth_heading="Etherium (RHS)"
## #
## # h_just=0
## # v_just=0.75
## # h_just_line=0
## # v_just_line=0.75
## #
## # num_brek=5
## # max_pri_y=80
## # min_pri_y=0
## # max_sec_y=8
## #
## # Daily_crypto_price_chart=line_bar_chart_niif(data_s,data_g,sales_heading,growth_heading,
## #       x_axis_interval="24 month",round_integer=FALSE,special_case=TRUE,
## #       graph_lim=30,data_unit='')
## #
## # Daily_crypto_price_title=Daily_crypto_price_chart[2][[1]]
## # Daily_crypto_price_chart=Daily_crypto_price_chart[1][[1]]
## # Daily_crypto_price_src=Bitcoin_src
##
##


## -------------------------------------
#World Bank Commodity | Copper
Mon_Cu_pri= data_query_clk_pg(id=723144)
Mon_Cu_pri_title=Mon_Cu_pri[2][[1]]
Mon_Cu_pri_src=Mon_Cu_pri[3][[1]]
Mon_Cu_pri=Mon_Cu_pri[1][[1]]
Mon_Cu_pri$Total=Mon_Cu_pri$Total


#World Bank Commodity | Iron ore, cfr spot
Monthly_Iron_ore_prices= data_query_clk_pg(id=723155)[1][[1]]
names(Monthly_Iron_ore_prices)[2]="Iron ore, 62% fine CFR Qingdao (USD/dry metric ton)"



## -------------------------------------
data_s=Mon_Cu_pri
data_g=list(Monthly_Iron_ore_prices)



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=15000
min_pri_y=0
max_sec_y=300

my_line_type=c("Iron ore, 62% fine CFR Qingdao (USD/dry metric ton)"="solid",
               "Copper (USD/tonne)"="solid")
my_leg_col=c("Iron ore, 62% fine CFR Qingdao (USD/dry metric ton)"="Gray 48",
             "Copper (USD/tonne)"="GOLDEN ROD 1")

Mon_Cu_Fe_ore_prices_WB_42_chart=line_bar_chart_niif(data_s,data_g,
             sales_heading="Copper (USD/tonne)",
             x_axis_interval="24 month",
             round_integer=FALSE,
             special_case=TRUE,graph_lim=30,data_unit='')

Mon_Cu_Fe_ore_prices_WB_42_title=Mon_Cu_Fe_ore_prices_WB_42_chart[2][[1]]
Mon_Cu_Fe_ore_prices_WB_42_chart=Mon_Cu_Fe_ore_prices_WB_42_chart[1][[1]]
Mon_Cu_Fe_ore_prices_WB_42_src=Mon_Cu_pri_src



## -------------------------------------
#  Foreign Exchange
Forex_resr = data_query_clk_pg(id=725953)
Forex_resr_title=tolower(Forex_resr[2][[1]])
Forex_resr_src=Forex_resr[3][[1]]
Forex_resr=Forex_resr[1][[1]]
Forex_resr$Total=Forex_resr$Total/10^9

Forex_import_cover = data_query_clk_pg(id=724009)
Forex_import_cover_title=tolower(Forex_import_cover[2][[1]])
Forex_import_cover_src=Forex_import_cover[3][[1]]
Forex_import_cover=Forex_import_cover[1][[1]]



## -------------------------------------
if (max(Forex_import_cover$Relevant_Date)<max(Forex_resr$Relevant_Date)){
    com_date=as.Date(min(max(Forex_import_cover$Relevant_Date),max(Forex_resr$Relevant_Date)))
    missing_mon=subset(Forex_resr,Forex_resr$Relevant_Date>as.Date(com_date))
    mer_import_avg=data_query_clk_pg(1502371)[1][[1]]

    mer_import_avg$Relevant_Date=as.Date(timeFirstDayInMonth(mer_import_avg$Relevant_Date))
    mer_import_avg=subset(mer_import_avg,mer_import_avg$Relevant_Date<=com_date)
    mer_import_avg=subset(mer_import_avg,mer_import_avg$Relevant_Date>=as.Date(com_date %m-% months(5)))
    mer_import_avg=mean(mer_import_avg$Value)

    ser_import_avg=data_query_clk_pg(1502360)[1][[1]]
    ser_import_avg$Relevant_Date=as.Date(timeFirstDayInMonth(ser_import_avg$Relevant_Date))
    ser_import_avg=subset(ser_import_avg,ser_import_avg$Relevant_Date<=com_date)
    ser_import_avg=subset(ser_import_avg,ser_import_avg$Relevant_Date>=as.Date(com_date %m-% months(5)))
    ser_import_avg=mean(ser_import_avg$Value)

    Forex_import_cover1=Forex_resr[Forex_resr$Relevant_Date>com_date,'Total']*10^9/sum(ser_import_avg+mer_import_avg)

    d1=data.frame(Relevant_Date=as.Date(c(missing_mon$Relevant_Date)),Total=Forex_import_cover1)
    Forex_import_cover=rbind(Forex_import_cover,d1)
}
names(Forex_import_cover)[2]='Forex to import cover (RHS, months)'


## -------------------------------------
data_s=Forex_resr
data_g=list(Forex_import_cover)

h_just=0
v_just=0.60
h_just_line=0.50
v_just_line=0.50

num_brek=5
max_pri_y=800
min_pri_y=0
max_sec_y=20

my_line_type=c("Forex to import cover (RHS, months)"="dashed")
my_leg_col=c("GOLDEN ROD 1","Gray 48")

Mon_Forex_Reserve_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="Forex reserves (LHS, USD billion)",
                                x_axis_interval="24 month",round_integer=FALSE,
                                special_case=TRUE,graph_lim=30,data_unit='',
                                WHITE_BACK=FALSE,show_fu_dt=TRUE,
                                key_spacing=0.10,DATE_HEADER=TRUE)

Mon_Forex_Reserve_title=Mon_Forex_Reserve_chart[2][[1]]
Mon_Forex_Reserve_chart=Mon_Forex_Reserve_chart[1][[1]]
Mon_Forex_Reserve_src=Forex_resr_src
Mon_Forex_Reserve_chart
Mon_Forex_Reserve_title


## -------------------------------------
#Product Wise
Mon_ferti_Sales=data_query_clk_pg(id=318858)
Mon_ferti_Sales_title=Mon_ferti_Sales[2][[1]]
Mon_ferti_Sales_src=Mon_ferti_Sales[3][[1]]
Mon_ferti_Sales=Mon_ferti_Sales[1][[1]]

Mon_ferti_Sales=Mon_ferti_Sales%>%mutate(across(c(2:5), .fns = ~./10^6))

Mon_ferti_Sales_niif=data_query_clk_pg(817604,150)[1][[1]]
Mon_ferti_Sales_niif=Mon_ferti_Sales_niif[Mon_ferti_Sales_niif$Relevant_Date<min(Mon_ferti_Sales$Relevant_Date),]
Mon_ferti_Sales=rbind(Mon_ferti_Sales_niif,Mon_ferti_Sales)

Mon_ferti_Sales=Mon_ferti_Sales[,c("Relevant_Date","NPK","MOP","DAP","UREA")]
colnames(Mon_ferti_Sales)=c("Relevant_Date","NPK ","MOP ","DAP ","UREA ")

#Sales Growth
Mon_ferti_gr=data_query_clk_pg(id=268609)[1][[1]]
Mon_ferti_gr_niif=data_query_clk_pg(817605,150)[1][[1]]
Mon_ferti_gr_niif=Mon_ferti_gr_niif[Mon_ferti_gr_niif$Relevant_Date<min(Mon_ferti_gr$Relevant_Date),]
names(Mon_ferti_gr_niif)[2]='Growth'
Mon_ferti_gr=rbind(Mon_ferti_gr_niif,Mon_ferti_gr)
names(Mon_ferti_gr)[2]='growth'



## -------------------------------------
data_s=Mon_ferti_Sales
data_g=Mon_ferti_gr

my_chart_col=c("DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1","GRAY 88")
my_legends_col=c("GOLDEN ROD 1","GRAY 96","BURLYWOOD 1","DARK ORANGE 2","GRAY GRAY 96")

h_just=0
v_just=0.75
h_just_line=2.25
v_just_line=0.50


n_row=2
n_col=3
chart_label=8


num_brek=4
max_pri_y=10
min_pri_y=-5
max_sec_y=60

Mon_ferti_Sales_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                 growth_heading="Growth (RHS, % yoy)",
                                                 x_axis_interval="12 month",
                                                 data_unit='',graph_lim=30,
                                                 round_integer=TRUE,
                                                 Exception=FALSE,
                                                 SIDE_BAR=FALSE,negative=FALSE,
                                                 GST_SPECIAL=FALSE)

Mon_ferti_Sales_title=Mon_ferti_Sales_chart[2][[1]]
Mon_ferti_Sales_chart=Mon_ferti_Sales_chart[1][[1]]
Mon_ferti_Sales_src=Mon_ferti_Sales_src



## -------------------------------------
#New Payroll Additions
Mon_enrl_epfo= data_query_clk_pg(id=808266)
Mon_enrl_epfo_title=Mon_enrl_epfo[2][[1]]
Mon_enrl_epfo_src=Mon_enrl_epfo[3][[1]]
# Mon_enrl_epfo=Mon_enrl_epfo[1][[1]]
# Mon_enrl_epfo$Total=Mon_enrl_epfo$Total/10^6
# colnames(Mon_enrl_epfo)=c("Relevant_Date","EPFO payroll (LHS, mn)")

# #ESIC Subscribers Addition For NIIF
# ESIC_Scbr= data_query_clk_pg(id=1384190)[1][[1]]
# ESIC_Scbr$Total=ESIC_Scbr$Total/10^6
# colnames(ESIC_Scbr)=c('Relevant_Date','ESIC subscriber (LHS, mn)')
# ESIC_NPS=merge(ESIC_Scbr,Mon_enrl_epfo,by="Relevant_Date")

epfo=data_query_clk_pg(1836413,year =TRUE)[1][[1]]
Epfo_esic=epfo[,c("Relevant_Date",'EPFO','ESIC')]
colnames(Epfo_esic)=c('Relevant_Date',"EPFO payroll (LHS, mn)",'ESIC subscriber (LHS, mn)')
Epfo_esic=Epfo_esic%>%mutate(across(c(2:3), .fns = ~./10^6))

#NPS Subscribers Added For NIIF
NPS_Subscribers= data_query_clk_pg(id=1836413)[1][[1]]
NPS_Subscribers=epfo[,c("Relevant_Date",'NPS')]
colnames(NPS_Subscribers)=c("Relevant_Date","growth")
NPS_Subscribers$growth=NPS_Subscribers$growth/1000


## -------------------------------------
data_s=Epfo_esic
data_g=NPS_Subscribers

my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48","TAN 1")
SIDE_BAR=TRUE
Exception=TRUE


h_just=0.40
v_just=-1.5
h_just_line=-5
v_just_line=-2

n_row=1
n_col=3


num_brek=4
max_pri_y=20
min_pri_y=0
max_sec_y=1000

Mon_enrollment_num_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                 growth_heading="NPS subscribers (RHS, 000s)",
                                 x_axis_interval="12 month",
                                 my_pattern=c('stripe','stripe'),
                                 data_unit='',graph_lim=600,
                                 round_integer=FALSE,Exception=TRUE,
                                 SIDE_BAR=TRUE,ptrn_den=0.1,ptrn_spc=0.02,
                                 negative=FALSE,GST_SPECIAL=FALSE,
                                 YTD =TRUE,show_shaded=TRUE,bar_thick=300,
                                 expand_x=c(0,0),single_bar_width=100)


Mon_enrollment_num_title=Mon_enrollment_num_chart[2][[1]]
Mon_enrollment_num_chart=Mon_enrollment_num_chart[1][[1]]
Mon_enrollment_num_src=Mon_enrl_epfo_src
Mon_enrollment_num_chart
bar_thick=12


## ----eval=FALSE, include=FALSE--------
## data1=Epfo_esic
## data2=NPS_Subscribers
## data1$Relevant_Date <- as.Date(data1$Relevant_Date)
## data1 <- data1[order(data1$Relevant_Date),]
##
## data1$Relevant_Date=as.Date(timeLastDayInMonth(data1$Relevant_Date))
## data2$Relevant_Date=as.Date(timeLastDayInMonth(data2$Relevant_Date))
##
## data_final=merge(data1, data2, by="Relevant_Date")
## data_final= reshape2::melt(data_final,id=c("Relevant_Date","growth"))
##
## data_final$Month <-as.Date(data_final$Relevant_Date)
## data_final=data_final %>%
##             rename(value_y_right=growth,
##                    category=variable,
##                    value_y_left=value)
##
## data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))
##
##
## data_final=na.omit(data_final)
## data_final=data_final[order(data_final$Relevant_Date),]
## data_final=data_final[data_final$Relevant_Date>=default_start_date,]
## prev_month=as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
## print(prev_month)
## data_final=data_final[data_final$Relevant_Date<=prev_month,]
## print(data_final)
## data_ends <- data_final %>% filter(Month == Month[length(Month)])
## data_final1=data_final[data_final$Relevant_Date<max(data_ends$Relevant_Date),]
## data_ends1=data_final[data_final$Relevant_Date>=max(data_final1$Relevant_Date),]


## ----eval=FALSE, include=FALSE--------
## library(ggpattern)
## install.packages('magick')
## bar_position='stack'
## ggplot(data=data_final,
##        aes(x=Month,y=value_y_left,
##                    pattern_type = category, pattern_fill = category))+
##
##
##     geom_col_pattern(
##     pattern              = 'magick',
##     width                = 1,
##     stat                 = "identity",
##     fill                 = 'white',
##     colour               = 'black',
##     pattern_scale        = 3,
##     pattern_aspect_ratio = 1,
##     pattern_key_scale_factor = 1.5,inherit.aes = TRUE
##   )+
##    scale_pattern_filename_discrete(choices= c("stripe", "crosshatch", "circle"))
##


## ----eval=FALSE, include=FALSE--------
## ggplot(data=data_final)+
##       geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
##                         position=bar_position,
##                         width = bar_thick) +
##
##       scale_fill_manual(values=my_chart_col)+
##
##       scale_colour_manual(values=c("GRAY 48"),
##                           guide = guide_legend(override.aes = list(linetype = c("solid"))))+
##
##       scale_linetype_manual(values=c("solid"))+
##


## ----eval=FALSE, include=FALSE--------
## #p <- ggplot(data=data_final,
## # aes(x=Month,y=value_y_left))+
## growth_heading="NPS subscribers (RHS, 000s)"
## bar_thick=40
## bar_position='dodge2'
## # position = "stack",
## Mon_enrollment_num_chart=ggplot(data=data_final1)+
##     geom_bar(aes(x=Month,y=(value_y_left),
##                  fill=category),
##                  stat="identity",
##                  position=bar_position,
##                  width = bar_thick) +
##
##     scale_fill_manual(values=my_chart_col)+
##     geom_line(aes(x=Month,y=(value_y_right/50),
##                       color=paste0(growth_heading)),
##                       size=line_thick,linetype =1,group=1)+
##
##
##    geom_line(data=data_ends1,
##      aes(x=Month,y=(value_y_right/50),
##                       color=paste0(growth_heading)),
##                       size=line_thick,linetype =2,group=1)+
##   geom_col_pattern(
##     data=data_ends,
##     aes(x=Month,y=value_y_left,
##            pattern_type=category,
##            pattern_fill = category,
##       pattern_filename = category
##     ),
##     width = bar_thick,
##     show.legend = FALSE,
##     position = bar_position,
##
##     pattern         = 'stripe',
##     pattern_type    = 'none',
##     fill            = c('grey'),
##     colour          = 'black',
##     pattern_scale   = -2,
##     pattern_filter  = 'point',
##     pattern_gravity = 'east',
##     pattern_density=1,
##     pattern_key_scale_factor = 1
##   )+
##   scale_pattern_filename_discrete(choices= c("stripe", "wave"))+
##   scale_pattern_fill_manual(values=my_chart_col)+
##   scale_linetype_manual(values=c("solid",'dashed'))+
##   scale_colour_manual(values=c("GRAY 48"),
##                       guide = guide_legend(override.aes = list(linetype = c('dashed'))))+
##
##   common_theme(Position = led_position)+
##   cutom_theme()
##
##
##   # theme_bw(18)+
##   # theme(legend.position = 'none') +
##
## Mon_enrollment_num_chart
##
##


## -------------------------------------
#GST Collection-Segment Wise
niif_gst = data_query_clk_pg(id=318916)
niif_gst_title=niif_gst[2][[1]]
niif_gst_src=niif_gst[3][[1]]
niif_gst=niif_gst[1][[1]]

niif_gst=niif_gst[,c("Relevant_Date",'CESS','IGST','SGST','CGST')]
names(niif_gst)[2]<-'Cess'
niif_gst=niif_gst%>%mutate(across(c(2:5), .fns = ~./10^9))

#NIIF_GST_gr
niif_gst_gr =data_query_clk_pg(id=1288705)[1][[1]]
colnames(niif_gst_gr)=c("Relevant_Date","growth")


## -------------------------------------
data_s=niif_gst
data_g=niif_gst_gr


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.75


legend_key_width=0.27
num_brek=5
max_pri_y=2000
min_pri_y=0
max_sec_y=10

n_col=6
n_row=1

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1")

GST_Col_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                          growth_heading="TTM GST revenue (% of TTM GDP)",
                                          x_axis_interval="12 month",data_unit='',
                                          graph_lim=30,
                                          round_integer=FALSE,
                                          Exception=FALSE,SIDE_BAR=FALSE,
                                          order_stack = TRUE,
                                          negative=FALSE,GST_SPECIAL=TRUE,
                                          DUAL_AXIS=TRUE,
                                          key_spacing=1,legends_reverse=FALSE)

GST_Col_title=GST_Col_chart[2][[1]]
GST_Col_chart=GST_Col_chart[1][[1]]
GST_Col_src=niif_gst_src
key_spacing=0.10
GST_Col_chart


## -------------------------------------
#CPI | Food and beverages
cpi_food = data_query_clk_pg(id=726007)
cpi_food_title=cpi_food[2][[1]]
cpi_food_src=cpi_food[3][[1]]
cpi_food=cpi_food[1][[1]]
cpi_food$Inflation=cpi_food$Inflation*45.86/100
names(cpi_food)[2]='Food and beverages'

#CPI | Pan, tobacco and intoxicants
cpi_tobaco = data_query_clk_pg(id=726029)[1][[1]]
cpi_tobaco$Inflation=cpi_tobaco$Inflation*2.38/100
names(cpi_tobaco)[2]='Intoxicants'

#CPI | Clothing and footwear
cpi_clth = data_query_clk_pg(id=725998)[1][[1]]
cpi_clth$Inflation=cpi_clth$Inflation*6.53/100
names(cpi_clth)[2]='Clothing and footwear'


#CPI | Housing
cpi_h = data_query_clk_pg(id=726015)[1][[1]]
cpi_h$Inflation=cpi_h$Inflation*10.07/100
names(cpi_h)[2]='Housing'

#CPI | Fuel and light
cpi_fuel = data_query_clk_pg(id=726010)[1][[1]]
cpi_fuel$Inflation=cpi_fuel$Inflation*6.84/100
names(cpi_fuel)[2]='Fuel and light'


#CPI | Miscellaneous | Household goods and services
cpi_hh = data_query_clk_pg(id=726008)[1][[1]]
cpi_hh$Inflation=cpi_hh$Inflation*3.8/100
names(cpi_hh)[2]='Household goods and services'

#CPI | Miscellaneous | Health
cpi_hl = data_query_clk_pg(id=726001)[1][[1]]
cpi_hl$Inflation=cpi_hl$Inflation*5.89/100
names(cpi_hl)[2]='Health'

#CPI | Miscellaneous | Transport and communication
cpi_tr = data_query_clk_pg(id=726026)[1][[1]]
cpi_tr$Inflation=cpi_tr$Inflation*8.59/100
names(cpi_tr)[2]='Transport and communication'


# CPI | Miscellaneous | Recreation and amusement
cpi_re = data_query_clk_pg(id=726023)[1][[1]]
cpi_re$Inflation=cpi_re$Inflation*1.68/100
names(cpi_re)[2]='Recreation'

# CPI | Miscellaneous | Education
cpi_edu = data_query_clk_pg(id=725991)[1][[1]]
cpi_edu$Inflation=cpi_edu$Inflation*4.46/100
names(cpi_edu)[2]='Education'


#CPI | Miscellaneous | Personal care and effects
cpi_prsn = data_query_clk_pg(id=726016)[1][[1]]
cpi_prsn$Inflation=cpi_prsn$Inflation*3.89/100
names(cpi_prsn)[2]='Personal care'



cpi_sect=cbind(cpi_food,cpi_tobaco,cpi_clth,cpi_h,cpi_fuel,
               cpi_hh,cpi_hl,cpi_tr,cpi_re,cpi_edu,cpi_prsn,
               by="Relevant_Date")

cpi_sect=cpi_sect[,c("Relevant_Date","Personal care","Education","Recreation" ,
                "Transport and communication","Health","Household goods and services",
                "Fuel and light","Housing","Clothing and footwear","Intoxicants","Food and beverages")]


# cpi_sect=cpi_sect[,re_arrange_columns(cpi_sect,sort_type ='')]


#CPI
cpi_sect_gr =data_query_clk_pg(id=1445961)[1][[1]]
names(cpi_sect_gr)[2]="growth"


## -------------------------------------
data_s=cpi_sect
data_g=cpi_sect_gr


h_just=-1.8
v_just=0.75
h_just_line=0.1
v_just_line=0.1


legend_key_width=0.27

num_brek=6
max_pri_y=12
min_pri_y=2
max_sec_y=12

n_col=1
n_row=15



my_chart_col=c("#8B6914","#8B2500","#FEECDA","GRAY 88","TAN 1","BURLYWOOD 1","#EE9572",
               "GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")


my_legends_col=c("#8B6914","#8B2500","#FEECDA","GRAY 88","TAN 1","BURLYWOOD 1","#EE9572",
               "GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")


Cpi_sect_chart=stacked_bar_line_chart_niif(data_s,data_g,
                      growth_heading="CPI (% yoy)",
                      x_axis_interval="12 month",
                      data_unit='',graph_lim=120,
                      round_integer=TRUE,
                      Exception=FALSE,SIDE_BAR=FALSE,
                      negative=TRUE,
                      GST_SPECIAL=FALSE,
                      DUAL_AXIS=FALSE,
                      legends_break = TRUE,
                      order_stack = TRUE,add_std_col = TRUE,
                      key_spacing=0.75,legends_reverse=TRUE,
                      legend_placing ='right',legend_ver_sp=0.20)

Cpi_sect_title=Cpi_sect_chart[2][[1]]
Cpi_sect_chart=Cpi_sect_chart[1][[1]]
Cpi_sect_src=cpi_food_src


## -------------------------------------
#CPI | Food and beverages | Cereals and products
cpi_crls = data_query_clk_pg(id=1605540)
cpi_crls_title=cpi_crls[2][[1]]
cpi_crls_src=cpi_crls[3][[1]]
cpi_crls=cpi_crls[1][[1]]
cpi_crls$Inflation=cpi_crls$Inflation*9.67/39.06
names(cpi_crls)[2]='Cereals and products'

#CPI | Food and beverages | Meat and fish
cpi_meat = data_query_clk_pg(id=1605551)[1][[1]]
cpi_meat$Inflation=cpi_meat$Inflation*3.61/39.06
names(cpi_meat)[2]='Meat and fish'


#CPI | Food and beverages | Milk and products
cpi_milk = data_query_clk_pg(id=1605553)[1][[1]]
cpi_milk$Inflation=cpi_milk$Inflation*6.61/39.06
names(cpi_milk)[2]='Milk'


#CPI | Food and beverages | Oils and fats
cpi_oil = data_query_clk_pg(id=1605557)[1][[1]]
cpi_oil$Inflation=cpi_oil$Inflation*3.56/39.06
names(cpi_oil)[2]='Oil'

#CPI | Food and beverages | Fruits
cpi_fruit = data_query_clk_pg(id=1605549)[1][[1]]
cpi_fruit$Inflation=cpi_fruit$Inflation*2.89/39.06
names(cpi_fruit)[2]='Fruits'

#CPI | Food and beverages | Vegetables
cpi_veg = data_query_clk_pg(id=1605567)[1][[1]]
cpi_veg$Inflation=cpi_veg$Inflation*6.04/39.06
names(cpi_veg)[2]='Vegetables'

#CPI | Food and beverages | Pulses and products
cpi_pulses = data_query_clk_pg(id=1605561)[1][[1]]
cpi_pulses$Inflation=cpi_pulses$Inflation*2.38/39.06
names(cpi_pulses)[2]='Pulses'


#CPI | Food and beverages | Spices
cpi_spicies = data_query_clk_pg(id=1605563)[1][[1]]
cpi_spicies$Inflation=cpi_spicies$Inflation*2.5/39.06
names(cpi_spicies)[2]='Spices'

#CPI | Food and beverages | Egg
cpi_egg = data_query_clk_pg(id=1605544)[1][[1]]
cpi_egg$Inflation=cpi_egg$Inflation*0.43/39.06
names(cpi_egg)[2]='Egg'

#CPI | Food and beverages | Sugar and Confectionery
cpi_sugar = data_query_clk_pg(id=1605565)[1][[1]]
cpi_sugar$Inflation=cpi_sugar$Inflation*1.36/39.06
names(cpi_sugar)[2]='Sugar and confectionery'



cpi_com=cbind(cpi_crls,cpi_meat,cpi_milk,cpi_oil,cpi_fruit,cpi_veg,
              cpi_pulses,cpi_egg,cpi_sugar,
              cpi_spicies,by="Relevant_Date")



cpi_com=cpi_com[,c('Relevant_Date','Sugar and confectionery',
           'Egg','Spices','Pulses','Vegetables','Fruits','Oil','Milk',
           'Meat and fish','Cereals and products')]

# cpi_com=cpi_com[,re_arrange_columns(cpi_com,sort_type ='')]


#CFPI
cpi_com_gr =data_query_clk_pg(id=2084627)[1][[1]]
names(cpi_com_gr)[2]="growth"


## -------------------------------------
data_s=cpi_com
data_g=cpi_com_gr

h_just=-1.8
v_just=0.75
h_just_line=0.1
v_just_line=0.1
Position="left"

legend_key_width=0.27
num_brek=9
max_pri_y=18
min_pri_y=4
max_sec_y=18

n_col=1
n_row=10

my_chart_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
               "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")


my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
               "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")



Cpi_com_chart=stacked_bar_line_chart_niif(data_s,data_g,
                      growth_heading="CPI food (% yoy)",
                      x_axis_interval="12 month",
                      data_unit='',graph_lim=120,
                      round_integer=TRUE,
                      Exception=FALSE,SIDE_BAR=FALSE,
                      negative=TRUE,
                      GST_SPECIAL=FALSE,
                      DUAL_AXIS=FALSE,
                      legends_break = TRUE,
                      key_spacing=1,legends_reverse=TRUE,
                      order_stack = TRUE,add_std_col = TRUE,
                      legend_placing ='right',legend_ver_sp=0.20)

Cpi_com_title=Cpi_com_chart[2][[1]]
Cpi_com_chart=Cpi_com_chart[1][[1]]
Cpi_com_src=cpi_crls_src



## -------------------------------------
#IIP | Primary goods
iip_pri = data_query_clk_pg(id=318918)
iip_pri_title=iip_pri[2][[1]]
iip_pri_src=iip_pri[3][[1]]
iip_pri=iip_pri[1][[1]]
iip_pri$growth=iip_pri$growth*34.048612/100
names(iip_pri)[2]='Primary goods'

# IIP | Capital goods
iip_capital = data_query_clk_pg(id=318884)[1][[1]]
iip_capital$growth=iip_capital$growth*8.223043/100
names(iip_capital)[2]='Capital goods'

#IIP | Intermediate goods
iip_inter = data_query_clk_pg(id=318911)[1][[1]]
iip_inter$growth=iip_inter$growth*17.221487/100
names(iip_inter)[2]='Intermediate goods'


#IIP | Infrastructure/ construction goods
iip_infra = data_query_clk_pg(id=318905)[1][[1]]
iip_infra$growth=iip_infra$growth*12.338363/100
names(iip_infra)[2]='Infrastructure and construction goods'

#IIP | Consumer durables
iip_con = data_query_clk_pg(id=318891)[1][[1]]
iip_con$growth=iip_con$growth*12.839296/100
names(iip_con)[2]='Consumer durables'

#IIP | Consumer non-durables
iip_con_nd = data_query_clk_pg(id=318898)[1][[1]]
iip_con_nd$growth=iip_con_nd$growth*15.329199/100
names(iip_con_nd)[2]='Consumer non-durables'

iip_used=cbind(iip_pri,iip_capital,iip_inter,iip_infra,iip_con,by="Relevant_Date")
iip_used=iip_used[iip_used$Relevant_Date>=min(iip_con_nd$Relevant_Date),]
iip_used=cbind(iip_used,iip_con_nd,by="Relevant_Date")

iip_used=iip_used[,c('Relevant_Date',
                "Consumer non-durables",'Consumer durables',
                "Infrastructure and construction goods",
                "Intermediate goods","Capital goods",
                "Primary goods")]

# iip_used=iip_used[,re_arrange_columns(iip_used,sort_type ='')]

#IIP
iip_used_gr =data_query_clk_pg(id=1288704)[1][[1]]
names(iip_used_gr)[2]="growth"


## -------------------------------------
data_s=iip_used
data_g=iip_used_gr

h_just=-1.8
v_just=0.75
h_just_line=0.1
v_just_line=0.1


legend_key_width=0.27

num_brek=10
max_pri_y=25
min_pri_y=25
max_sec_y=25

n_col=1
n_row=6

my_chart_col=c("BURLYWOOD 1","TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("BURLYWOOD 1","TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")



iip_used_chart=stacked_bar_line_chart_niif(data_s,data_g,
                      growth_heading="IIP (% yoy)",
                      x_axis_interval="12 month",
                      data_unit='',graph_lim=120,
                      round_integer=TRUE,
                      Exception=FALSE,SIDE_BAR=FALSE,
                      negative=TRUE,
                      GST_SPECIAL=FALSE,
                      DUAL_AXIS=FALSE,
                      legends_break = TRUE,
                      order_stack = TRUE,add_std_col = TRUE,
                      key_spacing=0.75,legends_reverse=TRUE,
                      legend_placing ='right',legend_ver_sp=0.20)

iip_used_title=iip_used_chart[2][[1]]
iip_used_chart=iip_used_chart[1][[1]]
iip_used_src=iip_pri_src



## -------------------------------------
#IIP | Mining
iip_minng = data_query_clk_pg(id=318913)
iip_minng_title=iip_minng[2][[1]]
iip_minng_src=iip_minng[3][[1]]
iip_minng=iip_minng[1][[1]]
iip_minng$growth=iip_minng$growth*14.372472/100
names(iip_minng)[2]='Mining'

#IIP | Manufacturing
iip_manu = data_query_clk_pg(id=318906)[1][[1]]
iip_manu$growth=iip_manu$growth*77.63321/100
names(iip_manu)[2]='Manufacturing'


#IIP | Electricity
iip_elec = data_query_clk_pg(id=318902)[1][[1]]
iip_elec$growth=iip_elec$growth*7.994318/100
names(iip_elec)[2]='Electricity'


iip_sect=cbind(iip_minng,iip_manu,iip_elec,by="Relevant_Date")

iip_sect=iip_sect[,c('Relevant_Date',
                     "Electricity","Manufacturing",
                     "Mining")]

# iip_sect=iip_sect[,re_arrange_columns(iip_sect,sort_type ='')]

#IIP
iip_sect_gr =data_query_clk_pg(id=1288704)[1][[1]]
names(iip_sect_gr)[2]="growth"


## -------------------------------------
data_s=iip_sect
data_g=iip_sect_gr


h_just=-1.8
v_just=0.75
h_just_line=0.1
v_just_line=0.1


legend_key_width=0.27


num_brek=10
max_pri_y=25
min_pri_y=25
max_sec_y=25


n_col=1
n_row=6

my_chart_col=c("#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")

my_legends_col=c("#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")


iip_sect_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                           growth_heading="IIP (% yoy)",
                                           x_axis_interval="12 month",
                                           data_unit='',graph_lim=120,
                                           round_integer=TRUE,
                                           Exception=FALSE,SIDE_BAR=FALSE,
                                           negative=TRUE,
                                           GST_SPECIAL=FALSE,
                                           DUAL_AXIS=FALSE,
                                           legends_break = TRUE,
                                           order_stack = TRUE,add_std_col = TRUE,
                                           key_spacing=0.75,legends_reverse=TRUE,
                                           legend_placing ='right',legend_ver_sp=0.20)

iip_sect_title=iip_sect_chart[2][[1]]
iip_sect_chart=iip_sect_chart[1][[1]]
iip_sect_src=iip_minng_src



## -------------------------------------
#WPI | Food Articles
wpi_food = data_query_clk_pg(id=726000)[1][[1]]
wpi_food$Inflation=wpi_food$Inflation*15.25585/100
names(wpi_food)[2]='Food articles'

#WPI | Non Food Articles
wpi_non_food = data_query_clk_pg(id=726025)[1][[1]]
wpi_non_food$Inflation=wpi_non_food$Inflation*4.11894/100
names(wpi_non_food)[2]='Non-food articles'

#WPI | Minerals
wpi_minrl = data_query_clk_pg(id=726022)[1][[1]]
wpi_minrl$Inflation=wpi_minrl$Inflation*0.83317/100
names(wpi_minrl)[2]='Minerals'


#WPI | Crude Petroleum And Natural Gas
wpi_crd_oil = data_query_clk_pg(id=725988)[1][[1]]
wpi_crd_oil$Inflation=wpi_crd_oil$Inflation*2.4096/100
names(wpi_crd_oil)[2]="Crude oil"

#WPI | Coal
wpi_coal = data_query_clk_pg(id=725959)[1][[1]]
wpi_coal$Inflation=wpi_coal$Inflation*2.13813/100
names(wpi_coal)[2]='Coal'

#WPI | Mineral Oils
wpi_mn_oil = data_query_clk_pg(id=726017)[1][[1]]
wpi_mn_oil$Inflation=wpi_mn_oil$Inflation*7.94968/100
names(wpi_mn_oil)[2]="Mineral oils"


#WPI | Electricity
wpi_elec = data_query_clk_pg(id=725989)[1][[1]]
wpi_elec$Inflation=wpi_elec$Inflation*3.06409/100
names(wpi_elec)[2]="Electricity"


# WPI | Manufactured Products
wpi_mnf = data_query_clk_pg(id=726011)
wpi_mnf_title=wpi_mnf[2][[1]]
wpi_mnf_src=wpi_mnf[3][[1]]
wpi_mnf=wpi_mnf[1][[1]]
wpi_mnf$Inflation=wpi_mnf$Inflation*64.23054/100
names(wpi_mnf)[2]='Manufactured products'



wpi_sect=cbind(wpi_mnf,wpi_food,wpi_non_food,wpi_crd_oil,
               wpi_minrl,wpi_coal,wpi_mn_oil,wpi_elec,
               by="Relevant_Date")

wpi_sect=wpi_sect[,c('Relevant_Date',
                     'Manufactured products',
                     "Electricity",
                     "Mineral oils",
                     'Coal',
                     "Crude oil",
                     "Minerals" ,
                     "Non-food articles",
                     "Food articles")]

# wpi_sect=wpi_sect[,re_arrange_columns(wpi_sect,sort_type ='')]

#WPI
wpi_sect_gr =data_query_clk_pg(id=725962)[1][[1]]
names(wpi_sect_gr)[2]="growth"


## -------------------------------------
data_s=wpi_sect
data_g=wpi_sect_gr


h_just=-1.8
v_just=0.75
h_just_line=0.1
v_just_line=0.1


legend_key_width=0.27



num_brek=6
max_pri_y=20
min_pri_y=10
max_sec_y=20

n_col=1
n_row=10

my_chart_col=c("GRAY 88","#EE9572","BURLYWOOD 1",
               "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")

my_legends_col=c("GRAY 88","#EE9572","BURLYWOOD 1",
                 "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")


wpi_sect_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                           growth_heading="WPI (% yoy)",
                                           x_axis_interval="12 month",
                                           data_unit='',graph_lim=120,
                                           round_integer=FALSE,
                                           Exception=FALSE,SIDE_BAR=FALSE,
                                           negative=TRUE,
                                           GST_SPECIAL=FALSE,
                                           DUAL_AXIS=FALSE,
                                           legends_break = TRUE,
                                           order_stack = TRUE,add_std_col = TRUE,
                                           key_spacing=0.75,legends_reverse=TRUE,
                                           legend_placing ='right',legend_ver_sp=0.20)

wpi_sect_title=wpi_sect_chart[2][[1]]
wpi_sect_chart=wpi_sect_chart[1][[1]]
wpi_sect_src=wpi_mnf_src



## -------------------------------------
#Exports Composition
Exports_Compo = data_query_clk_pg(id=1502354)
Exports_Compo_src=Exports_Compo[3][[1]]

#EXPORT
#OIL
older_nonoil=data_query_clk_pg(1502354)[1][[1]]
older_nonoil$Value=older_nonoil$Value/10^9
colnames(older_nonoil)=c("Relevant_Date",'Non-oil exports')

#OIL
older_oil=data_query_clk_pg(1502351)[1][[1]]
older_oil$Value=older_oil$Value/10^9
colnames(older_oil)=c("Relevant_Date","Oil exports")

#
# older_niif=cbind(older_nonoil,older_oil,by="Relevant_Date")
older_niif=merge(older_nonoil, older_oil, by="Relevant_Date",all=T)
older_niif=older_niif[,c("Relevant_Date",'Non-oil exports',"Oil exports")]
Exports_com_b=older_niif

#TRADE
older_mer=data_query_clk_pg(1502373)[1][[1]]
older_mer$Value=older_mer$Value/10^9
colnames(older_mer)=c("Relevant_Date","growth")
Exports_com_g=older_mer

Exports_com_b=Exports_com_b[!duplicated(Exports_com_b[c("Relevant_Date")]), ]
Exports_com_g=Exports_com_g[!duplicated(Exports_com_g[c("Relevant_Date")]), ]



## -------------------------------------
data_s=Exports_com_b
data_g=Exports_com_g

draw_key_line=draw_key_rect

h_just=1
v_just=0.20
h_just_line=0
v_just_line=0.75

max_overlap=8
Position="left"

num_brek=6
max_pri_y=70
min_pri_y=0
max_sec_y=70


my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1","GRAY 88")

Mon_Exports_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                  growth_heading="Merchandize exports",
                                                  x_axis_interval='24 month',data_unit='',
                                                  graph_lim=75,
                                                  round_integer=TRUE,Exception=TRUE,
                                                  SIDE_BAR=FALSE,negative=FALSE,
                                                  order_stack = TRUE,
                                                  GST_SPECIAL=FALSE,DUAL_AXIS=FALSE)

Mon_Exports_title=Mon_Exports_chart[2][[1]]
Mon_Exports_chart=Mon_Exports_chart[1][[1]]
Mon_Exports_src=Exports_Compo_src



## -------------------------------------
#Imports Composition
Imports_Com = data_query_clk_pg(id=1502362)
Imports_Com_src=Imports_Com[3][[1]]

#OIL
older_nonoil=data_query_clk_pg(1502362)[1][[1]]
older_nonoil$Value=older_nonoil$Value/10^9
colnames(older_nonoil)=c("Relevant_Date",'Non-oil imports')

#IMPORT
older_oil=data_query_clk_pg(1502357)[1][[1]]
older_oil$Value=older_oil$Value/10^9
colnames(older_oil)=c("Relevant_Date","Oil imports")
older_niif=merge(older_nonoil, older_oil, by="Relevant_Date",all=T)
# older_niif=cbind(older_nonoil,older_oil,by="Relevant_Date")
older_niif=older_niif[,c("Relevant_Date",'Non-oil imports',"Oil imports")]
Imports_Com_b=older_niif

older_mer=data_query_clk_pg(1502371)[1][[1]]
older_mer$Value=older_mer$Value/10^9
colnames(older_mer)=c("Relevant_Date","growth")
Imports_Com_g=older_mer


Imports_Com_b=Imports_Com_b[!duplicated(Imports_Com_b[c("Relevant_Date")]), ]
Imports_Com_g=Imports_Com_g[!duplicated(Imports_Com_g[c("Relevant_Date")]), ]



## -------------------------------------
data_s=Imports_Com_b
data_g=Imports_Com_g


my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1","GRAY 88")


num_brek=6
max_pri_y=70
min_pri_y=0
max_sec_y=70


h_just=0
v_just=2.5
h_just_line=0
v_just_line=0.75

Mon_Import_chart=stacked_bar_line_chart_niif(data_s,data_g,
                   growth_heading="Merchandize imports",
                   x_axis_interval="24 month",data_unit='',
                   round_integer=TRUE,Exception=TRUE,graph_lim=75,
                   order_stack = TRUE,
                   SIDE_BAR=FALSE,negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=FALSE)

Mon_Import_title=Mon_Import_chart[2][[1]]
Mon_Import_chart=Mon_Import_chart[1][[1]]
Mon_Import_src=Imports_Com_src




## -------------------------------------
#Households Work Demand
HH_Work_Demand=data_query_clk_pg(id=1072572)
HH_Work_Demand_title=HH_Work_Demand[2][[1]]
HH_Work_Demand_src=HH_Work_Demand[3][[1]]
HH_Work_Demand=HH_Work_Demand[1][[1]]
HH_Work_Demand$Total=HH_Work_Demand$Total/10^6
colnames(HH_Work_Demand)=c("Relevant_Date","Jobs demanded (MHH)")


#Households Employment Provided
HH_Work_provided=data_query_clk_pg(id=1072573)
HH_Work_provided=HH_Work_provided[1][[1]]
HH_Work_provided$Total=HH_Work_provided$Total/10^6
colnames(HH_Work_provided)=c("Relevant_Date","Jobs provided (MHH)")

#Ratio of employment to Work Demand
EW_ratio=data_query_clk_pg(1502149,Water_Reservior=FALSE)[1][[1]]
colnames(EW_ratio)=c("Relevant_Date","growth")



## -------------------------------------
data_s=HH_Work_provided
data_g=EW_ratio



my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48","TAN 1")

h_just=0.60
v_just=0.60
h_just_line=0
v_just_line=0.75

n_row=2
n_col=2
legend_key_width=0.27
chart_label=8

num_brek=6
max_pri_y=50
min_pri_y=-25
max_sec_y=100

Mon_MGNREGS_employ_data_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                        growth_heading="Growth (RHS, % yoy)",
                                        x_axis_interval="24 month",
                                        data_unit='',graph_lim=60,
                                        round_integer=FALSE,
                                        Exception=FALSE,SIDE_BAR=TRUE,
                                        negative=FALSE,GST_SPECIAL=FALSE)

Mon_MGNREGS_employ_data_title=Mon_MGNREGS_employ_data_chart[2][[1]]
Mon_MGNREGS_employ_data_chart=Mon_MGNREGS_employ_data_chart[1][[1]]
Mon_MGNREGS_employ_data_src=HH_Work_Demand_src



## -------------------------------------
#Naukri jobspeak index
Naukri_jobspeak_index= data_query_clk_pg(id=808881)
Naukri_jobspeak_index_title=Naukri_jobspeak_index[2][[1]]
Naukri_jobspeak_index_src=Naukri_jobspeak_index[3][[1]]
Naukri_jobspeak_index=Naukri_jobspeak_index[1][[1]]
colnames(Naukri_jobspeak_index)=c("Relevant_Date","Naukri jobspeak index")

# Growth In Jobs Index
Naukri_jobspeak_index_gr= data_query_clk_pg(id=1682365)[1][[1]]
colnames(Naukri_jobspeak_index_gr)=c("Relevant_Date","growth")



## -------------------------------------
data_s=Naukri_jobspeak_index
data_g=Naukri_jobspeak_index_gr

num_brek=7
max_pri_y=3500
min_pri_y=-1500
max_sec_y=70


h_just_line=0.75
v_just_line=0.75
my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48")

Naukri_jobspeak_index_chart=stacked_bar_line_chart_niif(data_s,data_g,
                            growth_heading="Growth (RHS, % yoy)",
                            x_axis_interval="12 month",
                            data_unit='',graph_lim=60,
                            round_integer=FALSE,
                            Exception=FALSE,SIDE_BAR=TRUE,
                            negative=TRUE,GST_SPECIAL=FALSE)


Naukri_jobspeak_index_title=Naukri_jobspeak_index_chart[2][[1]]
Naukri_jobspeak_index_chart=Naukri_jobspeak_index_chart[1][[1]]
Naukri_jobspeak_index_src=Naukri_jobspeak_index_src
Naukri_jobspeak_index_chart


## -------------------------------------
Mon_gen_from_energy_src= data_query_clk_pg(id=725964)
Mon_gen_from_energy_src_title=Mon_gen_from_energy_src[2][[1]]
Mon_gen_from_energy_src_src=Mon_gen_from_energy_src[3][[1]]
Mon_gen_from_energy_src=Mon_gen_from_energy_src[1][[1]]

Mon_gen_from_energy_src$Total=Mon_gen_from_energy_src$Total/10^3
Mon_gen_from_energy_src$Total1=Mon_gen_from_energy_src$Total1/10^3


colnames(Mon_gen_from_energy_src)=c("Relevant_Date","Conventional","Renewable")
energy_srcs=Mon_gen_from_energy_src[,c("Relevant_Date","Conventional", "Renewable")]

older_niif=data_query_clk_pg(894698,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Conventional","Renewable")
older_niif=older_niif%>%mutate(across(c(2:3), .fns = ~./10^3))
older_niif=older_niif[older_niif$Relevant_Date<min(energy_srcs$Relevant_Date),]
energy_srcs=rbind(older_niif,energy_srcs)

energy_share=data_query_clk_pg(2475764,150)[1][[1]]
colnames(energy_share)=c("Relevant_Date","growth")
older_niif=data_query_clk_pg(894701,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth")
older_niif=older_niif[older_niif$Relevant_Date<min(energy_share$Relevant_Date),]
energy_share=rbind(older_niif,energy_share)
energy_share$Relevant_Date=as.Date(energy_share$Relevant_Date)



## -------------------------------------
data_s=energy_srcs
data_g=energy_share

my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY 48")
x_axis_interval="12 month"

h_just=-1
v_just=0
h_just_line=0
v_just_line=3
nug_x=0
nug_y=0

n_row=3
n_col=2

num_brek=5
max_pri_y=200
min_pri_y=0
max_sec_y=20

Mon_gen_energy_srcs_chart=stacked_bar_line_chart_niif(data_s,data_g,
                          growth_heading="Share of renewable energy (RHS, %)",
                          x_axis_interval="24 month",data_unit='',
                          graph_lim=75,
                          order_stack = TRUE,
                          round_integer=FALSE,Exception=FALSE,SIDE_BAR=FALSE,
                          negative=FALSE,GST_SPECIAL=FALSE)


Mon_gen_energy_srcs_title=Mon_gen_energy_srcs_chart[2][[1]]
Mon_gen_energy_srcs_chart=Mon_gen_energy_srcs_chart[1][[1]]
Mon_gen_energy_srcs_src=Mon_gen_from_energy_src_src
Mon_gen_energy_srcs_chart



## -------------------------------------
#O/S Dues For NIIF
older_niif=data_query_clk_pg(894113,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Billed","Paid")
older_niif$Billed=older_niif$Billed/10^9
older_niif$Paid=older_niif$Paid/10^9
Monthly_bill_paid=older_niif

# Overdues From DISCOMS
Mon_outst_Overdue_g=data_query_clk_pg(id=2046205)
Mon_outst_Overdue_g_title=Mon_outst_Overdue_g[2][[1]]
Mon_outst_Overdue_g_src=Mon_outst_Overdue_g[3][[1]]
Mon_outst_Overdue_g=Mon_outst_Overdue_g[1][[1]]

older_niif=data_query_clk_pg(894684,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_outst_Overdue_g$Relevant_Date),]
Mon_outst_Overdue_g=rbind(older_niif,Mon_outst_Overdue_g)
Mon_outst_Overdue_g$Total=Mon_outst_Overdue_g$Total/10^9
colnames(Mon_outst_Overdue_g)=c("Relevant_Date","growth")



## -------------------------------------
data_s=Mon_outst_Overdue_g


divisor=1
multiplier=1

my_chart_col=c("GOLDEN ROD 1","GRAY 88")
my_legends_col=c("GOLDEN ROD 1","GRAY 88","TAN 1")
max_overlaps=30
h_just=0.60
v_just=0.60
h_just_line=0
v_just_line=0.60

n_row=1
n_col=3

num_brek=6
max_pri_y=1400
min_pri_y=0
max_sec_y=1200

Mon_outst_dues_of_discoms_chart=line_chart_niif(data_s,x_axis_interval="12 month",
                                      sales_heading="Monthly overdues from discoms (INR billion)",
                                      graph_lim=30,SPECIAL_LINE=FALSE)

Mon_outst_dues_of_discoms_title=Mon_outst_dues_of_discoms_chart[2][[1]]
Mon_outst_dues_of_discoms_chart=Mon_outst_dues_of_discoms_chart[1][[1]]
Mon_outst_dues_of_discoms_src=Mon_outst_Overdue_g_src



## -------------------------------------
#Foreign Direct Investment by India
FDI_by_India=data_query_clk_pg(id=318845)
FDI_by_India_title=FDI_by_India[2][[1]]
FDI_by_India_src=FDI_by_India[3][[1]]
FDI_by_India=FDI_by_India[1][[1]]
FDI_by_India=mon_year_df_creator(FDI_by_India,keep_col=c("Relevant_Date","Value"),Sum_date = TRUE)
FDI_by_India$Value=roundexcel((-FDI_by_India$Value/10^9),1)

colnames(FDI_by_India)=c("Relevant_Date","Direct investments by India")

#Foreign Direct Investment to India
FDI_to_India=data_query_clk_pg(id=318847)[1][[1]]
FDI_to_India=mon_year_df_creator(FDI_to_India,keep_col=c("Relevant_Date","Value"),Sum_date = TRUE)
# FDI_to_India$Value=roundexcel((FDI_to_India$Value/10^9),1)
FDI_to_India$Value=FDI_to_India$Value/10^9

colnames(FDI_to_India)=c("Relevant_Date","Direct investments to India")
FDI_invest=merge(FDI_to_India,FDI_by_India,by="Relevant_Date")
FDI_invest=FDI_invest[!duplicated(FDI_invest),]

#Net Foreign Direct Investment into India
FDI_invest_gr=data_query_clk_pg(id=318903)[1][[1]]
FDI_invest_gr=mon_year_df_creator(FDI_invest_gr,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
colnames(FDI_invest_gr)=c("Relevant_Date","growth")
FDI_invest_gr=FDI_invest_gr[!duplicated(FDI_invest_gr),]
FDI_invest_gr$growth=FDI_invest_gr$growth/10^9



## -------------------------------------
data_s=FDI_invest
data_g=FDI_invest_gr


my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")

num_brek=5
max_pri_y=60
min_pri_y=20
max_sec_y=60

n_row=1
n_col=3

h_just=-1
v_just=-0.75
h_just_line=0.75
v_just_line=0.75

Mon_FDI_invest_chart=stacked_bar_line_chart_niif(data_s,data_g,
                     growth_heading="Net FDI inflows into India",
                     x_axis_interval="24 month",data_unit="",
                     graph_lim=470,
                     round_integer=TRUE,
                     Exception=FALSE,SIDE_BAR=FALSE,
                     negative=TRUE,
                     GST_SPECIAL=FALSE,
                     DUAL_AXIS=TRUE,key_spacing=1,
                     ptrn_den=0.1,ptrn_spc=0.01,
                     order_stack = TRUE,
                     format_date =paste0("%Y"),
                     YTD =TRUE,show_shaded = TRUE,bar_thick=100)


Annual_FDI_invest_title=Mon_FDI_invest_chart[2][[1]]
Annual_FDI_invest_chart=Mon_FDI_invest_chart[1][[1]]
Annual_FDI_invest_src=FDI_by_India_src
Annual_FDI_invest_chart


## -------------------------------------
#Foreign Direct Investment by India
FDI_by_India=data_query_clk_pg(id=318845)
FDI_by_India_title=FDI_by_India[2][[1]]
FDI_by_India_src=FDI_by_India[3][[1]]
FDI_by_India=FDI_by_India[1][[1]]
FDI_by_India$Value=roundexcel((-FDI_by_India$Value/10^9),1)

colnames(FDI_by_India)=c("Relevant_Date","Direct investments by India")

#Foreign Direct Investment to India
FDI_to_India=data_query_clk_pg(id=318847)[1][[1]]
# FDI_to_India$Value=roundexcel((FDI_to_India$Value/10^9),1)
FDI_to_India$Value=FDI_to_India$Value/10^9

colnames(FDI_to_India)=c("Relevant_Date","Direct investments to India")
FDI_invest=merge(FDI_to_India,FDI_by_India,by="Relevant_Date")
FDI_invest=FDI_invest[!duplicated(FDI_invest),]

#Net Foreign Direct Investment into India
FDI_invest_gr=data_query_clk_pg(id=318903)[1][[1]]
colnames(FDI_invest_gr)=c("Relevant_Date","growth")
FDI_invest_gr=FDI_invest_gr[!duplicated(FDI_invest_gr),]
FDI_invest_gr$growth=FDI_invest_gr$growth/10^9



## -------------------------------------
data_s=FDI_invest
data_g=FDI_invest_gr


my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")

num_brek=5
max_pri_y=20
min_pri_y=5
max_sec_y=20

n_row=1
n_col=3

h_just=1.10
v_just=1.10
h_just_line=-2.5
v_just_line=1.25

Mon_FDI_invest_chart=stacked_bar_line_chart_niif(data_s,data_g,
                           growth_heading="Net FDI inflows into India",
                           x_axis_interval="24 month",data_unit="",
                           graph_lim=90,
                           round_integer=TRUE,
                           Exception=FALSE,SIDE_BAR=FALSE,
                           negative=TRUE,
                           order_stack = TRUE,
                           GST_SPECIAL=FALSE,
                           DUAL_AXIS=TRUE,key_spacing=1,YTD =FALSE,bar_thick=8)


Mon_FDI_invest_title=Mon_FDI_invest_chart[2][[1]]
Mon_FDI_invest_chart=Mon_FDI_invest_chart[1][[1]]
Mon_FDI_invest_src=FDI_by_India_src
Mon_FDI_invest_chart



## -------------------------------------
#MF | Corporates
mf_corporates = data_query_clk_pg(1623668,exception = TRUE)
mf_corporates_title=mf_corporates[2][[1]]
mf_corporates_src=mf_corporates[3][[1]]
mf_corporates=mf_corporates[1][[1]]
mf_corporates$Value=mf_corporates$Value/10^12
colnames(mf_corporates)=c("Relevant_Date",'Corporates')

#MF | FIIs
mf_fii = data_query_clk_pg(1623670,exception = TRUE)[1][[1]]
mf_fii$Value=mf_fii$Value/10^12
colnames(mf_fii)=c("Relevant_Date","FII")

#MF | High Networth Individuals
mf_hni = data_query_clk_pg(1623671,exception = TRUE)[1][[1]]
mf_hni$Value=mf_hni$Value/10^12
colnames(mf_hni)=c("Relevant_Date","HNI")

#MF | Retail
mf_retail = data_query_clk_pg(1623672,exception = TRUE)[1][[1]]
mf_retail$Value=mf_retail$Value/10^12
colnames(mf_retail)=c("Relevant_Date","Retail")

#MF | Banks/FIs
mf_banks = data_query_clk_pg(1623669,exception = TRUE)[1][[1]]
mf_banks$Value=mf_banks$Value/10^12
colnames(mf_banks)=c("Relevant_Date","BFSI")

mf_aaum=cbind(mf_corporates,mf_fii,mf_hni,mf_retail,mf_banks,by="Relevant_Date")
mf_aaum=mf_aaum[,c("Relevant_Date","Retail","HNI","FII","BFSI",'Corporates')]

#total aum at the end of quarter
mf_aum_gr =data_query_clk_pg(1623740,exception = TRUE)[1][[1]]
mf_aum_gr$Value=mf_aum_gr$Value/10^12
colnames(mf_aum_gr)=c("Relevant_Date","growth")



## -------------------------------------

data_s=mf_aaum
data_g=mf_aum_gr

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60


legend_key_width=0.27


num_brek=5
max_pri_y=60
min_pri_y=0
max_sec_y=60

n_col=6
n_row=1

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1",
                 "GRAY 48")

Mf_aum_chart=stacked_bar_line_chart_niif(data_s,data_g,growth_heading="Total",
                                         x_axis_interval="24 month",data_unit='',
                                         graph_lim=90,round_integer=FALSE,
                                         Exception=FALSE,SIDE_BAR=FALSE,
                                         order_stack = TRUE,
                                         negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=TRUE,
                                         key_spacing=0.75,bar_thick=40)



Mf_aum_title=Mf_aum_chart[2][[1]]
Mf_aum_chart=Mf_aum_chart[1][[1]]
Mf_aum_src=mf_corporates_src
Mf_aum_chart


## -------------------------------------
#Mutual Funds | Debt | Government Securities
mf_debt_gsc = data_query_clk_pg(1685093)
mf_debt_gsc_src=mf_debt_gsc[3][[1]]
mf_debt_gsc=mf_debt_gsc[1][[1]]
mf_debt_gsc=mon_qtr_df_creator(mf_debt_gsc,keep_col =c("Relevant_Date","Value"))
names(mf_debt_gsc)[2]='Government securities'

#Mutual Funds | Debt | Instruments
mf_debt_inst = data_query_clk_pg(1685094)[1][[1]]
mf_debt_inst=mon_qtr_df_creator(mf_debt_inst,keep_col =c("Relevant_Date","Value"))
colnames(mf_debt_inst)=c("Relevant_Date","Instruments (ex-corporate debt)")

#Mutual Funds | Debt | Corporate Debt
mf_debt_cd = data_query_clk_pg(1685095)[1][[1]]
mf_debt_cd=mon_qtr_df_creator(mf_debt_cd,keep_col =c("Relevant_Date","Value"))
colnames(mf_debt_cd)=c("Relevant_Date","Corporate debt")

#Mutual Funds | Debt | Others
mf_others = data_query_clk_pg(2465542)[1][[1]]
mf_others=mon_qtr_df_creator(mf_others,keep_col =c("Relevant_Date","Value"))
colnames(mf_others)=c("Relevant_Date","Others")

mf_debt=cbind(mf_debt_gsc,mf_debt_inst,mf_debt_cd,mf_others,by="Relevant_Date")
mf_debt=mf_debt[,c("Relevant_Date","Others","Corporate debt","Instruments (ex-corporate debt)","Government securities")]
mf_debt=mf_debt%>%mutate(across(c(2:5), .fns = ~./10^12))

#total aum at the end of quarter
mf_debt_gr =data_query_clk_pg(1685092)[1][[1]]
mf_debt_gr=mon_qtr_df_creator(mf_debt_gr,keep_col =c("Relevant_Date","Value"))
mf_debt_gr$Value=mf_debt_gr$Value/10^12
colnames(mf_debt_gr)=c("Relevant_Date","growth")



## -------------------------------------
data_s=mf_debt
data_g=mf_debt_gr

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60


legend_key_width=0.27
num_brek=7
max_pri_y=30
min_pri_y=0
max_sec_y=30

n_col=4
n_row=2

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1","GRAY 48")


Mf_debt_chart=stacked_bar_line_chart_niif(data_s,data_g,growth_heading="",
                                         x_axis_interval="24 month",data_unit='',
                                         graph_lim=30,round_integer=FALSE,
                                         Exception=FALSE,SIDE_BAR=FALSE,
                                         negative=FALSE,GST_SPECIAL=FALSE,
                                         DUAL_AXIS=FALSE,
                                         order_stack = TRUE,
                                         top_column = "Others",
                                         key_spacing=0.60,bar_thick=40)



Mf_debt_title=Mf_debt_chart[2][[1]]
Mf_debt_chart=Mf_debt_chart[1][[1]]
Mf_debt_src=mf_debt_gsc_src
Mf_debt_chart


## -------------------------------------
#Internal Debt | International Financial Institutions
gov_int_fi = data_query_clk_pg(1685429,exception=TRUE)
gov_int_fi_src=gov_int_fi[3][[1]]
gov_int_fi=gov_int_fi[1][[1]]
names(gov_int_fi)[2]='International finance institutions (LHS, INR tn)'

#Internal Debt | Market Securities
gov_int_ms = data_query_clk_pg(1685430,exception=TRUE)[1][[1]]
names(gov_int_ms)[2]='Market securities (LHS, INR tn)'

#Internal Debt | Small Savings
gov_int_ss = data_query_clk_pg(1685428,exception=TRUE)[1][[1]]
names(gov_int_ss)[2]='Small savings (LHS, INR tn)'

#Internal Debt | Others
gov_int_oth = data_query_clk_pg(1685427,exception=TRUE)[1][[1]]
names(gov_int_oth)[2]='Others (LHS, INR tn)'


gov_int_borr=cbind(gov_int_fi,gov_int_ms,gov_int_ss,gov_int_oth,by="Relevant_Date")
gov_int_borr=gov_int_borr[,c("Relevant_Date",
                             "Others (LHS, INR tn)",
                             "Small savings (LHS, INR tn)",
                             "International finance institutions (LHS, INR tn)",
                             "Market securities (LHS, INR tn)")]

gov_int_borr=gov_int_borr%>%mutate(across(c(2:5), .fns = ~./10^12))

#total aum at the end of quarter
gov_int_gdp =data_query_clk_pg(1685436,exception=TRUE)[1][[1]]
colnames(gov_int_gdp)=c("Relevant_Date","growth")


## -------------------------------------
data_s=gov_int_borr
data_g=gov_int_gdp

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60


legend_key_width=0.23


num_brek=6
max_pri_y=140
min_pri_y=0
max_sec_y=80

n_col=1
n_row=4

n_col1=1
n_row1=4

my_chart_col=c("GRAY 88","BURLYWOOD 1","DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","BURLYWOOD 1","DARK ORANGE 2","GOLDEN ROD 1")


govt_int_debt_chart=stacked_bar_line_chart_niif(data_s,data_g,
                     growth_heading="Internal debt to TTM GDP (RHS, %)",
                     x_axis_interval="24 month",data_unit='',
                     graph_lim=60,round_integer=FALSE,
                     Exception=FALSE,SIDE_BAR=FALSE,
                     negative=FALSE,GST_SPECIAL=FALSE,
                     DUAL_AXIS=TRUE,legends_break=TRUE,
                     top_column ="Others (LHS, INR tn)",
                     order_stack = TRUE,
                     key_spacing=0.75,bar_thick=40)


govt_int_debt_title=govt_int_debt_chart[2][[1]]
govt_int_debt_chart=govt_int_debt_chart[1][[1]]
govt_int_debt_src=gov_int_fi_src



## -------------------------------------
#External Debt | Long-term
gov_ex_lng = data_query_clk_pg(2447664,exception = TRUE)
gov_ex_lng_src=gov_ex_lng[3][[1]]
gov_ext_borr=gov_ex_lng[1][[1]]
gov_ext_borr$Value=gov_ext_borr$Value/10^12
names(gov_ext_borr)[2]='External debt (LHS, INR tn)'

# #External Debt | Short-term
# gov_ex_srt = data_query_clk_pg(1685098)[1][[1]]
# names(gov_ex_srt)[2]='Short-term (LHS, INR tn)'

# gov_ext_borr=cbind(gov_ex_lng,gov_ex_srt,by="Relevant_Date")
# gov_ext_borr=gov_ext_borr[,c("Relevant_Date",
#                              "Short-term (LHS, INR tn)",
#                              "Long-term (LHS, INR tn)")]
# gov_ext_borr=gov_ext_borr%>%mutate(across(c(2:3), .fns = ~./10^12))


#total aum at the end of quarter
gov_ext_gdp =data_query_clk_pg(1685435,exception=TRUE)[1][[1]]
colnames(gov_ext_gdp)=c("Relevant_Date","growth")


## -------------------------------------
data_s=gov_ext_borr
data_g=gov_ext_gdp

h_just=0
v_just=0.85
h_just_line=0
v_just_line=0.30


legend_key_width=0.23

num_brek=6
max_pri_y=140
min_pri_y=0
max_sec_y=7

n_col=1
n_row=2

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1","GRAY 48")


govt_ext_debt_chart=stacked_bar_line_chart_niif(data_s,data_g,
                         growth_heading="External debt to TTM GDP (RHS, % TTM GDP)",
                         x_axis_interval="24 month",data_unit='',
                         graph_lim=150,round_integer=TRUE,
                         Exception=FALSE,SIDE_BAR=FALSE,legends_break=TRUE,
                         negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=TRUE,
                         order_stack = TRUE,
                         key_spacing=0.75,bar_thick=40)



govt_ext_debt_title=govt_ext_debt_chart[2][[1]]
govt_ext_debt_chart=govt_ext_debt_chart[1][[1]]
govt_ext_debt_src=gov_ex_lng_src



## -------------------------------------
#MF | High Networth Individuals
mf_hni = data_query_clk_pg(1623676,exception = TRUE)
mf_hni_title=mf_hni[2][[1]]
mf_hni_src=mf_hni[3][[1]]
mf_hni=mf_hni[1][[1]]
mf_hni$Total=mf_hni$Total/10^6
colnames(mf_hni)=c("Relevant_Date","HNI")

#MF | Retail
mf_retail = data_query_clk_pg(1623674,exception = TRUE)[1][[1]]
mf_retail$Total=mf_retail$Total/10^6
colnames(mf_retail)=c("Relevant_Date","Retail")


mf_folios=cbind(mf_hni,mf_retail,by="Relevant_Date")
mf_folios=mf_folios[,c("Relevant_Date","Retail","HNI")]

#total aum at the end of quarter
mf_folios_gr =data_query_clk_pg(1623738,exception = TRUE)[1][[1]]
mf_folios_gr$Total=mf_folios_gr$Total/10^6
colnames(mf_folios_gr)=c("Relevant_Date","growth")



## -------------------------------------
data_s=mf_folios
data_g=mf_folios_gr

h_just=0.60
v_just=0
h_just_line=0.60
v_just_line=0.60


legend_key_width=0.27

num_brek=5
max_pri_y=180
min_pri_y=0
max_sec_y=180

n_col=6
n_row=1

my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY 48")


Mf_folios_chart=stacked_bar_line_chart_niif(data_s,data_g,growth_heading="Total",
                                         x_axis_interval="24 month",data_unit='',
                                         graph_lim=60,round_integer=FALSE,
                                         Exception=FALSE,SIDE_BAR=FALSE,
                                         negative=FALSE,GST_SPECIAL=FALSE,
                                         DUAL_AXIS=TRUE,order_stack = TRUE,
                                         key_spacing=0.75,bar_thick=40)



Mf_folios_title=Mf_folios_chart[2][[1]]
Mf_folios_chart=Mf_folios_chart[1][[1]]
Mf_folios_src=mf_hni_src


## -------------------------------------
#stocks
food_grain_fci= data_query_clk_pg(id=318923)
food_grain_fci_title=food_grain_fci[2][[1]]
food_grain_fci_src=food_grain_fci[3][[1]]
food_grain_fci=food_grain_fci[1][[1]]
colnames(food_grain_fci)=c("Relevant_Date","Rice","Wheat")


## -------------------------------------
data_s=list(food_grain_fci)

h_just=0
v_just=0.75
h_just_line=1.15
v_just_line=1.15

num_brek=4
max_pri_y=80
min_pri_y=0

my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1")

Mon_Food_Grain_Stock_FCI_chart=stacked_bar_chart_niif(data_s,
                                                      x_axis_interval="24 month",
                                                      data_unit="",
                                                      order_stack = TRUE,
                                                      led_position = "center",
                                                      graph_lim=120,negative=FALSE)

##
Mon_Food_Grain_Stock_FCI_title=Mon_Food_Grain_Stock_FCI_chart[2][[1]]
Mon_Food_Grain_Stock_FCI_chart=Mon_Food_Grain_Stock_FCI_chart[1][[1]]
Mon_Food_Grain_Stock_FCI_src=food_grain_fci_src
Mon_Food_Grain_Stock_FCI_chart


## ----eval=FALSE, include=FALSE--------
## #RHS
## GDP_contri_priv= data_query_clk_pg(1502404)
## GDP_contri_priv_title=GDP_contri_priv[2][[1]]
## GDP_contri_priv_src=GDP_contri_priv[3][[1]]
## GDP_contri_priv=GDP_contri_priv[1][[1]]
## colnames(GDP_contri_priv)=c("Relevant_Date","Private consumption")
##
## #GDP | Expenditure Components of GDP | GFCE
## GDP_contri_govt= data_query_clk_pg(1502400)[1][[1]]
## colnames(GDP_contri_govt)=c("Relevant_Date","Govt. expenditure")
##
## #GDP/Investments
## GDP_contri_iniv= data_query_clk_pg(1502403)[1][[1]]
## colnames(GDP_contri_iniv)=c("Relevant_Date","Investments")
##
## #GDP/Net exports
## GDP_contri_export= data_query_clk_pg(1502397)[1][[1]]
## colnames(GDP_contri_export)=c("Relevant_Date","Net exports")
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(GDP_contri_priv,GDP_contri_govt,GDP_contri_iniv,GDP_contri_export)
##
## h_just=0
## v_just=0.60
##
## num_brek=6
## max_pri_y=120
## min_pri_y=20
##
##
## my_chart_col=c("GRAY 48","BURLYWOOD 1","GOLDEN ROD 1","#BF6E00")
## my_legends_col=c("DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1","#BF6E00")
##
## Qtr_contri_seg_chart=stacked_bar_chart_niif(data_s,
##                                             x_axis_interval="24 month",data_unit="",
##                                             graph_lim=60,negative=TRUE)
##
## ##
## Qtr_contri_seg_title=Qtr_contri_seg_chart[2][[1]]
## Qtr_contri_seg_chart=Qtr_contri_seg_chart[1][[1]]
## Qtr_contri_seg_src=GDP_contri_priv_src
##


## -------------------------------------
#Tax Collection
dir_tax= data_query_clk_pg(1502433)
dir_tax_title=dir_tax[2][[1]]
dir_tax_src=dir_tax[3][[1]]
dir_tax=dir_tax[1][[1]]
colnames(dir_tax)=c("Relevant_Date","","Monthly direct tax (%)")
dir_tax=dir_tax[,c("Relevant_Date","Monthly direct tax (%)")]


#Indirect taxes
indir_tax=data_query_clk_pg(1502431)[1][[1]]
colnames(indir_tax)=c("Relevant_Date","","Monthly indirect tax (%)")
indir_tax=indir_tax[,c("Relevant_Date","Monthly indirect tax (%)")]



## -------------------------------------
data_s=list(dir_tax,indir_tax)

h_just=0
v_just=0.75
h_just_line=1.15
v_just_line=1.15

num_brek=6
max_pri_y=25
min_pri_y=0

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2")


Tax_Col_govt_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",graph_lim=30,negative=FALSE,order_stack = TRUE)


Tax_Col_govt_title=Tax_Col_govt_chart[2][[1]]
Tax_Col_govt_chart=Tax_Col_govt_chart[1][[1]]
Tax_Col_govt_src=dir_tax_src


## -------------------------------------
# Government Market Borrowings
capital_exp= data_query_clk_pg(1502432)
capital_exp_title=capital_exp[2][[1]]
capital_exp_src=capital_exp[3][[1]]
capital_exp=capital_exp[1][[1]]
colnames(capital_exp)=c("Relevant_Date","Monthly capital expenditure (%)")
capital_exp=capital_exp[,c("Relevant_Date","Monthly capital expenditure (%)")]

#
revinue_exp=data_query_clk_pg(1502434)[1][[1]]
colnames(revinue_exp)=c("Relevant_Date","Monthly revenue expenditure (%)")
revinue_exp=revinue_exp[,c("Relevant_Date","Monthly revenue expenditure (%)")]



## -------------------------------------
data_s=list(capital_exp,revinue_exp)

h_just=0
v_just=0.75
h_just_line=1.15
v_just_line=1.15

num_brek=5
max_pri_y=35
min_pri_y=10

my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1")

Govt_Gross_market_Bor_21_chart=stacked_bar_chart_niif(data_s,
                                                      x_axis_interval="12 month",
                                                      data_unit="",
                                                      order_stack = TRUE,
                                                      graph_lim=30,negative=FALSE)

Govt_Gross_market_Bor_21_title=Govt_Gross_market_Bor_21_chart[2][[1]]
Govt_Gross_market_Bor_21_chart=Govt_Gross_market_Bor_21_chart[1][[1]]

Govt_Gross_market_Bor_21_src=capital_exp_src


## -------------------------------------
#Throught Debt Instruments
Mon_debt_funds= data_query_clk_pg(id=318917)
Mon_debt_funds_title=Mon_debt_funds[2][[1]]
Mon_debt_funds_src=Mon_debt_funds[3][[1]]
Mon_debt_funds=Mon_debt_funds[1][[1]]
Mon_debt_funds=Mon_debt_funds%>%mutate(across(c(2:3), .fns = ~./10^9))

colnames(Mon_debt_funds)=c("Relevant_Date","Public and rights issues","Private placement")
Mon_debt_funds=Mon_debt_funds[,c("Relevant_Date","Private placement","Public and rights issues")]



## -------------------------------------
data_s=list(Mon_debt_funds)

h_just=0.60
v_just=0.60
h_just_line=1.85
v_just_line=1.85

num_brek=6
max_pri_y=1400
min_pri_y=0

my_chart_col=c("GRAY 48","GOLDEN ROD 1")
my_legends_col=c("GRAY 48","GOLDEN ROD 1")

Mon_debt_funds_mobi_India_chart=stacked_bar_chart_niif(data_s,data_unit="",
                                                       order_stack = TRUE,
                                                       graph_lim=150,negative=FALSE)

Mon_debt_funds_mobi_India_title=Mon_debt_funds_mobi_India_chart[2][[1]]
Mon_debt_funds_mobi_India_chart=Mon_debt_funds_mobi_India_chart[1][[1]]
Mon_debt_funds_mobi_India_src=Mon_debt_funds_src




## -------------------------------------
#Throught Equity Instruments
Mon_equity_funds= data_query_clk_pg(id=318940)
Mon_equity_funds_title=Mon_equity_funds[2][[1]]
Mon_equity_funds_src=Mon_equity_funds[3][[1]]
Mon_equity_funds=Mon_equity_funds[1][[1]]
Mon_equity_funds=Mon_equity_funds%>%mutate(across(c(2:5), .fns = ~./10^9))

colnames(Mon_equity_funds)=c("Relevant_Date","IPOs","Rights issues","QIPs","Preferential allotments")



## -------------------------------------
data_s=list(Mon_equity_funds)

h_just=0.55
v_just=0.55

num_brek=6
max_pri_y=1400
min_pri_y=0

my_chart_col=c("GRAY 48","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 48","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")

Mon_equity_fu_mob_India_chart=stacked_bar_chart_niif(data_s,
                                                     x_axis_interval="24 month",
                                                     data_unit="",
                                                     order_stack = TRUE,
                                                     graph_lim=150)

Mon_equity_fu_mob_India_title=Mon_equity_fu_mob_India_chart[2][[1]]
Mon_equity_fu_mob_India_chart=Mon_equity_fu_mob_India_chart[1][[1]]
Mon_equity_fu_mob_India_src=Mon_equity_funds_src

min_factor=1



## -------------------------------------
#Domestic Investments
DII_inv_in_India=data_query_clk_pg(id=1418351)
DII_inv_in_India_title=DII_inv_in_India[2][[1]]
DII_inv_in_India_src=DII_inv_in_India[3][[1]]


older_niif=data_query_clk_pg(1418351,150)[1][[1]]
older_niif=mon_year_df_creator(older_niif,keep_col=c("Relevant_Date","Value"),Sum_date = TRUE)

older_niif$Value=older_niif$Value/10^9
colnames(older_niif)=c("Relevant_Date","DII investments")
DII_inv_in_India=older_niif




## -------------------------------------
data_s=list(DII_inv_in_India)

max_overlap=30
my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

num_brek=5
max_pri_y=3000
min_pri_y=2000

h_just=0.25
v_just=0.60
h_just_line=0
v_just_line=0.60

DII_invest_India_chart=stacked_bar_chart_niif(data_s,
                                              x_axis_interval="24 month",
                                              data_unit="",
                                              graph_lim=470,
                                              negative=TRUE,YTD=TRUE,
                                              bar_thick=200,show_shaded=TRUE,
                                              format_date =paste0("%Y"))

Annual_DII_invest_India_title=DII_invest_India_chart[2][[1]]
Annual_DII_invest_India_chart=DII_invest_India_chart[1][[1]]
Annual_DII_invest_India_src=DII_inv_in_India_src
bar_thick=8
Annual_DII_invest_India_chart


## -------------------------------------
#Domestic Investments
DII_inv_in_India=data_query_clk_pg(id=1418351)
DII_inv_in_India_title=DII_inv_in_India[2][[1]]
DII_inv_in_India_src=DII_inv_in_India[3][[1]]


older_niif=data_query_clk_pg(1418351,150)[1][[1]]
older_niif$Value=older_niif$Value/10^9
colnames(older_niif)=c("Relevant_Date","DII investments")
DII_inv_in_India=older_niif




## -------------------------------------
data_s=list(DII_inv_in_India)

max_overlap=30
my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

num_brek=6
max_pri_y=800
min_pri_y=600
h_just=0.25
v_just=0.60
h_just_line=0
v_just_line=0.60

DII_invest_India_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                              graph_lim=100,negative=TRUE,YTD=FALSE,bar_thick=8)

DII_invest_India_title=DII_invest_India_chart[2][[1]]
DII_invest_India_chart=DII_invest_India_chart[1][[1]]
DII_invest_India_src=DII_inv_in_India_src

DII_invest_India_chart


## -------------------------------------
# Net Investments Into Mutual Funds
net_inflo_into_mf= data_query_clk_pg(id=725973)
net_inflo_into_mf_title=net_inflo_into_mf[2][[1]]
net_inflo_into_mf_src=net_inflo_into_mf[3][[1]]
net_inflo_into_mf_data_up=net_inflo_into_mf[4][[1]]
net_inflo_into_mf=net_inflo_into_mf[1][[1]]
net_inflo_into_mf=max_rel_date_replacor(net_inflo_into_mf,net_inflo_into_mf_data_up)

net_inflo_into_mf=net_inflo_into_mf%>%mutate(across(c(2:3), .fns = ~./10^9))

colnames(net_inflo_into_mf)=c("Relevant_Date","Net debt investment","Net equity investment")
net_inflo_into_mf=net_inflo_into_mf[,c("Relevant_Date","Net equity investment","Net debt investment")]

# #HARD CODe::As data for max date  is partial
# net_inflo_into_mf=net_inflo_into_mf[net_inflo_into_mf$Relevant_Date<max(net_inflo_into_mf$Relevant_Date),]



## -------------------------------------
data_s=list(net_inflo_into_mf)
h_just=0.60
v_just=0.60

num_brek=8
max_pri_y=1200
min_pri_y=400

x_axis_interval="24 month"


my_chart_col=c("GRAY 48","GOLDEN ROD 1")
my_legends_col=c("GRAY 48","GOLDEN ROD 1")

net_inflows_mf_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",
                                            order_stack = TRUE,
                                            data_unit="",legend_reverse = TRUE,
                                            graph_lim=100,negative=TRUE,
                                            DATE_HEADER = FALSE)


net_inflows_mf_title=net_inflows_mf_chart[2][[1]]
net_inflows_mf_chart=net_inflows_mf_chart[1][[1]]
net_inflows_mf_src=net_inflo_into_mf_src



## -------------------------------------
#Electricity Generation From Renewables
Mon_gen_frm_renew=data_query_clk_pg(id=725984)
Mon_gen_frm_renew_title=Mon_gen_frm_renew[2][[1]]
Mon_gen_frm_renew_src=Mon_gen_frm_renew[3][[1]]
Mon_gen_frm_renew=Mon_gen_frm_renew[1][[1]]
Mon_gen_frm_renew=Mon_gen_frm_renew%>%mutate(across(c(2:5), .fns = ~./10^9))

colnames(Mon_gen_frm_renew)=c("Relevant_Date","Wind","Solar","Small hydro","Others")
Mon_gen_frm_renew=Mon_gen_frm_renew[,c("Relevant_Date","Small hydro","Wind","Solar","Others")]

older_niif=data_query_clk_pg(894697,150)[1][[1]]
older_niif=older_niif%>%mutate(across(c(2:5), .fns = ~./10^3))

colnames(older_niif)=c("Relevant_Date","Small hydro","Wind","Solar","Others")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_gen_frm_renew$Relevant_Date),]
Mon_gen_frm_renew=rbind(older_niif,Mon_gen_frm_renew)
Mon_gen_frm_renew=Mon_gen_frm_renew[!duplicated(Mon_gen_frm_renew[c("Relevant_Date")]), ]



## -------------------------------------
data_s=list(Mon_gen_frm_renew)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=5
max_pri_y=30
min_pri_y=0

my_chart_col=c("GRAY 48","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 48","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1")



Mon_gen_frm_renew_chart=stacked_bar_chart_niif(data_s,
                                               x_axis_interval="24 month",
                                               data_unit="",
                                               top_column='Others',
                                               order_stack = TRUE,legend_reverse = TRUE,
                                               graph_lim=90,negative=FALSE)

Mon_gen_frm_renew_title=Mon_gen_frm_renew_chart[2][[1]]
Mon_gen_frm_renew_chart=Mon_gen_frm_renew_chart[1][[1]]
Mon_gen_frm_renew_src=Mon_gen_frm_renew_src



## -------------------------------------
# Amount Of Funds Raised
# AIF | Category I
Aif_Fun_rai_1=data_query_clk_pg(id=1501672)
Aif_Fun_rai_1_title=Aif_Fun_rai_1[2][[1]]
Aif_Fun_rai_1_src=Aif_Fun_rai_1[3][[1]]
Aif_Fun_rai_1=Aif_Fun_rai_1[1][[1]]
Aif_Fun_rai_1$Value=roundexcel(Aif_Fun_rai_1$Value/10^9,1)
colnames(Aif_Fun_rai_1)=c("Relevant_Date","Category I")


#AIF | Category II
Aif_Fun_rai_2=data_query_clk_pg(1501699)[1][[1]]
Aif_Fun_rai_2$Value=roundexcel(Aif_Fun_rai_2$Value/10^9,1)
colnames(Aif_Fun_rai_2)=c("Relevant_Date","Category II")
##AIF | Category III
Aif_Fun_rai_3=data_query_clk_pg(1501685)[1][[1]]
Aif_Fun_rai_3$Value=roundexcel(Aif_Fun_rai_3$Value/10^9,1)
colnames(Aif_Fun_rai_3)=c("Relevant_Date","Category III")



## -------------------------------------
data_s=list(Aif_Fun_rai_1,Aif_Fun_rai_2,Aif_Fun_rai_3)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=500
min_pri_y=50



my_chart_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")
my_legends_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")


Qtr_AIF_net_fundrai_chart=stacked_bar_chart_niif(data_s,
                                              x_axis_interval="24 month",
                                              data_unit="",
                                              order_stack = TRUE,
                                              graph_lim=120,negative=TRUE,bar_thick=40,
                                              legend_reverse = TRUE)

Qtr_AIF_net_fundrai_title=Qtr_AIF_net_fundrai_chart[2][[1]]
Qtr_AIF_net_fundrai_chart=Qtr_AIF_net_fundrai_chart[1][[1]]
Qtr_AIF_net_fundrai_src=Aif_Fun_rai_1_src
Qtr_AIF_net_fundrai_chart


## -------------------------------------
# Amount Of Investments Made
# AIF | Category I
Aif_invest_1=data_query_clk_pg(id=1501677)
Aif_invest_1_title=Aif_invest_1[2][[1]]
Aif_invest_1_src=Aif_invest_1[3][[1]]
Aif_invest_1=Aif_invest_1[1][[1]]
Aif_invest_1$Value=Aif_invest_1$Value/10^9
colnames(Aif_invest_1)=c("Relevant_Date","Category I")

#AIF | Category II
Aif_invest_2=data_query_clk_pg(1501701)[1][[1]]
Aif_invest_2$Value=Aif_invest_2$Value/10^9
colnames(Aif_invest_2)=c("Relevant_Date","Category II")

##AIF | Category III
Aif_invest_3=data_query_clk_pg(1501689)[1][[1]]
Aif_invest_3$Value=Aif_invest_3$Value/10^9
colnames(Aif_invest_3)=c("Relevant_Date","Category III")



## -------------------------------------
data_s=list(Aif_invest_1,Aif_invest_2,Aif_invest_3)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=500
min_pri_y=50

my_chart_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")
my_legends_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")

Qtr_AIF_net_invest_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",graph_lim=120,negative=TRUE,bar_thick=40,order_stack = TRUE,legend_reverse = TRUE)

Qtr_AIF_net_invest_title=Qtr_AIF_net_invest_chart[2][[1]]
Qtr_AIF_net_invest_chart=Qtr_AIF_net_invest_chart[1][[1]]
Qtr_AIF_net_invest_src=Aif_invest_1_src
Qtr_AIF_net_invest_chart

##--------------------------------------------

# Amount Of Funds Raised
# AIF | Category I
Aif_Fun_rai_1=data_query_clk_pg(id=1501672)
Aif_Fun_rai_1_src=Aif_Fun_rai_1[3][[1]]
Aif_Fun_rai_1_an=Aif_Fun_rai_1[1][[1]]
Aif_Fun_rai_1_an=mon_year_df_creator(Aif_Fun_rai_1_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)

Aif_Fun_rai_1_an=Aif_Fun_rai_1_an[Aif_Fun_rai_1_an$Relevant_Date>'2013-03-31',]
Aif_Fun_rai_1_an$Value=roundexcel(Aif_Fun_rai_1_an$Value/10^9,1)
colnames(Aif_Fun_rai_1_an)=c("Relevant_Date","Category I")


#AIF | Category II
Aif_Fun_rai_2_an=data_query_clk_pg(1501699)[1][[1]]
Aif_Fun_rai_2_an=mon_year_df_creator(Aif_Fun_rai_2_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)

Aif_Fun_rai_2_an=Aif_Fun_rai_2_an[Aif_Fun_rai_2_an$Relevant_Date>'2013-03-31',]
Aif_Fun_rai_2_an$Value=roundexcel(Aif_Fun_rai_2_an$Value/10^9,1)
colnames(Aif_Fun_rai_2_an)=c("Relevant_Date","Category II")
##AIF | Category III
Aif_Fun_rai_3_an=data_query_clk_pg(1501685)[1][[1]]
Aif_Fun_rai_3_an=mon_year_df_creator(Aif_Fun_rai_3_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
Aif_Fun_rai_3_an=Aif_Fun_rai_3_an[Aif_Fun_rai_3_an$Relevant_Date>'2013-03-31',]
Aif_Fun_rai_3_an$Value=roundexcel(Aif_Fun_rai_3_an$Value/10^9,1)
colnames(Aif_Fun_rai_3_an)=c("Relevant_Date","Category III")

## ------------------------------------------------------------

data_s=list(Aif_Fun_rai_1_an,Aif_Fun_rai_2_an,Aif_Fun_rai_3_an)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=900
min_pri_y=0



my_chart_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")
my_legends_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")


Annual_AIF_net_fundrai_chart=stacked_bar_chart_niif(data_s,
                                              x_axis_interval="24 month",
                                              data_unit="",
                                              format_date ='FY-%y',
                                              order_stack = TRUE,
                                              legend_reverse = TRUE,
                                              graph_lim=200,negative=TRUE,bar_thick=80,YTD = TRUE)

Annual_AIF_net_fundrai_title=Annual_AIF_net_fundrai_chart[2][[1]]
Annual_AIF_net_fundrai_chart=Annual_AIF_net_fundrai_chart[1][[1]]
Annual_AIF_net_fundrai_src=Aif_Fun_rai_1_src
Annual_AIF_net_fundrai_chart
##------------------------------------------

# Amount Of Investments Made
# AIF | Category I
Aif_invest_1=data_query_clk_pg(id=1501677)
Aif_invest_1_src=Aif_invest_1[3][[1]]
Aif_invest_1_an=Aif_invest_1[1][[1]]
Aif_invest_1_an=mon_year_df_creator(Aif_invest_1_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
Aif_invest_1_an=Aif_invest_1_an[Aif_invest_1_an$Relevant_Date>'2013-03-31',]
Aif_invest_1_an$Value=Aif_invest_1_an$Value/10^9
colnames(Aif_invest_1_an)=c("Relevant_Date","Category I")

#AIF | Category II
Aif_invest_2_an=data_query_clk_pg(1501701)[1][[1]]
Aif_invest_2_an=mon_year_df_creator(Aif_invest_2_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
Aif_invest_2_an=Aif_invest_2_an[Aif_invest_2_an$Relevant_Date>'2013-03-31',]
Aif_invest_2_an$Value=Aif_invest_2_an$Value/10^9
colnames(Aif_invest_2_an)=c("Relevant_Date","Category II")

##AIF | Category III
Aif_invest_3_an=data_query_clk_pg(1501689)[1][[1]]
Aif_invest_3_an=mon_year_df_creator(Aif_invest_3_an,
                                  keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
Aif_invest_3_an=Aif_invest_3_an[Aif_invest_3_an$Relevant_Date>'2013-03-31',]
Aif_invest_3_an$Value=Aif_invest_3_an$Value/10^9
colnames(Aif_invest_3_an)=c("Relevant_Date","Category III")


## ---------------------------------------------------------
data_s=list(Aif_invest_1_an,Aif_invest_2_an,Aif_invest_3_an)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=900
min_pri_y=0

my_chart_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")
my_legends_col=c("Category I"="GRAY 88","Category III"="TAN 1","Category II"="GOLDEN ROD 1")


Annual_AIF_net_invest_chart=stacked_bar_chart_niif(data_s,
                                              x_axis_interval="24 month",
                                              data_unit="",
                                              format_date ='FY-%y',
                                              order_stack = TRUE,
                                              legend_reverse = TRUE,
                                              graph_lim=200,negative=TRUE,
                                              bar_thick=80,YTD = TRUE)

Annual_AIF_net_invest_title=Annual_AIF_net_invest_chart[2][[1]]
Annual_AIF_net_invest_chart=Annual_AIF_net_invest_chart[1][[1]]
Annual_AIF_net_invest_src=Aif_invest_1_src
Annual_AIF_net_invest_chart

## -------------------------------------
# Amount Of Funds Raised
# AIF | Category I | Infrastructure Fund
Infra_fun=data_query_clk_pg(id=1501711)
Infra_fun_title=Infra_fun[2][[1]]
Infra_fun_src=Infra_fun[3][[1]]
Infra_fun=Infra_fun[1][[1]]
Infra_fun$Value=Infra_fun$Value/10^9
colnames(Infra_fun)=c("Relevant_Date","Infrastructure")

#AIF | Category I | Social Venture Fund
social_fun=data_query_clk_pg(1501691)[1][[1]]
social_fun$Value=social_fun$Value/10^9
colnames(social_fun)=c("Relevant_Date","Social venture")

#AIF | Category I |Venture Capital Fund
venture_fun=data_query_clk_pg(1501703)[1][[1]]
venture_fun$Value=venture_fun$Value/10^9
colnames(venture_fun)=c("Relevant_Date","Venture capital")

#AIF | Category I | SME Fund
sme_fun=data_query_clk_pg(1501679)[1][[1]]
sme_fun$Value=sme_fun$Value/10^9
colnames(sme_fun)=c("Relevant_Date","SME")



## -------------------------------------
data_s=list(Infra_fun,social_fun,venture_fun,sme_fun)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=180
min_pri_y=25


my_chart_col=c("GRAY 88","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")


Qtr_AIF_CI_net_fundrai_chart=stacked_bar_chart_niif(data_s,
                                                    x_axis_interval="24 month",data_unit="",order_stack = TRUE,
                                                    graph_lim=120,negative=TRUE,bar_thick=40,legend_reverse = TRUE)

Qtr_AIF_CI_net_fundrai_title=Qtr_AIF_CI_net_fundrai_chart[2][[1]]
Qtr_AIF_CI_net_fundrai_chart=Qtr_AIF_CI_net_fundrai_chart[1][[1]]
Qtr_AIF_CI_net_fundrai_src=Infra_fun_src
Qtr_AIF_CI_net_fundrai_chart



## -------------------------------------
# Amount Of Investments Made
# AIF | Category I | Infrastructure Fund
Infra_inv=data_query_clk_pg(id=1501713)
Infra_inv_title=Infra_inv[2][[1]]
Infra_inv_src=Infra_inv[3][[1]]
Infra_inv=Infra_inv[1][[1]]
Infra_inv$Value=Infra_inv$Value/10^9
colnames(Infra_inv)=c("Relevant_Date","Infrastructure")

#AIF | Category I | Social Venture Fund
social_inv=data_query_clk_pg(1501696)[1][[1]]
social_inv$Value=social_inv$Value/10^9
colnames(social_inv)=c("Relevant_Date","Social venture")

#AIF | Category I |Venture Capital Fund
venture_inv=data_query_clk_pg(1501708)[1][[1]]
venture_inv$Value=venture_inv$Value/10^9
colnames(venture_inv)=c("Relevant_Date","Venture capital")

#AIF | Category I | SME Fund
sme_inv=data_query_clk_pg(1501684)[1][[1]]
sme_inv$Value=sme_inv$Value/10^9
colnames(sme_inv)=c("Relevant_Date","SME")



## -------------------------------------
data_s=list(Infra_inv,social_inv,venture_inv,sme_inv)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=9
max_pri_y=180
min_pri_y=25


my_chart_col=c("GRAY 88","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","TAN 1","GOLDEN ROD 1")

Qtr_AIF_CI_net_inv_chart=stacked_bar_chart_niif(data_s,
                                        x_axis_interval="24 month",
                                        order_stack =TRUE,
                                        data_unit="",graph_lim=150,negative=TRUE,
                                        round_integer = TRUE,bar_thick=40,legend_reverse = TRUE)

Qtr_AIF_CI_net_inv_title=Qtr_AIF_CI_net_inv_chart[2][[1]]
Qtr_AIF_CI_net_inv_chart=Qtr_AIF_CI_net_inv_chart[1][[1]]
Qtr_AIF_CI_net_inv_src=Infra_fun_src
Qtr_AIF_CI_net_inv_chart


## -------------------------------------
#Road Construction in India
Road_Con=data_query_clk_pg(id=318872)
Road_Con_title=tolower(Road_Con[2][[1]])
Road_Con_src=Road_Con[3][[1]]
Road_Con=Road_Con[1][[1]]
colnames(Road_Con)=c("Relevant_Date","Roads constructed (Km)","Roads awarded (Km)")


## -------------------------------------
data_s=list(Road_Con)


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=5000
min_pri_y=0


my_chart_col=c("GRAY 48","GOLDEN ROD 1")
my_legends_col=c("GRAY 48","GOLDEN ROD 1")

Mon_road_con_India_chart=stacked_bar_chart_niif(data_s,
                        x_axis_interval="12 month",data_unit="",
                        graph_lim=45,negative=FALSE,SIDE_BAR=TRUE,bar_thick=20)

Mon_road_con_India_title=Mon_road_con_India_chart[2][[1]]
Mon_road_con_India_chart=Mon_road_con_India_chart[1][[1]]
Mon_road_con_India_src=Road_Con_src


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## aaum_t_30=data_query_clk_pg(id=286291)
## aaum_t_30_src=aaum_t_30[3][[1]]
## aaum_t_30=aaum_t_30[1][[1]]
## names(aaum_t_30)[2]='Top 30 cities'
## #%%
## aaum_byt_30=data_query_clk_pg(id=286282)[1][[1]]
## names(aaum_byt_30)[2]='Beyond top 30 cities'


## ----eval=FALSE, include=FALSE--------
## data_s=list(aaum_t_30,aaum_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=50
## min_pri_y=10
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## MF_AAUM_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=50,
##                         negative=TRUE,SIDE_BAR=TRUE,bar_thick=25,
##                         single_bar_width = 100,
##                         order_stack = FALSE)
##
## MF_AAUM_top_city_gr_title=MF_AAUM_top_city_gr_chart[2][[1]]
## MF_AAUM_top_city_gr_chart=MF_AAUM_top_city_gr_chart[1][[1]]
## MF_AAUM_top_city_gr_src=aaum_t_30_src
## MF_AAUM_top_city_gr_chart


## -------------------------------------
#Road Construction in India
# fw_t_30=data_query_clk_pg(id=2214225)
# fw_t_30_src=fw_t_30[3][[1]]
# fw_t_30=fw_t_30[1][[1]]
# fw_t_30$Total=fw_t_30$Total/1000
# names(fw_t_30)[2]="Top 30 cities"
# #%%
# fw_byt_30=data_query_clk_pg(id=2214227)[1][[1]]
# fw_byt_30$Total=fw_byt_30$Total/1000
# names(fw_byt_30)[2]="Beyond top 30 cities"


# ## -------------------------------------
# data_s=list(fw_t_30,fw_byt_30)

# h_just=0
# v_just=0.60
# h_just_line=0
# v_just_line=0.60

# num_brek=5
# max_pri_y=200
# min_pri_y=10


# my_chart_col=c("GRAY 48","GOLDEN ROD 1")
# my_legends_col=c("GRAY 48","GOLDEN ROD 1")

# FW_reg_top_city_gr_chart=stacked_bar_chart_niif(data_s,
#                         x_axis_interval="12 month",data_unit="",
#                         graph_lim=30,negative=TRUE,SIDE_BAR=TRUE,bar_thick=25,order_stack = FALSE)

# FW_reg_top_city_gr_title=FW_reg_top_city_gr_chart[2][[1]]
# FW_reg_top_city_gr_chart=FW_reg_top_city_gr_chart[1][[1]]
# FW_reg_top_city_gr_src=fw_t_30_src
# FW_reg_top_city_gr_chart


# ## -------------------------------------
# #Road Construction in India
# credit_t_30=data_query_clk_pg(id=2214236)
# credit_t_30_src=credit_t_30[3][[1]]
# credit_t_30=credit_t_30[1][[1]]
# credit_t_30$Growth=as.numeric(credit_t_30$Growth)
# names(credit_t_30)[2]='Top 30 cities'
# #%%
# credit_byt_30=data_query_clk_pg(id=2214235)[1][[1]]
# credit_byt_30$Growth=as.numeric(credit_byt_30$Growth)
# names(credit_byt_30)[2]='Beyond top 30 cities'


# ## -------------------------------------
# data_s=list(credit_t_30,credit_byt_30)

# h_just=0
# v_just=0.60
# h_just_line=0
# v_just_line=0.60

# num_brek=5
# max_pri_y=40
# min_pri_y=10


# my_chart_col=c("GRAY 48","GOLDEN ROD 1")
# my_legends_col=c("GRAY 48","GOLDEN ROD 1")

# Credit_top_city_gr_chart=stacked_bar_chart_niif(data_s,
#                         x_axis_interval="12 month",data_unit="",
#                         graph_lim=30,negative=TRUE,SIDE_BAR=TRUE,bar_thick=10,order_stack = FALSE)

# Credit_top_city_gr_title=Credit_top_city_gr_chart[2][[1]]
# Credit_top_city_gr_chart=Credit_top_city_gr_chart[1][[1]]
# Credit_top_city_gr_src=credit_t_30_src
# Credit_top_city_gr_chart


## -------------------------------------
#Power Exchange | IEX | DAM
iex_avg_pri= data_query_clk_pg(2214567)
iex_avg_pri_src=iex_avg_pri[3][[1]]
iex_avg_pri=iex_avg_pri[1][[1]]
iex_avg_pri$Relevant_Date <- as.POSIXct(iex_avg_pri$Relevant_Date,
                                        format="%Y-%m-%d %H:%M:%S")

iex_avg_pri$Relevant_Date <- iex_avg_pri$Relevant_Date - as.difftime(5.5, units="hours")
names(iex_avg_pri)[2]='Avg price'

#Power Exchange | IEX | DAM
iex_min_pri= data_query_clk_pg(2214566)[1][[1]]
iex_min_pri$Relevant_Date <- as.POSIXct(iex_min_pri$Relevant_Date,
                                        format="%Y-%m-%d %H:%M:%S")

iex_min_pri$Relevant_Date <- iex_min_pri$Relevant_Date - as.difftime(5.5, units="hours")

names(iex_min_pri)[2]='Min price'

#Power Exchange | IEX | DAM
iex_max_pri= data_query_clk_pg(2214568)[1][[1]]
iex_max_pri$Relevant_Date <- as.POSIXct(iex_max_pri$Relevant_Date,
                                        format="%Y-%m-%d %H:%M:%S")

iex_max_pri$Relevant_Date <- iex_max_pri$Relevant_Date - as.difftime(5.5, units="hours")

names(iex_max_pri)[2]='Max price'


## -------------------------------------
data_s= list(iex_min_pri,iex_avg_pri,iex_max_pri)


my_legends_col=c("Avg price"="DARK ORANGE 2","Min price"="GOLDEN ROD 1",
                 "Max price"="#BF6E00")

my_line_type=c("Avg price"="solid","Min price"="solid","Max price"="solid")

h_just_line=0
v_just_line=0.60

n_col=5
n_row=2
num_brek=4
max_pri_y=15
min_pri_y=0

elec_spot_pri_chart=multi_line_chart_niif(data_s,
                              x_axis_interval="2 hours",graph_lim=100,
                              negative=FALSE,Hour_minute = TRUE,
                              format_date ='%H:%M',
                              exculde_FY=TRUE)

elec_spot_pri_title=elec_spot_pri_chart[2][[1]]
elec_spot_pri_chart=elec_spot_pri_chart[1][[1]]
elec_spot_pri_src=iex_avg_pri_src
elec_spot_pri_chart


## -------------------------------------
#Agriculture,forestry and fishing
GVA_agri= data_query_clk_pg(1502345)
GVA_agri_title=GVA_agri[2][[1]]
GVA_agri_src=GVA_agri[3][[1]]
GVA_agri=GVA_agri[1][[1]]

colnames(GVA_agri) = c("Relevant_Date","Agriculture")
GVA_agri$Relevant_Date=as.Date(timeLastDayInMonth(GVA_agri$Relevant_Date))

#GVA at basic prices/Manufacturing
GVA_industry= data_query_clk_pg(1502346)[1][[1]]
colnames(GVA_industry) = c("Relevant_Date","Industry")
GVA_industry$Relevant_Date=as.Date(timeLastDayInMonth(GVA_industry$Relevant_Date))

#GVA Services
GVA_services= data_query_clk_pg(1502347)[1][[1]]
colnames(GVA_services) = c("Relevant_Date","Services")
GVA_services$Relevant_Date=as.Date(timeLastDayInMonth(GVA_services$Relevant_Date))
GVA_growth=list(GVA_agri,GVA_industry,GVA_services)




## -------------------------------------
data_s=GVA_growth

max_overlaps =30
h_just_line=0
v_just_line=0.75

n_col=5
n_row=2

num_brek=4
max_pri_y=30
min_pri_y=10

my_legends_col=c("Agriculture"="DARK ORANGE 2","Industry"="GOLDEN ROD 1","Services"="#BF6E00")
my_line_type=c("Agriculture"="solid","Industry"="solid","Services"="solid")



Qtr_real_GVA_com_gr_chart=multi_line_chart_niif(data_s,
                              x_axis_interval="24 month",graph_lim=90,negative=TRUE,
                              PMI_reference=FALSE,BSE_Index=FALSE,
                              legend_key_width=0.27,
                              CPI_reference=FALSE,
                              round_integer=FALSE)

Qtr_real_GVA_com_gr_title=Qtr_real_GVA_com_gr_chart[2][[1]]
Qtr_real_GVA_com_gr_chart=Qtr_real_GVA_com_gr_chart[1][[1]]
Qtr_real_GVA_com_gr_src=GVA_agri_src



## -------------------------------------
#LHS
GDP_private= data_query_clk_pg(1502343)
GDP_private_title=GDP_private[2][[1]]
GDP_private_src=GDP_private[3][[1]]
GDP_private=GDP_private[1][[1]]

colnames(GDP_private) = c("Relevant_Date","Private consumption")
GDP_private$Relevant_Date=as.Date(timeLastDayInMonth(GDP_private$Relevant_Date))

#GVA at basic prices/Manufacturing
GDP_govt_exp= data_query_clk_pg(1502340)[1][[1]]
colnames(GDP_govt_exp) = c("Relevant_Date","Govt. expenditure")
GDP_govt_exp$Relevant_Date=as.Date(timeLastDayInMonth(GDP_govt_exp$Relevant_Date))

#GVA Total
GDP_invest= data_query_clk_pg(1502342)[1][[1]]
colnames(GDP_invest) = c("Relevant_Date","Investments")
GDP_invest$Relevant_Date=as.Date(timeLastDayInMonth(GDP_invest$Relevant_Date))
GDP_growth=list(GDP_private,GDP_govt_exp,GDP_invest)



## -------------------------------------
data_s=GDP_growth

max_overlaps =30
h_just_line=0
v_just_line=0.75

n_col=5
n_row=2

num_brek=4
max_pri_y=30
min_pri_y=10

my_legends_col=c("Private consumption"="GRAY 48","Govt. expenditure"="GOLDEN ROD 1","Investments"="#BF6E00")

my_line_type=c("Private consumption"="solid","Govt. expenditure"="solid",
                "Investments"="solid")

Qtr_real_GDP_seg_gr_chart=multi_line_chart_niif(data_s,
                                                x_axis_interval="24 month",graph_lim=90,negative=TRUE,
                                                PMI_reference=FALSE,BSE_Index=FALSE,legend_key_width=0.27,led_position="center")

Qtr_real_GDP_seg_gr_title=Qtr_real_GDP_seg_gr_chart[2][[1]]
Qtr_real_GDP_seg_gr_chart=Qtr_real_GDP_seg_gr_chart[1][[1]]
Qtr_real_GDP_seg_gr_src=GDP_private_src


## ----include=FALSE--------------------
#All Scheduled Commercial Banks
CD_Ratio= data_query_clk_pg(id=1443680)
CD_Ratio_title=tolower(CD_Ratio[2][[1]])
CD_Ratio_src=CD_Ratio[3][[1]]
CD_Ratio=CD_Ratio[1][[1]]

older_niif=data_query_clk_pg(1443682,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Ratio")
older_niif=older_niif[older_niif$Relevant_Date<min(CD_Ratio$Relevant_Date),]
CD_Ratio=rbind(older_niif,CD_Ratio)
CD_Ratio$Relevant_Date=as.Date(timeLastDayInMonth(CD_Ratio$Relevant_Date))
names(CD_Ratio)[2]="Credit-to-deposit ratio (%)"

# CD_Ratio=CD_Ratio[CD_Ratio$Relevant_Date>='2023-06-30',]
##
##Credit Deposit Difference without HDFC
# CD_Ratio_excl_hdfc= data_query_clk_pg(id=2232825)[1][[1]]
# older_niif=data_query_clk_pg(1443682,150)[1][[1]]
# colnames(older_niif)=c("Relevant_Date","Ratio")
# older_niif=older_niif[older_niif$Relevant_Date<min(CD_Ratio_excl_hdfc$Relevant_Date),]
# CD_Ratio_excl_hdfc=rbind(older_niif,CD_Ratio_excl_hdfc)

# CD_Ratio_excl_hdfc$Relevant_Date=as.Date(timeLastDayInMonth(CD_Ratio_excl_hdfc$Relevant_Date))
# names(CD_Ratio_excl_hdfc)[2]="Credit-to-deposit ratio excluding merger (%)"


## -------------------------------------
# data_s=list(CD_Ratio,CD_Ratio_excl_hdfc)
data_s=list(CD_Ratio)


h_just_line=0
v_just_line=0.75
my_chart_col=c("GOLDEN ROD 1")

num_brek=6
max_pri_y=80
min_pri_y=60

# my_legends_col=c("Credit-to-deposit ratio including merger (%)"="GRAY 48",
#                  "Credit-to-deposit ratio (%)"="GOLDEN ROD 1")
#
# my_line_type=c("Credit-to-deposit ratio including merger (%)"="dotted",
#                "Credit-to-deposit ratio (%)"="solid")

my_legends_col=c("Credit-to-deposit ratio (%)"="GOLDEN ROD 1")
my_line_type=c("Credit-to-deposit ratio (%)"="solid")

CD_Ratio_chart=multi_line_chart_niif(data_s,
                                    x_axis_interval="24 month",graph_lim=90,
                                    negative=TRUE,
                                    PMI_reference=FALSE,BSE_Index=FALSE,
                                    legend_key_width=0.27,
                                    led_position="center")


CD_Ratio_title=CD_Ratio_chart[2][[1]]
CD_Ratio_chart=CD_Ratio_chart[1][[1]]
draw_key_line=draw_key_rect
CD_Ratio_src=CD_Ratio_src
CD_Ratio_chart


## -------------------------------------
#Labour Force Participation
partici_rate=data_query_clk_pg(id=1383578)
partici_rate_title=tolower(partici_rate[2][[1]])
partici_rate_src=partici_rate[3][[1]]
partici_rate=partici_rate[1][[1]]
colnames(partici_rate)=c("Relevant_Date","Labour force participation rate")

#Unemployment Rate
unemployment_rate=data_query_clk_pg(id=1383579)[1][[1]]
colnames(unemployment_rate)=c("Relevant_Date","Unemployment rate")
Quarterly_PLFS=list(partici_rate,unemployment_rate)


## -------------------------------------
data_s=Quarterly_PLFS

n_row=1
n_col=2

h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=50
min_pri_y=0

my_legends_col=c("Labour force participation rate"="GRAY 48",
                 "Unemployment rate"="GOLDEN ROD 1")

my_line_type=c("Labour force participation rate" ="solid",
               "Unemployment rate"="solid")

Quarterly_PLFS_chart=multi_line_chart_niif(data_s,
                                           x_axis_interval="12 month",
                                           graph_lim=90,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE)

Quarterly_PLFS_title=Quarterly_PLFS_chart[2][[1]]
Quarterly_PLFS_chart=Quarterly_PLFS_chart[1][[1]]
Quarterly_PLFS_src=partici_rate_src


## ----eval=FALSE, include=FALSE--------
## #Avg Monthly Wages
## MGNREGA_Wages=data_query_clk_pg(id=725978)
## MGNREGA_Wages_title=MGNREGA_Wages[2][[1]]
## MGNREGA_Wages_src=MGNREGA_Wages[3][[1]]
## Avg_Wages=MGNREGA_Wages[1][[1]]
## colnames(Avg_Wages)=c("Relevant_Date","MNREGA wages")
##
## #Daily Avg Agri Wages For NIIF
## Agri_Wages=data_query_clk_pg(id=1384192)[1][[1]]
## colnames(Agri_Wages)=c("Relevant_Date","Agricultural wages")
##
## #Daily Avg Non Agri Wages For NIIF
## Non_agri_Wages=data_query_clk_pg(id=1384193)[1][[1]]
## colnames(Non_agri_Wages)=c("Relevant_Date","Non-agricultural wages")
##
## MGNREGA_Wages=list(Avg_Wages)
##


## ----eval=FALSE, include=FALSE--------
## data_s=MGNREGA_Wages
##
## h_just_line=0
## v_just_line=0.40
##
## num_brek=4
## max_pri_y=400
## min_pri_y=0
## n_row=1
## n_col=3
##
## Position="left"
## my_legends_col=c("MNREGA wages"="GRAY 48")
## my_line_type=c("MNREGA wages" ="solid")
##
## MGNREGA_Wages_chart=
## multi_line_chart_niif(data_s,x_axis_interval="24 month",graph_lim=20,negative=FALSE,PMI_reference=FALSE,BSE_Index=FALSE)
##
## MGNREGA_Wages_title=MGNREGA_Wages_chart[2][[1]]
## MGNREGA_Wages_chart=MGNREGA_Wages_chart[1][[1]]
## MGNREGA_Wages_src=MGNREGA_Wages_src
##
##


## -------------------------------------
#Monthly Composition
Mon_Trade_Com= data_query_clk_pg(id=1502373)
Mon_Trade_Com_src=Mon_Trade_Com[3][[1]]

#Export
older_exp=data_query_clk_pg(1502373)[1][[1]]
older_exp$Value=older_exp$Value/10^9
colnames(older_exp)=c("Relevant_Date","Merchandize exports")

#IMPORT
older_imp=data_query_clk_pg(1502371)[1][[1]]
older_imp$Value=older_imp$Value/10^9
colnames(older_imp)=c("Relevant_Date","Merchandize imports")

#TRADE
older_trd=data_query_clk_pg(1502375)[1][[1]]
older_trd$Value=older_trd$Value/10^9
colnames(older_trd)=c("Relevant_Date","Trade balance")

Mon_Trade_Com=list(older_exp,older_imp,older_trd)


## -------------------------------------
data_s=Mon_Trade_Com

h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=80
min_pri_y=40
neagative=TRUE
n_row=1
n_col=3

Position="left"
my_legends_col=c("Merchandize exports"="GRAY 48",
                 "Merchandize imports"="GOLDEN ROD 1" ,
                 "Trade balance"="DARK ORANGE 2")
my_line_type=c("Merchandize exports" ="solid","Merchandize imports"="solid","Trade balance"="solid")

Mon_Trade_Compo_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",graph_lim =90)

Mon_Trade_Compo_title=Mon_Trade_Compo_chart[2][[1]]
Mon_Trade_Compo_chart=Mon_Trade_Compo_chart[1][[1]]
Mon_Trade_Compo_src=Mon_Trade_Com_src



## -------------------------------------
#Export
#Services exports
Services_exp= data_query_clk_pg(1502367)
Services_exp_title=tolower(Services_exp[2][[1]])
Services_exp_src=Services_exp[3][[1]]
Services_exp=Services_exp[1][[1]]
Services_exp$Value=Services_exp$Value/10^9
colnames(Services_exp)=c("Relevant_Date","Services exports")


#IMPORT
Services_imp=data_query_clk_pg(1502360)[1][[1]]
Services_imp$Value=Services_imp$Value/10^9
colnames(Services_imp)=c("Relevant_Date","Services imports")

#Trade balance
trade_defi=data_query_clk_pg(1502364)[1][[1]]
trade_defi$Value=trade_defi$Value/10^9
colnames(trade_defi)=c("Relevant_Date","Services balance")


Mon_services=list(Services_exp,Services_imp,trade_defi)



## -------------------------------------
data_s=Mon_services

h_just_line=0
v_just_line=0.60

num_brek=7
max_pri_y=35
min_pri_y=0
neagative=TRUE
n_row=1
n_col=3

Position="left"
my_legends_col=c("Services exports"="GRAY 48", "Services imports"="GOLDEN ROD 1" ,
                 "Services balance"="DARK ORANGE 2")
my_line_type=c("Services exports" ="solid","Services imports"="solid","Services balance"="solid")


Mon_services_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",graph_lim=90,round_integer = TRUE)

Mon_services_title=Mon_services_chart[2][[1]]
Mon_services_chart=Mon_services_chart[1][[1]]
Mon_services_src=Services_exp_src
Mon_services_chart


## -------------------------------------
# #Major Sectoral Indices
# Mjr_Sect_Indi= data_query_clk_pg(id=1384047)
# Mjr_Sect_Indi_title=Mjr_Sect_Indi[2][[1]]
# Mjr_Sect_Indi_src=Mjr_Sect_Indi[3][[1]]
# Mjr_Sect_Indi=Mjr_Sect_Indi[1][[1]]

# colnames(Mjr_Sect_Indi)=c("Relevant_Date","Bank","Power","FMCG","Health care","Capital Goods")
# Mjr_Sect_Indi=Mjr_Sect_Indi[,c("Relevant_Date","Bank","Capital Goods","FMCG","Health care","Power")]





# ## -------------------------------------
# data_s=list(Mjr_Sect_Indi)
# #
# h_just_line=0.55
# v_just_line=0.55

# num_brek=5
# max_pri_y=50000
# min_pri_y=0


# n_row=1
# n_col=5

# my_legends_col=c("Bank"="black","Capital Goods"="GOLDEN ROD 1","FMCG"="BURLYWOOD 1",
#                  "Health care"="GRAY 48","Power"="#BF6E00")

# my_line_type=c("Bank"="solid","Capital Goods"="solid","FMCG"="solid",
#               "Health care"="solid","Power"="solid")

# BSE_sectoral_indices_chart=multi_line_chart_niif(data_s,
#                                                  x_axis_interval="24 month",
#                                                  graph_lim=20,negative=TRUE,PMI_reference=FALSE,
#                                                  BSE_Index=FALSE,legend_key_width=1)

# BSE_sectoral_indices_title=BSE_sectoral_indices_chart[2][[1]]
# BSE_sectoral_indices_chart=BSE_sectoral_indices_chart[1][[1]]

# BSE_sectoral_indices_src=Mjr_Sect_Indi_src



## -------------------------------------
#Cash Circulation
Core_industry_gr= data_query_clk_pg(id=724219)
Core_industry_gr_title=Core_industry_gr[2][[1]]
Core_industry_gr_src=Core_industry_gr[3][[1]]
Core_industry_gr=Core_industry_gr[1][[1]]
colnames(Core_industry_gr) = c("Relevant_Date","Core industry growth")


#IIP
IIP_gr= data_query_clk_pg(id=1288704)
IIP_gr_title=IIP_gr[2][[1]]
IIP_gr_src=IIP_gr[3][[1]]
IIP_gr=IIP_gr[1][[1]]



#NOTE:In this case we are taking max date of this two

d1=data.frame(date1=c(max(Core_industry_gr$Relevant_Date),max(IIP_gr$Relevant_Date)))
common_max=max(as.Date(d1$date1))
IIP_gr=IIP_gr[IIP_gr$Relevant_Date<=common_max,]
Core_industry_gr=Core_industry_gr[Core_industry_gr$Relevant_Date<=common_max,]

#HARD CODE
#NOTE:some times IIP lags 1 month

if (max(IIP_gr$Relevant_Date)<max(Core_industry_gr$Relevant_Date)){
   d2=data.frame(Relevant_Date=c(max(Core_industry_gr$Relevant_Date),max(IIP_gr$Relevant_Date)))
   d2$Relevant_Date=as.Date(d2$Relevant_Date)
   d2$growth=IIP_gr[IIP_gr$Relevant_Date==max(IIP_gr$Relevant_Date),'growth'][1][[1]]
   d2=d2[d2$Relevant_Date>max(IIP_gr$Relevant_Date),]
   IIP_gr=rbind(IIP_gr,d2)
}

colnames(IIP_gr) = c("Relevant_Date","IIP growth")



## -------------------------------------
data_s=list(IIP_gr,Core_industry_gr)

n_col=6
n_row=1

my_legends_col=c("IIP growth"="GRAY 48","Core industry growth"="GOLDEN ROD 1")
my_line_type=c("IIP growth"="solid","Core industry growth"="solid")

num_brek=6
max_pri_y=16
min_pri_y=10

h_just_line=0
v_just_line=0.75
legend_key_size=0

Index_of_Ind_Prod_IIP_chart=multi_line_chart_niif(data_s,
                          x_axis_interval="24 month",graph_lim=120,
                          round_integer=TRUE,
                          negative=TRUE,PMI_reference=FALSE,
                          BSE_Index=FALSE,legend_key_width=0.27)

Index_of_Ind_Prod_IIP_title=Index_of_Ind_Prod_IIP_chart[2][[1]]
Index_of_Ind_Prod_IIP_title=paste0(Index_of_Ind_Prod_IIP_title)
Index_of_Ind_Prod_IIP_chart=Index_of_Ind_Prod_IIP_chart[1][[1]]
Index_of_Ind_Prod_IIP_src=Core_industry_gr_src




## -------------------------------------
#Consumer Price Inflation
Retail_Infl=data_query_clk_pg(id=725955)
Retail_Infl_title=tolower(Retail_Infl[2][[1]])
Retail_Infl_src=Retail_Infl[3][[1]]
Retail_Infl=Retail_Infl[1][[1]]
Retail_Infl=subset(Retail_Infl, Total!= Inf)

Retail_Infl_wt_Food_Fu_Light=Retail_Infl[,c("Relevant_Date","Total")]
older_niif=data_query_clk_pg(1384196,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Retail_Infl_wt_Food_Fu_Light$Relevant_Date),]
Retail_Infl_wt_Food_Fu_Light=rbind(older_niif,Retail_Infl_wt_Food_Fu_Light)
colnames(Retail_Infl_wt_Food_Fu_Light)=c("Relevant_Date",
                                      "Core inflation: CPI excluding food and beverages, and fuel and light")

##
##
Retail_Infl=data_query_clk_pg(id=1445961)
Retail_Infl=Retail_Infl[1][[1]]
Retail_Infl=subset(Retail_Infl, Inflation!= Inf)

cpi_Retail_Infl=Retail_Infl[,c("Relevant_Date","Inflation")]
colnames(cpi_Retail_Infl)=c("Relevant_Date","CPI")
cpi_Retail_Infl=list(cpi_Retail_Infl,Retail_Infl_wt_Food_Fu_Light)


## -------------------------------------
data_s=cpi_Retail_Infl

line_type=2
h_just_line=0
v_just_line=0.75

num_brek=5
max_pri_y=12
min_pri_y=0

legend_key_width=0.27

my_legends_col=c("CPI"="GRAY 48",
                 "Core inflation: CPI excluding food and beverages, and fuel and light"="GOLDEN ROD 1")

my_line_type=c("CPI"="solid",
               "Core inflation: CPI excluding food and beverages, and fuel and light"="solid")


CPI_Infl_Tar_range_chart=multi_line_chart_niif(data_s,
                                               x_axis_interval="24 month",graph_lim=90,
                                               negative=FALSE,PMI_reference=FALSE,
                                               BSE_Index=FALSE,legend_key_width=0.27,
                                               CPI_reference=TRUE,round_integer=TRUE)

CPI_Infl_Tar_range_title=CPI_Infl_Tar_range_chart[2][[1]]
CPI_Infl_Tar_range_chart=CPI_Infl_Tar_range_chart[1][[1]]
CPI_Infl_Tar_range_src=Retail_Infl_src




## ----eval=FALSE, include=FALSE--------
## #India G-sec and Treasury Bill
## # India_G_sec_bench_trea= data_query_clk_pg(id=725977)
## # India_G_sec_bench_trea_title=India_G_sec_bench_trea[2][[1]]
## # India_G_sec_bench_trea_src=India_G_sec_bench_trea[3][[1]]
## # India_G_sec_bench_trea=India_G_sec_bench_trea[1][[1]]
## #
## # India_G_sec_bench_trea_T_bill=India_G_sec_bench_trea[,c("Relevant_Date","Total_Repo")]
## # colnames(India_G_sec_bench_trea_T_bill)=c("Relevant_Date","3-m T-bill")
## #
## # India_G_sec_bench_trea_G_Sec=India_G_sec_bench_trea[,c("Relevant_Date","Total_Rev_Repo")]
## # colnames(India_G_sec_bench_trea_G_Sec)=c("Relevant_Date","10-yr G-Sec")
## #
## # #HARD_CODE_due to x axis issue showing from Jan 13 instead of dec12
## # India_G_sec_bench_trea_T_bill=India_G_sec_bench_trea_T_bill[India_G_sec_bench_trea_T_bill$Relevant_Date>="2013-01-01",]
## #
## # India_G_sec_bench_trea_G_Sec=India_G_sec_bench_trea_G_Sec[India_G_sec_bench_trea_G_Sec$Relevant_Date>="2013-01-01",]
## #
## # India_G_sec_bench_trea_T_bill=list(India_G_sec_bench_trea_G_Sec,India_G_sec_bench_trea_T_bill)
##
##
##


## ----eval=FALSE, include=FALSE--------
## # data_s=India_G_sec_bench_trea_T_bill
## #
## # my_legends_col=c("10-yr G-Sec"="GOLDEN ROD 1",
## #                  "3-m T-bill"="GRAY 48")
## #
## #
## # my_line_type=c("10-yr G-Sec"="solid",
## #               "3-m T-bill"="solid")
## #
## #
## # h_just_line=0
## # v_just_line=0.75
## #
## # num_brek=5
## # max_pri_y=12
## # min_pri_y=0
## #
## # my_chart_col=c("GOLDEN ROD 1","GRAY 48")
## # my_legends_col=c("GOLDEN ROD 1","GRAY 48")
## #
## # India_G_sec_bill_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
## #                                              graph_lim=30,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE,
## #                                              legend_key_width=0.27,Position="left",CPI_reference=FALSE,
## #                                              round_integer=FALSE,Repo=TRUE)
## #
## #
## # India_G_sec_bill_title=India_G_sec_bill_chart[2][[1]]
## # India_G_sec_bill_chart=India_G_sec_bill_chart[1][[1]]
## # India_G_sec_bill_src=India_G_sec_bench_trea_src
##
##


## -------------------------------------
# 725990
#Monthly PMI
PMI_mnf_Services= data_query_clk_pg(2314964)
PMI_mnf_Services_src=PMI_mnf_Services[3][[1]]
PMI_mnf_Services=PMI_mnf_Services[1][[1]]
PMI_mnf_Services=PMI_mnf_Services[!duplicated(PMI_mnf_Services),]
names(PMI_mnf_Services)[2]="PMI manufacturing"

pmi_ser=data_query_clk_pg(2314963)[1][[1]]
names(pmi_ser)[2]="PMI services"




## -------------------------------------
data_s=list(PMI_mnf_Services,pmi_ser)

my_line_type=c("PMI manufacturing"="solid", "PMI services"="solid")
my_legends_col=c("PMI manufacturing"="GOLDEN ROD 1","PMI services"="GRAY 48")

n_col=6
n_row=1

num_brek=4
max_pri_y=65
min_pri_y=40

h_just_line=0.45
v_just_line=1.75
legend_key_size=0

Mon_PMI_mnf_Ser_8_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                              graph_lim=100,negative=TRUE,
                                              PMI_reference=FALSE,BSE_Index=FALSE)

Mon_PMI_mnf_Ser_8_title=Mon_PMI_mnf_Ser_8_chart[2][[1]]
Mon_PMI_mnf_Ser_8_chart=Mon_PMI_mnf_Ser_8_chart[1][[1]]
Mon_PMI_mnf_Ser_8_src=PMI_mnf_Services_src

Mon_PMI_mnf_Ser_8_chart


## -------------------------------------
# 725961
#Monthly PMI
PMI_mjr_economies= data_query_clk_pg(id=2314956)
PMI_mjr_economies_src=PMI_mjr_economies[3][[1]]
PMI_india=PMI_mjr_economies[1][[1]]
names(PMI_india)[2]='India'

PMI_usa=data_query_clk_pg(id=2314962)[1][[1]]
names(PMI_usa)[2]='USA'

PMI_uk=data_query_clk_pg(id=2314961)[1][[1]]
names(PMI_uk)[2]='UK'

PMI_brazil=data_query_clk_pg(id=2314960)[1][[1]]
names(PMI_brazil)[2]='Brazil'

PMI_china=data_query_clk_pg(id=2314959)[1][[1]]
names(PMI_china)[2]='China'

PMI_japan=data_query_clk_pg(id=2314958)[1][[1]]
names(PMI_japan)[2]='Japan'

PMI_france=data_query_clk_pg(id=2314955)[1][[1]]
names(PMI_france)[2]='France'

PMI_germany=data_query_clk_pg(id=2314957)[1][[1]]
names(PMI_germany)[2]='Germany'




## -------------------------------------
data_s=list(PMI_usa,PMI_uk,PMI_india,PMI_brazil,PMI_china,PMI_japan,PMI_france,PMI_germany)

legend_key_width=0.75


max_overlaps =30
num_brek=8
max_pri_y=70
min_pri_y=35
#

h_just_line=0
v_just_line=0.75

n_col=8
n_row=1



my_chart_col=c("USA"="GRAY 48","UK"="GOLDEN ROD 1","India"="#EE9572","Brazil"="DARK ORANGE 2",
               "China"="TAN 1","Japan"="GRAY 88","France"="BURLYWOOD 1","Germany"="#BF6E00")


my_legends_col=c("USA"="GRAY 48","UK"="GOLDEN ROD 1","India"="#EE9572","Brazil"="DARK ORANGE 2",
               "China"="TAN 1","Japan"="GRAY 88","France"="BURLYWOOD 1","Germany"="#BF6E00")

my_line_type=c("USA"="solid","UK"="solid","India"="dashed","Brazil"="solid","China"="solid",
               "Japan"="solid","France"="solid","Germany"="solid")

PMI_mjr_economies_9_chart=multi_line_chart_niif(data_s,
                                                x_axis_interval="6 month",
                                                graph_lim=90,negative=TRUE,
                                                PMI_reference=TRUE,
                                                BSE_Index=FALSE,legend_key_width=1,

                                                CPI_reference=FALSE,
                                                round_integer=FALSE,key_spacing=2)



PMI_mjr_economies_9_title=PMI_mjr_economies_9_chart[2][[1]]
PMI_mjr_economies_9_chart=PMI_mjr_economies_9_chart[1][[1]]
PMI_mjr_economies_9_src=PMI_mjr_economies_src
draw_key_line=draw_key_rect
Position="left"



## -------------------------------------
#return yoy
# BSE_SENSEX_Index
BSE_SENSEX_Index= data_query_clk_pg(id=2314934)
BSE_SENSEX_Index_title=BSE_SENSEX_Index[2][[1]]
BSE_SENSEX_Index_src=BSE_SENSEX_Index[3][[1]]
BSE_SENSEX_Index=BSE_SENSEX_Index[1][[1]]

colnames(BSE_SENSEX_Index) = c("Relevant_Date","BSE sensex")
BSE_SENSEX_Index$Relevant_Date=as.Date(timeLastDayInMonth(BSE_SENSEX_Index$Relevant_Date))

# NIFTY_50_Index
NIFTY_50_Index= data_query_clk_pg(id=318890)[1][[1]]
colnames( NIFTY_50_Index) = c("Relevant_Date","NSE Nifty 50")
NIFTY_50_Index$Relevant_Date=as.Date(timeLastDayInMonth(NIFTY_50_Index$Relevant_Date))

#Dow_Jones_Index
Dow_Jones_Index= data_query_clk_pg(id=1525124)[1][[1]]
colnames(Dow_Jones_Index) = c("Relevant_Date","Dow Jones")
Dow_Jones_Index$Relevant_Date=as.Date(timeLastDayInMonth(Dow_Jones_Index$Relevant_Date))


#S&P_Index
SP_Index= data_query_clk_pg(id=1525127)[1][[1]]
colnames(SP_Index) = c("Relevant_Date","S&P 500")
SP_Index$Relevant_Date=as.Date(timeLastDayInMonth(SP_Index$Relevant_Date))


#Singapore_Index
Singapore_Index= data_query_clk_pg(id=1525128)[1][[1]]
colnames(Singapore_Index) = c("Relevant_Date","Singapore")
Singapore_Index$Relevant_Date=as.Date(timeLastDayInMonth(Singapore_Index$Relevant_Date))


#KOSPI_Index
KOSPI_Index= data_query_clk_pg(id= 1525126)[1][[1]]
colnames(KOSPI_Index) = c("Relevant_Date","KOSPI")
KOSPI_Index$Relevant_Date=as.Date(timeLastDayInMonth(KOSPI_Index$Relevant_Date))


#CAC_40_Index
CAC_40_Index= data_query_clk_pg(id=1525120)[1][[1]]
colnames(CAC_40_Index) = c("Relevant_Date","CAC40")
CAC_40_Index$Relevant_Date=as.Date(timeLastDayInMonth(CAC_40_Index$Relevant_Date))




#Hang_Seng_Index
Hang_Seng_Index= data_query_clk_pg(id=1525125)[1][[1]]
colnames(Hang_Seng_Index) = c("Relevant_Date","Hang Seng")
Hang_Seng_Index$Relevant_Date=as.Date(timeLastDayInMonth(Hang_Seng_Index$Relevant_Date))


#FTSE_100_Index
FTSE_100_Index=data_query_clk_pg(id=1525121)[1][[1]]
colnames(FTSE_100_Index) = c("Relevant_Date","FTSE 100")
FTSE_100_Index$Relevant_Date=as.Date(timeLastDayInMonth(FTSE_100_Index$Relevant_Date))


#NIKKEI_225_Index
NIKKEI_225_Index=data_query_clk_pg(id=1525122)[1][[1]]
colnames(NIKKEI_225_Index) = c("Relevant_Date","NIKKEI 225")
NIKKEI_225_Index$Relevant_Date=as.Date(timeLastDayInMonth(NIKKEI_225_Index$Relevant_Date))




Global_equity=list(BSE_SENSEX_Index,NIFTY_50_Index,Dow_Jones_Index,SP_Index,NIKKEI_225_Index,FTSE_100_Index,
                   Hang_Seng_Index,CAC_40_Index,KOSPI_Index,Singapore_Index)




## -------------------------------------
data_s=Global_equity

# legend_key_width=0.50

max_overlaps =30
h_just_line=0
v_just_line=0.75

n_col=5
n_row=2


num_brek=8
max_pri_y=90
min_pri_y=30

my_legends_col=c("BSE Sensex"="GRAY 48",
                 "NSE Nifty 50"="GOLDEN ROD 1",
                 "Dow Jones "="TAN 1",
                 "S&P 500"="GRAY 88",
                 "NIKKEI 225"="#BF6E00",
                 "Hang Seng"="#0541B4",
                 "CAC40"="#00B050",
                 "KOSPI"="BURLYWOOD 1",
                 "FTSE 100"="GRAY 96",
                 "Singapore"="RED 3")

my_line_type=c("BSE Sensex"="solid",
               "NSE Nifty 50"="solid",
               "Dow Jones "="solid",
               "S&P 500"="solid",
               "NIKKEI 225"="solid",
               "Hang Seng"="solid",
               "CAC40"="solid","KOSPI"="solid",
               "FTSE 100"="solid",
               "Singapore"="solid")


Mon_Nft_Sen_gbl_indices_chart=multi_line_chart_niif(data_s,x_axis_interval="12 month",
                                                    graph_lim=30,negative=TRUE,
                                                    PMI_reference=FALSE,
                                                    BSE_Index=TRUE,
                                                    legend_key_width=0.50,
                                                    led_position='left',
                                                    CPI_reference=FALSE,
                                                    round_integer=FALSE,key_spacing=1)

Mon_Nft_Sen_gbl_indices_title=Mon_Nft_Sen_gbl_indices_chart[2][[1]]
Mon_Nft_Sen_gbl_indices_chart=Mon_Nft_Sen_gbl_indices_chart[1][[1]]
Mon_Nft_Sen_gbl_indices_src=BSE_SENSEX_Index_src



## ----include=FALSE--------------------
#IRB InvIT
IRB_InvIT= data_query_clk_pg(id=724552)
IRB_InvIT_title=IRB_InvIT[2][[1]]
IRB_InvIT_src=IRB_InvIT[3][[1]]
IRB_InvIT=IRB_InvIT[1][[1]]
colnames(IRB_InvIT) = c("Relevant_Date","IRB InvIT")

#INDIGRID
India_Grid_InvIT= data_query_clk_pg(id=724553)
India_Grid_InvIT=India_Grid_InvIT[1][[1]]
colnames(India_Grid_InvIT) = c("Relevant_Date","India Grid InvIT")

#EMBASSY
Embassy_REIT= data_query_clk_pg(id=724555)
Embassy_REIT=Embassy_REIT[1][[1]]
colnames(Embassy_REIT) = c("Relevant_Date","Embassy REIT")

#Mindspace
Mindspace_REIT= data_query_clk_pg(id=724557)
Mindspace_REIT=Mindspace_REIT[1][[1]]
colnames(Mindspace_REIT) = c("Relevant_Date","Mindspace REIT")

#BrookField_India REIT
BrookField_India_REIT= data_query_clk_pg(id=724559)
BrookField_India_REIT=BrookField_India_REIT[1][[1]]
colnames(BrookField_India_REIT) = c("Relevant_Date","BrookField India REIT")

#PGCIL_InvIT
PGCIL_InvIT= data_query_clk_pg(id=724561)
PGCIL_InvIT=PGCIL_InvIT[1][[1]]
colnames(PGCIL_InvIT) = c("Relevant_Date","PGCIL InvIT")

#NHIT InvIT
NHIT_InvIT= data_query_clk_pg(id=1684508)
NHIT_InvIT=NHIT_InvIT[1][[1]]
colnames(NHIT_InvIT) = c("Relevant_Date","NHIT InvIT")

#SHREM InvIT
SHREM_InvIT= data_query_clk_pg(id=1684506)
SHREM_InvIT=SHREM_InvIT[1][[1]]
colnames(SHREM_InvIT) = c("Relevant_Date","SHREM InvIT")


REITs_InvITs=list(Embassy_REIT,BrookField_India_REIT,Mindspace_REIT,India_Grid_InvIT,IRB_InvIT,PGCIL_InvIT)



## -------------------------------------
data_s=REITs_InvITs

n_row=2
n_col=4
legend_key_width=0.27


h_just_line=0
v_just_line=0.75
max_overlaps =30
num_brek=5
max_pri_y=500
min_pri_y=0

my_legends_col=c("Embassy REIT"="GRAY 48",
                 "BrookField India REIT"="GOLDEN ROD 1",
                 "Mindspace REIT"="TAN 1",
                 "India Grid InvIT"="DARK ORANGE 2",
                 "IRB InvIT"="BURLYWOOD 1",
                 "PGCIL InvIT"="#BF6E00"
                 )

my_line_type=c("Embassy REIT"="solid",
               "BrookField India REIT"="solid",
               "Mindspace REIT"="solid",
               "India Grid InvIT"="solid",
               "IRB InvIT"="solid",
               "PGCIL InvIT"="solid"
               )

Daily_market_pri_REITs_InvITs_chart=multi_line_chart_niif(data_s,
                                                x_axis_interval="12 month",graph_lim=90,
                                                negative=TRUE,PMI_reference=FALSE,
                                                    BSE_Index=FALSE,legend_key_width=1,led_position="left",DATE_HEADER=FALSE,
                                                key_spacing=1)

Daily_market_pri_REITs_InvITs_title=Daily_market_pri_REITs_InvITs_chart[2][[1]]
Daily_market_pri_REITs_InvITs_chart=Daily_market_pri_REITs_InvITs_chart[1][[1]]
Daily_market_pri_REITs_InvITs_src=IRB_InvIT_src


## -------------------------------------
#Beverages
Mon_Bev_index= data_query_clk_pg(723121)
Mon_Bev_index_title=Mon_Bev_index[2][[1]]
Mon_Bev_index_src=Mon_Bev_index[3][[1]]
Mon_Bev_index=Mon_Bev_index[1][[1]]
colnames(Mon_Bev_index) = c("Relevant_Date","Beverages")

#FOOD
Mon_Food_index= data_query_clk_pg(723122)[1][[1]]
colnames(Mon_Food_index) = c("Relevant_Date","Food")

Monthly_commodity_index_World_Bank=cbind(Mon_Bev_index,Mon_Food_index)


## -------------------------------------
data_s=list(Monthly_commodity_index_World_Bank)

n_row=1
n_col=6

h_just_line=0
v_just_line=0.75

num_brek=9
max_pri_y=180
min_pri_y=0

my_legends_col=c("Energy"="GRAY 48","Beverages"="GOLDEN ROD 1","Food" ="DARK ORANGE 2",
               "Base metals (ex. iron ore)"="BISQUE1","Precious metals"="#BF6E00")

my_line_type=c("Energy"="solid","Beverages"="solid","Food" ="solid",
               "Base metals (ex. iron ore)"="solid","Precious metals"="solid")


Mon_com_index_World_Bank_chart=multi_line_chart_niif(data_s,x_axis_interval="12 month",graph_lim=60,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE,legend_key_width=1,
                                                     led_position="center")

Mon_com_index_World_Bank_title=Mon_com_index_World_Bank_chart[2][[1]]
Mon_com_index_World_Bank_chart=Mon_com_index_World_Bank_chart[1][[1]]

Mon_com_index_World_Bank_src=Mon_Bev_index_src
Mon_com_index_World_Bank_chart



## -------------------------------------
#Eco-Indicators | RBI Professional Forecasters | Real GDP
bi_real_gdp= data_query_clk_pg(1627096)
bi_real_gdp_title=bi_real_gdp[2][[1]]
bi_real_gdp_src=bi_real_gdp[3][[1]]
bi_real_gdp_proj=bi_real_gdp[1][[1]]
colnames(bi_real_gdp_proj) = c("Relevant_Date","GDP")


#Eco-Indicators | RBI Professional Forecasters | Real GVA
bi_real_gva_proj= data_query_clk_pg(1627097)[1][[1]]
colnames(bi_real_gva_proj) = c("Relevant_Date","GVA")
bi_real_gva_gdp=cbind(bi_real_gdp_proj,bi_real_gva_proj)



## -------------------------------------
data_s=list(bi_real_gva_gdp)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=10
min_pri_y=0

my_legends_col=c("GDP"="GRAY 48","GVA"="GOLDEN ROD 1")

my_line_type=c("GDP"="solid","GVA"="solid")

bi_real_gva_gdp_chart=multi_line_chart_niif(data_s,x_axis_interval="2 month",
                                         graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)

bi_real_gva_gdp_title=bi_real_gva_gdp_chart[2][[1]]
bi_real_gva_gdp_chart=bi_real_gva_gdp_chart[1][[1]]
bi_real_gva_gdp_title=strsplit(str_replace(bi_real_gva_gdp_title," ","-"),"-")[[1]][3]
# bi_real_gva_gdp_title=strsplit(bi_real_gva_gdp_title,"-")[[1]][2]

bi_real_gva_gdp_src=bi_real_gdp_src


## -------------------------------------
get_predicted_trend=function(df,mx_dt_df){
  max_date <- max(mx_dt_df$Relevant_Date)
  min_date <- min(df$Relevant_Date)
  col=names(df)[2]
  names(df)[2]="value1"
  new_dates <- seq(as.Date(min_date), as.Date(max_date), by = "quarter")
  lm_model_gdp <- lm(value1 ~ as.numeric(Relevant_Date), data = df)
  extrapolated_values <- predict(lm_model_gdp,
                              newdata = data.frame(Relevant_Date = new_dates))
  print(extrapolated_values)
  df1 <- data.frame(Relevant_Date = new_dates, value1 = extrapolated_values)
  print(df1)
  df=df1
  names(df)[2]=col
  return(df)

}


## -------------------------------------
gdp_trend= data_query_clk_pg(1502377)
gdp_trend_src=gdp_trend[3][[1]]
gdp_trend=gdp_trend[1][[1]]
gdp_trend$Value=gdp_trend$Value/10^12
names(gdp_trend)[2]='GDP (INR trillion)'

gdp_trend<-subset(gdp_trend,((Relevant_Date>='2012-03-31')))

gdp_trend_pre=gdp_trend[gdp_trend$Relevant_Date<='2020-03-31',]
names(gdp_trend_pre)[2]='Pre pandemic trend'

gdp_trend_post=gdp_trend[gdp_trend$Relevant_Date>'2020-09-30',]
names(gdp_trend_post)[2]='Post pandemic trend'

gdp_trend_pre=get_predicted_trend(gdp_trend_pre,gdp_trend_post)
gdp_trend_post=get_predicted_trend(gdp_trend_post,gdp_trend_post)


## -------------------------------------
data_s=list(gdp_trend,gdp_trend_pre,gdp_trend_post)
my_legends_col=c('GDP (INR trillion)'='Grey 48',
                 "Pre pandemic trend"="GOLDEN ROD 1",
                 'Post pandemic trend'='#BF6E00')

my_line_type=c('GDP (INR trillion)'="solid",
               "Pre pandemic trend"="dashed",
               "Post pandemic trend"="dashed")


n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=50
min_pri_y=20


gdp_trend_chart=multi_line_chart_trend_niif(data_s,
                        x_axis_interval="24 month",
                        graph_lim=180,negative=TRUE,
                        PMI_reference=FALSE,
                        round_integer=TRUE,
                        BSE_Index=FALSE,
                        show_actual_lbls = TRUE,
                        legend_key_width=1,
                        trend ='GDP (INR trillion)',key_spacing=0.25)

gdp_trend_title=gdp_trend_chart[2][[1]]
gdp_trend_chart=gdp_trend_chart[1][[1]]
gdp_trend_src=gdp_trend_src
gdp_trend_chart


## -------------------------------------
exp_trend= data_query_clk_pg(1502395)
exp_trend_src=exp_trend[3][[1]]
exp_trend=exp_trend[1][[1]]
exp_trend$Value=exp_trend$Value/10^12
names(exp_trend)[2]='Net exports (INR trillion)'
exp_trend<-subset(exp_trend,((Relevant_Date>='2012-03-31')))


exp_trend_pre=exp_trend[exp_trend$Relevant_Date<='2020-03-31',]
names(exp_trend_pre)[2]='Pre pandemic trend'
exp_trend_post=exp_trend[exp_trend$Relevant_Date>'2020-03-31',]
names(exp_trend_post)[2]='Post pandemic trend'
exp_trend_pre=get_predicted_trend(exp_trend_pre,exp_trend_post)
exp_trend_post=get_predicted_trend(exp_trend_post,exp_trend_post)

## -------------------------------------
data_s=list(exp_trend,exp_trend_post,exp_trend_pre)
my_legends_col=c("Pre pandemic trend"="GOLDEN ROD 1",
                 'Post pandemic trend'='#BF6E00',
                 'Net exports (INR trillion)'='Grey 48')

my_line_type=c("Pre pandemic trend"="dashed",
               "Post pandemic trend"="dashed",
               'Net exports (INR trillion)'="solid")


n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=1
min_pri_y=4


exp_trend_chart=multi_line_chart_trend_niif(data_s,
                                  x_axis_interval="24 month",
                                  graph_lim=180,negative=TRUE,
                                  PMI_reference=FALSE,
                                  round_integer=TRUE,
                                  BSE_Index=FALSE,
                                  legend_key_width=1,

              trend ='Net exports (INR trillion)',key_spacing=0.25)

exp_trend_title=exp_trend_chart[2][[1]]
exp_trend_chart=exp_trend_chart[1][[1]]
exp_trend_src=exp_trend_src
exp_trend_chart


## -------------------------------------
gefc_trend= data_query_clk_pg(1502329)
gefc_trend_src=gefc_trend[3][[1]]
gefc_trend=gefc_trend[1][[1]]
gefc_trend$Value=gefc_trend$Value/10^12
names(gefc_trend)[2]='GFCF (INR trillion)'
gefc_trend<-subset(gefc_trend,((Relevant_Date>='2012-03-31')))

gefc_trend_pre=gefc_trend[gefc_trend$Relevant_Date<='2020-03-31',]
names(gefc_trend_pre)[2]='Pre pandemic trend'
gefc_trend_post=gefc_trend[gefc_trend$Relevant_Date>'2020-03-31',]
names(gefc_trend_post)[2]='Post pandemic trend'
gefc_trend_pre=get_predicted_trend(gefc_trend_pre,gefc_trend_post)
gefc_trend_post=get_predicted_trend(gefc_trend_post,gefc_trend_post)


## -------------------------------------
data_s=list(gefc_trend,gefc_trend_post,gefc_trend_pre)
my_legends_col=c("Pre pandemic trend"="GOLDEN ROD 1",
                 'Post pandemic trend'='#BF6E00',
                 'GFCF (INR trillion)'='Grey 48')

my_line_type=c("Pre pandemic trend"="dashed",
               "Post pandemic trend"="dashed",
                'GFCF (INR trillion)'="solid")


n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=16
min_pri_y=6


gefc_trend_chart=multi_line_chart_trend_niif(data_s,
                                  x_axis_interval="24 month",
                                  graph_lim=180,negative=TRUE,
                                  PMI_reference=FALSE,
                                  round_integer=TRUE,
                                  BSE_Index=FALSE,
                                  legend_key_width=1,

              trend ='GFCF (INR trillion)',key_spacing=0.25)

gefc_trend_title=gefc_trend_chart[2][[1]]
gefc_trend_chart=gefc_trend_chart[1][[1]]
gefc_trend_src=gefc_trend_src
gefc_trend_chart


## -------------------------------------
pfcf_trend= data_query_clk_pg(1502330)
pfcf_trend_src=pfcf_trend[3][[1]]
pfcf_trend=pfcf_trend[1][[1]]
pfcf_trend$Value=pfcf_trend$Value/10^12
names(pfcf_trend)[2]='PFCE (INR trillion)'

pfcf_trend<-subset(pfcf_trend,((Relevant_Date>='2012-03-31') ))

pfcf_trend_pre=pfcf_trend[pfcf_trend$Relevant_Date<='2020-03-31',]
names(pfcf_trend_pre)[2]='Pre pandemic trend'
pfcf_trend_post=pfcf_trend[pfcf_trend$Relevant_Date>'2020-03-31',]
names(pfcf_trend_post)[2]='Post pandemic trend'
pfcf_trend_pre=get_predicted_trend(pfcf_trend_pre,pfcf_trend_post)
pfcf_trend_post=get_predicted_trend(pfcf_trend_post,pfcf_trend_post)


## -------------------------------------
data_s=list(pfcf_trend,pfcf_trend_post,pfcf_trend_pre)
my_legends_col=c("Pre pandemic trend"="GOLDEN ROD 1",
                 'Post pandemic trend'='#BF6E00',
                 'PFCE (INR trillion)'='Grey 48')

my_line_type=c("Pre pandemic trend"="dashed",
               "Post pandemic trend"="dashed",
               'PFCE (INR trillion)'="solid")


n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=3
max_pri_y=30
min_pri_y=10


pfcf_trend_chart=multi_line_chart_trend_niif(data_s,
                                  x_axis_interval="24 month",
                                  graph_lim=180,negative=TRUE,
                                  PMI_reference=FALSE,
                                  round_integer=TRUE,
                                  BSE_Index=FALSE,
                                  legend_key_width=1,

              trend ='PFCE (INR trillion)',key_spacing=0.25)

pfcf_trend_title=pfcf_trend_chart[2][[1]]
pfcf_trend_chart=pfcf_trend_chart[1][[1]]
pfcf_trend_src=pfcf_trend_src
pfcf_trend_chart


## -------------------------------------
#Eco-Indicators | RBI Professional Forecasters | CPI
bi_cpi= data_query_clk_pg(1627148)
bi_cpi_title=bi_cpi[2][[1]]
bi_cpi_src=bi_cpi[3][[1]]
bi_cpi_proj=bi_cpi[1][[1]]
colnames(bi_cpi_proj) = c("Relevant_Date","CPI")

#Eco-Indicators | RBI Professional Forecasters | CPI-Core
bi_cpi_core_proj= data_query_clk_pg(1627149)[1][[1]]
colnames(bi_cpi_core_proj) = c("Relevant_Date","CPI-Core")

bi_cpi_core=cbind(bi_cpi_proj,bi_cpi_core_proj)



## -------------------------------------
data_s=list(bi_cpi_core)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=10
min_pri_y=-4

my_legends_col=c("CPI"="GRAY 48","CPI-Core"="GOLDEN ROD 1")
my_line_type=c("CPI"="solid","CPI-Core"="solid")

bi_cpi_cpicore_chart=multi_line_chart_niif(data_s,x_axis_interval="2 month",
                                         graph_lim=30,negative=TRUE,PMI_reference=FALSE,round_integer=TRUE,
                                         BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25
)

bi_cpi_cpicore_title=bi_cpi_cpicore_chart[2][[1]]
bi_cpi_cpicore_chart=bi_cpi_cpicore_chart[1][[1]]

bi_cpi_cpicore_src=bi_cpi_src
bi_cpi_cpicore_title=strsplit(str_replace(bi_cpi_cpicore_title," ","-"),"-")[[1]][3]
# bi_cpi_cpicore_title=strsplit(bi_cpi_cpicore_title,"-")[[1]][2]
bi_cpi_cpicore_chart




## -------------------------------------
#Eco-Indicators | RBI Professional Forecasters | WPI
bi_wpi= data_query_clk_pg(1627150)
bi_wpi_title=bi_wpi[2][[1]]
bi_wpi_src=bi_wpi[3][[1]]
bi_wpi_proj=bi_wpi[1][[1]]
colnames(bi_wpi_proj) = c("Relevant_Date","WPI")


#Eco-Indicators | RBI Professional Forecasters | WPI-Core
bi_wpi_core_proj= data_query_clk_pg(1627151)[1][[1]]
colnames(bi_wpi_core_proj) = c("Relevant_Date","WPI-Core")


bi_wpi_wpicore=cbind(bi_wpi_proj,bi_wpi_core_proj)



## -------------------------------------
data_s=list(bi_wpi_wpicore)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=5
min_pri_y=0

my_legends_col=c("WPI"="GRAY 48","WPI-Core"="GOLDEN ROD 1")
my_line_type=c("WPI"="solid","WPI-Core"="solid")

bi_wpi_wpicore_chart=multi_line_chart_niif(data_s,x_axis_interval="2 month",
                                         round_integer=TRUE,
                                         graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)

bi_wpi_wpicore_title=bi_wpi_wpicore_chart[2][[1]]
bi_wpi_wpicore_chart=bi_wpi_wpicore_chart[1][[1]]

bi_wpi_wpicore_src=bi_wpi_src
bi_wpi_wpicore_title=strsplit(str_replace(bi_wpi_wpicore_title," ","-"),"-")[[1]][3]
# bi_wpi_wpicore_title=strsplit(bi_wpi_wpicore_title,"-")[[1]][2]
bi_wpi_wpicore_chart



## -------------------------------------
#Export Value growth
Qtr_mer_exp_val= data_query_clk_pg(1682047)
Qtr_mer_exp_val_title=Qtr_mer_exp_val[2][[1]]
Qtr_mer_exp_val_src=Qtr_mer_exp_val[3][[1]]
Qtr_mer_exp_val=Qtr_mer_exp_val[1][[1]]
Qtr_mer_exp_val=mon_qtr_df_creator(Qtr_mer_exp_val,keep_col =c("Relevant_Date","growth"))
names(Qtr_mer_exp_val)[2]="Value growth (% yoy)"


#Export Volume growth
Qtr_mer_exp_vol= data_query_clk_pg(1681910,exception = TRUE)[1][[1]]
# Qtr_mer_exp_vol=mon_qtr_df_creator(Qtr_mer_exp_vol,keep_col =c("Relevant_Date","growth"))


Qtr_mer_exp_vol=Qtr_mer_exp_vol[Qtr_mer_exp_vol$Relevant_Date>=default_start_date,]
names(Qtr_mer_exp_vol)[2]="Volume growth (% yoy)"


d1=data.frame(date1=c(max(Qtr_mer_exp_val$Relevant_Date),max(Qtr_mer_exp_vol$Relevant_Date)))
common_min=min(as.Date(d1$date1))
Qtr_mer_exp_vol=Qtr_mer_exp_vol[Qtr_mer_exp_vol$Relevant_Date<=common_min,]
Qtr_mer_exp_val=Qtr_mer_exp_val[Qtr_mer_exp_val$Relevant_Date<=common_min,]
Qtr_mer_exp_val=Qtr_mer_exp_val[Qtr_mer_exp_val$Relevant_Date>=min(Qtr_mer_exp_vol$Relevant_Date),]

Qtr_exp_growth=cbind(Qtr_mer_exp_vol,Qtr_mer_exp_val)



## -------------------------------------
data_s=list(Qtr_exp_growth)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#
h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=20
min_pri_y=20

my_legends_col=c("Volume growth (% yoy)"="GRAY 48","Value growth (% yoy)"="GOLDEN ROD 1")
my_line_type=c("Volume growth (% yoy)"="solid","Value growth (% yoy)"="solid")

Qtr_exp_gr_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                       graph_lim=150,negative=TRUE,PMI_reference=FALSE,
                                       BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)


Qtr_exp_gr_title=Qtr_exp_gr_chart[2][[1]]
Qtr_exp_gr_chart=Qtr_exp_gr_chart[1][[1]]
# Qtr_exp_gr_src=Qtr_mer_exp_val_src

#HARD CODE :: Different Sources
Qtr_exp_gr_src="Source: Thurro, MOCI, UNCTAD, NIIF Research"
Qtr_exp_gr_chart


## -------------------------------------
#Import Value growth
Qtr_mer_imp_val= data_query_clk_pg(1682048)
Qtr_mer_imp_val_title=Qtr_mer_imp_val[2][[1]]
Qtr_mer_imp_val_src=Qtr_mer_imp_val[3][[1]]
Qtr_mer_imp_val=Qtr_mer_imp_val[1][[1]]

Qtr_mer_imp_val=mon_qtr_df_creator(Qtr_mer_imp_val,keep_col =c("Relevant_Date","growth"))
names(Qtr_mer_imp_val)[2]="Value growth (% yoy)"


#Import Volume growth
Qtr_mer_imp_vol= data_query_clk_pg(1681909,exception = TRUE)[1][[1]]
# Qtr_mer_imp_vol=mon_qtr_df_creator(Qtr_mer_imp_vol,keep_col =c("Relevant_Date","growth"))

Qtr_mer_imp_vol=Qtr_mer_imp_vol[Qtr_mer_imp_vol$Relevant_Date>=default_start_date,]
names(Qtr_mer_imp_vol)[2]="Volume growth (% yoy)"

d1=data.frame(date1=c(max(Qtr_mer_exp_val$Relevant_Date),max(Qtr_mer_exp_vol$Relevant_Date)))
common_min=min(as.Date(d1$date1))
Qtr_mer_imp_vol=Qtr_mer_imp_vol[Qtr_mer_imp_vol$Relevant_Date<=common_min,]
Qtr_mer_imp_val=Qtr_mer_imp_val[Qtr_mer_imp_val$Relevant_Date<=common_min,]
Qtr_mer_imp_val=Qtr_mer_imp_val[Qtr_mer_imp_val$Relevant_Date>=min(Qtr_mer_imp_vol$Relevant_Date),]
Qtr_imp_growth=cbind(Qtr_mer_imp_vol,Qtr_mer_imp_val)



## -------------------------------------
data_s=list(Qtr_imp_growth)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#
h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=20
min_pri_y=20

my_legends_col=c("Volume growth (% yoy)"="GRAY 48","Value growth (% yoy)"="GOLDEN ROD 1")
my_line_type=c("Volume growth (% yoy)"="solid","Value growth (% yoy)"="solid")

Qtr_imp_gr_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                            graph_lim=150,negative=TRUE,PMI_reference=FALSE,
                            BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)


Qtr_imp_gr_title=Qtr_imp_gr_chart[2][[1]]
Qtr_imp_gr_chart=Qtr_imp_gr_chart[1][[1]]
# Qtr_imp_gr_src=Qtr_mer_imp_val_src

#HARD CODE::Multiple Source
Qtr_imp_gr_src="Source: Thurro, MOCI, UNCTAD, NIIF Research"
Qtr_imp_gr_chart



## -------------------------------------
Qtr_bus_exp= data_query_clk_pg(1623559,exception=TRUE)
Qtr_bus_exp_title=Qtr_bus_exp[2][[1]]
Qtr_bus_exp_src=Qtr_bus_exp[3][[1]]
Qtr_bus_exp=Qtr_bus_exp[1][[1]]
colnames(Qtr_bus_exp) = c("Relevant_Date","Expectations")
Qtr_bus_exp


## -------------------------------------
data_s=Qtr_bus_exp

my_chart_col=c("GOLDEN ROD 1","GRAY 88")
my_legends_col=c("GOLDEN ROD 1","GRAY 88","TAN 1")
max_overlaps=30
h_just=0.60
v_just=0.60
h_just_line=0
v_just_line=0.60


n_row=1
n_col=3

num_brek=6
# max_pri_y=600
max_pri_y=80
min_pri_y=0
max_sec_y=1200

Qtr_bus_exp_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                  sales_heading="RBI Business Expectations",
                                  graph_lim=180,SPECIAL_LINE=FALSE,DATE_HEADER =FALSE)

Qtr_bus_exp_title=Qtr_bus_exp_chart[2][[1]]
Qtr_bus_exp_chart=Qtr_bus_exp_chart[1][[1]]
Qtr_bus_exp_src=Qtr_bus_exp_src
Qtr_bus_exp_chart


## -------------------------------------

#Net Responses Expecting Increase in Selling Price
Qtr_bus_cost= data_query_clk_pg(1623560,exception=TRUE)
Qtr_bus_cost_title=Qtr_bus_cost[2][[1]]
Qtr_bus_cost_src=Qtr_bus_cost[3][[1]]
Qtr_bus_cost_sell_pri=Qtr_bus_cost[1][[1]]
colnames(Qtr_bus_cost_sell_pri) = c("Relevant_Date","Selling price")
Qtr_bus_cost_sell_pri=Qtr_bus_cost_sell_pri[Qtr_bus_cost_sell_pri$Relevant_Date>=default_start_date,]

#Net Responses Expecting Increase in Profit Margin
Qtr_bus_cost_pro_mrgn= data_query_clk_pg(1623561,exception=TRUE)[1][[1]]
colnames(Qtr_bus_cost_pro_mrgn) = c("Relevant_Date","Profit margins")
Qtr_bus_cost_pro_mrgn=Qtr_bus_cost_pro_mrgn[Qtr_bus_cost_pro_mrgn$Relevant_Date>=default_start_date,]

#Net Responses Expecting Increase in Cost of Raw Materials
Qtr_bus_cost_raw= data_query_clk_pg(1623562,exception=TRUE)[1][[1]]
Qtr_bus_cost_raw$Value=-(Qtr_bus_cost_raw$Value)
colnames(Qtr_bus_cost_raw) = c("Relevant_Date","Cost of raw materials")
Qtr_bus_cost_raw=Qtr_bus_cost_raw[Qtr_bus_cost_raw$Relevant_Date>=default_start_date,]

#Net Responses Expecting Increase in Cost of Finance
Qtr_bus_cost_finance= data_query_clk_pg(1623563,exception=TRUE)[1][[1]]
Qtr_bus_cost_finance$Value=-(Qtr_bus_cost_finance$Value)
colnames(Qtr_bus_cost_finance) = c("Relevant_Date","Cost of financing")
Qtr_bus_cost_finance=Qtr_bus_cost_finance[Qtr_bus_cost_finance$Relevant_Date>=default_start_date,]
Qtr_bus_cost=cbind(Qtr_bus_cost_sell_pri,Qtr_bus_cost_pro_mrgn,Qtr_bus_cost_raw,
                   Qtr_bus_cost_finance)



## -------------------------------------
data_s=list(Qtr_bus_cost)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#

h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=80
min_pri_y=80

my_legends_col=c("Selling price"="GRAY 48","Profit margins"="GOLDEN ROD 1",
                 "Cost of raw materials" ="DARK ORANGE 2",
                 "Cost of financing"="#BF6E00")

my_line_type=c("Selling price"="solid","Profit margins"="solid",
               "Cost of raw materials" ="solid",
               "Cost of financing"="solid")

Qtr_bus_cost_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                         graph_lim=180,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,key_spacing=0.10)

Qtr_bus_cost_title=Qtr_bus_cost_chart[2][[1]]
Qtr_bus_cost_chart=Qtr_bus_cost_chart[1][[1]]

Qtr_bus_cost_src=Qtr_bus_exp_src
Qtr_bus_cost_chart



## -------------------------------------
#walr on outstanding rupee loans
Qtr_walr_out= data_query_clk_pg(1664022)
Qtr_walr_out_src=Qtr_walr_out[3][[1]]
Qtr_walr_out_loan=Qtr_walr_out[1][[1]]
colnames(Qtr_walr_out_loan) = c("Relevant_Date","On outstanding loans")

add_proxy_dates_1=function(df_t,df,col_n=""){

  df2=df_t[df_t$Relevant_Date<min(df$Relevant_Date),]
  if (nrow(df2)> 0){
    names(df2)[2]='Volume'
    names(df)[2]='Volume'

    df2['Volume']=NA
    data <- rbind(df,df2)
  }else{
    data=df
  }
  df3=merge(df_t,data,by="Relevant_Date")
  return(df3)

}

#walr on fresh rupee loans sanctioned
Qtr_walr_fresh_loan= data_query_clk_pg(1664023)[1][[1]]
# Qtr_walr_fresh_loan=add_proxy_dates_1(Qtr_walr_out_loan,Qtr_walr_fresh_loan)
colnames(Qtr_walr_fresh_loan) = c("Relevant_Date","On fresh loans")

#Scheduled Commercial Banks | On Outstanding Rupee Term Deposits
Qtr_pri_out_loan= data_query_clk_pg(2083952)[1][[1]]
# Qtr_pri_out_loan=add_proxy_dates_1(Qtr_walr_out_loan,Qtr_pri_out_loan)
colnames(Qtr_pri_out_loan) = c("Relevant_Date","On outstanding term deposits")


#Scheduled Commercial Banks | On Fresh Rupee Term Deposits
Qtr_pri_fresh_loan= data_query_clk_pg(2083956)[1][[1]]
# Qtr_pri_fresh_loan=add_proxy_dates_1(Qtr_walr_out_loan,Qtr_pri_fresh_loan)
colnames(Qtr_pri_fresh_loan) = c("Relevant_Date","On fresh term deposits")


# Qtr_walr=cbind(Qtr_walr_out_loan,Qtr_walr_fresh_loan,Qtr_pri_out_loan,
#                Qtr_pri_fresh_loan)

Qtr_walr <- merge(Qtr_walr_out_loan,Qtr_walr_fresh_loan,by = "Relevant_Date", all = TRUE)
Qtr_walr <- merge(Qtr_walr,Qtr_pri_out_loan,by = "Relevant_Date", all = TRUE)
Qtr_walr <- merge(Qtr_walr,Qtr_pri_fresh_loan,by = "Relevant_Date", all = TRUE)



## -------------------------------------
data_s=list(Qtr_walr)

n_row=4
n_col=2

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=14
min_pri_y=0

my_legends_col=c("On outstanding loans"="#595959",
                 "On fresh loans"="#F9A148",
                 "On outstanding term deposits"="#D9D9D9",
                 "On fresh term deposits"="#FED8B1")

my_line_type=c("On outstanding loans"="solid",
               "On fresh loans"="solid",
               "On outstanding term deposits"="solid",
               "On fresh term deposits"="solid")

Qtr_walr_chart=multi_line_chart_niif(data_s,
                                     x_axis_interval="24 month",
                                     graph_lim=90,
                                     negative=TRUE,PMI_reference=FALSE,
                                     BSE_Index=FALSE,legend_key_width=1,
                                     key_spacing=0.25)

Qtr_walr_title=Qtr_walr_chart[2][[1]]
Qtr_walr_chart=Qtr_walr_chart[1][[1]]
Qtr_walr_src=Qtr_walr_out_src
Qtr_walr_chart


## -------------------------------------

#Schedule Commercial Banks
scb_npa= data_query_clk_pg(1684512,exception=TRUE)
scb_npa_src=scb_npa[3][[1]]
scb_npa=scb_npa[1][[1]]
colnames(scb_npa) = c("Relevant_Date","SCBs")


#Public Sector Banks
pub_npa= data_query_clk_pg(1684513,exception=TRUE)[1][[1]]
colnames(pub_npa) = c("Relevant_Date","Public banks")


#Private Sector Banks
pri_npa= data_query_clk_pg(1684510,exception=TRUE)[1][[1]]
colnames(pri_npa) = c("Relevant_Date","Private banks")

#Net Responses Expecting Increase in Cost of Finance
frn_npa= data_query_clk_pg(1684511,exception=TRUE)[1][[1]]
colnames(frn_npa) = c("Relevant_Date","Foreign banks")


Qtr_npa=cbind(scb_npa,pub_npa,pri_npa,frn_npa)



## -------------------------------------
data_s=list(Qtr_npa)

n_row=1
n_col=6

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=16
min_pri_y=0

my_legends_col=c("SCBs"="GRAY 48","Public banks"="GOLDEN ROD 1",
                 "Private banks" ="DARK ORANGE 2",
                 "Foreign banks"="#BF6E00")

my_line_type=c("SCBs"="solid","Public banks"="solid",
               "Private banks" ="solid",
               "Foreign banks"="solid")

Qtr_gross_npa_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                         graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)

Qtr_gross_npa_title=Qtr_gross_npa_chart[2][[1]]
Qtr_gross_npa_chart=Qtr_gross_npa_chart[1][[1]]
Qtr_gross_npa_src=scb_npa_src



## -------------------------------------
#Total Holdings (AUM)
mf_aaum= data_query_clk_pg(1623672,exception=TRUE)
mf_aaum_src=mf_aaum[3][[1]]
mf_aaum=mf_aaum[1][[1]]

##Total Folios
mf_folio_unit=data_query_clk_pg(1623674,exception=TRUE)[1][[1]]

retail_trend <- merge(mf_aaum,mf_folio_unit,by = "Relevant_Date", all = TRUE)
retail_trend$Trend=retail_trend$Value/retail_trend$Total
retail_trend=retail_trend[,c('Relevant_Date','Trend')]
retail_trend$Trend=retail_trend$Trend/10^3
names(retail_trend)[2]='Retail investment'



## -------------------------------------
data_s=list(retail_trend)

n_row=4
n_col=2

legend_key_width=0.27
max_overlaps =10
#


h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=100
min_pri_y=50

my_legends_col=c('Retail investment'='GOLDEN ROD 1')
my_line_type=c('Retail investment'='solid')


retail_trend_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                 graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                 BSE_Index=FALSE,legend_key_width=1,key_spacing=0.25)


retail_trend_title=retail_trend_chart[2][[1]]
retail_trend_chart=retail_trend_chart[1][[1]]
retail_trend_src=mf_aaum_src
retail_trend_chart


## -------------------------------------
#Daily Market Prices
#Zomato
Zomato= data_query_clk_pg(id=724563)
Zomato_title=Zomato[2][[1]]
Zomato_src=Zomato[3][[1]]
Zomato=Zomato[1][[1]]
colnames(Zomato) = c("Relevant_Date","Zomato")

#PayTM
PayTM= data_query_clk_pg(id=724569)[1][[1]]
colnames(PayTM) = c("Relevant_Date","PayTM")

#Nykaa
Nykaa= data_query_clk_pg(id=724567)[1][[1]]
colnames(Nykaa) = c("Relevant_Date","Nykaa")
Nykaa$Nykaa=Nykaa$Nykaa/6

#policybazaar
policybazaar= data_query_clk_pg(id=724568)[1][[1]]
colnames(policybazaar) = c("Relevant_Date","Policybazaar")


#Map_my_India
Map_my_India= data_query_clk_pg(id=724571)[1][[1]]
colnames(Map_my_India) = c("Relevant_Date","Map my India")

#CarTrade
CarTrade= data_query_clk_pg(id=724565)[1][[1]]
colnames(CarTrade) = c("Relevant_Date","CarTrade")


#RateGain
RateGain= data_query_clk_pg(id=724570)[1][[1]]
colnames(RateGain) = c("Relevant_Date","RateGain")


new_age_companies=list(Zomato,PayTM,Nykaa,policybazaar,Map_my_India,CarTrade,RateGain)


## -------------------------------------
data_s=new_age_companies

num_brek=6
max_overlaps =30

h_just_line=0
v_just_line=0.60


n_row=2
n_col=4
num_brek=5
max_pri_y=2000
min_pri_y=0

my_legends_col=c("Zomato"="GRAY 48","PayTM"="GOLDEN ROD 1","Nykaa"="TAN 1",
             "Policybazaar"="#0541B4","Map my India"="GRAY 88","CarTrade"="BURLYWOOD 1",
             "RateGain"="DARK ORANGE 2")

my_line_type=c("Zomato"="solid","PayTM"="solid","Nykaa"="solid",
             "Policybazaar"="solid","Map my India"="solid","CarTrade"="solid",
             "RateGain"="solid")

market_prices_new_age_com_chart=multi_line_chart_niif(data_s,x_axis_interval="3 month",
                                                      graph_lim=20,negative=TRUE,PMI_reference=FALSE,
                                                      BSE_Index=TRUE,legend_key_width=0.40,key_spacing=0.40)

market_prices_new_age_com_title=market_prices_new_age_com_chart[2][[1]]
market_prices_new_age_com_chart=market_prices_new_age_com_chart[1][[1]]
market_prices_new_age_com_src= Zomato_src



## -------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)

qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,
DATE_FORMAT((Relevant_Date), '%Y-%m-01') AS
Start_Date,LAST_DAY(Relevant_Date) as Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA
where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA
where  Relevant_Date<=(select LAST_DAY(DATE_SUB(MAX(Relevant_Date),INTERVAL 1 MONTH)) as Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA)) group by State_Clean;"


# qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
# Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,
# DATE_FORMAT((Relevant_Date), '%Y-%m-01') AS
# Start_Date,LAST_DAY(Relevant_Date) as Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA
# where Relevant_Date in ('2023-09-30') group by State_Clean;"

res =dbGetQuery(mysql,qstring)
dbDisconnect(mysql)
res=res %>%  rename(State=State_Clean)
Monthly_Rainfall=res
text_size=30

ladakh=Monthly_Rainfall[Monthly_Rainfall$State=="Jammu and Kashmir",]
ladakh["State"]="Ladakh"
Monthly_Rainfall=rbind(Monthly_Rainfall,ladakh)
unique(Monthly_Rainfall$Rainfall)
Monthly_Rainfall$Rainfall=factor(Monthly_Rainfall$Rainfall,
                          levels=c("Large excess (60% or more)","Excess (20% to 59%)",
                                   "Normal (-19% to 19%)","Deficient (-20% to -59%)",
                                   "Large deficient (-99% to -60%)"))


## -------------------------------------
data_s=Monthly_Rainfall
my_legends_col=c('Large excess (60% or more)'="#595959",
               'Excess (20% to 59%)'="#979797",
               'Normal (-19% to 19%)'="#D9D9D9",
               'Deficient (-20% to -59%)'="TAN 1",
               'Large deficient (-99% to -60%)'="#BF6E00")


n_row=3
legend_key_width=0.20


Monthly_Rainfall_map_14_chart=map_rainfall_niif(data_s)


Monthly_Rainfall_map_14_title=Monthly_Rainfall_map_14_chart[2][[1]]
Monthly_Rainfall_map_14_chart=Monthly_Rainfall_map_14_chart[1][[1]]

#HARD CODE::Long source not getting enough space in PPT
Monthly_Rainfall_map_14_src="Source: Thurro, India Meteorological Department, NIIF Research"


## ----eval=FALSE, include=FALSE--------
## mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)
##
## qstring="select State_Clean,(Actual_Rainfall_mm) as Actual_Rainfall_mm,  case when Actual_Rainfall_mm = 0 then 'No'
## when Actual_Rainfall_mm <= 2.4 and Actual_Rainfall_mm >=0.1 then 'Very light'
## when Actual_Rainfall_mm <= 15.5  and Actual_Rainfall_mm >=2.5 then 'Light'
## when Actual_Rainfall_mm <= 64.4 and Actual_Rainfall_mm >=15.6 then 'Moderate'
## when Actual_Rainfall_mm <=115.5  and Actual_Rainfall_mm >=64.5 then 'Heavy'
## when Actual_Rainfall_mm <=204.4 and Actual_Rainfall_mm >=115.6 then 'Very heavy'
## else 'Extremly heavy' end as Rainfall, date_add(date_add(LAST_DAY(Relevant_Date),interval 1 DAY),interval -1 MONTH) AS
## Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA) group by State_Clean;"
##
## res =dbGetQuery(mysql,qstring)
## dbDisconnect(mysql)
## res=res %>%  rename(State=State_Clean)
## Monthly_rf_act=res
## text_size=30
##
## ladakh=Monthly_rf_act[Monthly_rf_act$State=="Jammu and Kashmir",]
## ladakh["State"]="Ladakh"
## Monthly_rf_act=rbind(Monthly_rf_act,ladakh)
## unique(Monthly_rf_act$Rainfall)
## Monthly_rf_act$Rainfall=factor(Monthly_rf_act$Rainfall,
##                 levels=c("Extremly heavy","Very heavy","Heavy","Moderate","Light","Very light",'No'))
##


## ----eval=FALSE, include=FALSE--------
## data_s=Monthly_rf_act
##
## my_chart_col=c("#ED7009","#FF9900","#F5D513","#FAF486","#D9D9D9","#979797","#595959")
## my_legends_col=c("#ED7009","#FF9900","#F5D513","#FAF486","#D9D9D9","#979797","#595959")
##
##
##
## mon_rf_act_map_chart=map_rainfall_niif(data_s)
##
## mon_rf_act_map_title=mon_rf_act_map_chart[2][[1]]
## mon_rf_act_map_chart=mon_rf_act_map_chart[1][[1]]
##
## #HARD CODE::Long source not getting enough space in PPT
## mon_rf_act_map_src="Source: Thurro, India Meteorological Department, NIIF Research"


## -------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)

qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,DATE_FORMAT(NOW(), CONCAT('%Y', '-06-01')) AS
Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA
where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA where Relevant_Date<=DATE_FORMAT(NOW(), CONCAT('%Y', '-09-30'))) group by State_Clean;"


# qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
# Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,DATE_FORMAT(NOW(), CONCAT('%Y', '-06-01')) AS
# Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA
# where Relevant_Date in ('2023-09-30') group by State_Clean;"

res =dbGetQuery(mysql,qstring)
dbDisconnect(mysql)
res=res %>% rename(State=State_Clean)


cumulative_rainfall=res
ladakh=cumulative_rainfall[cumulative_rainfall$State=="Jammu and Kashmir",]
ladakh["State"]="Ladakh"
cumulative_rainfall=rbind(cumulative_rainfall,ladakh)
cumulative_rainfall$Rainfall=factor(cumulative_rainfall$Rainfall,
                          levels=c("Large excess (60% or more)","Excess (20% to 59%)",
                                   "Normal (-19% to 19%)","Deficient (-20% to -59%)",
                                   "Large deficient (-99% to -60%)"))



## -------------------------------------
data_s=cumulative_rainfall
my_legends_col=c('Large excess (60% or more)'="#595959",
               'Excess (20% to 59%)'="#979797",
               'Normal (-19% to 19%)'="#D9D9D9",
               'Deficient (-20% to -59%)'="TAN 1",
               'Large deficient (-99% to -60%)'="#BF6E00")




Cumulative_Rainfall_map_14_chart=map_rainfall_niif(data_s)

Cumulative_Rainfall_map_14_title=Cumulative_Rainfall_map_14_chart[2][[1]]
Cumulative_Rainfall_map_14_chart=Cumulative_Rainfall_map_14_chart[1][[1]]

#HARD CODE::NIIF Used full form in PPT
Cumulative_Rainfall_map_14_src="Source: Thurro, India Meteorological Department, NIIF Research"


## ----eval=FALSE, include=FALSE--------
## mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)
##
## qstring="select State_Clean,(Actual_Rainfall_mm) as Actual_Rainfall_mm,  case when Actual_Rainfall_mm = 0 then 'No'
## when Actual_Rainfall_mm <= 2.4 and Actual_Rainfall_mm >=0.1 then 'Very light'
## when Actual_Rainfall_mm <= 15.5  and Actual_Rainfall_mm >=2.5 then 'Light'
## when Actual_Rainfall_mm <= 64.4 and Actual_Rainfall_mm >=15.6 then 'Moderate'
## when Actual_Rainfall_mm <=115.5  and Actual_Rainfall_mm >=64.5 then 'Heavy'
## when Actual_Rainfall_mm <=204.4 and Actual_Rainfall_mm >=115.6 then 'Very heavy'
## else 'Extremly heavy' end as Rainfall, date_add(date_add(LAST_DAY(Relevant_Date),interval 1 DAY),interval -1 MONTH) AS
## Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA) group by State_Clean;"
##
## ##Hard Coded
## qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
## Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,DATE_FORMAT(Relevant_Date, CONCAT('%Y', '-06-01')) AS
## Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA
## where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA where Relevant_Date<='2023-09-30') group by State_Clean;"
##
## res =dbGetQuery(mysql,qstring)
## dbDisconnect(mysql)
## res=res %>%  rename(State=State_Clean)
## Cumulative_rf_act=res
## text_size=30
##
## ladakh=Cumulative_rf_act[Cumulative_rf_act$State=="Jammu and Kashmir",]
## ladakh["State"]="Ladakh"
## Cumulative_rf_act=rbind(Cumulative_rf_act,ladakh)
## unique(Monthly_rf_act$Rainfall)
## Cumulative_rf_act$Rainfall=factor(Cumulative_rf_act$Rainfall,
##                 levels=c("Extremly heavy","Very heavy","Heavy","Moderate","Light","Very light",'No'))
##


## ----eval=FALSE, include=FALSE--------
## data_s=Cumulative_rf_act
##
## my_chart_col=c("#ED7009","#FF9900","#F5D513","#FAF486","#D9D9D9","#979797","#595959")
## my_legends_col=c("#ED7009","#FF9900","#F5D513","#FAF486","#D9D9D9","#979797","#595959")
##
## cum_rf_act_map_chart=map_rainfall_niif(data_s)
##
## cum_rf_act_map_title=cum_rf_act_map_chart[2][[1]]
## cum_rf_act_map_chart=cum_rf_act_map_chart[1][[1]]
##
## #HARD CODE::Long source not getting enough space in PPT
## cum_rf_act_map_src="Source: Thurro, India Meteorological Department, CEIC, NIIF Research"


## -------------------------------------
#Weekly_rainfall
Weekly_rainfall= data_query_clk_pg(id=725958)
Weekly_rainfall_title=Weekly_rainfall[2][[1]]
Weekly_rainfall_src=Weekly_rainfall[3][[1]]
Weekly_rainfall=Weekly_rainfall[1][[1]]
colnames(Weekly_rainfall) = c("Relevant_Date","Period","Long Period")
Weekly_rainfall$year=format(Weekly_rainfall$Relevant_Date,format="%Y")
# Weekly_rainfall$month=format(Weekly_rainfall$Relevant_Date,format="%B")

Weekly_rainfall["2018"]<-NA
Weekly_rainfall["2019"]<-NA
Weekly_rainfall["2020"]<-NA
Weekly_rainfall["2021"]<-NA
Weekly_rainfall["2022"]<-NA
Weekly_rainfall["2023"]<-NA
Weekly_rainfall["2024"]<-NA


for(i in 1:length(Weekly_rainfall$year)){
        if(Weekly_rainfall$year[i]==2018){
            Weekly_rainfall$`2018`[i]=Weekly_rainfall$Period[i]

        }
        if(Weekly_rainfall$year[i]==2019){
            Weekly_rainfall$`2019`[i]=Weekly_rainfall$Period[i]

        }
        if(Weekly_rainfall$year[i]==2020){
            Weekly_rainfall$`2020`[i]=Weekly_rainfall$Period[i]

        }
        if(Weekly_rainfall$year[i]==2021){
            Weekly_rainfall$`2021`[i]=Weekly_rainfall$Period[i]

        }
        if(Weekly_rainfall$year[i]==2022){
            Weekly_rainfall$`2022`[i]=Weekly_rainfall$Period[i]

        }
       if(Weekly_rainfall$year[i]==2023){
                Weekly_rainfall$`2023`[i]=Weekly_rainfall$Period[i]

            }
       if(Weekly_rainfall$year[i]==2024){
                Weekly_rainfall$`2024`[i]=Weekly_rainfall$Period[i]

            }
}


Weekly_rainfall=subset(Weekly_rainfall,select =-c(year))
Weekly_rainfall=melt(Weekly_rainfall,id=c("Relevant_Date"))
Weekly_rainfall=Weekly_rainfall %>%drop_na("value")
#HARD_CODE:: Since we are not showing 2018 data points
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$Relevant_Date>='2020-01-01',]

Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="2018",]
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="2019",]
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="2020",]
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="Period",]



## -------------------------------------
data_s=Weekly_rainfall


max_overlap=10
my_legends_col=c("Long Period"="#00B050","2018"="BURLYWOOD 1",
                 "2019"="RED 3","2020"="GRAY 32",
                 "2021"="GOLDEN ROD 1","2022"="blue")

my_line_type=c("Long Period"="dashed","2018"="solid",
               "2019"="solid","2020"="solid",
               "2021"="solid","2022"="longdash")

draw_key_line=draw_key_smooth

n_col=5
n_row=1

legend_key_width=0.75


max_pri_y=100
min_pri_y=0

h_just_line=0
v_just_line=0.75

Weekly_rainfall_13_chart=multi_line_chart_rainfall_niif(data_s,num_brek=6,key_spacing=0.15)

Weekly_rainfall_13_title=Weekly_rainfall_13_chart[2][[1]]
Weekly_rainfall_13_chart=Weekly_rainfall_13_chart[1][[1]]
##HARD CODE:Full for of IMD required
Weekly_rainfall_13_src="Source: Thurro, India Meteorological Department, CEIC, NIIF Research"
Weekly_rainfall_13_chart


## -------------------------------------
#cumulative_rainfall
cumulative_rainfall= data_query_clk_pg(id=725965)

cumulative_rainfall_title=cumulative_rainfall[2][[1]]
cumulative_rainfall_src=cumulative_rainfall[3][[1]]
cumulative_rainfall=cumulative_rainfall[1][[1]]
cumulative_rainfall
colnames(cumulative_rainfall) = c("Relevant_Date","Period","Long Period")
cumulative_rainfall$year=format(cumulative_rainfall$Relevant_Date,format="%Y")
# cumulative_rainfall$month=format(cumulative_rainfall$Relevant_Date,format="%B")

cumulative_rainfall["2018"]<-NA
cumulative_rainfall["2019"]<-NA
cumulative_rainfall["2020"]<-NA
cumulative_rainfall["2021"]<-NA
cumulative_rainfall["2022"]<-NA
cumulative_rainfall["2023"]<-NA
cumulative_rainfall["2024"]<-NA


for(i in 1:length(cumulative_rainfall$year)){
        if(cumulative_rainfall$year[i]==2018){
            cumulative_rainfall$`2018`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2019){
            cumulative_rainfall$`2019`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2020){
            cumulative_rainfall$`2020`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2021){
            cumulative_rainfall$`2021`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2022){
            cumulative_rainfall$`2022`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2023){
            cumulative_rainfall$`2023`[i]=cumulative_rainfall$Period[i]

        }
        if(cumulative_rainfall$year[i]==2024){
            cumulative_rainfall$`2024`[i]=cumulative_rainfall$Period[i]

        }
}


cumulative_rainfall=subset(cumulative_rainfall,select =-c(year))
cumulative_rainfall=melt(cumulative_rainfall,id=c("Relevant_Date"))
cumulative_rainfall=cumulative_rainfall %>%
                drop_na("value")

#HARD_CODE:: Since we are not showing 2018 data points
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$Relevant_Date>='2020-01-01',]

cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="2018",]
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="2019",]
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="2020",]
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="Period",]



## -------------------------------------
data_s=cumulative_rainfall

h_just_line=0
v_just_line=0.75

my_legends_col=c("Long Period"="#00B050","2018"="BURLYWOOD 1",
               "2019"="RED 3","2020"="GRAY 48"
               ,"2021"="GOLDEN ROD 1","2022"="blue")
my_line_type=c("Long Period"="dashed","2018"="solid",
               "2019"="solid","2020"="solid"
               ,"2021"="solid","2022"="longdash")


num_brek=6
max_pri_y=1000
min_pri_y=0


cumulative_rainfall_13_chart=multi_line_chart_rainfall_niif(data_s,num_brek=6)

cumulative_rainfall_13_title=cumulative_rainfall_13_chart[2][[1]]
cumulative_rainfall_13_chart=cumulative_rainfall_13_chart[1][[1]]

##HARD CODE:Full for of IMD required
cumulative_rainfall_13_src="Source: Thurro, India Meteorological Department, CEIC, NIIF Research"
cumulative_rainfall_13_chart
Position="left"
legend_key_width=0.27



## -------------------------------------
#Wholesale Price Index
Wholesale_Infla=data_query_clk_pg(id=725962)
Wholesale_Infla_title=tolower(Wholesale_Infla[2][[1]])
Wholesale_Infla_src=Wholesale_Infla[3][[1]]
Wholesale_Infla=Wholesale_Infla[1][[1]]


## -------------------------------------
data_s=Wholesale_Infla

h_just_line=0
v_just_line=0.70
legend_key_width=0.27
max_overlaps=30
my_chart_col=c("GRAY 48")


num_brek=6
max_pri_y=18
min_pri_y=6

Wholesale_Infla_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                      sales_heading="Monthly wholesale price inflation (% yoy)",
                      graph_lim=90,
                      SPECIAL_LINE=FALSE,key_spacing=0.10
)

Wholesale_Infla_title=Wholesale_Infla_chart[2][[1]]
Wholesale_Infla_chart=Wholesale_Infla_chart[1][[1]]
Wholesale_Infla_src=Wholesale_Infla_src



## -------------------------------------
#Coal, South African
Mon_coal_pri_SA= data_query_clk_pg(id=723139)
Mon_coal_pri_SA_title=Mon_coal_pri_SA[2][[1]]
Mon_coal_pri_SA_src=Mon_coal_pri_SA[3][[1]]
Mon_coal_pri_SA=na.omit(Mon_coal_pri_SA[1][[1]])

#Coal, Australian
Monthly_coal_prices_Australian=na.omit (data_query_clk_pg(id=723138)[1][[1]])



## -------------------------------------
data_s=Mon_coal_pri_SA


h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=400
min_pri_y=0

my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48")

Mon_coal_pri_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                   sales_heading="Coal, South African (USD/tonne)",
                                   graph_lim=90,SPECIAL_LINE=FALSE)

Mon_coal_pri_title=Mon_coal_pri_chart[2][[1]]
Mon_coal_pri_chart=Mon_coal_pri_chart[1][[1]]
Mon_coal_pri_src=Mon_coal_pri_SA_src



## -------------------------------------
#World Bank Commodity | Crude oil, Brent
Mon_brent_crd_oil_pri=data_query_clk_pg(id=723146)
Mon_brent_crd_oil_pri_title=Mon_brent_crd_oil_pri[2][[1]]
Mon_brent_crd_oil_pri_src=Mon_brent_crd_oil_pri[3][[1]]
Mon_brent_crd_oil_pri=Mon_brent_crd_oil_pri[1][[1]]



## -------------------------------------
data_s=Mon_brent_crd_oil_pri

my_chart_col=c("GRAY 48")


num_brek=5
max_pri_y=120
min_pri_y=0

Mon_brent_crd_oil_pri_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                            sales_heading="Monthly brent crude oil price, (USD/barrel)",
                                            graph_lim=90,SPECIAL_LINE=FALSE)
Mon_brent_crd_oil_pri_title=Mon_brent_crd_oil_pri_chart[2][[1]]
Mon_brent_crd_oil_pri_chart=Mon_brent_crd_oil_pri_chart[1][[1]]
Mon_brent_crd_oil_pri_src=Mon_brent_crd_oil_pri_src



## -------------------------------------
#World Bank Commodity | Gold
Mon_gold_pri=data_query_clk_pg(id=723152)
Mon_gold_pri_title=Mon_gold_pri[2][[1]]
Mon_gold_price_src=Mon_gold_pri[3][[1]]
Mon_gold_pri=Mon_gold_pri[1][[1]]


## -------------------------------------
data_s=Mon_gold_pri
divisor=1

my_chart_col=c("GRAY 48")

num_brek=5
max_pri_y=2500
min_pri_y=0


Mon_gold_pri_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                   sales_heading="Monthly gold prices (USD/troy oz)",
                                   graph_lim=90,SPECIAL_LINE=FALSE)

Mon_gold_pri_title=Mon_gold_pri_chart[2][[1]]
Mon_gold_pri_chart=Mon_gold_pri_chart[1][[1]]
Mon_gold_pri_src=Mon_gold_price_src



## -------------------------------------
#DAM_Clearing_Price
Mon_dmd_defi_power= data_query_clk_pg(id=2043691)
Mon_dmd_defi_power_title=Mon_dmd_defi_power[2][[1]]
Mon_dmd_defi_power_src=Mon_dmd_defi_power[3][[1]]
Mon_dmd_defi_power=Mon_dmd_defi_power[1][[1]]
names(Mon_dmd_defi_power)[2]='Ratio'
Mon_dmd_defi_power$Ratio=-Mon_dmd_defi_power$Ratio

older_niif=data_query_clk_pg(894692,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Ratio")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_dmd_defi_power$Relevant_Date),]
Mon_dmd_defi_power=rbind(older_niif,Mon_dmd_defi_power)

Mon_dmd_defi_power=na.omit(Mon_dmd_defi_power)



## -------------------------------------
data_s=Mon_dmd_defi_power

my_chart_col=c("GOLDEN ROD 1")

h_just_line=0
v_just_line=0.75

num_brek=5
max_pri_y=1
min_pri_y=15

Mon_dmd_defi_power_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                         sales_heading="Monthly demand deficit of power (%)",
                                         graph_lim=90,SPECIAL_LINE=TRUE)

Mon_dmd_defi_power_title=Mon_dmd_defi_power_chart[2][[1]]
Mon_dmd_defi_power_chart=Mon_dmd_defi_power_chart[1][[1]]
Mon_dmd_defi_power_src=Mon_dmd_defi_power_src



## -------------------------------------
#Volatility Index
Daily_NSE_ni_volati_Index=data_query_clk_pg(id=1384048)
Daily_NSE_ni_volati_Index_title=Daily_NSE_ni_volati_Index[2][[1]]
Daily_NSE_ni_volati_Index_src=Daily_NSE_ni_volati_Index[3][[1]]
Daily_NSE_ni_volati_Index=Daily_NSE_ni_volati_Index[1][[1]]



## -------------------------------------
data_s=Daily_NSE_ni_volati_Index

max_overlaps=20

my_chart_col=c("GRAY 48")



num_brek=4
max_pri_y=70
min_pri_y=0
legend_key_width=0.27

Daily_nft_volatility_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                     sales_heading="Monthly NSE NIFTY Volatility Index",
                     graph_lim=90,
                     SPECIAL_LINE=FALSE)

Daily_nft_volatility_title=Daily_nft_volatility_chart[2][[1]]
Daily_nft_volatility_chart=Daily_nft_volatility_chart[1][[1]]
Daily_nft_volatility_src=Daily_NSE_ni_volati_Index_src




## -------------------------------------
#Mon_inflow_into_SIPs-Monthly Inflow
Mon_inflow_into_SIPs=data_query_clk_pg(id=523075)
Mon_inflow_into_SIPs_title=Mon_inflow_into_SIPs[2][[1]]
Mon_inflow_into_SIPs_src=Mon_inflow_into_SIPs[3][[1]]
Mon_inflow_into_SIPs=Mon_inflow_into_SIPs[1][[1]]
Mon_inflow_into_SIPs$Value=Mon_inflow_into_SIPs$Value/10^9



## -------------------------------------
data_s=Mon_inflow_into_SIPs

h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=250
min_pri_y=0

my_chart_col=c("GRAY 48")


Mon_inflows_SIPs=line_chart_niif(data_s,x_axis_interval="12 month",
                   sales_heading="Monthly inflows into SIPs (INR billion)",
                   graph_lim=150,
                   SPECIAL_LINE=FALSE)

Mon_inflow_into_SIPs_38_title=Mon_inflows_SIPs[2][[1]]
Mon_inflows_SIPs=Mon_inflows_SIPs[1][[1]]
Mon_inflow_into_SIPs_38_src=Mon_inflow_into_SIPs_src
Mon_inflows_SIPs



## ----eval=FALSE, include=FALSE--------
## #SILVER
## Silver_Spot_Pri=data_query_clk_pg(id=282838)
## Silver_Spot_Pri_title=Silver_Spot_Pri[2][[1]]
## Silver_Spot_Pri_src=Silver_Spot_Pri[3][[1]]
## Silver_Spot_Pri=Silver_Spot_Pri[1][[1]]
## Silver_Spot_Pri$Price=Silver_Spot_Pri$Price/1000


## ----eval=FALSE, include=FALSE--------
## data_s=Silver_Spot_Pri
##
## my_chart_col=c("GOLDEN ROD 1")
##
##
## h_just_line=0
## v_just_line=0.60
## num_brek=4
## max_pri_y=80
## min_pri_y=0
##
##
## Mon_silicon_silver_pri_chart=line_chart_niif(data_s,x_axis_interval="12 month",
##                                              sales_heading="Silver (INR/gm)",graph_lim=10,
##                                              SPECIAL_LINE=FALSE)
##
## Mon_silicon_silver_pri_title=Mon_silicon_silver_pri_chart[2][[1]]
## Mon_silicon_silver_pri_chart=Mon_silicon_silver_pri_chart[1][[1]]
## Mon_silicon_silver_pri_src=Silver_Spot_Pri_src
##


## ----eval=FALSE, include=FALSE--------
## #Credit Deposit Difference
## CD_diff= data_query_clk_pg(id=1443678)
## CD_diff_title=tolower(CD_diff[2][[1]])
## CD_diff_src=CD_diff[3][[1]]
## CD_diff=CD_diff[1][[1]]
##
##
## older_niif=data_query_clk_pg(1443681,150)[1][[1]]
## colnames(older_niif)=c("Relevant_Date","Value")
## older_niif=older_niif[older_niif$Relevant_Date<min(CD_diff$Relevant_Date),]
## CD_diff=rbind(older_niif,CD_diff)
## CD_diff$Value=CD_diff$Value/10^12


## ----eval=FALSE, include=FALSE--------
## data_s=CD_diff
##
## h_just_line=0
## v_just_line=0.75
## my_chart_col=c("GOLDEN ROD 1")
##
##
## num_brek=6
## max_pri_y=60
## min_pri_y=0
##
##
## CD_diff_chart=line_chart_niif(data_s,x_axis_interval="24 month",
##                               sales_heading="Monthly credit-deposit difference (INR trillion)",
##                               graph_lim=60,SPECIAL_LINE=FALSE)
##
## CD_diff_title=CD_diff_chart[2][[1]]
## CD_diff_chart=CD_diff_chart[1][[1]]
## draw_key_line=draw_key_rect
## CD_diff_src=CD_diff_src


## -------------------------------------
#Statutory Liquidity Ratio
SL_ratio= data_query_clk_pg(1502399)
SL_ratio_title=tolower(SL_ratio[2][[1]])
SL_ratio_src=SL_ratio[3][[1]]
SL_ratio=SL_ratio[1][[1]]



## -------------------------------------
data_s=SL_ratio

h_just_line=0
v_just_line=0.75
my_chart_col=c("GOLDEN ROD 1")

max_overlaps=30

num_brek=4
max_pri_y=40
min_pri_y=0

SL_ratio_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                               sales_heading="Monthly statutory liquidity ratio (%)",
                               graph_lim=60,SPECIAL_LINE=FALSE,
                               legend_key_width=0.27,Reference=TRUE)

SL_ratio_title=SL_ratio_chart[2][[1]]
SL_ratio_chart=SL_ratio_chart[1][[1]]
SL_ratio_src=SL_ratio_src



## -------------------------------------
#Monthly Interest Rates
RBI_Repo_Reverse_Repo= data_query_clk_pg(id=725999)
RBI_Repo_Reverse_Repo_title=RBI_Repo_Reverse_Repo[2][[1]]
RBI_Repo_Reverse_Repo_src=RBI_Repo_Reverse_Repo[3][[1]]
RBI_Repo_Reverse_Repo=RBI_Repo_Reverse_Repo[1][[1]]

RBI_Repo_Rates=RBI_Repo_Reverse_Repo[,c("Relevant_Date","Total_Repo")]
RBI_Reverse_Repo_Rates=RBI_Repo_Reverse_Repo[,c("Relevant_Date","Total_Rev_Repo")]



## -------------------------------------
data_s=RBI_Repo_Rates
data_g=RBI_Reverse_Repo_Rates

num_brek=5
max_pri_y=10
min_pri_y=0


h_just_line=0
v_just_line=0.75
my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48")


RBI_Repo_Reverse_Repo_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                            sales_heading="Repo",show_fu_dt=TRUE,
                                            graph_lim=160,SPECIAL_LINE=FALSE,
                                            DATE_HEADER=TRUE,Repo=TRUE)

RBI_Repo_Reverse_Repo_title=RBI_Repo_Reverse_Repo_chart[2][[1]]
RBI_Repo_Reverse_Repo_chart=RBI_Repo_Reverse_Repo_chart[1][[1]]
RBI_Repo_Reverse_Repo_src=RBI_Repo_Reverse_Repo_src



## ----include=FALSE--------------------
#Average Monthly P/E Ratio
Mon_avg_PE_ratio=data_query_clk_pg(id=318881)
Mon_avg_PE_ratio_title=Mon_avg_PE_ratio[2][[1]]
Mon_avg_PE_ratio_src=Mon_avg_PE_ratio[3][[1]]
Mon_avg_PE_ratio=Mon_avg_PE_ratio[1][[1]]


## -------------------------------------
data_s=Mon_avg_PE_ratio
my_chart_col=c("GOLDEN ROD 1")


num_brek=5
max_pri_y=50
min_pri_y=0

Mon_avg_PE_ratio_Nifty_50_com_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                      sales_heading="Monthly average P/E ratio for Nifty-50 companies",
                                      graph_lim=90,SPECIAL_LINE=FALSE)

Mon_avg_PE_ratio_Nifty_50_com_title=Mon_avg_PE_ratio_Nifty_50_com_chart[2][[1]]

Mon_avg_PE_ratio_Nifty_50_com_chart=Mon_avg_PE_ratio_Nifty_50_com_chart[1][[1]]
Mon_avg_PE_ratio_Nifty_50_com_src=Mon_avg_PE_ratio_src


## ----include=FALSE--------------------
#Real Effective Exchange Rate
Mon_Re_exch_rt=data_query_clk_pg(id=318883)
Mon_Re_exch_rt_src=Mon_Re_exch_rt[3][[1]]
Mon_Re_exch_rt=Mon_Re_exch_rt[1][[1]]


## -------------------------------------
data_s=Mon_Re_exch_rt
my_chart_col=c("GRAY 48")


num_brek=5
max_pri_y=120
min_pri_y=76

Mon_re_exch_rt_chart=line_chart_niif(data_s,x_axis_interval="12 month",
                                      sales_heading="",
                                      graph_lim=120,
                                      graph_lim1=0,
                                      SPECIAL_LINE=FALSE,
                                      Reference = TRUE,
                                      reverse_y=TRUE,
                                      exculde_FY=TRUE,
                                      round_integer=TRUE,
                                      show_legend=FALSE)

Mon_re_exch_rt_title=Mon_re_exch_rt_chart[2][[1]]

Mon_re_exch_rt_chart=Mon_re_exch_rt_chart[1][[1]]
Mon_re_exch_rt_src=Mon_Re_exch_rt_src
Mon_re_exch_rt_chart


## ----include=FALSE--------------------
#Real Effective Exchange Rate
Mon_doller_indx=data_query_clk_pg(id=318871)
Mon_doller_indx_src=Mon_doller_indx[3][[1]]
Mon_doller_indx=Mon_doller_indx[1][[1]]


## -------------------------------------
data_s=Mon_doller_indx
my_chart_col=c("GOLDEN ROD 1")


num_brek=6
max_pri_y=120
min_pri_y=60

Mon_doller_indx_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                                      sales_heading="",
                                      graph_lim=90,
                                      SPECIAL_LINE=FALSE,
                                      show_legend=FALSE)

Mon_doller_indx_title=Mon_doller_indx_chart[2][[1]]

Mon_doller_indx_chart=Mon_doller_indx_chart[1][[1]]
Mon_doller_indx_src=Mon_doller_indx_src
Mon_doller_indx_chart


## -------------------------------------
#Net Injection Absorption
Surplus_liquidity=data_query_clk_pg(id=724178,exception=FALSE,surplus=TRUE)
Surplus_liquidity_title=Surplus_liquidity[2][[1]]
Surplus_liquidity_src=Surplus_liquidity[3][[1]]
Surplus_liquidity=Surplus_liquidity[1][[1]]
Surplus_liquidity$Value=Surplus_liquidity$Value/10^12

Surplus_liquidity=Surplus_liquidity[,c("Relevant_Date","Value")]
Surplus_liquidity$avg=movavg(Surplus_liquidity$Value,7,t='s')
colnames(Surplus_liquidity)=c("Relevant_Date","value","Liquidity deficit/(surplus) (7 DMA)")
Surplus_liquidity=Surplus_liquidity[,c("Relevant_Date","Liquidity deficit/(surplus) (7 DMA)")]
#


## -------------------------------------
data_s=list(Surplus_liquidity)
h_just=0
v_just=0.60
h_just_line=1.15
v_just_line=1.15

num_brek=10
max_pri_y=2
min_pri_y=6

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

Surplus_liquidity_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                               graph_lim=160,negative=TRUE,SIDE_BAR=FALSE,
                                               DATE_HEADER = TRUE,surplus=FALSE,round_integer=TRUE)

Surplus_liquidity_title=Surplus_liquidity_chart[2][[1]]
Surplus_liquidity_chart=Surplus_liquidity_chart[1][[1]]
Surplus_liquidity_src=Surplus_liquidity_src



## -------------------------------------
#MF | Equity
qtr_mfh_t=data_query_clk_pg(id=1633862)
qtr_mfh_t_title=qtr_mfh_t[2][[1]]
qtr_mfh_t_src=qtr_mfh_t[3][[1]]
qtr_mfh_t=qtr_mfh_t[1][[1]]
qtr_mfh_t$Value=qtr_mfh_t$Value/10^12
names(qtr_mfh_t)[2]="Equity"
qtr_mfh_t=mon_qtr_df_creator(qtr_mfh_t,keep_col =c("Relevant_Date","Equity"))


## -------------------------------------
data_s=list(qtr_mfh_t)
h_just=0
v_just=0.60
h_just_line=1.15
v_just_line=1.15

num_brek=7
max_pri_y=35
min_pri_y=0

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

qtr_mfh_t_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",round_integer = TRUE,
                                       data_unit="",graph_lim=90,negative=FALSE,bar_thick=40)

qtr_mfh_t_title=qtr_mfh_t_chart[2][[1]]
qtr_mfh_t_chart=qtr_mfh_t_chart[1][[1]]
qtr_mfh_t_src=qtr_mfh_t_src
qtr_mfh_t_chart


## -------------------------------------
#Capital Flows | FPI | Equity
Mon_net_equi=data_query_clk_pg(id=1526330)
Mon_net_equi_src=Mon_net_equi[3][[1]]
Mon_net_equi=Mon_net_equi[1][[1]]
Mon_net_equi=mon_year_df_creator(Mon_net_equi,keep_col = c('Relevant_Date','Value'),
                                 Sum_date = TRUE)
names(Mon_net_equi)[2]="equity"

#Capital Flows | FPI | Debt
Mon_net_debt1=data_query_clk_pg(id=1526333)[1][[1]]
#Capital Flows | FPI | Debt-VRR
Mon_net_debt2=data_query_clk_pg(id=1526331)[1][[1]]
Mon_net_debt2$Value=(Mon_net_debt2$Value+Mon_net_debt1$Value)
Mon_net_debt=Mon_net_debt2
Mon_net_debt=mon_year_df_creator(Mon_net_debt,keep_col = c('Relevant_Date','Value'),
                                 Sum_date = TRUE)
names(Mon_net_debt)[2]="debt"

#Capital Flows | FPI | Hybrid
Mon_net_others=data_query_clk_pg(id=1526332)[1][[1]]
Mon_net_others=mon_year_df_creator(Mon_net_others,keep_col = c('Relevant_Date','Value'),
                                 Sum_date = TRUE)
names(Mon_net_others)[2]="others"

Mon_net_FPI=cbind(Mon_net_equi,Mon_net_debt,Mon_net_others,by="Relevant_Date")
Mon_net_FPI=Mon_net_FPI[,c("Relevant_Date","equity","debt","others")]
colnames(Mon_net_FPI)=c("Relevant_Date","Net equity","Net debt","Net others")
Mon_net_FPI=Mon_net_FPI%>%mutate(across(c(2:4), .fns = ~./10^9))

# #Monthly_gr
#Capital Flows | FPI
Mon_net_FPI_gr=data_query_clk_pg(id=1498513)[1][[1]]
Mon_net_FPI_gr=mon_year_df_creator(Mon_net_FPI_gr,keep_col = c('Relevant_Date','Value'),
                                 Sum_date = TRUE)
colnames(Mon_net_FPI_gr)=c("Relevant_Date","growth")
Mon_net_FPI_gr$growth=Mon_net_FPI_gr$growth/10^9



## -------------------------------------
data_s=Mon_net_FPI
data_g=Mon_net_FPI_gr

my_chart_col=c("GRAY 88","DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","GOLDEN ROD 1","GRAY 32")

h_just=-2
v_just=0
h_just_line=1
v_just_line=1.5

num_brek=5
max_pri_y=3000
min_pri_y=2000
max_sec_y=3000



line_thick=0.50
n_row=1
n_col=5
chart_label=7

Mon_FPI_invest_India_chart=stacked_bar_line_chart_niif(data_s,data_g,
                           growth_heading="Net FPI",
                           x_axis_interval="24 month",DUAL_AXIS=FALSE,
                           data_unit='',graph_lim=580,round_integer=TRUE,
                           Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                           GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,
                           ptrn_den=0.1,ptrn_spc=0.01,
                           order_stack = TRUE,
                           top_column = 'Net others',
                           format_date =paste0("%Y"),
                           YTD = TRUE,show_shaded = TRUE,bar_thick=200)


Annual_FPI_invest_India_title=Mon_FPI_invest_India_chart[2][[1]]
Annual_FPI_invest_India_chart=Mon_FPI_invest_India_chart[1][[1]]
Annual_FPI_invest_India_src=Mon_net_equi_src
Annual_FPI_invest_India_chart



## -------------------------------------
#Capital Flows | FPI | Equity
Mon_net_equi=data_query_clk_pg(id=1526330)
Mon_net_equi_src=Mon_net_equi[3][[1]]
Mon_net_equi=Mon_net_equi[1][[1]]
names(Mon_net_equi)[2]="equity"

#Capital Flows | FPI | Debt
Mon_net_debt1=data_query_clk_pg(id=1526333)[1][[1]]
#Capital Flows | FPI | Debt-VRR
Mon_net_debt2=data_query_clk_pg(id=1526331)[1][[1]]
Mon_net_debt2$Value=(Mon_net_debt2$Value+Mon_net_debt1$Value)
Mon_net_debt=Mon_net_debt2
names(Mon_net_debt)[2]="debt"

#Capital Flows | FPI | Hybrid
Mon_net_others=data_query_clk_pg(id=1526332)[1][[1]]
names(Mon_net_others)[2]="others"

Mon_net_FPI=cbind(Mon_net_equi,Mon_net_debt,Mon_net_others,by="Relevant_Date")
Mon_net_FPI=Mon_net_FPI[,c("Relevant_Date","equity","debt","others")]
colnames(Mon_net_FPI)=c("Relevant_Date","Net equity","Net debt","Net others")
Mon_net_FPI=Mon_net_FPI%>%mutate(across(c(2:4), .fns = ~./10^9))

# #Monthly_gr
#Capital Flows | FPI
Mon_net_FPI_gr=data_query_clk_pg(id=1498513)[1][[1]]
colnames(Mon_net_FPI_gr)=c("Relevant_Date","growth")
Mon_net_FPI_gr$growth=Mon_net_FPI_gr$growth/10^9



## -------------------------------------
data_s=Mon_net_FPI
data_g=Mon_net_FPI_gr

my_chart_col=c("GRAY 88","DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","DARK ORANGE 2","GOLDEN ROD 1","GRAY 32")

h_just=0
v_just=0
h_just_line=1.75
v_just_line=0



num_brek=6
max_pri_y=800
min_pri_y=600
max_sec_y=800


line_thick=0.50
n_row=1
n_col=5
chart_label=7

Mon_FPI_invest_India_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                       growth_heading="Net FPI",
                                       x_axis_interval="24 month",DUAL_AXIS=FALSE,
                                       data_unit='',graph_lim=100,round_integer=TRUE,
                                       Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                                       GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,
                                       top_column = 'Net others',
                                       order_stack = TRUE,
                                       YTD = FALSE)


Mon_FPI_invest_India_title=Mon_FPI_invest_India_chart[2][[1]]
Mon_FPI_invest_India_chart=Mon_FPI_invest_India_chart[1][[1]]
Mon_FPI_invest_India_src=Mon_net_equi_src
Mon_FPI_invest_India_chart


## -------------------------------------
# Current Account Balance
CA_bln= data_query_clk_pg(id=1783070,exception=TRUE)
CA_bln_title=tolower(CA_bln[2][[1]])
CA_Deficit_src=CA_bln[3][[1]]
CA_Deficit=CA_bln[1][[1]]
names(CA_Deficit)[2]="Current account balance (%)"
CA_Deficit=CA_Deficit[,c("Relevant_Date","Current account balance (%)")]
colnames(CA_Deficit)[2]='growth'

#Eco-Indicators | Merchandise
trade_bln_mer= data_query_clk_pg(id=1690708,exception=TRUE)[1][[1]]
colnames(trade_bln_mer)[2]='Goods trade balance (% GDP)'

#Eco-Indicators | Services
trade_bln_ser= data_query_clk_pg(id=1690709,exception=TRUE)[1][[1]]
colnames(trade_bln_ser)[2]='Services trade balance (% GDP)'

trade_balance=merge(trade_bln_mer,trade_bln_ser,by="Relevant_Date")



## -------------------------------------
data_s=trade_balance
data_g=CA_Deficit

my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.10

num_brek=10
max_pri_y=10
min_pri_y=20
max_sec_y=10

n_col=3
n_row=1


chart_label=8
legend_key_width=0.27



CA_Deficit_chart=stacked_bar_line_chart_niif(data_s,data_g,
                           growth_heading="Current account (% GDP)",
                           x_axis_interval="12 month",
                           data_unit='',graph_lim=150,round_integer=TRUE,
                           Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                           order_stack = TRUE,bar_thick =50,
                           GST_SPECIAL=FALSE,DUAL_AXIS = FALSE,key_spacing=0.50)

CA_Deficit_title=CA_Deficit_chart[2][[1]]
CA_Deficit_chart=CA_Deficit_chart[1][[1]]
CA_Deficit_src=CA_Deficit_src
CA_Deficit_chart


## -------------------------------------
#YEN
INR_with_yen=data_query_clk_pg(id=1288702)
INR_with_yen_title=tolower(INR_with_yen[2][[1]])
INR_with_yen_src=INR_with_yen[3][[1]]


## -------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum,
                    user = username, password = password)

#INR_USD
quary="select Relevant_Date,Exchange_Rate from RBI_CURRENCY_EXCHANGE_RATE_DAILY_DATA where Currency='USD'"

INR_with_usd=dbGetQuery(mysql,quary)
colnames(INR_with_usd)=c("Relevant_Date","USD")
INR_with_usd=INR_with_usd[,c("Relevant_Date","USD")]

#EURO
quary="select Relevant_Date,Exchange_Rate from RBI_CURRENCY_EXCHANGE_RATE_DAILY_DATA where Currency='EURO'"

INR_with_euro=dbGetQuery(mysql,quary)
colnames(INR_with_euro)=c("Relevant_Date","EUR")
INR_with_euro=INR_with_euro[,c("Relevant_Date","EUR")]

#GBP
quary="select Relevant_Date,Exchange_Rate from RBI_CURRENCY_EXCHANGE_RATE_DAILY_DATA where Currency='GBP'"

INR_with_gbp=dbGetQuery(mysql,quary)
colnames(INR_with_gbp)=c("Relevant_Date","GBP")
INR_with_gbp=INR_with_gbp[,c("Relevant_Date","GBP")]

#YEN
quary="select Relevant_Date,Exchange_Rate from RBI_CURRENCY_EXCHANGE_RATE_DAILY_DATA where Currency='YEN'"
INR_with_yen=dbGetQuery(mysql,quary)
colnames(INR_with_yen)=c("Relevant_Date","YEN")
INR_with_yen=INR_with_yen[,c("Relevant_Date","YEN")]


INR_with_all=list(INR_with_usd,INR_with_euro,INR_with_gbp,INR_with_yen)
INR_with_all=Reduce(function(x, y) merge(x, y, all=TRUE), INR_with_all)

# INR_with_all$Relevant_Date=timeLastDayInMonth(INR_with_all$Relevant_Date)
INR_with_all$one_month <- as.Date(as.Date(INR_with_all$Relevant_Date, format = "%Y-%m-%d")-dmonths(1),
                                  format = "%Y-%m-%d")
INR_with_all$Relevant_Date=as.Date(INR_with_all$Relevant_Date)
dbDisconnect(mysql)


## -------------------------------------
d1=data.frame()
value=c()
duration=c()
units=c()
Relevant_Date=c()
# my_seq=c(1,3,6,12,36,60,120)
# my_seq=c(30,91,182,365,1093,1823,3648)
my_seq=c(1,3,6,12,36,60,120)


c1=c("EUR","GBP","USD","YEN")

 for (j in c1){
  print(j)
  l1=INR_with_all$Relevant_Date
  # prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))
  # print(prev_month)
  m1=as.Date(max(INR_with_all$Relevant_Date))
  # m1=prev_month
  # m1=as.Date("2022-11-30",format ="%Y-%m-%d")
  current=INR_with_all[INR_with_all$Relevant_Date==m1,j]
  print(current)
  for (i in my_seq){
    # print(i)
    lubridate::duration()
    # one_mn=timeLastDayInMonth(as.Date(m1-duration(i,"months")))
    # one_mn=as.Date(m1-duration(i,"months"))
    one_mn=as.Date(m1 %m-% months(i))
    # one_mn=as.Date(m1-duration(i,"days"))
    one_mon=as.Date(one_mn,format ="%Y-%m-%d")
    print(one_mon)
    prev=INR_with_all[INR_with_all$Relevant_Date==one_mon,j]
    print(prev)
    if (length(prev)!=0){

    if (i>12){
       # r1=roundexcel(i/365)
       r1=i/12
       factor=(1/r1)
       val=(((1/current)/(1/prev))^factor-1)*100

    }else{
        # val=((prev/current)-1)*100
         val=(((1/current)/(1/prev))-1)*100
         # print(val)

      }

    print(as.numeric(val))
    value=append(value,val)
    Relevant_Date=append(Relevant_Date,m1)
    if (i>6){
      interval=paste0(i/12,"-year")
    }else{interval=paste0(i,"-month")}

    # if (i>6){
    #   interval=paste0(roundexcel(i/12),"-Year")
    #  }else{interval=paste0(roundexcel(i),"-Month")}


    duration=append(duration,interval)
    units=append(units,j)
  }
  }}
# d1=data.frame(Relevant_Date,duration,units,value)
d1=data.frame(Relevant_Date,duration,units,value)
INR_with_all_1=d1


## -------------------------------------
data_s=INR_with_all_1


h_just=0.5
v_just=1.5

num_brek=7
max_pri_y=15
min_pri_y=15

chart_label=8
side_width=0


my_chart_col=c("GRAY 48","DARK ORANGE 2","GOLDEN ROD 1","BURLYWOOD 1")
my_legends_col=c("GRAY 48","DARK ORANGE 2","GOLDEN ROD 1","BURLYWOOD 1")
custom_lev=c("1-month","3-month","6-month","1-year","3-year","5-year","10-year")
Rupee_Appre_Depre_chart=side_bar_chart_niif_rupee(data_s,graph_lim=30,
                                                  negative=TRUE,

                                                  DATE_HEADER = TRUE,
                                                  pos_d=0.5,
                                                  pos_lb=0.8,bar_thick=0.5,key_spacing=2)

Rupee_Appre_Depre_title=Rupee_Appre_Depre_chart[2][[1]]
Rupee_Appre_Depre_chart=Rupee_Appre_Depre_chart[1][[1]]

Rupee_Appre_Depre_src=INR_with_yen_src
draw_key_line=draw_key_smooth
Rupee_Appre_Depre_chart



## ----eval=FALSE, include=FALSE--------
## #Power | Wind
## wind_pwr=data_query_clk_pg(id=2262020)
## wind_pwr_src=wind_pwr[3][[1]]
## wind_pwr=wind_pwr[1][[1]]
## wind_pwr=mon_qtr_df_creator(wind_pwr,keep_col =c("Relevant_Date","Total"))
## wind_pwr$Total=wind_pwr$Total/1000
## names(wind_pwr)[2]='Wind power'
## #%%
## solar_pwr=data_query_clk_pg(id=2262019)[1][[1]]
## solar_pwr=mon_qtr_df_creator(solar_pwr,keep_col =c("Relevant_Date","Total"))
## solar_pwr$Total=solar_pwr$Total/1000
## names(solar_pwr)[2]='Solar power'
## #%%
## small_hydro=data_query_clk_pg(id=2262021)[1][[1]]
## small_hydro=mon_qtr_df_creator(small_hydro,keep_col =c("Relevant_Date","Total"))
## small_hydro$Total=small_hydro$Total/1000
## names(small_hydro)[2]='Small hydro'
## #%%
## biomass=data_query_clk_pg(id=2262018)[1][[1]]
## biomass=mon_qtr_df_creator(biomass,keep_col =c("Relevant_Date","Total"))
## biomass$Total=biomass$Total/1000
## names(biomass)[2]='Biomass'
## #%%
## waste_energy=data_query_clk_pg(id=2262016)[1][[1]]
## waste_energy=mon_qtr_df_creator(waste_energy,keep_col =c("Relevant_Date","Total"))
## waste_energy$Total=waste_energy$Total/1000
## names(waste_energy)[2]='Waste to energy (off-grid)'
## #%%
## large_hydro=data_query_clk_pg(id=2262015)[1][[1]]
## large_hydro=mon_qtr_df_creator(large_hydro,keep_col =c("Relevant_Date","Total"))
## large_hydro$Total=large_hydro$Total/1000
## names(large_hydro)[2]='Large hydro'
## #%%
## nuclear=data_query_clk_pg(id=2046209)[1][[1]]
## nuclear=mon_qtr_df_creator(nuclear,keep_col =c("Relevant_Date","Total"))
## nuclear$Total=nuclear$Total/1000
## names(nuclear)[2]='Nuclear'
## #%%
## coal_lignite=data_query_clk_pg(id=2262013)[1][[1]]
## coal_lignite=mon_qtr_df_creator(coal_lignite,keep_col =c("Relevant_Date","Total"))
## coal_lignite$Total=coal_lignite$Total/1000
## names(coal_lignite)[2]='Coal (+ lignite)'
## #%%
## gas=data_query_clk_pg(id=2262014)[1][[1]]
## gas=mon_qtr_df_creator(gas,keep_col =c("Relevant_Date","Total"))
## gas$Total=gas$Total/1000
## names(gas)[2]='Gas'
## #%%
## diesel=data_query_clk_pg(id=2262012)[1][[1]]
## diesel=mon_qtr_df_creator(diesel,keep_col =c("Relevant_Date","Total"))
## diesel$Total=diesel$Total/1000
## names(diesel)[2]='Diesel'


## ----eval=FALSE, include=FALSE--------
## data_s=list(wind_pwr,solar_pwr,small_hydro,biomass,waste_energy,large_hydro,nuclear,coal_lignite,gas,diesel)
##
## h_just=0
## v_just=0.60
##
## num_brek=5
## max_pri_y=450
## min_pri_y=0
##
## n_col=1
## n_row=10
##
## my_chart_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
##                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
##
## my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
##                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
##
##
## power_inst_capacity_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=30,negative=TRUE,SIDE_BAR=FALSE,
##                         bar_thick=50,order_stack = TRUE,
##                         add_std_col=TRUE,
##                         legends_break=TRUE,legend_reverse=TRUE,
##                         legend_placing ='right',legend_ver_sp=0.20)
##
## power_inst_capacity_title=power_inst_capacity_chart[2][[1]]
## power_inst_capacity_chart=power_inst_capacity_chart[1][[1]]
## power_inst_capacity_src=wind_pwr_src
## power_inst_capacity_chart


## ----eval=FALSE, include=FALSE--------
## #Power | Wind
## wind_pwr=data_query_clk_pg(id=2262036)
## wind_pwr_src=wind_pwr[3][[1]]
## wind_pwr_add=wind_pwr[1][[1]]
## wind_pwr_add=mon_qtr_df_creator(wind_pwr_add,keep_col=c("Relevant_Date","Total"))
## wind_pwr_add$Total=as.numeric(wind_pwr_add$Total)
## names(wind_pwr_add)[2]='Wind power'
##
## #%%
## solar_pwr_add=data_query_clk_pg(id=2262035)[1][[1]]
## solar_pwr_add=mon_qtr_df_creator(solar_pwr_add,keep_col =c("Relevant_Date","Total"))
## solar_pwr_add$Total=as.numeric(solar_pwr_add$Total)
## names(solar_pwr_add)[2]='Solar power'
## #%%
## small_hydro_add=data_query_clk_pg(id=2262040)[1][[1]]
## small_hydro_add=mon_qtr_df_creator(small_hydro_add,keep_col=c("Relevant_Date","Total"))
## small_hydro_add$Total=as.numeric(small_hydro_add$Total)
## names(small_hydro_add)[2]='Small hydro'
## #%%
## #HARD_CODE::Since madx data  exectly zero ordering is not happening properly so 0.0001 added
## biomass_add=data_query_clk_pg(id=2262039)[1][[1]]
## biomass_add=mon_qtr_df_creator(biomass_add,keep_col =c("Relevant_Date","Total"))
## biomass_add$Total=as.numeric(biomass_add$Total+0.0001)
## names(biomass_add)[2]='Biomass'
## #%%
## waste_energy_add=data_query_clk_pg(id=2262041)[1][[1]]
## waste_energy_add=mon_qtr_df_creator(waste_energy_add,keep_col =c("Relevant_Date","Total"))
## waste_energy_add$Total=as.numeric(waste_energy_add$Total)
## names(waste_energy_add)[2]='Waste to energy (off-grid)'
## #%%
## large_hydro_add=data_query_clk_pg(id=2262042)[1][[1]]
## large_hydro_add=mon_qtr_df_creator(large_hydro_add,keep_col =c("Relevant_Date","Total"))
## large_hydro_add$Total=as.numeric(large_hydro_add$Total)
## names(large_hydro_add)[2]='Large hydro'
## #%%
## nuclear_add=data_query_clk_pg(id=2262045)[1][[1]]
## nuclear_add=mon_qtr_df_creator(nuclear_add,keep_col =c("Relevant_Date","Total"))
## nuclear_add$Total=as.numeric(nuclear_add$Total+0.0001)
## names(nuclear_add)[2]='Nuclear'
## #%%
## coal_lignite_add=data_query_clk_pg(id=2262046)[1][[1]]
## coal_lignite_add=mon_qtr_df_creator(coal_lignite_add,keep_col =c("Relevant_Date","Total"))
## coal_lignite_add$Total=as.numeric(coal_lignite_add$Total)
## names(coal_lignite_add)[2]='Coal (+ lignite)'
## #%%
## gas_add=data_query_clk_pg(id=2262038)[1][[1]]
## gas_add=mon_qtr_df_creator(gas_add,keep_col =c("Relevant_Date","Total"))
## gas_add$Total=as.numeric(gas_add$Total+0.0001)
## names(gas_add)[2]='Gas'
## #%%
## diesel_add=data_query_clk_pg(id=2262047)[1][[1]]
## diesel_add=mon_qtr_df_creator(diesel_add,keep_col =c("Relevant_Date","Total"))
## diesel_add$Total=as.numeric(diesel_add$Total+0.0001)
## names(diesel_add)[2]='Diesel'


## ----eval=FALSE, include=FALSE--------
## data_s=list(wind_pwr_add,solar_pwr_add,waste_energy_add,small_hydro_add,
##             large_hydro_add,coal_lignite_add,gas_add,diesel_add,biomass_add,nuclear_add)
##
##
## h_just=0.60
## v_just=2.5
##
## num_brek=6
## max_pri_y=5000
## min_pri_y=1000
##
## n_col=1
## n_row=10
## max_overlaps=50
##
## my_chart_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
##                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
##
## my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
##                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
## power_inst_capacity_add_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=100,negative=TRUE,SIDE_BAR=FALSE,
##                         bar_thick=50,order_stack = TRUE,
##                         add_std_col=TRUE,
##                         legends_break=TRUE,legend_reverse=TRUE,
##                         legend_placing ='right',legend_ver_sp=0.20)
##
## power_inst_capacity_add_title=power_inst_capacity_add_chart[2][[1]]
## power_inst_capacity_add_chart=power_inst_capacity_add_chart[1][[1]]
## power_inst_capacity_add_src=wind_pwr_src
## power_inst_capacity_add_chart


## ----eval=FALSE, include=FALSE--------
## #Foodgrain Procurement
## wheat_procure= data_query_clk_pg(2156959)
## wheat_procure_src=wheat_procure[3][[1]]
## wheat_procure=wheat_procure[1][[1]]
## wheat_procure$Unit=wheat_procure$Unit/100000
## wheat_procure <- wheat_procure %>% mutate(Unit = na.locf(Unit, na.rm = F))
## colnames(wheat_procure)=c("Relevant_Date","Wheat (LMT)")
## wheat_procure=mon_year_df_creator(wheat_procure,
##                                   keep_col =c("Relevant_Date","Wheat (LMT)"),
##                                   ffill=FALSE)
##
##
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(wheat_procure)
##
## h_just=0
## v_just=0.75
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=4
## max_pri_y=500
## min_pri_y=0
##
## my_chart_col=c("GOLDEN ROD 1")
## my_legends_col=c("GOLDEN ROD 1")
##
## Wheat_proc_FCI_chart=stacked_bar_chart_niif(data_s,
##                                             x_axis_interval="12 month",
##                                             data_unit="",
##                                             format_date =paste0("FY","%y"),
##                                             graph_lim=470,negative=FALSE,YTD=TRUE,bar_thick=200)
##
## ##
## Wheat_proc_FCI_title=Wheat_proc_FCI_chart[2][[1]]
## Wheat_proc_FCI_chart=Wheat_proc_FCI_chart[1][[1]]
## Wheat_proc_FCI_src=wheat_procure_src
## Wheat_proc_FCI_chart


## ----eval=FALSE, include=FALSE--------
## #Foodgrain Procurement
## rice_procure= data_query_clk_pg(2156957)
## rice_procure_src=rice_procure[3][[1]]
## rice_procure=rice_procure[1][[1]]
## rice_procure$Unit=rice_procure$Unit/100000
## rice_procure <- rice_procure %>% mutate(Unit = na.locf(Unit, na.rm = F))
## colnames(rice_procure)=c("Relevant_Date","Rice (LMT)")
## rice_procure=mon_year_df_creator(rice_procure,
##                                   keep_col =c("Relevant_Date","Rice (LMT)"))


## ----eval=FALSE, include=FALSE--------
## data_s=list(rice_procure)
##
## h_just=0
## v_just=0.75
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=4
## max_pri_y=600
## min_pri_y=0
##
## my_chart_col=c("GOLDEN ROD 1")
## my_legends_col=c("GOLDEN ROD 1")
##
##
## Rice_proc_FCI_chart=stacked_bar_chart_niif(data_s,
##                                             x_axis_interval="12 month",
##                                             data_unit="",
##                                             format_date =paste0("FY","%y"),
##                                             YTD=TRUE,
##                                             graph_lim=470,negative=FALSE,bar_thick=200)
##
## ##
## Rice_proc_FCI_title=Rice_proc_FCI_chart[2][[1]]
## Rice_proc_FCI_chart=Rice_proc_FCI_chart[1][[1]]
## Rice_proc_FCI_src=rice_procure_src
## Rice_proc_FCI_chart


## ----eval=FALSE, include=FALSE--------
## #Foodgrain Procurement
## fg_prod= data_query_clk_pg(2211918)
## fg_prod_src=fg_prod[3][[1]]
## fg_prod=fg_prod[1][[1]]
## fg_prod$Unit=fg_prod$Unit/100000
## # fg_prod <- fg_prod %>% mutate(Unit = na.locf(Unit, na.rm = F))
## # fg_prod=mon_year_df_creator(fg_prod,keep_col =c("Relevant_Date","Unit"),
## #                             ffill = FALSE)
##
## colnames(fg_prod)=c("Relevant_Date","Foodgrain production (LMT)")


## ----eval=FALSE, include=FALSE--------
## data_s=list(fg_prod)
##
## h_just=0
## v_just=0.75
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=4
## max_pri_y=3500
## min_pri_y=0
##
## my_chart_col=c("GOLDEN ROD 1")
## my_legends_col=c("GOLDEN ROD 1")
##
##
## Fg_prod_FCI_chart=stacked_bar_chart_niif(data_s,
##                                             x_axis_interval="12 month",
##                                             data_unit="",YTD=TRUE,
##                                             format_date =paste0("FY","%y"),
##                                             graph_lim=470,negative=FALSE,bar_thick=200)
##
## ##
## Fg_prod_FCI_title=Fg_prod_FCI_chart[2][[1]]
## Fg_prod_FCI_chart=Fg_prod_FCI_chart[1][[1]]
## Fg_prod_FCI_src=fg_prod_src
## Fg_prod_FCI_chart


## ----eval=FALSE, include=FALSE--------
## #Foodgrain Procurement
## youth_employ= data_query_clk_pg(2157314,year = TRUE)
## youth_employ_src=youth_employ[3][[1]]
## youth_employ=youth_employ[1][[1]]
## colnames(youth_employ)=c("Relevant_Date","Rising employability of youth (%)")


## ----eval=FALSE, include=FALSE--------
## data_s=list(youth_employ)
##
## h_just=0
## v_just=0.75
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=4
## max_pri_y=60
## min_pri_y=0
##
## my_chart_col=c("GOLDEN ROD 1")
## my_legends_col=c("GOLDEN ROD 1")
##
##
## youth_employ_chart=stacked_bar_chart_niif(data_s,
##                                             x_axis_interval="12 month",
##                                             data_unit="",YTD=FALSE,
##                                             show_all = TRUE,
##                                             DATE_HEADER = FALSE,
##                                             format_date = '%Y',
##                                             show_fu_date = TRUE,
##                                             graph_lim=100,negative=FALSE,bar_thick=200)
##
## ##
## youth_employ_title=youth_employ_chart[2][[1]]
## youth_employ_chart=youth_employ_chart[1][[1]]
## youth_employ_src=youth_employ_src
## youth_employ_chart


## ----eval=FALSE, include=FALSE--------
## # Net FDI
## india= data_query_clk_pg(id=2157834,year=TRUE)
## india_src=india[3][[1]]
## india=india[1][[1]]
## india$Category="India"
## india$Category1="India"
##
## brazil= data_query_clk_pg(id=2157832,year=TRUE)[1][[1]]
## brazil$Category="Brazil"
## brazil$Category1="Brazil"
##
##
## china= data_query_clk_pg(id=2157833,year=TRUE)[1][[1]]
## china$Category="China"
## china$Category1="China"
##
## france= data_query_clk_pg(id=2157806,year=TRUE)[1][[1]]
## france$Category="France"
## france$Category1="France"
##
##
## south_af= data_query_clk_pg(id=2157838,year=TRUE)[1][[1]]
## south_af$Category="South africa"
## south_af$Category1="South africa"
##
##
## usa= data_query_clk_pg(id=2157824,year=TRUE)[1][[1]]
## usa$Category="US"
## usa$Category1="US"
##
## uk= data_query_clk_pg(id=2157823,year=TRUE)[1][[1]]
## uk$Category="UK"
## uk$Category1="UK"
##
## germany= data_query_clk_pg(id=2157807,year=TRUE)[1][[1]]
## germany$Category="Germany"
## germany$Category1="Germany"
##
## indonesia= data_query_clk_pg(id=2157835,year=TRUE)[1][[1]]
## indonesia$Category="Indonesia"
## indonesia$Category1="Indonesia"
##
## argen= data_query_clk_pg(id=2232387,year=TRUE)[1][[1]]
## argen$Category="Mexico"
## argen$Category1="Mexico"
##
## net_fdi=rbind(india,brazil,china,france,south_af,usa,uk,germany,indonesia,argen)
## net_fdi=net_fdi[,c("Relevant_Date","Category","Category1","Value")]
## net_fdi$Value=net_fdi$Value/10^9
## net_fdi=net_fdi[net_fdi$Relevant_Date>=max(net_fdi$Relevant_Date),]
##


## ----eval=FALSE, include=FALSE--------
## data_s=net_fdi
##
##
## h_just=0.5
## v_just=1.5
##
## num_brek=7
## max_pri_y=100
## min_pri_y=150
##
## chart_label=8
## side_width=0
##
## n_col=5
## n_row=2
##
## my_chart_col=c("India"="GOLDEN ROD 1",
##                "Brazil"="DARK ORANGE 2",
##                "China"="#BF6E00",
##                "France"="GRAY 48",
##                "South africa"="TAN 1",
##                "US"="BURLYWOOD 1",
##                "UK"="#EE9572",
##                "Germany"="GRAY 88",
##                "Indonesia"="#8B2500",
##                "Mexico"="#8B6914")
##
##
## my_legends_col=c("India"="GOLDEN ROD 1",
##                "Brazil"="DARK ORANGE 2",
##                "China"="#BF6E00",
##                "France"="GRAY 48",
##                "South africa"="TAN 1",
##                "US"="BURLYWOOD 1",
##                "UK"="#EE9572",
##                "Germany"="GRAY 88",
##                "Indonesia"="#8B2500",
##                "Mexico"="#8B6914")
##
##
## #
## # my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
## #                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
## custom_lev=c("India","Brazil","China","France","South africa","US","UK","Germany","Indonesia","Mexico")
##
## country_fdi_chart=side_bar_chart_niif_rupee(data_s,graph_lim=30,
##                                                   negative=TRUE,
##
##                                                   DATE_HEADER = TRUE,
##                                                   pos_d=0.5,
##                                                   legends_break = TRUE,
##                                                   show_legend=FALSE,
##                                                   x_angle1=0,
##                                                   pos_lb=0.8,bar_thick=0.5,key_spacing=2)
##
## country_fdi_title=country_fdi_chart[2][[1]]
## country_fdi_chart=country_fdi_chart[1][[1]]
##
## country_fdi_src=india_src
## draw_key_line=draw_key_smooth
## country_fdi_chart
##


## ----eval=FALSE, include=FALSE--------
## #Health | Hib3
## Hib3= data_query_clk_pg(id=2157485,year = TRUE,set_perid=40)
## Hib3_src=Hib3[3][[1]]
## Hib3=Hib3[1][[1]]
## colnames(Hib3) = c("Relevant_Date","Hib3")
##
## # Health | Polio
## pol= data_query_clk_pg(id=2157486,year = TRUE,set_perid=40)[1][[1]]
## colnames(pol) = c("Relevant_Date","Polio")
##
##
## #Health | Measles
## Measles= data_query_clk_pg(id=2157487,year = TRUE,set_perid=40)[1][[1]]
## colnames(Measles) = c("Relevant_Date","Measles")
##
##
##
## #Health | Hepatitis B
## HepB3= data_query_clk_pg(id=2157488,year=TRUE,set_perid=40)[1][[1]]
## colnames(HepB3) = c("Relevant_Date","Hepatitis B")
##
##
## #Health | DPT
## DPT= data_query_clk_pg(id=2157489,year=TRUE,set_perid=40)[1][[1]]
## colnames(DPT) = c("Relevant_Date","DPT")
##
## #Health | BCG
## BCG= data_query_clk_pg(id= 2157490,year=TRUE,set_perid=40)[1][[1]]
## colnames(BCG) = c("Relevant_Date","BCG")
##
## immunization=cbind(DPT,HepB3,Hib3,Measles,pol)
## immunization=immunization[,c("Relevant_Date","DPT","Hepatitis B","Hib3","Measles","Polio")]
## immunization <- merge(immunization,BCG,by = "Relevant_Date", all = TRUE)
## immunization=immunization[,c("Relevant_Date",'BCG',"DPT","Hepatitis B","Hib3","Measles","Polio")]
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(immunization)
##
## # legend_key_width=0.50
##
## max_overlaps =60
## h_just_line=0
## v_just_line=3
##
## n_col=6
## n_row=1
##
##
## num_brek=8
## max_pri_y=120
## min_pri_y=0
##
## #
## # my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
## #                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
## my_legends_col=c("BCG"="GOLDEN ROD 1",
##                  "DPT"="DARK ORANGE 2",
##                  "Hepatitis B"="#BF6E00",
##                  "Hib3"="GRAY 48",
##                  "Measles"="#8B6914",
##                  "Polio"="BURLYWOOD 1")
##
## my_line_type=c("BCG"="solid",
##                "DPT"="solid",
##                "Hepatitis B"="solid",
##                "Hib3"="solid",
##                "Measles"="solid",
##                "Polio"="solid")
##
##
## immunization_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
##                                          graph_lim=120,show_older=TRUE,
##                                          format_date =paste0("%Y"),
##                                          x_angle1=0,calender_date = TRUE,
##                                          negative=TRUE,PMI_reference=FALSE,
##                                          BSE_Index=FALSE,legend_key_width=0.75,
##                                          led_position="center",CPI_reference=FALSE,
##                                          round_integer=FALSE,key_spacing=1)
##
## immunization_title=immunization_chart[2][[1]]
## immunization_chart=immunization_chart[1][[1]]
## immunization_src=Hib3_src
## immunization_chart


## ----eval=FALSE, include=FALSE--------
## #Health | Hib3
## india= data_query_clk_pg(id=2157667,year = TRUE,set_perid=40)
## india_src=india[3][[1]]
## india=india[1][[1]]
## colnames(india) = c("Relevant_Date","India")
##
## #Access To Electricity (% Of Population)
## China= data_query_clk_pg(id=2157715,year = TRUE,set_perid=40)[1][[1]]
## colnames(China) = c("Relevant_Date","China")
##
## #Access To Electricity (% Of Population)
## Bangladesh= data_query_clk_pg(id=2157741,year = TRUE,set_perid=40)[1][[1]]
## colnames(Bangladesh) = c("Relevant_Date","Bangladesh")
##
## #Access To Electricity (% Of Population)
## UAS= data_query_clk_pg(id=2157550,year=TRUE,set_perid=40)[1][[1]]
## colnames(UAS) = c("Relevant_Date","United states")
##
## #Access To Electricity (% Of Population)
## world= data_query_clk_pg(id=2157491,year=TRUE,set_perid=40)[1][[1]]
## colnames(world) = c("Relevant_Date","World")
##
## world_elec=list(world,China,india,UAS,Bangladesh)


## ----eval=FALSE, include=FALSE--------
## data_s=world_elec
##
## max_overlaps =30
## h_just_line=0
## v_just_line=0.75
##
## n_col=6
## n_row=1
##
## num_brek=5
## max_pri_y=125
## min_pri_y=0
##
##
##
## my_legends_col=c("World"="GOLDEN ROD 1",
##                  "China"="yellow",
##                  "India"="#BF6E00",
##                  "United states"="DARK ORANGE 2",
##                  "Bangladesh"="TAN 1")
##
## my_line_type=c("India"="dotted",
##                "World"="solid",
##                "China"="solid",
##                "Bangladesh"="solid",
##                "United states"="solid")
##
##
## world_elec_chart=multi_line_chart_niif(data_s,
##                                          x_axis_interval="24 month",
##                                          graph_lim=30,show_older=TRUE,
##                                          format_date =paste0("%Y"),
##                                          x_angle1=0,calender_date = TRUE,
##                                          negative=TRUE,
##                                          PMI_reference=FALSE,
##                                          BSE_Index=FALSE,
##                                          legend_key_width=0.50,
##                                          led_position="center",
##                                          CPI_reference=FALSE,
##                                          round_integer=FALSE,key_spacing=1)
##
## world_elec_title=world_elec_chart[2][[1]]
## world_elec_chart=world_elec_chart[1][[1]]
## world_elec_src=india_src
## world_elec_chart


## ----eval=FALSE, include=FALSE--------
## #
## india_ser_shr= data_query_clk_pg(id=2158076,year = TRUE,set_perid=40)
## india_ser_shr_src=india_ser_shr[3][[1]]
## india_ser_shr=india_ser_shr[1][[1]]
## colnames(india_ser_shr) = c("Relevant_Date","Overall (%)")
##
## #Access To Electricity (% Of Population)
## india_mdrn_ser_shr= data_query_clk_pg(id=2158075,year = TRUE,set_perid=40)[1][[1]]
## colnames(india_mdrn_ser_shr) = c("Relevant_Date","Modern services (%)")
##
##
## india_ser_mdrn_shr=list(india_ser_shr,india_mdrn_ser_shr)


## ----eval=FALSE, include=FALSE--------
## data_s=india_ser_mdrn_shr
##
## max_overlaps =30
## h_just_line=0
## v_just_line=0.75
##
## n_col=6
## n_row=1
##
##
## num_brek=5
## max_pri_y=8
## min_pri_y=0
##
## my_legends_col=c("Overall (%)"="GOLDEN ROD 1",
##                  "Modern services (%)"="DARK ORANGE 2")
##
## my_line_type=c("Overall (%)"="solid",
##                "Modern services (%)"="solid")
##
##
## india_overall_ser_shr_chart=multi_line_chart_niif(data_s,
##                                          x_axis_interval="12 month",
##                                          graph_lim=30,show_older=TRUE,
##                                          format_date =paste0("%Y"),
##                                          x_angle1=0,
##                                          negative=TRUE,
##                                          PMI_reference=FALSE,
##                                          BSE_Index=FALSE,
##                                          legend_key_width=0.50,
##                                          led_position="center",
##                                          CPI_reference=FALSE,
##                                          round_integer=FALSE,key_spacing=1)
##
## india_overall_ser_shr_title=india_overall_ser_shr_chart[2][[1]]
## india_overall_ser_shr_chart=india_overall_ser_shr_chart[1][[1]]
## india_overall_ser_shr_src=india_ser_shr_src
## india_overall_ser_shr_chart


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## aaum_t_30=data_query_clk_pg(id=286291)
## aaum_t_30_src=aaum_t_30[3][[1]]
## aaum_t_30=aaum_t_30[1][[1]]
## names(aaum_t_30)[2]='Top 30 cities'
## #%%
## aaum_byt_30=data_query_clk_pg(id=286282)[1][[1]]
## names(aaum_byt_30)[2]='Beyond top 30 cities'


## ----eval=FALSE, include=FALSE--------
## data_s=list(aaum_t_30,aaum_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=50
## min_pri_y=10
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## MF_AAUM_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=50,
##                         negative=TRUE,SIDE_BAR=TRUE,bar_thick=25,
##                         single_bar_width = 100,
##                         order_stack = FALSE)
##
## MF_AAUM_top_city_gr_title=MF_AAUM_top_city_gr_chart[2][[1]]
## MF_AAUM_top_city_gr_chart=MF_AAUM_top_city_gr_chart[1][[1]]
## MF_AAUM_top_city_gr_src=aaum_t_30_src
## MF_AAUM_top_city_gr_chart


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## fw_t_30=data_query_clk_pg(id=2214225)
## fw_t_30_src=fw_t_30[3][[1]]
## fw_t_30=fw_t_30[1][[1]]
## fw_t_30$Total=fw_t_30$Total/1000
## names(fw_t_30)[2]="Top 30 cities"
## #%%
## fw_byt_30=data_query_clk_pg(id=2214227)[1][[1]]
## fw_byt_30$Total=fw_byt_30$Total/1000
## names(fw_byt_30)[2]="Beyond top 30 cities"


## ----eval=FALSE, include=FALSE--------
## data_s=list(fw_t_30,fw_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=200
## min_pri_y=10
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## FW_reg_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=30,negative=TRUE,SIDE_BAR=TRUE,bar_thick=25,order_stack = FALSE)
##
## FW_reg_top_city_gr_title=FW_reg_top_city_gr_chart[2][[1]]
## FW_reg_top_city_gr_chart=FW_reg_top_city_gr_chart[1][[1]]
## FW_reg_top_city_gr_src=fw_t_30_src
## FW_reg_top_city_gr_chart


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## credit_t_30=data_query_clk_pg(id=2214236)
## credit_t_30_src=credit_t_30[3][[1]]
## credit_t_30=credit_t_30[1][[1]]
## credit_t_30$Growth=as.numeric(credit_t_30$Growth)
## names(credit_t_30)[2]='Top 30 cities'
## #%%
## credit_byt_30=data_query_clk_pg(id=2214235)[1][[1]]
## credit_byt_30$Growth=as.numeric(credit_byt_30$Growth)
## names(credit_byt_30)[2]='Beyond top 30 cities'


## ----eval=FALSE, include=FALSE--------
## data_s=list(credit_t_30,credit_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=40
## min_pri_y=10
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## Credit_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         graph_lim=30,negative=TRUE,SIDE_BAR=TRUE,bar_thick=10,order_stack = FALSE)
##
## Credit_top_city_gr_title=Credit_top_city_gr_chart[2][[1]]
## Credit_top_city_gr_chart=Credit_top_city_gr_chart[1][[1]]
## Credit_top_city_gr_src=credit_t_30_src
## Credit_top_city_gr_chart


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## nwcom_t_30=data_query_clk_pg(id=2214351)
## nwcom_t_30_src=nwcom_t_30[3][[1]]
## nwcom_t_30=nwcom_t_30[1][[1]]
## nwcom_t_30$Total=as.numeric(nwcom_t_30$Total/1000)
## names(nwcom_t_30)[2]="Top 30 cities"
## #%%
## nwcom_byt_30=data_query_clk_pg(id=2214352)[1][[1]]
## nwcom_byt_30$Total=as.numeric(nwcom_byt_30$Total/1000)
## names(nwcom_byt_30)[2]="Beyond top 30 cities"


## ----eval=FALSE, include=FALSE--------
## data_s=list(nwcom_t_30,nwcom_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=5
## max_pri_y=20
## min_pri_y=0
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## nwcom_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="24 month",data_unit="",
##                         graph_lim=30,negative=TRUE,SIDE_BAR=TRUE,bar_thick=10,order_stack = FALSE)
##
## nwcom_top_city_gr_title=nwcom_top_city_gr_chart[2][[1]]
## nwcom_top_city_gr_chart=nwcom_top_city_gr_chart[1][[1]]
## nwcom_top_city_gr_src=nwcom_t_30_src
## nwcom_top_city_gr_chart


## ----eval=FALSE, include=FALSE--------
## #Road Construction in India
## airport_t_30=data_query_clk_pg(id=2214216)
## airport_t_30_src=airport_t_30[3][[1]]
## airport_t_30=airport_t_30[1][[1]]
## airport_t_30$Total=as.numeric(airport_t_30$Total/10^6)
## names(airport_t_30)[2]='Top 30 cities'
## #%%
## airport_byt_30=data_query_clk_pg(id=2214213)[1][[1]]
## airport_byt_30$Total=as.numeric(airport_byt_30$Total/10^6)
## names(airport_byt_30)[2]='Beyond top 30 cities'


## ----eval=FALSE, include=FALSE--------
## data_s=list(airport_t_30,airport_byt_30)
##
## h_just=0
## v_just=0.60
## h_just_line=0
## v_just_line=0.60
##
## num_brek=3
## max_pri_y=15
## min_pri_y=0
##
##
## my_chart_col=c("GRAY 48","GOLDEN ROD 1")
## my_legends_col=c("GRAY 48","GOLDEN ROD 1")
##
## airport_top_city_gr_chart=stacked_bar_chart_niif(data_s,
##                         x_axis_interval="12 month",data_unit="",
##                         round_integer =TRUE,
##                         graph_lim=30,negative=FALSE,SIDE_BAR=TRUE,bar_thick=10,order_stack = FALSE,
##                         legend_reverse = FALSE)
##
## airport_top_city_gr_title=airport_top_city_gr_chart[2][[1]]
## airport_top_city_gr_chart=airport_top_city_gr_chart[1][[1]]
## airport_top_city_gr_src=airport_t_30_src
## airport_top_city_gr_chart


## ----eval=FALSE, include=FALSE--------
## # Tanishq, Peter England,  Swiggy listings, dominos, Bata, Barbeque nation
##
## Tanishq= data_query_clk_pg(id=286291)
## Tanishq_src=Tanishq[3][[1]]
## Tanishq_t_30=Tanishq[1][[1]]
## Tanishq_t_30$Category="Tanishq"
## Tanishq_t_30$Category1='Top 30 cities'
##
## Tanishq_byt_30= data_query_clk_pg(id=286282)[1][[1]]
## Tanishq_byt_30$Category="Tanishq"
## Tanishq_byt_30$Category1='Beyond top 30 cities'
##
## #Peter England
## pe_t_30= data_query_clk_pg(id=286601)[1][[1]]
## pe_t_30$Category="Peter England"
## pe_t_30$Category1='Top 30 cities'
##
## pe_byt_30= data_query_clk_pg(id=286591)[1][[1]]
## pe_byt_30$Category="Peter England"
## pe_byt_30$Category1='Beyond top 30 cities'
##
## # #Swiggy
## sw_t_30= data_query_clk_pg(id=286291)[1][[1]]
## sw_t_30$Category="Swiggy"
## sw_t_30$Category1='Top 30 cities'
## #
## sw_byt_30= data_query_clk_pg(id=286291)[1][[1]]
## sw_byt_30$Category="Swiggy"
## sw_byt_30$Category1='Beyond top 30 cities'
##
## # #dominos
## do_t_30= data_query_clk_pg(id=286291)[1][[1]]
## do_t_30$Category="Dominos"
## do_t_30$Category1='Top 30 cities'
## #
## do_byt_30= data_query_clk_pg(id=286291)[1][[1]]
## do_byt_30$Category="Dominos"
## do_byt_30$Category1='Beyond top 30 cities'
##
## #Bata
## bata_t_30= data_query_clk_pg(id=286444)[1][[1]]
## bata_t_30$Category="Bata"
## bata_t_30$Category1='Top 30 cities'
##
## bata_byt_30= data_query_clk_pg(id=286432)[1][[1]]
## bata_byt_30$Category="Bata"
## bata_byt_30$Category1='Beyond top 30 cities'
##
## # #Barbeque nation
## bqn_t_30= data_query_clk_pg(id=286291)[1][[1]]
## bqn_t_30$Category="Barbeque nation"
## bqn_t_30$Category1='Top 30 cities'
## #
## bqn_byt_30= data_query_clk_pg(id=286291)[1][[1]]
## bqn_byt_30$Category="Barbeque nation"
## bqn_byt_30$Category1='Beyond top 30 cities'
##
## # c1=rev(sort(store_count$Category1))
## # store_count=rbind(Tanishq_t_30,pe_t_30,bata_t_30)
##
## store_count=rbind(Tanishq_byt_30,Tanishq_t_30,pe_t_30,pe_byt_30,bata_byt_30,bata_t_30,sw_t_30,
##                   sw_byt_30,do_t_30,do_byt_30,bqn_t_30,bqn_byt_30)
## store_count=store_count[,c("Relevant_Date","Category","Category1","growth")]
## # net_fdi$Value=net_fdi$Value/10^9
## store_count=store_count[store_count$Relevant_Date>=max(store_count$Relevant_Date),]
## store_count$Category1=factor(store_count$Category1,levels=rev(sort(unique(store_count$Category1))))
##


## ----eval=FALSE, include=FALSE--------
## data_s=store_count
##
##
## h_just=0.5
## v_just=-0.5
##
## num_brek=5
## max_pri_y=50
## min_pri_y=10
##
## chart_label=8
## side_width=0
##
## n_col=5
## n_row=2
##
## my_chart_col=c("Top 30 cities"="GOLDEN ROD 1","Beyond top 30 cities"="DARK ORANGE 2")
## my_legends_col=c("Top 30 cities"="GOLDEN ROD 1","Beyond top 30 cities"="DARK ORANGE 2")
##
##
##
## custom_lev=c('Tanishq','Peter England','Bata','Swiggy','Dominos','Barbeque nation')
##
## store_locator_chart=side_bar_chart_niif_rupee(data_s,graph_lim=30,
##                                                   negative=TRUE,
##                                                   legends_reverse=FALSE,
##                                                   DATE_HEADER = TRUE,
##                                                   pos_d=0.5,
##                                                   legends_break = FALSE,
##                                                   show_legend=TRUE,
##                                                   SIDE_BAR=TRUE,
##                                                   x_angle1=0,
##                                                   pos_lb=0,bar_thick=0.5,key_spacing=2)
##
## store_locator_title=store_locator_chart[2][[1]]
## store_locator_chart=store_locator_chart[1][[1]]
##
## store_locator_src=Tanishq_src
## store_locator_line=draw_key_smooth
## store_locator_chart
##


## ----eval=FALSE, include=FALSE--------
## # Net FDI
## india_total= data_query_clk_pg(id=2158077,year=TRUE,set_perid=12)
## india_total_src=india_total[3][[1]]
## india_total=india_total[1][[1]]
## india_total$category1='Total (India)'
##
## global_total= data_query_clk_pg(id=2158082,year=TRUE,set_perid=12)[1][[1]]
## global_total$category1='Total (Global)'
##
## india_mdrn= data_query_clk_pg(id=2158071,year=TRUE,set_perid=12)[1][[1]]
## india_mdrn$category1='Modern (India)'
##
## global_mdrn= data_query_clk_pg(id=2158081,year=TRUE,set_perid=12)[1][[1]]
## global_mdrn$category1='Modern (Global)'
##
##
## india_ser=rbind(india_total,global_total,india_mdrn,global_mdrn)
## india_ser$Category = ifelse(india_ser$Relevant_Date<='2015-03-31',
##                             '2012-15', india_ser$Relevant_Date)
##
## # india_ser$Relevant_Date = ifelse(india_ser$Relevant_Date<='2015-03-31',
## #                             as.Date('2015-03-31'), india_ser$Relevant_Date)
##
## india_ser$Category = ifelse(india_ser$Relevant_Date>'2015-03-31',
##                             '2015-17', india_ser$Category)
## # india_ser$Relevant_Date = ifelse(india_ser$Relevant_Date>'2015-03-31',
## #                             as.Date('2017-03-31'), india_ser$Relevant_Date)
##
## india_ser$Category = ifelse(india_ser$Relevant_Date>'2020-03-31',
##                             '2020-23', india_ser$Category)
## # india_ser$Relevant_Date = ifelse(india_ser$Relevant_Date>'2017-03-31',
## #                             as.Date('2020-03-31'), india_ser$Relevant_Date)
##
##
## india_ser=india_ser[,c("Relevant_Date","Category","category1","growth")]
## india_ser$Relevant_Date=as.Date('2022-12-31')
## india_ser=india_ser[!duplicated(india_ser[ , c("Category","category1")]),]
##
## # india_ser$Value=india_ser$Value/10^9
## # india_ser=india_ser[india_ser$Relevant_Date>=max(india_ser$Relevant_Date),]
##
## india_ser=india_ser[order(india_ser$Relevant_Date), ]


## ----eval=FALSE, include=FALSE--------
## data_s=india_ser
##
## h_just=0.5
## v_just=1.5
##
## num_brek=7
## max_pri_y=40
## min_pri_y=20
##
## chart_label=8
## side_width=0
##
## n_col=5
## n_row=1
##
## my_chart_col=c("Total (India)"="GOLDEN ROD 1",
##                "Total (Global)"="DARK ORANGE 2",
##                "Modern (Global)"="#BF6E00",
##                "Modern (India)"="GRAY 48")
##
##
## my_legends_col=c("Total (India)"="GOLDEN ROD 1",
##                "Total (Global)"="DARK ORANGE 2",
##                "Modern (India)"="#BF6E00",
##                "Modern (Global)"="GRAY 48")
##
##
## #
## # my_legends_col=c("#8B6914","#8B2500","GRAY 88","#EE9572","BURLYWOOD 1",
## #                "TAN 1","GRAY 48","#BF6E00","DARK ORANGE 2","GOLDEN ROD 1")
##
## custom_lev=c("2012-15","2015-17","2020-23")
##
## india_glbl_ser_gr_chart=side_bar_chart_niif_rupee(data_s,graph_lim=30,
##                                                   negative=TRUE,
##
##                                                   DATE_HEADER = TRUE,
##                                                   pos_d=0.5,
##                                                   legends_break = FALSE,
##                                                   show_legend=TRUE,
##                                                   x_angle1=0,
##                                                   pos_lb=0.8,bar_thick=0.5,key_spacing=2
## )
##
## india_glbl_ser_gr_title=india_glbl_ser_gr_chart[2][[1]]
## india_glbl_ser_gr_chart=india_glbl_ser_gr_chart[1][[1]]
## india_glbl_ser_gr_src=india_total_src
## draw_key_line=draw_key_smooth
## india_glbl_ser_gr_chart
##


## ----eval=FALSE, include=FALSE--------
## #School Enrolment | Primary School | Boys
## psch_boy=data_query_clk_pg(id=2309708,year = TRUE,set_perid = 15)
## psch_boy_src=psch_boy[3][[1]]
## psch_boy=psch_boy[1][[1]]
## psch_boy$Total=psch_boy$Total/10^6
## names(psch_boy)[2]='Boys'
##
##
## #School Enrolment | Primary School | Girls
## psch_girl=data_query_clk_pg(id=2309709,year = TRUE,set_perid = 15)[1][[1]]
## psch_girl$Total=psch_girl$Total/10^6
## names(psch_girl)[2]='Girls '
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(psch_boy,psch_girl)
##
## h_just=0
## v_just=0.65
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=9
## max_pri_y=140
## min_pri_y=0
##
##
## my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
## my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")
##
## primary_schl_chart=stacked_bar_chart_niif(data_s,
##                                         x_axis_interval="12 month",
##                                         order_stack =FALSE,
##                                         show_grid = FALSE,
##                                         format_date ='FY%Y',
##                                         add_std_col = FALSE,
##
##                                         data_unit="",graph_lim=120,negative=TRUE,
##                                         round_integer = TRUE,bar_thick=150)
##
## primary_schl_title=primary_schl_chart[2][[1]]
## primary_schl_chart=primary_schl_chart[1][[1]]
## primary_schl_src=psch_boy_src
## primary_schl_chart
##


## ----eval=FALSE, include=FALSE--------
## # School Enrolment | Middle School | Boys
## mdlsch_boy=data_query_clk_pg(id=2309711,year = TRUE,set_perid = 15)
## mdlsch_boy_src=mdlsch_boy[3][[1]]
## mdlsch_boy=mdlsch_boy[1][[1]]
## mdlsch_boy$Total=mdlsch_boy$Total/10^6
## names(mdlsch_boy)[2]='Boys'
##
##
## #School Enrolment | Middle School | Girls
## mdlsch_girl=data_query_clk_pg(id=2309712,year = TRUE,set_perid = 15)[1][[1]]
## mdlsch_girl$Total=mdlsch_girl$Total/10^6
## names(mdlsch_girl)[2]='Girls '
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(mdlsch_boy,mdlsch_girl)
##
## h_just=0
## v_just=0.65
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=9
## max_pri_y=140
## min_pri_y=0
##
##
## my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
## my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")
##
## middle_schl_chart=stacked_bar_chart_niif(data_s,
##                                         x_axis_interval="12 month",
##                                         order_stack =FALSE,
##                                         add_std_col = FALSE,
##                                         data_unit="",graph_lim=120,negative=TRUE,
##                                         round_integer = TRUE,
##                                         bar_thick=150,
##                                         show_grid = FALSE,
##                                         format_date ='FY%Y')
##
## middle_schl_title=middle_schl_chart[2][[1]]
## middle_schl_chart=middle_schl_chart[1][[1]]
## middle_schl_src=mdlsch_boy_src
## middle_schl_chart
##


## ----eval=FALSE, include=FALSE--------
## # School Enrolment |  High School  | Boys
## highsch_boy=data_query_clk_pg(id=2309714,year = TRUE,set_perid = 15)
## highsch_boy_src=highsch_boy[3][[1]]
## highsch_boy=highsch_boy[1][[1]]
## highsch_boy$Total=highsch_boy$Total/10^6
## names(highsch_boy)[2]='Boys'
##
## #School Enrolment |  High School | Girls
## highsch_girl=data_query_clk_pg(id=2309715,year = TRUE,set_perid = 15)[1][[1]]
## highsch_girl$Total=highsch_girl$Total/10^6
## names(highsch_girl)[2]='Girls '
##


## ----eval=FALSE, include=FALSE--------
## data_s=list(highsch_boy,highsch_girl)
##
## h_just=0
## v_just=0.65
## h_just_line=1.15
## v_just_line=1.15
##
## num_brek=9
## max_pri_y=140
## min_pri_y=0
##
##
## my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
## my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1")
##
## high_schl_chart=stacked_bar_chart_niif(data_s,
##                                         x_axis_interval="12 month",
##                                         order_stack =FALSE,
##                                         add_std_col = FALSE,
##                                         data_unit="",graph_lim=120,negative=TRUE,
##                                         round_integer = TRUE, bar_thick=150,
##                                         show_grid = FALSE,
##                                         format_date ='FY%Y')
##
## high_schl_title=high_schl_chart[2][[1]]
## high_schl_chart=high_schl_chart[1][[1]]
## high_schl_src=highsch_boy_src
## high_schl_chart
##


  ## -------------------------------------
  summary1="Strong domestic demand continued to drive growth in India: automobile registrations, bank credit, passenger travel via air and rail, energy consumption have been robust over the past few months. Domestic demand along with higher private investments has driven the GDP growth in Q2FY23. Equity markets saw foreign portfolio investors become net buyers in November. However, industrial and core infrastructure growth weakened in October partially due to lesser workdays on account of festive season. Slowdown in global demand has impacted goods export, however large services trade surplus provides comfort for India's current account. Consumer inflation in November moderated as food prices softened. RBI reduced the pace of rate hikes as repo was increased by 35 bps in December-another 25-bps expected in Q4FY23."

  Growth="Growth: GDP growth in Q2FY23 at 6.3% yoy driven by higher domestic demand and private investments, supply side growth driven by services sector. Industrial production and core infrastructure growth slowed in Oct as cement and coal production weakened. With monsoons going away and sun not as bright (as winters approach), conventional energy made a come back in generation proportion."

  Demand="Demand: Domestic demand remained strong with higher automobile registrations, increased freight activity, higher than pre-Covid travel via rail in Oct, air travel stabilized at ~11 mn passengers in Nov. Electronic toll collection remains high at INR 46.5 bn in Nov. Banks may reduce excess SLR holdings as credit growth continued to record double-digit growth in Nov. Energy demand also picked up in Nov after a yoy fall in Oct"

  Inflation="Inflation: Retail inflation moderated to 5.9% in Nov, within RBI's target range, as compared to 6.8% in Oct driven by food prices softening. Wholesale inflation moderated to 5.9% in Nov from 8.4% in Oct as food and manufactured product prices contract. RBI raised repo by 35 bps in Dec taking the repo to 6.25%. Bloomberg consensus (median) forecasts projected another 25-bps rate hike in Q4FY23"

  Markets="Markets: RBI continued to withdraw surplus liquidity that has kept short-term yields elevated, above repo rate. Indian 10-year government yield remained stable even as global yields have been volatile. Indian benchmark indices performed strongly in Nov. Indian corporates mobilized more capital from debt markets in Oct than equity. Energy prices fell sharply on the expectation of a global growth slowdown"


  Trade="Trade: Merchandize exports fell materially from the monthly average ~USD 38 bn this year partly driven by global demand slowing. Imports remained high reflecting high commodity prices. Sharp fall in shipping freight rates as port activity remains stable. Services trade grew sharply in Oct: services trade surplus is at an all-time high of ~USD 12 bn"


  Foreign_exchange="Foreign exchange: Rupee strengthened against dollar in Nov by 1.0% over the previous month; volatility in currencies over last year is due to a stronger dollar. Forex reserves picked up in Nov to USD 550 billion (mainly due to currency revaluation) which provides seven months of healthy import cover"


  Investments="Investments: Foreign portfolio investors turn net buyers in Indian equity markets in Nov: net inflows of INR 338 bn. Domestic investors continued to invest in the market. Net foreign direct investment remained strong in H1FY23 at USD 20.2 bn. Investments via alternative channels picked up strongly across categories. Funds raised by venture capital funds at an all-time high of INR 30.8 billion in Q1FY23"

  Fiscal="Fiscal: GST revenue has stabilized at ~INR 1.5 tn in Nov. Gross tax revenues of the government were robust at INR 16.1 tn on a financial year-to-date basis up to Oct '22, or 58.4% of full year budget estimate. Government's fiscal deficit between Apr and Oct close to 46% of full year budget"

  Economy="Economy: Economic activity in India as seen in composite PMI remained in the expansionary zone; the US,UK, Germany and France recorded a contraction. Core infrastructure maintained growth momentum in Oct. With monsoons going away and sun not as bright (as winters approach), conventional energy made a come back in generation proportion"


  ############# EXCEL DESCRIPTION ################
  df <- read_excel("C:\\Users\\Administrator\\AdQvestDir\\codes\\NIIF_TITLE_AND_DESCRIPTION_GENERATION\\niif_updated_widgtes_description.xlsx")
  # df <- read_excel("C:\\Users\\Santonu\\Desktop\\ADQvest\\Error files\\Modified(corr)\\R_PPT\\niif_up.xlsx")
  grouped_data <- df %>%
    group_by(chart) %>%
    distinct( chart_title, slide_heading, slide_sub_heading, .keep_all = TRUE) %>%
    ungroup()
  df <- grouped_data %>% mutate_all(~replace_na(., ""))
  write.csv(df, "C:\\Users\\Administrator\\AdQvestDir\\codes\\NIIF_TITLE_AND_DESCRIPTION_GENERATION\\processed_data.csv", row.names = FALSE)
  titles <- df$chart_title
  headings <- df$slide_heading
  sub_headings <- df$slide_sub_heading


  ## -------------------------------------
  my_ppt=read_pptx(f)
  # officer::layout_summary(my_ppt)
  # p1=layout_properties(my_ppt,layout ="2 CHARTS TEXTBOX")
  i=1
  #######################################################################################
  my_ppt=add_slide(my_ppt,layout ="COVER",master ="NIIF MONTHLY REPORTS")
  my_ppt=executive_summary("Executive Summary",
                           summary1,Growth,Demand,Inflation,Markets,Trade,
                           Foreign_exchange,Investments,Fiscal)

  my_ppt=tabe_content("Macroeconomic indicators",
                      paste0("Economy and demand\nInflation and employment\nFiscal position"),

                      "Markets",
                      paste0("Liquidity\nBalance of payments and foreign exchange markets\nDebt,equity and commodity market"),

                      "Investments",
                      paste0("Institutional investments - FPI and DII\nForeign Direct Investments\nAlternative investments"),

                      "Infrastructure",
                      paste0("Ports, roads, railways and aviation\nPower and renewables"),

                      "Global",
                      paste0("Growth and inflation\nMarkets"))

  my_ppt=section_breaker("Macroeconomic indicators","Comment")

  ##########################################Page 5###########################################################
  #************Quarterly real GDP growth for India,/Quarterly real GVA growth : Page 5 ****************************************

  my_ppt=Two_Chart_big_comment(sub_headings[i],
                               Qtr_real_GDP_growth_chart,Qtr_real_GVA_gr_chart,
                               paste0("Quarterly real GDP growth for India, ",Qtr_real_GDP_growth_title),
                               titles[i],
                               paste0("Quarterly real GVA growth (% yoy), ",Qtr_real_GVA_gr_title),
                               titles[i+1],
                               Qtr_real_GDP_growth_src,Qtr_real_GVA_gr_src,
                               "Note: 1. Real GDP growth is based on 2012 prices\n           2. 10-year average real GDP growth is 5.5%",

                               'Note: 10-year average real GVA growth is  5.5%',
                               headings[i])
  i=i+2
  #########################################Page 6############################################################
  #************Quarterly real GDP growth by segments/current prices : Page 6 ****************************************
  my_ppt=Two_Chart_big_comment(sub_headings[i],
                               Qtr_real_GDP_seg_gr_chart,Qtr_real_GVA_com_gr_chart,
                               paste0("Quarterly real GDP growth by components (% yoy), ",Qtr_real_GDP_seg_gr_title),
                               titles[i],
                               paste0("Quarterly real GVA growth by components (% yoy), ",Qtr_real_GVA_com_gr_title),

                               titles[i+1],
                               Qtr_real_GDP_seg_gr_src,Qtr_real_GVA_com_gr_src,
                               note1="",note2="",
                               headings[i])
  i=i+2

  #########################################Page 7############################################################
  #************* Index of Industrial Production -IIP / Cement Production / Growth in Steel Production / Coal Production : Page 7 ****************************************
  my_ppt=Four_Chart(sub_headings[i],
                    Index_of_Ind_Prod_IIP_chart,Cement_Prod_7_chart,
                    Steel_Prod_7_chart,Coal_Prod_7_chart,

                    paste0("IIP and output of eight core industries (% yoy), ",
                           Index_of_Ind_Prod_IIP_title),
                    titles[i],
                    paste0("Monthly cement production, ",Cement_Prod_7_title),
                    titles[i+1],
                    paste0("Monthly crude steel production, ",Steel_Prod_7_title),
                    titles[i+2],

                    paste0("Monthly coal production, ",Coal_Prod_7_title),
                    titles[i+3],
                    Index_of_Ind_Prod_IIP_src,Cement_Prod_7_src,
                    Steel_Prod_7_src,Coal_Prod_7_src,

                    c1_n="Note: IIP growth for May '20 and Apr '21 not shown due to low base effect",
                    c2_n="Note: Growth in cement production in Apr '21 not shown in the chart due to low base effect",
                    c3_n="Note: Growth in steel production in Apr '21 not shown in the chart due to low base effect",
                    c4_n="",
                    headings[i])
  i=i+4
  ######################################Page 8###############################################################
  #************ No of E way Bills / Monthly PMI manufacturing and Services : Page 8 ****************************************
  my_ppt=Two_Chart(sub_headings[i],
                   e_way_bills_chart,Mon_PMI_mnf_Ser_8_chart,

                   paste0("Monthly number of e-way bills, ",e_way_bills_8_title),
                   titles[i],
                   paste0("Monthly India PMI manufacturing and services, ",Mon_PMI_mnf_Ser_8_title),
                   titles[i+1],
                   e_way_bills_8_src,Mon_PMI_mnf_Ser_8_src,
                   headings[i],

                   paste0("Includes all inter-state and intra-state e-way bills\ne-way bill is a document required to be carried by a person in charge of the conveyance carrying any consignment of goods of value exceeding INR 50,000 under the Goods and Services Tax Act"),

                   paste0("Purchase Managers Index (PMI) is based on a monthly survey of supply chain managers across 19 industries: a number above 50 indicates expansion and below 50 indicates contraction.\nPMI for manufacturing and services dropped sharply between Apr '20 and Oct'20 due to impact of COVID-19"))

  i=i+2
  ######################################Page 9###############################################################
  #** Passenger Vehicle Sales (4W) / 2W vehicle sales : Page 9 *******************************

  my_ppt=Two_Chart(sub_headings[i],
                   passenger_vehicle_chart,TWO_W_chart,

                   paste0('Monthly passenger vehicle (PV) registrations, ',passenger_vehicle_title),
                   titles[i],
                   paste0('Monthly two-wheeler (2W) registrations, ',TWO_W_title),
                   titles[i+1],
                   passenger_vehicle_src,TWO_W_src,
                   headings[i],

                   paste0("Growth in passenger vehicles registration not shown in Jun '21 due to low base effect"),

                   paste0("Low growth in two-wheeler registration for Apr '20 and May '20, due to the impact of Covid lockdown, not shown in the chart"))

  i=i+2
  ##########################################Page 10######################################################
  #************ Commercial Vehicle Sales / 3W Sales : Page 10 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Com_Vehicle_chart,TW_chart,

                   paste0("Monthly commercial vehicle (CV) registrations, ",Com_Vehicle_title),
                   titles[i],

                   paste0("Monthly three-wheeler (3W) registrations, ",TW_title),
                   titles[i+1],

                   Com_Vehicle_src,TW_src,
                   headings[i],

                   paste0("Low commercial vehicle registrations growth in Apr 20 and May '20, due to impact of Covid lockdown, not shown in the chart"),

                   paste0("Growth in three-wheeler registrations for Apr '21 and May'21, and May '22 not depicted due to low base effect of Apr '20 and May '20, and May '21 respectively"))

  i=i+2
  ################################################Page 11#####################################################
  #************ Monthly Electricity Demand / Petroleum Consumption / Diesel Consumption and price / Petroll Consumption and price : Page 11 ****************************************
  #*
  my_ppt=Four_Chart(sub_headings[i],
                    Mon_Elec_Demand_chart,
                    Petroleum_Con_chart,
                    Diesel_Con_price_chart,
                    Petrol_Consumption_price_chart,

                    paste0("Monthly electricity demand in India, ",Mon_Elec_Demand_title),
                    titles[i],

                    paste0("Monthly petroleum consumption in India, ",Petroleum_Con_title),
                    titles[i+1],
                    paste0("Monthly diesel consumption and prices, ",Diesel_Con_price_title),
                    titles[i+2],
                    paste0("Monthly petrol consumption and prices, ",Petrol_Consumption_price_title),
                    titles[i+3],

                    Mon_Elec_Demand_src,
                    Petroleum_Con_src,
                    Diesel_Con_price_src,
                    Petrol_Consumption_price_src,

                    c1_n="",c2_n="",c3_n="",c4_n="",
                    headings[i]
  )
  i=i+4
  #############################################Page 12########################################################
  #************ Monthly Fertilizer Sales / Monthly Food Grain  Stock FCI / Water Reservoir Level / Domestic Tractor Sales : Page 12 ****************************************
  my_ppt=Four_Chart(sub_headings[i],
                    Mon_ferti_Sales_chart,
                    Mon_Food_Grain_Stock_FCI_chart,
                    Water_Reservoir_Level_chart,
                    Domestic_Tractor_reg_chart,

                    paste0("Monthly fertilizer sales, ",Mon_ferti_Sales_title),
                    titles[i],

                    paste0("Monthly food grain stocks with FCI (million tonnes), ",Mon_Food_Grain_Stock_FCI_title),

                    titles[i+1],

                    paste0("Monthly live water reservoir storage, ",Water_Reservoir_Level_title),
                    titles[i+2],

                    paste0("Monthly domestic tractor registrations, ",Domestic_Tractor_reg_title),
                    titles[i+3],


                    Mon_ferti_Sales_src,
                    Mon_Food_Grain_Stock_FCI_src,
                    Water_Reservoir_Level_src,
                    Domestic_Tractor_reg_src,

                    c1_n="",
                    c2_n="Note: Rice is excluding paddy",
                    c3_n="",
                    c4_n="Note: 1.Growth in tractor sales in March '20 and April '21 not shown above due to base effects \n2.Excluding Telangana, Andhra Pradesh, Madhya Pradesh,Lakshadweep",

                    headings[i])
  i=i+4
  #########################Page 13##################################################################
  #************ Rainfall August / Cumulative Rainfall : Page 13
  #* ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Weekly_rainfall_13_chart,cumulative_rainfall_13_chart,

                   paste0("Weekly rainfall (in mm), ",Weekly_rainfall_13_title),
                   titles[i],

                   paste0("Cumulative rainfall at the end of the week (in mm),",cumulative_rainfall_13_title),
                   titles[i+1],


                   Weekly_rainfall_13_src,cumulative_rainfall_13_src,

                   headings[i],

                   paste0(""),
                   paste0("")
  )
  i=i+2
  ###############################Page 14######################################################################
  #************ Rainfall August / Cumulative Rainfall : Page 14 ****************************************

  my_ppt=Two_Chart_map("High Frequency Indicators",
                       Monthly_Rainfall_map_14_chart,Cumulative_Rainfall_map_14_chart,

                       paste0("Monthly rainfall across states in mm, ",Monthly_Rainfall_map_14_title),
                       "Deficient rainfall in the Gangetic belt and north-eastern states in May",

                       paste0("Cumulative rainfall in mm, ",Cumulative_Rainfall_map_14_title),
                       "Possible drought in the Gangetic plains may impact kharif production",

                       Monthly_Rainfall_map_14_src,Cumulative_Rainfall_map_14_src,

                       paste0("Deficient rainfall in the rice growing Gangetic plains this monsoon may impact yield")
  )
  i=i+1
  #####################################################################################################
  #************ Rainfall Monthly / Cumulative Actual : Page Exp ****************************************
  #
  # my_ppt=Two_Chart_map("High Frequency Indicators",
  #                      mon_rf_act_map_chart,cum_rf_act_map_chart,
  #
  #                      paste0("Monthly rainfall across states in mm, ",mon_rf_act_map_title),
  #                     "Header",
  #
  #                      paste0("Cumulative rainfall in mm, ",cum_rf_act_map_title),
  #                     "Header",
  #
  #                     mon_rf_act_map_src,cum_rf_act_map_src,
  #
  # paste0("Title")
  # )
  # i=i+1

  ####################################Page 15#################################################################
  #************ Scheduled , Scheduled Commercial And Total Deposit with Growth Rate  / Scheduled , Scheduled Commercial And Total Credit with Growth Rate /Monthly credit-deposit difference/ CD Ratio  : Page 15 ***************************************
  #*CD_diff_chart,
  my_ppt=Four_Chart(sub_headings[i],
                    SCB_Deposit_chart,
                    SCB_Credit_chart,
                    SL_ratio_chart,
                    CD_Ratio_chart,

                    paste0("Monthly total bank deposits, ",SCB_Deposit_title),
                    titles[i],

                    paste0("Monthly total credit outstanding, ",SCB_Credit_title),
                    titles[i+1],

                    paste0("Monthly statutory liquidity ratio (SLR) of banks, ",SL_ratio_title),
                    titles[i+2],

                    paste0("Monthly outstanding credit-deposit ratio with SCBs, ",CD_Ratio_title),
                    titles[i+3],

                    SCB_Deposit_src,
                    SCB_Credit_src,
                    SL_ratio_src,
                    CD_Ratio_src,

                    c1_n="Note: 1.Total deposits for scheduled commercial banks\n           2.HDFC merger took effect in July 2023",
                    c2_n="Note: 1.Outstanding credit for scheduled commercial banks (SCBs)\n           2.HDFC merger took effect in July 2023",
                    c3_n="Note: Banks are required to hold 18% of their net demand and time liabilities as SLR,depicted as the black dotted line above",
                    c4_n="Note: HDFC merger took effect in July 2023",

                    headings[i])

  i=i+4
  ################################Page 16#####################################################################
  #************ UPI Tran  / Cash Circulation / RTGS Tran / Credit Card Tranactions   : Page 16 ****************************************
  #*
  my_ppt=Four_Chart(sub_headings[i],
                    UPI_Tran_chart,
                    Cash_Cir_chart,
                    RTGS_Tran_chart,
                    Credit_Card_Tran_chart,

                    paste0("Unified Payment Interface (UPI), ",UPI_Tran_title),
                    titles[i],
                    paste0("Currency in circulation, ",Cash_Cir_title),
                    titles[i+1],
                    paste0("RTGS transactions, ",RTGS_Tran_title),
                    titles[i+2],
                    paste0("Credit card transactions, ",Credit_Card_Tran_title),
                    titles[i+3],

                    UPI_Tran_src,
                    Cash_Cir_src,
                    RTGS_Tran_src,
                    Credit_Card_Tran_src,

                    c1_n="",c2_n="",
                    c3_n=paste0("Note: RTGS stands for Real Time Gross Settlements, that enables payments from one bank to another for a minimum amount of INR 200,000"),
                    c4_n="",

                    headings[i])
  i=i+4

  #####################################Page 17################################################################
  #************ Retail Inflation- CPI  / CPI - Inflation Target range / Wholesale Inflation - WPI /  : Page 17 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   CPI_Infl_Tar_range_chart, Wholesale_Infla_chart,

                   paste0("Monthly consumer price inflation (% yoy), ",CPI_Infl_Tar_range_title),
                   titles[i],

                   paste0("Monthly wholesale price inflation (% yoy), ",Wholesale_Infla_title),
                   titles[i+1],

                   CPI_Infl_Tar_range_src, Wholesale_Infla_src,
                   headings[i],

                   paste0("RBI in 2016 adopted flexible inflation target set at 4%, with 6% as upper bound and 2% as lower bound"))
  i=i+2
  ###################################Page 18##################################################################
  #************* Monthly labour participation and unemployment  / Quarterly Periodic Labor Force Survey/ Monthly enrollment numbers /Naukri jobspeak index:Page 18 ****************************************
  #*
  my_ppt=Four_Chart(sub_headings[i],
                    "Data not Available",
                    Quarterly_PLFS_chart,
                    Mon_enrollment_num_chart,
                    Naukri_jobspeak_index_chart,

                    "Monthly urban labour participation and unemployment, FY2016-FY2023 ",
                    titles[i],

                    paste0("Quarterly Periodic Labor Force Survey (urban), ",Quarterly_PLFS_title),
                    titles[i+1],

                    paste0("Monthly enrollment numbers, ",Mon_enrollment_num_title),
                    titles[i+2],

                    paste0("Naukri jobspeak index, ",Naukri_jobspeak_index_title),
                    titles[i+3],

                    "Source: Bloomberg, NIIF Research",
                    Quarterly_PLFS_src,
                    Mon_enrollment_num_src,
                    Naukri_jobspeak_index_src,

                    c1_n="",c2_n="Note: The quarterly PLFS is conducted by NSSO only for the urban areas",
                    c3_n="",
                    c4_n=paste0("Note: Naukri Jobspeak Index is calculated based on job listings added Naukri.com on monthly basis. (July 2008 = 1000)"),

                    headings[i])
  i=i+4
  ##################################Page 19###################################################################
  #************ Rural Wages : Page 19 ****************************************
  my_ppt=Four_Chart(sub_headings[i],
                    "Data not Available","Data not Available",
                    "Data not Available",Mon_MGNREGS_employ_data_chart,
                    "Chart Sub header1",titles[i],
                    "Chart Sub header2",titles[i+1],

                    "Chart Sub header1",titles[i+2],


                    paste0("Monthly MNREGA employment data, ",Mon_MGNREGS_employ_data_title),
                    titles[i+3],

                    "Source: Bloomberg, NIIF Research","Source: Bloomberg, NIIF Research",
                    "Source: Bloomberg, NIIF Research",Mon_MGNREGS_employ_data_src,

                    c1_n="",c2_n="",c3_n="",c4_n="",headings[i])
  i=i+4
  #################################Page 20####################################################################
  #*********** Rural Wages : Page 20 ***************************************
  my_ppt=Single_Chart(sub_headings[i],"Data Not  Available",
                      "Monthly snapshot of central government fiscal health (INR trillion), ",
                      titles[i],
                      "Source: CEIC, NIIF Research",

                      headings[i],

                      paste0("FY2022 is the period between April 2021 and March 2022; and FY2023 is the period between April 2022 and March 2023 \nYTD refers to financial year to date, i.e., from April onwards \nBE is the budget estimate for the stated financial year"))

  i=i+1
  #####################################Page 21################################################################
  my_ppt=Single_Chart(sub_headings[i],
                      GST_Col_chart,

                      paste0("Monthly composition of GST Revenue (INR billion), ",GST_Col_title),
                      titles[i],

                      GST_Col_src,
                      headings[i],

                      paste0("TTM is trailing twelve months\nGST collected for April '20 and May '20 assumed to be entirely CGST\nNominal GDP for FY2023 is the revised estimate of INR 273.1 trillion, and for FY2024 is the budget estimate of INR 301.8 trillion"))


  i=i+1
  ###############################Page 22######################################################################
  #**********************page:22*************************************
  my_ppt=section_breaker("Markets","Comment")

  #######################################Page 23##############################################################
  #************ RBI Repo And Reverse Repo Rates  / Surplus Liquidity : Page 23 ****************************************
  my_ppt=Two_Chart(sub_headings[i],
                   RBI_Repo_Reverse_Repo_chart,Surplus_liquidity_chart,

                   paste0("Repo rates (%), ",RBI_Repo_Reverse_Repo_title),
                   titles[i],

                   paste0("Liquidity injection or absorption by RBI (INR trillion), ",Surplus_liquidity_title),
                   titles[i+1],

                   RBI_Repo_Reverse_Repo_src,Surplus_liquidity_src,
                   headings[i],
                   "",
                   paste0("Liquidity operations by RBI include repo, term-repo, long-term repo operations, open market operations, marginal standing facility, and standing liquidity facilities"))
  i=i+2
  ########################################Page 24#############################################################
  #*************Projections for RBI's benchmark repo rate  page:24***************************

  my_ppt=Single_Chart(sub_headings[i],"Data Not  Available",
                      "Projections for RBI's benchmark repo rate (%), ",
                      titles[i],
                      "Source: Bloomberg, NIIF Research",
                      headings[i])
  i=i+1
  ########################################Page 25#############################################################
  #************ Monthly Trade Composition  / Servives Exp-Imp / Exports / Imports : Page 25 ****************************************
  #CA_Deficit_chart,
  my_ppt=Four_Chart(sub_headings[i],
                    Mon_Trade_Compo_chart,
                    Mon_services_chart,
                    Mon_Exports_chart,
                    Mon_Import_chart,

                    paste0("Monthly trade composition (USD billion), ",Mon_Trade_Compo_title),

                    titles[i],
                    paste0("Monthly services trade (USD billion), ",Mon_services_title),

                    titles[i+1],
                    paste0("Monthly merchandize exports (USD billion), ",Mon_Exports_title),

                    titles[i+2],
                    paste0("Monthly merchandize imports (USD billion), ",Mon_Import_title),

                    titles[i+3],

                    Mon_Trade_Compo_src,
                    Mon_services_src,
                    Mon_Exports_src,
                    Mon_Import_src,

                    c1_n="",c2_n="",c3_n="",c4_n="",
                    headings[i])
  i=i+4
  ######################################Page 26###############################################################
  #*********** Current Account Balance page:23 ***************************************
  #*
  my_ppt=Single_Chart(sub_headings[i],CA_Deficit_chart,
                      paste0("Quarterly current account balance (% of GDP), ",CA_Deficit_title),
                      titles[i],

                      CA_Deficit_src,
                      headings[i])
  i=i+1
  #####################################Page 27###############################################################
  #************ Monthly Forex Reserve  : Page 24 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_Forex_Reserve_chart,"Data not Available",
                   paste0("Monthly foreign exchange reserves, ",Mon_Forex_Reserve_title),
                   titles[i],

                   "Chart Sub header2",titles[i+1],
                   Mon_Forex_Reserve_src,"Source2",
                   headings[i],

                   paste0("Import cover calculated on total imports (merchandize plus services)\nServices imports for September assumed to be an average of previous six months \nImports for October assumed to be the average of last six months"),

                   paste0('"Surplus" forex buffer refers to the reserves available with RBI above the six-month import cover. Calculated as the difference between current (last available for the month) forex reserves and the six-month import cover (or "healthy forex reserves")\nGoods and services import for November assumed to be the average of last six months'))
  i=i+2
  ########################################Page 28#############################################################
  #************RBI remains a net buyer of US dollar : Page 25 ****************************************

  my_ppt=Single_Chart(sub_headings[i],Mon_purchase_sale_dollar_chart,
                      paste0("Monthly net purchase/(sale) of USD by RBI, ",Mon_purchase_sale_dollar_title),

                      titles[i],
                      Mon_purchase_sale_dollar_src,

                      headings[i],
                      paste0("Net purchase and sale of foreign currency in over-the-counter segment"))
  i=i+1
  ##########################################Page 29###########################################################
  #************Rupee Appreciation and Depreciation: Page 26 ****************************************

  my_ppt=Single_Chart(sub_headings[i],Rupee_Appre_Depre_chart,

                      paste0("INR performance vis-a-vis major currencies (%), ",Rupee_Appre_Depre_title),
                      titles[i],

                      Rupee_Appre_Depre_src,
                      headings[i],
                      paste0("Numbers are annualized for periods above 1 year \nPositive return indicates appreciation and negative means depreciation"))
  i=i+1
  #************EXPERIMENT***************************
  #
  # my_ppt=Two_Chart("Foreign Exchange Markets",
  #                  Rupee_Appre_Depre_chart,"Data not Available",
  #                  paste0("INR performance vis-à-vis major currencies (%), ",Rupee_Appre_Depre_title),
  #                 "INR appreciated against major currencies except USD over last year",
  #
  #                  "Chart Sub header2","Chart header 2",
  #
  #                  Rupee_Appre_Depre_src,"Source2",
  #
  #                 paste0("Rupee strengthens against dollar in Nov; volatility in the past year due to a stronger dollar"),
  #
  # paste0("Numbers are annualized for periods above 1 year \nPositive return indicates appreciation and negative means depreciation"),
  #
  # paste0(''))



  ################################Page 30#####################################################################
  #**********************page:27*************************************
  my_ppt=Single_Chart(sub_headings[i],"Data Not  Available",
                      "Daily India G-sec and corporate bond yields (%), ",
                      titles[i],
                      "Source: Bloomberg, NIIF Research",
                      headings[i])
  i=i+1
  ##########################################Page 31###########################################################
  #************ Monthly BSE Sensex performance,  / Monthly NSE Nifty performance : Page 28 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_BSE_Sen_perform_chart,Mon_NSE_Nifty_perform_chart,

                   paste0("Monthly BSE Sensex performance, ",Mon_BSE_Sen_perform_title),
                   titles[i],

                   paste0("Monthly NSE Nifty performance, ",Mon_NSE_Nifty_perform_title),
                   titles[i+1],

                   Mon_BSE_Sen_perform_src,Mon_NSE_Nifty_perform_src,
                   headings[i],

                   paste0("TTM: trailing twelve months\nMonthly data for stock indices is as on end of the month\nReturns do not take into account any dividend payouts and stock buybacks, if any"),

                   paste0("Monthly data for stock indices is as on end of the month\nReturns do not take into account any dividend payouts and stock buybacks, if any"))
  i=i+2
  ##############################################Page 32#######################################################
  #************ Daily NSE NIFTY Volatility Index  / Monthly average P/E ratio for Nifty-50 companies : Page 29 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Daily_nft_volatility_chart,Mon_avg_PE_ratio_Nifty_50_com_chart,

                   paste0("Daily NSE NIFTY Volatility Index (X), ",Daily_nft_volatility_title),
                   titles[i],

                   paste0("Monthly average P/E ratio for Nifty-50 companies,",Mon_avg_PE_ratio_Nifty_50_com_title),

                   titles[i+1],

                   Daily_nft_volatility_src,Mon_avg_PE_ratio_Nifty_50_com_src,
                   headings[i],

                   paste0("Volatility Index (VIX) represents the market's expectations of volatility over the next 30 days. India VIX is a based on the NIFTY Index Option prices\nVIX reached a peak of 83.6 on March 24, 2020 after announcement of nation-wide lockdowns due to COVID-19."),

                   paste0("Earnings assumed for P/E ratios are trailing 4-quarter earnings"))
  i=i+2
  ##############################################Page 33#######################################################
  #************ Monthly debt funds mobilized in India  / Monthly equity funds mobilized in India (INR billion) : Page 30 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_debt_funds_mobi_India_chart,Mon_equity_fu_mob_India_chart,

                   paste0("Monthly debt fund raising by corporate sector (INR billion),",Mon_debt_funds_mobi_India_title),
                   titles[i],

                   paste0("Monthly equity fund raising by corporate sector (INR billion), ",Mon_equity_fu_mob_India_title),
                   titles[i+1],

                   Mon_debt_funds_mobi_India_src,Mon_equity_fu_mob_India_src,

                   headings[i],
                   "",
                   paste0("IPO here includes only fresh issuances, does not include OFS or secondary sales"))

  i=i+2
  ########################################Page 34#############################################################
  #************Monthly inflows into SIPs (INR billion)  /Monthly net inflows into mutual funds (INR billion) : Page 34 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_inflows_SIPs,net_inflows_mf_chart,

                   paste0("Monthly inflows into SIPs, ",Mon_inflow_into_SIPs_38_title),
                   titles[i],

                   paste0("Monthly net inflows into mutual funds (INR billion), ",net_inflows_mf_title),
                   titles[i+1],

                   Mon_inflow_into_SIPs_38_src,net_inflows_mf_src,
                   headings[i],

                   paste0("SIP stands for Systematic Investment Plans, an investment route offered by mutual funds wherein one can invest a fixed amount in a Mutual Fund scheme at regular intervals"),
                   paste0("")
  )
  i=i+2
  #########################################Page 35############################################################
  #************ Daily market prices for listed REITs and InvITs  : Page 32 ****************************

  my_ppt=Chart_Medium_Table(sub_headings[i],
                            Daily_market_pri_REITs_InvITs_chart,
                            reit_returns_39,

                            paste0("Daily market prices for listed REITs and InvITs (INR), ",Daily_market_pri_REITs_InvITs_title),

                            titles[i],

                            Daily_market_pri_REITs_InvITs_src,
                            headings[i],

                            paste0("Return is calculated as on month end\nReturns are only based on stock price movement and do not take into account distribution via dividends"))

  i=i+2
  ######################################Page 36###############################################################
  #************ Monthly commodity index  : Page 33 ****************************************
  my_ppt=Single_Chart(sub_headings[i],Mon_com_index_World_Bank_chart,
                      paste0("Monthly commodity index, 2010=100, ",Mon_com_index_World_Bank_title),
                      titles[i],

                      Mon_com_index_World_Bank_src,
                      headings[i],

                      paste0("All commodity indices composed with CY2010 average prices as base for the index.\nEnergy comprises of crude oil,coal and natural gas; beverages comprise of cocoa, tea and coffee; food comprises of food oils, cereals (barley,maize, wheat and rice), meat and fruits (bananas, oranges); base metals comprise of aluminium, copper,zinc, lead, nickel and tin; precious metals comprise of gold, silver and platinum"))
  i=i+1
  ########################################Page 37#############################################################
  #************ Monthly coal prices  / Monthly brent crude oil prices / Monthly copper and iron ore prices / Monthly gold prices : Page 34 ****************************************

  my_ppt=Four_Chart(sub_headings[i],
                    Mon_coal_pri_chart,
                    Mon_brent_crd_oil_pri_chart,
                    Mon_Cu_Fe_ore_prices_WB_42_chart,
                    Mon_gold_pri_chart,

                    paste0("Monthly coal (South African) prices, ",Mon_coal_pri_title),
                    titles[i],

                    paste0("Daily Brent crude oil prices, ",Mon_brent_crd_oil_pri_title),
                    titles[i+1],

                    paste0("Daily copper and iron ore prices, ",Mon_Cu_Fe_ore_prices_WB_42_title),
                    titles[i+2],

                    paste0("Monthly gold prices, ",Mon_gold_pri_title),
                    titles[i+3],

                    Mon_coal_pri_src,
                    Mon_brent_crd_oil_pri_src,
                    Mon_Cu_Fe_ore_prices_WB_42_src,
                    Mon_gold_pri_src,

                    c1_n="",
                    c2_n="",
                    c3_n="",
                    c4_n="",

                    headings[i])
  i=i+4
  my_ppt=section_breaker("Investments","Comment")

  #####################################Page 39################################################################
  #************ Annual net FPI investments in India /Annual net DII investments in India  : Page 36 ****************************************
  # i=73
  my_ppt=Two_Chart(sub_headings[i],
                   Annual_FPI_invest_India_chart,Annual_DII_invest_India_chart,

                   paste0("Annual net FPI investments in India (INR billion), ",Annual_FPI_invest_India_title),
                   titles[i],

                   paste0("Annual net DII investments in India (INR billion), ",Annual_DII_invest_India_title),
                   titles[i+1],

                   Annual_FPI_invest_India_src,Annual_DII_invest_India_src,
                   headings[i],
                   paste0("Others comprise of debt-VRR and hybrid investments. Hybrid include investments in InvITs and REITs. Debt-VRR (voluntary retention route) allows FPIs to participate in repo transactions and also invest in exchange traded funds that invest in debt instruments."),

                   paste0("Domestic institutional investors (DII) are those institutional investors who undertake investment in securities and other financial assets (debt, AIFs, etc.) within India. These include insurance companies, banks, DFIs, mutual funds, NPS, EPFO."))
  i=i+2
  ###########################################Page 40##########################################################
  #*********** Monthly net FPI investments in India / Monthly net DII investments in India  : Page 36 ***************************************
  my_ppt=Two_Chart(sub_headings[i],
                   Mon_FPI_invest_India_chart,DII_invest_India_chart,

                   paste0("Monthly net FPI investments in India (INR billion), ",Mon_FPI_invest_India_title),
                   titles[i],

                   paste0("Monthly net DII investments in India (INR billion), ",DII_invest_India_title),
                   titles[i+1],

                   Mon_FPI_invest_India_src,DII_invest_India_src,
                   headings[i],
                   paste0("Others comprise of debt-VRR and hybrid investments. Hybrid include investments in InvITs and REITs. Debt-VRR (voluntary retention route) allows FPIs to participate in repo transactions and also invest in exchange traded funds that invest in debt instruments."),

                   paste0("Domestic institutional investors (DII) are those institutional investors who undertake investment in securities and other financial assets (debt, AIFs, etc.) within India. These include insurance companies, banks, DFIs, mutual funds, NPS, EPFO."))
  i=i+2
  ############################################Page 41#########################################################
  #************ Annual FDI investments  : Page 41 ****************************************
  my_ppt=Single_Chart(sub_headings[i],Annual_FDI_invest_chart,

                      paste0("Annual foreign direct investments (USD billion), ",Annual_FDI_invest_title),
                      titles[i],

                      Annual_FDI_invest_src,
                      headings[i],

                      "")
  i=i+1
  #########################################Page 42############################################################
  #************ Monthly FDI investments  : Page 37 ****************************************
  my_ppt=Single_Chart(sub_headings[i],Mon_FDI_invest_chart,

                      paste0("Monthly foreign direct investments (USD billion), ",Mon_FDI_invest_title),
                      titles[i],

                      Mon_FDI_invest_src,
                      headings[i],

                      "")
  i=i+1
  #43
  ###########################################Page 43##########################################################
  #************Quarterly AIF net fundraises (INR billion)\Quarterly Category I AIF net fundraises (INR billion): Page 38 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Qtr_AIF_net_fundrai_chart,Qtr_AIF_net_invest_chart,

                   paste0("Quarterly AIF net fundraises (INR billion), ",Qtr_AIF_net_fundrai_title),
                   titles[i],

                   paste0("Quarterly AIF net investments (INR billion), ",Qtr_AIF_net_invest_title),
                   titles[i+1],



                   Qtr_AIF_net_fundrai_src,Qtr_AIF_net_invest_src,
                   headings[i],

                   paste0("Category I Alternative Investment Funds (AIFs) invest in startup or early-stage ventures or social ventures, SMEs, infrastructure, or other sectors which the government or regulators consider as socially or economically desirable\nCategory II AIFs are those that do not fall in Category I and II and which do not undertake leverage other than to meet day-to-day operational requirements, such as real estate funds, private equity funds, etc.\nCategory III AIFs employ diverse trading strategies and may employ leverage including through investment in listed or unlisted derivatives such as hedge funds, PIPE funds, etc."),

                   paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number.")

  )
  i=i+2
  ##########################################Page 44###########################################################
  #************Annual AIF net fundraises (INR billion)\Annual Category I AIF net fundraises (INR billion): Page 38 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Annual_AIF_net_fundrai_chart,Annual_AIF_net_invest_chart,

                   paste0("Annual AIF net fundraises (INR billion), ",Annual_AIF_net_fundrai_title),
                   titles[i],

                   paste0("Annual AIF net investments (INR billion), ",Annual_AIF_net_invest_title),
                   titles[i+1],



                   Annual_AIF_net_fundrai_src,Annual_AIF_net_invest_src,
                   headings[i],

                   paste0("Category I Alternative Investment Funds (AIFs) invest in startup or early-stage ventures or social ventures, SMEs, infrastructure, or other sectors which the government or regulators consider as socially or economically desirable\nCategory II AIFs are those that do not fall in Category I and II and which do not undertake leverage other than to meet day-to-day operational requirements, such as real estate funds, private equity funds, etc.\nCategory III AIFs employ diverse trading strategies and may employ leverage including through investment in listed or unlisted derivatives such as hedge funds, PIPE funds, etc."),

                   paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number.")

  )
  i=i+2
  ##########################################Page 45###########################################################
  #************Quarterly AIF net investments (INR billion)\Quarterly Category I AIF net investments (INR billion):Page 39 ****************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Qtr_AIF_CI_net_fundrai_chart,Qtr_AIF_CI_net_inv_chart,

                   paste0("Quarterly Category I AIF net fundraises (INR billion), ",Qtr_AIF_CI_net_fundrai_title),
                   titles[i],



                   paste0("Quarterly Category I AIF net investments (INR billion), ",Qtr_AIF_CI_net_inv_title),
                   titles[i+1],

                   Qtr_AIF_CI_net_fundrai_src,Qtr_AIF_CI_net_inv_src,
                   headings[i],


                   paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number."),

                   paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number.")
  )
  i=i+2
  ##########################################################
  #
  #
  #
  my_ppt=section_breaker("Infrastructure","Comment")

  ############################################Page 46#########################################################
  #************ Monthly India truck freight index / Monthly national electronic road toll collection / Monthly road construction in India  : Page 44 ****************************************
  ##44
  ############################################Page 46#########################################################
  #*********** Monthly India truck freight index / Monthly national electronic road toll collection / Monthly road construction in India  : Page 44 ***************************************
  my_ppt=Four_Chart(sub_headings[i],
                    Mon_ihmcl_col_chart,
                    Mon_etc_road_toll_col_chart,
                    'Data not available',
                    Mon_road_con_India_chart,

                    paste0('Monthly road toll collection at NHAI tolls, ',Mon_ihmcl_col_title),
                    titles[i],
                    paste0("Monthly national electronic road toll collection, ",Mon_etc_road_toll_col_title),
                    titles[i+1],

                    "Monthly India truck freight index ('000s), FY2017-FY2023 (Oct '22)",
                    titles[i+2],

                    paste0("Monthly road construction in India, ",Mon_road_con_India_title),
                    titles[i+3],


                    Mon_ihmcl_col_src,
                    Mon_etc_road_toll_col_src,
                    "Source: Bloomberg, NIIF Research",
                    Mon_road_con_India_src,

                    c1_n="",
                    c2_n="Note: FasTag is primarily used at 800 national and 300 state highways, and at a few parking lots",
                    c3_n="Note: The index tracks average monthly truck freight rates between Delhi and 81 cities in India",
                    c4_n="",


                    headings[i])

  i=i+4
  ##############################################Page 47#######################################################
  #*********** Monthly cargo traffic at major ports / Daily shipping freight Indices  : Page: 42 ***************************************
  my_ppt=Two_Chart(sub_headings[i],
                   Mon_crg_tfc_mjr_port_chart,"Data not available",

                   paste0("Monthly cargo traffic at major ports, ",Mon_crg_tfc_mjr_port_title),
                   titles[i],

                   "Chart sub header",titles[i+1],
                   Mon_crg_tfc_mjr_port_src,"Source: Bloomberg, NIIF Research",

                   headings[i],
                   "",
                   paste0("Baltic Indices represent average shipping freights across 12 major international routes.Index units measured in points. (January 4, 1985 = 1,000).\nBaltic Dry Index measures freight rates for ships carrying bulk commodities like coal, iron ore, food grains, bauxite and alumina, steel and fertilizers.\nContainer freight measures actual spot freight rates in USD for 40-feet containers for 8 major east-west trade routes compiled as World Container Index (WCI). \n10-year average up to March 2023 for container freight is USD 2,700/FEU and for Baltic Dry is 1,347"))
  i=i+2
  #REMOVED
  #Container freight measures actual spot freight rates in USD for 40-feet containers for 8 major east-west trade routes compiled as World Container Index (WCI).

  ###########################################Page 48##########################################################
  #*********** Monthly passenger rail traffic / Monthly  traffic : Page 43 ***************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_psgr_rail_tfc,Mon_rail_freight_tfc_chart,

                   paste0("Monthly passenger rail traffic, ",Mon_psgr_rail_tfc_title),
                   titles[i],

                   paste0("Monthly rail freight traffic, ",Mon_rail_freight_tfc_title),
                   titles[i+1],

                   Mon_psgr_rail_tfc_src,Mon_rail_freight_tfc_src,
                   headings[i],
                   paste0("Growth in railway passengers for Apr '21 to Nov '21, and May '22 not depicted due to low base effect of Apr '20 to Nov '20, and May '21 respectively \nThis data reflects only inter-city passengers. It does not include intra-city commuters"))
  i=i+2
  ##########################################Page 49###########################################################
  #*********** rail cargo : Page 49 ***************************
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    rail_cargo_85,
    titles[i],
    paste0("Monthly railway freight (mn tonnes), ",rail_cargo_85_title),
    paste0("Source: Thurro, Indian Railways, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=6.450)
  i=i+1
  ##########################################Page 50###########################################################
  #*********** Monthly domestic air passengers / Monthly air cargo traffic : Page 44 ***************************************

  my_ppt=Two_Chart(sub_headings[i],
                   Mon_dom_air_psns_chart,Mon_air_cargo_tfc_chart,

                   paste0("Monthly domestic air passengers, ",Mon_dom_air_psns_title),
                   titles[i],

                   paste0("Monthly air cargo traffic, ",Mon_air_cargo_tfc_title),
                   titles[i+1],

                   Mon_dom_air_psns_src,Mon_air_cargo_tfc_src,
                   headings[i],

                   paste0("Growth in air passengers between Apr '21 and Oct '21, and May '22 not depicted due to low base effect of Apr '20 to Oct '20, and May '21."),

                   paste0("Growth in air cargo traffic between Apr '21 and Jun '21 not depicted due to low base effect of Apr '20 to Jun '20."))
  i=i+2
  ############################################Page 51#########################################################
  #*********** Monthly demand deficit of power  / Monthly clearance prices on IEX DAM / Monthly electricity generation / Monthly outstanding dues of discoms : Page 45 ***************************************
  # Mon_plant_load_factor_chart
  my_ppt=Four_Chart(sub_headings[i],
                    Mon_dmd_defi_power_chart,
                    Mon_DAM_Clearing_Price_chart,
                    Mon_electricity_gen_chart,
                    Mon_outst_dues_of_discoms_chart,

                    paste0("Monthly peak demand deficit of power (%), ",Mon_dmd_defi_power_title),
                    titles[i],

                    paste0("Monthly clearance prices on IEX DAM (INR/kWh), ",Mon_DAM_Clearing_Price_title),
                    titles[i+1],

                    paste0("Monthly electricity generation, ",Mon_electricity_gen_title),
                    titles[i+2],

                    paste0("Monthly outstanding dues of discoms, ",Mon_outst_dues_of_discoms_title),
                    titles[i+3],

                    Mon_dmd_defi_power_src,
                    Mon_DAM_Clearing_Price_src,
                    Mon_electricity_gen_src,
                    Mon_outst_dues_of_discoms_src,

                    c1_n="",c2_n="",
                    c3_n="Note: The residual difference between units of electricity generated and demanded is auxiliary consumption to run the power plants",
                    c4_n="",

                    headings[i])
  i=i+4

  # #####################################################################################################
  # #**********************IEX DAM hourly spot prices************************************
  # ##################################Page 53###################################################################
  # #**********************IEX DAM hourly spot prices************************************
  my_ppt=Single_Chart(sub_headings[i],elec_spot_pri_chart,

                      paste0("IEX DAM hourly spot prices",elec_spot_pri_title),
                      titles[i],

                      elec_spot_pri_src,
                      headings[i],

                      paste0(""))
  i=i+1


  #########################################Page 52############################################################
  #*********** Monthly generation from renewables  / Monthly generation from energy sources / Annual solar and wind power tariff / Monthly silicon and silver prices : Page 46 ***************************************

  my_ppt=Three_Chart_B(
    sub_headings[i],
    Mon_gen_frm_renew_chart,Mon_gen_energy_srcs_chart,"Data not Available",
    paste0("Monthly generation from renewables (billion kWh), ",Mon_gen_frm_renew_title),
    titles[i],

    paste0("Monthly generation from energy sources (billion kWh), ",Mon_gen_energy_srcs_title),
    titles[i+1],

    paste0("Chart Sub header"),
    titles[i+2],

    Mon_gen_frm_renew_src,
    Mon_gen_energy_srcs_src,

    "Source: Bloomberg, NIIF Research",
    headings[i])
  i=i+3
  #####
  my_ppt=section_breaker("Global","Comment")

  ########################################Page 54#############################################################
  #***************IMF GDP growth projections page:48***************************
  #*
  my_ppt=Single_Chart(
    sub_headings[i],"Data Not  Available",
    "IMF real GDP growth projections (%), ",
    titles[i],
    "Source: Bloomberg, NIIF Research",

    headings[i],
    paste0("For India, data and forecasts are presented on a fiscal year basis (Apr-Mar)\nFY stands for financial year with the period starting Apr 1 and ending on Mar 31\nThe 6.8% GDP growth for India under the 2022 column is projected for FY2022-23 Calendar year-wise, India's growth projections by IMF are 6.9% in CY2022 and 5.4% in CY2023"))
  i=i+1
  ########################################Page 55#############################################################
  #*********** Monthly PMI Composite Indices : Page 49 ***************************************
  my_ppt=Single_Chart(
    sub_headings[i],
    PMI_mjr_economies_9_chart,

    paste0("Monthly PMI composite indices across major economies, ",PMI_mjr_economies_9_title),
    titles[i],

    PMI_mjr_economies_9_src,
    titles[i],

    paste0("Impact of Covid on economic activity seen across countries for months between Feb '20 and May '20 and hence not shown in the chart.\nThe headline PMI Composite (Output) Index is a weighted average of the headline PMI Services Index and the Manufacturing Output Index (not the headline PMI manufacturing). Hence, a simple average of PMI Services and Manufacturing indices may not reflect in the PMI Composite."))
  i=i+1
  ######################################Page 56###############################################################
  #***************IMF GDP growth projections page:50***************************
  #*
  my_ppt=Single_Chart(
    sub_headings[i],"Data Not  Available",
    "Quarterly real GDP growth across countries (% yoy), ",

    titles[i],
    "Source: Bloomberg, NIIF Research",

    headings[i])

  i=i+1
  ######################################Page 57###############################################################
  #***************IMF GDP growth projections page:51***************************
  #*
  my_ppt=Single_Chart(
    sub_headings[i],"Data Not  Available",
    "Monthly consumer price inflation (% yoy), ",

    titles[i],

    "Source: Bloomberg, NIIF Research",
    headings[i])
  i=i+1
  #########################################Page 58############################################################
  #*********** Monthly performance of Nifty-50, Sensex and other global indices : Page 52 ***************************************

  my_ppt=Chart_Small_Table(sub_headings[i],
                           Mon_Nft_Sen_gbl_indices_chart,equity_markets_34,
                           paste0("Monthly performance of Nifty-50, Sensex and other global indices, returns in local currency (% yoy), ",Mon_Nft_Sen_gbl_indices_title),

                           titles[i],
                           Mon_Nft_Sen_gbl_indices_src,

                           headings[i],
                           "Return is calculated as on month end")
  i=i+2
  #####################################################################################################
  # #*********** Tax Collection govt / Govt's Gross market Borrowing : Page 21/Govt_Gross_market_Bor_21_chart ***************************************
  # #
  # my_ppt=Two_Chart("Fiscal Position",
  #                  Tax_Col_govt_chart,Govt_Gross_market_Bor_21_chart,
  #                  paste0("Monthly direct and indirect tax revenues (INR trillion), ",Tax_Col_govt_title),
  #                  "Tax collections for central government at INR 3.7 trillion",
  #
  #                  paste0("Monthly government market borrowing (INR trillion), ",Govt_Gross_market_Bor_21_title),
  #                  "Government's gross market borrowings at INR 2.9 trillion in October",
  #
  #                  Tax_Col_govt_src,Govt_Gross_market_Bor_21_src,
  #                  "Tax collections remain strong; governments" market borrowings picked up in FY2023")

  #####################################################################################################
  #*********** India G-sec and benchmark treasury bill yields  / 10-y benchmark yields across major DMs : Page 53 ***************************************

  # my_ppt=Two_Chart("Debt Markets",
  #                  India_G_sec_bill_chart,"Data not Available",
  #                  paste0("India G-sec and benchmark treasury bill yields (%), ",India_G_sec_bill_title),
  #                  "Short-term rates sharply rose as liquidity is withdrawn",
  #
  #                  "Chart Sub header2",
  #                  India_G_sec_bill_src,"Source2",
  #                  "Indian 10-year yield remains stable while major economies see volatility",
  #                  "Monthly Data as on Month End")

  #####################################################################################################

  ##Abdul
  clk  <- DBI::dbConnect(RClickhouse::clickhouse(), dbname =DB_cl,host = host_cl, port = port_cl,
                         user = user_cl,password =password_cl)
  pg  <- DBI::dbConnect(RPostgres::Postgres(), dbname =DB_pg,host = host_pg, port = port_pg,
                        user = user_pg,password =password_pg)
  #####################################################################################################
  #**********************page:53*************************************
  my_ppt=section_breaker("Annexures","Comment")
  # i=111
  #################################Page 60####################################################################
  #*********** Change in major economic indicators (% yoy) : Page 54 ***************************************
  #*
  #HARD COde: Since multiple source is there i.e why source is hard coded and it is done across all the tables
  my_ppt=abdul_table_type1(
    headings[i],
    sub_headings[i],
    economic_indicator_pg1_gr,
    titles[i],
    paste0("Change in major economic indicators (% yoy), ",economic_indicator_pg1_gr_title),

    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways, Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),

    l1=1.35,t1=1.6,
    l2=1.35,t2=1.2,
    l3=1.35,t3=1.38,
    l4=1.35,t4=6.98,
    l5=1.35,t5=7.30,
    w2=10,

    "Conditional formatting based on absolute values with respect to zero, with the largest negative values represented by dark red and largest positive values represented by dark green for each variable")
  i=i+1
  #####################################################################################################
  my_ppt=abdul_table_type1(
    headings[i],
    sub_headings[i],
    economic_indicator_pg2_gr,
    titles[i],
    paste0("Change in major economic indicators (% yoy), ",economic_indicator_pg2_gr_title),

    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways, Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),

    l1=1.35,t1=1.6,
    l2=1.35,t2=1.2,
    l3=1.35,t3=1.38,
    l4=1.35,t4=6.98,
    l5=1.35,t5=7.30,
    w2=10,

    "Conditional formatting based on absolute values with respect to zero, with the largest negative values represented by dark red and largest positive values represented by dark green for each variable")
  i=i+1
  # #####################################Page 61################################################################
  #*********** Major economic indicators (absolute values) : Page 55 ***************************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    economic_indicator_pg1,
    titles[i],
    paste0("Major economic indicators (absolute values), ",economic_indicator_pg1_title),

    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways,Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),

    l1=1.19,t1=1.75,l2=1.19,t2=7.35)
  i=i+1
  #####################################################################################################
  #*********** Major economic indicators (absolute values) : Page 55 ***************************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    economic_indicator_pg2,
    titles[i],
    paste0("Major economic indicators (absolute values), ",economic_indicator_pg2_title),

    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways,Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),

    l1=1.19,t1=1.75,l2=1.19,t2=7.35)
  i=i+1
  ########################################Page 62#############################################################

  ########################################Page 62#############################################################

  my_ppt=abdul_table_type1(
    headings[i],
    sub_headings[i],
    economic_indicator_pg1_gr,
    titles[i],
    paste0("Change in major economic indicators (% yoy), ",economic_indicator_pg1_gr_title),

    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways, Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),

    l1=1.35,t1=1.6,
    l2=1.35,t2=1.2,
    l3=1.35,t3=1.38,
    l4=1.35,t4=6.98,
    l5=1.35,t5=7.30,
    w2=10,

    "Conditional formatting based on absolute values with respect to zero, with the largest negative values represented by dark red and largest positive values represented by dark green for each variable")
  i=i+1
  #########################################Page 63############################################################

  my_ppt=abdul_table_type1(
    paste0(" "),
    "Subtitle",
    economic_indicator_abs_st_54,
    " ",
    paste0("Change in major economic indicators across major states, ",format(as.Date(max_date_abs), '%b %Y')),


    paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, NIIF Research"),
    l1=1.35,t1=1.6,
    l2=1.35,t2=1.2,
    l3=1.35,t3=1.38,
    l4=1.35,t4=6.98,
    l5=1.35,t5=7.30,
    w2=10,
    note_abs)

  #######################################Page 64##############################################################
  #*********** Monthly index of industrial production (% yoy) : Page 56 ***************************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    industrial_Prod_56,
    titles[i],
    paste0("Monthly index of industrial production (% yoy), ",industrial_Prod_56_title),
    paste0("Source: Thurro, MOSPI, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=5.882)
  i=i+1
  #######################################Page 65##############################################################
  #*********** Monthly total credit outstanding by sector (INR trillion): Page 57 ***************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    credit_outstanding_57,
    titles[i],
    paste0("Monthly total credit outstanding by sector (INR trillion), ",credit_outstanding_57_title),
    paste0("Source: Thurro, RBI, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=6.5)
  i=i+1
  ########################################Page 66#############################################################
  #************Retail Loan : Page 66 ****************************
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    retail_loan_86,
    titles[i],
    paste0("Monthly retail loans (INR trillion), ",retail_loan_86_title),
    paste0("Source: Thurro, RBI, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=6.450)
  i=i+1
  #####################################Page 67################################################################
  #*********** Monthly consumer price inflation (% yoy): Page 58 ***************************
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    consumer_inflation_58,
    titles[i],
    paste0("Monthly consumer price inflation (% yoy), ",consumer_inflation_58_title),
    paste0("Source: Thurro, MoSPI, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=6.450)
  i=i+1
  #################################Page 68####################################################################
  #***************Wholesale inflation moderates page:59***************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    wholesale_inflation_59,

    titles[i],

    paste0("Monthly wholesale price inflation (% yoy), ",wholesale_inflation_59_title),
    paste0("Source: Thurro, EAI, NIIF Research"),
    l1=1.19,t1=2,l2=1.19,t2=5.6494)
  i=i+1
  #####################################################################################################

  my_ppt=section_breaker("Appendix","Comment")
  ##70
  ###########################################Page 70##########################################################
  #***Projections for real GDP growth /Bi-monthly median real GDP projections Page:61***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    "Data Not Available",bi_real_gva_gdp_chart,

    paste0("Projections for real GDP growth in FY2025 (% yoy), Jan '22 to Jul '22,"),
    titles[i],

    paste0("Bi-monthly median real GDP projections for FY2025 by RBI (% yoy), ",bi_real_gva_gdp_title),
    titles[i+1],

    "Source: CMIE, NIIF Research",bi_real_gva_gdp_src,
    headings[i],

    paste0("The data shown above is the projections made by same agencies across two time periods,Jan to April 2022 and May to July 2022"),

    paste0("RBI's Professional Forecasters' Survey presents short to medium term economic development on GDP growth, among other macroeconomic indicators. In every round of survey,questionnaires are shared with 30 to 40 selected forecasters."))
  i=i+2
  ###########################################Page 71##########################################################
  #*****Bi-monthly projections for consumer price inflation /Bi-monthly projections for wholesale price inflation  Page:62***************************
  #HARD CODE:FY is hard coded because its a projection.
  my_ppt=Two_Chart(
    sub_headings[i],
    bi_cpi_cpicore_chart,bi_wpi_wpicore_chart,
    paste0("Bi-monthly projections for consumer price inflation in FY2025 (% yoy) , ",bi_cpi_cpicore_title),
    titles[i],

    paste0("Bi-monthly projections for wholesale price inflation in FY2025 (% yoy). ",bi_wpi_wpicore_title),
    titles[i+1],

    bi_cpi_cpicore_src,bi_wpi_wpicore_src,
    headings[i],

    paste0("RBI's Professional Forecasters' Survey presents short to medium term economic development  on inflation, among other macroeconomic indicators. In every round of survey, questionnaires are shared with 30 to 40 selected forecasters."))
  i=i+2
  #############################################Page 72########################################################
  #*****Outstanding domestic central government borrowings /Outstanding central external government borrowings Page:63***************************

  my_ppt=Two_Chart(
    sub_headings[i],
    govt_int_debt_chart,govt_ext_debt_chart,
    paste0("Outstanding central government internal debt, ",govt_int_debt_title),
    titles[i],

    paste0("Outstanding central government external debt, ",govt_ext_debt_title),
    titles[i+1],

    govt_int_debt_src,govt_ext_debt_src,
    headings[i])

  i=i+2
  ###############################################Page 73######################################################
  #*****Quarterly growth in merchandize exports/Quarterly growth in merchandize imports  Page:64***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    Qtr_exp_gr_chart,Qtr_imp_gr_chart,
    paste0("Quarterly growth in merchandize exports (% yoy), ",Qtr_exp_gr_title),
    titles[i],

    paste0("Quarterly growth in merchandize imports (% yoy), ",Qtr_imp_gr_title),
    titles[i+1],

    Qtr_exp_gr_src,Qtr_imp_gr_src,
    headings[i])
  i=i+2
  #################################################Page 74####################################################
  #*****Quarterly survey for business expectations/Quarterly survey for business costs and profits Page:65***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    Qtr_bus_exp_chart,Qtr_bus_cost_chart,
    paste0("Quarterly survey for business expectations (%), ",Qtr_bus_exp_title),
    titles[i],

    paste0("Quarterly survey for business costs and profits (%), ",Qtr_bus_cost_title),
    titles[i+1],

    Qtr_bus_exp_src,Qtr_bus_cost_src,
    headings[i],

    paste0("The survey covers the non-government, non-financial private and public limited companies engaged in manufacturing for qualitative responses on indicators of demand, financial situation, price and employment expectations, etc. The expectations are reported as the  difference in percentage of the respondents' reporting optimism and that reporting pessimism. Better overall business situation is deemed to be optimistic"),

    paste0("Each metric is reported as the difference in percentage of the respondents' reporting optimism and that reporting pessimism. Values greater than zero indicate expansion while values less than zero indicate contraction"))
  i=i+2
  #############################################Page 75########################################################
  #*****Quarterly telecom-internet subscribers/Quarterly smartphone shipments Page:66***************
  #Santonu_Start
  # i=131
  my_ppt=Two_Chart(sub_headings[i],
                   Qtr_tele_sub_chart,retail_trend_chart,

                   paste0("Quarterly telecom and internet subscribers (million), ",Qtr_tele_sub_title),
                   titles[i],

                   paste0("Retail investment in mutual funds (INR ‘000s per folio), ",retail_trend_title),
                   titles[i+1],

                   Qtr_tele_sub_src,retail_trend_src,
                   headings[i],

                   "Internet subscribers' data available till Mar '22")

  i=i+2
  #############################################Page 76########################################################
  #*****Quarterly gross non-performing assets/Quarterly weighted average lending rates for SCBs Page:67***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    Qtr_gross_npa_chart,Qtr_walr_chart,

    paste0("Quarterly gross non-performing assets (% of gross advances), ", Qtr_gross_npa_title),
    titles[i],

    paste0("Monthly weighted average lending and deposit rates for SCBs (%), ",Qtr_walr_title),
    titles[i+1],

    Qtr_gross_npa_src,Qtr_walr_src,
    headings[i])
  i=i+2
  ###############################################Page 77######################################################
  #*****Quarterly mutual fund AUMs /Quarterly mutual fund folios  Page:68***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    Mf_aum_chart,Mf_folios_chart,

    paste0("Quarterly mutual fund AUMs (INR trillion), ",Mf_aum_title),
    titles[i],

    paste0("Quarterly mutual fund folios (million units), ",Mf_folios_title),
    titles[i+1],

    Mf_aum_src,Mf_folios_src,
    headings[i],

    paste0("High net-worth individuals (HNI) are individuals with investable capital greater than INR 20 million"))
  i=i+2
  ##76
  ############################################Page 78#########################################################
  #*****Quarterly outstanding deployment debt/Quarterly outstanding deployment equity  Page:69***************************
  my_ppt=Two_Chart(
    sub_headings[i],
    Mf_debt_chart,qtr_mfh_t_chart,

    paste0("Quarterly outstanding deployment in debt (INR trillion), ",Mf_debt_title),
    titles[i],

    paste0("Quarterly outstanding deployment in equity (INR trillion), ",qtr_mfh_t_title),
    titles[i+1],

    Mf_debt_src,qtr_mfh_t_src,
    headings[i],

    paste0("Instruments (ex-corporate debt) comprise of commercial paper, bank CDs, treasury bills and collateralized borrowing and lending obligations.\nOthers include PSU bonds/debt, equity linked debentures and notes, securitized debt, bank FDs and other instruments."))
  i=i+2
  #####################################################################################################
  #*****Annual FDI inflows by sector Page:74***************************
  #
  # my_ppt=abdul_table_type2(
  #        paste0("Higher trade deficit driving current account deficit in Q4FY22"),
  #       "Balance of payments",
  #        quarterly_bop_74_chart,
  #        paste0("Higher merchandize imports drive current account balance into deficit, higher investments boost financial account surplus"),
  #
  #        paste0("Quarterly balance of payments (USD billion), ",quarterly_bop_74_title),
  #        paste0("Source: Thurro, RBI, NIIF Research"),
  #        l1=1.19,t1=2,l2=1.19,t2=6.5)

  ##############################################Page 79#######################################################
  #*****New BOP***************************
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    quarterly_bop_74_chart,
    titles[i],

    paste0("Quarterly balance of payments (USD billion), ",quarterly_bop_74_title),
    paste0("Source: Thurro, RBI, NIIF Research"),
    l1=1.19,t1=1.5,l2=1.19,t2=7.35)

  i=i+1

  #####################################################################################################
  #*****Annual FDI inflows by sector Page:75***************************

  # my_ppt=abdul_table_type2(
  #        paste0("Higher trade deficit driving current account deficit in Q4FY22"),
  #       "Balance of payments",
  #        quarterly_bop_annual_chart,
  #        paste0("Higher merchandize imports drive current account balance into deficit, higher investments boost financial account surplus"),

  #        paste0("Annual balance of payments (USD billion), ",quarterly_bop_annual_title),
  #        paste0("Source: Thurro, RBI, NIIF Research"),
  #        l1=1.19,t1=2,l2=1.19,t2=6.5)

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    quarterly_bop_annual_chart,
    titles[i],

    paste0("Annual balance of payments (USD billion), ",quarterly_bop_annual_title),
    paste0("Source: Thurro, RBI, NIIF Research"),
    l1=1.19,t1=1.5,l2=1.19,t2=7.35)


  i=i+1




  #########################################Page 81############################################################
  #*****Annual FDI inflows by country Page:75***************************
  #*
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    quarterly_mfh_75_chart,
    titles[i],
    paste0("Monthly equity deployment by mutual funds (INR trillion), ",quarterly_mfh_75_title),

    paste0("Source: Thurro, SEBI, NIIF Research"),
    l1=1.19,t1=1.75,l2=1.19,t2=7.18)
  i=i+1
  #########################################Page 82############################################################
  #*****Annual FDI inflows by sector Page:76***************************

  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    Annual_FDI_inflows_by_Sector_76_chart,
    titles[i],
    paste0("Annual gross FDI inflows by sector (USD billion), ",Annual_FDI_inflows_by_Sector_76_title),
    paste0("Source: Thurro, Department for Promotion of Industry and Internal Trade, NIIF Research"),
    l1=1.19,t1=2,l2=1.19,t2=6.051)
  i=i+1
  ######################################Page 83###############################################################
  #*****Annual FDI inflows by country Page:77***************************
  #*
  my_ppt=abdul_table_type2(
    headings[i],
    sub_headings[i],
    Annual_FDI_inflows_by_country_77_chart,
    titles[i],
    paste0("Annual gross FDI inflows by sector (USD billion), ",Annual_FDI_inflows_by_country_77_title),
    paste0("Source: Thurro, Department for Promotion of Industry and Internal Trade, NIIF Research"),
    l1=1.19,t1=2,l2=1.19,t2=6.2163)
  i=i+1
  ##
  # 83
  ################################Page 84#########################################
  my_ppt=Single_Chart(sub_headings[i],
                      Cpi_sect_chart,

                      paste0("Contribution of key components to consumer price index (CPI) inflation (% yoy/pp), ",Cpi_sect_title),
                      titles[i],

                      Cpi_sect_src,
                      headings[i],

                      paste0("Food and beverages account for ~45.86%, intoxicants 2.38%, clothing and footwear 6.53%, housing 10.07%, fuel and light 6.84%, and household goods and services 3.8%, health 5.89%, transport and communication 8.59%, recreation 1.68%, education 4.46%, and personal care 3.89% weight in the headline consumer price index inflation"))
  i=i+1

  ################################Page 85#########################################
  #********** CPI Components Inflation: Page 76 **************************************
  my_ppt=Single_Chart(sub_headings[i],
                      Cpi_com_chart,

                      paste0("Contribution of key components to consumer food price inflation (CPI food) (% yoy/pp), ",Cpi_com_title),
                      titles[i],

                      Cpi_com_src,
                      headings[i],
                      paste0("Cereals account for ~9.67%, meat and fish 3.61%, egg 0.43%, milk 6.61%, oils 3.56%, fruits 2.89%, vegetables 6.04%, pulses 2.38%, sugar and confectionary 1.36%, and spices 2.5% weight in the consumer food price index inflation"))
  i=i+1
  ##################################Page 86########################################
  #********** WPI Sector inflation: Page 76 **************************************
  my_ppt=Single_Chart(sub_headings[i],
                      wpi_sect_chart,

                      paste0("Contribution of key components to wholesale price inflation (WPI) (% yoy/pp), ",wpi_sect_title),
                      titles[i],

                      wpi_sect_src,
                      headings[i],



                      paste0("Food articles account for ~15.26%, non-food 4.12%, mineral 0.83%, crude oil 2.41%, coal 2.14%, mineral oils 7.95%, electricity 3.06%, and manufactured products 64.23% weight in the wholesale price index inflation"))
  i=i+1
  ################################Page 87##########################################
  #********** IIP Sector: Page 76 **************************************
  my_ppt=Single_Chart(sub_headings[i],
                      iip_sect_chart,

                      paste0("Contribution from key components of index of industrial production-sector-based classification (% yoy/pp), ",iip_sect_title),
                      titles[i],

                      iip_sect_src,
                      headings[i],

                      paste0("Mining accounts for ~14.37%, manufacturing 77.63%, and electricity 7.99% weight in the Index of Industrial Production"))
  i=i+1
  ##
  #*********** IIP Used: Page 88 ***************************************
  my_ppt=Single_Chart(sub_headings[i],
                      iip_used_chart,

                      paste0("Contribution from key components of index of industrial production-use-based classification (% yoy/pp), ",iip_used_title),
                      titles[i],

                      iip_used_src,
                      headings[i],

                      paste0("Primary goods account for ~34.05%, capital goods 8.22%, intermediate 17.22%, infrastructure and construction goods 12.34%, consumer durables 12.84%, and consumer non-durable goods 15.33% weight in the Index of Industrial Production"))
  i=i+1

  ######################################################################## page 89
  #*
  my_ppt=Four_Chart(sub_headings[i],
                    pfcf_trend_chart,
                    gefc_trend_chart,
                    exp_trend_chart,
                    gdp_trend_chart,

                    paste0("sub header, ",pfcf_trend_title),
                    titles[i],

                    paste0("sub heading, ",gefc_trend_title),
                    titles[i+1],
                    paste0("sub heading, ",exp_trend_title),
                    titles[i+2],

                    paste0("sub heading, ",gdp_trend_title),
                    titles[i+3],

                    pfcf_trend_src,
                    gefc_trend_src,
                    exp_trend_src,
                    gdp_trend_src,

                    c1_n="",c2_n="",c3_n="",c4_n="",
                    headings[i]
  )
  i=i+4
  #####################################################################################################
  my_ppt=Three_Chart_B(
    sub_headings[i],
    gdp_trend_chart,
    pfcf_trend_chart,
    gefc_trend_chart,

    paste0("sub heading, ",gdp_trend_title),
    titles[i],
    paste0("sub header, ",pfcf_trend_title),
    titles[i+1],
    paste0("sub heading, ",gefc_trend_title),
    titles[i+2],

    gdp_trend_src,
    pfcf_trend_src,
    gefc_trend_src,

    headings[i])
  i=i+3

  #####################################################################################################
  #*********** Real effective exchange rate/Doller index: Page 90 ***************************************
  my_ppt=Two_Chart(sub_headings[i],
                   #"Foreign Exchange Markets",
                   Mon_re_exch_rt_chart,Mon_doller_indx_chart,

                   paste0("Monthly real effective exchange rate of INR, base year 2015-16 (X), ",Mon_re_exch_rt_title),
                   titles[i],

                   paste0("Monthly average dollar index (X), ",Mon_doller_indx_title),
                   titles[i+1],

                   Mon_re_exch_rt_src,Mon_doller_indx_src,
                   headings[i],


                   paste0("Real effective exchange rate (REER) is the weighted average of a country's currency in relation to basket of currencies of its major trading partners."),
                   paste0("Dollar index measures the relative strength of USD compared to other major currencies (EUR, GBP, JPY, SEK, CAD, CHF). A higher number indicates a stronger USD."))
  i=i+2
  ######################################################################################################CEA
  # my_ppt=Single_Chart("Power",
  #                    power_inst_capacity_chart,
  #
  #                    paste0(power_inst_capacity_title),
  #                    "All india sourse wise installed capacity (GW)",
  #
  #                    power_inst_capacity_src,
  #                    "",
  #
  # paste0(""))

  ######################################################################################################CEA
  # my_ppt=Single_Chart("Power",
  #                    power_inst_capacity_add_chart,
  #
  #                    paste0(power_inst_capacity_add_title),
  #                     "All india sourse wise installed capacity addition (MW)",
  #
  #                    power_inst_capacity_add_src,
  #                    "",
  #
  # paste0(""))

  ######################################################################################################
  #************Global service| Modern service: Page 76 **************************
  # my_ppt=Single_Chart("Sub title",global_ser_chart,
  #
  #                     paste0("Global services, ",global_ser_title),
  #                     "title",
  #
  #                     global_ser_src,
  # paste0(""),
  #
  # paste0(""))
  #####################################################################################################
  #************India service| Modern service: Page 76 **************************
  # my_ppt=Single_Chart("Sub title",india_ser_chart,
  #
  #                     paste0("India services, ",india_ser_title),
  #                     "title",
  #
  #                     india_ser_src,
  # paste0(""),
  #
  # paste0(""))
  ############################################################################################
  #************Indias's growth in service: Page 76 ***********************************
  # my_ppt=Single_Chart("Sub title",india_glbl_ser_gr_chart,
  #
  #                     paste0("India's services growth, ",india_glbl_ser_gr_title),
  #                     "title",
  #
  #                     india_glbl_ser_gr_src,
  # paste0(""),
  #
  # paste0(""))

  ############################################################################################
  #************India's Share in Overall and Modern service: Page 76 **************************
  # my_ppt=Single_Chart("Sub title",india_overall_ser_shr_chart,
  #
  #                     paste0("India overall services share, ",india_overall_ser_shr_title),
  #                     "title",
  #
  #                     india_overall_ser_shr_src,
  # paste0(""),
  #
  # paste0(""))

  #####################################################################################################
  #************ Wheat Procurement: Page 76 ****************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     Fg_prod_FCI_chart,
  #
  #                    paste0("Foodgrain production, ",Fg_prod_FCI_title),
  #                     "Header",
  #
  #                    Fg_prod_FCI_src,
  #                    "Title",
  #
  # paste0(""))

  ##########################################################################
  #************ Wheat Procurement: Page 76 ****************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     Rice_proc_FCI_chart,
  #
  #                    paste0("Rice procurement, ",Rice_proc_FCI_title),
  #                     "Header",
  #
  #                    Rice_proc_FCI_src,
  #                    "Title",
  #
  # paste0(""))
  ##########################################################################
  #************ Rice Procurement: Page 76 ****************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     Wheat_proc_FCI_chart,
  #
  #                    paste0("Wheat procurement, ",Wheat_proc_FCI_title),
  #                     "Header",
  #
  #                    Wheat_proc_FCI_src,
  #                    "Title",
  #
  # paste0(""))
  ######################################################################################################

  # my_ppt=Single_Chart("Subtitle",
  #                     youth_employ_chart,
  #
  #                    paste0("Rising employability of youth, ",youth_employ_title),
  #                     "Header",
  #
  #                    youth_employ_src,
  #                    "Title",
  #
  # paste0(""))

  ######################################################################################################
  # my_ppt=Single_Chart("Sub title",country_fdi_chart,
  #
  #                     paste0("Countries net FDI inflows, ",country_fdi_title),
  #                     "title",
  #
  #                     country_fdi_src,
  # paste0(""),
  #
  # paste0("For Mexico, it is excluding SPE's"))
  #####################################################################################################

  # my_ppt=Single_Chart("Subtitle",
  #                     immunization_chart,
  #
  #                    paste0("Immunization for major diseases among infants,",immunization_title),
  #                     "Header",
  #
  #                    immunization_src,
  #                    "Title",
  #
  # paste0(""))

  #####################################################################################################

  # my_ppt=Single_Chart("Subtitle",
  #                     world_elec_chart,
  #
  #                    paste0("Percentage of population with access to electricity, ",world_elec_title),
  #                     "Header",
  #
  #                    world_elec_src,
  #                    "Title",
  #
  # paste0(""))


  #####################################################################################################
  #**********************Mutual Fund AAUM Growth*************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     MF_AAUM_top_city_gr_chart,
  #
  #                    paste0("MF AUM growth - top 30 cities vs beyond 30 cities  (in %), ",MF_AAUM_top_city_gr_title),
  #                     "Header",
  #
  #                    MF_AAUM_top_city_gr_src,
  #                    "Title",
  #
  # paste0(""))
  #####################################################################################################
  #**********************4W registrations Growth*************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     FW_reg_top_city_gr_chart,
  #
  #                    paste0("4W registrations - top 30 cities vs beyond 30 cities (in '000), ",FW_reg_top_city_gr_title),
  #                     "Header",
  #
  #                    FW_reg_top_city_gr_src,
  #                    "Title",
  #
  # paste0(""))
  #####################################################################################################
  #**********************Credit  Growth*************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     Credit_top_city_gr_chart,
  #
  #                    paste0("Credit growth - top 30 cities vs beyond 30 cities (in %), ",Credit_top_city_gr_title),
  #                     "Header",
  #
  #                    Credit_top_city_gr_src,
  #                    "Title",
  #
  # paste0(""))
  #####################################################################################################
  #**********************New company registrations Growth*************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     nwcom_top_city_gr_chart,
  #
  #                    paste0("New company registrations - top 30 cities vs beyond 30 cities (in '000), ",nwcom_top_city_gr_title),
  #                     "Header",
  #
  #                    nwcom_top_city_gr_src,
  #                    "Title",
  #
  # paste0(""))
  #####################################################################################################
  #**********************Airport footfall Growth*************************************
  # my_ppt=Single_Chart("Subtitle",
  #                     airport_top_city_gr_chart,
  #
  #                    paste0("Airport footfall - top 30 cities vs beyond 30 cities (in mn), ",airport_top_city_gr_title),
  #                     "Header",
  #
  #                    airport_top_city_gr_src,
  #                    "Title",
  #
  # paste0(""))

  #####################################################################################################
  #**********************Store Locator************************************
  # my_ppt=Single_Chart("Sub title",store_locator_chart,
  #
  #                     paste0("Store locators, ",store_locator_title),
  #                     "title",
  #
  #                     store_locator_src,
  # paste0(""),
  #
  # paste0(""))
  ################################################################################################

  #####################################################################################################
  #************ Yearly prmary School  /Yearly middle School / Yearly high School / Yearly prmary School : Page 45 ****************************************

  # my_ppt=Four_Chart("Sub title",
  #
  #                   primary_schl_chart,
  #                   middle_schl_chart,
  #                   high_schl_chart,
  #                   'Data not available',
  #
  #                   paste0("Primary school enrollment (millions), ",primary_schl_title),
  #                   "Primary school enrollment",
  #
  #                   paste0("Middle school enrollment (millions), ",middle_schl_title),
  #                   "Middle school enrollment",
  #
  #                   paste0("High school enrollment (millions), ",high_schl_title),
  #                   "High school enrollment",
  #
  #                   paste0("sub header, "),
  #                   "Header",
  #
  #                   primary_schl_src,
  #                   middle_schl_src,
  #                   high_schl_src,
  #                    "Source: Bloomberg, NIIF Research",
  #
  #                   c1_n="",c2_n="",
  #                   c3_n="",
  #                   c4_n="",
  #
  #                   "Title")

  #####################################################################################################
  #####################################################################################################
  #**********************page:60*************************************
  my_ppt=add_slide(my_ppt,layout ="2_Custom Layout",master ="NIIF MONTHLY REPORTS")

  ############################################################################################
  #**********************page:61*************************************
  my_ppt=contact(paste0(format(Sys.Date(),format="%b")," ", format(Sys.Date(),format="%d"),','," ", format( Sys.Date(),format="%Y")))
  dbDisconnect(clk)
  dbDisconnect(pg)
  ###############################################
  setwd(ppt_dir)
  print(my_ppt,target="C:\\Users\\Administrator\\AdQvestDir\\codes\\NIIF_TITLE_AND_DESCRIPTION_GENERATION\\NIIF_PPT.pptx")


  ## -------------------------------------
  end_time = Sys.time()
  exe_time_sec = end_time - start_time
  val2=paste0(paste0("'",end_time,"',"),
              paste0(exe_time_sec,","),
              paste0(0,","),
              paste0(rep("'NA'",4), collapse = ", "),
              paste0(","),
              paste0(rep("''", 3), collapse = ", "),
              paste0(", '",log_rel_date,"',"),
              paste0("'",Sys.time(),"'"))

  query  = paste0(q1,val1,val2,");")
  query <- gsub("'NA'", "NULL", query)

  #execute the query

  con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname,
                    port = portnum, user = username, password = password)
  dbExecute(con, query)
  dbDisconnect(con)
  generate_excel(excel_dir)
  print("DONE")

## -------------------------------------


