## ----setup, include=FALSE------------------------
rm(list = ls())
# knitr::opts_chunk$set(echo = FALSE)
# path="C:\\Users\\Santonu\\Desktop\\ADQvest\\Error files\\Modified(corr)\\R_PPT\\NIIF_R_PPT_new_widgets_verson_second_B_NOV.Rmd"
# knitr::purl(path)


## ----include=FALSE-------------------------------

r_file = 'X001_NIIF_R_PPT'
scheduler = '9_30_AM_WINDOWS_SERVER_NIIF_PPT'
run_by ='Adqvest_Bot'
Schedular_Start_Time=Sys.time()
start_time = Sys.time()
log_rel_date = Sys.Date()
end_time = Sys.time()



## ----include=FALSE-------------------------------
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
       openxlsx,readxl,
       install = TRUE)
default_start_date ="2013-04-01"#Properties_FILE


## ------------------------------------------------
# MYSQL
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

## ----include=FALSE-------------------------------
#-----DB Deatail----------------------------
username = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[1]
password = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[2]
hostname = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[3]
portnum  = as.numeric(as.character(Adqvest_Properties[2,2]))
DBName   = as.character(Adqvest_Properties[3,2])


## ------------------------------------------------
host_cl     = unlist(as.character(Adqvest_Clickhouse[1,2]))
port_cl     = as.numeric(as.character(Adqvest_Clickhouse[2,2]))
DB_cl       = as.character(Adqvest_Clickhouse[3,2])
user_cl     = unlist(strsplit(as.character(Adqvest_Clickhouse[4,2]),':|@'))[1]
password_cl = unlist(as.character(Adqvest_Clickhouse[5,2]))


## ----include=FALSE-------------------------------

host_pg     = unlist(as.character(Adqvest_Test_Thurro[1,2]))
port_pg     = as.numeric(as.character(Adqvest_Test_Thurro[2,2]))
DB_pg       = as.character(Adqvest_Test_Thurro[3,2])
user_pg     = unlist(strsplit(as.character(Adqvest_Test_Thurro[4,2]),':|@'))[1]
password_pg = unlist(as.character(Adqvest_Test_Thurro[5,2]))



## ------------------------------------------------
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


## ------------------------------------------------
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



## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
Single_Chart=function(title2,c1,c1_t,c1_t1="Chart header1",c1_s,title1='TITLE',com=""){
  
    my_ppt=add_slide(my_ppt,layout ="WITH NOTE BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
                     ph_with(value =title1,location=ph_location_type(type ="title")) %>%
                     ph_with(value =c1, location =ph_location_type(type ="tbl",id=1)) %>%
                     ph_with(value =title2, location =ph_location_type(type ="body",id=1)) %>%
                     ph_with(value =c1_t1, location =ph_location_type(type ="body",id=5))%>%
                     ph_with(value =c1_t, location =ph_location_type(type ="body",id=6)) %>%
                     ph_with(value =c1_s, location =ph_location_type(type ="body",id=2)) %>%
                     ph_with(value ="Note:", location =ph_location_type(type ="body",id=3)) %>%
                     ph_with(value =com, location =ph_location_type(type ="body",id=4)) %>%
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


## ------------------------------------------------
abdul_table_type1=function(title,s_t1,table,ch1,csh1,src,
                           l1,t1,l2,t2,l3,t3,l4,t4,
                           l5,t5,w2,n1){
  
  my_ppt=add_slide(my_ppt,layout ="WITH NOTE BIGGER CHART/TABLE",master ="NIIF MONTHLY REPORTS" ) %>%
      ph_with(value =title,location=ph_location_type(type ="title")) %>%
      ph_with(value =table,location =ph_location_template(type ="tbl",id=1, 
                                                          left =l1, top =t1)) %>%
    
      ph_with(value =s_t1,location =ph_location_type(type ="body",id=1)) %>%
      ph_with(value =ch1,location=ph_location_template(type="body",id=5,
                                                       left =l2,top =t2,width =w2)) %>%
    
      ph_with(value =csh1,location =ph_location_template(type ="body",id=6,
                                                         left =l3,top =t3,width =w2)) %>%
      ph_with(value =src,location =ph_location_template(type ="body",id=2, left = l4, top = t4, width = w2)) %>%
    
      ph_with(value =n1,location =ph_location_template(type ="body",id=4,
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



## ------------------------------------------------
roundexcel = function(x,digits=0){
  posneg = sign(x)
  z = abs(x)*10^digits
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^digits
  z*posneg
}


table_preprocessing <- function(data,frequency_normalizer = '',unit= 'NA',variable = 'NA',sector = 'NA',calculate_gr = FALSE,divisor = 0,rounder = 0){
  
  tryCatch({
  
  
  last_date <- today()
  start_date <- timeFirstDayInMonth(last_date %m-% months(13))
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

economic_indicator_table <- function(data, has_main_sector = FALSE,main_sector_bg = "GRAY 96",has_units = FALSE,rename_unit_col = 'Units',set_header_bg = 'white',padding_vals = list(),make_bold = c(),make_col_bold=c(),hlines = c(),vlines = c(),background_vals = list(),color_coding = FALSE,alpha = 0.5,median_change = list(),invert_map = c(),rounder_exeptions = c(), replace_null = '', caption = '',font_size = 9, var_col_width = 2,unit_col_width = 1,other_col_width = 0.61,line_space=0,row_height=0.05){
  
  
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
  
  
  if (has_units == TRUE){
    date_cols <- colnames(all_df)[c(3:length(colnames(all_df)))]
  }
  else{
    date_cols <- colnames(all_df)[c(2:length(colnames(all_df)))]
  }
  
  all_df[date_cols] <- sapply(all_df[date_cols],as.numeric)
  
  set_flextable_defaults(font.family = 'Calibri')
  
  ft <- flextable(all_df)
  
  
  ft <- theme_alafoli(ft)
  
  
  if (caption != ''){
    ft <- add_header_lines(ft,values = caption)
    ft <- border_remove(ft)
    ft <- theme_alafoli(ft)
    ft <- merge_at(ft, i = 1, part = 'header')
    ft <- align(ft, i = 1, align = "center", part = "header")
  }
  
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
    
    for (l in date_cols){
      ft <- ft %>% bg(bg = 'white',j = c(l) ,i = is.na(all_df[l]), part = "body")
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


read_query <- function(widget_id, p, frequency = '',quarter=FALSE){
  
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

  start_date <- (max_date %m-% years(as.numeric(period)))
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


## ------------------------------------------------
clk  <- DBI::dbConnect(RClickhouse::clickhouse(), dbname =DB_cl,host = host_cl, port = port_cl,
                       user = user_cl,password =password_cl)
pg  <- DBI::dbConnect(RPostgres::Postgres(), dbname =DB_pg,host = host_pg, port = port_pg,
                      user = user_pg,password =password_pg)



## ------------------------------------------------
### Rail freight
rail_freight = read_query(284813,14)
rail_freight=rail_freight[,c("Relevant_Date","Volume")]
names(rail_freight)[2]<-"Value"

## Rail freight Growth
rail_freight_gr = read_query(284801,14)
rail_freight_gr=rail_freight_gr[,c("Relevant_Date","growth")]
names(rail_freight_gr)[1]<-"Relevant_Date"
names(rail_freight_gr)[2]<-"Value"

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

##Air Cargo
air_cargo= read_query(295110,14)
air_cargo=air_cargo[,c("Relevant_Date","Volume")]
names(air_cargo)[2]<-"Value"
## Air Cargo growth
air_cargo_gr= read_query(295115,14)
air_cargo_gr=air_cargo_gr[,c("Relevant_Date","Growth")]
names(air_cargo_gr)[1]<-"Relevant_Date"
names(air_cargo_gr)[2]<-"Value"


## GST Collection
gst_coll= read_query(522953,14)
gst_coll=gst_coll[,c("Relevant_Date","Value")]
## GST Collection growth
gst_coll_gr= read_query(522952,14)
gst_coll_gr=gst_coll_gr[,c("Relevant_Date","growth")]
names(gst_coll_gr)[1]<-"Relevant_Date"
names(gst_coll_gr)[2]<-"Value"


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


## FastTag Transaction
# 310448
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


goi_gr = table_preprocessing(goi_gr,frequency_normalizer = 'Monthly', '% yoy','Central government expenditure','Fiscal', rounder = 1)
gst_coll_gr = table_preprocessing(gst_coll_gr,frequency_normalizer = 'Monthly', '% yoy','GST collection','Fiscal', rounder = 1)
rail_freight_gr = table_preprocessing(rail_freight_gr,frequency_normalizer = 'Monthly', '% yoy','Rail freight','Logistics', rounder = 1)
elec_gen_gr = table_preprocessing(elec_gen_gr,frequency_normalizer = 'Monthly', '% yoy','Electricity generation','Industry', rounder = 1)
air_cargo_gr = table_preprocessing(air_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Air cargo','Logistics', rounder = 1)
upi_trans_gr = table_preprocessing(upi_trans_gr,frequency_normalizer = 'Monthly', '% yoy','UPI transactions (volume)','Consumption', rounder = 1)
export_gr = table_preprocessing(export_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize exports','Trade', rounder = 1)
import_gr = table_preprocessing(import_gr,frequency_normalizer = 'Monthly', '% yoy','Merchandize imports','Trade', rounder = 1)
fast_tag_gr = table_preprocessing(fast_tag_gr,frequency_normalizer = 'Monthly', '% yoy','FASTag collection (volume)','Consumption', rounder = 1)
port_cargo_gr = table_preprocessing(port_cargo_gr,frequency_normalizer = 'Monthly', '% yoy','Port cargo','Logistics', rounder = 1)
e_way_bills_gr = table_preprocessing(e_way_bills_gr,frequency_normalizer = 'Monthly', '% yoy','E-way bills (volume)','Logistics', rounder = 1)
cpi_gr = table_preprocessing(cpi_gr,frequency_normalizer = 'Monthly', '% yoy','Consumer Price Index','Consumption', rounder = 1)
iip_gr = table_preprocessing(iip_gr,frequency_normalizer = 'Monthly', '% yoy','Index of industrial production','Industry', rounder = 1)
core_8_gr = table_preprocessing(core_8_gr,frequency_normalizer = 'Monthly', '% yoy','Index of eight core industries','Industry', rounder = 1)
deposits_gr = table_preprocessing(deposits_gr,frequency_normalizer = 'Monthly', '% yoy','Aggregate deposits','Banking', rounder = 1)
credits_gr = table_preprocessing(credits_gr,frequency_normalizer = 'Monthly', '% yoy','Outstanding credit','Banking', rounder = 1)

ser_exp_gr = table_preprocessing(ser_exp_gr,frequency_normalizer = 'Monthly', '% yoy','Services exports','Trade', rounder = 1)

ser_imp_gr = table_preprocessing(ser_imp_gr,frequency_normalizer = 'Monthly', '% yoy','Services imports','Trade', rounder = 1)




growth_df = rbind(goi_gr,gst_coll_gr,iip_gr,core_8_gr,elec_gen_gr,rail_freight_gr,
                  port_cargo_gr,air_cargo_gr,e_way_bills_gr,export_gr,import_gr,ser_exp_gr,
                  ser_imp_gr,deposits_gr,credits_gr,upi_trans_gr,fast_tag_gr,cpi_gr)



economic_indicator_gr_54 =economic_indicator_table(growth_df,has_main_sector = TRUE,
                                        has_units = TRUE,color_coding = TRUE, 
                                        invert_map = c("Consumer Price Index"), 
                                        median_change = 
                                         list("Consumer Price Index" = 6),
                                       
                                        rounder_exeptions = growth_df$Variable, 
                                        font_size = 7.7,row_height=0.03, 
                                        var_col_width = 2,unit_col_width = 0.8,
                                        other_col_width = 0.58)


economic_indicator_gr_54_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y"),
                                      " - ",
                                      format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"),
                                      sep = '')

economic_indicator_gr_54


goi = table_preprocessing(goi,frequency_normalizer = 'Monthly', 'INR trillion','Central government expenditure','Fiscal', divisor = 10^12, rounder = 1)
gst_coll = table_preprocessing(gst_coll,frequency_normalizer = 'Monthly', 'INR trillion','GST collection','Fiscal', divisor = 10^12, rounder = 1)
rail_freight = table_preprocessing(rail_freight,frequency_normalizer = 'Monthly', 'mn tonnes','Rail freight','Logistics', rounder = 1)
air_cargo = table_preprocessing(air_cargo,frequency_normalizer = 'Monthly', "'000 ton",'Air cargo','Logistics', divisor = 10^3, rounder = 1)#
port_cargo = table_preprocessing(port_cargo,frequency_normalizer = 'Monthly', 'mn tonnes','Port cargo','Logistics', rounder = 1)
e_way_bills = table_preprocessing(e_way_bills,frequency_normalizer = 'Monthly', 'million','E-way bills (volume)','Logistics', divisor = 10^6, rounder = 1)
upi_trans = table_preprocessing(upi_trans,frequency_normalizer = 'Monthly', 'billion','UPI transactions (volume)','Consumption', divisor = 10^9, rounder = 1)
elec_gen = table_preprocessing(elec_gen,frequency_normalizer = 'Monthly', 'billion kWh','Electricity generation','Industry', divisor = 10^3, rounder = 1)
export = table_preprocessing(export,frequency_normalizer = 'Monthly', 'USD billion','Merchandize exports','Trade', divisor = 10^9, rounder = 1)
import = table_preprocessing(import,frequency_normalizer = 'Monthly', 'USD billion','Merchandize imports','Trade', divisor = 10^9, rounder = 1)

ser_import = table_preprocessing(ser_imp,frequency_normalizer = 'Monthly', 'USD billion','Services imports','Trade', divisor = 10^9, rounder = 1)
ser_export = table_preprocessing(ser_exp,frequency_normalizer = 'Monthly', 'USD billion','Services exports','Trade', divisor = 10^9, rounder = 1)



fast_tag = table_preprocessing(fast_tag,frequency_normalizer = 'Monthly', 'million','FASTag collection (volume)','Consumption', divisor = 10^6, rounder = 1)
cpi = table_preprocessing(cpi,frequency_normalizer = 'Monthly', 'Index','Consumer Price Index','Consumption', rounder = 1)
iip = table_preprocessing(iip,frequency_normalizer = 'Monthly', 'Index','Index of industrial production','Industry', rounder = 1)
core_8 = table_preprocessing(core_8,frequency_normalizer = 'Monthly', 'Index','Index of eight core industries','Industry', rounder = 1)
deposits = table_preprocessing(deposits,frequency_normalizer = 'Monthly', 'INR trillion','Aggregate deposits','Banking', divisor = 10^12, rounder = 1)
credits = table_preprocessing(credits,frequency_normalizer = 'Monthly', 'INR trillion','Outstanding credit','Banking', divisor = 10^12, rounder = 1)





indicator_df = rbind(goi,gst_coll,iip,core_8,elec_gen,rail_freight,port_cargo,air_cargo,
                     e_way_bills,export,import,ser_export,ser_import,deposits,credits,
                     upi_trans,fast_tag,cpi)

economic_indicator_55 = economic_indicator_table(indicator_df,has_main_sector = TRUE,
                             has_units = TRUE,color_coding = FALSE,
                             rounder_exeptions = indicator_df$Variable, 
                             font_size =8, var_col_width = 2,
                             unit_col_width = 0.8,line_space=0,row_height=0.03,
                             other_col_width = 0.58) #
economic_indicator_55

economic_indicator_55_title=paste0(format(as.Date(timeFirstDayInMonth(today() %m-% months(14))),"%b '%y")," - ",
                                   format(as.Date(timeFirstDayInMonth(today() %m-% months(1))),"%b '%y"), sep = '')




## ------------------------------------------------
## IIP
iip_total_gr = read_query(1288704,12)
iip_total_gr = iip_total_gr[,c("Relevant_Date","growth")]
names(iip_total_gr)[1]<-"Relevant_Date"
names(iip_total_gr)[2]<-"Value"
iip_total_gr = table_preprocessing(iip_total_gr, sector = '', variable = 'IIP', rounder = 1)


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
                            var_col_width = 2,other_col_width = 0.6862)

industrial_Prod_56_title=paste0(format(min(read_query(1288704,12)[,c("Relevant_Date")]),"%b '%y")," - ",
                                 format(max(read_query(1288704,12)[,c("Relevant_Date")]),"%b '%y"))

industrial_Prod_56



## ------------------------------------------------
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


#Total Credit Outstanding
total_credit = read_query(525690,12)
total_credit=total_credit[,c("Relevant_Date","Value")]
names(total_credit)[1]<-"Relevant_Date"
names(total_credit)[2]<-"Value"
total_credit = table_preprocessing(total_credit, variable ="Total Credit Outstanding", 
                                   calculate_gr = TRUE, divisor = 10^12, rounder = 1)

# credit_df = rbind(agri_credit,industry_credit,construction_credit,infrastucture_credit,power_credit,telecom_credit,road_credit,airport_credit,port_credit,rail_credit,service_credit,service_nbfc_credit,service_shipping_trade,service_shipping_realest,retail_loans_credit,other_non_food_loans,non_food_credit,total_credit)

credit_df = rbind(agri_credit,industry_credit,construction_credit,infrastucture_credit,power_credit,telecom_credit,road_credit,service_credit,service_nbfc_credit,service_shipping_trade,service_shipping_realest,retail_loans_credit,other_non_food_loans,non_food_credit,total_credit)

# padding = list(pad_10 = c("Construction","Infrastructure","Services - NBFC","Services - Trade","Services- Commercial Real Estate"), pad_20 = c("Power","Telecom","Roads","Airports","Ports","Railways (other than Indian Railways)"))

padding = list(pad_10 = c("Construction","Infrastructure","Services - NBFC","Services - Trade","Services- Commercial Real Estate"), pad_20 = c("Power","Telecom","Roads"))

bg = list(col_D9D9D9 = c("Industry","Services"), col_F2F2F2 = c("Infrastructure"),col_FDECDA = c("Total Credit Outstanding"))
border = c("Services- Commercial Real Estate","Retail loans","Other non-food loans","Non-food Credit")
bold = c("Total Credit Outstanding")
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


## ------------------------------------------------
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



## ------------------------------------------------
# WPI
wpi_total_gr = read_query(725962,12)
wpi_total_gr=wpi_total_gr[,c("Relevant_Date","Inflation")]
names(wpi_total_gr)[1]<-"Relevant_Date"
names(wpi_total_gr)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = 'All commodities'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_total_gr = table_preprocessing(wpi_total_gr, unit = weight, variable = "WPI", rounder = 1)

# WPI Primary articles
wpi_primary = read_query(726030, 12)
wpi_primary=wpi_primary[,c("Relevant_Date","Inflation")]
names(wpi_primary)[1]<-"Relevant_Date"
names(wpi_primary)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = 'I    PRIMARY ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_primary = table_preprocessing(wpi_primary, unit = weight, variable = "Primary articles", rounder = 1)

# WPI Food articles
wpi_food = read_query(726000, 12)
wpi_food=wpi_food[,c("Relevant_Date","Inflation")]
names(wpi_food)[1]<-"Relevant_Date"
names(wpi_food)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(A).  FOOD ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_food = table_preprocessing(wpi_food, unit = weight, variable = "Food articles", rounder = 1)

# WPI Non-food articles
wpi_non_food = read_query(726025, 12)
wpi_non_food=wpi_non_food[,c("Relevant_Date","Inflation")]
names(wpi_non_food)[1]<-"Relevant_Date"
names(wpi_non_food)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(B).  NON-FOOD ARTICLES'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_non_food = table_preprocessing(wpi_non_food, unit = weight, variable = "Non-food articles", rounder = 1)

# WPI Minerals
wpi_minerals = read_query(726022, 12)
wpi_minerals=wpi_minerals[,c("Relevant_Date","Inflation")]
names(wpi_minerals)[1]<-"Relevant_Date"
names(wpi_minerals)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(C).  MINERALS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_minerals = table_preprocessing(wpi_minerals, unit = weight, variable = "Minerals", rounder = 1)

# WPI Crude oil, petroleum and natural gas
wpi_fossil_fuel = read_query(725988, 12)
wpi_fossil_fuel=wpi_fossil_fuel[,c("Relevant_Date","Inflation")]
names(wpi_fossil_fuel)[1]<-"Relevant_Date"
names(wpi_fossil_fuel)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(D). CRUDE PETROLEUM & NATURAL GAS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_fossil_fuel = table_preprocessing(wpi_fossil_fuel, unit = weight, variable = "Crude oil, petroleum and natural gas", rounder = 1)

# WPI Fuel and power
wpi_fuel_power = read_query(726006, 12)
wpi_fuel_power=wpi_fuel_power[,c("Relevant_Date","Inflation")]
names(wpi_fuel_power)[1]<-"Relevant_Date"
names(wpi_fuel_power)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = 'II FUEL & POWER'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_fuel_power = table_preprocessing(wpi_fuel_power, unit = weight, variable = "Fuel and power", rounder = 1)

# WPI Coal
wpi_coal = read_query(725959, 12)
wpi_coal=wpi_coal[,c("Relevant_Date","Inflation")]
names(wpi_coal)[1]<-"Relevant_Date"
names(wpi_coal)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(A). COAL'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_coal = table_preprocessing(wpi_coal, unit = weight, variable = "Coal", rounder = 1)

# WPI Mineral oils
wpi_mineral_oils = read_query(726017, 12)
wpi_mineral_oils=wpi_mineral_oils[,c("Relevant_Date","Inflation")]
names(wpi_mineral_oils)[1]<-"Relevant_Date"
names(wpi_mineral_oils)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(B). MINERAL OILS'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_mineral_oils = table_preprocessing(wpi_mineral_oils, unit = weight, variable = "Mineral oils", rounder = 1)

# WPI Electricity
wpi_electricity = read_query(725989, 12)
wpi_electricity=wpi_electricity[,c("Relevant_Date","Inflation")]
names(wpi_electricity)[1]<-"Relevant_Date"
names(wpi_electricity)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = '(C). ELECTRICITY'"
weight = dbGetQuery(clk,weight_query)$Weight[1]
wpi_electricity = table_preprocessing(wpi_electricity, unit = weight, variable = "Electricity", rounder = 1)

# WPI Manufactured products
wpi_mfg_pdt = read_query(726011, 12)
wpi_mfg_pdt=wpi_mfg_pdt[,c("Relevant_Date","Inflation")]
names(wpi_mfg_pdt)[1]<-"Relevant_Date"
names(wpi_mfg_pdt)[2]<-"Value"
weight_query = "select Weight from EAI_WPI_COMMODITY_INDEX_AND_WEIGHT_MONTHLY_DATA where Relevant_Date in (select max(Relevant_Date) from PIB_MOSPI_ALL_INDIA_GROUP_SUB_GROUP_WISE_CPI_MONTHLY_INDEX) and Commodity = 'III   MANUFACTURED PRODUCTS'"
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



## ------------------------------------------------
#Eco-Indicators | Current Account 
qtr_bop_ca=read_query(1640367,15,quarter=TRUE)
names(qtr_bop_ca)[1]<-"Relevant_Date"
names(qtr_bop_ca)[2]<-"Value"
qtr_bop_ca = table_preprocessing_annual(qtr_bop_ca,frequency_normalizer='quarter',period=2, 
                                    variable = "Current account", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account | Goods and Services 
qtr_bop_gs=read_query(1633403,15,quarter=TRUE)
names(qtr_bop_gs)[1]<-"Relevant_Date"
names(qtr_bop_gs)[2]<-"Value"
qtr_bop_gs = table_preprocessing_annual(qtr_bop_gs,frequency_normalizer='quarter',period=2, 
                                    variable = "Goods and services", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Current Account | Goods  
qtr_bop_g=read_query(1633411,15,quarter=TRUE)
names(qtr_bop_g)[1]<-"Relevant_Date"
names(qtr_bop_g)[2]<-"Value"
qtr_bop_g = table_preprocessing_annual(qtr_bop_g,frequency_normalizer='quarter',period=2, 
                                    variable = "Goods", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account | Services 
qtr_bop_s=read_query(1633412,15,quarter=TRUE)
names(qtr_bop_s)[1]<-"Relevant_Date"
names(qtr_bop_s)[2]<-"Value"
qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='quarter',period=2, 
                                    variable = "Services", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Current Account | Primary Income 
qtr_bop_pi=read_query(1633404 ,15,quarter=TRUE)
names(qtr_bop_pi)[1]<-"Relevant_Date"
names(qtr_bop_pi)[2]<-"Value"
qtr_bop_pi = table_preprocessing_annual(qtr_bop_pi,frequency_normalizer='quarter',period=2, 
                                    variable = "Primary income", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


#Eco-Indicators | Current Account | Secondary Income 
qtr_bop_si=read_query(1633405 ,15,quarter=TRUE)
names(qtr_bop_si)[1]<-"Relevant_Date"
names(qtr_bop_si)[2]<-"Value"
qtr_bop_si = table_preprocessing_annual(qtr_bop_si,frequency_normalizer='quarter',period=2, 
                                    variable = "Secondary income", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Capital Account 
qtr_bop_caa=read_query(1640368   ,15,quarter=TRUE)
names(qtr_bop_caa)[1]<-"Relevant_Date"
names(qtr_bop_caa)[2]<-"Value"
qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='quarter',period=2, 
                                    variable = "Capital account", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Financial Account 
qtr_bop_fa=read_query(1640369 ,15,quarter=TRUE)
names(qtr_bop_fa)[1]<-"Relevant_Date"
names(qtr_bop_fa)[2]<-"Value"
qtr_bop_fa = table_preprocessing_annual(qtr_bop_fa,frequency_normalizer='quarter',period=2, 
                                    variable = "Financial accounts", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Financial Account | Direct Investment 
qtr_bop_di=read_query(1633406 ,15,quarter=TRUE)
names(qtr_bop_di)[1]<-"Relevant_Date"
names(qtr_bop_di)[2]<-"Value"
qtr_bop_di = table_preprocessing_annual(qtr_bop_di,frequency_normalizer='quarter',period=2, 
                                    variable = "Direct investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Financial Account | Portfolio Investment
qtr_bop_poi=read_query(1633407 ,15,quarter=TRUE)
names(qtr_bop_poi)[1]<-"Relevant_Date"
names(qtr_bop_poi)[2]<-"Value"
qtr_bop_poi = table_preprocessing_annual(qtr_bop_poi,frequency_normalizer='quarter',period=2, 
                                    variable = "Portfolio investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Financial Account | Financial derivatives 
qtr_bop_fd=read_query(1633408 ,15,quarter=TRUE)
names(qtr_bop_fd)[1]<-"Relevant_Date"
names(qtr_bop_fd)[2]<-"Value"
qtr_bop_fd = table_preprocessing_annual(qtr_bop_fd,frequency_normalizer='quarter',period=2, 
                                    variable = "Financial derivatives (other than reserves)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Financial Account | Other investment
qtr_bop_oi=read_query(1633409 ,15,quarter=TRUE)
names(qtr_bop_oi)[1]<-"Relevant_Date"
names(qtr_bop_oi)[2]<-"Value"
qtr_bop_oi = table_preprocessing_annual(qtr_bop_oi,frequency_normalizer='quarter',period=2, 
                                    variable = "Other investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)



#Eco-Indicators | Financial Account | Reserve assets
qtr_bop_ra=read_query(1633410 ,15,quarter=TRUE)
names(qtr_bop_ra)[1]<-"Relevant_Date"
names(qtr_bop_ra)[2]<-"Value"
qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='quarter',period=2, 
                                    variable = "Reserve assets", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)

#Eco-Indicators | Net errors and omissions
qtr_bop_eo=read_query(1632782 ,15,quarter=TRUE)
names(qtr_bop_eo)[1]<-"Relevant_Date"
names(qtr_bop_eo)[2]<-"Value"
qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='quarter',period=2, 
                                    variable = "Net errors and omissions",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1)


bop=rbind(qtr_bop_ca,qtr_bop_gs,qtr_bop_g,qtr_bop_s,qtr_bop_pi,qtr_bop_si,qtr_bop_caa,
          qtr_bop_fa,qtr_bop_di,qtr_bop_poi,qtr_bop_fd,qtr_bop_oi,qtr_bop_ra,qtr_bop_eo)


bg = list(col_FDECDA = c("Current account","Capital account","Financial accounts",
                         "Net errors and omissions"))

padding = list(pad_10 = c("Goods and services","Primary income","Secondary income",
                          "Direct investments","Portfolio investments",
                          "Financial derivatives (other than reserves)",
                          "Other investments","Reserve assets"), 
               pad_20 = c("Goods","Services"))


border = c("Capital account","Financial accounts","Net errors and omissions")
bold = c("Current account","Capital account","Financial accounts","Net errors and omissions")
vborder =c(14)

quarterly_bop_74_chart = economic_indicator_table(bop,rounder_exeptions = bop$Variable,
                                                 padding_vals = padding,
                                                 background_vals = bg,
                                                 hlines = border, make_bold = bold,
                                                 font_size = 12,
                                                 var_col_width =2,other_col_width =0.9912)



quarterly_bop_74_chart
data=read_query(1632782,15,quarter=TRUE)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_bop_74_title=heading(data)



## ------------------------------------------------
#Eco-Indicators | Current Account 
qtr_bop_ca=read_query(1640367,162,quarter=TRUE)
names(qtr_bop_ca)[1]<-"Relevant_Date"
names(qtr_bop_ca)[2]<-"Value"
qtr_bop_ca = table_preprocessing_annual(qtr_bop_ca,frequency_normalizer='year',period=9, 
                                    variable = "Current account", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)


#Eco-Indicators | Current Account | Goods and Services 
qtr_bop_gs=read_query(1633403,162,quarter=TRUE)
names(qtr_bop_gs)[1]<-"Relevant_Date"
names(qtr_bop_gs)[2]<-"Value"
qtr_bop_gs = table_preprocessing_annual(qtr_bop_gs,frequency_normalizer='year',period=9, 
                                    variable = "Goods and services", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)



#Eco-Indicators | Current Account | Goods  
qtr_bop_g=read_query(1633411,162,quarter=TRUE)
names(qtr_bop_g)[1]<-"Relevant_Date"
names(qtr_bop_g)[2]<-"Value"
qtr_bop_g = table_preprocessing_annual(qtr_bop_g,frequency_normalizer='year',period=9, 
                                    variable = "Goods", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)


#Eco-Indicators | Current Account | Services 
qtr_bop_s=read_query(1633412,162,quarter=TRUE)
names(qtr_bop_s)[1]<-"Relevant_Date"
names(qtr_bop_s)[2]<-"Value"
qtr_bop_s = table_preprocessing_annual(qtr_bop_s,frequency_normalizer='year',period=9, 
                                    variable = "Services", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)



#Eco-Indicators | Current Account | Primary Income 
qtr_bop_pi=read_query(1633404 ,162,quarter=TRUE)
names(qtr_bop_pi)[1]<-"Relevant_Date"
names(qtr_bop_pi)[2]<-"Value"
qtr_bop_pi = table_preprocessing_annual(qtr_bop_pi,frequency_normalizer='year',period=9, 
                                    variable = "Primary income", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)


#Eco-Indicators | Current Account | Secondary Income 
qtr_bop_si=read_query(1633405,162,quarter=TRUE)
names(qtr_bop_si)[1]<-"Relevant_Date"
names(qtr_bop_si)[2]<-"Value"
qtr_bop_si = table_preprocessing_annual(qtr_bop_si,frequency_normalizer='year',period=9, 
                                    variable = "Secondary income", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)



#Eco-Indicators | Capital Account 
qtr_bop_caa=read_query(1640368,162,quarter=TRUE)
names(qtr_bop_caa)[1]<-"Relevant_Date"
names(qtr_bop_caa)[2]<-"Value"
qtr_bop_caa = table_preprocessing_annual(qtr_bop_caa,frequency_normalizer='year',period=9, 
                                    variable = "Capital account", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Financial Account 
qtr_bop_fa=read_query(1640369 ,162,quarter=TRUE)
names(qtr_bop_fa)[1]<-"Relevant_Date"
names(qtr_bop_fa)[2]<-"Value"
qtr_bop_fa = table_preprocessing_annual(qtr_bop_fa,frequency_normalizer='year',period=9, 
                                    variable = "Financial accounts", calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Financial Account | Direct Investment 
qtr_bop_di=read_query(1633406 ,162,quarter=TRUE)
names(qtr_bop_di)[1]<-"Relevant_Date"
names(qtr_bop_di)[2]<-"Value"
qtr_bop_di = table_preprocessing_annual(qtr_bop_di,frequency_normalizer='year',period=9, 
                                    variable = "Direct investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Financial Account | Portfolio Investment
qtr_bop_poi=read_query(1633407 ,162,quarter=TRUE)
names(qtr_bop_poi)[1]<-"Relevant_Date"
names(qtr_bop_poi)[2]<-"Value"
qtr_bop_poi = table_preprocessing_annual(qtr_bop_poi,frequency_normalizer='year',period=9, 
                                    variable = "Portfolio investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Financial Account | Financial derivatives 
qtr_bop_fd=read_query(1633408 ,162,quarter=TRUE)
names(qtr_bop_fd)[1]<-"Relevant_Date"
names(qtr_bop_fd)[2]<-"Value"
qtr_bop_fd = table_preprocessing_annual(qtr_bop_fd,frequency_normalizer='year',period=9, 
                                    variable = "Financial derivatives (other than reserves)",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Financial Account | Other investment
qtr_bop_oi=read_query(1633409 ,162,quarter=TRUE)
names(qtr_bop_oi)[1]<-"Relevant_Date"
names(qtr_bop_oi)[2]<-"Value"
qtr_bop_oi = table_preprocessing_annual(qtr_bop_oi,frequency_normalizer='year',period=9, 
                                    variable = "Other investments", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)



#Eco-Indicators | Financial Account | Reserve assets
qtr_bop_ra=read_query(1633410 ,162,quarter=TRUE)
names(qtr_bop_ra)[1]<-"Relevant_Date"
names(qtr_bop_ra)[2]<-"Value"
qtr_bop_ra = table_preprocessing_annual(qtr_bop_ra,frequency_normalizer='year',period=9, 
                                    variable = "Reserve assets", 
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)

#Eco-Indicators | Net errors and omissions
qtr_bop_eo=read_query(1632782 ,162,quarter=TRUE)
names(qtr_bop_eo)[1]<-"Relevant_Date"
names(qtr_bop_eo)[2]<-"Value"
qtr_bop_eo = table_preprocessing_annual(qtr_bop_eo,frequency_normalizer='year',period=9, 
                                    variable = "Net errors and omissions",
                                    calculate_gr = FALSE,
                                    divisor =10^9, rounder = 1,fy_format=TRUE)


bop=rbind(qtr_bop_ca,qtr_bop_gs,qtr_bop_g,qtr_bop_s,qtr_bop_pi,qtr_bop_si,qtr_bop_caa,
          qtr_bop_fa,qtr_bop_di,qtr_bop_poi,qtr_bop_fd,qtr_bop_oi,qtr_bop_ra,qtr_bop_eo)


bg = list(col_FDECDA = c("Current account","Capital account","Financial accounts",
                         "Net errors and omissions"))

padding = list(pad_10 = c("Goods and services","Primary income","Secondary income",
                          "Direct investments","Portfolio investments",
                          "Financial derivatives (other than reserves)",
                          "Other investments","Reserve assets"), 
               pad_20 = c("Goods","Services"))


border = c("Capital account","Financial accounts","Net errors and omissions")
bold = c("Current account","Capital account","Financial accounts","Net errors and omissions")
vborder =c(14)

quarterly_bop_annual_chart = economic_indicator_table(bop,rounder_exeptions = bop$Variable,
                                                 padding_vals = padding,
                                                 background_vals = bg,
                                                 hlines = border, make_bold = bold,
                                                 font_size = 12,
                                                 var_col_width =2,other_col_width =0.81)



quarterly_bop_annual_chart
data=read_query(1632782,162,quarter=TRUE)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_bop_annual_title=heading(data)



## ------------------------------------------------
#MF | AUTO
qtr_mfh_au=read_query(1633786,36,quarter=FALSE)
names(qtr_mfh_au)[1]<-"Relevant_Date"
names(qtr_mfh_au)[2]<-"Value"
qtr_mfh_auto = table_preprocessing_annual(qtr_mfh_au,frequency_normalizer='month',period=1, 
                                    variable = "Auto", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | AUTO ANCILLARIES
qtr_mfh_au_an=read_query(1633756,36,quarter=FALSE)
names(qtr_mfh_au_an)[1]<-"Relevant_Date"
names(qtr_mfh_au_an)[2]<-"Value"
qtr_mfh_au_an = table_preprocessing_annual(qtr_mfh_au_an,frequency_normalizer='month',period=1, 
                                    variable = "Auto ancillaries", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | BANKS
qtr_mfh_bnk=read_query(1633807,36,quarter=FALSE)
names(qtr_mfh_bnk)[1]<-"Relevant_Date"
names(qtr_mfh_bnk)[2]<-"Value"
qtr_mfh_bnk = table_preprocessing_annual(qtr_mfh_bnk,frequency_normalizer='month',period=1, 
                                    variable = "Banks", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | CEMENT
qtr_mfh_cmt=read_query(1633828,36,quarter=FALSE)
names(qtr_mfh_cmt)[1]<-"Relevant_Date"
names(qtr_mfh_cmt)[2]<-"Value"
qtr_mfh_cmt = table_preprocessing_annual(qtr_mfh_cmt,frequency_normalizer='month',period=1, 
                                    variable = "Cement", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



#MF | CHEMICALS
qtr_mfh_ch=read_query(1633753,36,quarter=FALSE)
names(qtr_mfh_ch)[1]<-"Relevant_Date"
names(qtr_mfh_ch)[2]<-"Value"
qtr_mfh_ch = table_preprocessing_annual(qtr_mfh_ch,frequency_normalizer='month',period=1, 
                                    variable = "Chemicals", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



#MF | CONSTRUCTION PROJECT
qtr_mfh_conp=read_query(1633783,36,quarter=FALSE)
names(qtr_mfh_conp)[1]<-"Relevant_Date"
names(qtr_mfh_conp)[2]<-"Value"
qtr_mfh_conp = table_preprocessing_annual(qtr_mfh_conp,frequency_normalizer='month',period=1, 
                                    variable = "Construction project", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | CONSUMER DURABLES
qtr_mfh_con_du=read_query(1633846,36,quarter=FALSE)
names(qtr_mfh_con_du)[1]<-"Relevant_Date"
names(qtr_mfh_con_du)[2]<-"Value"
qtr_mfh_con_du = table_preprocessing_annual(qtr_mfh_con_du,frequency_normalizer='month',period=1, 
                                    variable = "Consumer durables", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)


#MF | CONSUMER NON DURABLES
qtr_mfh_con_n_du=read_query(1633822,36,quarter=FALSE)
names(qtr_mfh_con_n_du)[1]<-"Relevant_Date"
names(qtr_mfh_con_n_du)[2]<-"Value"
qtr_mfh_con_n_du = table_preprocessing_annual(qtr_mfh_con_n_du,frequency_normalizer='month',period=1, 
                                    variable = "Consumer non durables", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | FERROUS METALS
qtr_mfh_fm=read_query(1633852,36,quarter=FALSE)
names(qtr_mfh_fm)[1]<-"Relevant_Date"
names(qtr_mfh_fm)[2]<-"Value"
qtr_mfh_fm = table_preprocessing_annual(qtr_mfh_fm,frequency_normalizer='month',period=1, 
                                    variable = "Ferrous metals", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | FINANCE
qtr_mfh_fi=read_query(1633765,36,quarter=FALSE)
names(qtr_mfh_fi)[1]<-"Relevant_Date"
names(qtr_mfh_fi)[2]<-"Value"
qtr_mfh_fi = table_preprocessing_annual(qtr_mfh_fi,frequency_normalizer='month',period=1, 
                                    variable = "Finance", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)


#MF | PETROLEUM PRODUCTS
qtr_mfh_pp=read_query(1633834,36,quarter=FALSE)
names(qtr_mfh_pp)[1]<-"Relevant_Date"
names(qtr_mfh_pp)[2]<-"Value"
qtr_mfh_pp = table_preprocessing_annual(qtr_mfh_pp,frequency_normalizer='month',period=1, 
                                    variable = "Petroleum products", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)


#MF | PHARMACEUTICALS
qtr_mfh_phar=read_query(1633792,36,quarter=FALSE)
names(qtr_mfh_phar)[1]<-"Relevant_Date"
names(qtr_mfh_phar)[2]<-"Value"
qtr_mfh_phar = table_preprocessing_annual(qtr_mfh_phar,frequency_normalizer='month',period=1, 
                                    variable = "Pharmaceuticals", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)


#MF | POWER
qtr_mfh_pwr=read_query(1633741,36,quarter=FALSE)
names(qtr_mfh_pwr)[1]<-"Relevant_Date"
names(qtr_mfh_pwr)[2]<-"Value"
qtr_mfh_pwr = table_preprocessing_annual(qtr_mfh_pwr,frequency_normalizer='month',period=1, 
                                    variable = "Power", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



#MF | RETAILING
qtr_mfh_rtl=read_query(1633810,36,quarter=FALSE)
names(qtr_mfh_rtl)[1]<-"Relevant_Date"
names(qtr_mfh_rtl)[2]<-"Value"
qtr_mfh_rtl = table_preprocessing_annual(qtr_mfh_rtl,frequency_normalizer='month',period=1, 
                                    variable = "Retailing", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



#MF | SOFTWARE
qtr_mfh_sof=read_query(1633813,36,quarter=FALSE)
names(qtr_mfh_sof)[1]<-"Relevant_Date"
names(qtr_mfh_sof)[2]<-"Value"
qtr_mfh_sof = table_preprocessing_annual(qtr_mfh_sof,frequency_normalizer='month',period=1, 
                                    variable = "Software", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | Equity Holdings | TELECOM - SERVICES
qtr_mfh_tele=read_query(1633798,36,quarter=FALSE)
names(qtr_mfh_tele)[1]<-"Relevant_Date"
names(qtr_mfh_tele)[2]<-"Value"
qtr_mfh_tele = table_preprocessing_annual(qtr_mfh_tele,frequency_normalizer='month',period=1, 
                                    variable = "Telecom services", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)

#MF | Equity Holdings | OTHERS
qtr_mfh_oth=read_query(1640371,36,quarter=FALSE)
names(qtr_mfh_oth)[1]<-"Relevant_Date"
names(qtr_mfh_oth)[2]<-"Value"
qtr_mfh_oth = table_preprocessing_annual(qtr_mfh_oth,frequency_normalizer='month',period=1, 
                                    variable = "Others", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



#MF | Equity
qtr_mfh_t=read_query(1633862,36,quarter=FALSE)
names(qtr_mfh_t)[1]<-"Relevant_Date"
names(qtr_mfh_t)[2]<-"Value"
qtr_mfh_t = table_preprocessing_annual(qtr_mfh_t,frequency_normalizer='month',period=1, 
                                    variable = "Total", 
                                    calculate_gr = FALSE,
                                    divisor =10^12, rounder = 1)



qtr_mf_hold2=rbind(qtr_mfh_t,qtr_mfh_auto,qtr_mfh_au_an,qtr_mfh_bnk,qtr_mfh_cmt,qtr_mfh_ch,
                  qtr_mfh_conp,
                  qtr_mfh_con_du,qtr_mfh_con_n_du,qtr_mfh_fm,qtr_mfh_fi,qtr_mfh_pp,qtr_mfh_phar,
                  qtr_mfh_pwr,qtr_mfh_rtl,qtr_mfh_sof,qtr_mfh_tele,qtr_mfh_oth)


# qtr_mf_hold2=new_row_insertion(qtr_mf_hold,new_row_name="Others",sub_row_name="Total")

border = c("Total","Auto")
bold = c("Total")
bg = list(col_FDECDA =c("Total"))

quarterly_mfh_75_chart = economic_indicator_table(qtr_mf_hold2,
                                                  rounder_exeptions =qtr_mf_hold2$Variable,
                                                  font_size = 12,background_vals = bg,
                                                  make_bold = bold,
                                                  var_col_width =2,other_col_width =0.6862)


quarterly_mfh_75_chart
data=read_query(1633862,12)
names(data)[1]<-"Relevant_Date"
names(data)[2]<-"Value"
quarterly_mfh_75_title=heading(data,title='month')



## ------------------------------------------------
#Capital Flows | FDI | COMPUTER SOFTWARE AND HARDWARE
fdi_computer = read_query(1633154,12)
names(fdi_computer)[1]<-"Relevant_Date"
names(fdi_computer)[2]<-"Value"
fdi_computer = table_preprocessing_annual(fdi_computer,frequency_normalizer='year', 
                                    variable ="Computer software and hardware",
                                    sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9,rounder = 1)
                                    

#Capital Flows | FDI | CONSTRUCTION INFRASTRUCTURE ACTIVITIES
fdi_const = read_query(1633178,12)
names(fdi_const)[1]<-"Relevant_Date"
names(fdi_const)[2]<-"Value"
fdi_const = table_preprocessing_annual(fdi_const,frequency_normalizer='year', 
                               variable = "Construction and infrastructure",
                               sector ='Sector', 
                               calculate_gr = FALSE,period=9,
                               divisor =10^9,rounder = 1)

#Capital Flows | FDI | SERVICES SECTOR
fdi_service = read_query(1633144,12)
names(fdi_service)[1]<-"Relevant_Date"
names(fdi_service)[2]<-"Value"
fdi_service = table_preprocessing_annual(fdi_service,frequency_normalizer='year', 
                                    variable = "Services",sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)
                                   
#Capital Flows | FDI | AUTOMOBILE INDUSTRY
fdi_automobile = read_query(1633113,12)
names(fdi_automobile)[1]<-"Relevant_Date"
names(fdi_automobile)[2]<-"Value"
fdi_automobile = table_preprocessing_annual(fdi_automobile,frequency_normalizer='year', 
                                    variable = "Automobile",sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)
                                 

#Capital Flows | FDI | TRADING
fdi_trading = read_query(1633174,12)
names(fdi_trading)[1]<-"Relevant_Date"
names(fdi_trading)[2]<-"Value"
fdi_trading = table_preprocessing_annual(fdi_trading,frequency_normalizer='year', 
                                    variable = "Trading",sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | DRUGS AND PHARMACEUTICALS
fdi_drugs = read_query(1633165,12)
names(fdi_drugs)[1]<-"Relevant_Date"
names(fdi_drugs)[2]<-"Value"
fdi_drugs = table_preprocessing_annual(fdi_drugs,frequency_normalizer='year', 
                                    variable = "Drugs and pharmaceuticals",
                                    sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CHEMICALS OTHER THAN FERTILIZER
fdi_chem = read_query(1633169,12)
names(fdi_chem)[1]<-"Relevant_Date"
names(fdi_chem)[2]<-"Value"
fdi_chem = table_preprocessing_annual(fdi_chem,frequency_normalizer='year', 
                                    variable = "Chemicals,excl. Fertilizers",
                                    sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | TELECOMMUNICATIONS
fdi_tele = read_query(1633160,12)
names(fdi_tele)[1]<-"Relevant_Date"
names(fdi_tele)[2]<-"Value"
fdi_tele = table_preprocessing_annual(fdi_tele,frequency_normalizer='year', 
                                    variable = "Telecommunications",
                                    sector ='Sector', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)



fdi_Sector = rbind(fdi_computer,fdi_const,fdi_service,fdi_trading,fdi_automobile,fdi_drugs,
                   fdi_chem,
                   fdi_tele)

fdi_Sector <- fdi_Sector[order(as.numeric(fdi_Sector$'2023'), decreasing = TRUE),]


#Capital Flows | FDI
fdi_gross = read_query(1681734,12)
names(fdi_gross)[1]<-"Relevant_Date"
names(fdi_gross)[2]<-"Value"
fdi_gross = table_preprocessing_annual(fdi_gross,frequency_normalizer='year', 
                                variable = "Gross FDI inflows",sector ='Sector', 
                                calculate_gr = FALSE,period=9,
                                divisor =10^9, rounder = 1)


#Capital Flows | FDI
fdi_total_1 = read_query(1681734,9)
names(fdi_total_1)[1]<-"Relevant_Date"

fdi_Sector[, 3:ncol(fdi_Sector)] <- sapply(fdi_Sector[, 3:3:ncol(fdi_Sector)], as.numeric)
fdi_mjr_sectors=as.data.frame(colSums(fdi_Sector[, c(3,4,5,6,7,8,9,10,11,12)], na.rm=TRUE))
fdi_mjr_sectors['Relevant_Date']=as.Date(fdi_total_1$Relevant_Date)
names(fdi_mjr_sectors)[1]<-"Value"
fdi_mjr_sectors=fdi_mjr_sectors[,c('Relevant_Date',"Value")]

fdi_mjr_sectors = table_preprocessing_annual(fdi_mjr_sectors,frequency_normalizer='year', 
                                  variable = "FDI inflows across major sectors",
                                  sector ='Sector', 
                                  calculate_gr = FALSE,period=9,
                                  rounder = 1)


fdi_Sector=rbind(fdi_Sector,fdi_mjr_sectors,fdi_gross)




bg = list(col_FDECDA = c("Gross FDI inflows"),
          col_D9D9D9 = c("FDI inflows across major sectors"))
bold = c("FDI inflows across major sectors","Gross FDI inflows")
borders = c("FDI inflows across major sectors","Gross FDI inflows")

Annual_FDI_inflows_by_Sector_76_chart  =economic_indicator_table(fdi_Sector,has_main_sector = TRUE,
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


## ------------------------------------------------

#Capital Flows | FDI | SINGAPORE
fdi_singapore = read_query(1633159,12)
names(fdi_singapore)[1]<-"Relevant_Date"
names(fdi_singapore)[2]<-"Value"
fdi_singapore = table_preprocessing_annual(fdi_singapore,frequency_normalizer='year', 
                                    variable = "Singapore",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9,rounder = 1)
                                    

#Capital Flows | FDI | U.S.A.
fdi_usa = read_query(1633128,12)
names(fdi_usa)[1]<-"Relevant_Date"
names(fdi_usa)[2]<-"Value"
fdi_usa = table_preprocessing_annual(fdi_usa,frequency_normalizer='year', 
                               variable = "USA",sector ='Country', 
                               calculate_gr = FALSE,period=9,
                               divisor =10^9,rounder = 1)

#Capital Flows | FDI | MAURITIUS
fdi_mauritius = read_query(1633175,12)
names(fdi_mauritius)[1]<-"Relevant_Date"
names(fdi_mauritius)[2]<-"Value"
fdi_mauritius = table_preprocessing_annual(fdi_mauritius,frequency_normalizer='year', 
                                    variable = "Mauritius",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)
                                   
#Capital Flows | FDI | UAE
fdi_uae = read_query(1633152,12)
names(fdi_uae)[1]<-"Relevant_Date"
names(fdi_uae)[2]<-"Value"
fdi_uae = table_preprocessing_annual(fdi_uae,frequency_normalizer='year', 
                                    variable = "UAE",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)
                                 

#Capital Flows | FDI | NETHERLAND
fdi_netherlands = read_query(1633145,12)
names(fdi_netherlands)[1]<-"Relevant_Date"
names(fdi_netherlands)[2]<-"Value"
fdi_netherlands = table_preprocessing_annual(fdi_netherlands,frequency_normalizer='year', 
                                    variable = "Netherlands",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CAYMAN ISLANDS
fdi_Cayman = read_query(1633179,12)
names(fdi_Cayman)[1]<-"Relevant_Date"
names(fdi_Cayman)[2]<-"Value"
fdi_Cayman = table_preprocessing_annual(fdi_Cayman,frequency_normalizer='year', 
                                    variable = "Cayman islands",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | UNITED KINGDOM
fdi_uk = read_query(1633171,12)
names(fdi_uk)[1]<-"Relevant_Date"
names(fdi_uk)[2]<-"Value"
fdi_uk = table_preprocessing_annual(fdi_uk,frequency_normalizer='year', 
                                    variable = "UK",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | JAPAN
fdi_japan = read_query(1633112,12)
names(fdi_japan)[1]<-"Relevant_Date"
names(fdi_japan)[2]<-"Value"
fdi_japan = table_preprocessing_annual(fdi_japan,frequency_normalizer='year', 
                                    variable = "Japan",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | GERMANY
fdi_germany = read_query(1633168,12)
names(fdi_germany)[1]<-"Relevant_Date"
names(fdi_germany)[2]<-"Value"
fdi_germany = table_preprocessing_annual(fdi_germany,frequency_normalizer='year', 
                                    variable = "Germany",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

#Capital Flows | FDI | CYPRUS
fdi_cyprus = read_query(1633164,12)
names(fdi_cyprus)[1]<-"Relevant_Date"
names(fdi_cyprus)[2]<-"Value"
fdi_cyprus = table_preprocessing_annual(fdi_cyprus,frequency_normalizer='year', 
                                    variable = "Cyprus",sector ='Country', 
                                    calculate_gr = FALSE,period=9,
                                    divisor =10^9, rounder = 1)

fdi_country = rbind(fdi_singapore,fdi_cyprus,fdi_usa,fdi_mauritius,fdi_uae,fdi_netherlands,fdi_Cayman,
                    fdi_uk,fdi_japan,fdi_germany)

fdi_country <- fdi_country[order(as.numeric(fdi_country$'2023'), decreasing = TRUE),]
                   

#Capital Flows | FDI
fdi_total = read_query(1681734,12)
names(fdi_total)[1]<-"Relevant_Date"
names(fdi_total)[2]<-"Value"
fdi_total = table_preprocessing_annual(fdi_total,frequency_normalizer='year', 
                                variable = "Total FDI Inflows",sector ='Country', 
                                calculate_gr = FALSE,period=9,
                                divisor =10^9, rounder = 1)


#Capital Flows | FDI
fdi_total_1 = read_query(1681734,9)
names(fdi_total_1)[1]<-"Relevant_Date"


fdi_country[, 3:ncol(fdi_country)] <- sapply(fdi_country[, 3:ncol(fdi_country)], as.numeric)
fdi_mjr_inv=as.data.frame(colSums(fdi_country[, c(3,4,5,6,7,8,9,10,11,12)], na.rm=TRUE))
fdi_mjr_inv['Relevant_Date']=as.Date(fdi_total_1$Relevant_Date)
names(fdi_mjr_inv)[1]<-"Value"
fdi_mjr_inv=fdi_mjr_inv[,c('Relevant_Date',"Value")]

fdi_mjr_inv = table_preprocessing_annual(fdi_mjr_inv,frequency_normalizer='year', 
                                  variable = "FDI inflows from major investors",
                                  sector ='Country',period=9, 
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



## ------------------------------------------------
req_date <- as.Date(timeLastDayInMonth(today() %m-% months(1)))

## BSE Sensex
#724030
bse_sensex = read_query(318893,2)
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
#725341
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



## ----eval=FALSE, include=FALSE-------------------
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


## ------------------------------------------------
##Embassy_REIT
embassy = read_query(724555,1, frequency = 'daily')
names(embassy)[2]<-"Value"
embassy = growth_and_market_cap_calc(embassy,"Embassy REIT")

## Brookfield REIT
brookfield = read_query(724559,1, frequency = 'daily')
names(brookfield)[2]<-"Value"
brookfield = growth_and_market_cap_calc(brookfield,"Brookfield REIT")

## Mindspace REIT
mindspace = read_query(724557,1, frequency = 'daily')
names(mindspace)[2]<-"Value"
mindspace = growth_and_market_cap_calc(mindspace,"Mindspace REIT")

## IRB InvIT
irb_invit = read_query(724552,1, frequency = 'daily')
names(irb_invit)[2]<-"Value"
irb_invit = growth_and_market_cap_calc(irb_invit,"IRB InvIT")

## India Grid
india_grid = read_query(724553,1, frequency = 'daily')
names(india_grid)[2]<-"Value"
india_grid = growth_and_market_cap_calc(india_grid,"India Grid")


#NHIT InvIT
nhit_invit = read_query(1684508,1, frequency = 'daily')
names(nhit_invit)[2]<-"Value"
nhit_invit = growth_and_market_cap_calc(nhit_invit,"NHIT InvIT")

#SHREM InvIT
shrem_invit = read_query(1684506,1, frequency = 'daily')
names(shrem_invit)[2]<-"Value"
shrem_invit = growth_and_market_cap_calc(shrem_invit,"SHREM InvIT")

## PGCIL InvIT
pgcil_invit = read_query(724561,1, frequency = 'daily')
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


## ----eval=FALSE, include=FALSE-------------------
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


## ----include=FALSE-------------------------------
font_paths()
t=font_files() %>%tibble()
font_add(family='Calibri_Regular',regular ='calibril.ttf')


## ------------------------------------------------
common_theme=function(){
  
  theme_bw()+
  theme (legend.position = "top",
         legend.direction="horizontal",
         legend.justification=Position,
         legend.title = element_blank(),
         legend.title.align = 0,
         legend.key.size = unit(legend_key_size, "cm"),    #Length of key
         legend.key.width= unit(legend_key_width, 'cm'),
         legend.key.height = NULL,                         # key height (unit)
         legend.spacing.x = unit(0.05, 'cm'),
         legend.spacing.y = unit(0.05, 'cm'),
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

  theme(axis.text.x=element_text(angle =0,
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
  df3=merge(df_t,data,by="Relevant_Date")
  return(df3)
  
}
mon_year_df_creator=function(df,keep_col=c("Relevant_Date"),Sum_date=FALSE){

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
                current_f_year=paste0(as.character(current_f_year),'-03-31')
                 print(current_f_year)
      
       
    
  }
  else{
      
      current_f_year=format(max_date,format="%Y" )
      current_f_year=paste0(as.character(current_f_year),'-03-31')
      print(current_f_year)}
}
  
    
  
  df["year_mon"]=apply(df,1,f1)
  df_2 <- df %>% mutate_at(c(which(names(df)=="year_mon")), as.Date)
  df_2 <- df_2 %>% mutate_at(c(which(names(df_2)=="Relevant_Date")), as.Date)
  
  if (Sum_date==TRUE){
    df_2 <- df_2 %>%
              group_by(year_mon) %>%
              summarize(Sum_Value = sum(Value))
     names(df_2)[c(which(names(df_2)==paste0('Sum_Value')))]=col
    
     
  }else{
    df1 <- df1 %>% mutate_at(c(which(names(df1)=="Relevant_Date")), as.Date)
    names(df1)[1]='year_mon'
    df_2=merge(df_2,df1,by="year_mon")
    df_2=df_2[!duplicated(df_2[c("year_mon")]), ]
    names(df_2)[c(which(names(df_2)==paste0(col,'.x')))]=col
   
   
  }
  names(df_2)[1]='Relevant_Date'
  df_2=df_2[,c("Relevant_Date",col)]
  df_2=df_2[,keep_col]
  # df_2=rbind(df_2,df3)
  df_2[df_2$Relevant_Date==max(df_2$Relevant_Date),"Relevant_Date"]<-max_date
  df_2=df_2[df_2$Relevant_Date<=prev_month,]
  
  return (df_2)
}



## ------------------------------------------------




## ------------------------------------------------
line_bar_chart_niif <- function(data1,data2,sales_heading,growth_heading,
                                x_axis_interval="24 month",round_integer=FALSE,
                                special_case=FALSE,
                                graph_lim=30,data_unit='',
                                WHITE_BACK=FALSE,
                                Position="center",
                                key_spacing=0.01,DATE_HEADER=FALSE
){
  
  tryCatch({
    my_chart_col=c("GOLDEN ROD 1","GRAY 48")
    my_legends_col=c("GOLDEN ROD 1","GRAY 48")
    legend_key_width=0.27
   
  
   #############################Data_formatting##############################
    showtext_auto()
    data1$Relevant_Date=as.Date(data1$Relevant_Date)
    data2$Relevant_Date=as.Date(data2$Relevant_Date)
    
    data1 = data1[order(data1$Relevant_Date),]

    data1$value_y_left=data1[1:nrow(data1),2]  
    data2$value_y_right=data2[1:nrow(data2),2]  
    
    data1$Month <- as.Date(data1$Relevant_Date, format = "%Y-%m-%d")
    data1$Month =as.Date(data1$Month)
  
    #special_case:For Dual axis line Chart
    #special_case_2:When no of data in left is lesser than right
    if( nrow(data1)!=nrow(data2)){
          data_final=merge(data1, data2, by="Relevant_Date",all=T)
          data_final=do.call(data.frame,
                             lapply(data_final,function(x) replace(x, is.infinite(x), NA))) 
                             
                             
 
    }else{
         data_final=cbind(data1[,c("Relevant_Date","value_y_left","Month")],value_y_right=data2$value_y_right)
         data_final=do.call(data.frame,
                            lapply(data_final,function(x) replace(x, is.infinite(x), NA))) 
         data_final=na.omit(data_final)
         }
    
    print(paste("Current working rows:",nrow(data_final)))

    if(DATE_HEADER==TRUE){
                        prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))
                        
    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}
          
    
    print(prev_month)
    
    #This is specially for dual axis line Chart
    data_final=data_final[order(data_final$Relevant_Date),]
    data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
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
    if (data_ends$value_y_right<0){
               line_lab=paste0("(",format(roundexcel(abs(data_ends$value_y_right),1),nsmall=1,big.mark=","),")")
                                    
         
    }else{line_lab=format(roundexcel(abs(data_ends$value_y_right),1),nsmall=1,big.mark=",")}
    
    if (data_ends$value_y_left<0){
               line_lab_l=paste0("(",format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=","),")")
         
    }else{line_lab_l=format(roundexcel(abs(data_ends$value_y_left),1),nsmall=1,big.mark=",")}

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
            
            scale_col= scale_colour_manual(name = NULL,values=my_legends_col)
      }
      else{
            LEFT_CHART=geom_col(aes(x=Month,y=value_y_left,
                                fill=paste0(sales_heading)),
                                width =bar_thick,group=1)
            
            LEFT_CHART=geom_bar(aes(x=Month,y=(value_y_left),
                                fill=paste0(sales_heading)),
                                stat="identity",
                                position='identity',
                                width = bar_thick) 


            scale_col=scale_color_manual(name = NULL, values = c("GRAY 48"))          #
      }
    
     if(WHITE_BACK==TRUE){
           theme1=cutom_theme_pmi
      
     }else{theme1=cutom_theme
     }
    normalizer <-max_sec_y/y_max
    

   
   ###################################HEADER_SUB_HEADER##################
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)
    
    current_month=format(data_ends$Month,format="%b")
    current_day=format(data_ends$Month,format="%d")
    current_month_num=format(data_ends$Month,"%m")
    first_mon_mum=format(first_date,"%m")
    
  
    if (as.numeric(current_month_num)>=4){
                current_f_year=format(current_date+years(1),format="%Y")
      
       
    }else{current_f_year=format(current_date,format="%Y" )}
    
    if (as.numeric(first_mon_mum)>=4){
                first_f_year=format(first_date+years(1),format="%Y" ) 
      
      
    }else{first_f_year=format(first_date,format="%Y" ) 
    }
    
    if (DATE_HEADER==TRUE){
          sub_h=paste0(data_unit , "FY",first_f_year,"-","FY",
                       current_f_year," (",current_day," ",
                       current_month," '",
                       format(data_ends$Month,format="%y"),")")
      
    }else{
         sub_h=paste0(data_unit , "FY",first_f_year,"-","FY",
                      current_f_year," (",current_month," '",
                      format(data_ends$Month,format="%y"),")")
    
    }
   
   #############################Graph#######################################
    
  line=ggplot(data=data_final)+
      LEFT_CHART+
      
      
      geom_text_repel(aes(x=Month,y=value_y_left,
                      label=line_lab_l),data=data_ends,
                      direction="y",
                      max.overlaps=max_overlap,
                      font="bold",
                      min.segment.length = Inf,           
                      nudge_x =nug_x,
                      na.rm=TRUE,hjust=h_just,vjust=v_just,
                      size =chart_label,family="Calibri")+
      
      geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                      color=paste0(growth_heading)),
                      stat="identity",
                      size=line_thick,
                      linetype =1,group=1)+
      
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
      scale_y_continuous(expand = expansion(mult=c(0,0.04)),breaks=pretty_breaks(n=num_brek),
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
                   labels =date_format("%b-%y"),
                   breaks =function(x) seq.Date(from = min(x),to = max(x),by=x_axis_interval),
                   expand =c(0,0))+
      guides(color =guide_legend(order =2),
             fill =guide_legend(order =1))+
      common_theme()+
      theme1()
        
       
   ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))
  }, 
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",growth_heading)
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
    con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, 
                      port = portnum, user = username, password = password)
    
    dbExecute(con, query)
    dbDisconnect(con)
    
    }
  )
    
    
    
}

#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.



## ------------------------------------------------
stacked_bar_line_chart_niif=function(data1,data2,growth_heading,
                                     x_axis_interval="24 month",
                                     data_unit='',graph_lim=30,round_integer=FALSE,
                                     Exception=FALSE,
                                     SIDE_BAR=FALSE,negative=FALSE,GST_SPECIAL=FALSE,
                                     DUAL_AXIS=TRUE,
                                     key_spacing=0.10,JNPT=FALSE,
                                     legends_break=FALSE,legends_reverse=FALSE,YTD=FALSE){ 
  
  tryCatch({
    showtext_auto()
    Position="center"
  #NEW_MODIFIED
    ##########################DATA_PROCESSING###########################
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    
    data1$Relevant_Date=as.Date(timeLastDayInMonth(data1$Relevant_Date))
    data2$Relevant_Date=as.Date(timeLastDayInMonth(data2$Relevant_Date))
    
    data_final=merge(data1, data2, by="Relevant_Date")
    data_final= reshape2::melt(data_final,id=c("Relevant_Date","growth"))

    data_final$Month <-as.Date(data_final$Relevant_Date)
    data_final=data_final %>% 
                rename(value_y_right=growth,
                       category=variable,
                       value_y_left=value)
    
    data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))
                                    
                          
    data_final=na.omit(data_final)
    data_final=data_final[order(data_final$Relevant_Date),]
    data_final=data_final[data_final$Relevant_Date>=default_start_date,]
    prev_month=as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
    print(prev_month)
    data_final=data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)
    
    print(paste("Current working rows:",nrow(data_final)))

    x_max=max(data_final$Month)
    x_min=max(data_final$Month)
    
    
    max_factor=max_pri_y/max(data_final$value_y_left)
    min_factor=min_pri_y/abs(min(data_final$value_y_left))
    
    if (Exception==TRUE) {
      y_max = max_factor * max(data_final$value_y_left)
      y_min = min_factor * min(data_final$value_y_left)
      
    } else if (round_integer == TRUE) {
      y_max = ceiling(max_factor * max(data_final$value_y_left))
      y_min = ceiling(min_factor * min(data_final$value_y_left))
      
    } else{
      y_max = round(max_factor * max(data_final$value_y_left), digits = -1)
      y_min = round(min_factor * min(data_final$value_y_left), digits = -1)
    }
    
    
    
    print(paste("Min factor:",min_factor))
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    
    normalizer <-max_sec_y/y_max
    print( normalizer)
    max_date_actual=as.Date(max(data_final$Month))
    if (YTD==TRUE)
    {
      data_final['Month']=apply(data_final,1,f1)
      data_final$Month <-as.Date(data_final$Month)
      print(data_final)
      print(names(data_final))
    }    
    data_ends <- data_final %>% filter(Month == Month[length(Month)])
    print(data_ends)
    
    # print(data_ends)
    if (legends_break==TRUE){
      guides_type=guides(fill =guide_legend(nrow=n_row,byrow=TRUE,reverse=TRUE,order =1))
      
    }else if(legends_reverse==TRUE){
      guides_type=guides(color =guide_legend(order =2),
                          fill =guide_legend(order =1,reverse=TRUE))
      
      
      
    }else{
      guides_type=guides(color =guide_legend(order =2),
                          fill =guide_legend(order =1,reverse=TRUE))
      
    }
    
    if (SIDE_BAR == TRUE) {
      bar_position = "dodge2"
      bar_position = position_dodge(width = bar_thick)
      
      
    } else{bar_position = "stack"
    }
    
    if  (negative==TRUE){
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
        
     
    }else {
      line_1=geom_line(aes(x=Month,y=(value_y_right)/normalizer,
                      color=paste0(growth_heading)),
                      size=line_thick,linetype =1,group=1)
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
    ###################################HEADER_SUB_HEADER##################
    if (YTD==TRUE){
      data_ends[data_ends$Month==max(data_ends$Month),"Month"]<-max_date_actual
    }
  
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

    }else{first_f_year=format(first_date,format="%Y")
    }
  
    sub_h=paste0(data_unit, "FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                format(data_ends$Month,format="%y"),")")
  
    ##################################################GRAPH#####################
    stacked_bar=ggplot(data=data_final)+
      geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
                        position=bar_position,
                        width = bar_thick) +
      text_label_right+
      
      line_1+

      geom_text_repel(aes(x=Month,y=(value_y_right)/normalizer,
                     label=line_lab),
                     data=data_ends[1,],
                     direction="y",           #All changes will be effect on verticle axis.
                     min.segment.length = Inf,#Reomes line
                     nudge_x =nug_x,
                     font="bold",
                     na.rm=TRUE,hjust=h_just_line,vjust=v_just_line,
                     size =chart_label,family="Calibri")+
      
      scale_fill_manual(values=my_chart_col)+
      
      scale_colour_manual(values=c("GRAY 48"),
                          guide = guide_legend(override.aes = list(linetype = c("solid"))))+
      
      scale_linetype_manual(values=c("solid"))+
      RIGHT_CHART_scale+
      coord_cartesian(ylim =c(y_min,y_max))+
      
      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =function(x) seq.Date(from = min(x),to = max(x),by=x_axis_interval),
                   expand =c(0,0))+
      
      #Fill--->for bar chart  
      #Order=1 means it will place that type at top
      #color--->line Chart
      guides_type+
      # guides(fill =guide_legend(nrow=n_row,byrow=TRUE,order =2))+
                                                
      # guides(color =guide_legend(order =2),
      #         fill =guide_legend(order =1))+

      # guides(fill=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE,order=1))+
      # guides(color =guide_legend(ncol=n_col1,nrow=n_col1,byrow=TRUE,order=1))+
      
      common_theme()+
      cutom_theme()
    ########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))
    
  }, 
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",growth_heading)
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
  )
       
}

# NOTE:-if u see bar are surround with another colour that means u have done something wrong while arranging colors.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.
#Scale fill manual represents what will be the color of your graph.


## ------------------------------------------------
stacked_bar_chart_niif <-  function(data1,x_axis_interval="24 month",data_unit="",
                                    graph_lim=30,negative=FALSE,
                                    SIDE_BAR=FALSE,Position="center",DATE_HEADER=FALSE,
                                    surplus=FALSE,round_integer=FALSE,YTD=FALSE){ 
  tryCatch({
    
     ##########################DATA_PROCESSING###########################
    showtext_auto()
    Position="center"
    
    data1=Reduce(function(x, y) merge(x, y, all=TRUE),data1)
    data1=melt(data1,id=c("Relevant_Date"))
    data1=na.omit(data1)

    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data1$Month <- as.Date(data1$Relevant_Date, format = "%Y-%m-%d")
    max_overlap=10
    
    data1$value_y_left=data1[1:nrow(data1),3] 
    data1$category=data1[1:nrow(data1),2] 

    data_final=data1[,c("Relevant_Date","value_y_left","Month","category")]
    data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))
                         

    data_final=data_final[order(data_final$Relevant_Date),]
    data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
    
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
    }   
    
    data_ends <- data_final %>% filter(Month == Month[length(Month)])
   
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
   
     ###################################HEADER_SUB_HEADER##################
    if (YTD==TRUE){
      data_ends[data_ends$Month==max(data_ends$Month),"Month"]<-max_date_actual
    }
  
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)

    current_month=format(data_ends$Month,format="%b")
    current_day=format(data_ends$Month,format="%d")
    current_month_num=format(data_ends$Month,"%m")[1]
    first_mon_mum=format(first_date,"%m")


    if (as.numeric(current_month_num)>=4){
                                        current_f_year=format(current_date+years(1),format="%Y" )

    }else{current_f_year=format(current_date,format="%Y" )}

    if (as.numeric(first_mon_mum)>=4){
                                     first_f_year=format(first_date+years(1),format="%Y" )
                                     print(first_f_year)

    }else{first_f_year=format(first_date,format="%Y" )
    }
    
  
    if (DATE_HEADER==TRUE){
           sub_h=paste0(data_unit , "FY",first_f_year,"-","FY",current_f_year," (",current_day,' ',current_month," '",
                             format(data_ends$Month,format="%y"),")")
      
    }else{
          sub_h=paste0(data_unit , "FY",first_f_year,"-","FY",
                       current_f_year," (",current_month," '",
                       format(data_ends$Month,format="%y"),")")
    
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
      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =function(x) seq.Date(from = min(x),to = max(x) ,by=x_axis_interval),
                   expand =c(0,0))+
      
      guides(fill =guide_legend(order =1,reverse=TRUE))+
                          
    
      common_theme()+
      cutom_theme()

    
     ##########################################################RETURN######################
      return(list("chart"= stacked_bar,"s_header"=sub_h[1]))
    
  }, 
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =e[[1]]
    error_msg =paste0(e[[1]],"-in-",unique(data_final$category))
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
  )
}

#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## ------------------------------------------------
multi_line_chart_niif <- function(data1,x_axis_interval="24 month",
                                  graph_lim=30,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE,
                                  legend_key_width=0.27,Position="center",CPI_reference=FALSE,
                                  round_integer=FALSE,Repo=FALSE,DATE_HEADER=FALSE){
  
  tryCatch({
  
  # Note: Always put key_spacing before max_pri_y order of the legends_key
  ##########################DATA_PROCESSING###########################
    showtext_auto()
  
    data1=Reduce(function(x, y) merge(x, y, all=TRUE),data1)
    data1=melt(data1,id=c("Relevant_Date"))
    data1=na.omit(data1)
    
    #For Every multi line chart provide data as a  list of dataframe 

   
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data1$Month <- as.Date(data1$Relevant_Date, format = "%Y-%m-%d")
    max_overlap=10
        
    
    data1$value_y_left=data1[1:nrow(data1),3] 
    data1$category=data1[1:nrow(data1),2] 

    data_final=data1[,c("Relevant_Date","value_y_left","Month","category")]
    data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))
                     
    
    
    data_final=data_final[order(data_final$Relevant_Date),]
    data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
    # prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))
    # print(prev_month)
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
  
    data_ends <- data_final %>% filter(Month == Month[length(Month)])
    data_ends=data_ends[!duplicated(data_ends[c("category")]), ]
    data_ends$value_y_left=roundexcel((data_ends$value_y_left),1)
    print(data_ends)
    
    
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
    
    
    
    ###################################HEADER_SUB_HEADER##################
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)
    
    current_month=format(data_ends$Month,format="%b")
    current_month_num=format(data_ends$Month,"%m")[1]
    first_mon_mum=format(first_date,"%m")
    
  
    if (as.numeric(current_month_num)>=4){
          current_f_year=format(current_date+years(1),format="%Y" )
      
    }else{current_f_year=format(current_date,format="%Y" )}
    
    if (as.numeric(first_mon_mum)>=4){
       first_f_year=format(first_date+years(1),format="%Y" ) 
       
      
    }else{first_f_year=format(first_date,format="%Y" ) 
    }
  
    sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                format(data_ends$Month,format="%y"),")")
    
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
                                                                style_negative = c("parens") ))+
                                                                
                                             
                                              
      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x_date(limits =as.Date(c(NA,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =function(x) seq.Date(from = min(x),to = max(x),by=x_axis_interval),
                   expand =c(0,0))+
                                                
                   
      reference_line_1+
      a1+
      reference_line_2+
      a2+
      

      #To exclude that legend that i don't want.
      guides(fill=FALSE)+
      guides(colour=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE))+
      common_theme()+
      theme1()

    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))
      
    
 
  }, 
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",unique(data_final$category))
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
  )
}
  
#Scale fill manual represents what will be the color of your graph
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## ------------------------------------------------
multi_line_chart_rainfall_niif <- function(data1, x_axis_interval="1 month",num_brek=8
                                        ){
    legend_key_width=0.25
    Position="center"
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
      
      common_theme()+
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



## ------------------------------------------------
line_chart_niif <- function(data1,x_axis_interval="24 month",sales_heading,
                            graph_lim=10,SPECIAL_LINE=FALSE,Position="center",
                            legend_key_width=0.27,Reference=FALSE,DATE_HEADER=FALSE,Repo=FALSE){
  
  tryCatch({
  
    my_chart_col=c("GOLDEN ROD 1","GRAY 48")
    ##########################DATA_PROCESSING###########################
    showtext_auto()
    data1$Relevant_Date <- as.Date(data1$Relevant_Date)
    data1 <- data1[order(data1$Relevant_Date),]
    data1$Month <- as.Date(data1$Relevant_Date,format = "%Y-%m-%d")
    data1$value_y_left=data1[1:nrow(data1),2]  
    
    data_final=cbind(data1[,c("Relevant_Date","value_y_left","Month")])
    data_final=do.call(data.frame,lapply(data_final,function(x) replace(x, is.infinite(x), NA)))

                          
    data_final=na.omit(data_final)
    data_final=data_final[order(data_final$Relevant_Date),]
    data_final<- data_final[data_final$Relevant_Date>=default_start_date,]
    
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
              reference_line_1=geom_segment(x =x_min, y = 18,
                                            xend =x_max, yend =18,
                                            color = "black",size=0.75,
                                            show.legend =FALSE,
                                            linetype =2)
              
              a1=annotate("text",x =x_max-20,y =20,label="18", angle=0, size=10.5,family="Calibri_Regular") 
                           
      
    
      }else{
           reference_line_1=geom_segment(x =x_min, y =0,
                                         xend =x_max, yend =0,
                                         color = "GRAY 32",size=0.25,
                                         show.legend =FALSE,
                                         linetype =1)
            a1=annotate("text",x =x_min,y = 0, label="", angle=0, size=14, color="black")

      }
    
    ###################################HEADER_SUB_HEADER##################
   
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)

    current_month=format(data_ends$Month,format="%b")
    current_month_num=format(data_ends$Month,"%m")
    current_day=format(data_ends$Month,format="%d")
    first_mon_mum=format(first_date,"%m")


    if (as.numeric(current_month_num)>=4){
                                        current_f_year=format(current_date+years(1),format="%Y" )

    }else{current_f_year=format(current_date,format="%Y" )}

    if (as.numeric(first_mon_mum)>=4){
                                     first_f_year=format(first_date+years(1),format="%Y" )
                                     print(first_f_year)

    }else{first_f_year=format(first_date,format="%Y" )
    }

    
    if (DATE_HEADER==TRUE){
          sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_day,' ',current_month," '",
                             format(data_ends$Month,format="%y"),")")
      
    }else{
          sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                       format(data_ends$Month,format="%y"),")")
    
    }
    
    
    ##################################################GRAPH#####################
    
    line=ggplot(data=data_final)+
      geom_line(aes(x=Month,y=value_y_left,color=paste0(sales_heading)),
                size=line_thick,group=1,
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
      scale_y_continuous(expand = expansion(mult = c(0,0.04)),breaks=pretty_breaks(n=num_brek),
                         labels =number_format(big.mark =",",
                                               style_positive = c("none"),
                                               style_negative = c("parens")))+
      coord_cartesian(ylim =c(y_min,y_max))+
      scale_x_date(limits =as.Date(c(x_min,x_max+graph_lim)),
                   labels =date_format("%b-%y"),
                   breaks =function(x) seq.Date(from = min(x),to = max(x),by=x_axis_interval),
                   expand =c(0,0))+
      
      reference_line_1+
      a1+
      common_theme()+
      cutom_theme()
     
    ########################################################RETURN######################
      return(list("chart"=line,"s_header"=sub_h[1]))
  
},
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",unique(data_final$category))
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
    con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, 
                      port = portnum, 
                      user = username, password = password)
    
    dbExecute(con, query)
    dbDisconnect(con)

    }
  )
}


## ------------------------------------------------
stacked_bar_line_chart_special_niif <-  function(data1,data2,data3,x_axis_interval,
                                                 growth_heading,graph_lim=30,
                                                 negative=TRUE,
                                                 DUAL_AXIS=TRUE){ 
  
  tryCatch({
  
    showtext_auto()
    legend_key_width=0.27
    Position="center"
   

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
                   breaks =function(x) seq.Date(from = min(x),to = max(x)
                                                ,by=x_axis_interval),
                   expand =c(0,0))+
      
                #To exclude that legend that i don't want.
                # guides( "none")+
                # guides(fill=FALSE)+
                #Fill--->for bar chart  #Order=1 means it will place that type at top
                #color--->line Chart
                guides(fill=guide_legend(ncol=n_col,nrow=n_row,byrow=TRUE,order=1))+
                guides(color =guide_legend(ncol=2,nrow=1,byrow=TRUE,order=2))+
                       
                # guides(fill=guide_legend(reverse=T))+
                 common_theme()+
    
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
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",unique(data_final$category))

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
  )
}


## ------------------------------------------------
side_bar_chart_niif_rupee <-  function(data1,graph_lim=30,negative=TRUE,
                                       Position="center",
                                       DATE_HEADER=FALSE,pos_d=0.3,pos_lb=0.1){
  
  tryCatch({
  

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
    
########################################HEADER_SUB_HEADER##################
   
    current_date=max(data_ends$Relevant_Date)
    first_date=min(data_final$Relevant_Date)
    
    current_month=format(data_ends$Month,format="%b")
    current_day=format(data_ends$Month,format="%d")
    current_month_num=format(data_ends$Month,"%m")
    first_mon_mum=format(first_date,"%m")
    first_mon_mum
    
    if (as.numeric(current_month_num[1])>=4){
                                       current_f_year=format(current_date+years(1),format="%Y" )
                                       print(current_f_year)
                                         
    }else{current_f_year=format(current_date,format="%Y" )}
    
    
    if (as.numeric(first_mon_mum[1])>=4){
                                       first_f_year=format(first_date+years(1),format="%Y" )
                                       print(first_f_year)

    }else{first_f_year=format(first_date,format="%Y" )
    }

    first_f_year=2013
    
    
    if (DATE_HEADER==TRUE){
       sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_day,' ',current_month," '",
                                 format(data_ends$Month,format="%y"),")")
      
    }else{
       sub_h=paste0("FY",first_f_year,"-","FY",current_f_year," (",current_month," '",
                 format(data_ends$Month,format="%y"),")")
    
    }
    
    
##################################################GRAPH#####################
   stacked_bar=ggplot(data=data_final, 
                       aes(x=factor(x_axis,
                             level=c("1-month","3-month","6-month","1-year","3-year","5-year","10-year")), 
                            
                            y=value_y_left, 
                            fill=category)) +
      
                geom_bar(stat="identity", 
                         width =bar_thick,
                         position=position_dodge(pos_d))+
      
                geom_text(aes(label=label_1), 
                          data=data_ends,
                          vjust =v_just,hjust=h_just,,
                          color="black",
                          position = position_dodge(pos_lb),
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
      common_theme()+
      cutom_theme()

    ########################################################RETURN######################
    return(list("chart"=stacked_bar,"s_header"=sub_h[1]))
},
  error = function(e){
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-",unique(data_final$category))

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
  )
}
#Scale fill manual represents what will be the color of your graph.
#Scale color manual represents what will be the color of your corresponding legends
#Legends always placed with alphabatical order.


## ------------------------------------------------
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
    error_type = class(e)[1][[1]]
    error_msg =paste0(e[[1]],"-in-")

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
  )
}



## ------------------------------------------------
data_query_clk_pg=function(id,exception=FALSE,surplus=FALSE,
                           Water_Reservior=FALSE,VAHAN=FALSE,year=FALSE){
  
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
      period=10
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



## ------------------------------------------------
line_thick=0.60
bar_thick=8
max_overlap=10
num_brek=4
#####legends
legend_key_size=0
legend_key_width=0.27
legend_key_height=0.25
key_spacing=0.10
Position="center"

####Fonts
text_size=30
chart_label=8
v_just=0
x_v=0
nug_x=0.1
nug_y=0.3



## ------------------------------------------------
#Monthly Production & Growth
Cement_Prod1 =data_query_clk_pg(738892)
Cement_Prod1_title=Cement_Prod1[2][[1]]
Cement_Prod1_src=Cement_Prod1 [3][[1]]
Cement_Prod1=Cement_Prod1 [1][[1]]
Cement_Prod=Cement_Prod1[,c("Relevant_Date","Total")]

######################################################
#3W GROWTH RATE
Cement_Prod_gr=Cement_Prod1[,c("Relevant_Date","Total1")]




## ------------------------------------------------
data_s=Cement_Prod
data_g=Cement_Prod_gr

text_size1=35
chart_label=8
# l2=-1
# l1=-2
l2=-2.5
l1=-0.15



num_brek=5
max_pri_y=40
min_pri_y=-10
max_sec_y=40

h_just=-0.50
v_just=0.75
h_just_line=0.40
v_just_line=0.40

Cement_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
          sales_heading=paste0("Cement production (LHS, mn tonnes)"),
          growth_heading=paste0("Cement production growth (RHS, % yoy)"))
               
               
Cement_Prod_7_title=Cement_Prod_7_chart[2][[1]]
Cement_Prod_7_chart=Cement_Prod_7_chart[1][[1]]
Cement_Prod_7_src=Cement_Prod1_src
Cement_Prod_7_chart


## ------------------------------------------------
Steel_Prod1 =data_query_clk_pg(2041910)
Steel_Prod1_title=Steel_Prod1[2][[1]]
Steel_Prod1_src=Steel_Prod1[3][[1]]
Steel_Prod1=Steel_Prod1[1][[1]]
Steel_Prod=Steel_Prod1[,c("Relevant_Date","Total")]

######################################################
#GROWTH RATE
Steel_Prod_gr =data_query_clk_pg(2041915)[1][[1]]


## ------------------------------------------------
data_s=Steel_Prod
data_g=Steel_Prod_gr

h_just=0
v_just=0.30
h_just_line=2
v_just_line=0.30

num_brek=5
max_pri_y=20
min_pri_y=-10
max_sec_y=20

Steel_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
               sales_heading="Crude steel production (LHS, metric ton)",
               growth_heading="Crude steel production (RHS, % yoy)")
                              
Steel_Prod_7_title=Steel_Prod_7_chart[2][[1]]
Steel_Prod_7_chart=Steel_Prod_7_chart[1][[1]]
#HARD_CODE:: JPC WAS ADDEd
Steel_Prod_7_src='Source: Thurro, Ministry of Steel, JPC, NIIF Research'
Steel_Prod_7_chart


## ------------------------------------------------
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




## ------------------------------------------------
data_s=Coal_Prod_new
data_g=Coal_Prod_gr_new

h_just=0.25
v_just=0.0
h_just_line=0.60
v_just_line=0.60

num_brek=7
max_pri_y=100
min_pri_y=-40
max_sec_y=25

Coal_Prod_7_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Coal production (LHS, mn tonnes)",
                                      growth_heading="Coal production growth (RHS, % yoy)")

Coal_Prod_7_title=Coal_Prod_7_chart[2][[1]]
Coal_Prod_7_chart=Coal_Prod_7_chart[1][[1]]
Coal_Prod_7_src=Coal_Prod_src
Coal_Prod_7_chart


## ------------------------------------------------
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

#HARD Code ::NIIF taken from April-2019
e_way_bills=e_way_bills[e_way_bills$Relevant_Date>="2019-04-01",]



## ------------------------------------------------
data_s=e_way_bills
data_g=e_way_bills_gr

x_axis_interval="6 month"

num_brek=5
max_pri_y=100
min_pri_y=-20
max_sec_y=100

e_way_bills_chart=line_bar_chart_niif(data_s,data_g,
                                      graph_lim =90,
                                      sales_heading="e-way bills (LHS, million) ",
                                      growth_heading="e-way bills growth (RHS, % yoy)",
                                      x_axis_interval="12 month")

e_way_bills_8_title=e_way_bills_chart[2][[1]]
e_way_bills_chart=e_way_bills_chart[1][[1]]
e_way_bills_8_src=e_way_bills_src

e_way_bills_chart


## ------------------------------------------------
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
Four_W_reg=Four_W_reg %>% 
           mutate(Total = ifelse(four_el$Electric==0,Total, Total-Electric))


colnames(Four_W_reg)=c("Relevant_Date","PV total registrations (LHS, '000s)",
                      "PV EV registrations (LHS, '000s)")


#######################################################3W GROWTH RATE
Four_W_gr = data_query_clk_pg(id=526302,exception=FALSE,surplus=FALSE,
                              Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(Four_W_gr)[2]='growth'

#HARD Code ::id 526302 having NA value upto March-2014

Four_W_reg=Four_W_reg[Four_W_reg$Relevant_Date>="2014-01-31",]
Four_W_gr=Four_W_gr[Four_W_gr$Relevant_Date>="2014-01-31",]
# Four_W_gr$growth=movavg(Four_W_gr$growth,3,t="s")


## ------------------------------------------------
data_s=Four_W_reg
data_g=Four_W_gr

# BURLYWOOD 1
my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY GRAY 96")

h_just=0
v_just=0.40
h_just_line=0
v_just_line=0.75


n_row=3
n_col=1
chart_label=8
bar_thick=8

num_brek=5
max_pri_y=400
min_pri_y=-100
max_sec_y=80

passenger_vehicle_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="PV total growth registrations (RHS, % yoy)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=60,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
               legends_break =TRUE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=TRUE)


passenger_vehicle_title=passenger_vehicle_chart[2][[1]]
passenger_vehicle_chart=passenger_vehicle_chart[1][[1]]
passenger_vehicle_src=Four_W_reg_src

#HARD CODE::Long source not getting enough space in PPT
passenger_vehicle_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"
passenger_vehicle_chart


## ------------------------------------------------
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
TWO_Sales=TWO_Sales %>% 
           mutate(Total = ifelse(TWO_Sales$Electric==0,Total, Total-Electric))


colnames(TWO_Sales)=c("Relevant_Date","2W total registrations (LHS, '000s)",
                      "2W EV registrations (LHS, '000s)")

#######################################################3W GROWTH RATE
TWO_gr =data_query_clk_pg(id=526384,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(TWO_gr)[2]='growth'

#HARD Code ::id 526384 having NA value upto March-2014

TWO_Sales=TWO_Sales[TWO_Sales$Relevant_Date>="2014-01-01",]
TWO_gr=TWO_gr[TWO_gr$Relevant_Date>="2014-01-01",]
# TWO_gr$growth=movavg(TWO_gr$growth,3,"t")



## ------------------------------------------------
data_s=TWO_Sales
data_g=TWO_gr

# BURLYWOOD 1
my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY GRAY 96")

h_just=0
v_just=0.40
h_just_line=0
v_just_line=0.75

n_row=2
n_col=3
chart_label=8
bar_thick=8

num_brek=5
max_pri_y=2000
min_pri_y=-500
max_sec_y=80

TWO_W_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="2W total growth registrations (RHS, % yoy)",
               x_axis_interval="24 month",legends_break =TRUE,
               data_unit='',graph_lim=60,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
               GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=TRUE)


TWO_W_title=TWO_W_chart[2][[1]]
TWO_W_chart=TWO_W_chart[1][[1]]
TWO_W_src=TWO_Sales_src
#HARD CODE::Long source not getting enough space in PPT
TWO_W_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"



## ------------------------------------------------
CV_Sales =data_query_clk_pg(id=724271)
CV_Sales_title=CV_Sales[2][[1]]
CV_Sales_src=CV_Sales[3][[1]]
CV_Sales=CV_Sales[1][[1]]
CV_Sales$Total=CV_Sales$Total/1000

#######################################################3W GROWTH RATE
CV_gr =data_query_clk_pg(id=724270,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(CV_gr)[2]='growth'

#HARD Code ::id 724270 having NA value upto Dec-2013
CV_gr=CV_gr[CV_gr$Relevant_Date>='2014-01-31',]
CV_Sales=CV_Sales[CV_Sales$Relevant_Date>=min(CV_gr$Relevant_Date),]
# CV_gr$growth=movavg(CV_gr$growth,3,t="s")



## ------------------------------------------------
data_s=CV_Sales
data_g=CV_gr



num_brek=5
max_pri_y=100
min_pri_y=-20
max_sec_y=100

Com_Vehicle_chart=line_bar_chart_niif(data_s,data_g,
                            sales_heading="CV registrations (LHS, '000s)",
                            growth_heading="CV registrations growth (RHS, % yoy)")

Com_Vehicle_title=Com_Vehicle_chart[2][[1]]
Com_Vehicle_chart=Com_Vehicle_chart[1][[1]]
Com_Vehicle_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"



## ------------------------------------------------
TW_Sales =data_query_clk_pg(id=526220)
TW_Sales_title=TW_Sales[2][[1]]
TW_Sales_src=TW_Sales[3][[1]]
TW_Sales=TW_Sales[1][[1]]
TW_Sales$Total=TW_Sales$Total/1000
#######################################################3W GROWTH RATE
TW_gr =data_query_clk_pg(id=526270,exception=FALSE,surplus=FALSE,Water_Reservior=FALSE,VAHAN=TRUE)[1][[1]]
names(TW_gr)[2]='growth'
#HARD Code ::id 526270 having NA value upto Dec-2013
TW_Sales=TW_Sales[TW_Sales$Relevant_Date>="2014-01-31",]
TW_gr=TW_gr[TW_gr$Relevant_Date>="2014-01-31",]
# TW_gr$growth=movavg(TW_gr$growth,3,t="s")



## ------------------------------------------------
data_s=TW_Sales
data_g=TW_gr 

h_just=1.25
v_just=1.25
h_just_line=0
v_just_line=0.60
max_pri_y=100
min_pri_y=-20
max_sec_y=100

TW_chart=line_bar_chart_niif(data_s,data_g,
                       sales_heading="3W registrations (LHS, '000s)",
                       growth_heading="3W registrations growth (RHS, % yoy)",
                       x_axis_interval="24 month",round_integer=FALSE,
                       special_case=FALSE,graph_lim =250,data_unit='')
                       
                              
TW_title=TW_chart[2][[1]]
TW_chart=TW_chart[1][[1]]
#HARD CODE::Long source not getting enough space in PPT
TW_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"


## ------------------------------------------------
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

elec_demand1$Relevant_Date=as.Date(timeLastDayInMonth(elec_demand1$Relevant_Date))
elec_demand_gr$Relevant_Date=as.Date(timeLastDayInMonth(elec_demand_gr$Relevant_Date))



## ------------------------------------------------

data_s=elec_demand1
data_g=elec_demand_gr

v_just=0.60
h_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=4
max_pri_y=150
min_pri_y=-50
max_sec_y=30

Mon_Elec_Demand_chart=line_bar_chart_niif(data_s,data_g,
                                          sales_heading="Electricity demand (LHS, billion kWh)",
                                          growth_heading="Electricity demand growth (RHS, % yoy)")

Mon_Elec_Demand_title=Mon_Elec_Demand_chart[2][[1]]
Mon_Elec_Demand_chart=Mon_Elec_Demand_chart[1][[1]]
Mon_Elec_Demand_src=elec_demand_src



## ------------------------------------------------
# Petroleum Products-Consumption
Petroleum_Con= data_query_clk_pg(id=318939)
Petroleum_Con_title=Petroleum_Con[2][[1]]
Petroleum_Con_src=Petroleum_Con[3][[1]]
Petroleum_Con=Petroleum_Con[1][[1]]
Petroleum_Con$Quantity_KMT=Petroleum_Con$Quantity_KMT/10^6

#Consumption Growth Rate
Petroleum_Con_gr=data_query_clk_pg(id=318921)[1][[1]]


## ------------------------------------------------
data_s=Petroleum_Con
data_g=Petroleum_Con_gr

v_just=0.60
h_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=20
min_pri_y=-6
max_sec_y=20

Petroleum_Con_chart=line_bar_chart_niif(data_s,data_g,
                                        sales_heading="Petroleum consumption (LHS, mn tonnes)",
                                        growth_heading="Petroleum consumption (RHS, % yoy)")

Petroleum_Con_title=Petroleum_Con_chart[2][[1]]
Petroleum_Con_chart=Petroleum_Con_chart[1][[1]]
Petroleum_Con_src=Petroleum_Con_src



## ------------------------------------------------
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
d1=data.frame(date1=c(max(Diesel_price$Relevant_Date),max(Diesel_Con$Relevant_Date)))
common_min=min(as.Date(d1$date1))

Diesel_price=Diesel_price[Diesel_price$Relevant_Date<=common_min,]
Diesel_price$Relevant_Date=as.Date(timeLastDayInMonth(Diesel_price$Relevant_Date))
Diesel_Con=Diesel_Con[Diesel_Con$Relevant_Date<=common_min,]




## ------------------------------------------------
data_s=Diesel_Con
data_g=Diesel_price

h_just=0
v_just=0.20
h_just_line=0.50
v_just_line=0

num_brek=6
max_pri_y=12
min_pri_y=0
max_sec_y=100

Diesel_Con_price_chart=line_bar_chart_niif(data_s,data_g,
                             sales_heading="Diesel consumption (LHS, mn tonnes)",
                             growth_heading="Diesel prices-Delhi (RHS, INR/ltr)")

Diesel_Con_price_title=Diesel_Con_price_chart[2][[1]]
Diesel_Con_price_chart=Diesel_Con_price_chart[1][[1]]
Diesel_Con_price_src=Diesel_Con_src
Diesel_Con_price_title
Diesel_Con_price_chart



## ------------------------------------------------
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




d1=data.frame(date1=c(max(Petrol_Pric_Delhi$Relevant_Date),
                      max(Petrol_Consumption$Relevant_Date)))

common_min=min(as.Date(d1$date1))

Petrol_Pric_Delhi=Petrol_Pric_Delhi[Petrol_Pric_Delhi$Relevant_Date<=common_min,]
Petrol_Pric_Delhi$Relevant_Date=as.Date(timeLastDayInMonth(Petrol_Pric_Delhi$Relevant_Date))
Petrol_Consumption=Petrol_Consumption[Petrol_Consumption$Relevant_Date<=common_min,]



## ------------------------------------------------
data_s=Petrol_Consumption
data_g=Petrol_Pric_Delhi

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.50

num_brek=5
max_pri_y=10
min_pri_y=0
max_sec_y=100

Petrol_Consumption_price_chart=line_bar_chart_niif(data_s,data_g,
                                                   sales_heading="Petrol consumption (LHS, mn tonnes)",
                                                   growth_heading="Petrol prices-Delhi (RHS, INR/ltr)",
                                                   x_axis_interval="24 month",
                                                   round_integer=FALSE,
                                                   special_case=FALSE,
                                                   graph_lim=30,data_unit='',
                                                   WHITE_BACK=FALSE)

Petrol_Consumption_price_title=Petrol_Consumption_price_chart[2][[1]]
Petrol_Consumption_price_chart=Petrol_Consumption_price_chart[1][[1]]
Petrol_Consumption_price_src=Petrol_Consumption_src




## ------------------------------------------------
Water_Reservoir_Level= data_query_clk_pg(id=523077,exception=FALSE,surplus=FALSE,Water_Reservior=TRUE)
Water_Reservoir_Level_title=Water_Reservoir_Level[2][[1]]
Water_Reservoir_Level_src=Water_Reservoir_Level[3][[1]]
Water_Reservoir_Level=Water_Reservoir_Level[1][[1]]

#Reservoir Volume
Water_Reservoir_Level_gr= data_query_clk_pg(id=318929)[1][[1]]



## ------------------------------------------------
data_s=Water_Reservoir_Level
data_g=Water_Reservoir_Level_gr


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
  
Water_Reservoir_Level_chart=line_bar_chart_niif(data_s,data_g,
                  sales_heading="Current reservoir volume - All India (LHS, BCM)",
                  growth_heading="Water reservoir volume (RHS, % yoy)",
                  x_axis_interval="24 month")

Water_Reservoir_Level_title=Water_Reservoir_Level_chart[2][[1]]
Water_Reservoir_Level_chart=Water_Reservoir_Level_chart[1][[1]]


## ------------------------------------------------
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



## ------------------------------------------------
data_s=Domestic_Tractor_reg
data_g=Domestic_Tractor_gr



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=5
max_pri_y=100
min_pri_y=-50
max_sec_y=100

Domestic_Tractor_reg_chart=line_bar_chart_niif(data_s,data_g,
                                       sales_heading="Domestic tractor registrations (LHS, '000 unit)",
                                       growth_heading="Growth (RHS, % yoy)")

Domestic_Tractor_reg_title=Domestic_Tractor_reg_chart[2][[1]]
Domestic_Tractor_reg_chart=Domestic_Tractor_reg_chart[1][[1]]
# Domestic_Tractor_reg_src=Domestic_Tractor_reg_src
#HARD CODE::Long source not getting enough space in PPT
Domestic_Tractor_reg_src="Source: Thurro, VAHAN (Excluding Telangana, Lakshadweep), NIIF Research"



## ------------------------------------------------
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
SCB_Deposit$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Deposit$Relevant_Date))


## ------------------------------------------------
data_s=SCB_Deposit
data_g=SCB_Deposit_gr 


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.50

num_brek=5
max_pri_y=200
min_pri_y=0
max_sec_y=40

SCB_Deposit_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Bank deposit (INR, trillion)",
                                      growth_heading="Growth (RHS, % yoy)")
SCB_Deposit_title=SCB_Deposit_chart[2][[1]]
SCB_Deposit_chart=SCB_Deposit_chart[1][[1]]
SCB_Deposit_src=SCB_Deposit_src
SCB_Deposit_chart


## ------------------------------------------------
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
SCB_Credit_1$Relevant_Date=as.Date(timeLastDayInMonth(SCB_Credit_1$Relevant_Date))



## ------------------------------------------------
data_s=SCB_Credit_1
data_g=SCB_Credit_gr

h_just=0.75
v_just=0.50
h_just_line=0.60
v_just_line=0.50

num_brek=4
max_pri_y=175
min_pri_y=0
max_sec_y=35

SCB_Credit_chart=line_bar_chart_niif(data_s,data_g,
                                     sales_heading="Bank credit (INR  trillion)",
                                     growth_heading="Growth (RHS, % yoy)")


SCB_Credit_title=SCB_Credit_chart[2][[1]]
SCB_Credit_chart=SCB_Credit_chart[1][[1]]
SCB_Credit_src=SCB_Credit_src
SCB_Credit_chart
SCB_Credit_title
draw_key_line=draw_key_smooth


## ------------------------------------------------
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



## ------------------------------------------------
data_s=UPI_Trx_Volumes
data_g=UPI_Trx_values



h_just=0.60
v_just=0
h_just_line=0.60
v_just_line=0


num_brek=4
max_pri_y=15
min_pri_y=0
max_sec_y=15

UPI_Tran_chart=line_bar_chart_niif(data_s,data_g,
                                   sales_heading="Volume (LHS, billion)",
                                   growth_heading="Value (RHS, INR trillion)",
                                   x_axis_interval="12 month",round_integer=TRUE,
                                   special_case=FALSE,graph_lim=90,data_unit='')


UPI_Tran_title=UPI_Tran_chart[2][[1]]
UPI_Tran_chart=UPI_Tran_chart[1][[1]]
UPI_Tran_src=UPI_Trx_Volumes_src
UPI_Tran_chart


## ------------------------------------------------
#In Circulation
Cash_Cir_org=data_query_clk_pg(id=726003)
Cash_Cir_org_title=tolower(Cash_Cir_org[2][[1]])
Cash_Cir_org_src=Cash_Cir_org[3][[1]]
Cash_Cir_org=Cash_Cir_org[1][[1]]
Cash_Cir=Cash_Cir_org[,c("Relevant_Date","Total")]


older_niif=data_query_clk_pg(869227,137)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Cash_Cir$Relevant_Date),]
Cash_Cir=rbind(older_niif,Cash_Cir)
Cash_Cir$Total=Cash_Cir$Total/10^12


Cash_Cir_gr=Cash_Cir_org[,c("Relevant_Date","growth_Total")]

#HARD CODE ::NIIF taken from Feb-2020
Cash_Cir_gr=Cash_Cir_gr[Cash_Cir_gr$Relevant_Date>="2020-02-01",]

older_niif=data_query_clk_pg(878190,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth_Total")
older_niif=older_niif[older_niif$Relevant_Date<min(Cash_Cir_gr$Relevant_Date),]
Cash_Cir_gr=rbind(older_niif,Cash_Cir_gr)




## ------------------------------------------------
data_s=Cash_Cir
data_g=Cash_Cir_gr

special_case_2=TRUE


h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=40
min_pri_y=-20
max_sec_y=40

Cash_Cir_chart=line_bar_chart_niif(data_s,data_g,
                                   sales_heading="Currency in circulation (LHS, INR trillion)",
                                   growth_heading="Growth (RHS, % yoy)")

Cash_Cir_title=Cash_Cir_chart[2][[1]]
Cash_Cir_chart=Cash_Cir_chart[1][[1]]
Cash_Cir_src=Cash_Cir_org_src
Cash_Cir_title
Cash_Cir_chart


## ------------------------------------------------
#RTGS_Tran_volume-RTGS Txn Volume
RTGS_Tran_vol=data_query_clk_pg(id=731129)
RTGS_Tran_vol_title=tolower(RTGS_Tran_vol[2][[1]])
RTGS_Tran_vol_src=RTGS_Tran_vol[3][[1]]
RTGS_Tran_vol=RTGS_Tran_vol[1][[1]]
RTGS_Tran_vol$Volume=RTGS_Tran_vol$Volume/10^6

#RTGS_Tran_value-RTGS Flow
RTGS_Tran_value=data_query_clk_pg(id=731134)[1][[1]]
RTGS_Tran_value$Value=RTGS_Tran_value$Value/10^12



## ------------------------------------------------
data_s=RTGS_Tran_vol
data_g=RTGS_Tran_value



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=25
min_pri_y=1
max_sec_y=300

RTGS_Tran_chart=line_bar_chart_niif(data_s,data_g,sales_heading="Volume (LHS, mn)",
                                    growth_heading="Value (RHS, INR trillion)")

RTGS_Tran_title=RTGS_Tran_chart[2][[1]]
RTGS_Tran_chart=RTGS_Tran_chart[1][[1]]

RTGS_Tran_src=RTGS_Tran_vol_src




## ------------------------------------------------
# Credit_Card_Tran_volume-Credit Card Tran
Credit_Crd_Tran_vol=data_query_clk_pg(id=731108)
Credit_Crd_Tran_vol_title=tolower(Credit_Crd_Tran_vol[2][[1]])
Credit_Crd_Tran_vol_src=Credit_Crd_Tran_vol[3][[1]]
Credit_Crd_Tran_vol=Credit_Crd_Tran_vol[1][[1]]
Credit_Crd_Tran_vol$Volume=Credit_Crd_Tran_vol$Volume/10^6


#Credit_Card_Tran_value-Credit Card Spends
Credit_Card_Tran_value=data_query_clk_pg(id=731107)[1][[1]]
Credit_Card_Tran_value$Value=Credit_Card_Tran_value$Value/10^9



## ------------------------------------------------
data_s=Credit_Crd_Tran_vol
data_g=Credit_Card_Tran_value



h_just=0
v_just=0.60
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=300
min_pri_y=0
max_sec_y=3000

Credit_Card_Tran_chart=line_bar_chart_niif(data_s,data_g,
                                           sales_heading="Volume (LHS, mn)",
                                           growth_heading="Value (RHS, INR billion)")

Credit_Card_Tran_title=Credit_Card_Tran_chart[2][[1]]
Credit_Card_Tran_chart=Credit_Card_Tran_chart[1][[1]]

Credit_Card_Tran_src=Credit_Crd_Tran_vol_src



## ------------------------------------------------
#Net Purchase/Sale#US Dollar
Mon_net_pur_sale = data_query_clk_pg(id=808880)
Mon_net_pur_sale_title=Mon_net_pur_sale[2][[1]]
Mon_net_pur_sale_src=Mon_net_pur_sale[3][[1]]
Mon_net_pur_sale=Mon_net_pur_sale[1][[1]]

Mon_net_pur_sale=Mon_net_pur_sale[,c("Relevant_Date","Value")]
Mon_net_pur_sale$Value=Mon_net_pur_sale$Value/10^9
colnames(Mon_net_pur_sale)=c("Relevant_Date","Net purchase/(sale) of USD (LHS, USD billion)")

#######################################################
#average foreign exchange rate
Avg_Foreign_Exc_Rate_gr = data_query_clk_pg(id=808879)[1][[1]]




## ------------------------------------------------
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
                                                      graph_lim=90,negative=TRUE,SIDE_BAR=FALSE)

Mon_purchase_sale_dollar_title=Mon_purchase_sale_dollar_chart[2][[1]]

Mon_purchase_sale_dollar_chart=Mon_purchase_sale_dollar_chart[1][[1]]
Mon_purchase_sale_dollar_src=Mon_net_pur_sale_src



## ------------------------------------------------
#Monthly Performance
Mon_BSE_Sen = data_query_clk_pg(id=318870)
Mon_BSE_Sen_title=Mon_BSE_Sen[2][[1]]
Mon_BSE_Sen_src=Mon_BSE_Sen[3][[1]]
Mon_BSE_Sen=Mon_BSE_Sen[1][[1]]
Mon_BSE_Sen=Mon_BSE_Sen[,c("Relevant_Date","Index")]

#######################################################3W GROWTH RATE
Mon_BSE_Sen_gr = data_query_clk_pg(id=318893)[1][[1]]
Mon_BSE_Sen_gr=Mon_BSE_Sen_gr[,c("Relevant_Date","growth")]



## ------------------------------------------------
data_s=Mon_BSE_Sen
data_g=Mon_BSE_Sen_gr



h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=80000
min_pri_y=-20000
max_sec_y=80

Mon_BSE_Sen_perform_chart=line_bar_chart_niif(data_s,data_g,
                                              sales_heading="BSE Sensex (LHS)",
                                              growth_heading="BSE Sensex TTM returns (RHS, %)")

Mon_BSE_Sen_perform_title=Mon_BSE_Sen_perform_chart[2][[1]]
Mon_BSE_Sen_perform_chart=Mon_BSE_Sen_perform_chart[1][[1]]
Mon_BSE_Sen_perform_src=Mon_BSE_Sen_src



## ------------------------------------------------
#Monthly Performance
Mon_NSE_Ni = data_query_clk_pg(id=318865)
Mon_NSE_Ni_title=Mon_NSE_Ni[2][[1]]
Mon_NSE_Ni_src=Mon_NSE_Ni[3][[1]]
Mon_NSE_Ni=Mon_NSE_Ni[1][[1]]
Mon_NSE_Ni=Mon_NSE_Ni[,c("Relevant_Date","Value")]

#######################################################3W GROWTH RATE
Mon_NSE_Ni_gr = data_query_clk_pg(id=318890)[1][[1]]
Mon_NSE_Ni_gr=Mon_NSE_Ni_gr[,c("Relevant_Date","growth")]



## ------------------------------------------------
data_s=Mon_NSE_Ni
data_g=Mon_NSE_Ni_gr



num_brek=5
max_pri_y=20000
min_pri_y=-5000
max_sec_y=80

Mon_NSE_Nifty_perform_chart=line_bar_chart_niif(data_s,data_g,
                                                sales_heading="NSE Nifty 50 (LHS)",
                                                growth_heading="NSE Nifty 50 TTM returns (RHS, %)")

Mon_NSE_Nifty_perform_title=Mon_NSE_Nifty_perform_chart[2][[1]]
Mon_NSE_Nifty_perform_chart=Mon_NSE_Nifty_perform_chart[1][[1]]
Mon_NSE_Nifty_perform_src=Mon_NSE_Ni_src



## ------------------------------------------------
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




## ------------------------------------------------
data_s=etc_road_toll_col
data_g=etc_road_toll_col_gr



h_just=0.55
v_just=0.55
h_just_line=1
v_just_line=0.50

num_brek=6
max_pri_y=350
min_pri_y=0
max_sec_y=70

  
Mon_etc_road_toll_col_chart=line_bar_chart_niif(data_s,data_g,sales_heading="Volume (LHS, mn) ",
                                                growth_heading="Value (RHS, INR billion)",
                                                x_axis_interval="12 month")

Mon_etc_road_toll_col_title=Mon_etc_road_toll_col_chart[2][[1]]
Mon_etc_road_toll_col_chart=Mon_etc_road_toll_col_chart[1][[1]]
Mon_etc_road_toll_col_src=etc_road_toll_col_src



## ------------------------------------------------
#Revenue Collected
ihmcl_toll_col=data_query_clk_pg(1836371)
ihmcl_toll_col_title=ihmcl_toll_col[2][[1]]
ihmcl_toll_col_src=ihmcl_toll_col[3][[1]]
ihmcl_toll_col_vol=ihmcl_toll_col[1][[1]]
ihmcl_toll_col_vol$Value=ihmcl_toll_col_vol$Value/10^6

#Traffic
ihmcl_toll_col_val=data_query_clk_pg(1836370)[1][[1]]
ihmcl_toll_col_val$Value=ihmcl_toll_col_val$Value/10^9


## ------------------------------------------------
data_s=ihmcl_toll_col_vol
data_g=ihmcl_toll_col_val

h_just=0.55
v_just=0.55
h_just_line=1
v_just_line=0.50

num_brek=6
max_pri_y=300
min_pri_y=0
max_sec_y=60
bar_thick=8
  
Mon_ihmcl_col_chart=line_bar_chart_niif(data_s,data_g,sales_heading="Volume (LHS, mn) ",
                                                growth_heading="Value (RHS, INR billion)",
                                                x_axis_interval="6 month")

Mon_ihmcl_col_title=Mon_ihmcl_col_chart[2][[1]]
Mon_ihmcl_col_chart=Mon_ihmcl_col_chart[1][[1]]
Mon_ihmcl_col_src=ihmcl_toll_col_src
Mon_ihmcl_col_chart


## ------------------------------------------------
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

colnames(crg_tfc_mjr_port1)=c("Relevant_Date","Ports cargo traffic (LHS, mn tonnes)","JNPT (LHS, mn tonnes)")


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


## ------------------------------------------------
data_s=crg_tfc_mjr_port1
data_g=crg_tfc_mjr_port_gr
# BURLYWOOD 1
my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY GRAY 96")

h_just=0
v_just=0.40
h_just_line=0
v_just_line=0.75

n_row=2
n_col=3
chart_label=8
bar_thick=8

num_brek=6
max_pri_y=80
min_pri_y=-20
max_sec_y=40

Mon_crg_tfc_mjr_port_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                       growth_heading="Ports cargo traffic (RHS, % yoy)",
                                       x_axis_interval="24 month",
                                       data_unit='',graph_lim=30,round_integer=TRUE,
                                       Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
                                       GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=TRUE)


Mon_crg_tfc_mjr_port_title=Mon_crg_tfc_mjr_port_chart[2][[1]]
Mon_crg_tfc_mjr_port_chart=Mon_crg_tfc_mjr_port_chart[1][[1]]
Mon_crg_tfc_mjr_port_src=crg_tfc_mjr_port_src
Mon_crg_tfc_mjr_port_chart


## ------------------------------------------------
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
Mon_psgr_rail_tfc_gr$growth=movavg(Mon_psgr_rail_tfc_gr$growth,3,t='s')



## ------------------------------------------------
data_s=Mon_psgr_rail_tfc
data_g=Mon_psgr_rail_tfc_gr

# BURLYWOOD 1
my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY GRAY 96")

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1",
               "TAN 1",'gray96',"#F5D513")
my_legends_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","#BF6E00","GOLDEN ROD 1",
                 "TAN 1",'gray96',"#F5D513","GRAY 48") 

h_just=0
v_just=0.40
h_just_line=2.25
v_just_line=0.50

Position="left"
n_row=2
n_col=2
chart_label=8
bar_thick=8

num_brek=5
max_pri_y=100
min_pri_y=50
max_sec_y=100

Mon_psgr_rail_tfc=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="Passenger rail traffic (RHS, % yoy, 3 MMA)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=30,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break = TRUE,
               GST_SPECIAL=FALSE,key_spacing=0.75,JNPT=FALSE)


Mon_psgr_rail_tfc_title=Mon_psgr_rail_tfc[2][[1]]
Mon_psgr_rail_tfc=Mon_psgr_rail_tfc[1][[1]]
Mon_psgr_rail_tfc_src=Mon_psgr_rail_tfc_src
Mon_psgr_rail_tfc


## ------------------------------------------------
#Monthly Volume
Mon_rail_freight_tfc=data_query_clk_pg(id=284813)
Mon_rail_freight_tfc_title=Mon_rail_freight_tfc[2][[1]]
Mon_rail_freight_tfc_src=Mon_rail_freight_tfc[3][[1]]
Mon_rail_freight_tfc=Mon_rail_freight_tfc[1][[1]]

#Monthly Volume of Top 3 Categories
Mon_rail_freight_tfc=data_query_clk_pg(id=1836375)[1][[1]]
colnames(Mon_rail_freight_tfc)=c("Relevant_Date","Coal (LHS, mn tonnes)","Iron ore (LHS, mn tonnes)","Cement clinker (LHS, mn tonnes)")

#growth by volume
Mon_rail_freight_tfc_gr= data_query_clk_pg(id=284801)[1][[1]]
Mon_rail_freight_tfc_gr$growth=movavg(Mon_rail_freight_tfc_gr$growth,3,t="s")


## ------------------------------------------------
data_s=Mon_rail_freight_tfc
data_g=Mon_rail_freight_tfc_gr

h_just=0
v_just=0.40
h_just_line=2.25
v_just_line=1.50

n_row=4
n_col=1

num_brek=5
max_pri_y=150
min_pri_y=-50
max_sec_y=60
Position="left"
Mon_rail_freight_tfc_chart=stacked_bar_line_chart_niif(data_s,data_g,
               growth_heading="Rail freight traffic (RHS, % yoy, 3 MMA)",
               x_axis_interval="24 month",
               data_unit='',graph_lim=30,round_integer=TRUE,
               Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
               legends_break = TRUE,
               GST_SPECIAL=FALSE,key_spacing=0.75,JNPT=FALSE)


Mon_rail_freight_tfc_title=Mon_rail_freight_tfc_chart[2][[1]]
Mon_rail_freight_tfc_chart=Mon_rail_freight_tfc_chart[1][[1]]
Mon_rail_freight_tfc_src=Mon_rail_freight_tfc_src

Position="center"


## ------------------------------------------------
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
Mon_dom_air_psn_gr$Growth=movavg(Mon_dom_air_psn_gr$Growth,3,t="s")



## ------------------------------------------------
data_s=Mon_dom_air_psn
data_g=Mon_dom_air_psn_gr



h_just=0.75
v_just=0.75
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=15
min_pri_y=-10
max_sec_y=40

Mon_dom_air_psns_chart=line_bar_chart_niif(data_s,data_g,
                         sales_heading="Domestic air passenger (LHS, million)",
                         growth_heading="Domestic air passenger (RHS, % yoy, 3 MMA)")

Mon_dom_air_psns_title=Mon_dom_air_psns_chart[2][[1]]
 
Mon_dom_air_psns_chart=Mon_dom_air_psns_chart[1][[1]]

Mon_dom_air_psns_src=Mon_dom_air_psn_src
Mon_dom_air_psns_chart


## ------------------------------------------------
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
Mon_air_cargo_tfc_gr$Growth=movavg(Mon_air_cargo_tfc_gr$Growth,3,t="s")


## ------------------------------------------------
data_s=Mon_air_cargo_tfc
data_g=Mon_air_cargo_tfc_gr

h_just=0.60
v_just=0.60
h_just_line=0.60
v_just_line=0.60


num_brek=5
max_pri_y=400
min_pri_y=-100
max_sec_y=80

Mon_air_cargo_tfc_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Air cargo traffic (LHS, '000 tonnes)",
                                      growth_heading="Air cargo traffic growth (RHS, % yoy, 3 MMA)")


Mon_air_cargo_tfc_title=Mon_air_cargo_tfc_chart[2][[1]]
Mon_air_cargo_tfc_chart=Mon_air_cargo_tfc_chart[1][[1]]
Mon_air_cargo_tfc_src=Mon_air_cargo_tfc_src
Mon_air_cargo_tfc_chart



## ------------------------------------------------
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




## ------------------------------------------------
data_s=DAM_Traded_vol
data_g=DAM_Clearing_Price



h_just=0.45
v_just=0.45
h_just_line=0.60
v_just_line=0.60

num_brek=5
max_pri_y=10
min_pri_y=0
max_sec_y=10

Mon_DAM_Clearing_Price_chart=line_bar_chart_niif(data_s,data_g,
                                                 sales_heading="IEX volume (LHS, billion kWh)",
                                                 growth_heading="IEX prices (RHS, INR per kWh)")

Mon_DAM_Clearing_Price_title=Mon_DAM_Clearing_Price_chart[2][[1]]
Mon_DAM_Clearing_Price_chart=Mon_DAM_Clearing_Price_chart[1][[1]]
Mon_DAM_Clearing_Price_src=DAM_Clearing_Price_src
Mon_DAM_Clearing_Price_chart


## ------------------------------------------------
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

#HARD CODE :: For id 2046283 we have taken from Nov-2019 
Mon_elec_gen_gr=Mon_elec_gen_gr[Mon_elec_gen_gr$Relevant_Date>"2019-11-01",]

older_niif=data_query_clk_pg(894695,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth")
older_niif=older_niif[older_niif$Relevant_Date<min(Mon_elec_gen_gr$Relevant_Date),]
Mon_elec_gen_gr=rbind(older_niif,Mon_elec_gen_gr)
Mon_elec_gen_gr$Relevant_Date=as.Date(timeLastDayInMonth(Mon_elec_gen_gr$Relevant_Date))



## ------------------------------------------------
data_s=Mon_elec_gen
data_g=Mon_elec_gen_gr

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=175
min_pri_y=-50
max_sec_y=75

Mon_electricity_gen_chart=line_bar_chart_niif(data_s,data_g,
                                              sales_heading="Electricity Generation (LHS, billion kWh) ",
                                              growth_heading="Growth (RHS, yoy %)")

Mon_electricity_gen_title=Mon_electricity_gen_chart[2][[1]]
Mon_electricity_gen_chart=Mon_electricity_gen_chart[1][[1]]
Mon_electricity_gen_src=Mon_elec_gen_src
Mon_electricity_gen_chart


## ------------------------------------------------
#Avg Daily Generation
gdp_const_pri=data_query_clk_pg( 1502377)
gdp_const_pri_title=gdp_const_pri[2][[1]]
gdp_const_pri_src=gdp_const_pri[3][[1]]
gdp_const_pri=gdp_const_pri[1][[1]]
gdp_const_pri$Value=gdp_const_pri$Value/10^12

#Growth
gdp_const_pri_gr=data_query_clk_pg(1502376)[1][[1]]
gdp_const_pri_gr=gdp_const_pri_gr[gdp_const_pri_gr$Relevant_Date>default_start_date,]



## ------------------------------------------------
data_s=gdp_const_pri
data_g=gdp_const_pri_gr

h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=4
max_pri_y=60
min_pri_y=-20
max_sec_y=15

Qtr_real_GDP_growth_chart=line_bar_chart_niif(data_s,data_g,
                                 sales_heading=paste0("GDP (LHS, INR trillion)")
                                ,growth_heading=paste0("Growth (RHS, % yoy)"))

Qtr_real_GDP_growth_title=Qtr_real_GDP_growth_chart[2][[1]]
Qtr_real_GDP_growth_chart=Qtr_real_GDP_growth_chart[1][[1]]
Qtr_real_GDP_growth_src=gdp_const_pri_src
Qtr_real_GDP_growth_chart


## ------------------------------------------------
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



## ------------------------------------------------
data_s=services_trd
data_g=services_trd_gr

legend_key_width=0.27
h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=8
min_pri_y=-2
max_sec_y=40

Services_trd_chart=line_bar_chart_niif(data_s,data_g,
                                       sales_heading="Services trade (LHS, INR trillion)",
                                       growth_heading="Growth (RHS, % yoy)",
                                       x_axis_interval="24 month",round_integer=TRUE,
                                       special_case=FALSE,
                                       graph_lim=30,data_unit='',WHITE_BACK=FALSE,
                                       Position="center",key_spacing=0)

Services_trd_title=Services_trd_chart[2][[1]]
Services_trd_chart=Services_trd_chart[1][[1]]
Services_trd_src=services_trd_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=services_real_est
data_g=services_real_est_gr



h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=4
min_pri_y=-1
max_sec_y=40
services_real_est_chart=line_bar_chart_niif(data_s,data_g,
                                sales_heading="Services commercial real estate (LHS, INR trillion)",
                                growth_heading="Growth (RHS, yoy %)",
                                x_axis_interval="24 month",round_integer=TRUE,
                                special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE)

services_real_est_title=services_real_est_chart[2][[1]]
services_real_est_chart=services_real_est_chart[1][[1]]
services_real_est_src=services_real_est_src




## ------------------------------------------------
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


## ------------------------------------------------
data_s=GVA_act
data_g=GVA_total



h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=60
min_pri_y=-20
max_sec_y=15
Qtr_real_GVA_gr_chart=line_bar_chart_niif(data_s,data_g,
                                    sales_heading="GVA (LHS, INR trillion)",
                                    growth_heading="GVA (RHS,  % yoy)",
                                    x_axis_interval="24 month",round_integer=TRUE,
                                          special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE)

Qtr_real_GVA_gr_title=Qtr_real_GVA_gr_chart[2][[1]]
Qtr_real_GVA_gr_chart=Qtr_real_GVA_gr_chart[1][[1]]
Qtr_real_GVA_gr_src=GVA_act_src


## ------------------------------------------------
#Telecom (Wireless + Wireline)
#Total Subscribers
telecom_t= data_query_clk_pg(1669458,exception=TRUE)
telecom_t_src=telecom_t[3][[1]]
telecom_t=telecom_t[1][[1]]



telecom_t=telecom_t[telecom_t$Relevant_Date>=default_start_date,]
telecom_t$Value=telecom_t$Value/10^6
colnames(telecom_t) = c("Relevant_Date","telecom")

#Telecom | Internet
tele_int= data_query_clk_pg(1669459,exception=TRUE)[1][[1]]

tele_int=tele_int[tele_int$Relevant_Date>=default_start_date,]
tele_int$Total=tele_int$Total/10^6
colnames(tele_int) = c("Relevant_Date","internet")



## ------------------------------------------------
data_s=telecom_t
data_g=tele_int

bar_thick=40
h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=1500
min_pri_y=-20
max_sec_y=1500
Qtr_tele_sub_chart=line_bar_chart_niif(data_s,data_g,
                                      sales_heading="Telecom subscribers",
                                      growth_heading="Internet subscribers",
                                      x_axis_interval="24 month",round_integer=TRUE,
                                      special_case=FALSE,graph_lim=30,data_unit='',WHITE_BACK=FALSE)

Qtr_tele_sub_title=Qtr_tele_sub_chart[2][[1]]
Qtr_tele_sub_chart=Qtr_tele_sub_chart[1][[1]]

Qtr_tele_sub_src=telecom_t_src
bar_thick=8


## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
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


## ------------------------------------------------
#World Bank Commodity | Copper
Mon_Cu_pri= data_query_clk_pg(id=723144)
Mon_Cu_pri_title=Mon_Cu_pri[2][[1]]
Mon_Cu_pri_src=Mon_Cu_pri[3][[1]]
Mon_Cu_pri=Mon_Cu_pri[1][[1]]
Mon_Cu_pri$Total=Mon_Cu_pri$Total


#World Bank Commodity | Iron ore, cfr spot
Monthly_Iron_ore_prices= data_query_clk_pg(id=723155)[1][[1]]




## ------------------------------------------------
data_s=Mon_Cu_pri
data_g=Monthly_Iron_ore_prices



h_just=0
v_just=0.75
h_just_line=0
v_just_line=0.75

num_brek=4
max_pri_y=15000
min_pri_y=0
max_sec_y=300

my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48") 
  
Mon_Cu_Fe_ore_prices_WB_42_chart=line_bar_chart_niif(data_s,data_g,
                               sales_heading="Copper (USD/tonne)",
                               growth_heading="Iron ore, 62% fine CFR Qingdao (USD/dry metric ton)",
                               x_axis_interval="24 month",round_integer=FALSE,
                               special_case=TRUE,graph_lim=30,data_unit='')

Mon_Cu_Fe_ore_prices_WB_42_title=Mon_Cu_Fe_ore_prices_WB_42_chart[2][[1]]
Mon_Cu_Fe_ore_prices_WB_42_chart=Mon_Cu_Fe_ore_prices_WB_42_chart[1][[1]]
Mon_Cu_Fe_ore_prices_WB_42_src=Mon_Cu_pri_src



## ------------------------------------------------
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



## ------------------------------------------------
if (max(Forex_import_cover$Relevant_Date)<max(Forex_resr$Relevant_Date)){
    com_date=as.Date(min(max(Forex_import_cover$Relevant_Date),max(Forex_resr$Relevant_Date)))
    missing_mon=subset(Forex_resr,Forex_resr$Relevant_Date>as.Date(com_date))[1,1]
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
    
    d1=data.frame(Relevant_Date=as.Date(missing_mon),Total=Forex_import_cover1)
    Forex_import_cover=rbind(Forex_import_cover,d1)
}


## ------------------------------------------------
data_s=Forex_resr
data_g=Forex_import_cover


key_spacing=0

h_just=0
v_just=0.60
h_just_line=0.50
v_just_line=0.50

num_brek=5
max_pri_y=800
min_pri_y=0
max_sec_y=20

Mon_Forex_Reserve_chart=line_bar_chart_niif(data_s,data_g,
                                            sales_heading="Forex reserves (LHS, USD billion)",
                                            growth_heading="Forex to import cover (RHS, months)",
                                            x_axis_interval="24 month",round_integer=FALSE,
                                            special_case=TRUE,graph_lim=30,data_unit='',
                                            WHITE_BACK=FALSE,Position="center",
                                            key_spacing=0.10,DATE_HEADER=TRUE)

Mon_Forex_Reserve_title=Mon_Forex_Reserve_chart[2][[1]]
Mon_Forex_Reserve_chart=Mon_Forex_Reserve_chart[1][[1]]
Mon_Forex_Reserve_src=Forex_resr_src
Mon_Forex_Reserve_chart
Mon_Forex_Reserve_title


## ------------------------------------------------
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



## ------------------------------------------------
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
bar_thick=8


num_brek=4
max_pri_y=10
min_pri_y=-5
max_sec_y=60

Mon_ferti_Sales_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                 growth_heading="Growth (RHS, % yoy)",
                                                 x_axis_interval="12 month",
                                                 data_unit='',graph_lim=30,round_integer=TRUE,
                                                 Exception=FALSE,SIDE_BAR=FALSE,negative=FALSE,
                                                 GST_SPECIAL=FALSE)

Mon_ferti_Sales_title=Mon_ferti_Sales_chart[2][[1]]
Mon_ferti_Sales_chart=Mon_ferti_Sales_chart[1][[1]]
Mon_ferti_Sales_src=Mon_ferti_Sales_src



## ------------------------------------------------
#New Payroll Additions
Mon_enrl_epfo= data_query_clk_pg(id=808266)
Mon_enrl_epfo_title=Mon_enrl_epfo[2][[1]]
Mon_enrl_epfo_src=Mon_enrl_epfo[3][[1]]
Mon_enrl_epfo=Mon_enrl_epfo[1][[1]]
Mon_enrl_epfo$Total=Mon_enrl_epfo$Total/10^6
colnames(Mon_enrl_epfo)=c("Relevant_Date","EPFO payroll (LHS, mn)")

#ESIC Subscribers Addition For NIIF 
ESIC_Scbr= data_query_clk_pg(id=1384190)[1][[1]]
ESIC_Scbr$Total=ESIC_Scbr$Total/10^6
colnames(ESIC_Scbr)=c('Relevant_Date','ESIC subscriber (LHS, mn)')
ESIC_NPS=merge(ESIC_Scbr,Mon_enrl_epfo,by="Relevant_Date")
NPS_Subscribers= data_query_clk_pg(id=1384191)[1][[1]]

epfo=data_query_clk_pg(1836413,year =TRUE)[1][[1]]
Epfo_esic=epfo[,c("Relevant_Date",'EPFO','ESIC')]
colnames(Epfo_esic)=c('Relevant_Date',"EPFO payroll (LHS, mn)",'ESIC subscriber (LHS, mn)')
Epfo_esic=Epfo_esic%>%mutate(across(c(2:3), .fns = ~./10^6))

#NPS Subscribers Added For NIIF
NPS_Subscribers=epfo[,c("Relevant_Date",'NPS')]
colnames(NPS_Subscribers)=c("Relevant_Date","growth")
NPS_Subscribers$growth=NPS_Subscribers$growth/1000


## ------------------------------------------------
data_s=Epfo_esic
data_g=NPS_Subscribers

bar_thick=60

my_chart_col=c("GOLDEN ROD 1","GRAY 48")
my_legends_col=c("GOLDEN ROD 1","GRAY 48","TAN 1")
SIDE_BAR=TRUE
Exception=TRUE


h_just=0
v_just=0
h_just_line=-0.75
v_just_line=1.10

n_row=1
n_col=3


num_brek=4
max_pri_y=20
min_pri_y=0
max_sec_y=1000

Mon_enrollment_num_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                     growth_heading="NPS subscribers (RHS, 000s)",
                                                     x_axis_interval="12 month",
                                                     data_unit='',graph_lim=470,
                                                     round_integer=FALSE,Exception=TRUE,
                                                     SIDE_BAR=TRUE,
                                                     negative=FALSE,GST_SPECIAL=FALSE,YTD =TRUE)


Mon_enrollment_num_title=Mon_enrollment_num_chart[2][[1]]
Mon_enrollment_num_chart=Mon_enrollment_num_chart[1][[1]]
Mon_enrollment_num_src=Mon_enrl_epfo_src
Mon_enrollment_num_chart
bar_thick=12


## ------------------------------------------------
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


## ------------------------------------------------
data_s=niif_gst
data_g=niif_gst_gr


h_just=0
v_just=0.60
h_just_line=0
v_just_line=0.75


legend_key_width=0.27
key_spacing=0.75

num_brek=5
max_pri_y=2000
min_pri_y=0
max_sec_y=10

n_col=6
n_row=1

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1","DARK ORANGE 2","BURLYWOOD 1","GRAY 48") 
                          
GST_Col_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                          growth_heading="TTM GST revenue (% of TTM GDP)",
                                          x_axis_interval="12 month",data_unit='',graph_lim=30,
                                          round_integer=FALSE,Exception=FALSE,SIDE_BAR=FALSE,
                                          negative=FALSE,GST_SPECIAL=TRUE,DUAL_AXIS=TRUE,key_spacing=0.75,legends_reverse=FALSE)

GST_Col_title=GST_Col_chart[2][[1]]
GST_Col_chart=GST_Col_chart[1][[1]]
GST_Col_src=niif_gst_src
key_spacing=0.10
GST_Col_chart


## ------------------------------------------------
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
older_niif=cbind(older_nonoil,older_oil,by="Relevant_Date")
older_niif=older_niif[,c("Relevant_Date",'Non-oil exports',"Oil exports")]
Exports_com_b=older_niif

#TRADE
older_mer=data_query_clk_pg(1502373)[1][[1]]
older_mer$Value=older_mer$Value/10^9
colnames(older_mer)=c("Relevant_Date","growth")
Exports_com_g=older_mer

Exports_com_b=Exports_com_b[!duplicated(Exports_com_b[c("Relevant_Date")]), ]
Exports_com_g=Exports_com_g[!duplicated(Exports_com_g[c("Relevant_Date")]), ]



## ------------------------------------------------
data_s=Exports_com_b
data_g=Exports_com_g

draw_key_line=draw_key_rect

h_just=1
v_just=0.20
h_just_line=0
v_just_line=0.75

max_overlap=8
Position="center"

num_brek=6
max_pri_y=70
min_pri_y=0
max_sec_y=70


my_chart_col=c("GOLDEN ROD 1","GRAY 88")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY 88") 
                                  
Mon_Exports_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                  growth_heading="Merchandize exports",
                                                  x_axis_interval='24 month',data_unit='',
                                                  graph_lim=200,
                                                  round_integer=TRUE,Exception=TRUE,
                                                  SIDE_BAR=FALSE,negative=FALSE,
                                                  GST_SPECIAL=FALSE,DUAL_AXIS=FALSE)

Mon_Exports_title=Mon_Exports_chart[2][[1]]
Mon_Exports_chart=Mon_Exports_chart[1][[1]]
Mon_Exports_src=Exports_Compo_src



## ------------------------------------------------
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
older_niif=cbind(older_nonoil,older_oil,by="Relevant_Date")
older_niif=older_niif[,c("Relevant_Date",'Non-oil imports',"Oil imports")]
Imports_Com_b=older_niif

older_mer=data_query_clk_pg(1502371)[1][[1]]
older_mer$Value=older_mer$Value/10^9
colnames(older_mer)=c("Relevant_Date","growth")
Imports_Com_g=older_mer


Imports_Com_b=Imports_Com_b[!duplicated(Imports_Com_b[c("Relevant_Date")]), ]
Imports_Com_g=Imports_Com_g[!duplicated(Imports_Com_g[c("Relevant_Date")]), ]



## ------------------------------------------------
data_s=Imports_Com_b
data_g=Imports_Com_g



my_chart_col=c("GOLDEN ROD 1","GRAY 88")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY 88") 

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
                   round_integer=TRUE,Exception=TRUE,graph_lim=200,
                   SIDE_BAR=FALSE,negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=FALSE)

Mon_Import_title=Mon_Import_chart[2][[1]]
Mon_Import_chart=Mon_Import_chart[1][[1]]
Mon_Import_src=Imports_Com_src




## ------------------------------------------------
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



## ------------------------------------------------
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



## ------------------------------------------------
#Naukri jobspeak index
Naukri_jobspeak_index= data_query_clk_pg(id=808881)
Naukri_jobspeak_index_title=Naukri_jobspeak_index[2][[1]]
Naukri_jobspeak_index_src=Naukri_jobspeak_index[3][[1]]
Naukri_jobspeak_index=Naukri_jobspeak_index[1][[1]]
colnames(Naukri_jobspeak_index)=c("Relevant_Date","Naukri jobspeak index")

# Growth In Jobs Index
Naukri_jobspeak_index_gr= data_query_clk_pg(id=1682365)[1][[1]]
colnames(Naukri_jobspeak_index_gr)=c("Relevant_Date","growth")



## ------------------------------------------------
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


## ------------------------------------------------
Mon_gen_from_energy_src= data_query_clk_pg(id=725964)
Mon_gen_from_energy_src_title=Mon_gen_from_energy_src[2][[1]]
Mon_gen_from_energy_src_src=Mon_gen_from_energy_src[3][[1]]
Mon_gen_from_energy_src=Mon_gen_from_energy_src[1][[1]]

Mon_gen_from_energy_src$Total=Mon_gen_from_energy_src$Total/10^3
Mon_gen_from_energy_src$Total1=Mon_gen_from_energy_src$Total1/10^3


colnames(Mon_gen_from_energy_src)=c("Relevant_Date","Conventional","Renewable","Share of renewable")
energy_srcs=Mon_gen_from_energy_src[,c("Relevant_Date","Conventional", "Renewable")]
                    
older_niif=data_query_clk_pg(894698,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Conventional","Renewable")
older_niif=older_niif%>%mutate(across(c(2:3), .fns = ~./10^3))
older_niif=older_niif[older_niif$Relevant_Date<min(energy_srcs$Relevant_Date),]
energy_srcs=rbind(older_niif,energy_srcs)



energy_share=Mon_gen_from_energy_src[,c("Relevant_Date","Share of renewable")]
colnames(energy_share)=c("Relevant_Date","growth")
older_niif=data_query_clk_pg(894701,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","growth")
older_niif=older_niif[older_niif$Relevant_Date<min(energy_share$Relevant_Date),]
energy_share=rbind(older_niif,energy_share)
energy_share$Relevant_Date=as.Date(energy_share$Relevant_Date)



## ------------------------------------------------
data_s=energy_srcs
data_g=energy_share

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY 48")
x_axis_interval="12 month"

h_just=0
v_just=0.10
h_just_line=0
v_just_line=0.75
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
                          round_integer=FALSE,Exception=FALSE,SIDE_BAR=FALSE,
                          negative=FALSE,GST_SPECIAL=FALSE)


Mon_gen_energy_srcs_title=Mon_gen_energy_srcs_chart[2][[1]]
Mon_gen_energy_srcs_chart=Mon_gen_energy_srcs_chart[1][[1]]
Mon_gen_energy_srcs_src=Mon_gen_from_energy_src_src
Mon_gen_energy_srcs_chart



## ------------------------------------------------
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



## ------------------------------------------------
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



## ------------------------------------------------
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
FDI_invest_gr=mon_year_df_creator(FDI_invest_gr,keep_col=c("Relevant_Date","Value"),
                                  Sum_date = TRUE)
colnames(FDI_invest_gr)=c("Relevant_Date","growth")
FDI_invest_gr=FDI_invest_gr[!duplicated(FDI_invest_gr),]
FDI_invest_gr$growth=FDI_invest_gr$growth/10^9



## ------------------------------------------------
data_s=FDI_invest
data_g=FDI_invest_gr
bar_thick=40
key_spacing=1

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2")

num_brek=5
max_pri_y=60
min_pri_y=20
max_sec_y=60

n_row=1
n_col=3

h_just=1.10
v_just=1.10
h_just_line=-2.5
v_just_line=1.25

Mon_FDI_invest_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                                 growth_heading="Net FDI inflows into India",
                                                 x_axis_interval="24 month",data_unit="",
                                                 graph_lim=470,
                                                 round_integer=TRUE,
                                                 Exception=FALSE,SIDE_BAR=FALSE,
                                                 negative=TRUE,
                                                 GST_SPECIAL=FALSE,
                                                 DUAL_AXIS=TRUE,key_spacing=1,YTD =TRUE)


Annual_FDI_invest_title=Mon_FDI_invest_chart[2][[1]]
Annual_FDI_invest_chart=Mon_FDI_invest_chart[1][[1]]
Annual_FDI_invest_src=FDI_by_India_src
Annual_FDI_invest_chart

key_spacing=0.10
bar_thick=8


## ------------------------------------------------
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



## ------------------------------------------------
data_s=FDI_invest
data_g=FDI_invest_gr
key_spacing=1

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2")

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
                           GST_SPECIAL=FALSE,
                           DUAL_AXIS=TRUE,key_spacing=1,YTD =FALSE)


Mon_FDI_invest_title=Mon_FDI_invest_chart[2][[1]]
Mon_FDI_invest_chart=Mon_FDI_invest_chart[1][[1]]
Mon_FDI_invest_src=FDI_by_India_src
Mon_FDI_invest_chart

key_spacing=0.10
bar_thick=8


## ------------------------------------------------
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



## ------------------------------------------------

data_s=mf_aaum
data_g=mf_aum_gr

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60

bar_thick=40
legend_key_width=0.27
key_spacing=0.50

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
                                         negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=TRUE,
                                         key_spacing=0.75)

                                         

Mf_aum_title=Mf_aum_chart[2][[1]]
Mf_aum_chart=Mf_aum_chart[1][[1]]
Mf_aum_src=mf_corporates_src
Mf_aum_chart


## ------------------------------------------------
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
mf_others = data_query_clk_pg(1685096)[1][[1]]
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



## ------------------------------------------------
data_s=mf_debt
data_g=mf_debt_gr

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60

bar_thick=40
legend_key_width=0.27
key_spacing=0.10

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
                                         negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=FALSE,
                                         key_spacing=0.75)

                                         

Mf_debt_title=Mf_debt_chart[2][[1]]
Mf_debt_chart=Mf_debt_chart[1][[1]]
Mf_debt_src=mf_debt_gsc_src
Mf_debt_chart


## ------------------------------------------------
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


## ------------------------------------------------
data_s=gov_int_borr
data_g=gov_int_gdp

h_just=0
v_just=0.40
h_just_line=0.40
v_just_line=0.60

bar_thick=40
legend_key_width=0.23
key_spacing=0.0

num_brek=6
max_pri_y=140
min_pri_y=0
max_sec_y=80

n_col=1
n_row=4

n_col1=1
n_row1=4

my_chart_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1","#BF6E00")
my_legends_col=c("GRAY 88","DARK ORANGE 2","BURLYWOOD 1","GOLDEN ROD 1","#BF6E00","GRAY 48") 

                
govt_int_debt_chart=stacked_bar_line_chart_niif(data_s,data_g,
                     growth_heading="Domestic borrowings to TTM GDP (RHS, %)",
                     x_axis_interval="24 month",data_unit='',
                     graph_lim=60,round_integer=FALSE,
                     Exception=FALSE,SIDE_BAR=FALSE,
                     negative=FALSE,GST_SPECIAL=FALSE,
                     DUAL_AXIS=TRUE,legends_break=TRUE,
                     key_spacing=0.75)


govt_int_debt_title=govt_int_debt_chart[2][[1]]
govt_int_debt_chart=govt_int_debt_chart[1][[1]]
govt_int_debt_src=gov_int_fi_src



## ------------------------------------------------
#External Debt | Long-term
gov_ex_lng = data_query_clk_pg(1685097)
gov_ex_lng_src=gov_ex_lng[3][[1]]
gov_ex_lng=gov_ex_lng[1][[1]]
names(gov_ex_lng)[2]='Long-term (LHS, INR tn)'

#External Debt | Short-term
gov_ex_srt = data_query_clk_pg(1685098)[1][[1]]
names(gov_ex_srt)[2]='Short-term (LHS, INR tn)'

gov_ext_borr=cbind(gov_ex_lng,gov_ex_srt,by="Relevant_Date")
gov_ext_borr=gov_ext_borr[,c("Relevant_Date",
                             "Short-term (LHS, INR tn)",
                             "Long-term (LHS, INR tn)")]
gov_ext_borr=gov_ext_borr%>%mutate(across(c(2:3), .fns = ~./10^12))


#total aum at the end of quarter
gov_ext_gdp =data_query_clk_pg(1685435,exception=TRUE)[1][[1]]
colnames(gov_ext_gdp)=c("Relevant_Date","growth")


## ------------------------------------------------
data_s=gov_ext_borr
data_g=gov_ext_gdp

h_just=0
v_just=0.85
h_just_line=0
v_just_line=0.30


bar_thick=40
legend_key_width=0.23
key_spacing=0.10

num_brek=6
max_pri_y=140
min_pri_y=0
max_sec_y=80

n_col=1
n_row=2

my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("DARK ORANGE 2","GOLDEN ROD 1","GRAY 48") 
                 
                          
govt_ext_debt_chart=stacked_bar_line_chart_niif(data_s,data_g,
                         growth_heading="External borrowings to TTM GDP (RHS, % TTM GDP)",
                         x_axis_interval="24 month",data_unit='',
                         graph_lim=150,round_integer=FALSE,
                         Exception=FALSE,SIDE_BAR=FALSE,legends_break=TRUE,
                         negative=FALSE,GST_SPECIAL=FALSE,DUAL_AXIS=TRUE,
                         key_spacing=0.75)

                                         

govt_ext_debt_title=govt_ext_debt_chart[2][[1]]
govt_ext_debt_chart=govt_ext_debt_chart[1][[1]]
govt_ext_debt_src=gov_ex_lng_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=mf_folios
data_g=mf_folios_gr

h_just=0.60
v_just=0
h_just_line=0.60
v_just_line=0.60

bar_thick=40
legend_key_width=0.27
key_spacing=0.50

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
                                         DUAL_AXIS=TRUE,
                                         key_spacing=0.75)

                                         

Mf_folios_title=Mf_folios_chart[2][[1]]
Mf_folios_chart=Mf_folios_chart[1][[1]]
Mf_folios_src=mf_hni_src
bar_thick=8


## ------------------------------------------------
#stocks
food_grain_fci= data_query_clk_pg(id=318923)
food_grain_fci_title=food_grain_fci[2][[1]]
food_grain_fci_src=food_grain_fci[3][[1]]
food_grain_fci=food_grain_fci[1][[1]]
colnames(food_grain_fci)=c("Relevant_Date","Rice","Wheat")

# food_grain_fci$Relevant_Date=as.Date(timeFirstDayInMonth(as.Date(food_grain_fci$Relevant_Date-duration(1,"days"))))



## ------------------------------------------------
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
bar_thick=8

Mon_Food_Grain_Stock_FCI_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",
                                                      data_unit="",graph_lim=120,negative=FALSE)

## 
Mon_Food_Grain_Stock_FCI_title=Mon_Food_Grain_Stock_FCI_chart[2][[1]]
Mon_Food_Grain_Stock_FCI_chart=Mon_Food_Grain_Stock_FCI_chart[1][[1]]
Mon_Food_Grain_Stock_FCI_src=food_grain_fci_src



## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
## data_s=list(GDP_contri_priv,GDP_contri_govt,GDP_contri_iniv,GDP_contri_export)
## 
## h_just=0
## v_just=0.60
## 
## num_brek=6
## max_pri_y=120
## min_pri_y=20
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


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(dir_tax,indir_tax)

h_just=0
v_just=0.75
h_just_line=1.15
v_just_line=1.15

num_brek=6
max_pri_y=25
min_pri_y=0
bar_thick=8

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2")


Tax_Col_govt_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",graph_lim=30,negative=FALSE)


Tax_Col_govt_title=Tax_Col_govt_chart[2][[1]]
Tax_Col_govt_chart=Tax_Col_govt_chart[1][[1]]
Tax_Col_govt_src=dir_tax_src


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(capital_exp,revinue_exp)

h_just=0
v_just=0.75
h_just_line=1.15
v_just_line=1.15

num_brek=5
max_pri_y=35
min_pri_y=10
bar_thick=8

my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1")

Govt_Gross_market_Bor_21_chart=stacked_bar_chart_niif(data_s,
                                                      x_axis_interval="12 month",
                                                      data_unit="",graph_lim=30,negative=FALSE)

Govt_Gross_market_Bor_21_title=Govt_Gross_market_Bor_21_chart[2][[1]]
Govt_Gross_market_Bor_21_chart=Govt_Gross_market_Bor_21_chart[1][[1]]

Govt_Gross_market_Bor_21_src=capital_exp_src


## ------------------------------------------------
#Throught Debt Instruments
Mon_debt_funds= data_query_clk_pg(id=318917)
Mon_debt_funds_title=Mon_debt_funds[2][[1]]
Mon_debt_funds_src=Mon_debt_funds[3][[1]]
Mon_debt_funds=Mon_debt_funds[1][[1]]
Mon_debt_funds=Mon_debt_funds%>%mutate(across(c(2:3), .fns = ~./10^9))
                        
colnames(Mon_debt_funds)=c("Relevant_Date","Public and rights issues","Private placement")
Mon_debt_funds=Mon_debt_funds[,c("Relevant_Date","Private placement","Public and rights issues")]



## ------------------------------------------------
data_s=list(Mon_debt_funds)

h_just=0.60
v_just=0.60
h_just_line=1.85
v_just_line=1.85

num_brek=6
max_pri_y=1400
min_pri_y=0

my_chart_col=c("GOLDEN ROD 1","GRAY 88")
my_legends_col=c("GOLDEN ROD 1","GRAY 88") 

Mon_debt_funds_mobi_India_chart=stacked_bar_chart_niif(data_s,data_unit="",graph_lim=150,negative=FALSE)

Mon_debt_funds_mobi_India_title=Mon_debt_funds_mobi_India_chart[2][[1]]
Mon_debt_funds_mobi_India_chart=Mon_debt_funds_mobi_India_chart[1][[1]]
Mon_debt_funds_mobi_India_src=Mon_debt_funds_src




## ------------------------------------------------
#Throught Equity Instruments
Mon_equity_funds= data_query_clk_pg(id=318940)
Mon_equity_funds_title=Mon_equity_funds[2][[1]]
Mon_equity_funds_src=Mon_equity_funds[3][[1]]
Mon_equity_funds=Mon_equity_funds[1][[1]]
Mon_equity_funds=Mon_equity_funds%>%mutate(across(c(2:5), .fns = ~./10^9))

colnames(Mon_equity_funds)=c("Relevant_Date","IPOs","Rights issues","QIPs","Preferential allotments")



## ------------------------------------------------
data_s=list(Mon_equity_funds)

h_just=0.55
v_just=0.55

num_brek=6
max_pri_y=1400
min_pri_y=0

my_chart_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2")
my_legends_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2")

Mon_equity_fu_mob_India_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                                     graph_lim=150)

Mon_equity_fu_mob_India_title=Mon_equity_fu_mob_India_chart[2][[1]]
Mon_equity_fu_mob_India_chart=Mon_equity_fu_mob_India_chart[1][[1]]
Mon_equity_fu_mob_India_src=Mon_equity_funds_src

min_factor=1



## ------------------------------------------------
#Domestic Investments
DII_inv_in_India=data_query_clk_pg(id=1418351)
DII_inv_in_India_title=DII_inv_in_India[2][[1]]
DII_inv_in_India_src=DII_inv_in_India[3][[1]]


older_niif=data_query_clk_pg(1418351,150)[1][[1]]
older_niif=mon_year_df_creator(older_niif,keep_col=c("Relevant_Date","Value"),Sum_date = TRUE)

older_niif$Value=older_niif$Value/10^9
colnames(older_niif)=c("Relevant_Date","DII investments")
DII_inv_in_India=older_niif




## ------------------------------------------------
data_s=list(DII_inv_in_India)

max_overlap=30
my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

num_brek=5
max_pri_y=3000
min_pri_y=2000
bar_thick=60
h_just=0.25
v_just=0.60
h_just_line=0
v_just_line=0.60

DII_invest_India_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                              graph_lim=470,negative=TRUE,YTD=TRUE)

Annual_DII_invest_India_title=DII_invest_India_chart[2][[1]]
Annual_DII_invest_India_chart=DII_invest_India_chart[1][[1]]
Annual_DII_invest_India_src=DII_inv_in_India_src
bar_thick=8
Annual_DII_invest_India_chart


## ------------------------------------------------
#Domestic Investments
DII_inv_in_India=data_query_clk_pg(id=1418351)
DII_inv_in_India_title=DII_inv_in_India[2][[1]]
DII_inv_in_India_src=DII_inv_in_India[3][[1]]


older_niif=data_query_clk_pg(1418351,150)[1][[1]]
older_niif$Value=older_niif$Value/10^9
colnames(older_niif)=c("Relevant_Date","DII investments")
DII_inv_in_India=older_niif




## ------------------------------------------------
data_s=list(DII_inv_in_India)

max_overlap=30
my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

num_brek=5
max_pri_y=600
min_pri_y=600
h_just=0.25
v_just=0.60
h_just_line=0
v_just_line=0.60

DII_invest_India_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                              graph_lim=100,negative=TRUE,YTD=FALSE)

DII_invest_India_title=DII_invest_India_chart[2][[1]]
DII_invest_India_chart=DII_invest_India_chart[1][[1]]
DII_invest_India_src=DII_inv_in_India_src
bar_thick=8
DII_invest_India_chart


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(net_inflo_into_mf)
h_just=0.60
v_just=0.60

num_brek=8
max_pri_y=1200
min_pri_y=400

x_axis_interval="24 month"


my_chart_col=c("GRAY 88","GOLDEN ROD 1")
my_legends_col=c("GRAY 88","GOLDEN ROD 1") 

net_inflows_mf_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",
                                            graph_lim=100,negative=TRUE,
                                            DATE_HEADER = FALSE)


net_inflows_mf_title=net_inflows_mf_chart[2][[1]]
net_inflows_mf_chart=net_inflows_mf_chart[1][[1]]
net_inflows_mf_src=net_inflo_into_mf_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(Mon_gen_frm_renew)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=5
max_pri_y=25
min_pri_y=0

my_chart_col=c("GRAY 48","GOLDEN ROD 1","BISQUE1","TAN 1","DARK ORANGE 2","#BF6E00")
my_legends_col=c("GRAY 48","GOLDEN ROD 1","BISQUE1","TAN 1","DARK ORANGE 2","#BF6E00") 

Mon_gen_frm_renew_chart=stacked_bar_chart_niif(data_s,
                                               x_axis_interval="24 month",data_unit="",
                                               graph_lim=90,negative=FALSE)

Mon_gen_frm_renew_title=Mon_gen_frm_renew_chart[2][[1]]
Mon_gen_frm_renew_chart=Mon_gen_frm_renew_chart[1][[1]]
Mon_gen_frm_renew_src=Mon_gen_frm_renew_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(Aif_Fun_rai_1,Aif_Fun_rai_2,Aif_Fun_rai_3)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15
bar_thick=40
num_brek=8
max_pri_y=300
min_pri_y=50


my_chart_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")
my_legends_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")


Qtr_AIF_net_fundrai_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",
                                                 data_unit="",graph_lim=90,negative=TRUE)

Qtr_AIF_net_fundrai_title=Qtr_AIF_net_fundrai_chart[2][[1]]
Qtr_AIF_net_fundrai_chart=Qtr_AIF_net_fundrai_chart[1][[1]]
Qtr_AIF_net_fundrai_src=Aif_Fun_rai_1_src


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(Infra_fun,social_fun,venture_fun,sme_fun)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=8
max_pri_y=40
min_pri_y=50

my_chart_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")
my_legends_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")

Qtr_AIF_CI_net_fundrai_chart=stacked_bar_chart_niif(data_s,
                                                    x_axis_interval="24 month",data_unit="",
                                                    graph_lim=90,negative=TRUE)

Qtr_AIF_CI_net_fundrai_title=Qtr_AIF_CI_net_fundrai_chart[2][[1]]
Qtr_AIF_CI_net_fundrai_chart=Qtr_AIF_CI_net_fundrai_chart[1][[1]]
Qtr_AIF_CI_net_fundrai_src=Infra_fun_src
Qtr_AIF_CI_net_fundrai_chart



## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(Aif_invest_1,Aif_invest_2,Aif_invest_3)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=8
max_pri_y=300
min_pri_y=50

my_chart_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")
my_legends_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")

Qtr_AIF_net_invest_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",data_unit="",graph_lim=90,negative=TRUE)

Qtr_AIF_net_invest_title=Qtr_AIF_net_invest_chart[2][[1]]
Qtr_AIF_net_invest_chart=Qtr_AIF_net_invest_chart[1][[1]]
Qtr_AIF_net_invest_src=Aif_invest_1_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(Infra_inv,social_inv,venture_inv,sme_inv)

h_just=0
v_just=0.65
h_just_line=1.15
v_just_line=1.15

num_brek=5
max_pri_y=20
min_pri_y=40

my_chart_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")
my_legends_col=c("GRAY 88","TAN 1","GOLDEN ROD 1","DARK ORANGE 2","#BF6E00")

Qtr_AIF_CI_net_inv_chart=stacked_bar_chart_niif(data_s,
                                        x_axis_interval="24 month",
                                        data_unit="",graph_lim=90,negative=TRUE,round_integer = TRUE)

Qtr_AIF_CI_net_inv_title=Qtr_AIF_CI_net_inv_chart[2][[1]]
Qtr_AIF_CI_net_inv_chart=Qtr_AIF_CI_net_inv_chart[1][[1]]
Qtr_AIF_CI_net_inv_src=Infra_fun_src
bar_thick=8



## ------------------------------------------------
#Road Construction in India
Road_Con=data_query_clk_pg(id=318872)
Road_Con_title=tolower(Road_Con[2][[1]])
Road_Con_src=Road_Con[3][[1]]
Road_Con=Road_Con[1][[1]]
colnames(Road_Con)=c("Relevant_Date","Roads constructed (Km)","Roads awarded (Km)")


## ------------------------------------------------
data_s=list(Road_Con)
bar_thick=20

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
                        graph_lim=30,negative=FALSE,SIDE_BAR=TRUE)

Mon_road_con_India_title=Mon_road_con_India_chart[2][[1]]
Mon_road_con_India_chart=Mon_road_con_India_chart[1][[1]]
Mon_road_con_India_src=Road_Con_src


## ------------------------------------------------
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




## ------------------------------------------------
data_s=GVA_growth

key_spacing=0.10
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
                                                legend_key_width=0.27,Position="center",CPI_reference=FALSE,
                                                round_integer=FALSE)

Qtr_real_GVA_com_gr_title=Qtr_real_GVA_com_gr_chart[2][[1]]
Qtr_real_GVA_com_gr_chart=Qtr_real_GVA_com_gr_chart[1][[1]]
Qtr_real_GVA_com_gr_src=GVA_agri_src



## ------------------------------------------------
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



## ------------------------------------------------
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
                                                PMI_reference=FALSE,BSE_Index=FALSE,legend_key_width=0.27,Position="center")

Qtr_real_GDP_seg_gr_title=Qtr_real_GDP_seg_gr_chart[2][[1]]
Qtr_real_GDP_seg_gr_chart=Qtr_real_GDP_seg_gr_chart[1][[1]]
Qtr_real_GDP_seg_gr_src=GDP_private_src


## ------------------------------------------------
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


## ------------------------------------------------
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
                                           graph_lim=30,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE)

Quarterly_PLFS_title=Quarterly_PLFS_chart[2][[1]]
Quarterly_PLFS_chart=Quarterly_PLFS_chart[1][[1]]
Quarterly_PLFS_src=partici_rate_src


## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
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
## Position="center"
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


## ------------------------------------------------
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


## ------------------------------------------------
data_s=Mon_Trade_Com

h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=80
min_pri_y=40
neagative=TRUE
n_row=1
n_col=3

Position="center"
my_legends_col=c("Merchandize exports"="GRAY 48",
                 "Merchandize imports"="GOLDEN ROD 1" ,
                 "Trade balance"="DARK ORANGE 2")
my_line_type=c("Merchandize exports" ="solid","Merchandize imports"="solid","Trade balance"="solid")

Mon_Trade_Compo_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",graph_lim =90)

Mon_Trade_Compo_title=Mon_Trade_Compo_chart[2][[1]]
Mon_Trade_Compo_chart=Mon_Trade_Compo_chart[1][[1]]
Mon_Trade_Compo_src=Mon_Trade_Com_src



## ------------------------------------------------
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



## ------------------------------------------------
data_s=Mon_services

h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=30
min_pri_y=0
neagative=TRUE
n_row=1
n_col=3

Position="center"
my_legends_col=c("Services exports"="GRAY 48", "Services imports"="GOLDEN ROD 1" ,
                 "Services balance"="DARK ORANGE 2")
my_line_type=c("Services exports" ="solid","Services imports"="solid","Services balance"="solid")


Mon_services_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",graph_lim=90)

Mon_services_title=Mon_services_chart[2][[1]]
Mon_services_chart=Mon_services_chart[1][[1]]
Mon_services_src=Services_exp_src



## ------------------------------------------------
#Major Sectoral Indices
Mjr_Sect_Indi= data_query_clk_pg(id=1384047)
Mjr_Sect_Indi_title=Mjr_Sect_Indi[2][[1]]
Mjr_Sect_Indi_src=Mjr_Sect_Indi[3][[1]]
Mjr_Sect_Indi=Mjr_Sect_Indi[1][[1]]

colnames(Mjr_Sect_Indi)=c("Relevant_Date","Bank","Power","FMCG","Health care","Capital Goods")
Mjr_Sect_Indi=Mjr_Sect_Indi[,c("Relevant_Date","Bank","Capital Goods","FMCG","Health care","Power")]





## ------------------------------------------------
data_s=list(Mjr_Sect_Indi)
# 
h_just_line=0.55
v_just_line=0.55

num_brek=5
max_pri_y=50000
min_pri_y=0

key_spacing=0.50

n_row=1
n_col=5

my_legends_col=c("Bank"="black","Capital Goods"="GOLDEN ROD 1","FMCG"="BURLYWOOD 1",
                 "Health care"="GRAY 48","Power"="#BF6E00")

my_line_type=c("Bank"="solid","Capital Goods"="solid","FMCG"="solid",
              "Health care"="solid","Power"="solid")

BSE_sectoral_indices_chart=multi_line_chart_niif(data_s,
                                                 x_axis_interval="24 month",
                                                 graph_lim=20,negative=TRUE,PMI_reference=FALSE,
                                                 BSE_Index=FALSE,legend_key_width=1)

BSE_sectoral_indices_title=BSE_sectoral_indices_chart[2][[1]]
BSE_sectoral_indices_chart=BSE_sectoral_indices_chart[1][[1]]

BSE_sectoral_indices_src=Mjr_Sect_Indi_src
key_spacing=0.10


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(IIP_gr,Core_industry_gr)

n_col=6
n_row=1

my_legends_col=c("IIP growth"="GRAY 48","Core industry growth"="GOLDEN ROD 1")
my_line_type=c("IIP growth"="solid","Core industry growth"="solid")

num_brek=5
max_pri_y=10
min_pri_y=10

h_just_line=0
v_just_line=0.75
legend_key_size=0

Index_of_Ind_Prod_IIP_chart=multi_line_chart_niif(data_s,
                                              x_axis_interval="24 month",graph_lim=120,
                                              negative=TRUE,PMI_reference=FALSE,
                                              BSE_Index=FALSE,legend_key_width=0.27)

Index_of_Ind_Prod_IIP_title=Index_of_Ind_Prod_IIP_chart[2][[1]]
Index_of_Ind_Prod_IIP_title=paste0(Index_of_Ind_Prod_IIP_title)
Index_of_Ind_Prod_IIP_chart=Index_of_Ind_Prod_IIP_chart[1][[1]]
Index_of_Ind_Prod_IIP_src=Core_industry_gr_src




## ------------------------------------------------
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
Retail_Infl=data_query_clk_pg(id=1445961)
Retail_Infl=Retail_Infl[1][[1]]
Retail_Infl=subset(Retail_Infl, Inflation!= Inf)

cpi_Retail_Infl=Retail_Infl[,c("Relevant_Date","Inflation")]
older_niif=data_query_clk_pg(1384194,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Inflation")
older_niif=older_niif[older_niif$Relevant_Date<min(cpi_Retail_Infl$Relevant_Date),]

cpi_Retail_Infl=rbind(older_niif,cpi_Retail_Infl)
colnames(cpi_Retail_Infl)=c("Relevant_Date","CPI")
cpi_Retail_Infl=list(cpi_Retail_Infl,Retail_Infl_wt_Food_Fu_Light)


## ------------------------------------------------
data_s=cpi_Retail_Infl
key_spacing=0
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
                                               Position="center",CPI_reference=TRUE,round_integer=TRUE)

CPI_Infl_Tar_range_title=CPI_Infl_Tar_range_chart[2][[1]]
CPI_Infl_Tar_range_chart=CPI_Infl_Tar_range_chart[1][[1]]
CPI_Infl_Tar_range_src=Retail_Infl_src




## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
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
## #                                              legend_key_width=0.27,Position="center",CPI_reference=FALSE,
## #                                              round_integer=FALSE,Repo=TRUE)
## #
## #
## # India_G_sec_bill_title=India_G_sec_bill_chart[2][[1]]
## # India_G_sec_bill_chart=India_G_sec_bill_chart[1][[1]]
## # India_G_sec_bill_src=India_G_sec_bench_trea_src
## 
## 


## ------------------------------------------------
#Monthly PMI
PMI_mnf_Services= data_query_clk_pg(725990)
PMI_mnf_Services_title=PMI_mnf_Services[2][[1]]
PMI_mnf_Services_src=PMI_mnf_Services[3][[1]]
PMI_mnf_Services=PMI_mnf_Services[1][[1]]
PMI_mnf_Services=PMI_mnf_Services[!duplicated(PMI_mnf_Services),]
colnames(PMI_mnf_Services) = c("Relevant_Date","PMI manufacturing","PMI services")



## ------------------------------------------------
data_s=list(PMI_mnf_Services)

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


## ------------------------------------------------
#Monthly PMI
PMI_mjr_economies= data_query_clk_pg(id=725961)
PMI_mjr_economies_title=PMI_mjr_economies[2][[1]]
PMI_mjr_economies_src=PMI_mjr_economies[3][[1]]
PMI_mjr_economies=PMI_mjr_economies[1][[1]]



## ------------------------------------------------
data_s=list(PMI_mjr_economies)

legend_key_width=0.75
key_spacing=1

max_overlaps =30
num_brek=8
max_pri_y=70
min_pri_y=35
# 

h_just_line=0
v_just_line=0.75

n_col=8
n_row=1



my_chart_col=c("USA"="GRAY 48","UK"="GOLDEN ROD 1","India"="#0541B4","Brazil"="DARK ORANGE 2",
               "China"="TAN 1","Japan"="GRAY 88","France"="BURLYWOOD 1","Germany"="#BF6E00")
               

my_legends_col=c("USA"="GRAY 48","UK"="GOLDEN ROD 1","India"="#0541B4","Brazil"="DARK ORANGE 2",
               "China"="TAN 1","Japan"="GRAY 88","France"="BURLYWOOD 1","Germany"="#BF6E00")

my_line_type=c("USA"="solid","UK"="solid","India"="dashed","Brazil"="solid","China"="solid",
               "Japan"="solid","France"="solid","Germany"="solid")

PMI_mjr_economies_9_chart=multi_line_chart_niif(data_s,
                                                x_axis_interval="6 month",
                                                graph_lim=90,negative=TRUE,
                                                PMI_reference=TRUE,
                                                BSE_Index=FALSE,legend_key_width=1,
                                                Position="center",
                                                CPI_reference=FALSE,
                                                round_integer=FALSE)



PMI_mjr_economies_9_title=PMI_mjr_economies_9_chart[2][[1]]
PMI_mjr_economies_9_chart=PMI_mjr_economies_9_chart[1][[1]]
PMI_mjr_economies_9_src=PMI_mjr_economies_src
draw_key_line=draw_key_rect
Position="center"



## ------------------------------------------------
#return yoy
# BSE_SENSEX_Index
BSE_SENSEX_Index= data_query_clk_pg(id=318893)
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




## ------------------------------------------------
data_s=Global_equity

# legend_key_width=0.50

max_overlaps =30
h_just_line=0
v_just_line=0.75

n_col=5
n_row=2
key_spacing=1

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
                                                            graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                                            BSE_Index=TRUE,legend_key_width=0.50,Position="left",
                                                            CPI_reference=FALSE,round_integer=FALSE)

Mon_Nft_Sen_gbl_indices_title=Mon_Nft_Sen_gbl_indices_chart[2][[1]]
Mon_Nft_Sen_gbl_indices_chart=Mon_Nft_Sen_gbl_indices_chart[1][[1]]
Mon_Nft_Sen_gbl_indices_src=BSE_SENSEX_Index_src



## ----include=FALSE-------------------------------
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



## ------------------------------------------------
data_s=REITs_InvITs

n_row=2
n_col=4
legend_key_width=0.27
key_spacing=1

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
                                                    BSE_Index=FALSE,legend_key_width=1,Position="left",DATE_HEADER=FALSE)

Daily_market_pri_REITs_InvITs_title=Daily_market_pri_REITs_InvITs_chart[2][[1]]
Daily_market_pri_REITs_InvITs_chart=Daily_market_pri_REITs_InvITs_chart[1][[1]]
Daily_market_pri_REITs_InvITs_src=IRB_InvIT_src  


## ------------------------------------------------
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


## ------------------------------------------------
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


Mon_com_index_World_Bank_chart=multi_line_chart_niif(data_s,x_axis_interval="12 month",graph_lim=60,negative=TRUE,PMI_reference=FALSE,BSE_Index=FALSE,legend_key_width=1,Position="center")

Mon_com_index_World_Bank_title=Mon_com_index_World_Bank_chart[2][[1]]
Mon_com_index_World_Bank_chart=Mon_com_index_World_Bank_chart[1][[1]]

Mon_com_index_World_Bank_src=Mon_Bev_index_src
Mon_com_index_World_Bank_chart



## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(bi_real_gva_gdp)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
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
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

bi_real_gva_gdp_title=bi_real_gva_gdp_chart[2][[1]]
bi_real_gva_gdp_chart=bi_real_gva_gdp_chart[1][[1]]
bi_real_gva_gdp_title=strsplit(str_replace(bi_real_gva_gdp_title," ","-"),"-")[[1]][3]
# bi_real_gva_gdp_title=strsplit(bi_real_gva_gdp_title,"-")[[1]][2]

bi_real_gva_gdp_src=bi_real_gdp_src


## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(bi_cpi_core)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
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
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

bi_cpi_cpicore_title=bi_cpi_cpicore_chart[2][[1]]
bi_cpi_cpicore_chart=bi_cpi_cpicore_chart[1][[1]]

bi_cpi_cpicore_src=bi_cpi_src
bi_cpi_cpicore_title=strsplit(str_replace(bi_cpi_cpicore_title," ","-"),"-")[[1]][3]
# bi_cpi_cpicore_title=strsplit(bi_cpi_cpicore_title,"-")[[1]][2]
bi_cpi_cpicore_chart




## ------------------------------------------------
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



## ------------------------------------------------
data_s=list(bi_wpi_wpicore)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
max_overlaps =10
# 


h_just_line=0
v_just_line=0.60

num_brek=6
max_pri_y=10
min_pri_y=4

my_legends_col=c("WPI"="GRAY 48","WPI-Core"="GOLDEN ROD 1") 
my_line_type=c("WPI"="solid","WPI-Core"="solid")

bi_wpi_wpicore_chart=multi_line_chart_niif(data_s,x_axis_interval="2 month",
                                         round_integer=TRUE,
                                         graph_lim=30,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

bi_wpi_wpicore_title=bi_wpi_wpicore_chart[2][[1]]
bi_wpi_wpicore_chart=bi_wpi_wpicore_chart[1][[1]]

bi_wpi_wpicore_src=bi_wpi_src
bi_wpi_wpicore_title=strsplit(str_replace(bi_wpi_wpicore_title," ","-"),"-")[[1]][3]
# bi_wpi_wpicore_title=strsplit(bi_wpi_wpicore_title,"-")[[1]][2]
bi_wpi_wpicore_chart



## ------------------------------------------------
#Export Value growth
Qtr_mer_exp_val= data_query_clk_pg(1682047)
Qtr_mer_exp_val_title=Qtr_mer_exp_val[2][[1]]
Qtr_mer_exp_val_src=Qtr_mer_exp_val[3][[1]]
Qtr_mer_exp_val=Qtr_mer_exp_val[1][[1]]
Qtr_mer_exp_val=mon_qtr_df_creator(Qtr_mer_exp_val,keep_col =c("Relevant_Date","growth"))
names(Qtr_mer_exp_val)[2]="Value growth (% yoy)"


#Export Volume growth
Qtr_mer_exp_vol= data_query_clk_pg(1681910)[1][[1]]
Qtr_mer_exp_vol=mon_qtr_df_creator(Qtr_mer_exp_vol,keep_col =c("Relevant_Date","growth"))


Qtr_mer_exp_vol=Qtr_mer_exp_vol[Qtr_mer_exp_vol$Relevant_Date>=default_start_date,]
names(Qtr_mer_exp_vol)[2]="Volume growth (% yoy)"


d1=data.frame(date1=c(max(Qtr_mer_exp_val$Relevant_Date),max(Qtr_mer_exp_vol$Relevant_Date)))
common_min=min(as.Date(d1$date1))
Qtr_mer_exp_vol=Qtr_mer_exp_vol[Qtr_mer_exp_vol$Relevant_Date<=common_min,]
Qtr_mer_exp_val=Qtr_mer_exp_val[Qtr_mer_exp_val$Relevant_Date<=common_min,]
Qtr_mer_exp_val=Qtr_mer_exp_val[Qtr_mer_exp_val$Relevant_Date>=min(Qtr_mer_exp_vol$Relevant_Date),]

Qtr_exp_growth=cbind(Qtr_mer_exp_vol,Qtr_mer_exp_val)



## ------------------------------------------------
data_s=list(Qtr_exp_growth)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
max_overlaps =10
# 
h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=120
min_pri_y=60

my_legends_col=c("Volume growth (% yoy)"="GRAY 48","Value growth (% yoy)"="GOLDEN ROD 1") 
my_line_type=c("Volume growth (% yoy)"="solid","Value growth (% yoy)"="solid")

Qtr_exp_gr_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                       graph_lim=60,negative=TRUE,PMI_reference=FALSE,
                                       BSE_Index=FALSE,legend_key_width=1,Position="center")


Qtr_exp_gr_title=Qtr_exp_gr_chart[2][[1]]
Qtr_exp_gr_chart=Qtr_exp_gr_chart[1][[1]]
# Qtr_exp_gr_src=Qtr_mer_exp_val_src

#HARD CODE :: Different Sources
Qtr_exp_gr_src="Source: Thurro, MOCI, UNCTAD, NIIF Research"
Qtr_exp_gr_chart


## ------------------------------------------------
#Import Value growth
Qtr_mer_imp_val= data_query_clk_pg(1682048)
Qtr_mer_imp_val_title=Qtr_mer_imp_val[2][[1]]
Qtr_mer_imp_val_src=Qtr_mer_imp_val[3][[1]]
Qtr_mer_imp_val=Qtr_mer_imp_val[1][[1]]

Qtr_mer_imp_val=mon_qtr_df_creator(Qtr_mer_imp_val,keep_col =c("Relevant_Date","growth"))
names(Qtr_mer_imp_val)[2]="Value growth (% yoy)"


#Import Volume growth
Qtr_mer_imp_vol= data_query_clk_pg(1681909)[1][[1]]
Qtr_mer_imp_vol=mon_qtr_df_creator(Qtr_mer_imp_vol,keep_col =c("Relevant_Date","growth"))

Qtr_mer_imp_vol=Qtr_mer_imp_vol[Qtr_mer_imp_vol$Relevant_Date>=default_start_date,]
names(Qtr_mer_imp_vol)[2]="Volume growth (% yoy)"

d1=data.frame(date1=c(max(Qtr_mer_exp_val$Relevant_Date),max(Qtr_mer_exp_vol$Relevant_Date)))
common_min=min(as.Date(d1$date1))
Qtr_mer_imp_vol=Qtr_mer_imp_vol[Qtr_mer_imp_vol$Relevant_Date<=common_min,]
Qtr_mer_imp_val=Qtr_mer_imp_val[Qtr_mer_imp_val$Relevant_Date<=common_min,]
Qtr_mer_imp_val=Qtr_mer_imp_val[Qtr_mer_imp_val$Relevant_Date>=min(Qtr_mer_imp_vol$Relevant_Date),]
Qtr_imp_growth=cbind(Qtr_mer_imp_vol,Qtr_mer_imp_val)



## ------------------------------------------------
data_s=list(Qtr_imp_growth)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
max_overlaps =10
# 
h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=120
min_pri_y=90

my_legends_col=c("Volume growth (% yoy)"="GRAY 48","Value growth (% yoy)"="GOLDEN ROD 1") 
my_line_type=c("Volume growth (% yoy)"="solid","Value growth (% yoy)"="solid")

Qtr_imp_gr_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                            graph_lim=60,negative=TRUE,PMI_reference=FALSE,
                            BSE_Index=FALSE,legend_key_width=1,Position="center")


Qtr_imp_gr_title=Qtr_imp_gr_chart[2][[1]]
Qtr_imp_gr_chart=Qtr_imp_gr_chart[1][[1]]
# Qtr_imp_gr_src=Qtr_mer_imp_val_src

#HARD CODE::Multiple Source
Qtr_imp_gr_src="Source: Thurro, MOCI, UNCTAD, NIIF Research"
Qtr_imp_gr_chart



## ------------------------------------------------
Qtr_bus_exp= data_query_clk_pg(1623559,exception=TRUE)
Qtr_bus_exp_title=Qtr_bus_exp[2][[1]]
Qtr_bus_exp_src=Qtr_bus_exp[3][[1]]
Qtr_bus_exp=Qtr_bus_exp[1][[1]]
colnames(Qtr_bus_exp) = c("Relevant_Date","Expectations")
Qtr_bus_exp


## ------------------------------------------------
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


## ------------------------------------------------

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



## ------------------------------------------------
data_s=list(Qtr_bus_cost)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.10
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
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

Qtr_bus_cost_title=Qtr_bus_cost_chart[2][[1]]
Qtr_bus_cost_chart=Qtr_bus_cost_chart[1][[1]]

Qtr_bus_cost_src=Qtr_bus_exp_src
Qtr_bus_cost_chart



## ------------------------------------------------
#walr on outstanding rupee loans
Qtr_walr_out= data_query_clk_pg(1664022)
Qtr_walr_out_src=Qtr_walr_out[3][[1]]
Qtr_walr_out_loan=Qtr_walr_out[1][[1]]
colnames(Qtr_walr_out_loan) = c("Relevant_Date","On outstanding loans")

#HARD CODE:: Growth chart starts from Sep-2014
Qtr_walr_out_loan=Qtr_walr_out_loan[Qtr_walr_out_loan$Relevant_Date>='2014-09-30',]

#walr on fresh rupee loans sanctioned
Qtr_walr_fresh_loan= data_query_clk_pg(1664023)[1][[1]]
colnames(Qtr_walr_fresh_loan) = c("Relevant_Date","On fresh loans")
Qtr_walr=cbind(Qtr_walr_out_loan,Qtr_walr_fresh_loan)



## ------------------------------------------------
data_s=list(Qtr_walr)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
max_overlaps =10
# 


h_just_line=0
v_just_line=0.60

num_brek=9
max_pri_y=14
min_pri_y=0

my_legends_col=c("On outstanding loans"="GRAY 48","On fresh loans"="GOLDEN ROD 1") 
my_line_type=c("On outstanding loans"="solid","On fresh loans"="solid")

Qtr_walr_chart=multi_line_chart_niif(data_s,x_axis_interval="24 month",
                                         graph_lim=90,negative=TRUE,PMI_reference=FALSE,
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

Qtr_walr_title=Qtr_walr_chart[2][[1]]
Qtr_walr_chart=Qtr_walr_chart[1][[1]]
Qtr_walr_src=Qtr_walr_out_src


## ------------------------------------------------

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
                   


## ------------------------------------------------
data_s=list(Qtr_npa)

n_row=1
n_col=6

legend_key_width=0.27
key_spacing=0.25
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
                                         BSE_Index=FALSE,legend_key_width=1,Position="center")

Qtr_gross_npa_title=Qtr_gross_npa_chart[2][[1]]
Qtr_gross_npa_chart=Qtr_gross_npa_chart[1][[1]]
Qtr_gross_npa_src=scb_npa_src



## ------------------------------------------------
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


## ------------------------------------------------
data_s=new_age_companies

num_brek=6
max_overlaps =30

h_just_line=0
v_just_line=0.60
key_spacing=0.40

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
                                                      BSE_Index=TRUE,legend_key_width=0.40,Position="left")

market_prices_new_age_com_title=market_prices_new_age_com_chart[2][[1]]
market_prices_new_age_com_chart=market_prices_new_age_com_chart[1][[1]]
market_prices_new_age_com_src= Zomato_src



## ------------------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)
          
qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall, 
DATE_FORMAT((Relevant_Date), '%Y-%m-01') AS
Start_Date,LAST_DAY(Relevant_Date) as Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA 
where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA 
where  Relevant_Date<=(select LAST_DAY(DATE_SUB(MAX(Relevant_Date),INTERVAL 1 MONTH)) as Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_MTD_PIT_DATA)) group by State_Clean;"

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


## ------------------------------------------------
data_s=Monthly_Rainfall
my_legends_col=c('Large excess (60% or more)'="#595959",
               'Excess (20% to 59%)'="#979797",
               'Normal (-19% to 19%)'="#D9D9D9",
               'Deficient (-20% to -59%)'="TAN 1",
               'Large deficient (-99% to -60%)'="#BF6E00")


n_row=3
legend_key_width=0.20
key_spacing=0.05

Monthly_Rainfall_map_14_chart=map_rainfall_niif(data_s)


Monthly_Rainfall_map_14_title=Monthly_Rainfall_map_14_chart[2][[1]]
Monthly_Rainfall_map_14_chart=Monthly_Rainfall_map_14_chart[1][[1]]

#HARD CODE::Long source not getting enough space in PPT
Monthly_Rainfall_map_14_src="Source: Thurro, India Meteorological Department, NIIF Research"


## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
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


## ------------------------------------------------
mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)
          
qstring="select State_Clean, avg(Departure_Pct) as Departure_Pct, case when Departure_Pct >= 60 then 'Large excess (60% or more)' when Departure_Pct <= 59 and Departure_Pct >= 20 then 'Excess (20% to 59%)' when Departure_Pct <= 19 and
Departure_Pct >= -19 then 'Normal (-19% to 19%)' when Departure_Pct <= -20 and Departure_Pct >= -59 then 'Deficient (-20% to -59%)' else 'Large deficient (-99% to -60%)' end as Rainfall,DATE_FORMAT(NOW(), CONCAT('%Y', '-06-01')) AS
Start_Date, Relevant_Date from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA 
where Relevant_Date in (select max(Relevant_Date) from IMD_STATE_WISE_RAINFALL_DAILY_YTD_PIT_DATA where Relevant_Date<=DATE_FORMAT(NOW(), CONCAT('%Y', '-09-30'))) group by State_Clean;"

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



## ------------------------------------------------
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


## ----eval=FALSE, include=FALSE-------------------
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


## ----eval=FALSE, include=FALSE-------------------
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


## ------------------------------------------------
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
}


Weekly_rainfall=subset(Weekly_rainfall,select =-c(year))
Weekly_rainfall=melt(Weekly_rainfall,id=c("Relevant_Date"))
Weekly_rainfall=Weekly_rainfall %>%drop_na("value")
#HARD_CODE:: Since we are not showing 2018 data points
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$Relevant_Date>='2020-01-01',]

Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="2018",]
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="2019",]
Weekly_rainfall=Weekly_rainfall[Weekly_rainfall$variable!="Period",]



## ------------------------------------------------
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
key_spacing=0.15

max_pri_y=100
min_pri_y=0

h_just_line=0
v_just_line=0.75

Weekly_rainfall_13_chart=multi_line_chart_rainfall_niif(data_s,num_brek=6)
                                                       
Weekly_rainfall_13_title=Weekly_rainfall_13_chart[2][[1]]
Weekly_rainfall_13_chart=Weekly_rainfall_13_chart[1][[1]]
##HARD CODE:Full for of IMD required
Weekly_rainfall_13_src="Source: Thurro, India Meteorological Department, CEIC, NIIF Research"
Weekly_rainfall_13_chart


## ------------------------------------------------
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
}


cumulative_rainfall=subset(cumulative_rainfall,select =-c(year))
cumulative_rainfall=melt(cumulative_rainfall,id=c("Relevant_Date"))
cumulative_rainfall=cumulative_rainfall %>%
                drop_na("value")

#HARD_CODE:: Since we are not showing 2018 data points
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$Relevant_Date>='2020-01-01',]

cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="2018",]
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="2019",]
cumulative_rainfall=cumulative_rainfall[cumulative_rainfall$variable!="Period",]



## ------------------------------------------------
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
Position="center"
legend_key_width=0.27
key_spacing=0.10



## ------------------------------------------------
#Wholesale Price Index
Wholesale_Infla=data_query_clk_pg(id=725962)
Wholesale_Infla_title=tolower(Wholesale_Infla[2][[1]])
Wholesale_Infla_src=Wholesale_Infla[3][[1]]
Wholesale_Infla=Wholesale_Infla[1][[1]]

older_niif=data_query_clk_pg(1384195,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Inflation")
older_niif=older_niif[older_niif$Relevant_Date<min(Wholesale_Infla$Relevant_Date),]
cpi_Retail_Infl=rbind(older_niif,Wholesale_Infla)



## ------------------------------------------------
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
                      SPECIAL_LINE=FALSE)

Wholesale_Infla_title=Wholesale_Infla_chart[2][[1]]
Wholesale_Infla_chart=Wholesale_Infla_chart[1][[1]]
Wholesale_Infla_src=Wholesale_Infla_src



## ------------------------------------------------
#Coal, South African
Mon_coal_pri_SA= data_query_clk_pg(id=723139)
Mon_coal_pri_SA_title=Mon_coal_pri_SA[2][[1]]
Mon_coal_pri_SA_src=Mon_coal_pri_SA[3][[1]]
Mon_coal_pri_SA=na.omit(Mon_coal_pri_SA[1][[1]])

#Coal, Australian
Monthly_coal_prices_Australian=na.omit (data_query_clk_pg(id=723138)[1][[1]])



## ------------------------------------------------
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



## ------------------------------------------------
#World Bank Commodity | Crude oil, Brent
Mon_brent_crd_oil_pri=data_query_clk_pg(id=723146)
Mon_brent_crd_oil_pri_title=Mon_brent_crd_oil_pri[2][[1]]
Mon_brent_crd_oil_pri_src=Mon_brent_crd_oil_pri[3][[1]]
Mon_brent_crd_oil_pri=Mon_brent_crd_oil_pri[1][[1]]



## ------------------------------------------------
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



## ------------------------------------------------
#World Bank Commodity | Gold
Mon_gold_pri=data_query_clk_pg(id=723152)
Mon_gold_pri_title=Mon_gold_pri[2][[1]]
Mon_gold_price_src=Mon_gold_pri[3][[1]]
Mon_gold_pri=Mon_gold_pri[1][[1]]


## ------------------------------------------------
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



## ------------------------------------------------
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



## ------------------------------------------------
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



## ------------------------------------------------
#Volatility Index
Daily_NSE_ni_volati_Index=data_query_clk_pg(id=1384048)
Daily_NSE_ni_volati_Index_title=Daily_NSE_ni_volati_Index[2][[1]]
Daily_NSE_ni_volati_Index_src=Daily_NSE_ni_volati_Index[3][[1]]
Daily_NSE_ni_volati_Index=Daily_NSE_ni_volati_Index[1][[1]]



## ------------------------------------------------
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




## ------------------------------------------------
#Mon_inflow_into_SIPs-Monthly Inflow
Mon_inflow_into_SIPs=data_query_clk_pg(id=523075)
Mon_inflow_into_SIPs_title=Mon_inflow_into_SIPs[2][[1]]
Mon_inflow_into_SIPs_src=Mon_inflow_into_SIPs[3][[1]]
Mon_inflow_into_SIPs=Mon_inflow_into_SIPs[1][[1]]
Mon_inflow_into_SIPs$Value=Mon_inflow_into_SIPs$Value/10^9



## ------------------------------------------------
data_s=Mon_inflow_into_SIPs

h_just_line=0
v_just_line=0.60

num_brek=5
max_pri_y=150
min_pri_y=0

my_chart_col=c("GRAY 48")

  
Mon_inflows_SIPs=line_chart_niif(data_s,x_axis_interval="12 month",
                                 sales_heading="Monthly inflows into SIPs (INR billion)",
                                 graph_lim=150,
                                 SPECIAL_LINE=FALSE)

Mon_inflow_into_SIPs_38_title=Mon_inflows_SIPs[2][[1]]
Mon_inflows_SIPs=Mon_inflows_SIPs[1][[1]]
Mon_inflow_into_SIPs_38_src=Mon_inflow_into_SIPs_src




## ----eval=FALSE, include=FALSE-------------------
## #SILVER
## Silver_Spot_Pri=data_query_clk_pg(id=282838)
## Silver_Spot_Pri_title=Silver_Spot_Pri[2][[1]]
## Silver_Spot_Pri_src=Silver_Spot_Pri[3][[1]]
## Silver_Spot_Pri=Silver_Spot_Pri[1][[1]]
## Silver_Spot_Pri$Price=Silver_Spot_Pri$Price/1000


## ----eval=FALSE, include=FALSE-------------------
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


## ------------------------------------------------
#Credit Deposit Difference
CD_diff= data_query_clk_pg(id=1443678)
CD_diff_title=tolower(CD_diff[2][[1]])
CD_diff_src=CD_diff[3][[1]]
CD_diff=CD_diff[1][[1]]


older_niif=data_query_clk_pg(1443681,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Value")
older_niif=older_niif[older_niif$Relevant_Date<min(CD_diff$Relevant_Date),]
CD_diff=rbind(older_niif,CD_diff)
CD_diff$Value=CD_diff$Value/10^12


## ------------------------------------------------
data_s=CD_diff

h_just_line=0
v_just_line=0.75
my_chart_col=c("GOLDEN ROD 1")


num_brek=6
max_pri_y=60
min_pri_y=0

  
CD_diff_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                              sales_heading="Monthly credit-deposit difference (INR trillion)",
                              graph_lim=60,SPECIAL_LINE=FALSE)

CD_diff_title=CD_diff_chart[2][[1]]
CD_diff_chart=CD_diff_chart[1][[1]]
draw_key_line=draw_key_rect
CD_diff_src=CD_diff_src




## ------------------------------------------------
#Statutory Liquidity Ratio
SL_ratio= data_query_clk_pg(1502399)
SL_ratio_title=tolower(SL_ratio[2][[1]])
SL_ratio_src=SL_ratio[3][[1]]
SL_ratio=SL_ratio[1][[1]]



## ------------------------------------------------
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
                               graph_lim=10,SPECIAL_LINE=FALSE,Position="center",
                               legend_key_width=0.27,Reference=TRUE)

SL_ratio_title=SL_ratio_chart[2][[1]]
SL_ratio_chart=SL_ratio_chart[1][[1]]
SL_ratio_src=SL_ratio_src



## ----include=FALSE-------------------------------
#All Scheduled Commercial Banks
CD_Ratio= data_query_clk_pg(id=1443680)
CD_Ratio_title=tolower(CD_Ratio[2][[1]])
CD_Ratio_src=CD_Ratio[3][[1]]
CD_Ratio=CD_Ratio[1][[1]]

older_niif=data_query_clk_pg(1443682,150)[1][[1]]
colnames(older_niif)=c("Relevant_Date","Ratio")
older_niif=older_niif[older_niif$Relevant_Date<min(CD_Ratio$Relevant_Date),]
CD_Ratio=rbind(older_niif,CD_Ratio)



## ------------------------------------------------
data_s=CD_Ratio

h_just_line=0
v_just_line=0.75
my_chart_col=c("GRAY 48")


num_brek=6
max_pri_y=80
min_pri_y=60

CD_Ratio_chart=line_chart_niif(data_s,x_axis_interval="24 month",
                               sales_heading="Credit-to-deposit ratio (%)",
                               graph_lim=90,SPECIAL_LINE=FALSE)

CD_Ratio_title=CD_Ratio_chart[2][[1]]
CD_Ratio_chart=CD_Ratio_chart[1][[1]]
draw_key_line=draw_key_rect
CD_Ratio_src=CD_Ratio_src



## ------------------------------------------------
#Monthly Interest Rates
RBI_Repo_Reverse_Repo= data_query_clk_pg(id=725999)
RBI_Repo_Reverse_Repo_title=RBI_Repo_Reverse_Repo[2][[1]]
RBI_Repo_Reverse_Repo_src=RBI_Repo_Reverse_Repo[3][[1]]
RBI_Repo_Reverse_Repo=RBI_Repo_Reverse_Repo[1][[1]]

RBI_Repo_Rates=RBI_Repo_Reverse_Repo[,c("Relevant_Date","Total_Repo")]
RBI_Reverse_Repo_Rates=RBI_Repo_Reverse_Repo[,c("Relevant_Date","Total_Rev_Repo")]



## ------------------------------------------------
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
                                            sales_heading="Repo",
                                            graph_lim=160,SPECIAL_LINE=FALSE,DATE_HEADER=TRUE,Repo=TRUE)

RBI_Repo_Reverse_Repo_title=RBI_Repo_Reverse_Repo_chart[2][[1]]
RBI_Repo_Reverse_Repo_chart=RBI_Repo_Reverse_Repo_chart[1][[1]]
RBI_Repo_Reverse_Repo_src=RBI_Repo_Reverse_Repo_src



## ----include=FALSE-------------------------------
#Average Monthly P/E Ratio
Mon_avg_PE_ratio=data_query_clk_pg(id=318881)
Mon_avg_PE_ratio_title=Mon_avg_PE_ratio[2][[1]]
Mon_avg_PE_ratio_src=Mon_avg_PE_ratio[3][[1]]
Mon_avg_PE_ratio=Mon_avg_PE_ratio[1][[1]]


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
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



## ------------------------------------------------
#MF | Equity
qtr_mfh_t=data_query_clk_pg(id=1633862)
qtr_mfh_t_title=qtr_mfh_t[2][[1]]
qtr_mfh_t_src=qtr_mfh_t[3][[1]]
qtr_mfh_t=qtr_mfh_t[1][[1]]
qtr_mfh_t$Value=qtr_mfh_t$Value/10^12
names(qtr_mfh_t)[2]="Equity"
qtr_mfh_t=mon_qtr_df_creator(qtr_mfh_t,keep_col =c("Relevant_Date","Equity"))


## ------------------------------------------------
data_s=list(qtr_mfh_t)
h_just=0
v_just=0.60
h_just_line=1.15
v_just_line=1.15
bar_thick=40
num_brek=10
max_pri_y=30
min_pri_y=0

my_chart_col=c("GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1")

qtr_mfh_t_chart=stacked_bar_chart_niif(data_s,x_axis_interval="24 month",
                                       data_unit="",graph_lim=90,negative=FALSE)

qtr_mfh_t_title=qtr_mfh_t_chart[2][[1]]
qtr_mfh_t_chart=qtr_mfh_t_chart[1][[1]]
qtr_mfh_t_src=qtr_mfh_t_src
qtr_mfh_t_chart
bar_thick=8


## ------------------------------------------------
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



## ------------------------------------------------
data_s=Mon_net_FPI
data_g=Mon_net_FPI_gr

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2","BISQUE1")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY 32","#BF6E00")

h_just=0
v_just=0.60
h_just_line=0.25
v_just_line=0.25

num_brek=5
max_pri_y=3000
min_pri_y=2000
max_sec_y=3000


bar_thick=60
line_thick=0.50
n_row=1
n_col=5
chart_label=7

Mon_FPI_invest_India_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                       growth_heading="Net FPI",
                                       x_axis_interval="24 month",DUAL_AXIS=FALSE,
                                       data_unit='',graph_lim=470,round_integer=TRUE,
                                       Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                                       GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,YTD = TRUE)


Annual_FPI_invest_India_title=Mon_FPI_invest_India_chart[2][[1]]
Annual_FPI_invest_India_chart=Mon_FPI_invest_India_chart[1][[1]]
Annual_FPI_invest_India_src=Mon_net_equi_src
bar_thick=8
Annual_FPI_invest_India_chart


## ------------------------------------------------
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



## ------------------------------------------------
data_s=Mon_net_FPI
data_g=Mon_net_FPI_gr

my_chart_col=c("GOLDEN ROD 1","DARK ORANGE 2","BISQUE1")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2","GRAY 32","#BF6E00")

h_just=0
v_just=0.60
h_just_line=0.25
v_just_line=0.25

num_brek=5
max_pri_y=600
min_pri_y=600
max_sec_y=600


line_thick=0.50
n_row=1
n_col=5
chart_label=7

Mon_FPI_invest_India_chart=stacked_bar_line_chart_niif(data_s,data_g,
                                       growth_heading="Net FPI",
                                       x_axis_interval="24 month",DUAL_AXIS=FALSE,
                                       data_unit='',graph_lim=100,round_integer=TRUE,
                                       Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                                       GST_SPECIAL=FALSE,key_spacing=0.05,JNPT=FALSE,YTD = FALSE)


Mon_FPI_invest_India_title=Mon_FPI_invest_India_chart[2][[1]]
Mon_FPI_invest_India_chart=Mon_FPI_invest_India_chart[1][[1]]
Mon_FPI_invest_India_src=Mon_net_equi_src
bar_thick=8
Mon_FPI_invest_India_chart


## ------------------------------------------------
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



## ------------------------------------------------
data_s=trade_balance
data_g=CA_Deficit

my_chart_col=c("DARK ORANGE 2","GOLDEN ROD 1")
my_legends_col=c("GOLDEN ROD 1","DARK ORANGE 2") 

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
bar_thick=8
legend_key_width=0.27
key_spacing=0.50


CA_Deficit_chart=stacked_bar_line_chart_niif(data_s,data_g,
                           growth_heading="Current account (% GDP)",
                           x_axis_interval="12 month",
                           data_unit='',graph_lim=150,round_integer=TRUE,
                           Exception=FALSE,SIDE_BAR=FALSE,negative=TRUE,
                           GST_SPECIAL=FALSE,DUAL_AXIS = FALSE)

CA_Deficit_title=CA_Deficit_chart[2][[1]]
CA_Deficit_chart=CA_Deficit_chart[1][[1]]
CA_Deficit_src=CA_Deficit_src
CA_Deficit_chart


## ------------------------------------------------
#YEN
INR_with_yen=data_query_clk_pg(id=1288702)
INR_with_yen_title=tolower(INR_with_yen[2][[1]])
INR_with_yen_src=INR_with_yen[3][[1]]


## ------------------------------------------------
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


## ------------------------------------------------
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


## ------------------------------------------------
data_s=INR_with_all_1

key_spacing=2
h_just=0.5
v_just=1.5

num_brek=7
max_pri_y=15
min_pri_y=15

chart_label=8
side_width=0
bar_thick=0.5

my_chart_col=c("GRAY 48","DARK ORANGE 2","GOLDEN ROD 1","BURLYWOOD 1")
my_legends_col=c("GRAY 48","DARK ORANGE 2","GOLDEN ROD 1","BURLYWOOD 1")

Rupee_Appre_Depre_chart=side_bar_chart_niif_rupee(data_s,graph_lim=30,
                                                  negative=TRUE,
                                                  Position="center",
                                                  DATE_HEADER = TRUE,
                                                  pos_d=0.5,
                                                  pos_lb=0.8)

Rupee_Appre_Depre_title=Rupee_Appre_Depre_chart[2][[1]]
Rupee_Appre_Depre_chart=Rupee_Appre_Depre_chart[1][[1]]

Rupee_Appre_Depre_src=INR_with_yen_src
draw_key_line=draw_key_smooth
Rupee_Appre_Depre_chart



## ------------------------------------------------
summary1="Strong domestic demand continued to drive growth in India: automobile registrations, bank credit, passenger travel via air and rail, energy consumption have been robust over the past few months. Domestic demand along with higher private investments has driven the GDP growth in Q2FY23. Equity markets saw foreign portfolio investors become net buyers in November. However, industrial and core infrastructure growth weakened in October partially due to lesser workdays on account of festive season. Slowdown in global demand has impacted goods export, however large services trade surplus provides comfort for India's current account. Consumer inflation in November moderated as food prices softened. RBI reduced the pace of rate hikes as repo was increased by 35 bps in December – another 25-bps expected in Q4FY23."

Growth="Growth: GDP growth in Q2FY23 at 6.3% yoy driven by higher domestic demand and private investments, supply side growth driven by services sector. Industrial production and core infrastructure growth slowed in Oct as cement and coal production weakened. With monsoons going away and sun not as bright (as winters approach), conventional energy made a come back in generation proportion."

Demand="Demand: Domestic demand remained strong with higher automobile registrations, increased freight activity, higher than pre-Covid travel via rail in Oct, air travel stabilized at ~11 mn passengers in Nov. Electronic toll collection remains high at INR 46.5 bn in Nov. Banks may reduce excess SLR holdings as credit growth continued to record double-digit growth in Nov. Energy demand also picked up in Nov after a yoy fall in Oct"

Inflation="Inflation: Retail inflation moderated to 5.9% in Nov, within RBI's target range, as compared to 6.8% in Oct driven by food prices softening. Wholesale inflation moderated to 5.9% in Nov from 8.4% in Oct as food and manufactured product prices contract. RBI raised repo by 35 bps in Dec taking the repo to 6.25%. Bloomberg consensus (median) forecasts projected another 25-bps rate hike in Q4FY23"

Markets="Markets: RBI continued to withdraw surplus liquidity that has kept short-term yields elevated, above repo rate. Indian 10-year government yield remained stable even as global yields have been volatile. Indian benchmark indices performed strongly in Nov. Indian corporates mobilized more capital from debt markets in Oct than equity. Energy prices fell sharply on the expectation of a global growth slowdown"


Trade="Trade: Merchandize exports fell materially from the monthly average ~USD 38 bn this year partly driven by global demand slowing. Imports remained high reflecting high commodity prices. Sharp fall in shipping freight rates as port activity remains stable. Services trade grew sharply in Oct: services trade surplus is at an all-time high of ~USD 12 bn"


Foreign_exchange="Foreign exchange: Rupee strengthened against dollar in Nov by 1.0% over the previous month; volatility in currencies over last year is due to a stronger dollar. Forex reserves picked up in Nov to USD 550 billion (mainly due to currency revaluation) which provides seven months of healthy import cover"


Investments="Investments: Foreign portfolio investors turn net buyers in Indian equity markets in Nov: net inflows of INR 338 bn. Domestic investors continued to invest in the market. Net foreign direct investment remained strong in H1FY23 at USD 20.2 bn. Investments via alternative channels picked up strongly across categories. Funds raised by venture capital funds at an all-time high of INR 30.8 billion in Q1FY23"

Fiscal="Fiscal: GST revenue has stabilized at ~INR 1.5 tn in Nov. Gross tax revenues of the government were robust at INR 16.1 tn on a financial year-to-date basis up to Oct '22, or 58.4% of full year budget estimate. Government's fiscal deficit between Apr and Oct close to 46% of full year budget"

Economy="Economy: Economic activity in India as seen in composite PMI remained in the expansionary zone; the US,UK, Germany and France recorded a contraction. Core infrastructure maintained growth momentum in Oct. With monsoons going away and sun not as bright (as winters approach), conventional energy made a come back in generation proportion"



## ------------------------------------------------
my_ppt=read_pptx(f)
# officer::layout_summary(my_ppt)
# p1=layout_properties(my_ppt,layout ="2 CHARTS TEXTBOX")

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

#####################################################################################################
#************Quarterly real GDP growth for India,/Quarterly real GVA growth : Page 5 ****************************************

my_ppt=Two_Chart_big_comment("Growth",
                   Qtr_real_GDP_growth_chart,Qtr_real_GVA_gr_chart,
                   paste0("Quarterly real GDP growth for India, ",Qtr_real_GDP_growth_title),
                   "Real GDP grew by 6.3% in Q2FY23 as the base normalized",
                   paste0("Quarterly real GVA growth (% yoy), ",Qtr_real_GVA_gr_title),
                   "Real GVA grew by 5.6% in Q2FY23",
                   Qtr_real_GDP_growth_src,Qtr_real_GVA_gr_src,
                   "Note: 1. Real GDP growth is based on 2012 prices\n           2. 10-year average real GDP growth is 5.5%",
        
                   'Note: 10-year average real GVA growth is  5.5%',
                   "Economic growth in Q2FY23 moderated to 6.3% yoy due to base effects")

#####################################################################################################
#************Quarterly real GDP growth by segments/current prices : Page 6 ****************************************
my_ppt=Two_Chart_big_comment("Growth",
                   Qtr_real_GDP_seg_gr_chart,Qtr_real_GVA_com_gr_chart,
                   paste0("Quarterly real GDP growth by components (% yoy), ",Qtr_real_GDP_seg_gr_title),
                   "Higher investments and domestic demand drive GDP growth",
                   paste0("Quarterly real GVA growth by components (% yoy), ",Qtr_real_GVA_com_gr_title),
                   
                   "Strong growth in services sector supports GVA growth",
                   Qtr_real_GDP_seg_gr_src,Qtr_real_GVA_com_gr_src,
                   note1="",note2="",
                   "Higher domestic demand and investments, and rebound in services sector supported growth ")

#####################################################################################################
#************* Index of Industrial Production -IIP / Cement Production / Growth in Steel Production / Coal Production : Page 7 ****************************************
my_ppt=Four_Chart("Core sectors",
                   Index_of_Ind_Prod_IIP_chart,Cement_Prod_7_chart,
                   Steel_Prod_7_chart,Coal_Prod_7_chart,
           
                   paste0("IIP and output of eight core industries (% yoy), ",
                          Index_of_Ind_Prod_IIP_title),
                   "Industrial and core infrastructure growth weakens in Oct",
                   paste0("Monthly cement production, ",Cement_Prod_7_title),
                  "Cement production weakens in October",
                   paste0("Monthly crude steel production, ",Steel_Prod_7_title),
                  "Crude steel production remained stable at ~10 million tonnes",
                  
                   paste0("Monthly coal production, ",Coal_Prod_7_title),
                   "Coal production growth slows down to 3.6% yoy in October",
                   Index_of_Ind_Prod_IIP_src,Cement_Prod_7_src,
                   Steel_Prod_7_src,Coal_Prod_7_src,
           
                   c1_n="Note: IIP growth for May '20 and Apr '21 not shown due to low base effect",
                   c2_n="Note: Growth in cement production in Apr '21 not shown in the chart due to low base effect",
                   c3_n="Note: Growth in steel production in Apr '21 not shown in the chart due to low base effect",
                   c4_n="",
                   "Core infrastructure growth slowed as cement and coal production weakened in October")

#####################################################################################################
#************ No of E way Bills / Monthly PMI manufacturing and Services : Page 8 ****************************************
my_ppt=Two_Chart("Activity levels",
                 e_way_bills_chart,Mon_PMI_mnf_Ser_8_chart,
                 
                 paste0("Monthly number of e-way bills, ",e_way_bills_8_title),
                 "Number of monthly e-way bills remain high at ~77 million",
                 paste0("Monthly India PMI manufacturing and services, ",Mon_PMI_mnf_Ser_8_title),
                 "Manufacturing and services activity remain in expansionary zone",
                 e_way_bills_8_src,Mon_PMI_mnf_Ser_8_src,
                 "Logistics movement remained high; manufacturing and services activity maintained momentum",
                 
paste0("Includes all inter-state and intra-state e-way bills\ne-way bill is a document required to be carried by a person in charge of the conveyance carrying any consignment of goods of value exceeding INR 50,000 under the Goods and Services Tax Act"),
                
paste0("Purchase Managers Index (PMI) is based on a monthly survey of supply chain managers across 19 industries: a number above 50 indicates expansion and below 50 indicates contraction.\nPMI for manufacturing and services dropped sharply between Apr '20 and Oct'20 due to impact of COVID-19"))


#####################################################################################################
#** Passenger Vehicle Sales (4W) / 2W vehicle sales : Page 9 ******************************* 

my_ppt=Two_Chart("Automobile sector (1/2)",
                 passenger_vehicle_chart,TWO_W_chart,
                 
                 paste0('Monthly passenger vehicle (PV) registrations, ',passenger_vehicle_title),
                 'Passenger vehicle registrations back to pre-Covid levels',
                 paste0('Monthly two-wheeler (2W) registrations, ',TWO_W_title),
                 'Strong demand for two-wheelers over the last few months',
                 passenger_vehicle_src,TWO_W_src,
                 'Higher demand for passenger vehicles and two-wheelers in the last few months',
                 
paste0("Growth in passenger vehicles registration not shown in Jun '21 due to low base effect"),
                 
paste0("Low growth in two-wheeler registration for Apr '20 and May '20, due to the impact of Covid lockdown, not shown in the chart"))


################################################################################################
#************ Commercial Vehicle Sales / 3W Sales : Page 10 ****************************************

my_ppt=Two_Chart("Automobile sector (2/2)",
                 Com_Vehicle_chart,TW_chart,
                 
                 paste0("Monthly commercial vehicle (CV) registrations, ",Com_Vehicle_title),
                 "Sharp pickup in demand for commercial vehicles",
                 
                 paste0("Monthly three-wheeler (3W) registrations, ",TW_title),
                 "Strong growth in three-wheeler registrations in November",
                 
                 Com_Vehicle_src,TW_src,
                 "Sharp pick up in demand for three-wheelers and commercial vehicles in November",
                 
paste0("Low commercial vehicle registrations growth in Apr 20 and May '20, due to impact of Covid lockdown, not shown in the chart"),
                
paste0("Growth in three-wheeler registrations for Apr '21 and May'21, and May '22 not depicted due to low base effect of Apr '20 and May '20, and May '21 respectively"))
                  

#####################################################################################################
#************ Monthly Electricity Demand / Petroleum Consumption / Diesel Consumption and price / Petroll Consumption and price : Page 11 ****************************************
#*
my_ppt=Four_Chart("Energy and fuel",
                  Mon_Elec_Demand_chart,
                  Petroleum_Con_chart,
                  Diesel_Con_price_chart,
                  Petrol_Consumption_price_chart,
                  
                  paste0("Monthly electricity demand in India, ",Mon_Elec_Demand_title),
                  "Strong growth in electricity demand in November",
                  
                  paste0("Monthly petroleum consumption in India, ",Petroleum_Con_title),
                  "Petroleum consumption growth remained muted",
                  paste0("Monthly diesel consumption and prices, ",Diesel_Con_price_title),
                  "Diesel consumption remains high; retail prices stable since May",
                  paste0("Monthly petrol consumption and prices, ",Petrol_Consumption_price_title),
                  "Petrol consumption remains high; retail prices stable since May",
                  
                  Mon_Elec_Demand_src,
                  Petroleum_Con_src,
                  Diesel_Con_price_src,
                  Petrol_Consumption_price_src,
                  
                  c1_n="",c2_n="",c3_n="",c4_n="",
                  "Pickup in demand for energy in November; October consumption was muted"
)

#####################################################################################################
#************ Monthly Fertilizer Sales / Monthly Food Grain  Stock FCI / Water Reservoir Level / Domestic Tractor Sales : Page 12 ****************************************
my_ppt=Four_Chart("Rural India",
                  Mon_ferti_Sales_chart,
                  Mon_Food_Grain_Stock_FCI_chart,
                  Water_Reservoir_Level_chart,
                  Domestic_Tractor_reg_chart,
                  
                  paste0("Monthly fertilizer sales, ",Mon_ferti_Sales_title),
                 "Fertilizer sales picked up by 16% yoy to 5.4 million tonnes in October",

                 paste0("Monthly food grain stocks with FCI (million tonnes), ",Mon_Food_Grain_Stock_FCI_title),
                 
                  "Wheat and rice stock with FCI higher than required buffer limits",

                  paste0("Monthly live water reservoir storage, ",Water_Reservoir_Level_title),
                  "Live storage in reservoirs increased by 9.4% yoy",

                  paste0("Monthly domestic tractor registrations, ",Domestic_Tractor_reg_title),
                  "Domestic tractor registrations sharply grew in November",

                  
                  Mon_ferti_Sales_src,
                  Mon_Food_Grain_Stock_FCI_src,
                  Water_Reservoir_Level_src,
                  Domestic_Tractor_reg_src,
                 
                  c1_n="",
                  c2_n="Note: Rice is excluding paddy",
                  c3_n="",
                  c4_n="Note: 1.Growth in tractor sales in March '20 and April '21 not shown above due to base effects \n2.Excluding Telangana, Andhra Pradesh, Madhya Pradesh,Lakshadweep",
                  
                  "Rural demand supported by stable tractor registrations and strong fertilizer sales")

###########################################################################################
#************ Rainfall August / Cumulative Rainfall : Page 13
#* ****************************************

my_ppt=Two_Chart("High Frequency Indicators",
         Weekly_rainfall_13_chart,cumulative_rainfall_13_chart,

         paste0("Weekly rainfall (in mm), ",Weekly_rainfall_13_title),
         "Rainfall in May picks up, above the long period average",

         paste0("Cumulative rainfall at the end of the week (in mm),",cumulative_rainfall_13_title),
         "Cumulative rainfall this year tracking the long period average",


         Weekly_rainfall_13_src,cumulative_rainfall_13_src,

         paste0('Normal rainfall this season so far but uneven distribution across regions'),

         paste0(""),
         paste0("")
                 )

#####################################################################################################
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


#####################################################################################################
#************ Scheduled , Scheduled Commercial And Total Deposit with Growth Rate  / Scheduled , Scheduled Commercial And Total Credit with Growth Rate /Monthly credit-deposit difference/ CD Ratio  : Page 13 ***************************************
#*CD_diff_chart,
my_ppt=Four_Chart("Banks: credit and deposits",
                  SCB_Deposit_chart,
                  SCB_Credit_chart,
                  SL_ratio_chart,
                  CD_Ratio_chart,
                  
                  paste0("Monthly total bank deposits, ",SCB_Deposit_title),
                  "Bank deposits growth stable at ",
                  
                  paste0("Monthly total credit outstanding, ",SCB_Credit_title),
                  "Credit to deposit ratio remains high in ",
                  
                  paste0("Monthly statutory liquidity ratio (SLR) of banks, ",SL_ratio_title),
                  "Banks maintained excess SLR holdings despite increase in credit",
                  
                  paste0("Monthly outstanding credit-deposit ratio with SCBs, ",CD_Ratio_title),
                  "Credit to deposit ratio remains high in ",
                  
                  SCB_Deposit_src,
                  SCB_Credit_src,
                  SL_ratio_src,
                  CD_Ratio_src,
                  
                  c1_n="Note: Total deposits for scheduled commercial banks",
                  c2_n="Note: Outstanding credit for scheduled commercial banks (SCBs)",
                  c3_n="Note: Banks are required to hold 18% of their net demand and time liabilities as SLR,depicted as the black dotted line above",
                  c4_n="",
                  
                  "Strong credit growth may put pressure on banks to leverage its excess SLR holdings")
  

#####################################################################################################
#************ UPI Tran  / Cash Circulation / RTGS Tran / Credit Card Tranactions   : Page 14 ****************************************
#*
my_ppt=Four_Chart("Currency and transactions",
                  UPI_Tran_chart,
                  Cash_Cir_chart,
                  RTGS_Tran_chart,
                  Credit_Card_Tran_chart,
                  
                  paste0("Unified Payment Interface (UPI), ",UPI_Tran_title),
                  "UPI values at an all-time high in October, crosses ~INR 12 trillion",
                  paste0("Currency in circulation, ",Cash_Cir_title),
                  "Growth in currency in circulation slows down",
                  paste0("RTGS transactions, ",RTGS_Tran_title),
                  "RTGS transactions value at ~115 trillion in October",
                  paste0("Credit card transactions, ",Credit_Card_Tran_title),
                  "Transactions using credit cards remain high at ~INR 1.3 trillion",
                  
                  UPI_Tran_src,
                  Cash_Cir_src,
                  RTGS_Tran_src,
                  Credit_Card_Tran_src,
                  
                  c1_n="",c2_n="",
                  c3_n=paste0("Note: RTGS stands for Real Time Gross Settlements, that enables payments from one bank to another for a minimum amount of INR 200,000"),
                  c4_n="",
                  
paste0("Digital payments including credit cards showed robust growth; currency in circulation remain high"))

#####################################################################################################
#************ Retail Inflation- CPI  / CPI - Inflation Target range / Wholesale Inflation - WPI /  : Page 15 ****************************************

my_ppt=Two_Chart("Inflation: India",
                 CPI_Infl_Tar_range_chart, Wholesale_Infla_chart,
                 
                 paste0("Monthly consumer price inflation (% yoy), ",CPI_Infl_Tar_range_title),
                 "Retail inflation in Nov within target range; core inflation remains sticky",
                 
                 paste0("Monthly wholesale price inflation (% yoy), ",Wholesale_Infla_title),
                 "Wholesale inflation moderated to 8.4% yoy in October",
                 
                 CPI_Infl_Tar_range_src, Wholesale_Infla_src,
                 "Wholesale and retail inflation moderated in November",
                 
                 paste0("RBI in 2016 adopted flexible inflation target set at 4%, with 6% as upper bound and 2% as lower bound"))

#####################################################################################################
#************* Monthly labour participation and unemployment  / Quarterly Periodic Labor Force Survey/ Monthly enrollment numbers /Naukri jobspeak index:Page 16 ****************************************
#*
my_ppt=Four_Chart("Employment",
                  "Data not Available",
                  Quarterly_PLFS_chart,
                  Mon_enrollment_num_chart,
                  Naukri_jobspeak_index_chart,
                  
                  "Monthly urban labour participation and unemployment, FY2016-FY2023 ",
                  "Urban unemployment rate picked up in ",
                  
                  paste0("Quarterly Periodic Labor Force Survey (urban), ",Quarterly_PLFS_title),
                  "Unemployment in urban areas fell in June quarter",
                  
                  paste0("Monthly enrollment numbers, ",Mon_enrollment_num_title),
                  "Continued momentum in addition to formal workforce",
                  
                  paste0("Naukri jobspeak index, ",Naukri_jobspeak_index_title),
                  "Hiring activity picked up in November",
                  
                  "Source: Bloomberg, NIIF Research",
                  Quarterly_PLFS_src,
                  Mon_enrollment_num_src,
                  Naukri_jobspeak_index_src,
                  
                  c1_n="",c2_n="Note: The quarterly PLFS is conducted by NSSO only for the urban areas",
                  c3_n="",
                  c4_n=paste0("Note: Naukri Jobspeak Index is calculated based on job listings added Naukri.com on monthly basis. (July 2008 = 1000)"),
                  
                  "Labor force participation remained low; moderation seen in hiring activity")
#####################################################################################################
#************ Rural Wages : Page 17 ****************************************
my_ppt=Four_Chart("Sub title",
                  "Data not Available","Data not Available",
                   "Data not Available",Mon_MGNREGS_employ_data_chart,
                  "Chart Sub header1","Chart header1",
                  "Chart Sub header2","Chart header2",
                  
                  "Chart Sub header1","Chart header1",
                 
                  
                  paste0("Monthly MNREGA employment data, ",Mon_MGNREGS_employ_data_title),
                  "Jobs provided under  MNREGA back to pre-Covid level",
                  
                  "Source: Bloomberg, NIIF Research","Source: Bloomberg, NIIF Research",
                  "Source: Bloomberg, NIIF Research",Mon_MGNREGS_employ_data_src,
                  
                  c1_n="",c2_n="",c3_n="",c4_n="","Title")

#####################################################################################################
#************ Rural Wages : Page 18 ****************************************
my_ppt=Single_Chart("Fiscal Position","Data Not  Available",
                  "Monthly snapshot of central government fiscal health (INR trillion), ",
                  "Government has front-loaded capex, at 54% of budget estimate",
                  "Source: CEIC, NIIF Research",
                  
                  "Centre's fiscal deficit between April and October close to 46% of full year budget",
                  
paste0("FY2022 is the period between April 2021 and March 2022; and FY2023 is the period between April 2022 and March 2023\nYTD refers to financial year to date, i.e., from April onwards\nBE is the budget estimate for the stated financial year"))


#####################################################################################################
#************ GST Collection : Page 19 ****************************************
my_ppt=Single_Chart("Fiscal Position",
                    GST_Col_chart,
                    
                   paste0("Monthly composition of GST Revenue (INR billion), ",GST_Col_title),
                    "GST collections stabilized at ~INR 1.5 trillion",

                   GST_Col_src,
                   "Monthly government revenue from GST stabilized around INR 1.5 trillion",
                   
paste0("TTM is trailing twelve months\nGST collected for April '20 and May '20 assumed to be entirely CGST\nNominal GDP for FY2023 is the revised estimate of INR 273.1 trillion, and for FY2024 is the budget estimate of INR 301.8 trillion"))

#####################################################################################################
#**********************page:19*************************************
my_ppt=section_breaker("Markets","Comment")

#####################################################################################################
#************ RBI Repo And Reverse Repo Rates  / Surplus Liquidity : Page 20 ****************************************
my_ppt=Two_Chart("Policy rate and liquidity",
                 RBI_Repo_Reverse_Repo_chart,Surplus_liquidity_chart,
                 
                 paste0("Repo rates (%), ",RBI_Repo_Reverse_Repo_title),
                 "RBI hiked repo rates by a total of 190 bps to contain inflation", 
                 
                 paste0("Liquidity injection or absorption by RBI (INR trillion), ",Surplus_liquidity_title),
                 "Liquidity remains low in the system, at INR 0.8 trillion in October",
                 
                 RBI_Repo_Reverse_Repo_src,Surplus_liquidity_src,
                 "RBI hiked interest rates to contain inflation, also withdrawing liquidity from the system",
                 "",
paste0("Liquidity operations by RBI include repo, term-repo, long-term repo operations, open market operations, marginal standing facility, and standing liquidity facilities"))

#####################################################################################################
#*************Projections for RBI's benchmark repo rate  page:21***************************

my_ppt=Single_Chart("Forecast of policy rates","Data Not  Available",
                  "Projections for RBI's benchmark repo rate (%), ",
                  "Consensus (median) forecasts projects another 25-bps rate hike in Q4FY23",
                  "Source: Bloomberg, NIIF Research")

#####################################################################################################
#************ Monthly Trade Composition  / Servives Exp-Imp / Exports / Imports : Page 22 ****************************************
#CA_Deficit_chart,
my_ppt=Four_Chart("Balance of Payments",
                  Mon_Trade_Compo_chart,
                  Mon_services_chart,
                  Mon_Exports_chart,
                  Mon_Import_chart,
                  
                  paste0("Monthly trade composition (USD billion), ",Mon_Trade_Compo_title),
                  
                  "Exports moderated in October; trade deficit remained wide",
                  paste0("Monthly services trade (USD billion), ",Mon_services_title),

                  "Services exports remained high in October, supported higher trade surplus",
                  paste0("Monthly merchandize exports (USD billion), ",Mon_Exports_title),
                  
                  "Fall in both oil and non-oil merchandize exports",
                  paste0("Monthly merchandize imports (USD billion), ",Mon_Import_title),
                  
                  "Lower non-oil imports drove fall in total imports",
                  
                  Mon_Trade_Compo_src,
                  Mon_services_src,
                  Mon_Exports_src,
                  Mon_Import_src,
                  
                  c1_n="",c2_n="",c3_n="",c4_n="",
                  "Goods trade deficit remained wide with elevated imports and slower growth in exports")
#####################################################################################################
#************ Current Account Balance page:23 ****************************************
#*
my_ppt=Single_Chart("Foreign Exchange Markets",CA_Deficit_chart,
                    paste0("Quarterly current account balance (% of GDP), ",CA_Deficit_title),
                    "Current account deficit at 2.8% of India's GDP",
                    
                    CA_Deficit_src,
                    "India's current account deficit remained wide at 2.8% of GDP in Q1FY23")

####################################################################################################
#************ Monthly Forex Reserve  : Page 24 ****************************************

my_ppt=Two_Chart("Balance of payments",
                 Mon_Forex_Reserve_chart,"Data not Available",
                 paste0("Monthly foreign exchange reserves, ",Mon_Forex_Reserve_title),
                 "Forex reserves still provide ~7 months of import cover",
                 
                 "Chart Sub header2","Chart header 2",
                 Mon_Forex_Reserve_src,"Source2",
                 "RBI had limited liquid reserves left to manage rupee volatility and a healthy import cover",
                 
paste0("Import cover calculated on total imports (merchandize plus services)\nServices imports for September assumed to be an average of previous six months \nImports for October assumed to be the average of last six months"),
                 
paste0('"Surplus" forex buffer refers to the reserves available with RBI above the six-month import cover. Calculated as the difference between current (last available for the month) forex reserves and the six-month import cover (or "healthy forex reserves")\nGoods and services import for November assumed to be the average of last six months'))

#####################################################################################################
#************RBI remains a net buyer of US dollar : Page 25 ****************************************

my_ppt=Single_Chart("Foreign Exchange Markets",Mon_purchase_sale_dollar_chart,
                    paste0("Monthly net purchase/(sale) of USD by RBI, ",Mon_purchase_sale_dollar_title),
                    
                    "RBI remains a net seller of USD in ",
                     Mon_purchase_sale_dollar_src,
                    
                    "RBI continued to sell dollars in the market to manage volatility in Indian rupee",
                     paste0("Net purchase and sale of foreign currency in over-the-counter segment"))

#####################################################################################################
#************Rupee Appreciation and Depreciation: Page 26 ****************************************

my_ppt=Single_Chart("Foreign Exchange Markets",Rupee_Appre_Depre_chart,
                    
                    paste0("INR performance vis-a-vis major currencies (%), ",Rupee_Appre_Depre_title),
                    "INR appreciated against major currencies except USD over last year",
                    
                    Rupee_Appre_Depre_src,
paste0("Rupee strengthens against dollar in Nov; volatility in the past year due to a stronger dollar"),
                   
paste0("Numbers are annualized for periods above 1 year \nPositive return indicates appreciation and negative means depreciation"))
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



#####################################################################################################
#**********************page:27*************************************
my_ppt=Single_Chart("Debt Markets","Data Not  Available",
                  "Daily India G-sec and corporate bond yields (%), ",
                  "Borrowing cost for AA corporate bonds remain high",
                  "Source: Bloomberg, NIIF Research")

#####################################################################################################
#************ Monthly BSE Sensex performance,  / Monthly NSE Nifty performance : Page 28 ****************************************

my_ppt=Two_Chart("Equity Markets",
                 Mon_BSE_Sen_perform_chart,Mon_NSE_Nifty_perform_chart,

                 paste0("Monthly BSE Sensex performance, ",Mon_BSE_Sen_perform_title),
                 "BSE Sensex generated returns of 2.4% yoy in Oct ",
                 
                 paste0("Monthly NSE Nifty performance, ",Mon_NSE_Nifty_perform_title),
                 "NSE Nifty generated returns of 1.9% yoy in Oct ",
                 
                 Mon_BSE_Sen_perform_src,Mon_NSE_Nifty_perform_src,
                 "Indian benchmark indices recorded low year-on-year returns in October",
                 
paste0("TTM: trailing twelve months\nMonthly data for stock indices is as on end of the month\nReturns do not take into account any dividend payouts and stock buybacks, if any"),
                 
paste0("Monthly data for stock indices is as on end of the month\nReturns do not take into account any dividend payouts and stock buybacks, if any"))

#####################################################################################################
#************ Daily NSE NIFTY Volatility Index  / Monthly average P/E ratio for Nifty-50 companies : Page 29 ****************************************

my_ppt=Two_Chart("Equity Markets",
                 Daily_nft_volatility_chart,Mon_avg_PE_ratio_Nifty_50_com_chart,
                 
                 paste0("Daily NSE NIFTY Volatility Index (X), ",Daily_nft_volatility_title),
                 "Low volatility in Indian equity markets",
                 
                 paste0("Monthly average P/E ratio for Nifty-50 companies,",Mon_avg_PE_ratio_Nifty_50_com_title),
                 
                 "Equity valuations for Nifty-50 companies remain fairly priced",
                 
                 Daily_nft_volatility_src,Mon_avg_PE_ratio_Nifty_50_com_src,
                 "Nifty-50 valuation in the fair value territory; uncertainty in markets reduces",
                 
paste0("Volatility Index (VIX) represents the market's expectations of volatility over the next 30 days. India VIX is a based on the NIFTY Index Option prices\nVIX reached a peak of 83.6 on March 24, 2020 after announcement of nation-wide lockdowns due to COVID-19."),
                 
paste0("Earnings assumed for P/E ratios are trailing 4-quarter earnings"))

#####################################################################################################
#************ Monthly debt funds mobilized in India  / Monthly equity funds mobilized in India (INR billion) : Page 30 ****************************************

my_ppt=Two_Chart("Debt and equity markets: issuance",
                 Mon_debt_funds_mobi_India_chart,Mon_equity_fu_mob_India_chart,
                 
                 paste0("Monthly debt fund raising by corporate sector (INR billion),",Mon_debt_funds_mobi_India_title),
                 "Capital raised through debt markets picked up in Oct to ~INR 368 billion",
                 
                 paste0("Monthly equity fund raising by corporate sector (INR billion), ",Mon_equity_fu_mob_India_title),
                 "Capital raised via IPO and rights issues remain low at ~INR 29 billion",
                 
                 Mon_debt_funds_mobi_India_src,Mon_equity_fu_mob_India_src,
                 
paste0("Corporates mobilized more from the debt markets in September – total INR 397 billion capital raised"),
                 "",
paste0("IPO here includes only fresh issuances, does not include OFS or secondary sales"))


#####################################################################################################
#************Monthly inflows into SIPs (INR billion)  /Monthly net inflows into mutual funds (INR billion) : Page 34 ****************************************

my_ppt=Two_Chart("Debt and equity markets: mutual funds",
     Mon_inflows_SIPs,net_inflows_mf_chart,
     
     paste0("Monthly inflows into SIPs, ",Mon_inflow_into_SIPs_38_title),
     "Monthly SIP flows at an all-time high of ~INR 130 billion in October",
     
     paste0("Monthly net inflows into mutual funds (INR billion), ",net_inflows_mf_title),
     "Mutual funds saw net outflows of ~INR 30 billion in October",
     
     Mon_inflow_into_SIPs_38_src,net_inflows_mf_src,
     "Net flows into SIP at an all-time high, mutual fund investments saw net outflows",
     
     paste0("SIP stands for Systematic Investment Plans, an investment route offered by mutual funds wherein one can invest a fixed amount in a Mutual Fund scheme at regular intervals"),
     paste0("")
     )

#####################################################################################################
#************ Daily market prices for listed REITs and InvITs  : Page 32 ****************************

my_ppt=Chart_Medium_Table("Alternative Investments",
                          Daily_market_pri_REITs_InvITs_chart,
                          reit_returns_39,
                          
                          paste0("Daily market prices for listed REITs and InvITs (INR), ",Daily_market_pri_REITs_InvITs_title),
                          
                          paste0("REITs generated average returns of 1.2% as compared to 6.5% returns on InvITs over the past year"),

                          Daily_market_pri_REITs_InvITs_src,
                          "Returns on InvITs outperformed REITs over the last year",
                          
paste0("Return is calculated as on month end\nReturns are only based on stock price movement and do not take into account distribution via dividends"))


#####################################################################################################
#************ Monthly commodity index  : Page 33 ****************************************
my_ppt=Single_Chart("Commodity Markets",Mon_com_index_World_Bank_chart,
                    paste0("Monthly commodity index, 2010=100, ",Mon_com_index_World_Bank_title),
                    "Prices for energy and beverages moderated, base and previous metal prices pick up",
                    
                    Mon_com_index_World_Bank_src,
                    "Prices for base metal and precious metals moderated",
                    
paste0("All commodity indices composed with CY2010 average prices as base for the index.\nEnergy comprises of crude oil,coal and natural gas; beverages comprise of cocoa, tea and coffee; food comprises of food oils, cereals (barley,maize, wheat and rice), meat and fruits (bananas, oranges); base metals comprise of aluminium, copper,zinc, lead, nickel and tin; precious metals comprise of gold, silver and platinum"))

#####################################################################################################
#************ Monthly coal prices  / Monthly brent crude oil prices / Monthly copper and iron ore prices / Monthly gold prices : Page 34 ****************************************

my_ppt=Four_Chart("Commodity Markets",
                  Mon_coal_pri_chart,
                  Mon_brent_crd_oil_pri_chart,
                  Mon_Cu_Fe_ore_prices_WB_42_chart,
                  Mon_gold_pri_chart,
                  
                  paste0("Monthly coal (South African) prices, ",Mon_coal_pri_title),
                  "Coal prices rose to USD 327 per tonne",
                  
                  paste0("Daily Brent crude oil prices, ",Mon_brent_crd_oil_pri_title),
                  "Brent crude prices moderated to USD 93 per bbl",
                  
                  paste0("Daily copper and iron ore prices, ",Mon_Cu_Fe_ore_prices_WB_42_title),
                  "Copper and iron ore prices corrected over the last month",
                  
                  paste0("Monthly gold prices, ",Mon_gold_pri_title),
                  "Gold prices moderated further to USD 1,664 per troy oz",
                  
                  Mon_coal_pri_src,
                  Mon_brent_crd_oil_pri_src,
                  Mon_Cu_Fe_ore_prices_WB_42_src,
                  Mon_gold_pri_src,
                  
                  c1_n="",
                  c2_n="",
                  c3_n="",
                  c4_n="",
                  
                  "Major commodity prices moderated on the expectation of a global growth slowdown")

my_ppt=section_breaker("Investments","Comment")

#####################################################################################################
#************ Annual net FPI investments in India /Annual net DII investments in India  : Page 36 ****************************************
my_ppt=Two_Chart("Flows: portfolio",
                 Annual_FPI_invest_India_chart,Annual_DII_invest_India_chart,
                 
                 paste0("Annual net FPI investments in India (INR billion), ",Annual_FPI_invest_India_title),
                 "Net portfolio outflows from India in October at ~INR 31 billion",
                 
                 paste0("Annual net DII investments in India (INR billion), ",Annual_DII_invest_India_title),
                 "Net domestic investments remain stable in October",
                 
                 Annual_FPI_invest_India_src,Annual_DII_invest_India_src,
paste0("Foreign portfolio investments remained volatile, domestic investors continued to  support the market"),
                 
paste0("Others comprise of debt-VRR and hybrid investments. Hybrid include investments in InvITs and REITs. Debt-VRR (voluntary retention route) allows FPIs to participate in repo transactions and also invest in exchange traded funds that invest in debt instruments."),
                 
paste0("Domestic institutional investors (DII) are those institutional investors who undertake investment in securities and other financial assets (debt, AIFs, etc.) within India. These include insurance companies, banks, DFIs, mutual funds, NPS, EPFO."))

#####################################################################################################
#************ Monthly net FPI investments in India / Monthly net DII investments in India  : Page 36 ****************************************
my_ppt=Two_Chart("Flows: portfolio",
                 Mon_FPI_invest_India_chart,DII_invest_India_chart,
                 
                 paste0("Monthly net FPI investments in India (INR billion), ",Mon_FPI_invest_India_title),
                 "Net portfolio outflows from India in October at ~INR 31 billion",
                 
                 paste0("Monthly net DII investments in India (INR billion), ",DII_invest_India_title),
                 "Net domestic investments remain stable in October",
                 
                 Mon_FPI_invest_India_src,DII_invest_India_src,
paste0("Foreign portfolio investments remained volatile, domestic investors continued to  support the market"),
                 
paste0("Others comprise of debt-VRR and hybrid investments. Hybrid include investments in InvITs and REITs. Debt-VRR (voluntary retention route) allows FPIs to participate in repo transactions and also invest in exchange traded funds that invest in debt instruments."),
                 
paste0("Domestic institutional investors (DII) are those institutional investors who undertake investment in securities and other financial assets (debt, AIFs, etc.) within India. These include insurance companies, banks, DFIs, mutual funds, NPS, EPFO."))

#####################################################################################################
#************ Annual FDI investments  : Page 37 ****************************************
my_ppt=Single_Chart("Flows: FDI",Annual_FDI_invest_chart,
                    
                    paste0("Annual foreign direct investments (USD billion), ",Annual_FDI_invest_title),
                    "Net FDI flows in FY2023 (April-Sep) remain strong at USD 20.2 billion",
                    
                    Annual_FDI_invest_src,
                    paste("Net foreign direct investment flows to India remain strong in H1FY23"),
                    
                    "")

#####################################################################################################
#************ Monthly FDI investments  : Page 37 ****************************************
my_ppt=Single_Chart("Flows: FDI",Mon_FDI_invest_chart,
                    
                    paste0("Monthly foreign direct investments (USD billion), ",Mon_FDI_invest_title),
                    "Net FDI flows in FY2023 (April-Sep) remain strong at USD 20.2 billion",
                    
                    Mon_FDI_invest_src,
                    paste("Net foreign direct investment flows to India remain strong in H1FY23"),
                    
                    "")

#####################################################################################################
#************Quarterly AIF net fundraises (INR billion)\Quarterly Category I AIF net fundraises (INR billion): Page 38 ****************************************

my_ppt=Two_Chart("Alternative Investments",
                 Qtr_AIF_net_fundrai_chart,Qtr_AIF_CI_net_fundrai_chart,
                 
                 paste0("Quarterly AIF net fundraises (INR billion), ",Qtr_AIF_net_fundrai_title),
                 "Funds raising activity by AIFs maintains momentum",
                 
                 paste0("Quarterly Category I AIF net fundraises (INR billion), ",Qtr_AIF_CI_net_fundrai_title),
                 "Funds raised by venture capital funds at an all-time high",
                 
                 Qtr_AIF_net_fundrai_src,Qtr_AIF_CI_net_fundrai_src,
                 "Increased fund-raising activity by AIFs; venture capital funds attract highest funds",
                 
paste0("Category I Alternative Investment Funds (AIFs) invest in startup or early-stage ventures or social ventures, SMEs, infrastructure, or other sectors which the government or regulators consider as socially or economically desirable\nCategory II AIFs are those that do not fall in Category I and II and which do not undertake leverage other than to meet day-to-day operational requirements, such as real estate funds, private equity funds, etc.\nCategory III AIFs employ diverse trading strategies and may employ leverage including through investment in listed or unlisted derivatives such as hedge funds, PIPE funds, etc."),

paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number.")
)

#####################################################################################################
#************Quarterly AIF net investments (INR billion)\Quarterly Category I AIF net investments (INR billion):Page 39 ****************************************

my_ppt=Two_Chart("Alternative Investments",
                 Qtr_AIF_net_invest_chart,Qtr_AIF_CI_net_inv_chart,
                 
                 paste0("Quarterly AIF net investments (INR billion), ",Qtr_AIF_net_invest_title),
                 "Net investments by AIFs remain strong at INR 273 billion",
                 
                 paste0("Quarterly Category I AIF net investments (INR billion), ",Qtr_AIF_CI_net_inv_title),
                 "Net investments by VCs and infra funds remain strong",
                 
                 Qtr_AIF_net_invest_src,Qtr_AIF_CI_net_inv_src,
                 "Alternative investments and deal activity by VC firms remain strong",

paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number."),

paste0("SEBI publishes quarterly data on Alternative Investment Funds (AIFs) related to cumulative commitments raised, funds raised, and investments made up to a quarter-end. Therefore, the chart above shows AIF activity in a quarter by subtracting the cumulative numbers provided by SEBI for current quarter from the previous quarter to get a flow number.")
)


my_ppt=section_breaker("Infrastracture","Comment")

#####################################################################################################
#************ Monthly India truck freight index / Monthly national electronic road toll collection / Monthly road construction in India  : Page 44 ****************************************
my_ppt=Four_Chart("Roads",
                   Mon_ihmcl_col_chart,
                   Mon_etc_road_toll_col_chart,
                   'Data not available',
                   Mon_road_con_India_chart,
                  
                  paste0('Subheader ',Mon_ihmcl_col_title),
                  'Header',
                  paste0("Monthly national electronic road toll collection, ",Mon_etc_road_toll_col_title),
                  "Electronic toll collection picked up to ~INR 43 billion",
                  
                  "Monthly India truck freight index ('000s), FY2017-FY2023 (Oct '22)",
                  "Road freight rates remain elevated",
                  
                  paste0("Monthly road construction in India, ",Mon_road_con_India_title),
                  "Road awarding and construction maintained momentum",
                  
                   
                   Mon_ihmcl_col_src,
                   Mon_etc_road_toll_col_src,
                  "Source: Bloomberg, NIIF Research",
                   Mon_road_con_India_src,
                  
                  c1_n="",
                  c2_n="Note: FasTag is primarily used at ~800 national and ~300 state highways, and at a few parking lots",
                  c3_n="Note: The index tracks average monthly truck freight rates between Delhi and 81 cities in India",
                  c4_n="",
                    
                     
                 "E-toll collection picked up in October, road construction activity maintained momentum")


#####################################################################################################
#************ Monthly cargo traffic at major ports / Daily shipping freight Indices  : Page: 42 ****************************************
my_ppt=Two_Chart("Ports",
                 Mon_crg_tfc_mjr_port_chart,"Data not available",
                 
                 paste0("Monthly cargo traffic at major ports, ",Mon_crg_tfc_mjr_port_title),
                 "Ports' cargo traffic stable at ~62 million tonnes",
                 
                 "Chart sub header","Chart header",
                 Mon_crg_tfc_mjr_port_src,"Source: Bloomberg, NIIF Research",
                 
                 "Cargo traffic at major ports stable; sharp decline in shipping freights for containers",
                 "",    
paste0("Baltic Indices represent average shipping freights across 12 major international routes.Index units measured in points. (January 4, 1985 = 1,000).\nBaltic Dry Index measures freight rates for ships carrying bulk commodities like coal, iron ore, food grains, bauxite and alumina, steel and fertilizers.\nContainer freight measures actual spot freight rates in USD for 40-feet containers for 8 major east-west trade routes compiled as World Container Index (WCI). \n10-year average up to March 2023 for container freight is USD 2,700/FEU and for Baltic Dry is 1,347"))

#REMOVED
#Container freight measures actual spot freight rates in USD for 40-feet containers for 8 major east-west trade routes compiled as World Container Index (WCI).

#####################################################################################################
#************ Monthly passenger rail traffic / Monthly  traffic : Page 43 ****************************************

my_ppt=Two_Chart("Railways",
                 Mon_psgr_rail_tfc,Mon_rail_freight_tfc_chart,
                 
                 paste0("Monthly passenger rail traffic, ",Mon_psgr_rail_tfc_title),
                 "Railway passenger traffic at 552 million, a 32-month high",
                 
                 paste0("Monthly rail freight traffic, ",Mon_rail_freight_tfc_title),
                 "Rail freight stable at ~119 million tonnes in October",
                 
                 Mon_psgr_rail_tfc_src,Mon_rail_freight_tfc_src, 
                 paste0("Rail passenger traffic recovery underway, freight traffic remains high over the last few months"),
                 paste0("Growth in railway passengers for Apr '21 to Nov '21, and May '22 not depicted due to low base effect of Apr '20 to Nov '20, and May '21 respectively \nThis data reflects only inter-city passengers. It does not include intra-city commuters"))

#####################################################################################################
#************ Monthly domestic air passengers / Monthly air cargo traffic : Page 44 ****************************************

my_ppt=Two_Chart("Aviation",
                 Mon_dom_air_psns_chart,Mon_air_cargo_tfc_chart,
                 
                 paste0("Monthly domestic air passengers, ",Mon_dom_air_psns_title),
                 "Domestic air passenger traffic stabilized around ~11 million",
                 
                 paste0("Monthly air cargo traffic, ",Mon_air_cargo_tfc_title),
                 "Air cargo traffic remains high at ~273 kilo tonnes",
                 
                 Mon_dom_air_psns_src,Mon_air_cargo_tfc_src,
                 "Domestic air passenger traffic stabilized, cargo traffic maintains an annual growth momentum",
                 
paste0("Growth in air passengers between Apr '21 and Oct '21, and May '22 not depicted due to low base effect of Apr '20 to Oct '20, and May '21."),
                 
paste0("Growth in air cargo traffic between Apr '21 and Jun '21 not depicted due to low base effect of Apr '20 to Jun '20."))

#####################################################################################################
#************ Monthly demand deficit of power  / Monthly clearance prices on IEX DAM / Monthly electricity generation / Monthly outstanding dues of discoms : Page 45 ****************************************
# Mon_plant_load_factor_chart
my_ppt=Four_Chart("Power",
                  Mon_dmd_defi_power_chart,
                  Mon_DAM_Clearing_Price_chart,
                  Mon_electricity_gen_chart,
                  Mon_outst_dues_of_discoms_chart,
                  
                  paste0("Monthly peak demand deficit of power (%), ",Mon_dmd_defi_power_title),
                  "Power deficit at 0.1% in Oct",
                  
                  paste0("Monthly clearance prices on IEX DAM (INR/kWh), ",Mon_DAM_Clearing_Price_title),
                  "Traded power tariffs fell in October",
                  
                  paste0("Monthly electricity generation, ",Mon_electricity_gen_title),
                  "Electricity generation remains high at 120 billion kWh",
                  
                  paste0("Monthly outstanding dues of discoms, ",Mon_outst_dues_of_discoms_title),
                  "Outstanding dues of distribution companies fell",
                
                  Mon_dmd_defi_power_src,
                  Mon_DAM_Clearing_Price_src,
                  Mon_electricity_gen_src,
                  Mon_outst_dues_of_discoms_src,
                  
                  c1_n="",c2_n="",c3_n="",c4_n="",

                  "Traded power tariffs pick up, power generation remains high in October")

#####################################################################################################
#************ Monthly generation from renewables  / Monthly generation from energy sources / Annual solar and wind power tariff / Monthly silicon and silver prices : Page 46 ****************************************

my_ppt=Three_Chart_B(
       "Renewables",
       Mon_gen_frm_renew_chart,Mon_gen_energy_srcs_chart,"Data not Available",
         paste0("Monthly generation from renewables (billion kWh), ",Mon_gen_frm_renew_title),
        "Solar and wind energy generation fell in October",
        
        paste0("Monthly generation from energy sources, ",Mon_gen_energy_srcs_title),
        "Share of renewable in total energy generation fell in October",
        
        paste0("Chart Sub header"),
        "Chart header",
        
        Mon_gen_frm_renew_src,
        Mon_gen_energy_srcs_src,
        
        "Source: Bloomberg, NIIF Research",
        "RE's share in total generation fell as winter approaches; input prices for solar module are elevated")

my_ppt=section_breaker("Global","Comment")

#####################################################################################################
#***************IMF GDP growth projections page:48***************************
#*
my_ppt=Single_Chart(
       "Global economy","Data Not  Available",
       "IMF real GDP growth projections (%), ",
        paste0("IMF estimates India's FY2024 growth at 6.1%, expected to be the fastest growing large economy in a slowing world"),
       "Source: Bloomberg, NIIF Research",
       
paste0("Global growth forecasts for next year lowered, India expected to maintain strong growth momentum"),
       
paste0("For India, data and forecasts are presented on a fiscal year basis (Apr-Mar)\nFY stands for financial year with the period starting Apr 1 and ending on Mar 31\nThe 6.8% GDP growth for India under the 2022 column is projected for FY2022-23 Calendar year-wise, India's growth projections by IMF are 6.9% in CY2022 and 5.4% in CY2023"))
         
#####################################################################################################
#************ Monthly PMI Composite Indices : Page 49 ****************************************
my_ppt=Single_Chart(
       "Purchasing managers' index: Global",
        PMI_mjr_economies_9_chart,
        
        paste0("Monthly PMI composite indices across major economies, ",PMI_mjr_economies_9_title),
        paste0("Major countries including USA and China's composite PMI contracted while India remains in the expansionary zone"),
        
        PMI_mjr_economies_9_src,
        "Economic activity in India maintained expansionary momentum, fell across major economies",
        
paste0("Impact of Covid on economic activity seen across countries for months between Feb '20 and May '20 and hence not shown in the chart.\nThe headline PMI Composite (Output) Index is a weighted average of the headline PMI Services Index and the Manufacturing Output Index (not the headline PMI manufacturing). Hence, a simple average of PMI Services and Manufacturing indices may not reflect in the PMI Composite"))

#####################################################################################################
#***************IMF GDP growth projections page:50***************************
#*
my_ppt=Single_Chart(
      "Growth","Data Not  Available",
      "Quarterly real GDP growth across countries (% yoy), ",
      
      "Growth across major countries slows down in the June quarter",
      "Source: Bloomberg, NIIF Research",
      
      "Many countries are witnessing a growth slowdown, India has maintained its growth momentum")


#####################################################################################################
#***************IMF GDP growth projections page:51***************************
#*
my_ppt=Single_Chart(
       "Inflation: Global","Data Not  Available",
        "Monthly consumer price inflation (% yoy), ",
       
paste0("Inflation showed sign of moderation across some major economies, continued to pick up in UK, Germany, Japan, and France"),
        
        "Source: Bloomberg, NIIF Research",
        "Inflationary pressures remain strong in UK, Germany, Japan, and France")

#####################################################################################################
#************ Monthly performance of Nifty-50, Sensex and other global indices : Page 52 ****************************************

my_ppt=Chart_Small_Table("Equity markets: global",
                  Mon_Nft_Sen_gbl_indices_chart,equity_markets_34,
                  paste0("Monthly performance of Nifty-50, Sensex and other global indices, returns in local currency (% yoy), ",Mon_Nft_Sen_gbl_indices_title),
                  
                  "Global equity markets generated median negative returns of 8.4% over the past twelve months",
                  Mon_Nft_Sen_gbl_indices_src,
                  
                  "Indian equity markets generated positive returns while most major countries corrected sharply",
                  "Return is calculated as on month end")

#####################################################################################################
# #************ Tax Collection govt / Govt's Gross market Borrowing : Page 21/Govt_Gross_market_Bor_21_chart ****************************************
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
#************ India G-sec and benchmark treasury bill yields  / 10-y benchmark yields across major DMs : Page 53 ****************************************

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

#####################################################################################################
#************ Change in major economic indicators (% yoy) : Page 54 ****************************************
#*
#HARD COde: Since multiple source is there i.e why source is hard coded and it is done across all the tables
my_ppt=abdul_table_type1(
             paste0("Continued momentum in economy, supported by strong domestic demand and credit growth"),
             "High Frequency Indicators",
             economic_indicator_gr_54,
             "High-frequency indicators continue to support economic growth, trade slows down", 
             paste0("Change in major economic indicators (% yoy), ",economic_indicator_gr_54_title),
             
             paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways, Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),
             
             l1=1.35,t1=1.6,
             l2=1.35,t2=1.2,
             l3=1.35,t3=1.38,
             l4=1.35,t4=6.98,
             l5=1.35,t5=7.30,
             w2=10,
             
             "Conditional formatting based on absolute values with respect to zero, with the largest negative values represented by dark red and largest positive values represented by dark green for each variable")


#####################################################################################################
#************ Major economic indicators (absolute values) : Page 55 ****************************************

my_ppt=abdul_table_type2(
            paste0("Continued momentum in economy, supported by strong domestic demand and credit growth"),
            "High Frequency Indicators",
            economic_indicator_55,
            "High-frequency indicators continued to support economic growth; trade slows down",
            paste0("Major economic indicators (absolute values), ",economic_indicator_55_title),
            
            paste0("Source: Thurro, CGA, Ministry of Finance, MoSPI, EAI, POSOCO, Indian Railways,Indian Ports Association, AAI, GSTN, RBI, NPCI, NIIF Research"),
            
           l1=1.19,t1=1.75,l2=1.19,t2=7.35)

#####################################################################################################
#************ Monthly index of industrial production (% yoy) : Page 56 ****************************************

my_ppt=abdul_table_type2(
             paste0("Industrial production weakened driven by contraction in consumer goods"),
             "High Frequency Indicators",
             industrial_Prod_56,
             "Industrial production growth remains weak driven by the slowdown in consumer goods category",
             paste0("Monthly index of industrial production (% yoy), ",industrial_Prod_56_title),
             paste0("Source: Thurro, MOSPI, NIIF Research"),
             l1=1.19,t1=1.75,l2=1.19,t2=5.882)
                         
#####################################################################################################
#************ Monthly total credit outstanding by sector (INR trillion): Page 57 ****************************

my_ppt=abdul_table_type2(
        paste0("Strong credit take-off supported by growth in retail and NBFC"),
       "High Frequency Indicators",
       credit_outstanding_57,
       "Strong credit growth driven by retail and NBFC credit",
       paste0("Monthly total credit outstanding by sector (INR trillion), ",credit_outstanding_57_title),
       paste0("Source: Thurro, RBI, NIIF Research"),
       l1=1.19,t1=1.75,l2=1.19,t2=6.068)

#####################################################################################################
#************ Monthly consumer price inflation (% yoy): Page 58 ****************************
my_ppt=abdul_table_type2(
         paste0("Moderation in retail inflation primarily due to softening of food prices"),
         "Inflation",
         consumer_inflation_58,
         "Strong credit growth driven by retail and NBFC credit",
         paste0("Monthly consumer price inflation (% yoy), ",consumer_inflation_58_title),
         paste0("Source: Thurro, MoSPI, NIIF Research"),
         l1=1.19,t1=1.75,l2=1.19,t2=6.450)

#####################################################################################################
#***************Wholesale inflation moderates page:59***************************

my_ppt=abdul_table_type2(
        paste0("Moderation in wholesale inflation over the last few months as commodity prices ease"),
       "Inflation",
       wholesale_inflation_59,
       
       paste0("Wholesale inflation moderated to 8.4% in October driven by ease in food articles and manufactured products"),
       
       paste0("Monthly wholesale price inflation (% yoy), ",wholesale_inflation_59_title),
       paste0("Source: Thurro, EAI, NIIF Research"),
       l1=1.19,t1=2,l2=1.19,t2=5.6494)
#####################################################################################################

my_ppt=section_breaker("Appendix","Comment")
#####################################################################################################
#***Projections for real GDP growth /Bi-monthly median real GDP projections Page:61***************************
my_ppt=Two_Chart(
        "Growth",
        "Data Not Available",bi_real_gva_gdp_chart,
                 
        paste0("Projections for real GDP growth in FY2023 (% yoy), Jan '22 to Jul '22,"),
       "Median GDP growth projection for FY2023 lowered to 7.4%",
           
        paste0("Bi-monthly median real GDP projections for FY2024 by RBI (% yoy), ",bi_real_gva_gdp_title),
        "RBI's recent survey projects lower growth in FY2024",
           
        "Source: CMIE, NIIF Research",bi_real_gva_gdp_src,
        "Growth projections for FY2024 lowered as global economy expected slowdown",
           
paste0("The data shown above is the projections made by same agencies across two time periods,Jan to April 2022 and May to July 2022"),
           
paste0("RBI's Professional Forecasters' Survey presents short to medium term economic development on GDP growth, among other macroeconomic indicators. In every round of survey,questionnaires are shared with 30 to 40 selected forecasters."))

#####################################################################################################
#*****Bi-monthly projections for consumer price inflation /Bi-monthly projections for wholesale price inflation  Page:62***************************
#HARD CODE:FY is hard coded because its a projection.
my_ppt=Two_Chart(
      "Inflation",
       bi_cpi_cpicore_chart,bi_wpi_wpicore_chart,
      paste0("Bi-monthly projections for consumer price inflation in FY2024 (% yoy) , ",bi_cpi_cpicore_title),
      "CPI projections for FY2024 revised upwards",
      
      paste0("Bi-monthly projections for wholesale price inflation in FY2024 (% yoy). ",bi_wpi_wpicore_title),
      "WPI projections moderated; WPI core expected to be lower",
      
      bi_cpi_cpicore_src,bi_wpi_wpicore_src,
      "Wholesale inflation projections moderated as global commodity prices fall",
      
paste0("RBI's Professional Forecasters' Survey presents short to medium term economic development  on inflation, among other macroeconomic indicators. In every round of survey, questionnaires are shared with 30 to 40 selected forecasters."))

#####################################################################################################
#*****Outstanding domestic central government borrowings /Outstanding central external government borrowings Page:63***************************

my_ppt=Two_Chart(
      "Fiscal",
      govt_int_debt_chart,govt_ext_debt_chart,
      paste0("Outstanding domestic central government borrowings, ",govt_int_debt_title),
      "Domestic market borrowings by the government have increased",
      
      paste0("Outstanding central external government borrowings, ",govt_ext_debt_title),
      "External debt borrowings of the government stable ",
      
      govt_int_debt_src,govt_ext_debt_src,
      "Government domestic borrowings remain elevated, exposure to external market stable")


#####################################################################################################
#*****Quarterly growth in merchandize exports/Quarterly growth in merchandize imports  Page:64***************************
my_ppt=Two_Chart(
       "Trade",
       Qtr_exp_gr_chart,Qtr_imp_gr_chart,
       paste0("Quarterly growth in merchandize exports (% yoy), ",Qtr_exp_gr_title),
       "Merchandize exports supported by both volume and price growth",

       paste0("Quarterly growth in merchandize imports (% yoy), ",Qtr_imp_gr_title),
       "Merchandize import supported by both volume and price growth",

       Qtr_exp_gr_src,Qtr_imp_gr_src,
      "Merchandize trade growth supported by both volume and price growth")

#####################################################################################################
#*****Quarterly survey for business expectations/Quarterly survey for business costs and profits Page:65***************************
my_ppt=Two_Chart(
       "Business sentiments",
       Qtr_bus_exp_chart,Qtr_bus_cost_chart,
       paste0("Quarterly survey for business expectations (%), ",Qtr_bus_exp_title),
       "Companies' optimism about Sep quarter's business situation improves",
       
       paste0("Quarterly survey for business costs and profits (%), ",Qtr_bus_cost_title),
       "Profit margins expected to pick up in Sep quarter; cost pressure to remain",
       
       Qtr_bus_exp_src,Qtr_bus_cost_src,
      "Companies expect higher profit margins in Q2FY23; input cost pressure expected to remain",
      
paste0("The survey covers the non-government, non-financial private and public limited companies engaged in manufacturing for qualitative responses on indicators of demand, financial situation, price and employment expectations, etc. The expectations are reported as the  difference in percentage of the respondents' reporting optimism and that reporting pessimism. Better overall business situation is deemed to be optimistic"),
       
paste0("Each metric is reported as the difference in percentage of the respondents' reporting optimism and that reporting pessimism. Values greater than zero indicate expansion while values less than zero indicate contraction"))

#####################################################################################################
#*****Quarterly telecom-internet subscribers/Quarterly smartphone shipments Page:66***************

my_ppt=Two_Chart("Digital",
                 Qtr_tele_sub_chart,'Data not available',
                 
                 paste0("Quarterly telecom and internet subscribers (million), ",Qtr_tele_sub_title),
                 "Telecom and internet subscribers remain steady",
                 
                 paste0("Quarterly smartphone shipments (million units), "),
                 "Smartphone sales remain above pre-Covid average in Q1FY23",
                 
                 Qtr_tele_sub_src,"Source: CMIE, NIIF Research",
                 "Internet subscribers remain steady; sale of smartphones above pre-Covid average",
                 
                 "Internet subscribers' data available till Mar '22")


#####################################################################################################
#*****Quarterly gross non-performing assets/Quarterly weighted average lending rates for SCBs Page:67***************************
my_ppt=Two_Chart(
       "Banking and financial institutions",
        Qtr_gross_npa_chart,Qtr_walr_chart,
       
       paste0("Quarterly gross non-performing assets (% of gross advances), ", Qtr_gross_npa_title),
       "Banks report improvement in asset quality",
       
       paste0("Monthly weighted average lending rates for SCBs(%), ",Qtr_walr_title),
       "Lending rates for scheduled commercial banks pick up",
       
      Qtr_gross_npa_src,Qtr_walr_src,
      "Bank asset quality improves; transmission of policy rate hikes begins")
#####################################################################################################
#*****Quarterly mutual fund AUMs /Quarterly mutual fund folios  Page:68***************************
my_ppt=Two_Chart(
       "Mutual funds",
        Mf_aum_chart,Mf_folios_chart,
       
       paste0("Quarterly mutual fund AUMs (INR trillion), ",Mf_aum_title),
       "Investments in mutual funds fall to INR 35.6 trillion",
       
       paste0("Quarterly mutual fund folios (million units), ",Mf_folios_title),
       "Retail participation in mutual funds pick up",
       
       Mf_aum_src,Mf_folios_src,
       "Corporate sectors reduces its exposure to mutual funds",
       
paste0("High net-worth individuals (HNI) are individuals with investable capital greater than INR 20 million"))

#####################################################################################################
#*****Quarterly outstanding deployment debt/Quarterly outstanding deployment equity  Page:69***************************
my_ppt=Two_Chart(
       "Mutual funds",
        Mf_debt_chart,qtr_mfh_t_chart,
       
       paste0("Quarterly outstanding deployment in debt (INR trillion), ",Mf_debt_title),
       "Investments in debt instruments by mutual funds remains high",
       
       paste0("Quarterly outstanding deployment in equity (INR trillion), ",qtr_mfh_t_title),
       "Investments by mutual funds in equity remains high",
       
       Mf_debt_src,qtr_mfh_t_src,
       "Mutual fund investments in equity and debt remains high in Q1FY23",
       
paste0("Instruments (ex-corporate debt) comprise of commercial paper, bank CDs, treasury bills and collateralized borrowing and lending obligations.\nOthers include PSU bonds/debt, equity linked debentures and notes, securitized debt, bank FDs and other instruments."))

#####################################################################################################
#*****Annual FDI inflows by sector Page:74***************************

my_ppt=abdul_table_type2(
       paste0("Higher trade deficit driving current account deficit in Q4FY22"),
      "Balance of payments",
       quarterly_bop_74_chart,
       paste0("Higher merchandize imports drive current account balance into deficit, higher investments boost financial account surplus"),
      
       paste0("Quarterly balance of payments (USD billion), ",quarterly_bop_74_title),
       paste0("Source: Thurro, RBI, NIIF Research"),
       l1=1.19,t1=2,l2=1.19,t2=6.5)

#####################################################################################################
#*****Annual FDI inflows by sector Page:75***************************

my_ppt=abdul_table_type2(
       paste0("Higher trade deficit driving current account deficit in Q4FY22"),
      "Balance of payments",
       quarterly_bop_annual_chart,
       paste0("Higher merchandize imports drive current account balance into deficit, higher investments boost financial account surplus"),
      
       paste0("Annual balance of payments (USD billion), ",quarterly_bop_annual_title),
       paste0("Source: Thurro, RBI, NIIF Research"),
       l1=1.19,t1=2,l2=1.19,t2=6.5)



#####################################################################################################
#*****Annual FDI inflows by country Page:75***************************
#*
my_ppt=abdul_table_type2(
        paste0("Equity investments by mutual funds continue to grow; banks and software lead investments"),
        "Mutual funds",
        quarterly_mfh_75_chart,
        paste0("Banks, finance, and petroleum products sectors attract stable equity investments by mutual funds"),
        paste0("Monthly equity deployment by mutual funds (INR trillion), ",quarterly_mfh_75_title),
        
        paste0("Source: Thurro, SEBI, NIIF Research"),
         l1=1.19,t1=1.75,l2=1.19,t2=7.18)

#####################################################################################################
#*****Annual FDI inflows by sector Page:76***************************

my_ppt=abdul_table_type2(
       paste0("FDI investments into services, trading, and automobile have recorded steep growth"),
      "FDI flows",
       Annual_FDI_inflows_by_Sector_76_chart,
       paste0("Computer software and hardware, services, and automobile attract largest investments in 2022"),
       paste0("Annual FDI inflows by sector (USD billion), ",Annual_FDI_inflows_by_Sector_76_title),
       paste0("Source: Thurro, Department for Promotion of Industry and Internal Trade, NIIF Research"),
        l1=1.19,t1=2,l2=1.19,t2=6.051)

#####################################################################################################
#*****Annual FDI inflows by country Page:77***************************
#*
my_ppt=abdul_table_type2(
        paste0("FDI inflows from Mauritius and Netherlands have witnessed sharp growth"),
        "FDI flows",
        Annual_FDI_inflows_by_country_77_chart,
        paste0("Singapore and USA continue to be the largest foreign investors in India"),
        paste0("Annual FDI inflows by sector (USD billion), ",Annual_FDI_inflows_by_country_77_title),
        paste0("Source: Thurro, Department for Promotion of Industry and Internal Trade, NIIF Research"),
         l1=1.19,t1=2,l2=1.19,t2=6.2163)

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
setwd("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT")

# setwd("C:\\Users\\Santonu\\Documents")
print(my_ppt,target="NIIF_PPT.pptx")


## ------------------------------------------------
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

