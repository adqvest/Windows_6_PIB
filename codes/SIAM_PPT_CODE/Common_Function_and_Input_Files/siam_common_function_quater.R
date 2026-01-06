#-----LODING PACKAGE----------------------------
library(pacman)
library(officedown)
library(officer)
library(ggplot2)
# library(feather)
library(dplyr)
library(tidyr)
library(jsonlite)
library(lubridate)
library(zoo)
library(officer)
library(magrittr)
p_load(XML,downloader,varhandle,gdata,mFilter,Matrix,dplyr,igraph,reshape,reshape2,
       stringr,DBI,lubridate,RMySQL,zoo,lattice,pracma,plotrix,timeDate,neverhpfilter,
       png,jpeg,RCurl,pracma,ggplot2,scales,RPostgres,jsonlite,glue,plotly,sf,
       ggnewscale,ggrepel,extrafont,showtext,flextable,grid,gtable,tibble,data.table,scales,
       paletteer,padr,tidyr,epitools,lubridate,maps,mapdata,ggmap,stringr,reprex,ggfittext,
       ggalluvial,lubridate,timeDate,english,RClickhouse,officer,officedown,ggthemes,
       openxlsx,readxl,
       install = TRUE)
default_start_date ="2013-04-01"#Properties_FILE
########################### DB FILES #################################
Adqvest_Properties = read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\AdQvest_properties.txt",
                                  sep = " ", header = T)
Adqvest_Clickhouse=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Adqvest_ClickHouse_properties.txt", 
                                  sep = "", header = T)

Adqvest_Test_Thurro=read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\Postgress_properties_test.txt",
                                  sep = "", header = T)


#-----PPT TEMPLATE----------------------------
f     ="C:\\Users\\Administrator\\AdQvestDir\\codes\\SIAM_PPT_CODE\\Common_Function_and_Input_Files\\SIAM_template_quater.pptx"
my_ppt=read_pptx(f)

#-----COMMON_FUNCTION_VARIABLES----------------------------

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
text_size=10
chart_label=8
v_just=0
x_v=0
nug_x=0.1
nug_y=0.3
#-----DB Deatail----------------------------
username = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[1]
password = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[2]
hostname = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[3]
portnum  = as.numeric(as.character(Adqvest_Properties[2,2]))
DBName   = as.character(Adqvest_Properties[3,2])


host_cl     = unlist(as.character(Adqvest_Clickhouse[1,2]))
port_cl     = as.numeric(as.character(Adqvest_Clickhouse[2,2]))
DB_cl       = as.character(Adqvest_Clickhouse[3,2])
user_cl     = unlist(strsplit(as.character(Adqvest_Clickhouse[4,2]),':|@'))[1]
password_cl = unlist(as.character(Adqvest_Clickhouse[5,2]))

host_pg     = unlist(as.character(Adqvest_Test_Thurro[1,2]))
port_pg     = as.numeric(as.character(Adqvest_Test_Thurro[2,2]))
DB_pg       = as.character(Adqvest_Test_Thurro[3,2])
user_pg     = unlist(strsplit(as.character(Adqvest_Test_Thurro[4,2]),':|@'))[1]
password_pg = unlist(as.character(Adqvest_Test_Thurro[5,2]))

mysql  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)


#-----TABLE_FUNCTION----------------------------
graph_extend<-function(data){
  max_value<-max(data$Total)
  if (max_value >0 && max_value <=10){
    val= 5
  }else if(max_value >10 && max_value <=100){
    val= 8
  }else if (max_value >100 && max_value <=200){
    val= 10
  }else if (max_value >200 && max_value <=400){
    val= 15
  }else if(max_value >400 && max_value <=1000){
    val= 30
  }else if (max_value >1000 && max_value <=2000){
    val= 50
  }else{
    val= 200
  }
  return (val)
}
get_y_add_value<-function(y_v){
  if (y_v==5){
    val=1
  }else if(y_v==10){
    val=1
  }else if(y_v==30){
    val=2
  }else if(y_v==50){
    val=5
  }else if (y_v==100){
    val=10
  }else{
    val=30
  }
  return (val)
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


data_query_clk_pg=function(id,exception=FALSE,
                           surplus=FALSE,Water_Reservior=FALSE,
                           VAHAN=FALSE,year=FALSE){
  
  tryCatch({
    clk  <- DBI::dbConnect(RClickhouse::clickhouse(), 
                           dbname =DB_cl,host = host_cl, 
                           port = port_cl,user = user_cl,password =password_cl)
    
    pg  <- DBI::dbConnect(RPostgres::Postgres(),
                          dbname =DB_pg,host = host_pg, 
                          port = port_pg,user = user_pg,
                          password =password_pg)
    print('Ran successfully')
    if (surplus==TRUE){
      period=2500
    }else if(year==TRUE){
      period=10
    }else{period=162}
    
    qstring     = 'select * from widgets_new where widget_id=chart_id'
    quary_by_id = str_replace(qstring,"chart_id",glue({id}))
    res         = dbGetQuery(pg,quary_by_id)
    d1          = res[,"query_param"]
    descrip     = res[,"title"]
    data_upto   = res[,"data_upto"]
    source      = paste0("Source: ","Thurro, ",res[,"source"][1],", NIIF Research")
    quary_1     = fromJSON(d1, flatten=TRUE)
    y_lab       = quary_1[7][[1]][[1]]
    plot_type   = quary_1[6][[1]][[1]]
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
    result = list("data_frame" = final_data_frame,"description" =  descrip,
                  "source"=source,"data_upto"=data_upto)
    dbDisconnect(clk)
    dbDisconnect(pg)
    d1=tolower(final_quary)
    str_remove(d1,'where')
    s1=str_split(d1,'where')[[1]][1]
    s2=str_split(s1,'from')[[1]][2]
    mv_table=str_trim(s2, side="both")
    df1=data.frame(ID=c(id),MV_TABLE=c(mv_table))
    return(result)
  }, 
  error = function(e){
    print('In else block')
    print(e)
    print("An error occurred:\n")
    print("Error message:", e$message, "\n")
    print("Traceback:\n")
    print(traceback())
    print("Error occurred at line ", as.integer(sys.calls()[[1]]), ": ", conditionMessage(e), "\n")
  }
  )}

roundexcel = function(x,digits=0){
  posneg = sign(x)
  z = abs(x)*10^digits
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^digits
  z*posneg
}


#-----SLIDE THEME----------------------------
common_theme=function(x_angle=0,leg_plc="top",legend_key_sp_y=0.05,Position='center'){
  
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
siam_cutom_theme=function(){
  theme( panel.border=element_blank(),
         panel.grid=element_blank(),
         panel.grid.major=element_line(size = 0.2,linetype =0,colour = "grey"),
         panel.grid.major.x =element_blank(),
         #plot.background = element_rect(color = "black", linewidth = 0.2),
         axis.line.x.bottom=element_line(size =0.5,linetype =1,colour = "black"),
         axis.line.y=element_line(size =0.5,linetype =1,colour = "black"),
         plot.margin=unit(c(0,0,0.25,0), 'cm'))
}
################# SLIDE FUNCTIONS ######################

niif_hm_2_table=function(title,table1,table2,c1_t,c2_t,c1_th=1,t1_h=3,t2_h=3,l1=1.19,w2=10,h=0.1){
  print('in table')
  w2=is.numeric(w2)
  
  tbl1_t=c1_th+0.13
  c2_th=tbl1_t+t1_h+0.5
  tbl2_t=c2_th+0.13
  my_ppt=add_slide(my_ppt,layout ="2_TABLE__HEADER",
                   master ="NIIF MONTHLY REPORTS" ) %>%
    ph_with(value =title,location=ph_location_type(type ="title")) %>%
    
    ph_with(value =table1,location =ph_location_type(type ="tbl",id=1
    )) %>%
    
    ph_with(value =c1_t,location =ph_location_type(type ="body",id=1,left=l1,top=c1_th)) %>%
    
    ph_with(value =table2,location =ph_location_type(type ="tbl",id=2,left =l1,top =tbl2_t
    ))%>%
    
    ph_with(value =c2_t,location =ph_location_type(type ="body",id=3,left = l1,top = c2_th,width = w2,height=h 
    )) %>%
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
  
}
siam_3_table=function(title,table1,table2,table3,
                      c1_t,c2_t,c3_t,c1_th=1,t1_h=3,t2_h=3,l1=1.19,w2=10,h=0.1){
  tbl1_t=c1_th+0.13
  c2_th=tbl1_t+t1_h+0.5
  tbl2_t=c2_th+0.13
  print('-------------------This is c1_t,c2_t,c3_t-----------')
  print(paste('c1_t:',c1_t))
  print(paste('c2_t:',c2_t))
  print(paste('c3_t:',c3_t))
  my_ppt <- add_slide(
    my_ppt,
    layout = "3_TABLE_CHART",
    master = "NIIF MONTHLY REPORTS"
  ) %>%
    ph_with(value = title, location = ph_location_type(type = "title")) %>%
    ph_with(value = table1, location = ph_location_type(type = "tbl", id = 1),
            left = 1, top = 1, width = 1, height = 2) %>%
    ph_with(value =c1_t,location =ph_location_type(type ="body",id=4,left=l1,top=c1_th)) %>%
    
    ph_with(value =table2,location =ph_location_type(type ="tbl",id=2,
                                                       left =l1,top =tbl2_t))%>%
    
    ph_with(value =c2_t,location =ph_location_type(type ="body",id=1, 
                                                     left = l1,top = c2_th,width = w2,height=h)) %>%
    ph_with(value =table3,location =ph_location_type(type ="tbl",id=3,
                                                       left =l1,top =tbl2_t))%>%
    
    ph_with(value =c3_t,location =ph_location_type(type ="body",id=2, 
                                                     left = l1,top = c2_th,width = w2,height=h)) %>%
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}
Four_Chart=function(c1,c2,c3,c4,
                    c1_t,c1_t1,c2_t,c2_t1,c3_t,c3_t1,c4_t,c4_t1,
                    title1="TITLE"){
  
  my_ppt=add_slide(my_ppt,layout ="4 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
    ph_with(value =title1,location=ph_location_type(type ="title")) %>%
    ph_with(value = c1, location =ph_location_type(type ="chart",id=1)) %>%
    ph_with(value =c2, location =ph_location_type(type ="chart",id=4)) %>%
    ph_with(value =c3, location =ph_location_type(type ="chart",id=2)) %>%
    ph_with(value =c4, location =ph_location_type(type ="chart",id=3)) %>%
    
    ph_with(value =c1_t1, location =ph_location_type(type ="body",id=9)) %>%
    ph_with(value =c1_t, location =ph_location_type(type ="body",id=12)) %>%
    
    ph_with(value =c2_t1, location =ph_location_type(type ="body",id=15)) %>%
    ph_with(value =c2_t, location =ph_location_type(type ="body",id=8)) %>%
    
    ph_with(value =c3_t1, location =ph_location_type(type ="body",id=11)) %>%
    ph_with(value =c3_t, location =ph_location_type(type ="body",id=4)) %>%
    
    ph_with(value =c4_t1, location =ph_location_type(type ="body",id=6)) %>%
    ph_with(value =c4_t, location =ph_location_type(type ="body",id=7)) %>%
    
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
  
}
Three_Chart_With_Box=function(title,c1,c2,c3,
                              c1_t,c1_t1,c2_t,c2_t1,c3_t,c3_t1,
                              c1_s,c2_s,c3_s,c1_n,c2_n,c3_n,title1="TITLE",com1=''){  
  my_ppt=add_slide(my_ppt,layout ="3 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
    ph_with(value =title1,location=ph_location_type(type ="title")) %>%
    ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
    ph_with(value =c2, location =ph_location_type(type ="chart",id=2)) %>%
    ph_with(value =c3, location =ph_location_type(type ="chart",id=3)) %>%
    
    ph_with(value =c1_t1, location =ph_location_type(type ="body",id=1)) %>%
    ph_with(value =c1_t, location =ph_location_type(type ="body",id=2)) %>%
    ph_with(value =c2_t1, location =ph_location_type(type ="body",id=6)) %>%
    ph_with(value =c2_t, location =ph_location_type(type ="body",id=10)) %>%
    ph_with(value =c3_t1, location =ph_location_type(type ="body",id=8)) %>%
    ph_with(value =c3_t, location =ph_location_type(type ="body",id=3)) %>%
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    
    
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}
Two_Chart=function(title,c1,c2,c1_t,c1_t1="Chart header1",
                   c2_t,c2_t2="Chart header2",
                   c1_s,c2_s,title1="TITLE",com1='',com2=''){
  
  my_ppt=add_slide(my_ppt,layout ="WITH NAME 2 CHARTS",master ="NIIF MONTHLY REPORTS" ) %>%
    ph_with(value =title1,location=ph_location_type(type ="title")) %>%
    ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
    ph_with(value =c2,location =ph_location_type(type ="chart",id=2)) %>%
    
    # ph_with(value ='title', location =ph_location_type(type ="body",id=12)) %>%
    ph_with(value =c1_t1, location =ph_location_type(type ="body",id=8)) %>%
    ph_with(value =c1_t, location =ph_location_type(type ="body",id=1)) %>%
    ph_with(value =c2_t2, location =ph_location_type(type ="body",id=4)) %>%
    ph_with(value =c2_t, location =ph_location_type(type ="body",id=3)) %>%
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}
Single_Chart=function(title,c1,c1_t,c1_t1="Chart header1",
                      c1_s,title1="TITLE",com1='',com2=''){
  my_ppt=add_slide(my_ppt,layout ="1 Charts",master ="NIIF MONTHLY REPORTS" ) %>%
    ph_with(value =title1,location=ph_location_type(type ="title")) %>%
    ph_with(value =c1, location =ph_location_type(type ="chart",id=1)) %>%
    
    # ph_with(value ='title', location =ph_location_type(type ="body",id=12)) %>%
    ph_with(value =c1_t1, location =ph_location_type(type ="body",id=4)) %>%
    ph_with(value =c1_t, location =ph_location_type(type ="body",id=3)) %>%
    ph_with(value =' ', location =ph_location_type(type ="ftr",id=1)) %>%
    ph_with(value = empty_content(),location = ph_location_type(type ="sldNum"))
}
cover_slide=function(val,date){
  my_ppt=add_slide(my_ppt,layout ="Cover_page",master ="NIIF MONTHLY REPORTS") %>%
    ph_with(value =val,location=ph_location_type(type ="title"))%>%
    ph_with(value =paste(date,' NEW DELHI'), location =ph_location_type(type ="body",id=2))
}
thanks_slide=function(){
  my_ppt=add_slide(my_ppt,layout ="thank you layout",master ="NIIF MONTHLY REPORTS")%>%
    ph_with(value ='THANK YOU', location =ph_location_type(type ="body",id=1))
}
#################TABLE CREATION#############
siam_table=function(i,data,slide_heading,chart_title,QUATER, has_main_sector = FALSE,main_sector_bg = "GRAY 96",
                    has_units = FALSE,rename_unit_col = 'Units',set_header_bg = 'white',
                    padding_vals = list(),make_bold = c(),make_col_bold=c(),hlines = c(),
                    vlines = c(),background_vals = list(),color_coding = FALSE,alpha = 0.5,
                    median_change = list(),invert_map = c(), 
                    replace_null = '', caption = '',font_size = 9, var_col_width = 2,
                    unit_col_width = 1,other_col_width = 0.61,line_space=0,row_height=0.05,
                    yoy=FALSE){
  print(data)
  
  all_variables <- data$Category[!duplicated(data$Category)]
  all_df <- data
  print('all_df')
  print(all_df)
  years <- grep("^\\d{4}$", names(all_df), value = TRUE)
  
  date_cols <- colnames(all_df)[c(1:length(colnames(all_df)))]
  make_col_bold<-date_cols
  print('date_cols')
  print(date_cols)
  set_flextable_defaults(font.family = 'Arial')
  all_df$`CAGR %` <- as.numeric(all_df$`CAGR %`)
  ft <- flextable(all_df) %>%
    set_table_properties(width = 1) %>%
    flextable::width(j = "Category", width = 1) %>%
    compose(j = "CAGR %", 
            value = as_paragraph(as_chunk(all_df$`CAGR %`), as_chunk("%"))) %>%  # Remove `style` argument
    color(i = ~`CAGR %` >= 1, j = c("CAGR %"), color = "#008B00") %>%
    color(i = ~`CAGR %` < 1, j = c("CAGR %"), color = "red")  # Apply color to both %>%
  
  print('background_vals')
  print(background_vals)
  print('make_col_bold')
  print(make_col_bold)
  ## Background Color if specified : Please mention color value as "col_" and followed by the color hex value 
  ###  Example : if you want to color of #D44D44 then your list input should be col_D44D44
  if (length(background_vals) != 0){
    for (m in background_vals){
      print(m)
      val = '#B2DFEE'
      for (i in background_vals){
        row_formula = as.formula(paste("~ Category %in% '",i,"'",sep = ''))
        ft <- bg(ft,bg = val,i = row_formula , part = "body")
        ft <- bg(ft, bg = "#1874CD", part = "header")
      }
    }
  }
  ft <- ft %>% bold(j = make_col_bold, part = 'header')
  ft <- set_header_labels(ft, Variable = " ")
  ## Makes main sectors bold and gives background color
  
  ft <- ft %>% fontsize(size = font_size, part = 'all')
  
  ft <- width(ft, j = c('Category') ,width = 2)
  ncol_value<-length(date_cols)
  wid_cal=8.456/(ncol_value-1)
  print(wid_cal)
  ft <- width(ft, j = 2:ncol_value, width = wid_cal)  # Adjust column widths
  ft <- height_all(ft, height = 0.7, unit = "cm")
  
  ft <- padding(ft, padding.top = 1, padding.bottom = 1, part = "all")
  big_border = fp_border(color="black", width = 1)
  # big_border <- fix_border_issues(big_border)
  ft <- hline_bottom(ft, part="body", border = big_border)
  ## Adding horizontal lines below the specified rows
  small_border = fp_border(color="black", width = 0.5)
  if (length(hlines) != 0){
    for (m in hlines){
      formula = as.formula(paste("~ Category %in% '",m,"'",sep = ''))
      ft <- ft %>% hline(i = formula, part = 'body', border = small_border)
    } 
  }
  
  ## Adding Vertical lines below the specified rows
  dotted_border = fp_border(color = "black", style = "solid", width = 0.5)
  if (length(vlines) != 0){
    for (m in vlines){
      ft <- ft %>% vline(j = m, part = 'all', border = dotted_border)
    } 
  }
  
  big_border <- fp_border(color = "black", width = 0.5)
  
  ft<-hline_top(ft, j = NULL, border = big_border, part = "header")
  ft <- vline_left(ft, border = big_border)
  ft <- flextable::align(ft, align = "right", part = "body")  # Right-align body cells
  ft <- flextable::align(ft, j = 1, align = "left", part = "body")  # Left-align Category column
  ft <- flextable::align(ft, align = "center", part = "header")  # Center header text
  ft <- flextable::fontsize(ft, size = 13, part = "header") 
  return(ft)
}
################################### Charts Creation Function ###################################
charts<-function(i,data_final,x_axis_interval,chart_type,chart_category,max_pri_y,min_pri_y,
                 key_spacing,max_overlaps,h_just_line,v_just_line,n_col,n_row,num_brek,my_legends_col,my_line_type,
                 scale_shape_manual,special_case,graph_lim=30,function_param,special_case_cy){
  tryCatch({
    
    # Note: Always put key_spacing before max_pri_y order of the legends_key
    ##########################COMMON VARIABLE ASSINGNMENT###########################
    print('********************** INSIDE CHART CREATION FUNCTION************************')
    print('TRYING TO CREATE A CHART')
    special_case_cy=special_case_cy
    print(paste('special_case_cy:',special_case_cy))
    function_param=function_param
    print('Trying to print function param')
    print(function_param)
    print(chart_type)
    print(chart_category)
    if (chart_category=='bar line'){
      round_integer=function_param[[i]][[1]]
      data_unit=function_param[[i]][[2]]
      WHITE_BACK=function_param[[i]][[3]]
      show_older=function_param[[i]][[4]]
      format_date=function_param[[i]][[5]]
      x_angle1=function_param[[i]][[6]]
      key_spacing=function_param[[i]][[7]]
      DATE_HEADER=function_param[[i]][[8]]
      bar_thick=function_param[[i]][[9]]
      preprocessed_data=Data_preprocessing_line(data_final,max_pri_y,min_pri_y)
      data_final <- preprocessed_data$data_frame1
      data_ends <- preprocessed_data$data_frame2
    }else if (chart_category=='line'){
      preprocessed_data=Data_preprocessing_line(data_final,max_pri_y,min_pri_y)
      data_final <- preprocessed_data$data_frame1
      data_ends <- preprocessed_data$data_frame2
    }
    if (chart_category != 'stacked bar'){
      line_lab<-preprocessed_data$line_lab
      max_factor<-preprocessed_data$max_factor
      min_factor<- preprocessed_data$min_factor
      y_min<- preprocessed_data$y_min
      y_max <- preprocessed_data$y_max
      x_min<- preprocessed_data$x_min
      x_max <- preprocessed_data$x_max
      x_ref<- preprocessed_data$x_ref
      print('line_labSS')
    }
    print(chart_category)
    print(chart_type)
    print('In stacked bar')
    data_unit=function_param[[1]][[1]]
    negative=as.logical(function_param[[1]][[2]])
    show_all=as.logical(function_param[[1]][[3]])
    show_older=as.logical(function_param[[1]][[4]])
    SIDE_BAR=as.logical(function_param[[1]][[5]])
    DATE_HEADER=as.logical(function_param[[1]][[6]])
    surplus=as.logical(function_param[[1]][[7]])
    round_integer=as.logical(function_param[[1]][[8]])
    YTD=as.logical(function_param[[1]][[9]])
    format_date=function_param[[1]][[10]]
    order_stack=as.logical(function_param[[1]][[11]])
    top_column=function_param[[1]][[12]]
    led_position=function_param[[1]][[13]]
    bar_thick=function_param[[1]][[14]]
    add_std_col=as.logical(function_param[[1]][[15]])
    print(top_column)
    print(YTD)
    if(DATE_HEADER==TRUE){
      prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(0,"month")))
      
    }else{prev_month<- as.Date(timeLastDayInMonth(Sys.Date()-duration(1,"month")))}
    
    
    print(prev_month)
    data_final<- data_final[data_final$Relevant_Date<=prev_month,]
    print(data_final)
    
    x_max=max(data_final$Month)
    x_min=min(data_final$Month)
    print(paste0('x_min-->',x_min))
    print(paste0('x_min-->',x_min))
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
    
    print(paste("y_min:",y_min))
    print(paste("y_max:",y_max))
    max_date_actual=as.Date(max(data_final$Month))
    if (YTD==TRUE)
    {
      data_final['Month']=apply(data_final,1,f1)
      data_final$Month <-as.Date(data_final$Month)
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
    
    if (YTD==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
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
                   format(max(data_ends$Month),format="%y"),")")
      
    }else{
      sub_h=paste0(data_unit , "FY",first_f_year,"-","FY",
                   current_f_year," (",current_month," '",
                   format(max(data_ends$Month),format="%y"),")")
      
    }
    if (YTD==TRUE){
      data_ends <- data_final %>% filter(Month == Month[length(Month)])
      
    }
    print('this is y max')
    print(y_max)
    print(data_final)
    data_final$value_y_left<-roundexcel(data_final$value_y_left,digits=0)
    print(data_final)
    ######################################################GRAPH###################
    print(paste('x_max:',x_max))
    print(paste('x_min:',x_min))
    print(paste('special_case_cy:',special_case_cy))
    if (special_case_cy=='True'){
    stacked_bar=ggplot(data=data_final)+
      geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
               position=position_stack(),show.legend = FALSE,
               width = 100,color = "black") +
      
      geom_text(
        aes(
          x = Month, y = value_y_left, fill = category,
          label = scales::comma(value_y_left)
        ),
        fontface = "bold",
        data = na.omit(data_final),  # Filter out missing values
        stat = "identity",
        position = position_stack(),  # Center the labels vertically
        vjust = -1,
        hjust = 0.5,  # Center the labels horizontally
        size = 3.5,
        family = "Arial"
      ) +
      scale_fill_manual(values=my_line_type)+
      scale_colour_manual(values=my_legends_col)+
      
      scale_y_continuous(expand=expansion(mult = c(0,0.1)),
                         breaks = seq(0, y_max, length.out = 6),
                         labels =number_format(big.mark = ",",
                                               style_positive = c("none"),
                                               style_negative = c("parens")))+
      coord_cartesian(ylim =c(0,y_max + 0.1 * y_max))+
      scale_x_date(limits =as.Date(c(x_min-345,x_max+300)),
                   labels = function(x) paste0("CY - ", substring(year(x), 3)),
                   breaks =function(x) seq.Date(from = min(x),to = max(x) ,by='1 year'),
                   expand =c(0.01,0))+
      
      guides(fill =guide_legend(order =1,reverse=TRUE))+
      common_theme()+
      siam_cutom_theme()+  
      
      theme(
        axis.text.x = element_text(face = "bold",size=10),  
        axis.text.y = element_text(face = "bold",size=10)
      )
    }else{
      stacked_bar=ggplot(data=data_final)+
        geom_bar(aes(x=Month,y=(value_y_left),fill=category),stat="identity",
                 position=position_stack(),show.legend = FALSE,
                 width = 100,color = "black") +
        
        geom_text(
          aes(
            x = Month, y = value_y_left, fill = category,
            label = scales::comma(value_y_left)
          ),
          fontface = "bold",
          data = na.omit(data_final),  # Filter out missing values
          stat = "identity",
          position = position_stack(),  # Center the labels vertically
          vjust = -1,
          hjust = 0.5,  # Center the labels horizontally
          size = 3.5,
          family = "Arial"
        ) +
        scale_fill_manual(values=my_line_type)+
        scale_colour_manual(values=my_legends_col)+
        
        scale_y_continuous(expand=expansion(mult = c(0,0.1)),
                           breaks = seq(0, y_max, length.out = 6),
                           labels =number_format(big.mark = ",",
                                                 style_positive = c("none"),
                                                 style_negative = c("parens")))+
        coord_cartesian(ylim =c(0,y_max + 0.1 * y_max))+
        scale_x_date(limits =as.Date(c(x_min-345,x_max+300)),
                     labels = function(x) sprintf("%d-%02d", year(x), (year(x) %% 100) + 1),
                     breaks =function(x) seq.Date(from = min(x),to = max(x) ,by='1 year'),
                     expand =c(0.01,0))+
        
        guides(fill =guide_legend(order =1,reverse=TRUE))+
        common_theme()+
        siam_cutom_theme()+  
        
        theme(
          axis.text.x = element_text(face = "bold",size=10),  
          axis.text.y = element_text(face = "bold",size=10)
        )
      
    }
    #ggsave("D:/Adqvest/NIIF_files/intermediate_plot.png", plot = stacked_bar, device = "png", width = 6, height = 4)
    print('done done creating CHART for SIAM')
    print(stacked_bar)
    ##########################################################RETURN######################
    return(list("chart"= stacked_bar,"s_header"=sub_h[1]))
    
  }, 
  error = function(e){
    print('In else block')
    print(e)
    print("An error occurred:\n")
    print("Error message:", e$message, "\n")
    print("Traceback:\n")
    print(traceback())
    print("Error occurred at line ", as.integer(sys.calls()[[1]]), ": ", conditionMessage(e), "\n")
    
  }
  )
}
heading_date_format<- function(slide_heading,max_date_format){
  if (grepl("cumulative", tolower(slide_heading))) {
    result <- paste0(slide_heading, max_date_format)
  } else {
    result <- slide_heading
  }
  return(result)
}
###################################  FUNCTIONS ##################################
call_function_table <- function(r_json_data_table,slide_heading,chart_title,QUATER,max_date_format,file_name){
  print('--------------------------------slide_heading----------------------------------')
  print(slide_heading)
  len <- length(r_json_data_table)
  for (i in 1:len){
    if (i==1){
      json_data_frame = r_json_data_table$table1
    }else if (i==2){
      json_data_frame = r_json_data_table$table2
    }else{
      json_data_frame = r_json_data_table$table3
    }
  df <- fromJSON(json_data_frame)
  print('********************************************')
  df$Category <- as.factor(df$Category)
  print('*********************************************')
  print(head(df))
  line_space=0
  row_height=0.05
  set_header_bg = 'white'
  padding_vals = list()
  make_bold = c()
  padding_vals = list()
  has_main_sector = FALSE
  has_units = FALSE
  color_coding = FALSE
  #rounder_exeptions =SIAM$Variable 
  background_vals =c(df$Category)
  hlines = c(df$Category)
  make_bold = c(df$Category)
  vlines= 1:length(df)-1
  vlines=vlines[-1]
  print(vlines)
  font_size = 12
  var_col_width = 3
  other_col_width = 0.5
  print(df)
  df <- df[, c("Category", setdiff(names(df), "Category"))]
  print('TRYING TO CREATE A TABLE')
  table_siam=siam_table(i,df,slide_heading,chart_title,QUATER, has_main_sector = FALSE,main_sector_bg = "GRAY 96",
  has_units = FALSE,rename_unit_col = 'Units',set_header_bg = set_header_bg,
  padding_vals = list(),make_bold = make_bold,make_col_bold=make_col_bold,hlines = hlines,
  vlines = vlines,background_vals = background_vals,color_coding = FALSE,alpha = 0.5,
  median_change = list(),invert_map = c(), 
  replace_null = '', caption = '',font_size = 12, var_col_width = 2,
  unit_col_width = 1,other_col_width = 0.61,line_space=0,row_height=row_height,
  yoy=FALSE)
  chart_header<-chart_title[[1]]
  if (i==1){
    print('Assing variable for chart 1')
    chart1 = table_siam
    slide_heding1<-slide_heading[[i]]
    slide_heding1<-heading_date_format(slide_heding1,max_date_format)
    print(slide_heding1)
  } else if (i==2){
    print('Assing variable for chart 2')
    chart2 = table_siam
    slide_heding2<-slide_heading[[i]]
    slide_heding2<-heading_date_format(slide_heding2,max_date_format)
    print(slide_heding2)
  }else{
    print('Assigning variable for chart3')
    chart3 = table_siam
    slide_heding3<-slide_heading[[i]]
    slide_heding3<-heading_date_format(slide_heding3,max_date_format)
    print(slide_heding3)

  }
  }
  if (QUATER=='False'){
    Chart_heading<-paste0(chart_header)
  }else{
    Chart_heading<-chart_header
    Chart_heading<-heading_date_format(Chart_heading,max_date_format)
  }
  c1_th=1
  t1_h=3
  t2_h=3
  l1=1.19
  w2=10
  h=0.1
  if (len==3){
    print('TRYING TO MAKE SINGLE CHART')
    my_ppt=siam_3_table(Chart_heading,
                        chart1,chart2,chart3,
                        slide_heding1,slide_heding2,slide_heding3,
                        c1_th=1,t1_h=3,t2_h=3,
                        l1=1.19,w2=10,h=0.1
    )
  }else{
    print('TRYING TO CREATE TWO TABLE')
    my_ppt=niif_hm_2_table(Chart_heading,
                        chart1,chart2,
                        slide_heding1,slide_heding2,
                        c1_th=1,t1_h=3,t2_h=3,
                        l1=1.19,w2=10,h=0.1)
  }
  print('TRYING TO MAKE PPT TABLE')
  print(file_name)
  print(my_ppt,target=file_name)
  print('Done')
  return (chart1)
  }


call_function <- function(json_data,page, chart_creat_type, chart_type_case, chart_category_case, my_chart_col,
                          my_legends_col_exl, x_axis_interval, chart_key_param, no_col_row,
                          chart_pri_axis, graph_lim,chart_source,chart_title,
                          slide_heading,function_param,QUATER,special_case,file_name,max_date_format) {
  tryCatch({
    len <- length(json_data)
    print('This is the length of json data')
    print(len)
    for (i in 1:len)  { 
      index=1
      special_case_cy=special_case[[i]][[1]]
      print('THIS IS SPECIAL CASE')
      print(special_case_cy)
      key_spacing = as.numeric(chart_key_param[[i]][[1]])
      max_overlaps = as.numeric(chart_key_param[[i]][[2]])
      h_just_line = as.numeric(chart_key_param[[i]][[3]])
      v_just_line = as.numeric(chart_key_param[[i]][[4]])
      h_line=as.numeric(chart_key_param[[i]][[5]])
      v_line=as.numeric(chart_key_param[[i]][[6]])
      num_brek = as.numeric(chart_key_param[[i]][[7]])
      
      n_col = as.integer(no_col_row[[i]][1])
      n_row = as.integer(no_col_row[[i]][2])
      print('Printing chart pri axis')
      print(n_col,n_row)
      print(as.integer(chart_pri_axis[[i]][2]))
      max_pri_y = as.integer(chart_pri_axis[[i]][1])
      min_pri_y = as.integer(chart_pri_axis[[i]][2])
      print('TRYING TO PRINT MY LEGENDS')
      my_legends_col = my_legends_col_exl[[i]]
      my_legends_col <- unlist(my_legends_col)
      my_line_type = my_chart_col[[i]]
      my_line_type <- unlist(my_line_type)
      print('Trying to print graph lim')
      graph_lim <- graph_lim[i]
      print(chart_type_case[i])
      print(chart_category_case[i])
      chart_type<-unlist(chart_type_case[i])
      chart_category<-unlist(chart_category_case[i])
      print(chart_category)
      print('done assigning data variable')
      if (i==1){
        json_data_frame = json_data$data1
      }else if (i==2){
        json_data_frame = json_data$data2
      }else if (i==3){
        json_data_frame = json_data$data3
      }else{
        json_data_frame = json_data$data4
      }
      df <- fromJSON(json_data_frame)
      print('********************************************')
      df$Relevant_Date <- as_datetime(df$Relevant_Date / 1000)
      df$Month <- as_datetime(df$Month / 1000)
      df$Relevant_Date <- as.Date(df$Relevant_Date)
      df$Month <- as.Date(df$Month)
      df$category <- as.factor(df$category)
      print('*********************************************')
      print(head(df))
      max_pri_y=max(df$value_y_left)
      add_val=graph_extend(df)
      max_pri_y=max_pri_y+5
      min_pri_y=0
      SIAM_chart = charts(i,df,x_axis_interval, chart_type,chart_category,max_pri_y,min_pri_y,
                          key_spacing,max_overlaps,h_just_line,v_just_line,n_col,n_row,num_brek,
                          my_legends_col,my_line_type,graph_lim = graph_lim, function_param=function_param[i],special_case_cy=special_case_cy)
      print('done charts starting assingningSS')
      chart_header<-chart_title[[1]]
      if (i==1){
        print('Assing variable for chart 1')
        chart1 = SIAM_chart$chart
        title_chart1 = SIAM_chart$s_header
        slide_heding1<-slide_heading[[i]]
        print(slide_heding1)
        matches <- regmatches(title_chart1, gregexpr("\\(\\w+ '\\d{2}", title_chart1))
        title_chart1 <- sub("\\(", "", unlist(matches))
        source_chart1 = chart_source[i]
      } else if (i==2){
        print('Assigning variable for chart2')
        chart2 = SIAM_chart$chart
        slide_heding2<-slide_heading[[i]]
        print(slide_heding2)
        title_chart2 = SIAM_chart$s_header
        source_chart2 = chart_source[i]
      }else if (i==3){
        print('Assigning variable for chart3')
        chart3 = SIAM_chart$chart
        slide_heding3<-slide_heading[[i]]
        print(slide_heding3)
        title_chart3 = SIAM_chart$s_header
        source_chart3 = chart_source[i]
      }else {
        print('Assigning variable for chart4')
        chart4 = SIAM_chart$chart
        slide_heding4<-slide_heading[[i]]
        print(slide_heding4)
        title_chart4 = SIAM_chart$s_header
        source_chart4 = chart_source[i]
      }
    }
    if (QUATER=='False'){
      Chart_heading<-paste0(chart_header,title_chart1)
    }else{
      Chart_heading<-heading_date_format(chart_header,max_date_format)
    }
    print(Chart_heading)
    ############slide ##########
    print('Calling slide function')
    if (len==1){
      print('TRYING TO MAKE SINGLE CHART')
      my_ppt=Single_Chart(" ",
                          chart1,
                          
                          
                          slide_heding1,
                          " ",
                          
                          
                          source_chart1,
                          Chart_heading
      )
    }else if(len==2){
      print('TRYING TO MAKE TWO CHART')
      my_ppt=Two_Chart(" ",
                       chart1,
                       chart2,
                       
                       slide_heding1,
                       " ",
                       
                       slide_heding2,
                       " ",
                       
                       source_chart1,
                       source_chart2,
                       
                       
                       Chart_heading
      )
    }else if (len==3){
      print('TRYING TO MAKE THREE CHART')
      my_ppt=Three_Chart_With_Box(" ",
                                  chart1,
                                  chart2,
                                  chart3,
                                  
                                  slide_heding1,
                                  " ",
                                  
                                  slide_heding2,
                                  " ",
                                  slide_heding3,
                                  " ",
                                  
                                  source_chart1,
                                  source_chart2,
                                  source_chart3,
                                  
                                  c1_n="",c2_n="",c3_n="",
                                  Chart_heading
      )
    }else{
      print('TRYING TO MAKE FOUR CHART')
      my_ppt=Four_Chart(
        chart1,
        chart2,
        chart3,
        chart4,
        
        slide_heding1,
        " ",
        
        slide_heding2,
        " ",
        slide_heding3,
        " ",
        slide_heding4,
        " ",
        
        
        Chart_heading
      )
    }
    print('TRYING TO MAKE PPT')
    print(file_name)
    print(my_ppt,target=file_name)
    print('Done')
    return (SIAM_chart)
  },
  error = function(e) {
    print('In else block')
    print(e)
    print("An error occurred:\n")
    print("Error message:", e$message, "\n")
    print("Traceback:\n")
    print(traceback())
    print("Error occurred at line ", as.integer(sys.calls()[[1]]), ": ", conditionMessage(e), "\n")
  })
}