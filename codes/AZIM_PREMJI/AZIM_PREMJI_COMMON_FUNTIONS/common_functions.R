
Adqvest_Properties = read.delim("C:\\Users\\Administrator\\AdQvestDir\\codes\\R_Db_Properties\\AdQvest_properties.txt",
                                sep = " ", header = T)

#CLICK
Adqvest_Clickhouse=read.delim("C:\\Users\\Administrator\\AdQvestDir\\codes\\R_Db_Properties\\Adqvest_ClickHouse_properties.txt",
                              sep = "", header = T)

#PG_ADMIN
Adqvest_Prd_Thurro=read.delim("C:\\Users\\Administrator\\AdQvestDir\\codes\\R_Db_Properties\\Postgress_properties_prod.txt", 
                              sep = "", header = T)

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

#
#Database_Connection_PGADMIN


host_pg     = unlist(as.character(Adqvest_Prd_Thurro[3,2]))
port_pg     = as.numeric(as.character(Adqvest_Prd_Thurro[4,2]))
DB_pg       = as.character(Adqvest_Prd_Thurro[5,2])
user_pg     = unlist(strsplit(as.character(Adqvest_Prd_Thurro[1,2]),':|@'))[1]
password_pg = unlist(as.character(Adqvest_Prd_Thurro[2,2]))


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
    
    print(final_query)
    data = dbGetQuery(clk,final_query)
    
    if (frequency == 'daily'){
      print('daily data')
    }else {
      data$Relevant_Date <- as.Date(timeLastDayInMonth(data$Relevant_Date))
    }
    
    dbDisconnect(clk)
    dbDisconnect(pg)
    
    
    data=data[!duplicated(data[c("Relevant_Date")]), ]
    return (data)
  }, 
  error = function(e){
    print(e)
    
  })
  
}

data_query_clk_pg=function(id,period=60,exception=FALSE,surplus=FALSE,
                           quarter=FALSE,year=FALSE){
  
  tryCatch({
    
    clk  = DBI::dbConnect(RClickhouse::clickhouse(), 
                          dbname =DB_cl,host = host_cl, 
                          port = port_cl,user = user_cl,password =password_cl)
    
    pg  = DBI::dbConnect(RPostgres::Postgres(),
                         dbname =DB_pg,host = host_pg, 
                         port = port_pg,user = user_pg,
                         password =password_pg)
    
    
    
    qstring='select * from widgets_new where widget_id=chart_id'
    quary_by_id=str_replace(qstring,"chart_id",glue({id}))
    
    res =dbGetQuery(pg,quary_by_id)
    d1=res[,"query_param"]
    descrip=res[,"title"]
    data_upto=res[,"data_upto"]
    source=paste0("Source: ","Thurro, ",res[,"source"][1])
    query_1=fromJSON(d1, flatten=TRUE)
    y_lab=query_1[7][[1]][[1]]
    plot_type=query_1[6][[1]][[1]]
    
    if (quarter==TRUE){
      final_query=str_replace(query_1[1],"@@periodQtr@@", glue({period}))
      
    }
    else{final_query=str_replace(query_1[1], "@@period@@", glue({period}))}
    
    
    
    
    final_data_frame = dbGetQuery(clk,final_query)
    
    result = list("data_frame" = final_data_frame,
                  "source"=source)
    
    dbDisconnect(clk)
    dbDisconnect(pg)
    print(final_query)
    
    
    return(result)
  }, 
  error = function(e){
    print(e)
  }
  )
  
}
re_arrange_columns=function(df,sort_type="",top_col=''){
  df1=df[df$Relevant_Date==max(df$Relevant_Date),]
  df1= reshape2::melt(df1,id=c("Relevant_Date"))
  if (sort_type=='desc'){
    ordered_df = df1 %>%arrange(desc(value))
  }else{
    ordered_df = df1 %>%arrange((value))
  }
  
  new_col=unique(c(ordered_df$variable))
  if (top_col!=''){
    new_col = subset(new_col, new_col != top_col)
    col_n=c('Relevant_Date',top_col)
  }else{
    col_n=c('Relevant_Date')
  }
  
  for (i in new_col){
    print(i)
    col_n=c(col_n,i)
  }
  lght_std_col=c("#4F81BD","#C0504D",'#9BBB59')
  lght_std_col=c('#4572A7','#AA4643','#89A54E','#71588F','#4F81BD','#DB843D',
                 '#93A9CF')
  
  lght_std_col=rev(lght_std_col)[1:length(col_n)-1]
  org_col = subset(col_n, col_n != 'Relevant_Date')
  my_shorted_col=setNames(lght_std_col, rev(org_col))
  
  return(list("columns"= col_n,"colours"=my_shorted_col))
  
}
roundexcel = function(x,digits=0){
  posneg = sign(x)
  z = abs(x)*10^digits
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^digits
  z*posneg
}
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
mon_year_df_creator=function(df,keep_col=c("Relevant_Date"),Sum_date=FALSE,
                             match_month='-03-31',ffill=FALSE){
  
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
  
  return (df_2)
}
get_val_pie=function(df){
 val_pp= c('','','22.3','34.6','74.8','128.4','138.9','222.0')
 original_list<-df$Value
 #get values for label
 modified_list <- lapply(original_list, function(x) ifelse(x < 4, '', as.character(x)))
 modified_string <- paste(paste0('"', modified_list, '"'), collapse = ', ')
 
 # Trim leading and trailing commas
 modified_string <- gsub("^,|,$", "", modified_string)
 
 print(val_pp)
 return (val_pp)
}
create_df_for_chart=function(df1,df2=as.data.frame(),
                             Stack_Bar_Line=FALSE,
                             bar_line=TRUE,
                             Stack_bar=FALSE,mtlti_line=FALSE,
                             Show_Older=FALSE,SHOW_FU_DATE=FALSE,
                             Order_Stack=FALSE,top_column1=''){
  
  
  
  if (Stack_Bar_Line==TRUE){
    
    df2=Reduce(function(x, y) merge(x, y, all=TRUE),df2)
    df2=melt(df2,id=c("Relevant_Date"))
    df2=na.omit(df2)
    
    names(df2)[which(names(df2)=='variable')]='category_r'
    names(df2)[which(names(df2)=='value')]='value_y_right'
    df2=df2[,c("Relevant_Date","value_y_right","category_r")]
    
    
    df1=Reduce(function(x, y) merge(x, y, all=TRUE),df1)
    if (Order_Stack==TRUE){
      my_chart_col=re_arrange_columns(df1,sort_type ='',
                                      top_col =top_column1)[[2]]
      
      df1=df1[,re_arrange_columns(df1,sort_type ='',top_col =top_column1)[[1]]]
    }
    
    df1=melt(df1,id=c("Relevant_Date"))
    df1=na.omit(df1)
    
    names(df1)[which(names(df1)=='variable')]='category_left'
    names(df1)[which(names(df1)=='value')]='value_y_left'
    df1=df1[,c("Relevant_Date","value_y_left","category_left")]
    
    
    #special_case:For Dual axis line Chart
    #special_case_2:When no of data in left is lesser than right
    if(nrow(df1)!=nrow(df2)){
      df_final=merge(df1, df2, by="Relevant_Date",all=T)
      df_final=do.call(data.frame,
                       lapply(df_final,function(x) replace(x, is.infinite(x), NA))) 
      
      
      
    }else{
      df_final=cbind(df1,df2,by="Relevant_Date")
      
      df_final=df_final[,c("Relevant_Date","value_y_left","category_r",
                           "value_y_left","category_left")]
      
      
      df_final=do.call(data.frame,
                       lapply(df_final,function(x) replace(x, is.infinite(x), NA))) 
      df_final=na.omit(df_final)
    }
  }
  else if(Stack_bar==TRUE){
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
    
    df1$Relevant_Date = as.Date(df1$Relevant_Date)
    df1 = df1[order(df1$Relevant_Date),]
    
    
    
    df1$Month = as.Date(df1$Relevant_Date, format = "%Y-%m-%d")
    max_overlap=10
    
    
    df1$value_y_left=df1[1:nrow(df1),3] 
    df1$category=df1[1:nrow(df1),2] 
    
    
    df_final=df1[,c("Relevant_Date","value_y_left","category")]
    
    
  }else if(mtlti_line==TRUE){
    
    df1=Reduce(function(x, y) merge(x, y, all=TRUE),df1)
    df1=melt(df1,id=c("Relevant_Date"))
    df1=na.omit(df1)
    
    
    names(df1)[which(names(df1)=='variable')]='category_left'
    names(df1)[which(names(df1)=='value')]='value_y_left'
    df_final=df1[,c("Relevant_Date","value_y_left","category_left")]
    df_final$Relevant_Date = as.Date(df_final$Relevant_Date, format = "%Y-%m-%d")
    
  }
  
  df_final=df_final[order(df_final$Relevant_Date),]
  if (SHOW_FU_DATE==FALSE){
    print(df_final)
  }
  
  
  
  if (Order_Stack==TRUE){
    return(list('df'=df_final,"chart_col"=my_chart_col))
    
  }else{
    return(list('df'=df_final))
  }
  
  
  
}
get_fy_qtr_frm_month=function(dt,output_format='financial_year'){
  fis_qua <- c(4,4,4,1,1,1,2,2,2,3,3,3)
  fis_month <- c(10,11,12,1,2,3,4,5,6,7,8,9)
  qtr_end_months=c(3,3,3,6,6,6,9,9,9,12,12,12)
  dt=as.Date(dt)
  fy=ifelse(month(dt)>3,year(dt)+1,year(dt))
  fq=fis_qua[month(dt)]
  
  # print( sprintf('%02d', as.numeric(qtr_end_months[month(dt)])))
  if (output_format=='quarters'){
    out_date=paste0(year(dt),'-',sprintf('%02d', as.numeric(qtr_end_months[month(dt)])),'-','28')
  }else if (output_format=='financial_year'){
    out_date=paste0(fy,'-03-31')
    
  }else if (output_format=='quarter_number_fy'){
    out_date=paste0(fq,'Q',substr(fy, 3, 4))
    
  }
  return(out_date)
  
}
get_eo_fy_dates=function(df){
  df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
  df['FY_Year']=apply(df['Relevant_Date'],1,f2)
  max_dt=max(df$Relevant_Date)
  df$FY_Year <- as.Date(df$FY_Year)
  df1=df[df$Relevant_Date %in% (unique(df$FY_Year)),]
  df2=df[df$Relevant_Date==max_dt,]
  df=rbind(df1,df2)
  df=df[!duplicated(df[c("Relevant_Date")]), ]
  
}
f1=function(x){
  get_fy_qtr_frm_month(x,output_format ='quarters')
}
f2=function(x){
  get_fy_qtr_frm_month(x,output_format ='financial_year')
}
f3=function(x){
  get_fy_qtr_frm_month(x,output_format ='quarter_number_fy')
}

table_preprocessing <- function(data,frequency_normalizer = '',unit= 'NA',variable = 'NA',sector = 'NA',calculate_gr = FALSE,divisor = 0,rounder = 0){
  
  tryCatch({
    
    
    last_date <- today()
    start_date <- timeFirstDayInMonth(last_date %m-% months(12))
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
    print(e)
  })
  
}

table_preprocessing_annual = function(df,frequency_normalizer = '',
                                          period=3,unit= 'NA',
                                          variable = 'NA',sector = 'NA',
                                          base_year='2019-03-31',
                                          make_max_dt_val_na=FALSE,
                                          divisor = 0,rounder = 0,fy_format=FALSE,do_sum=c()){
      
      tryCatch({
        df$Relevant_Date=as.Date(timeFirstDayInMonth(df$Relevant_Date))
        
        max_date = max(df$Relevant_Date)
        min_date_org = min(df$Relevant_Date)
        
        start_dt = (max_date %m-% years(as.numeric(period)))
        end_dt = (max_date)
        all_dates = format(seq(start_dt,end_dt,by=frequency_normalizer), "%Y-%m-%d")
        dt_df = data.frame(Relevant_Date = c(all_dates))
        dt_df$Relevant_Date = as.Date((dt_df$Relevant_Date))
        
        max_date = max(dt_df$Relevant_Date)
        
        min_date = min(dt_df$Relevant_Date)
        
        if ((as.Date(start_dt)!=as.Date(min_date_org))==TRUE){
          dt_df['Value'] = NA
          dt_df=dt_df[(dt_df$Relevant_Date)<as.Date(min_date_org),]
          df = rbind(df,dt_df) 
          df = df[order(as.Date(df$Relevant_Date, format="%Y-%m-%d")), ]
          rownames(df) = seq(length=nrow(df))
          df = df[df$Relevant_Date >= min_date &  df$Relevant_Date <= max_date, ]
          df[is.na(df)] = 0
          
        }else{
          df=merge(dt_df,df,by="Relevant_Date")
          dt_df['Value'] = NA
          df$Value = df$Value + 1
          
          df = rbind(df,dt_df) 
          df = df[order(as.Date(df$Relevant_Date, format="%Y-%m-%d")), ]
          
          rownames(df) = seq(length=nrow(df))
          df[is.na(df)] = 0
          
          df = aggregate(df['Value'], by=list(df$Relevant_Date), FUN=sum)
          names(df)[1]="Relevant_Date"
          names(df)[2]="Value"
          df = df[df$Relevant_Date >= min_date &  df$Relevant_Date <= max_date, ]
          df$Value = df$Value - 1
          df$Value = ifelse(df$Value == -1 ,NA, df$Value)
        }
        
        df = df[order(as.Date(df$Relevant_Date, format="%Y-%m-%d")), ]
        df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
        max_dt=max(df$Relevant_Date)
        f1=function(x){get_fy_qtr_frm_month(x,output_format ='quarters')}
        f2=function(x){get_fy_qtr_frm_month(x,output_format ='financial_year')}
        
        df['FY_QTR']=apply(df['Relevant_Date'],1,f1)
        df['FY_Year']=apply(df['Relevant_Date'],1,f2)
        # df <- df %>% mutate_at(c(which(names(df)=="FY_QTR")), as.Date)
        # df <- df %>% mutate_at(c(which(names(df)=="FY_Year")), as.Date)
        # 
        # df$FY_QTR=timeLastDayInMonth(df$FY_QTR)
        # df$FY_Year=timeLastDayInMonth(df$FY_Year)
        
        if (length(do_sum)!=0){
          for (i in do_sum){
            if (i=='quarter'){
              df = df %>%group_by(FY_QTR=as.Date(FY_QTR)) %>%summarize(Value = sum(Value))
              df['Relevant_Date']=df$FY_QTR
            }else if(i=='year'){
              df = df %>%group_by(FY_Year=as.Date(FY_Year)) %>% summarize(Value = sum(Value))
              df['Relevant_Date']=df$FY_Year
            }else if(i=='month'){
              df = df %>%group_by(Relevant_Date=as.Date(Relevant_Date)) %>% summarize(Value = sum(Value))
              
            }
          }
        }
        # names(df)[1]='Relevant_Date'
        if (frequency_normalizer=='year'){
          df$FY_Year <- as.Date(df$FY_Year)
          df1=df[df$Relevant_Date %in% (unique(df$FY_Year)),]
          df2=df[df$Relevant_Date==max_dt,]
          df=df
          
        }else if(frequency_normalizer=='quarters'){
          df1=df[df$Relevant_Date %in% (unique(df$FY_QTR)),]
          df2=df[df$Relevant_Date==max_dt,]
          df=rbind(df1,df2)
        }
        df=df[!duplicated(df[c("Relevant_Date")]), ]
        df=df[df$Relevant_Date>=base_year,]
        if (max_dt==max(df$FY_Year) & (max_dt<Sys.Date())){
          df_n=df[df$Relevant_Date==max(df$Relevant_Date),]
          df_n[df_n$Relevant_Date==max(df_n$Relevant_Date),'Relevant_Date']=Sys.Date()
          df=rbind(df,df_n)
        }else{
          df[df$Relevant_Date==max(df$Relevant_Date),'Relevant_Date']=as.Date(Sys.Date())
        }
        df=df[!duplicated(df[c("Relevant_Date")]), ]
        
        df=df %>%   mutate(Relevant_Date =ifelse(Relevant_Date!=max(Relevant_Date),
                                                 format(Relevant_Date,'FY%y'),
                                                 format(Relevant_Date,"%b %d, %Y")))
        
        
        df=df[,c('Relevant_Date','Value')]
        print(df)
        if (divisor != 0){
          df["Value"] = df["Value"]/divisor
          
          df["Value"] = roundexcel(df["Value"], rounder)
        }
        else{
          df["Value"] = roundexcel(df["Value"], rounder)
        }
        
        df = transpose(df)
        names(df) = df[1,]
        df = df[-1,]
        print(df)
        if (unit != 'NA'){
          df$Units = unit
          df = df %>%select(Units, everything())
        }
        
        if (variable != 'NA'){
          df$Variable = variable
          df = df %>%
            select(Variable, everything())
        }
        
        if (sector != 'NA'){
          df$Sector = sector
          df = df %>%
            select(Sector, everything())
        }
        return (df)
      },
      error = function(e){
        print(e)
      })
      
    }
    
light_house_economic_table=function(df,font_type= "Times New Roman", 
                                        row_unit=list(),foot_note=list(),
                                        var_col_width=5.5,show_var_header=FALSE,
                                        row_bracket=c(),notes='',spc=1){
      
      df = df %>% mutate_at(vars(-Variable), as.numeric)
      # width(j = "X", width = .2) %>% empty_blanks(part='header')
      set_flextable_defaults(font.family = font_type)
      ft = flextable(df) 
      ft = theme_alafoli(ft) %>% 
        set_formatter(i =1:nrow(df),j=2:ncol(df),part = "body", align = "right")
      
      row_unit=row_unit_1
      if (length(row_unit) != 0){
        for (m in names(row_unit)){
          if (strsplit(m,'_')[[1]][2]=='pct'){
            val='%'
          }else{
            val=strsplit(m,'_')[[1]][2]
            print(val)
          }
          for (i in row_unit[[m]]){ 
            row_formula = as.formula(paste("~ Variable %in% '",i,"'",sep = ''))
            ft=colformat_double(ft,i=row_formula,j=2:ncol(df)-1,suffix=val,digits=1)
          }
        }
      }
      
      ft=ft %>% 
        width(j=names(df)[length(df)],1.25) %>% 
        width(j=2:length(df)-1,1.25) %>% 
        width(j='Variable',3)%>% 
        width(j=length(df),0.5)
      
      if (show_var_header==FALSE){
        ft=ft %>% 
          set_header_labels(values = list('Variable'=''))
      }
      
      if (length(row_bracket)!=0){
        for (m in row_bracket){
          row_indx=which(df$Variable==m)
          for (cl in 2:ncol(df)-1){
            # print(cl)
            neg=paste("(",as.character(df[row_indx,cl]),")",sep='')
            ft = compose(ft, i = row_indx, j = cl, as_paragraph(as_chunk(neg)))
          }
        }
      }
      
      
      # if (length(foot_note) != 0){
      #   for (m in names(foot_note)){
      #    
      #     note=strsplit(m,'_')[[1]][1]
      #     ref=strsplit(m,'_')[[1]][2]
      #     # print(note)
      #     # print(ref)
      #     for (i in foot_note[[m]]){
      #       row_formula = as.formula(paste("~ Variable %in% '",i,"'",sep = ''))
      #       ft=footnote(x = ft, i = row_formula, j = names(df)[length(df)],
      #                   ref_symbols =paste0("",ref),
      #                   inline = FALSE,sep = ',',
      #                   value = as_paragraph(note)) %>% 
      #         merge_v(part = "footer")
      #         # merge_h(part = "footer")
      # 
      #     }
      #   }
      # }
      
      
      
      ft <- ft %>% 
        line_spacing(i = NULL, j = NULL, space = 0.5, part = "body") %>% 
        valign(i = nrow(df), j = NULL, valign = "top", part = "body") %>%
        bold(bold = TRUE, part = "header") %>% 
        fontsize(i = NULL, j = NULL, size = 12, part = "header") %>% 
        fontsize(i = NULL, j = NULL, size = 12, part = "body") %>% 
        bold(bold = FALSE, part = "body") %>%
        color(color = "black", part = "all") %>%
        padding(padding = 5, part = "all")
      
      std_border <- fp_border(color = "black")
      ft <- border_outer(x = ft, border = std_border)
      
      ft <- ft %>%
        add_footer_lines(notes) %>%
        fontsize(i = NULL, j = NULL, size = spc, part = "footer") %>% 
      line_spacing(i = NULL, j = NULL, space = 2, part = "footer") 
      ft <- ft %>%
      width(j=' ',1) 
      
    }
create_qtr_fyear_df_from_mon=function(df,do_sum=c(),divisor=1,rounder=1,output_freq=c()){
  df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
  
  df['FY_QTR']=apply(df['Relevant_Date'],1,f1)
  df['FY_Year']=apply(df['Relevant_Date'],1,f2)
  
  if (length(do_sum)!=0){
    for (i in do_sum){
      if (i=='quarter'){
        df = df %>%group_by(FY_QTR=as.Date(FY_QTR)) %>%summarize(Value = sum(Value))
        df['Relevant_Date']=df$FY_QTR
        df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
      }else if(i=='year'){
        df = df %>%group_by(FY_Year=as.Date(FY_Year)) %>% 
          summarize(Value = sum(Value))
        df['Relevant_Date']=df$FY_Year
        df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
      }else if(i=='month'){
        df = df %>%group_by(Relevant_Date=as.Date(Relevant_Date)) %>% 
          summarize(Value = sum(Value))
        
      }
    }
  }
  if(length(output_freq)!=0){
    for (i in output_freq){
      if (i=='quarter'){
        ## Only Gives you Quarter End dates
        df$FY_QTR=as.Date(timeLastDayInMonth(df$FY_QTR))
        df=subset(df,Relevant_Date %in% c(unique(df$FY_QTR)))
      }else if(i=='year'){
        ## Only Gives you FY End dates
        df$FY_Year=as.Date(timeLastDayInMonth(df$FY_Year))
        df=subset(df,Relevant_Date %in% c(unique(df$FY_Year)))
        
      }
    }
  }
  if (divisor != 0){
    df["Value"] = df["Value"]/divisor
    df["Value"] = roundexcel(df["Value"], rounder)
  }
  else{
    df["Value"] = roundexcel(df["Value"], rounder)
  }
  df=df[,c('Relevant_Date','Value')]
  
  
  return(df)
}

adjust_max_fy_data=function(df,uni_day){
  if ((max(df$Relevant_Date) != uni_day)| (max(df$Relevant_Date)>Sys.Date())){
    df1=subset(df,Relevant_Date<max(df$Relevant_Date))
    df1=subset(df,Relevant_Date==max(df1$Relevant_Date))
    df[df$Relevant_Date==max(df$Relevant_Date),'Value']=df1$Value
    
  }
  return(df)
  
}
mon_qtr_df_creator=function(df,keep_col=c("Relevant_Date","Equity"),sum=TRUE){
  f2=function(x){
    max_date=as.Date(x[1])
    current_month=format(max_date,format="%b")
    current_month_num=format(max_date,"%m")
    current_f_year=format(max_date %m-% months(0))}
  if (sum==TRUE){
    df$quarter <- as.integer(format(df$Relevant_date, "%q"))
    quarterly_balance <- aggregate(trade_balance ~ quarter, data = trade_data, sum)
    
  }
  df["Qtr_mon"]=apply(df,1,f2)
  df_2 <- df %>% mutate_at(c(which(names(df)=="Qtr_mon")), as.Date)
  df_2["Relevant_Date"]=as.Date(timeLastDayInQuarter(df_2$Qtr_mon))
  df_2=df_2[1:nrow(df_2),c(1,3)]
  
  df$Relevant_Date=as.Date(timeLastDayInMonth(df$Relevant_Date))
  
  df1=merge(df_2,df,by="Relevant_Date")
  df1=df1[!duplicated(df1[c("Relevant_Date")]), ]
  df1=df1[,keep_col]
  
}
create_sum_Quarter<-function(df){
  colnames(df)=c("Relevant_Date","Value")
  df$Relevant_Date <- as.Date(df$Relevant_Date)
  df <- df %>%
    mutate(Quarter_Year = paste0(year(Relevant_Date), "_Q", quarter(Relevant_Date)))
  df_quarterly <- df %>%
    group_by(Quarter_Year) %>%
    summarize(Total_Value = sum(Value))
  
  return (df_quarterly)
}
get_qtr_for_month<-function(month, year,mon=FALSE) {
  if (mon==TRUE){
    if (month %in% c(7, 8, 9)) {
      date_string <- paste(year+2000, sprintf("%02d", 9), "01", sep = "-")
    } else if (month %in% c(1,2,3)) {
      date_string <- paste(year+2000, sprintf("%02d", 3), "01", sep = "-")
    } else if (month %in% c(4,5,6)){
      date_string <- paste(year+2000, sprintf("%02d", 6), "01", sep = "-")
    }else{
      date_string <- paste(year+2000, sprintf("%02d", 12), "01", sep = "-")
    }}else{
      if (month == 9) {
        return(year+1)
      } else if (month == 3) {
        return(year )
      } else if (month == 6){
        return(year+1 )
      }else{
        return(year+1 )
      }
      
    }
  return (date_string)
}
create_df_with_date_range<-function(df,date,great=FALSE){
  min_x_axis_date <- as.Date(date)
  if (great==TRUE){
    df <- df %>%
      filter(Relevant_Date >= min_x_axis_date)
  }else{
    df <- df %>%
      filter(Relevant_Date <= min_x_axis_date)
  }
  df<-as.data.frame(df)
  return (df)
}

extract_value_one_year_ago <- function(df) {
  # Convert Relevant_Date to Date type
  df$Relevant_Date <- as.Date(df$Relevant_Date)
  
  # Find the maximum date
  max_date <- max(df$Relevant_Date)
  
  # Calculate one year ago from the maximum date
  one_year_ago <- max_date - years(1)
  
  # Filter the dataframe for the value corresponding to one year ago
  value_one_year_ago <- df$Value[df$Relevant_Date == one_year_ago]
  
  # Return the value
  return(value_one_year_ago)
}
