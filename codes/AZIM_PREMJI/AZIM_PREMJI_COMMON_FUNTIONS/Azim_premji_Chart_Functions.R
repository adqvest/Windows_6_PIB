
#-----LODING PACKAGE----------------------------

library(pacman)
library(ggplot2)
library(ggplot2)
library(officer)
library(magrittr)

p_load(XML,downloader,varhandle,gdata,mFilter,Matrix,dplyr,igraph,reshape2,
       stringr,DBI,lubridate,RMySQL,zoo,plotrix,RCurl,magrittr,
       ggplot2,RPostgres,jsonlite,glue,sf,pracma,flextable,scales,
       ggnewscale,ggrepel,extrafont,showtext,grid,gtable,tibble,data.table,
       paletteer,tidyr,epitools,ggfittext,timeDate,
       ggalluvial,english,RClickhouse,officer,officedown,ggthemes,
       ggpattern,hms,
       install = TRUE)
font_import(pattern = "times.ttf")
loadfonts(device = "win")
# Import the contents of theme.R
source("Theme.R")
# Import the contents of font.R
#source("font.R")
################################HEADING DATE FUNCTION###########################
get_heading_date<-function(title='month',date){
  if (title=='FY'){
    year <- format(as.Date(date), "%y")
    formatted_date <- paste0("FY", year)
    
  }else if (title=='month'){
    formatted_date <- format(as.Date(date), "%b %d, %Y")
    
  }else{
    formatted_date <- format(date, "%B '%y")
    
    
  }
  return (formatted_date)
}
#-----PPT TEMPLATE----------------------------
f     ="C:\\Users\\Administrator\\AdQvestDir\\codes\\AZIM_PREMJI\\AZIM_PREMJI_COMMON_FUNTIONS\\azim_premji_template.pptx"
my_ppt=read_pptx(f)
#################################FONT RELATED works###############################
#LIGHT HOUSE FONT Times New Roman
font_paths()
t1=font_files() %>%tibble()
filtered_fonts <- t1 %>%
  filter(str_detect(family, "Times"))
print(filtered_fonts)
#path_to_timesbd.ttf
font_add(family='Times New Roman',regular ='TIMES.TTF')
print('done font')

############################## FORMATTING FUNCTIONS#########################
#format date for financial year
format_fy_date <- function(year) {
  paste0("FY", substr((year), 3, 4))
}
#format date month year
my_date_format <- function(x) {
  sprintf("%s-%02d", month.abb[as.POSIXlt(x)$mon + 1], (year(x) %% 100))
}
#format date quarter
get_quarter_label <- function(month, year,mon=FALSE) {
  if (mon==TRUE){
    if (month == 9) {
      return("2")
    } else if (month == 3) {
      return("4")
    } else if (month == 6){
      return("1")
    }else{
      return("3")
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
}
format_quat_date <- function(data_final_test) {
  # Assuming x is a vector of dates
  result <- vector(mode = "character", length = length(data_final_test))  # Specify mode as "character"
  for (i in 1:length(data_final_test)) {
    mon_num <- as.numeric(format(x[i], "%m"))
    if (mon_num == 9) {
      result[i] <- paste0("2Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) + 1))
    } else {
      result[i] <- paste0("4Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y"))))
    }
  }
  return(result)
}
# Function to get quarter label
get_quarter_label <- function(month, year, mon = FALSE) {
  if (mon == TRUE) {
    if (month == 9) {
      return("2")
    } else if (month == 3) {
      return("4")
    } else if (month == 6) {
      return("1")
    } else {
      return("3")
    }
  } else {
    if (month == 9) {
      return(year + 1)
    } else if (month == 3) {
      return(year)
    } else if (month == 6) {
      return(year + 1)
    } else {
      return(year + 1)
    }
  }
}
############################### CHART FUNCTIONS##############################

single_axis_chart_LH <- function(data1, x_axis_interval="24 month",
                                 graph_lim=10,
                                 legend_direc='vertical', legend_pos='right',
                                 legend_key_width=0.27, y_dot_rng=15,
                                 reverse_y=FALSE, key_spacing=0.10, 
                                 expand_axis=c(0.1,0), num_brek=8,
                                 y_axis_value='INR Exchange', FY=FALSE, 
                                 chart_type='line', axis_per=FALSE, 
                                 min_x_axis='', Quarter=FALSE,
                                 name_label='', title_chart="", dual_axis=FALSE,
                                 val=2, all_qua=FALSE, less_than=FALSE,
                                 Currency=FALSE, line_size=1, 
                                 axis_val_y=c(0,0), rev_val=FALSE,
                                 bar_thick=10, val_date_mon=c(1,2,3,4,5,6,7,8,9,10,11,12),
                                 font_size=10, font="Times New Roman", 
                                 legend_spacing=1, h_just_line=0.5, v_just_line=0.5,
                                 led_position='center', stat='identity',
                                 font_size_axis=9, show_leg=FALSE, 
                                 value_in_bar=FALSE, legend_spacing_y=1,
                                 bar_position='stack', SIDE_BAR=FALSE, 
                                 expand_y=c(0,0), nrow=4, ncol=1 , note='' ,
                                 val_margin=0,leg_box_height=0.5,
                                 leg_box_width=0.5,str_wrp_wid=5 , 
                                 value_in_line=FALSE ,size_txt= 3 , 
                                 get_last_month_interval= FALSE , 
                                 v_just_x=0.5 ,special=FALSE) {
  tryCatch({
    data1 <- data_s
    ##########################DATA_PROCESSING###########################
    
    if (chart_type != 'bar' && chart_type != 'line' && special != TRUE) {
      print(chart_type)
      Hour_minute <- FALSE
      show_older <- FALSE
      data_final <- create_df_for_chart(data1, mtlti_line = TRUE)[[1]]
      data_final <- subset(data_final, !is.infinite(value_y_left))
      data_final$Month <- data_final$Relevant_Date
      data_final$category <- data_final$category_left
      print(head(data_final, 2))
    } else {
      data_final <- data1
      data_final$Month <- data_final$Relevant_Date
      data_final$value_y_left <- data_final$Value
      data_final$category_left<-'Value'
      print(head(data_final, 2))
    }
    if (chart_type=='bar'){
      data_final$category<-'Value'
    }
    if (special==TRUE){
      print(data_final)
      data_final$category_left<-data_final$Variable
      data_final$category<-data_final$Variable
      data_final$value_y_left<-data_final$Value
    }
    
    if (min_x_axis != '') {
      min_x_axis_date <- as.Date(min_x_axis)
      
      # Filter data_final to get data greater than min_x_axis_date
      if (less_than == TRUE) {
        data_final <- data_final %>%
          filter(Relevant_Date < min_x_axis_date)
      } else {
        data_final <- data_final %>%
          filter(Relevant_Date >= min_x_axis_date)
      }
    }
    
    if (axis_val_y[1] == 0 && axis_val_y[2] == 0) {
      y_min <- axis_val_y[1]
      y_max <- axis_val_y[2]
    } else {
      y_min <- axis_val_y[1]
      y_max <- axis_val_y[2]
    }   
    
    x_min <- min(data_final$Relevant_Date)
    x_max <- max(data_final$Relevant_Date)
    print(paste('This is min of x:', x_min))
    print(paste('This is max of x:', x_max))
    print(paste('This is min of y:', y_min))
    print(paste('This is max of y:', y_max))
    
    # Assigning geom segment
    reference_line_1 <- geom_segment(x = x_min, y = 0,
                                     xend = x_max, yend = 0,
                                     color = "grey", size = 0.25,
                                     show.legend = FALSE,
                                     linetype = 1,arrow = NULL
                                     )
    a1 <- annotate("text", x = x_min, y = 0, label = "", angle = 0, size = 14, color = "grey")
    
    # Coordinates are important while assigning limits
    if (reverse_y == TRUE) {
      cordi <- coord_cartesian(ylim = c(y_max, y_min))
      cordi <- scale_y_reverse(limits = c(y_max, y_min))
    } else {
      cordi <- coord_cartesian(ylim = c(y_min, y_max))
    }
    print('this is y max')
    print(y_max)
    # Handles x axis labels
    format_quat_date <- function(x) {
      # Assuming x is a vector of dates
      result <- vector(mode = "character", length = length(x))  # Specify mode as "character"
      for (i in 1:length(x)) {
        mon_num <- as.numeric(format(x[i], "%m"))
        if (mon_num == 9) {
          result[i] <- paste0("2Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) + 1))
        } else {
          result[i] <- paste0("4Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y"))))
        }
      }
      return(result)
    }
    
    if (Quarter == TRUE) {
      if (chart_type=='multi_line'){
        print('Inside multi_line chart')
        data_final_test<-subset(data_final,month(data_final$Relevant_Date) %in% val_date_mon)
        scale_x_date_1 <- scale_x_date(
          breaks = data_final_test$Relevant_Date,
          labels = format_quat_date(data_final_test$Relevant_Date),
          expand = expand_axis)
      }else{
        data_final['FY_QTR']=apply(data_final['Relevant_Date'],1,f3)
        v1=unique(data_final$FY_QTR)
        qtrs <- as.numeric(gsub("\\D", "", x_axis_interval))+1
        print(qtrs)
        desired_qtrs <- v1[seq(1, length(v1), by = qtrs)]
        desire_dates=unique(data_final$Relevant_Date)
        
        names(desire_dates) <- unique(data_final$FY_QTR)
        breaks_2=c()
        desire_dates_2=c()
        for( i in names(desire_dates)){
          if (i %in% desired_qtrs){
            desire_dates_2=c(desire_dates_2,desire_dates[i][1][[1]])
            dts <-as.Date(format(desire_dates[i][1][[1]],format="%Y-%m-%d"))
            desire_dates_2=c(desire_dates_2,.Date(as.numeric(dts)))
          }
        }
        
        desire_dates_2 <- .Date(as.numeric(desire_dates_2))
        desire_dates_2=unique(desire_dates_2)
        
        print('--------------------------------------------')
        print(length(breaks_2))
        print(breaks_2)
        print('--------------------------------------------')
        print(length(desire_dates))
        print(desire_dates)
        print('--------------------------------------------')
        
        
        scale_x_date_1 <- scale_x_date(limits = as.Date(c(NA, x_max+graph_lim)),
                                       labels =desired_qtrs,
                                       breaks =desire_dates_2,
                                       expand = expand_axis)
      }
    } else if (FY == TRUE) {
      data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
      scale_x_date_1 <- scale_x_date(breaks = data_final_test$Relevant_Date,
                                     labels = function(x) format_fy_date(year(x)),
                                     expand = expand_axis)
    } else if (get_last_month_interval == TRUE) {
      latest_date <- max(data_final$Relevant_Date)
      min_date <- min(data_final$Relevant_Date)
      
      # Generate breaks with a 3-month gap from the latest date until the minimum date
      breaks <- generate_sequence(latest_date, min_date, "-3 months")
      
      print(breaks)
      # Use scale_x_date with the breaks
      scale_x_date_1 <- scale_x_date(
        breaks = breaks,
        labels = date_format("%b-%y"),
        expand = expand_axis
      )}else{
      data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
      scale_x_date_1 <- scale_x_date(breaks = data_final_test$Relevant_Date,
                                     labels = date_format("%b-%y"),
                                     expand = expand_axis)
    }
    if (SIDE_BAR==TRUE){
      bar_position = position_dodge(width = bar_thick,preserve="total")
      
      
    }else{
      bar_position ="stack"
    }
    #Creating a chart with bar or line 
    if (chart_type == 'multi_line') {
      
      geom_chart <- geom_line(aes(x = Month, y = value_y_left, color = category, 
                                  group = category), linetype = "solid", size = line_size, show.legend = TRUE)
    } else if (chart_type == 'line') {
      print('not a multi line')
      geom_chart <- geom_line(aes(x = Month, y = value_y_left,show.legend=TRUE),
                              color = "#5c8bc3", size = line_size, group = 1,
                              linetype = 1)
    } else {
      geom_chart <- geom_bar(aes(x = Month, y = (value_y_left), fill = category), 
                             stat = 'identity',
                             position = bar_position, show.legend = show_leg,
                             width = bar_thick)
    }
    
    print(axis_per)
    #Y aixs formating
    #scale_y_continuous(labels = function(x) format(x, scientific = FALSE))
    if (dual_axis != TRUE) {
      scale_y_value <- scale_y_continuous(
        expand = expansion(mult = expand_y),
        breaks = pretty_breaks(n = num_brek),
        labels = function(x) {
          if (axis_per == TRUE) {
            ifelse(x < 0, paste0(format(x, digits = 2, scientific = FALSE), "%"), 
                   paste0(format(x, digits = 2, scientific = FALSE), "%"))
          } else {
            ifelse(
              x < 0,
              paste0(format(x, big.mark = ",", digits = 2, scientific = FALSE)),  # Keep negative values as they are
              paste0(format(abs(x), big.mark = ",", digits = 2, scientific = FALSE)) )
          }
        })
    } else {
      scale_y_value <- scale_y_continuous(ylim(y_min, y_max), breaks = pretty_breaks(n = num_brek), 
                                          labels = function(x) paste0(format(x * 1, digits = 2), "%"), 
                                          sec.axis = sec_axis(~ .*1, name = 'CRR (RHS)', 
                                                              breaks = pretty_breaks(n = num_brek),
                                                              labels = function(x) paste0(format(x, digits = 2), "%")))
    }
    data_final$Quarter <- paste0(data_final$quarter, "Q", data_final$year_adjusted)
    print(data_final)
    print(max(data_final$Relevant_Date))
    if (value_in_bar == TRUE) {
      data_final$value_y_left_1 <- data_final$value_y_left
      data_final$value_y_left <- roundexcel(data_final$value_y_left, digits = 0)
      data_final$value_y_left_dec <- sprintf("%.1f", data_final$value_y_left_1)
      geom_text_repel1 <- geom_text_repel(mapping = aes(x = Month, y = (value_y_left), fill = category,
                                                        # Here Fill is extremly_important
                                                        label = value_y_left_dec), data = data_final,
                                          stat = "identity",
                                          position = position_stack(vjust = 0.5),
                                          color = "white", size = 3,
                                          fontface = "bold", angle = 90, vjust = v_just_line, hjust = h_just_line,
                                          family = "Times New Roman")
    } else if(value_in_line==TRUE){
      data_final <- data_final %>%
        mutate(month = month(Relevant_Date))
      
      # Add a column for labels, set to value only for specified months
      data_final <- data_final %>%
        mutate(label = ifelse(month %in% val_date_mon, roundexcel(value_y_left, 2), NA))
      
      geom_text_repel1 <- geom_text_repel(aes(x = Month, y = value_y_left, label = label),
                                          segment.size = 0, #corresponds to arrow for label
                                          segment.color = "grey",# colour to arrow for label
                                          max.overlaps = 30, direction = "y", 
                                          family = "Times New Roman",size=size_txt,hjust = 0.5, vjust = 0.5)
      
    } else{
      geom_text_repel1 <- geom_text_repel(aes(x = Month, y = value_y_left,
                                              label = ''),
                                          max.overlaps = 5, data = data_final,
                                          direction = "y",
                                          font = "bold",
                                          min.segment.length = Inf,  # Remove line
                                          na.rm = TRUE, hjust = h_just_line, vjust = v_just_line,
                                          size = 2, family = "Times New Roman")
    }
    # Assuming your data frame is named 'data_final'
    data_final <- subset(data_final, select = -category_left)
    
    print('trying to create a chart')
    ##################################################GRAPH#####################
    line <- ggplot(data = data_final) +
      geom_chart +
      geom_text_repel1 +
      scale_fill_manual(values = my_legends_col) +
      scale_colour_manual(values = my_legends_col,
                          labels = function(x) str_wrap(x, width = str_wrp_wid)) +
      scale_linetype_manual(values = my_line_type) +
      scale_y_value +
      cordi +
      scale_x_date_1 +
      reference_line_1 +
      a1 +
      common_theme(Position = led_position) +
      geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
      ylab(y_axis_value) +
      # labs(
      #   fill = function(x) str_wrap(x, width = 5)  # Wrap fill legend
      # )  +
      theme(
        plot.margin = margin(0.5, 0, 0, 0, "cm"),
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1, size = font_size_axis,  family = "Times New Roman"),
        axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
        axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1), family = "Times New Roman"),
        plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
        legend.key.spacing.y = unit(legend_spacing, 'cm'),
        legend.key.spacing.x = unit(0.5, 'cm'),
        legend.justification = "center"
      ) 
    if (note!=''){
     line<-line+ labs(title = title_chart,
           caption = paste0(note, "")) +  # Combine title and caption settings
      # Set caption size and position (left bottom)
       theme(plot.caption = element_text(size = 9, hjust = 0, vjust = 0.5,family = font,face = "bold"))+
      light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                        val_margin=val_margin,leg_box_height=leg_box_height,
                        leg_box_width=leg_box_width,v_just_x=v_just_x,size=font_size_axis) +
      LH_common_theme()
    }else{
      line<-line+ labs(title = title_chart)
      line<-line+ 
        light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                          val_margin=val_margin,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width,size=font_size_axis) +
        LH_common_theme()
    }
    line <- line +
      theme(
        plot.background = element_rect(color = "gray", fill = NA, size = 1),
        panel.background = element_rect(color = NA, fill = NA, size = 0),  # Remove background from plot panel
        legend.background = element_rect(color = NA, fill = NA, size = 1),  # Add border around legend box
        legend.margin = margin(5, 5, 5, 5),  # Increase margin around legend box to prevent overlap
        plot.margin = margin(0.5, 0.5, 0.5, 0.5, "cm"),
        legend.key.spacing = unit(0.8,'cm')
      )
    
    
    print(line)
    
    print(line)
    ########################################################RETURN######################
    return(list("chart"=line,"s_header"=' '))
    
  },
  error = function(e){
    print(e)
    
  }
  )
}

single_axis_chart_special <- function(data1, x_axis_interval="24 month",
                                      graph_lim=10,
                                      legend_direc='vertical', legend_pos='right',
                                      legend_key_width=0.27, y_dot_rng=15,
                                      reverse_y=FALSE, key_spacing=0.10, 
                                      expand_axis=c(0,0), num_brek=8,
                                      y_axis_value='INR Exchange', axis_per=FALSE, 
                                      title_chart="",val=2, axis_val_y=c(0,0), 
                                      bar_thick=10, n_row=6,
                                      font_size=10, font="Times New Roman", 
                                      legend_spacing=1, h_just_line=0.5, v_just_line=0.5,
                                      led_position='center', stat='identity',
                                      font_size_axis=9, show_leg=FALSE, 
                                      value_in_bar=FALSE, legend_spacing_y=1,
                                      bar_position='stack', SIDE_BAR=FALSE, 
                                      expand_y=c(0.1,0.1), nrow=4, ncol=1 , note='' ,
                                      val_margin=0,leg_box_height=0.5,
                                      leg_box_width=0.5,str_wrp_wid=5 , 
                                      value_in_line=FALSE ,size_txt= 3 , 
                                      wid_pos_bar=0.5 , special_case = FALSE ,
                                      direc_var = "y" , angle_x = 45 , flip_cord=FALSE , 
                                      get_last_month_interval= TRUE) {
  tryCatch({
    data_final <- data1
    
    if (axis_val_y[1] == 0 && axis_val_y[2] == 0) {
      y_min <- axis_val_y[1]
      y_max <- axis_val_y[2]
    } else {
      y_min <- axis_val_y[1]
      y_max <- axis_val_y[2]
    }
    cordi <- coord_cartesian(ylim = c(y_min, y_max))
    ##########################DATA_PROCESSING###########################
    
    # Coordinates are important while assigning limits
    
    if (SIDE_BAR==TRUE){
      bar_position = position_dodge()
      
      
    }else{
      bar_position ="stack"
    }
    scale_y_value <- scale_y_continuous(
      expand = expansion(mult = expand_y),
      breaks = pretty_breaks(n = num_brek),
      labels = function(x) {
        if (axis_per == TRUE) {
          ifelse(x < 0, paste0(format(x, digits = 2, scientific = FALSE), "%"), 
                 paste0(format(x, digits = 2, scientific = FALSE), "%"))
        } else {
          ifelse(
            x < 0,
            paste0(format(x, big.mark = ",", digits = 2, scientific = FALSE)),  # Keep negative values as they are
            paste0(format(abs(x), big.mark = ",", digits = 2, scientific = FALSE)) )
        }
      })
    #Creating a chart with bar or line 
    
    ##################################################GRAPH#####################
    if (flip_cord==TRUE){
      line <- ggplot(data = data_final,
                     aes(x = Variable, y = Value, fill = Variable,label = Value)) +
        geom_bar(stat = "identity",width = bar_thick) +
        #scale_fill_manual(values=c(  "red",                              "white")) +
        geom_text(color = "black", size = 3,vjust=0.5,hjust =-0.5)+
        
        scale_fill_manual(values = my_legends_col) +
        scale_colour_manual(values = my_legends_col,
                            labels = function(x) str_wrap(x, width = str_wrp_wid)) +
        scale_y_value +
        coord_flip() +
        common_theme(Position = led_position) +
        geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
        ylab('')+
        xlab('')+
        theme(
          plot.margin = margin(0.5, 0, 0, 0, "cm"),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 0.85, size = font_size_axis,  family = "Times New Roman"),
          axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
          axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
          plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
          legend.position = "none", 
          legend.key.spacing.y = unit(legend_spacing, 'cm'),
          legend.key.spacing.x = unit(0.5, 'cm'),
          legend.justification = "center",
          axis.title.y = element_blank()
        ) 
    }else if (special_case==FALSE){
      line <- line<-ggplot(data_final, aes(x = Relevant_Date, y = Value, fill = Variable, label = roundexcel(Value,2))) +
        geom_bar(stat = "identity",width = bar_thick) +
        #scale_fill_manual(values=c(  "red",                              "white")) +
        geom_text(position = position_stack(vjust = 0.5),color = "black", size = 3)+
        geom_text(
          aes(label = roundexcel(after_stat(y), 2), group = Relevant_Date), 
          stat = 'summary', fun = sum, vjust = -1
        )+
        scale_x_date(breaks = data_final$Relevant_Date,
                     labels = date_format("%b-%y"),
                     expand = expand_y)+
        scale_fill_manual(values = my_legends_col) +
        scale_colour_manual(values = my_legends_col,
                            labels = function(x) str_wrap(x, width = str_wrp_wid)) +
        scale_y_value +
        cordi +
        common_theme(Position = led_position) +
        geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
        ylab(y_axis_value) +
        theme(
          plot.margin = margin(0.5, 0, 0, 0, "cm"),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 0.85, size = font_size_axis,  family = "Times New Roman"),
          axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
          axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1), family = "Times New Roman"),
          plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
          legend.key.spacing.y = unit(legend_spacing, 'cm'),
          legend.key.spacing.x = unit(0.5, 'cm'),
          legend.justification = "center"
        ) 
    }else{
      line <- ggplot(data = data_final,
                     aes(x = Variable, y = Value, fill =Relevant_Date )) +
        geom_bar(stat = "identity",position=bar_position, width = bar_thick)+
        geom_text_repel(label = roundexcel(data_final$Value, digits = 1),
                        position = position_dodge(width = 0.9),
                        max.overlaps = 10, direction = direc_var, font = "bold",
                        family = "Times New Roman",size=2.5,hjust = h_just_line, vjust = v_just_line)+
        scale_fill_manual(values = my_legends_col) +
        scale_colour_manual(values = my_legends_col,
                            labels = function(x) str_wrap(x, width = str_wrp_wid)) +
        scale_y_value +
        cordi +
        common_theme(Position = led_position) +
        geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
        ylab(y_axis_value) +
        theme(
          plot.margin = margin(0.5, 0, 0, 0, "cm"),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 0.85, size = font_size_axis,  family = "Times New Roman"),
          axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
          axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
          plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
          legend.key.spacing.y = unit(legend_spacing, 'cm'),
          legend.key.spacing.x = unit(0.5, 'cm'),
          legend.justification = "center"
        )
    }
    if (note!=''){
      line<-line+ labs(title = title_chart,
                       caption = paste0(note, "")) +  # Combine title and caption settings
        # Set caption size and position (left bottom)
        theme(plot.caption = element_text(size = 9, hjust = 0, vjust = 0.5,family = font,face = "bold"))+
        light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                          val_margin=val_margin,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width) +
        LH_common_theme()
    }else{
      line<-line+ labs(title = title_chart)
      line<-line+ 
        light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                          val_margin=val_margin,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width,angle_x=angle_x) +
        LH_common_theme()
    }
    line <- line +
      theme(
        plot.background = element_rect(color = "gray", fill = NA, size = 1),
        panel.background = element_rect(color = NA, fill = NA, size = 0),  # Remove background from plot panel
        legend.background = element_rect(color = NA, fill = NA, size = 1),  # Add border around legend box
        legend.margin = margin(5, 5, 5, 5),  # Increase margin around legend box to prevent overlap
        plot.margin = margin(0.5, 0.5, 0.5, 0.5, "cm"),
        legend.key.spacing = unit(0.8,'cm')
      )
    
    ########################################################RETURN######################
    return(list("chart"=line,"s_header"=' '))
    
  },
  error = function(e){
    print(e)
    
  }
  )
}
lollipop_chart_creation <- function(df){
  ##########################CREATING A LOLLIPOP CHART###########################
  lollipop_chart <- ggplot(df, aes(x = Relevant_Date, y = Value)) +
    geom_segment(aes(xend = Relevant_Date, yend = 0, color = 'black'), size = 0.5) +  # Lollipop sticks
    geom_point(aes(color = Variable), size = 5) +
    scale_x_date(breaks = df$Relevant_Date,
                 labels = date_format("%b-%y"),
                 expand = c(0.1,0.1))+
    scale_y_continuous(
      breaks = pretty_breaks(n = 8))+
    coord_cartesian(ylim = c(3.2, 5.2))+
    scale_color_manual(values = colors) +  
    theme_minimal() +   # Add labels
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    common_theme(Position = 'center') +
    geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
    ylab('')+
    theme(
      plot.margin = margin(0.5, 0, 0, 0, "cm"),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1, size = font_size_axis,  family = "Times New Roman"),
      axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
      axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1), family = "Times New Roman"),
      plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
      legend.key.spacing.y = unit(0.5, 'cm'),
      legend.key.spacing.x = unit(0.5, 'cm'),
      legend.justification = "center"
    )+labs(title = 'Forecast')+ 
    light_house_theme(legend_dir = 'horizontal', legend_position = 'bottom') +
    LH_common_theme()
  # Display the chart
  print(lollipop_chart)
  return (lollipop_chart)
}
waterfall_chart_creaion<-function(df.tmp,ymin=0,ymax=100){
 
  water_fall_chart<-ggplot(df.tmp, aes( x = str_wrap(x.axis.Var, width = 10), fill = cat.Var)) + 
    # Waterfall Chart
    geom_rect(aes(x = x.axis.Var,
                  xmin = group.id - 0.25, # control bar gap width
                  xmax = group.id + 0.25, 
                  ymin = end.Bar,
                  ymax = start.Bar,
                  fill = colors),
              color ='black'
    )+
    geom_text(aes(x = x.axis.Var, y = start.Bar + label , label = paste0(label,'%')),
              size = 3, color = "black", hjust = 0.5, vjust = -0.5)+
    scale_fill_manual(values = my_legends_col)+
    coord_cartesian(ylim = c(ymin, ymax))+
    theme(legend.position = "none")+
    common_theme(Position = 'center') +
    geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
    theme(
      plot.margin = margin(0.5, 0, 0, 0, "cm"),
      axis.text.x = element_text(angle = 0, vjust = 0.5, hjust = 0.85, size = font_size_axis,  family = "Times New Roman"),
      axis.text.y.left = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
      axis.text.y.right = element_text(size = font_size_axis-1, margin = margin(r = 1),  family = "Times New Roman"),
      plot.title = element_text(color = "black", size = font_size, face = "bold", vjust = 0.5, hjust = 0.5, family = "Times New Roman"),
      legend.position = "none", 
      legend.key.spacing.y = unit(0.1, 'cm'),
      legend.key.spacing.x = unit(0.5, 'cm'),
      legend.justification = "center"
    )+ylab('') +labs(title = 'CPI')+ 
    light_house_theme() +
    LH_common_theme()+
    theme(legend.position = "none")
  return(water_fall_chart)
}
heat_map_table <- function(data, ft, column_no, med = 0, invert_map = FALSE, manual_med = FALSE,
                           min_rng = 0, max_rng = 0, manual_min_max_rng = FALSE, use_weight = FALSE) {
  
  ########################## DATA_PROCESSING ###########################
  last_8_columns <- data[, tail(names(data), column_no)]
  
  # Calculate the minimum and maximum values row-wise
  min_values <- apply(last_8_columns, 1, min, na.rm = TRUE)
  max_values <- apply(last_8_columns, 1, max, na.rm = TRUE)
  
  global_min_range <- min(min_values)
  global_max_range <- max(max_values)
  
  # Print the minimum and maximum values
  print(paste("Global minimum of last 8 columns:", global_min_range))
  print(paste("Global maximum of last 8 columns:", global_max_range))
  
  all_variables <- data$Description[!duplicated(data$Description)]
  
  if (invert_map == TRUE) {
    mypal <- colorRampPalette(c('#22763F', '#69A760', '#73AC60', '#B0C162', '#D4C964', '#EACF65', '#F8A956', '#D44D44'))(10000)
    pospal <- as.character(paletteer_c("ggthemes::Red-Gold", 100))
    negpal <- rev(as.character(paletteer_c("ggthemes::Green-Gold", 100)))
  } else {
    mypal <- colorRampPalette(c("#D44D44", "#F8A956", "#EACF65", "#D4C964", "#B0C162", "#73AC60", "#69A760", "#22763F"))(10000)
    negpal <- rev(as.character(paletteer_c("ggthemes::Red-Gold", 10)))
    pospal <- as.character(paletteer_c("ggthemes::Green-Gold", 100))
  }
  
  
  if (manual_med == FALSE) {
    med <- (global_max_range + global_min_range) / 2
  }
  
  if (manual_min_max_rng == TRUE) {
    global_min_range <- min_rng
    global_max_range <- max_rng
  }
  
  
  get_bg_color <- function(value, med, min_range, max_range, weight_col, invert_map = FALSE) {
    colourer_neg <- scales::col_numeric(palette = negpal, domain = c(min_range, med))
    colourer_pos <- scales::col_numeric(palette = pospal, domain = c(med, max_range))
    tryCatch({
      if (invert_map == FALSE) {
        print('In inverted map false')
        print(med)
        print('--------------------')
        if (value < med) {
          
          return(colourer_neg(value))
        } else {
          
          return(colourer_pos(value))
        }
      } else {
        colourer_pos <- scales::col_numeric(palette = negpal, domain = c(min_range, med))
        colourer_neg <- scales::col_numeric(palette = pospal, domain = c(med, max_range))
        print('In inverted map true')
        print(med)
        print('--------------------')
        if (value > med) {
          
          return(colourer_neg(value))
        } else {
          
          return(colourer_pos(value))
        }
      }
    }, error = function(e) {
      return("white")  # Neutral color for NaN values
    })
  }
  
  # Apply background colors using the function
  for (i in 1:nrow(data)) {
    if (use_weight && !is.na(data[i, 3])) {
      row_med <- data[i, 3]
      row_min_range <- min(last_8_columns[i, ], na.rm = TRUE) - 1
      row_max_range <- max(last_8_columns[i, ], na.rm = TRUE) + 1
    } else {
      row_med <- med
      row_min_range <- global_min_range
      row_max_range <- global_max_range
    }
    for (col in tail(names(data), column_no)) {
      bg_color <- get_bg_color(data[i, col], row_med, row_min_range, row_max_range, data[i, 3], invert_map=invert_map)
      print(paste("Value:", data[i, col], "Min:", row_min_range, "Med:", row_med, "Max:", row_max_range, "Color:", bg_color))
      ft <- bg(ft, bg = bg_color, i = i, j = col, part = "body")
    }
  }
  
  return(ft)
}