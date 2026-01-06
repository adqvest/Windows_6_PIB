
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
       paletteer,padr,tidyr,epitools,ggfittext,timeDate,
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
    formatted_date <- format(date, "%B %Y")
    
    
  }
  return (formatted_date)
}
#-----PPT TEMPLATE----------------------------
f     ="C:\\Users\\Administrator\\AdQvestDir\\codes\\light_house_docs\\Light_house_common_functions\\light_house_template_note.pptx"
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
generate_sequence <- function(start_date, end_date, interval) {
  dates <- seq(start_date, end_date, by = interval)
  adjusted_dates <- sapply(dates, function(date) {
    if (month(date) == 3 && day(date) < 28) {
      day(date) <- 28 # Set February dates to the last valid day
      month(date) <- 2
    }
    return(date)
  })
  return(as.Date(adjusted_dates, origin = "1970-01-01"))
}
############################### CHART FUNCTIONS##############################
dual_axis_chart_LH <- function(data_b, data_l, ylim.prim = c(0, 100), ylim.sec = c(-18, 1),
                               x_axis_interval = "24 month",
                               graph_lim = 30, y_dot_rng = 15, num_brek = 8,
                               add_std_col = FALSE, y_axis_value = '',
                               bar_thick = 8, expand_axis = c(0, 0), expand_y = c(0, 0),
                               FY = TRUE, Quarter = FALSE, all_qua = FALSE, h_j = 0.60, v_j = 0.60,
                               axis_per = FALSE, title_chart = '',
                               led_pos = 'right', led_direc = 'horizontal',
                               min_x_axis = '', less_than = FALSE, val = 2, curr_acc = FALSE,
                               value_add = 4, bal_label = FALSE, manual_y = FALSE,
                               val_date_mon = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
                               num_brek1 = 8, led_position = 'center',
                               sec_y_axis_value='', year = FALSE, dual_axis_line = FALSE,
                               font_size=10, font="Times New Roman", chart_type='bar',
                               legend_spacing=1, font_size_axis=9, round_integer=FALSE,
                               n_row=2, n_col=2, legend_spacing_y=1 , note='' ,
                               val_mar=0,leg_box_height=0.5,
                               leg_box_width=0.5 , str_wrp_wid =10 , h_just_cap=-0.6 ,
                               get_last_month_interval=FALSE ,
                               last_month_interval="-3 months" , interval_year = 2) {

  tryCatch({
    ##########################DATA_PROCESSING###########################
    # Create data frames for both primary and secondary data
    data_final <- create_df_for_chart(data_b, mtlti_line = TRUE)[[1]]
    print('Done processing data for data_b')
    data_final_1 <- create_df_for_chart(data_l, mtlti_line = TRUE)[[1]]
    print('Done processing data for data_l')

    # Set 'Month' and 'category' columns for both data frames
    data_final_1$Month <- data_final_1$Relevant_Date
    data_final$Month <- data_final$Relevant_Date
    data_final_1$category <- data_final_1$category_left
    data_final$category <- data_final$category_left

    # Print data frames for validation
    print(data_final)
    print(data_final_1)

    # Filter data based on min_x_axis if provided
    if (min_x_axis != '') {
      min_x_axis_date <- as.Date(min_x_axis)
      if (less_than == TRUE) {
        data_final <- data_final %>% filter(Relevant_Date < min_x_axis_date)
        data_final_1 <- data_final_1 %>% filter(Relevant_Date < min_x_axis_date)
      } else {
        data_final <- data_final %>% filter(Relevant_Date > min_x_axis_date)
        data_final_1 <- data_final_1 %>% filter(Relevant_Date > min_x_axis_date)
      }
    }

    # Determine x-axis limits
    x_min <- min(data_final$Month)
    x_max <- max(data_final$Month)

    # Determine y-axis limits
    y_max <- max(data_final$value_y_left)
    y_min <- min(data_final$value_y_left)

    # Print limits for verification
    print(paste('Min of x:', x_min))
    print(paste('Max of x:', x_max))
    print(paste('Min of y:', y_min))
    print(paste('Max of y:', y_max))

    # Create quarter labels if Quarter is TRUE
    data_final$Relevant_Date=as.Date(timeLastDayInMonth(data_final$Relevant_Date))
    data_final_1$Relevant_Date=as.Date(timeLastDayInMonth(data_final_1$Relevant_Date))

    #FORMAT QUATER DATES
    format_quat_date <- function(x) {
      # Assuming x is a vector of dates
      result <- vector(mode = "character", length = length(x))  # Specify mode as "character"
      for (i in 1:length(x)) {
        mon_num <- as.numeric(format(x[i], "%m"))
        if (mon_num == 9) {
          result[i] <- paste0("2Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) + 1))
        } else if (mon_num == 12) {
          result[i] <- paste0("3Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) +1 ))
        }else if (mon_num == 6) {
          result[i] <- paste0("1Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) +1 ))
        }else{
          result[i] <- paste0("4Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y"))))

        }
      }
      return(result)
    }

    if (Quarter == TRUE) {
      # Subset data for quarters
      data_final_test <- subset(data_final_1, month(data_final_1$Relevant_Date) %in% val_date_mon)
      # Set x-axis scale for quarters
      scale_x_date_1 <- scale_x_date(
        breaks = data_final_test$Relevant_Date,
        labels = format_quat_date(data_final_test$Relevant_Date),
        expand = expand_axis
      )
    } else if (FY == TRUE) {
      if (year == TRUE) {
        # Subset data for fiscal years
        data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
        # Set x-axis scale for fiscal years with year labels
        v1 <- sort(unique(data_final_test$Relevant_Date), decreasing = TRUE)
        breaks=sort(v1[seq(1, length(v1), by = interval_year)])
        scale_x_date_1 <- scale_x_date(
          breaks = breaks,
          labels = function(x) format_fy_date(year(x)),
          expand = expand_axis
        )
      } else {
        # Subset data for fiscal years
        data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
        # Set x-axis scale for fiscal years with month-year labels
        scale_x_date_1 <- scale_x_date(
          breaks = data_final_test$Relevant_Date,
          labels = function(x) format_fy_date(year(x)),
          expand = expand_axis
        )
      }
    }else if (get_last_month_interval == TRUE) {
      latest_date <- max(data_final$Relevant_Date)
      min_date <- min(data_final$Relevant_Date)

      # Generate breaks with a 3-month gap from the latest date until the minimum date

      breaks <- generate_sequence(latest_date, min_date, last_month_interval)


      print(breaks)
      # Use scale_x_date with the breaks
      scale_x_date_1 <- scale_x_date(
        breaks = breaks,
        labels = date_format("%b-%y"),
        expand = expand_axis
      )} else {
      # Subset data for specified months
        print('in val month')
      data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
      # Set x-axis scale for specified months
      scale_x_date_1 <- scale_x_date(
        breaks = data_final_test$Relevant_Date,
        labels = date_format("%b-%y"),
        expand = expand_axis
      )
    }

    if (axis_per == TRUE) {
      if (round_integer == TRUE) {
        # Create a dual-axis scale with percentage and rounded numeric values
        scale_y_axis <- scale_y_continuous(
          breaks = pretty_breaks(n = num_brek),
          labels = function(x) format(round(x, 1)),
          sec.axis = sec_axis(
            ~ (. - a) / b,
            breaks = pretty_breaks(n = num_brek1),
            labels = function(x) paste0(format(round(x, 1)), "%"),
            name = sec_y_axis_value
          )
        )
      } else {
        # Create a dual-axis scale with percentage and numeric values
        scale_y_axis <- scale_y_continuous(
          expand = expansion(mult = expand_y),
          breaks = pretty_breaks(n = num_brek),
          labels = function(x) format(x, big.mark = ",", digits = 2),
          sec.axis = sec_axis(
            ~ (. - a) / b,
            breaks = pretty_breaks(n = num_brek1),
            labels = function(x) paste0(format(x), "%"),
            name = sec_y_axis_value
          )
        )
      }
    } else {
      # Create a single-axis scale with numeric values
      scale_y_axis <- scale_y_continuous(
        expand = expansion(mult = expand_y),
        breaks = pretty_breaks(n = num_brek),
        labels = function(x) paste0(format(x * 1, digits = 2)),
        sec.axis = sec_axis(
          ~ (. - a) / b,
          breaks = pretty_breaks(n = num_brek),
          labels = function(x) paste0(format(x), ""),
          name = sec_y_axis_value
        )
      )
    }

    if (curr_acc == TRUE) {
      # Adjust y-axis limits based on data and current accuracy setting
      ylim.prim <- c(min(data_final$value_y_left), max(data_final$value_y_left))
      ylim.sec <- c(min(data_final_1$value_y_left), max(data_final_1$value_y_left))
      b <- (max(ylim.prim) - min(ylim.prim)) / (max(ylim.sec) - min(ylim.sec) + value_add)
      a <- min(ylim.prim) - b * min(ylim.sec)

      # Print adjusted y-axis limits for verification
      print(ylim.prim)
      print(ylim.sec)
    } else if (manual_y == TRUE) {
      # Manually set y-axis limits if specified
      print('Inside manual run')
      print(ylim.prim)
      print(ylim.sec)

      # Calculate scaling factor 'b'
      b <- ((max(ylim.prim) - min(ylim.prim)) / (max(ylim.sec) - min(ylim.sec)))
      a <- min(ylim.prim) - b * min(ylim.sec)
    } else {
      # Set y-axis limits based on data
      ylim.prim <- c(min(data_final$value_y_left), max(data_final$value_y_left))
      ylim.sec <- c(min(data_final_1$value_y_left), max(data_final_1$value_y_left))
      print(ylim.prim)
      print(ylim.sec)

      # Calculate scaling factor 'b'
      b <- ((max(ylim.prim) - min(ylim.prim)) / (max(ylim.sec) - min(ylim.sec)))
      a <- min(ylim.prim) - b * min(ylim.sec)
    }
    labels=list(round(data_final$value_y_left, digits = 0))
    if (bal_label == TRUE) {
      # Create text labels with specified attributes
      text_val <- geom_text_repel(aes(x = Month, y = value_y_left, label = round(value_y_left, digits = 0)),
                                  segment.size = 0,
                                  max.overlaps = 10, direction = "y", font = "bold",
                                  family = "Times New Roman",size=2.5,hjust = h_j, vjust = v_j)
    } else {
      # Create empty text labels with specified attributes
      text_val <- geom_text_repel(aes(x = Month, y = value_y_left, label = ''), max.overlaps = 4, direction = "y", font = "bold",
                                  family = "Times New Roman",hjust = h_j, vjust = v_j)
    }

    if (manual_y == TRUE) {
      # Set cartesian coordinate limits based on manual y-axis limits
      coord_cart <- coord_cartesian(ylim = ylim.prim)
    } else {
      # Set cartesian coordinate limits based on calculated y-axis limits
      coord_cart <- coord_cartesian(ylim = c(y_min, y_max))
    }

    if (chart_type == 'multi_line') {
      # Create a line plot if dual axis is enabled
      geom_graph <- geom_line(aes(x = Month, y = value_y_left, color = category,
                                  group = category), linetype = "solid", size = 1, show.legend = TRUE)
    } else {
      # Create a bar plot if dual axis is not enabled
      print('this is bar type')
      #browser()
      geom_graph <- geom_bar(aes(x = Month, y = (value_y_left), fill = category,show.legend = TRUE),
                             stat = "identity",
                             position = "stack",
                             width = bar_thick,
                             legend.key.height=unit(8,'cm'))
    }
    print('MONTH')
    print(data_final$Month)
    print('trying to create a chart')
  ##################################################GRAPH#####################
    line <- ggplot(data = data_final) +
      geom_graph +
      geom_line(aes(x = Month, y = a + value_y_left * b, color = category, group = category,
                    show.legend = TRUE),
                data = data_final_1, linetype = "solid", size = 1,legend.key.height=unit(2,'cm'))+

      scale_fill_manual(values = my_legends_col) +
      scale_colour_manual(values = my_legends_col,
                          labels = function(x) str_wrap(x, width = str_wrp_wid)) +

      text_val +
      coord_cart +
      scale_y_axis +
      scale_x_date_1 +
      guides(color =guide_legend(ncol=1,nrow=n_row,byrow=TRUE,order =2,reverse=FALSE),
             fill =guide_legend(ncol=1,nrow=n_row,byrow=TRUE,order =1))+
      common_theme(Position = led_position)+
      geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5)+
      ylab(y_axis_value)
    print('basic line')

    line <-line + theme(plot.margin = margin(0.5, 0, 0, 0, "cm"))
    line <-line + theme(
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1,size = font_size_axis,face = "bold",family="Times New Roman"),
      axis.text.y.left = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman"),          # Adjust primary axis label size
      axis.text.y.right = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman")     # Adjust secondary axis label size
    )
    print('basic line')
    line <- line + labs(title = title_chart,
                        caption = paste0('', ""))  +
      theme(plot.title = element_text(color = "black", size = font_size, face = "bold",vjust = 0.5, hjust = 0.5,family="Times New Roman"))
    print('basic line')
    if (note!=''){
      line<-line+ labs(title = title_chart,
                       caption = paste0("\n",note, "")) +  # Combine title and caption settings
        # Set caption size and position (left bottom)
        theme(plot.caption = element_text(size = 9, hjust = h_just_cap, vjust = 0.5,family = font))+
        light_house_theme(legend_dir=led_direc,legend_position=led_pos,val=legend_spacing_y,
                          val_margin=val_mar,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width) +
        LH_common_theme() +
        theme(legend.key.spacing.y=unit(0.5,'cm'),
              legend.key.spacing.x=unit(0.5,'cm'),
              legend.justification = "center",
              legend.key.spacing = unit(0.8,'cm'))
    }else{
      line<-line+ labs(title = title_chart)
      line<-line+
        light_house_theme(legend_dir=led_direc,legend_position=led_pos,val=legend_spacing_y,
                          val_margin=val_mar,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width) +
        LH_common_theme() +
        theme(legend.key.spacing.y=unit(0.5,'cm'),
              legend.key.spacing.x=unit(0.5,'cm'),
              legend.justification = "center",
              legend.key.spacing = unit(0.8,'cm'))
    }
    # line <- line +
    #   theme(
    #     plot.background = element_rect(color = "black", fill = NA, size = 1),
    #     panel.background = element_rect(color = NA, fill = NA, size = 0)  # Remove background from plot panel
    #   )
    line <- line +
      theme(
        plot.background = element_rect(color = "gray", fill = NA, size = 0.8),
        panel.background = element_rect(color = NA, fill = NA, size = 0),  # Remove background from plot panel
        legend.background = element_rect(color = NA, fill = NA, size = 1),  # Add border around legend box
        legend.margin = margin(5, 5, 5, 5),  # Increase margin around legend box to prevent overlap
        plot.margin = margin(0.5, 0.5, 0.5, 0.5, "cm")
      )
    line <-line + theme(
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1,size = font_size_axis,face = "bold",family="Times New Roman"),
      axis.text.y.left = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman"),          # Adjust primary axis label size
      axis.text.y.right = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman"),
      axis.title.y.left = element_text(size = font_size_axis,margin = margin(r = 3),face = "bold",family="Times New Roman")

       )
    # print(line)
    print('basic line')

  ########################################################RETURN######################
  return(list("chart"=line,"s_header"=' '))
  },
  error = function(e){
    print(e)

  }
  )
}
single_axis_chart_LH <- function(data1, x_axis_interval="24 month",
                                 graph_lim=10,
                                 legend_direc='vertical', legend_pos='right',
                                 legend_key_width=0.27, y_dot_rng=15,
                                 reverse_y=FALSE, key_spacing=0.10,
                                 expand_axis=c(0,0), num_brek=8,
                                 y_axis_value='INR Exchange', FY=FALSE,
                                 chart_type='line', axis_per=FALSE,
                                 min_x_axis='', Quarter=FALSE,
                                 name_label='', title_chart="", dual_axis=FALSE,
                                 val=2, all_qua=FALSE, less_than=FALSE,
                                 Currency=FALSE, line_size=2,
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
                                 leg_box_width=0.5,str_wrp_wid=5,h_just_cap=5,
                                 x_axis_line=TRUE,
                                 get_last_month_interval=FALSE,
                                 last_month_interval="-3 months",
                                 monthly=FALSE) {
  tryCatch({
    data1 <- data_s
    ##########################DATA_PROCESSING###########################

    if (chart_type != 'bar' && chart_type != 'line') {
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
                                     color = "GRAY 32", size = 0.25,
                                     show.legend = FALSE,
                                     linetype = 1)
    a1 <- annotate("text", x = x_min, y = 0, label = "", angle = 0, size = 14, color = "black")

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
        } else if (mon_num == 12) {
          result[i] <- paste0("3Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) +1 ))
        }else if (mon_num == 6) {
          result[i] <- paste0("1Q", (as.numeric(format(data_final_test$Relevant_Date[i], "%y")) +1 ))
        }else{
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
        # Assuming data_final is a data.frame with 'Relevant_Date' and 'FY_QTR' columns

        # Ensure 'FY_QTR' column is treated as a factor and sort it in descending order
        data_final$FY_QTR <- factor(data_final$FY_QTR, levels = unique(data_final$FY_QTR))
        v1 <- sort(unique(data_final$FY_QTR), decreasing = TRUE)

        # Calculate the interval
        x_axis_interval <- x_axis_interval # replace with your actual interval value
        qtrs <- as.numeric(gsub("\\D", "", x_axis_interval)) + 1

        # Check if qtrs is correctly computed and greater than 0
        if (is.na(qtrs) || qtrs <= 0) {
          stop("Invalid interval. Please check the value of x_axis_interval.")
        }

        # Select the desired quarters based on the interval
        desired_qtrs <- sort(v1[seq(1, length(v1), by = qtrs)])

        # Get unique dates and map them to their quarters
        desire_dates <- unique(data_final$Relevant_Date)
        names(desire_dates) <- unique(data_final$FY_QTR)

        # Initialize vectors for breaks and desired dates
        breaks_2 <- c()
        desire_dates_2 <- c()

        # Loop through desired quarters and get corresponding dates
        for (i in names(desire_dates)) {
          if (i %in% desired_qtrs) {
            # Get the first date corresponding to the quarter
            print('there')
            n1=5
            date_to_add <- as.Date(desire_dates[i][1])
            desire_dates_2 <- c(desire_dates_2, date_to_add)
          }
        }

        # Ensure dates are unique and sorted
        desire_dates_2 <- sort(unique(as.Date(desire_dates_2)))

        # Print debug information
        cat('--------------------------------------------\n')
        cat('Number of breaks:', length(breaks_2), '\n')
        cat('Breaks:', breaks_2, '\n')
        cat('--------------------------------------------\n')
        cat('Number of desired dates:', length(desire_dates), '\n')
        cat('Desired dates:', desire_dates, '\n')
        cat('--------------------------------------------\n')

        # Define scale_x_date with updated limits and breaks
        scale_x_date_1 <- scale_x_date(
          limits = as.Date(c(NA, x_max + graph_lim)),
          labels = desired_qtrs,
          breaks = desire_dates_2,
          expand = expand_axis
        )

      }
    } else if (FY == TRUE) {
      data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
      scale_x_date_1 <- scale_x_date(breaks = data_final_test$Relevant_Date,
                                     labels = function(x) format_fy_date(year(x)),
                                     expand = expand_axis)
    } else if (chart_type =='stacked_bar') {
      if (monthly==TRUE){
        data_final_test <- subset(data_final, month(data_final$Relevant_Date) %in% val_date_mon)
        scale_x_date_1 <- scale_x_date(breaks = data_final_test$Relevant_Date,
                                       labels = date_format("%b-%y"),
                                       expand = expand_axis)
      }else{
      date_range <- range(data_final$Relevant_Date)

      # Calculate breaks at two-month intervals
      breaks <- seq.Date(
        from = floor_date(date_range[1], unit = "month"),  # Round down to the first day of the month
        to = ceiling_date(date_range[2], unit = "month"),  # Round up to the first day of the next month
        by = "2 months"  # Specify the interval
      )
      print(breaks)
      previous_month <- month(breaks) - 1  # Subtract 1 to get the previous month
      previous_year <- year(breaks)
      previous_year[previous_month == 0] <- previous_year[previous_month == 0] - 1  # Adjust year for January
      previous_month[previous_month == 0] <- 12  # Adjust month for January
      labels <- format(as.Date(paste(previous_year, previous_month, "01", sep = "-")), "%b-%y")
      print(labels)
      scale_x_date_1 <- scale_x_date(
        breaks = breaks,
        labels = labels,
        expand = expand_axis
      )}
    }else if (get_last_month_interval == TRUE) {
      latest_date <- max(data_final$Relevant_Date)
      min_date <- min(data_final$Relevant_Date)

      # Generate breaks with a 3-month gap from the latest date until the minimum date
      breaks <- generate_sequence(latest_date, min_date, last_month_interval)

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
                                  group = category), linetype = "solid", size = 1, show.legend = TRUE)
    } else if (chart_type == 'line') {
      print('not a multi line')
      geom_chart <- geom_line(aes(x = Month, y = value_y_left,show.legend=TRUE),
                              color = "#5c8bc3", size = 1, group = 1,
                              linetype = 1)
    } else  {
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
      data_final$value_y_left <- round(data_final$value_y_left, digits = 0)
      data_final$value_y_left_dec <- sprintf("%.1f", data_final$value_y_left_1)
      geom_text_repel1 <- geom_text(aes(x = Month, y = value_y_left,
                                        label=value_y_left_dec),
                                    position = position_stack(vjust = 0.8),
                                    color = "white", size = 2.8,
                                    fontface = "bold", angle = 90,
                                    family = "Times New Roman")
    } else {
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
    print(data_final)
    print('trying to create a chart')
    ##################################################GRAPH#####################
    line <- ggplot(data = data_final) +
      geom_chart +
      geom_text_repel1 +
      scale_fill_manual(values = my_legends_col)+
      scale_colour_manual(values = my_legends_col,
                          labels = function(x) str_wrap(x, width = str_wrp_wid)) +
      scale_linetype_manual(values = my_line_type) +
      scale_y_value +
      cordi +
      scale_x_date_1 +
      reference_line_1 +
      # a1 +
      common_theme(Position = led_position) +
      geom_hline(yintercept = 0, color = "black", linetype = "solid", size = 0.5) +
      ylab(y_axis_value)
    print('basic line')

    line <-line + theme(plot.margin = margin(0.5, 0, 0, 0, "cm"))
    line <-line + theme(
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1,size = font_size_axis,face = "bold",family="Times New Roman"),
      axis.text.y.left = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman"),          # Adjust primary axis label size
      axis.text.y.right = element_text(size = font_size_axis,margin = margin(r = 2),face = "bold",family="Times New Roman")     # Adjust secondary axis label size
    )
    print('basic line')
    line <- line + labs(title = title_chart,
                        caption = paste0('', ""))  +
      theme(plot.title = element_text(color = "black", size = font_size, face = "bold",vjust = 0.5, hjust = 0.5,family="Times New Roman"))
    print('basic line')
    if (note!=''){
      line<-line+ labs(title = title_chart,
                       caption = paste0("\n",note, "")) +  # Combine title and caption settings
        # Set caption size and position (left bottom)
        theme(plot.caption = element_text(size = 9, hjust = h_just_cap, vjust = 0,family = font))+
        light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                          val_margin=val_margin,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width) +
        LH_common_theme()
    }else{
      line<-line+ labs(title = title_chart)
      line<-line+
        light_house_theme(legend_dir = legend_direc, legend_position = legend_pos, val = legend_spacing_y,
                          val_margin=val_margin,leg_box_height=leg_box_height,
                          leg_box_width=leg_box_width) +
        LH_common_theme()
    }
    line <- line +
      theme(
        plot.background = element_rect(color = "gray", fill = NA, size = 0.8),
        panel.background = element_rect(color = NA, fill = NA, size = 0),  # Remove background from plot panel
        legend.background = element_rect(color = NA, fill = NA, size = 1),  # Add border around legend box
        legend.margin = margin(5, 5, 5, 5),  # Increase margin around legend box to prevent overlap
        plot.margin = margin(0.5, 0.5, 0.5, 0.5, "cm"),
        legend.key.spacing = unit(0.8,'cm')
      )
    if (x_axis_line==FALSE){
      print('removing x axis line')
      line<- line+
        theme(axis.line.x = element_blank(),
              axis.line.x.bottom = element_blank())
    }
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
light_house_pie_chart=function(df,show_legend=TRUE,bar_position ="stack",bar_thick=10,
                               Chart_title="Components of External Debt - March 2024 (Provisional)"){
  
  print(df)
  df<-ext_debt_com
  #browser()
  
  title_list<-get_val_pie(df)
  
  ##################################################GRAPH#####################
  pie_chart=ggplot(data=df,aes(x='',y=(Value),fill=reorder(Segment,Value)))+
    
    geom_bar(aes(x='',y=(Value),fill=reorder(Segment,Value)),
             stat="identity",width = 10,
             show.legend =TRUE,
             position='stack')+
    
    
    geom_text(aes(label =c(title_list),x=4),
              color = "white",size=2.9,
              font = "bold",
              family = "Times New Roman",
              position = position_stack(vjust =0.2)) +
    
    
    coord_polar(theta = "y")+
    scale_fill_manual(values=my_chart_col)+
    scale_colour_manual(values=my_chart_col)+
    
    guides(fill =guide_legend(nrow=length(unique(df$Segment)),
                              byrow=TRUE,reverse=TRUE,order =1))+
    
    theme_light_house(axis_line=FALSE,lg_p = 'right',
                      lg_d ='vertical',
                      title_plc=0.4,
                      lg_k_height=0.20,lg_k_width=0.40,
                      lg_k_sp_y= 0.5)+
    ggtitle(Chart_title)
  pie_chart<-pie_chart+
    theme(legend.key.spacing.y = unit(0.3, 'cm'))
  pie_chart <- pie_chart +
    theme(
      plot.background = element_rect(color = "black", fill = NA, size = 0.5),
      plot.margin = margin(0, 0, 0, 3.5, "cm"),
      panel.background = element_rect(color = NA, fill = NA, size = 0),  # Remove background from plot panel
      legend.background = element_rect(color = NA, fill = NA, size = 1),  # Add border around legend box
      legend.margin = margin(5, 5, 5, 5),
      legend.box.margin=unit(c(0, 3.5,0, 0), "cm")# Increase margin around legend box to prevent overlap
      
    )
  pie_chart <- pie_chart +
    theme(plot.title = element_text(hjust = 0))
  pie_chart <- pie_chart + theme(axis.ticks.y = element_blank())
  print(pie_chart)
  
}

