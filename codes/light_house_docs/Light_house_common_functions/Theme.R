
#################################THEME_RELATED_WORK#################################

common_theme <- function(x_angle = 0, leg_plc = "top", legend_key_sp_y = 0.05, 
                         Position = 'center',size=35,
                         legend_key_size=2,legend_key_width=1,
                         font='Times New Roman') {
  # Create a base theme using theme_bw()
  theme_bw() +
    # Set legend properties
    theme(legend.position = leg_plc,
          legend.direction = "horizontal",
          legend.justification = Position,
          legend.title = element_blank(),            # Remove legend title
          legend.title.align = 0,                   # Align legend title
          legend.key.size = unit(legend_key_size, "cm"),    # Length of key
          legend.key.width = unit(legend_key_width, 'cm'),
          legend.key.height = NULL,                         # Key height (unit)
          legend.spacing.x = unit(0.05, 'cm'),
          legend.spacing.y = unit(legend_key_sp_y, 'cm'),
          legend.box = NULL, 
          legend.box.margin = margin(0, 0, 0, 0, "cm"),
          legend.box.spacing = unit(0.20, 'cm'),
          legend.margin = margin(0, 0, 0, 0, "cm"),
          legend.text = element_text(size = size, 
                                     color = "black",
                                     family = font)) +
    # Remove axis titles
    theme(axis.title.x = element_blank(),
          axis.title.y = element_blank()) +
    # Set properties for axis text
    theme(axis.text.x = element_text(angle = x_angle,
                                     color = "black",
                                     size = size,
                                     family = font,
                                     ),
          axis.text.y.left = element_text(angle = 0,
                                          color = "black", 
                                          size = size,
                                          family = font),
          axis.text.y.right = element_text(angle = 0,
                                           color = "black",
                                           size = size,
                                           family = font)) +
    # Set properties for axis ticks
    theme(axis.ticks.x = element_line(size = 0.25),
          axis.ticks.y.right = element_line(size = 0.25),
          axis.ticks.y.left = element_line(size = 0.25),
          axis.ticks.length.y = unit(0.05, "cm"))
}

LH_common_theme <- function() {
  theme(panel.border = element_blank(),
        panel.grid = element_blank(),#remove panel
        panel.spacing = unit(2, "lines"),
        axis.line.x.bottom = element_line(size = 0.5, linetype = 1, colour = "black"),
        axis.line.y = element_line(size = 0.5, linetype = 1, colour = "black"),
        plot.margin = unit(c(0, 0, 0, 0), 'cm'))
}
theme_light_house=function(x_tick_ln=0.15,y_tick_ln=0.15,font_type="Times New Roman",
                           lg_p='right',lg_d='horizontal',lg_ap='center',axis_line=TRUE,
                           lg_k_sp_y=0.05,lg_k_height=0.5,lg_k_width=0.5,
                           leg_k_spacing=0.25,title_size=10,
                           tx_size=9,x_angl=90,sh_x_tick=TRUE,title_plc=0.5){
  if (sh_x_tick==FALSE){
    x_tick=element_blank()
  }else{
    x_tick=element_line(size =0.5,linetype =1,colour = "black")
  }
  if (axis_line==FALSE){
    show_axis_y=element_blank()
    show_axis_x=element_blank()
    show_x_axis_grid=element_blank()
    x_text=element_blank()
    y_left_text=element_blank()
    y_right_text=element_blank()
    thmeme_1=theme_void()
    y_axis_lab=element_blank()
  }else{
    show_axis_y=element_line(size =0.5,linetype =1,colour = "black")
    show_axis_x=element_line(size =0.5,linetype =1,colour = "black")
    show_x_axis_grid=element_line(size = 0.2,linetype =1,colour = "grey")
    thmeme_1=theme_bw()
    x_text=element_text(angle =x_angl,
                        color = "black",
                        size =tx_size,
                        family=font_type,
                        face = "bold"
                        
                        
    )
    y_left_text=element_text(angle =0,
                             color = "black", 
                             size =tx_size,
                             # margin = margin(r =y_tick_ln+0.25,unit = "cm"),
                             face = "bold",
                             family=font_type)
    y_right_text=element_text(angle =0,
                              color = "black",
                              size =tx_size,
                              # margin = margin(r =y_tick_ln+0.25,unit = "cm"),
                              face = "bold",
                              family=font_type)
    y_axis_lab= element_text(angle = 90, vjust = 0.5, 
                             hjust = 0.5, size = tx_size,
                             face="bold"
                             # margin = margin(r = x_tick_ln+0.50, unit = "cm")
    )
    
  }
  if (sh_x_tick==FALSE){
    x_tick=element_blank()
    show_axis_x=element_blank()
  }
  #Making back ground While
  thmeme_1+
    theme(
      #Removing_All borders
      panel.border=element_blank(),
      #Removing all minor-grids
      panel.grid=element_blank(),
      #Removing grids for y-axis verticle
      panel.grid.major.x =element_blank(),
      #Adding grids for y-axis horizontal
      panel.grid.major=show_x_axis_grid,
      #Adding x_axis line 
      axis.line.x.bottom=show_axis_x,
      #Adding y_axis line 
      axis.line.y=show_axis_y)+
    
    theme(
      axis.ticks.x=x_tick,
      axis.ticks.y=element_line(size =0.5,linetype =1,colour = "black"),
      axis.ticks.length.x=unit(x_tick_ln, "cm"),
      axis.ticks.length.y=unit(y_tick_ln, "cm")
    )+
    
    theme (
      legend.position =lg_p,
      legend.direction=lg_d,
      legend.justification=lg_ap,
      legend.title = element_blank(),
      legend.title.align = 0,
      legend.key.size = unit(lg_k_height, "cm"),    #Length of key
      legend.key.width= unit(lg_k_width, 'cm'),
      legend.key.height = NULL,                         # key height (unit)
      legend.spacing.x = unit(0.05, 'cm'),
      legend.spacing.y = unit(lg_k_sp_y, 'cm'),
      legend.box = NULL, 
      legend.box.margin= margin(0, 0, 0, 0, "cm"),
      legend.box.spacing=unit(0.20, 'cm'),
      legend.margin = margin(0, 0, 0, 0, "cm"),
      legend.text = element_text(size =8, 
                                 color = "black",
                                 family=font_type,
                                 face='bold',
                                 margin = margin(r =leg_k_spacing, unit="cm")))+
    
    theme(
      plot.title = element_text(angle = 0, vjust = 0.5,
                                hjust = title_plc, size =title_size,
                                face="bold",
                                family=font_type),
      
      axis.title.y =y_axis_lab,
      axis.title.x=element_blank()
    )+
    
    theme(axis.text.x=x_text,
          axis.text.y.right=y_right_text,
          axis.text.y.left=y_left_text)
}


light_house_theme <- function(legend_dir='vertical',legend_position='right',val=1,
                              font='Times New Roman',size=9,val_margin=0,leg_box_height=0.5,
                              leg_box_width=0.5) {
  print('this is vallllll')
  print(val)
  print(leg_box_height)
  theme(axis.text.x = element_text(angle = 90, size = size-0.5, vjust = 0.5, hjust = 1, face = "bold"),  
        axis.title.y = element_text(angle = 90, vjust = 0.5, hjust = 0.5, size = size, margin = margin(r = 5),
                                    face = 'bold', family = font),
        axis.text.y.left = element_text(size = size, margin = margin(r = 4), face = "bold", family = font),  
        axis.text.y.right = element_text(size = size, margin = margin(r = 4), face = "bold", family = font),  
        panel.grid.major.y = element_line(color = "gray", size = 0.2),
        legend.position = legend_position,
        legend.direction = legend_dir,
        legend.box.spacing=unit(0.3, 'cm'),
        legend.key.spacing.y=unit(val,"cm"),
        legend.key.spacing.x = unit(0.5, 'cm'),
        legend.spacing.x = unit(0.5, 'cm'),  # Adjust this value for desired spacing
        legend.spacing.y = unit(0.5, "cm"),
        legend.key.width = unit(leg_box_width, "cm"),
        legend.key.height = unit(leg_box_height, "cm"),
        legend.key.size = unit(0.5, "cm"),
        legend.justification = "center",
        legend.key.spacing = unit(0.5,'cm'),
        axis.ticks.length.y = unit(0.05, "cm"),
        legend.box.margin = margin(0,0,val_margin,0,"cm"),
        legend.text = element_text(size = 8, face = "bold", family = font, margin = margin(r = 5, l = 6)), # Adjust margin to increase space between legend labels
        strip.text = element_text(size = size, face = "bold"),
        plot.margin = margin(0.5, 1, 0, 1, "cm")) 
}
