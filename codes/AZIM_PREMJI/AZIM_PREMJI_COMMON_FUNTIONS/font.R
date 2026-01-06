
#################################FONT RELATED works###############################
#LIGHT HOUSE FONT Times New Roman
assign_font<-function(font=''){
  if (font=='Times New Roman'){
  font_paths()
  t1=font_files() %>%tibble()
  font_add(family='Times New Roman',regular ='timesbd.ttf')
  }
}
