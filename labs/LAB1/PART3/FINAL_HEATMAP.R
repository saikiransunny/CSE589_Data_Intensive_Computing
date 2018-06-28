#submitted by harikrishna rangineeni and sai kiran putta

rm(list = ls(all  = T))
setwd("C:\\Users\\harik\\Desktop\\DIC")


library(ggplot2)
library(ggmap)
library(maps)
library(mapdata)

referencedata = map_data("state")
data= read.csv("final_finaldata.csv")


#extract the coloum index of address and admnistrative_area_level_1
find_cols = function(x)
{
  cols = c()
  for(i in c("address", "administrative_area_level_1"))
  {
    cols = append(cols, grep(i, x))
  }
  return(cols)
}


#get state info from latitude and longitude. 
state_info = data.frame(revgeocode(c(data$lon[1], data$lat[1]), 
                                   output = "more"))
state_info = state_info[,find_cols(names(state_info))]
stateinforows = c(1)
for(i in 2:nrow(data)){
  temp = data.frame(revgeocode(c(data$lon[i], data$lat[i]),
                               output = "more"))
  if(ncol(temp) > 2){
    temp = temp[,find_cols(names(temp))]
    state_info = rbind(state_info, temp)
    stateinforows = append(stateinforows, i)
  }
}

#remove non-usa entries. 
row_select = c()
for(i in 1:nrow(state_info)){
  if(length(grep("USA", state_info$address[i])) > 0){
    row_select = append(row_select, i)
  }
}
state_info = state_info[row_select,]



#tidy string. 
state_info$administrative_area_level_1 = trimws(tolower(state_info$administrative_area_level_1), 
                                                which = c("both"))
names(state_info) = c("address", "state")

merge_data1 = data[sample(1:nrow(data), nrow(state_info)),]
merged_data = cbind(merge_data1, state_info)



library(plyr)
#calculate the count of each state occurance
count_df = data.frame(count(state_info, "state"))

#create a new bag with respective states' counts in it. 
referencedata_bag = c()
for(i in 1:nrow(referencedata)){
  current_row = referencedata[i,]
  state = current_row$region
  idx = grep(state, count_df$state)
  referencedata_bag = append(referencedata_bag, count_df$freq[idx[1]])
  print(i)
}


load("final_heatmapworkspace.RData")

#plotting the heat map
referencedata$count = referencedata_bag
ggplot(referencedata, aes(long, lat)) +
  geom_polygon(aes(group = group, fill = count)) + 
  coord_quickmap()
