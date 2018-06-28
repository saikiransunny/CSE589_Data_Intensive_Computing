# submitted by harikrishna rangineeni and sai kiran putta



mydat = read.csv("D:/Spring 2018/DIC/labs/lab1/data/final.csv")

library(ggmap)
#extract the locations from lat and long
geocodes = geocode(as.character(mydat$location[1:100]))

geocodes1 = geocode(as.character(mydat$location[101:2400]))
mydat = data.frame(mydat[,1:2],geocodes)

stateinfo = map_data("state")
statenames = unique(stateinfo$region)
mydat = mydat[which(mydat$lang == "en"),]

geocodes = rbind(geocodes, geocodes1)

set.seed(1234)
mydat = mydat[sample(1:nrow(mydat), 2400),]
mydat = data.frame(mydat, geocodes)
mydat = na.omit(mydat)

write.csv(mydat, "D:/Spring 2018/DIC/labs/lab1/data/final_finaldata.csv", row.names = F)
