## Storm Project

library(dplyr)
library(ggplot2)
library(plyr)
library(gridExtra)
setwd("~/R/project2")
list.files() 
storm <- read.csv(bzfile("repdata%2Fdata%2FStormData.csv.bz2"))

## Exploring Storm
str(storm)
head(storm$EVTYPE)
order(table(storm$EVTYPE))

# Number of unique event types
length(unique(storm$EVTYPE))

# Translate all letters to lowercase
event_types <- tolower(storm$EVTYPE)
table(event_types)

# Replace all punct. characters with a space
event_types <- gsub("[[:blank:][:punct:]+]", " ", event_types)
length(unique(event_types))

# Update the data frame
storm$EVTYPE
storm$EVTYPE <- event_types


casualties <- ddply(storm, .(EVTYPE), summarize,
                    fatalities = sum(FATALITIES),
                    injuries = sum(INJURIES))
head(casualties)

# Events that caused most death and injury
fatal_events <- head(casualties[order(casualties$fatalities, decreasing = T), ], 10)
injury_events <- head(casualties[order(casualties$injuries, decreasing = T), ], 10)

## which types of events are most harmful to population health?
fatal_events[, c("EVTYPE", "fatalities")]
injury_events[, c("EVTYPE", "injuries")]

exp_transform <- function(e) {
    if (e %in% c('h', 'H'))
        return(2)
    else if (e %in% c('k', 'K'))
        return(3)
    else if (e %in% c('m', 'M'))
        return(6)
    else if (e %in% c('b', 'B'))
        return(9)
    else if (!is.na(as.numeric(e))) # if a digit
        return(as.numeric(e))
    else if (e %in% c('', '-', '?', '+'))
        return(0)
    else {
        stop("Invalid exp.value.")
    }
}

prop_dmg_exp <- sapply(storm$PROPDMGEXP, FUN=exp_transform)
storm$prop_dmg <- storm$PROPDMG * (10 ** prop_dmg_exp)
crop_dmg_exp <- sapply(storm$CROPDMGEXP, FUN=exp_transform)
storm$crop_dmg <- storm$CROPDMG * (10 ** crop_dmg_exp)

# Loss by event type
loss <- ddply(storm, .(EVTYPE), summarize,
                   prop_dmg = sum(prop_dmg),
                   crop_dmg = sum(crop_dmg))
head(loss)

# Exclude events that caused no economic loss
loss <- loss[(loss$prop_dmg > 0 | loss$crop_dmg > 0), ]
prop_dmg_ev <- head(loss[order(loss$prop_dmg, decreasing = T), ], 10)
crop_dmg_ev <- head(loss[order(loss$crop_dmg, decreasing = T), ], 10)

## which types of events have the greatest economic consequences?
prop_dmg_ev[, c("EVTYPE", "prop_dmg")]
crop_dmg_ev[, c("EVTYPE", "crop_dmg")]

# Graph fatalities, injuries and property damage

str(fatal_events)

# Fatalities

ggplot(data=fatal_events, aes(x=EVTYPE, y=fatalities)) + 
  geom_bar(stat="identity") + xlab("EVTYPE") + ylab("fatalities") + 
  ggtitle("Fatalities By Event Type") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Injuries

ggplot(data=fatal_events, aes(x=EVTYPE, y=injuries)) + 
  geom_bar(stat="identity") + xlab("EVTYPE") + ylab("injuries") + 
  ggtitle("Injuries By Event Type") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# work with PROPDMG

reduced_storm <- storm[,c("EVTYPE", "PROPDMG")]
agg.prop.dmg.data <- aggregate(reduced_storm$PROPDMG, 
                               by=list(reduced_storm$EVTYPE), 
                               FUN=sum, na.rm=TRUE)
colnames(agg.prop.dmg.data) = c("event.type", "prop.dmg.total")
prop.dmg.sorted <- agg.prop.dmg.data[order(-agg.prop.dmg.data$prop.dmg.total),] 
top.prop.dmg <- prop.dmg.sorted[1:10,]
top.prop.dmg$event.type <- factor(top.prop.dmg$event.type,
                                  levels=top.prop.dmg$event.type, 
                                  ordered=TRUE)

# Property Damage

ggplot(data=top.prop.dmg, aes(x=event.type, y=prop.dmg.total)) + 
  geom_bar(stat="identity") + xlab("Event type") + 
  ylab("Total property damage") +  ggtitle("Property Damage By Event Type") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1))