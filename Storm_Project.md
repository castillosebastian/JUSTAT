---
title: "Storm-Project"
author: "Sebastián Castillo"
date: "30 de octubre de 2016"
output: md_document
---

```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)
```

Health and Economic Impact of Weather Events in the US
======================================================


Storms and other severe weather events can cause both public health and economic problems for communities and municipalities. Many severe events can result in fatalities, injuries, and property damage, and preventing such outcomes to the extent possible is a key concern.

This project involves exploring the U.S. National Oceanic and Atmospheric
Administration's (NOAA) storm database. This database tracks characteristics of major storms and weather events in the United States, including when and where they occur, as well as estimates of any fatalities, injuries, and property damage.

Synopsis
========

The analysis on the storm event database shows that, between 1950 and 2011,  tornadoes were the most dangerous weather event to the population health. The second most dangerous event type was the excessive heat. On the other hand flash floods and thunderstorm winds caused millions of dollars in property damages . 

Data Processing
===============

The analysis was performed on
[Storm Events Database](http://www.ncdc.noaa.gov/stormevents/ftp.jsp), provided by
[National Climatic Data Center](http://www.ncdc.noaa.gov/). The data is from a comma-separated-value file available
[here](https://d396qusza40orc.cloudfront.net/repdata%2Fdata%2FStormData.csv.bz2).
There is also some documentation of the data available
[here](https://d396qusza40orc.cloudfront.net/repdata%2Fpeer2_doc%2Fpd01016005curr.pdf).

The first step is to read the data into a data frame. 
```{r cache=TRUE}
library(dplyr)
library(ggplot2)
library(plyr)
library(gridExtra)
setwd("~/R/project2")
storm <- read.csv(bzfile("repdata%2Fdata%2FStormData.csv.bz2"))
str(storm)
```

Before the analysis, the data need some preprocessing in order to give levels the same format.

```{r}
# number of unique event types
length(unique(storm$EVTYPE))
# translate all letters to lowercase
event_types <- tolower(storm$EVTYPE)
# replace all punct. characters with a space
event_types <- gsub("[[:blank:][:punct:]+]", " ", event_types)
length(unique(event_types))
# update the data frame
storm$EVTYPE <- event_types
```

After the cleaning, as expected, the number of unique event types reduce
significantly. For further analysis, the cleaned event types are used.

Dangerous Events with respect to Population Health
================================================

To find the event types that are most harmful, the number of casualties are aggregated by the event type.

```{r}
library(plyr)
casualties <- ddply(storm, .(EVTYPE), summarize,
                    fatalities = sum(FATALITIES),
                    injuries = sum(INJURIES))

# Find events that caused most death and injury
fatal_events <- head(casualties[order(casualties$fatalities, decreasing = T), ], 10)
injury_events <- head(casualties[order(casualties$injuries, decreasing = T), ], 10)
```

Top 10 events that caused largest number of deaths are

```{r}
fatal_events[, c("EVTYPE", "fatalities")]
```

Top 10 events that caused most number of injuries are

```{r}
injury_events[, c("EVTYPE", "injuries")]
```

Economic Effects of Weather Events
==================================

To analyze the impact of weather events on the economy, available property
damage and crop damage reportings/estimates were used.
The first step in the analysis is to calculate the property and crop damage for each event.

```{r}
exp_transform <- function(e) {
    # h -> hundred, k -> thousand, m -> million, b -> billion
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
        stop("Invalid exponent value.")
    }
}
```

```{r cache=TRUE}
prop_dmg_exp <- sapply(storm$PROPDMGEXP, FUN=exp_transform)
storm$prop_dmg <- storm$PROPDMG * (10 ** prop_dmg_exp)
crop_dmg_exp <- sapply(storm$CROPDMGEXP, FUN=exp_transform)
storm$crop_dmg <- storm$CROPDMG * (10 ** crop_dmg_exp)
```


```{r}
# Compute the economic loss by event type
library(plyr)
econ_loss <- ddply(storm, .(EVTYPE), summarize,
                   prop_dmg = sum(prop_dmg),
                   crop_dmg = sum(crop_dmg))

# filter out events that caused no economic loss
econ_loss <- econ_loss[(econ_loss$prop_dmg > 0 | econ_loss$crop_dmg > 0), ]
prop_dmg_events <- head(econ_loss[order(econ_loss$prop_dmg, decreasing = T), ], 10)
crop_dmg_events <- head(econ_loss[order(econ_loss$crop_dmg, decreasing = T), ], 10)
```

Top 10 events that caused most property damage (in dollars) are as follows

```{r}
prop_dmg_events[, c("EVTYPE", "prop_dmg")]
```

Similarly, the events that caused biggest crop damage are

```{r}
crop_dmg_events[, c("EVTYPE", "crop_dmg")]
```

Results
=======

Health impact of weather events
-------------------------------

The following plot shows top dangerous weather event types.

```{r}
ggplot(data=fatal_events, aes(x=EVTYPE, y=fatalities)) + 
  geom_bar(stat="identity") + xlab("EVTYPE") + ylab("fatalities") + 
  ggtitle("Fatalities By Event Type") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(data=fatal_events, aes(x=EVTYPE, y=injuries)) + 
  geom_bar(stat="identity") + xlab("EVTYPE") + ylab("injuries") + 
  ggtitle("Injuries By Event Type") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

```

Tornadoes cause most number of deaths and injuries among all event types. There are more than 5,000 deaths and more than 10,000 injuries in the last 60 years in US, due to tornadoes. The other event types that are most dangerous with respect to population health are excessive heat and flash floods.

Economic impact of weather events
---------------------------------

Now we will see the most severe weather event types with respect to
economic cost that they have costed since 1950s.

First we are going to work again whit "PROPDMG", to combine property and crop damage. Then, we plot the results.

```{r}
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


ggplot(data=top.prop.dmg, aes(x=event.type, y=prop.dmg.total)) + 
  geom_bar(stat="identity") + xlab("Event type") + 
  ylab("Total property damage") +  ggtitle("Property Damage By Event Type") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
```

The data shows that flash floods and thunderstorm winds cost the largest
property damages among weather-related natural diseasters after Tornados. 