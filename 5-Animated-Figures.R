library(tidyverse)
library(feather)
library(marmap)
library(scales)
library(gganimate)

`%notin%` <- Negate(`%in%`)

setwd("~/Projects/South-China-Sea-IUU/")

aisdat <- read_csv("data/South-China-Sea-IUU_inter_hourly_loc_2016-09-22_2016-10-22.csv")
aisdat$t <- as.character(aisdat$t)
aisdat <- arrange(aisdat, t)

# dat <- as.data.frame(read_csv('~/Projects/IUU-test/data/South-China-Sea_KSDaily_2016-09-22_2016-10-22.csv'))

faisdat <- dplyr::filter(aisdat, timestamp == as.Date("2016-10-01 12:00:00"))
nrow(faisdat)

lon1 = 124.744211 - 3
lon2 = 124.744211 + 3
lat1 = 37.769946 - 3
lat2 = 37.769946 + 3



# Remove Longbeach
# dat = dat[dat['vessel_A_lat'] <= 33.5]
# dat = dat[dat['vessel_A_lat'] >= 32.5]
# 
# dat = dat[dat['vessel_A_lon'] >= -118]
# dat = dat[dat['vessel_A_lon'] <= -116]
# 


bat <- getNOAA.bathy(lon1, lon2, lat1, lat2, res = 1, keep = TRUE)

# print("Building plot")


# faisdat1 <- filter(faisdat, vessel_A_lat <= 10 & vessel_A_lon <= -85)



p1 <- autoplot(bat, geom=c("raster"), coast = TRUE) + 
  scale_fill_gradientn(colours = c("lightsteelblue4", "lightsteelblue2"), 
                         values = rescale(c(-1000, -500, 0)),
                         guide = "colorbar",
                       limits = c(-5000, 0)) +
  coord_cartesian(expand = 0) +
  geom_point(data= aisdat, aes(lon, lat), size=0.5) +   
  theme(legend.position = "none") +
  labs(x="Longitude", y="Latitude", title = "{current_frame}") +
  transition_manual(frames = timestamp) +
  NULL

p1

print("Animating plot")
ap1 <- animate(p1, nframes = 960)

print("Saving plot")
anim_save("figures/Oct2016_animation.gif", ap1)
