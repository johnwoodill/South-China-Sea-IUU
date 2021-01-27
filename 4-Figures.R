library(tidyverse)
library(feather)
library(lubridate)
library(cowplot)

setwd("~/Projects/South-China-Sea-IUU/")

nipy_spectral <- c("#000000", "#6a009d", "#0035dd", "#00a4bb", "#009b0f",
                   "#00e100", "#ccf900", "#ffb000", "#e50000")




build_plot <- function(dat, null_dat, lag=8, start_date, end_date=NULL, event_date, save_name){
  dat <- dplyr::filter(dat, t >= start_date & t <= end_date)
  
  p1 <- ggplot(dat, aes(t, lag, fill=ks)) + 
    geom_tile() +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct(event_date, tz = "GMT")), color='red') +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct("2017-03-26 12:00:00")), color='red') +
    # scale_fill_gradientn(colors = nipy_spectral[3:9], limits = c(0, round(max(dat$ks), 3)), breaks = seq(0, round(max(dat$ks), 2), 0.02)) +
    scale_fill_continuous(type = "viridis", limits = c(0, round(max(dat$ks), 3)), breaks = seq(0, round(max(dat$ks), 2), 0.01)) +
    scale_y_continuous(expand = c(0,0), breaks = seq(1, length(unique(dat$lag)), 24), labels = seq(1, 10, 1), limits = c(0, length(unique(dat$lag)))) +
    scale_x_datetime(expand = c(0, 0), breaks = "1 days") +
    labs(x=NULL, y="Lagged Days", fill="KS") + 
    guides(fill = guide_colorbar(title.position = "top", 
                            direction = "vertical",
                            frame.colour = "black",
                            barwidth = 1,
                            barheight = 17)) +
    
    theme_bw() +
    NULL
  
  
  lag_days = lag
  
  pdat <- dat %>% 
    select(-pvalue, -kl, -`Unnamed: 0`, -X1) %>%
    # select(-pvalue, -ks, -js, -hl) %>%
    filter(lag <= 24*lag_days) %>%
    gather(key = stat, value = value, -t, -lag) %>% 
    group_by(t, stat) %>% 
    summarise(mean = mean(value),
              kurt = moments::kurtosis(value)) %>% 
    ungroup()
  
  null_dat <- rbind(dat, null_dat)
  
  beg_null_date = as.Date(event_date) + 1
  last_null_date = as.Date(beg_null_date) + 14
  
  pndat <- null_dat %>% 
    filter(t >= beg_null_date & t <= last_null_date) %>% 
    select(-pvalue, -kl, -`Unnamed: 0`, -X1) %>%
    # select(-pvalue, -ks, -js, -hl) %>%
    filter(lag <= 24*lag_days) %>%
    gather(key = stat, value = value, -t, -lag) %>% 
    group_by(t, stat) %>% 
    summarise(mean = mean(value),
              kurt = moments::kurtosis(value)) %>% 
    ungroup()
  
  
  m95 <- quantile(pndat$mean, 0.99, na.rm=TRUE)
  k95 <- quantile(pndat$kurt, 0.99, na.rm=TRUE)
  
  pdat$m95 <- ifelse(pdat$mean >= m95, 1, 0)
  pdat$k95 <- ifelse(pdat$kurt >= k95, 1, 0)
  
  
  p2 <- ggplot(pdat, aes(t, mean, color=stat)) + geom_line(color='black') +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct(event_date, tz = "GMT")), color='red') +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct("2017-03-26 12:00:00")), color='red') +
    geom_point(data = filter(pdat, m95 == 1), aes(t, mean, color=factor(m95))) +
    theme_bw() +
    labs(x=NULL, y=paste0("Mean Anomaly Index (", lag_days, "-day lag)")) +
    scale_x_datetime(breaks = "2 days") +
    # theme(axis.text.x = element_text(hjust = 1)) +
    theme(legend.position = "none") +
    NULL
  
  p3 <- ggplot(pdat, aes(t, kurt, color=stat)) + geom_line(color='black') +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct(event_date, tz = "GMT")), color='red') +
    geom_vline(data = NULL, aes(xintercept = as.POSIXct("2017-03-26 12:00:00")), color='red') +
    geom_point(data = filter(pdat, k95 == 1), aes(t, kurt, color=factor(k95))) +
    theme_bw() +
    labs(x=NULL, y=paste0("Kurtosis Anomaly Index (", lag_days, "-day lag)")) +
    scale_x_datetime(breaks = "2 days") +
    theme(legend.position = "none") +
    NULL
  
  
  main_plot <- gridExtra::grid.arrange(p1, p2, p3, nrow = 2, layout_matrix = cbind(c(1, 2), c(1, 3)))
  
  ggsave(plot = main_plot, paste0("figures/", save_name, ".png"), height = 8, width = 12)
  main_plot
}



# Colombia sub #1
dat1 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2016-04-30_2016-05-28.csv"))
dat11 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2016-05-29_2016-06-29.csv"))
build_plot(dat1, dat11, lag = 6, start_date = "2016-05-09 12:00:00", end_date = "2016-05-17 00:00:00", 
           event_date = "2016-05-13 00:00:00", save_name = "Colombia_2016-05-13")



# Indonesia
dat2 <- as.data.frame(read_csv('~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2016-05-12_2016-06-11.csv'))
dat22 <- as.data.frame(read_csv('~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2016-06-12_2016-07-12.csv'))
build_plot(dat2, dat22, lag = 5, start_date = "2016-05-22 00:00:00", end_date = "2016-06-05 23:00:00", 
           event_date = "2016-05-28 00:00:00", save_name = "Indonesia_2016-05-27")



# # Korean Waters (NOT CLEAR ENOUGH SIGNAL)
# dat3 <- as.data.frame(read_csv('~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2016-10-15_2016-11-15.csv'))
# build_plot(dat3, lag = 4, start_date = "2016-10-20 00:00:00", end_date = "2016-11-10", event_date = "2016-11-01 00:00:00", save_name = "Korea_2016-11-01")



# Dominican Republic
dat4 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2017-03-20_2017-04-20.csv"))
dat44 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2017-04-21_2017-05-21.csv"))
build_plot(dat4, dat44, lag = 10, start_date = "2017-03-26 00:00:00", end_date = "2017-04-09 00:00:00", 
           event_date = "2017-04-05 00:00:00", save_name = "DomRep_2017-04-05")



# Liberia
dat5 <- as.data.frame(read_csv('~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2017-09-13_2017-10-13.csv'))
dat55 <- as.data.frame(read_csv('~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2017-10-14_2017-11-14.csv'))
build_plot(dat5, dat55, lag = 6, start_date = "2017-09-18 23:00:00", end_date = "2017-10-03 23:00:00", 
           event_date = "2017-09-28 00:00:00", save_name = "Liberia_2017-09-28")



# Colombia sub #2
dat6 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2018-03-28_2018-04-22.csv"))
dat66 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2018-04-23_2018-05-23.csv"))
build_plot(dat6, dat66, lag = 6, start_date = "2018-04-03 00:00:00", end_date = "2018-04-14 00:00:00", 
           event_date = "2018-04-07 00:00:00", save_name = "Colombia_2018-04-07")




# Malaysia
dat7 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2018-06-03_2018-07-03.csv"))
dat77 <- as.data.frame(read_csv("~/Projects/South-China-Sea-IUU/data/South-China-Sea-IUU_KSDaily_2018-07-04_2018-08-04.csv"))
build_plot(dat7, dat77, lag = 5, start_date = "2018-06-09 00:00:00", end_date = "2018-06-24 00:00:00", 
           event_date = "2018-06-19 00:00:00", save_name = "Malaysia_2018-06-19")






