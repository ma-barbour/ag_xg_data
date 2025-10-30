# This script generates xG data to be shared with Apples & Ginos
# It needs to be integrated with data pulled from Natural Stat Trick (skater names)
# It will be run automatically using task scheduler

### BASIC SETUP ################################################################

setwd("~/18_skaters/r_studio/every_day/ag_xg_data")

library(tidymodels)
library(readr)
library(stringr)
library(bundle)
library(rvest)
library(janitor)
library(jsonlite)
library(lubridate)
library(zoo)

current_season <- 20252026

#current_date <- as.Date("2025-04-18")
current_date <- Sys.Date()

toi_filter_5 <- 20
toi_filter_season <- case_when(current_date < as.Date("2025-11-01") ~ 20,
                               current_date < as.Date("2025-12-01") ~ 50,
                               current_date < as.Date("2026-01-01") ~ 100,
                               current_date < as.Date("2026-02-01") ~ 150,
                               current_date < as.Date("2026-03-01") ~ 200,
                               TRUE ~ 250)

refresh_date <- current_date - 8

refresh_all_data = FALSE
#refresh_all_data = TRUE

max_retries <- 3
sleep_time <- 5

### NATURAL STAT TRICK DATA ####################################################

# Pull NST Function

nst_pull <- function(url) {
        
        nst_css_selector <- "body > div:nth-child(1) > div:nth-child(8) > div > div"
        
        nst_page <- read_html(url) 
        
        nst_data <- html_element(nst_page, css = nst_css_selector) |>
                html_table()  |>
                as.data.frame()
        
        nst_data <- nst_data[-1]
        
        nst_data <- clean_names(nst_data)
        
        nst_ids <- as.data.frame(html_attr(html_nodes(nst_page, "a"), "href"))
        names(nst_ids)[1] <- "href"
        nst_ids <- mutate(nst_ids, length = nchar(nst_ids$href)) |>
                filter(length > 85) |>
                select(c(1))
        nst_ids$skater_id <- substring(nst_ids$href, regexpr("id=", nst_ids$href) + 3)
        nst_ids$skater_id <- substr(nst_ids$skater_id, 1, 7)
        nst_ids <- as.integer(nst_ids$skater_id)
        
        nst_data$skater_id <- nst_ids
        
        return(nst_data)
        
}

# Pull NST Data - Last 5 GP [Individual]

# NST URL

nst_url_5gp_ind <- paste0("https://www.naturalstattrick.com/playerteams.php?fromseason=", current_season, "&thruseason=", current_season, "&stype=2&sit=all&score=all&stdoi=std&rate=y&team=ALL&loc=B&toi=", toi_filter_5, "&gpfilt=gpteam&fd=&td=&tgp=5&lines=single&draftteam=ALL")

# Pull Data

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        nst_data_5gp_ind <- tryCatch(
                
                # This is the action
                
                {nst_data_5gp_ind <- nst_pull(nst_url_5gp_ind) 
                
                nst_data_5gp_ind <- nst_data_5gp_ind |>
                        mutate(position = if_else(position == "D", "D", "F")) |>
                        mutate(goals = round(goals_60 * (toi / 60))) |>
                        mutate(assists = round((first_assists_60 * (toi / 60)) + (second_assists_60 * (toi / 60)))) |>
                        mutate(points = goals + assists) |>
                        group_by(position) |>
                        mutate(i_shots_60_rank = as.integer(rank(-shots_60, ties.method = "first"))) |>
                        mutate(i_cf_60_rank = as.integer(rank(-i_cf_60, ties.method = "first"))) |>
                        mutate(i_scf_60_rank = as.integer(rank(-i_scf_60, ties.method = "first"))) |>
                        ungroup() |>
                        select(skater_id,
                               #skater = player,
                               position,
                               toi,
                               gp,
                               goals,
                               assists,
                               points,
                               toi_gp,
                               toi,
                               i_shots_60_rank,
                               i_cf_60_rank,
                               i_scf_60_rank,
                               ipp,
                               sh_percent)},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(nst_data_5gp_ind) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at nst_data_5gp_ind with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Pull NST Data - Last 5 GP [On-Ice]

# NST URL

nst_url_5gp_oi <- paste0("https://www.naturalstattrick.com/playerteams.php?fromseason=", current_season, "&thruseason=", current_season, "&stype=2&sit=all&score=all&stdoi=oi&rate=y&team=ALL&pos=S&loc=B&toi=", toi_filter_5, "&gpfilt=gpteam&fd=&td=&tgp=5&lines=single&draftteam=ALL")

# Pull Data

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        nst_data_5gp_oi <- tryCatch(
                
                # This is the action
                
                {nst_data_5gp_oi <- nst_pull(nst_url_5gp_oi)
                
                nst_data_5gp_oi <- nst_data_5gp_oi |>
                        mutate(position = if_else(position == "D", "D", "F")) |>
                        group_by(position) |>
                        mutate(oi_cf_60_rank = as.integer(rank(-cf_60, ties.method = "first"))) |>
                        mutate(oi_scf_60_rank = as.integer(rank(-scf_60, ties.method = "first"))) |>
                        ungroup() |>
                        select(skater_id,
                               oi_cf_60_rank,
                               oi_scf_60_rank,
                               oi_sh_percent = on_ice_sh_percent)},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(nst_data_5gp_oi) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at nst_data_5gp_oi with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Pull NST Data - Full Season [Individual]

# NST URL

nst_url_season_ind <- paste0("https://www.naturalstattrick.com/playerteams.php?fromseason=", current_season, "&thruseason=", current_season, "&stype=2&sit=all&score=all&stdoi=std&rate=y&team=ALL&pos=S&loc=B&toi=", toi_filter_season, "&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL")

# Pull Data

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        nst_data_season_ind <- tryCatch(
                
                # This is the action
                
                {nst_data_season_ind <- nst_pull(nst_url_season_ind)
                
                nst_data_season_ind <- nst_data_season_ind |>
                        mutate(position = if_else(position == "D", "D", "F")) |>
                        mutate(goals = round(goals_60 * (toi / 60))) |>
                        mutate(assists = round((first_assists_60 * (toi / 60)) + (second_assists_60 * (toi / 60)))) |>
                        mutate(points = goals + assists) |>
                        mutate(goals_82 = round((goals / gp) * 82)) |>
                        mutate(points_82 = round((points / gp) * 82)) |>
                        group_by(position) |>
                        mutate(i_shots_60_rank = as.integer(rank(-shots_60, ties.method = "first"))) |>
                        mutate(i_cf_60_rank = as.integer(rank(-i_cf_60, ties.method = "first"))) |>
                        mutate(i_scf_60_rank = as.integer(rank(-i_scf_60, ties.method = "first"))) |>
                        ungroup() |>
                        select(skater_id,
                               #skater = player,
                               position,
                               toi,
                               gp,
                               goals,
                               assists,
                               points,
                               goals_82,
                               points_82,
                               toi_gp,
                               i_shots_60_rank,
                               i_cf_60_rank,
                               i_scf_60_rank,
                               ipp,
                               sh_percent)},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(nst_data_season_ind) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at nst_data_season_ind with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Pull NST Data - Full Season [On-Ice]

# NST URL

nst_url_season_oi <- paste0("https://www.naturalstattrick.com/playerteams.php?fromseason=", current_season, "&thruseason=", current_season, "&stype=2&sit=all&score=all&stdoi=oi&rate=y&team=ALL&pos=S&loc=B&toi=", toi_filter_season, "&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL")

# Pull Data

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        nst_data_season_oi <- tryCatch(
                
                # This is the action
                
                {nst_data_season_oi <- nst_pull(nst_url_season_oi)
                
                nst_data_season_oi <- nst_data_season_oi |>
                        mutate(position = if_else(position == "D", "D", "F")) |>
                        group_by(position) |>
                        mutate(oi_cf_60_rank = as.integer(rank(-cf_60, ties.method = "first"))) |>
                        mutate(oi_scf_60_rank = as.integer(rank(-scf_60, ties.method = "first"))) |>
                        ungroup() |>
                        select(skater_id,
                               oi_cf_60_rank,
                               oi_scf_60_rank,
                               oi_sh_percent = on_ice_sh_percent)},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(nst_data_season_oi) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at nst_data_season_oi with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Join Full Season Data

nst_data_season <- nst_data_season_ind |>
        left_join(nst_data_season_oi, by = "skater_id")

### NHL GAME LOGS ##############################################################

# NHL URL

nhl_gl_url <- paste0("https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22goals%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22assists%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=-1&cayenneExp=gameDate%3C=%22", current_date - 1, "%2023%3A59%3A59%22%20and%20gameDate%3E=%22", current_date - 30, "%22%20and%20gameTypeId=2")

# Pull Data

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        nhl_gl_data <- tryCatch(
                
                # This is the action
                
                {nhl_gl_site <- read_json(nhl_gl_url)
                
                nhl_gl_data <- nhl_gl_site[["data"]] |>
                        tibble() |>
                        unnest_wider(1) |>
                        select(skater_id = playerId,
                               skater = skaterFullName,
                               game_id = gameId,
                               date = gameDate) |>
                        arrange(desc(date)) |>
                        arrange(skater_id) |>
                        group_by(skater_id) |>
                        mutate(game_count = row_number()) |>
                        ungroup()},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(nhl_gl_data) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at nhl_gl_data with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

### SCHEDULE ###################################################################

# Get Season Schedule Function

get_season_schedule <- function(season) {
        
        # Get team tri-codes for the season (using MTL as the base club)
        
        tri_code_url <- paste0("https://api-web.nhle.com/v1/club-schedule-season/mtl/", season)
        
        tri_code_data <- read_json(tri_code_url)
        
        tri_codes <- tri_code_data[["games"]] |>
                tibble() |>
                unnest_wider(1) |>
                filter(gameType == 2) |>
                select(awayTeam) |>
                unnest_wider(1)
        
        tri_codes <- unique(tri_codes$abbrev)
        
        # Loop through each team's season schedule
        
        base_url <- "https://api-web.nhle.com/v1/club-schedule-season/"
        
        schedule_loop_data <- list()
        
        for (i in (1:length(tri_codes))) {
                
                temp_schedule_data <- read_json(paste0(base_url, tri_codes[i], "/", season))
                
                temp_schedule <- temp_schedule_data[["games"]] |>
                        tibble() |>
                        unnest_wider(1) |>
                        filter(gameType == 2) |>
                        unnest_wider(awayTeam, names_sep = "_") |>
                        unnest_wider(homeTeam, names_sep = "_") |>
                        select(game_id = id,
                               season,
                               date = gameDate,
                               away_team = awayTeam_abbrev,
                               home_team = homeTeam_abbrev)
                
                temp_schedule$date <- as.Date(temp_schedule$date)
                
                schedule_loop_data[[i]] <- temp_schedule
                
        }
        
        # Combine the loop data
        
        schedule_data <- schedule_loop_data |>
                bind_rows()
        
        # Remove duplicates
        
        schedule_data <- unique(schedule_data)
        
        # Arrange by date
        
        schedule_data <- schedule_data |>
                arrange(date)
        
        return(schedule_data)
        
}

# Load / Get Schedule

if(!file.exists("schedule.rds")) {
        
        schedule <- get_season_schedule(current_season)
        
        write_rds(schedule,
                  "schedule.rds")
        
} else {
        
        schedule <- read_rds("schedule.rds")
        
}

### RAW PBP DATA ###############################################################

# Load Functions

get_play_by_play_data <- readRDS("~/18_skaters/r_studio/xg_model/get_play_by_play_data.rds")

get_on_ice_html <- readRDS("~/18_skaters/r_studio/xg_model/get_on_ice_html.rds")

# Get Refresh Period Data

new_game_ids <- schedule |>
        filter(date >= refresh_date) |>
        filter(date < current_date) |>
        select(game_id) |>
        as_vector()

new_pbp_data <- list()
new_oi_data <- list()

error_message <- NULL

for(i in 1:length(new_game_ids)) {
        
        print(paste0("Pulling data for game_id: ", new_game_ids[i]))
        
        loop_pbp <- tryCatch(
                
                # This is the action
                
                {loop_pbp <- get_play_by_play_data(new_game_ids[i])},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        cat("Error caught: ", e$message, "\n")})
        
        if (is.null(error_message) == TRUE) {
                
                loop_oi <- tryCatch(
                        
                        # This is the action
                        
                        {loop_oi <- get_on_ice_html(new_game_ids[i])},
                        
                        # This deals with errors
                        
                        error = function(e) {
                                
                                error_message <<- e$message
                                cat("Error caught: ", e$message, "\n")})
                
        }
        
        new_pbp_data[[i]] <- loop_pbp
        new_oi_data[[i]] <- loop_oi
        
        if (is.null(error_message) == FALSE) {
                
                error_report <- paste("Failed at refresh period data with error message:", error_message)
                saveRDS(error_report, "error_report.rds")
                stop()
                
        }
        
}

new_pbp_data <- new_pbp_data |>
        bind_rows()

new_oi_data <- new_oi_data |>
        bind_rows()

# Update Full Season Data

if(!file.exists("full_season_pbp.rds")) {
        
        write_rds(new_pbp_data,
                  "full_season_pbp.rds")
        full_season_pbp <- new_pbp_data
        
} else {
        
        full_season_pbp <- read_rds("full_season_pbp.rds")
        
        full_season_pbp <- full_season_pbp |>
                filter(!game_id %in% new_game_ids) |>
                bind_rows(new_pbp_data)
        
        write_rds(full_season_pbp,
                  "full_season_pbp.rds")
        
}

if(!file.exists("full_season_oi.rds")) {
        
        write_rds(new_oi_data,
                  "full_season_oi.rds")
        full_season_oi <- new_oi_data
        
} else {
        
        full_season_oi <- read_rds("full_season_oi.rds")
        
        full_season_oi <- full_season_oi |>
                filter(!game_id %in% new_game_ids) |>
                bind_rows(new_oi_data)
        
        write_rds(full_season_oi,
                  "full_season_oi.rds")
        
}

# Option To Refresh All Data

if(refresh_all_data == TRUE) {
        
        all_game_ids <- schedule |>
                filter(date < current_date) |>
                select(game_id) |>
                as_vector()
        
        all_pbp_data <- list()
        all_oi_data <- list()
        
        for(i in 1:length(all_game_ids)) {
                
                print(paste0("Pulling data for game_id: ", all_game_ids[i]))
                
                loop_pbp <- get_play_by_play_data(all_game_ids[i])
                loop_oi <- get_on_ice_html(all_game_ids[i])
                
                all_pbp_data[[i]] <- loop_pbp
                all_oi_data[[i]] <- loop_oi
                
        }
        
        full_season_pbp <- all_pbp_data |>
                bind_rows()
        
        full_season_oi <- all_oi_data |>
                bind_rows()
        
        write_rds(full_season_pbp,
                  "full_season_pbp.rds")
        
        write_rds(full_season_oi,
                  "full_season_oi.rds")
        
}

### EXPECTED GOALS #############################################################

# Load Functions and Models

xgboost_preprocessing_xg_2025 <- readRDS("~/18_skaters/r_studio/xg_model/xg_model_2025/xgboost_preprocessing_xg_2025.rds")

xg_model_v3_2025 <- readRDS("~/18_skaters/r_studio/xg_model/xg_model_2025/xg_model_v3_2025.rds") |>
        unbundle()

shot_prob_model_2025 <- readRDS("~/18_skaters/r_studio/xg_model/xg_model_2025/shot_prob_model_2025.rds") |>
        unbundle()

xg_adjustment_2025 <- readRDS("~/18_skaters/r_studio/xg_model/xg_model_2025/xg_adjustment_2025.rds")

# Add xG

full_season_pbp <- full_season_pbp |>
        mutate(temp_id = row_number())

xg_data <- xgboost_preprocessing_xg_2025(full_season_pbp)

xg_predictions <- predict(xg_model_v3_2025, 
                          new_data = xg_data,
                          type = "prob") |>
        select(.pred_goal)

xg_data$xg <- xg_predictions$.pred_goal

xg_data <- xg_data |>
        select(temp_id,
               xg)

full_season_pbp <- full_season_pbp |>
        left_join(xg_data, by = "temp_id") |>
        filter(xg > 0)

# Join On-Ice

full_season_pbp <- full_season_pbp |>
        left_join(full_season_oi, join_by(game_id, game_time_s))

### FULL SEASON SUMMARY ########################################################

# Individual xG

full_season_xg <- full_season_pbp |>
        group_by(shooter_id) |>
        summarise(i_xg_raw = sum(xg),
                  .groups = "drop") |>
        left_join(xg_adjustment_2025,
                  by = "shooter_id") |>
        mutate(xg_adjustment_500 = if_else(is.na(xg_adjustment_500) == TRUE, 1, xg_adjustment_500)) |>
        mutate(i_xg_adjusted = i_xg_raw * xg_adjustment_500) |>
        select(1,3,2,4)

full_season <- nst_data_season |>
        left_join(full_season_xg |> rename(skater_id = shooter_id), by = "skater_id") |>
        mutate(i_xg_raw_60 = i_xg_raw / (toi / 60)) |>
        mutate(i_xg_adjusted_60 = i_xg_adjusted / (toi / 60)) |>
        group_by(position) |>
        mutate(i_xg_raw_60_rank = rank(-i_xg_raw_60, ties.method = "first")) |>
        mutate(i_xg_adjusted_60_rank = rank(-i_xg_adjusted_60, ties.method = "first"),
               .after = i_scf_60_rank) |>
        ungroup() |>
        arrange(i_xg_adjusted_60_rank, position)

# On-Ice xG

full_season_xg_oi <- full_season_pbp |>
        left_join(xg_adjustment_2025, by = "shooter_id") |>
        mutate(xg_adjustment_500 = if_else(is.na(xg_adjustment_500) == TRUE, 1, xg_adjustment_500)) |>
        mutate(xg_adj = xg * xg_adjustment_500,
               .after = xg)

oi_xg_home <- full_season_xg_oi |>
        filter(event_team_home == 1) |>
        select(home_on_ice_1:home_on_ice_6, xg, xg_adj) |>
        pivot_longer(cols = home_on_ice_1:home_on_ice_6) |>
        select(skater_id = value,
               xg,
               xg_adj) |>
        filter(skater_id > 0) |>
        group_by(skater_id) |>
        summarize(xg_home = sum(xg),
                  xg_adj_home = sum(xg_adj),
                  .groups = "drop")

oi_xg_away <- full_season_xg_oi |>
        filter(event_team_home == 0) |>
        select(away_on_ice_1:away_on_ice_6, xg, xg_adj) |>
        pivot_longer(cols = away_on_ice_1:away_on_ice_6) |>
        select(skater_id = value,
               xg,
               xg_adj) |>
        filter(skater_id > 0) |>
        group_by(skater_id) |>
        summarize(xg_away = sum(xg),
                  xg_adj_away = sum(xg_adj),
                  .groups = "drop")

oi_xg_full <- oi_xg_home |>
        full_join(oi_xg_away, by = "skater_id")

oi_xg_full[is.na(oi_xg_full)] <- 0

oi_xg_full <- oi_xg_full |>
        mutate(oi_xg_raw = xg_home + xg_away) |>
        mutate(oi_xg_adjusted = xg_adj_home + xg_adj_away) |>
        select(skater_id,
               oi_xg_raw,
               oi_xg_adjusted) 

full_season <- full_season |>
        left_join(oi_xg_full, by = "skater_id") |>
        mutate(oi_xg_raw_60 = oi_xg_raw / (toi / 60)) |>
        mutate(oi_xg_adjusted_60 = oi_xg_adjusted / (toi / 60)) |>
        group_by(position) |>
        mutate(oi_xg_raw_60_rank = rank(-oi_xg_raw_60, ties.method = "first")) |>
        mutate(oi_xg_adjusted_60_rank = rank(-oi_xg_adjusted_60, ties.method = "first"),
               .after = oi_scf_60_rank) |>
        ungroup() |>
        mutate(skt_xg_adjusted_percent = i_xg_adjusted / oi_xg_adjusted,
               .after = ipp) |>
        select(skater_id:i_xg_adjusted_60_rank,
               oi_cf_60_rank:oi_xg_adjusted_60_rank,
               skt_xg_adjusted_percent,
               ipp,
               sh_percent,
               oi_sh_percent) |>
        select(-toi)

full_season[is.na(full_season)] <- 0

### 5GP SUMMARY ################################################################

# Find Skater Game Logs

skater_ids_5gp <- nst_data_5gp_ind$skater_id

nhl_gl_data <- nhl_gl_data |>
        filter(skater_id %in% skater_ids_5gp) |>
        left_join(nst_data_5gp_ind |> select(skater_id, gp), by = "skater_id") |>
        filter(game_count <= gp)

# Gather xG Data

five_game_xg <- list()

for(i in 1:length(skater_ids_5gp)) {
        
        loop_game_ids <- nhl_gl_data |>
                filter(skater_id == skater_ids_5gp[i])
        loop_game_ids <- loop_game_ids$game_id
        
        loop_data <- full_season_pbp |>
                filter(game_id %in% loop_game_ids) |>
                left_join(xg_adjustment_2025, by = "shooter_id") |>
                mutate(xg_adjustment_500 = if_else(is.na(xg_adjustment_500) == TRUE, 1, xg_adjustment_500)) |>
                mutate(xg_adjusted = xg * xg_adjustment_500)
        
        loop_xg <- loop_data |>
                group_by(shooter_id) |>
                summarise(i_xg_adjusted = sum(xg_adjusted),
                          #i_xg_delete = sum(xg),
                          .groups = "drop") |>
                filter(shooter_id == skater_ids_5gp[i])
        
        oi_xg_home <- loop_data |>
                filter(event_team_home == 1) |>
                select(home_on_ice_1:home_on_ice_6, xg_adjusted) |>
                pivot_longer(cols = home_on_ice_1:home_on_ice_6) |>
                select(skater_id = value,
                       xg_adjusted) |>
                filter(skater_id > 0) |>
                group_by(skater_id) |>
                summarize(xg_adjusted_home = sum(xg_adjusted),
                          .groups = "drop")
        
        oi_xg_away <- loop_data |>
                filter(event_team_home == 0) |>
                select(away_on_ice_1:away_on_ice_6, xg_adjusted) |>
                pivot_longer(cols = away_on_ice_1:away_on_ice_6) |>
                select(skater_id = value,
                       xg_adjusted) |>
                filter(skater_id > 0) |>
                group_by(skater_id) |>
                summarize(xg_adjusted_away = sum(xg_adjusted),
                          .groups = "drop")
        
        oi_xg_full <- oi_xg_home |>
                full_join(oi_xg_away, by = "skater_id")
        
        oi_xg_full[is.na(oi_xg_full)] <- 0
        
        oi_xg_full <- oi_xg_full |>
                mutate(oi_xg_adjusted = xg_adjusted_home + xg_adjusted_away) |>
                select(skater_id,
                       oi_xg_adjusted) |>
                filter(skater_id == skater_ids_5gp[i])
        
        loop_xg <- loop_xg |>
                full_join(oi_xg_full |> rename(shooter_id = skater_id), by = "shooter_id")
        
        loop_xg[is.na(loop_xg)] <- 0
        
        five_game_xg[[i]] <- loop_xg 
        
}

five_game_xg <- five_game_xg |>
        bind_rows()

# Join xG Data To NST

five_gp <- nst_data_5gp_ind |>
        left_join(nst_data_5gp_oi, by = "skater_id") |>
        left_join(five_game_xg |> rename(skater_id = shooter_id), by = "skater_id")

five_gp[is.na(five_gp)] <- 0

five_gp <- five_gp |>
        mutate(i_xg_adjusted_60 = i_xg_adjusted / (toi / 60)) |>
        mutate(oi_xg_adjusted_60 = oi_xg_adjusted / (toi / 60)) |>
        group_by(position) |>
        mutate(i_xg_adjusted_60_rank = rank(-i_xg_adjusted_60, ties.method = "first"),
               .after = i_scf_60_rank) |>
        mutate(oi_xg_adjusted_60_rank = rank(-oi_xg_adjusted_60, ties.method = "first"),
               .after = oi_scf_60_rank) |>
        ungroup() |>
        mutate(skt_xg_adjusted_percent = i_xg_adjusted / oi_xg_adjusted,
               .after = ipp) |>
        select(skater_id:i_xg_adjusted_60_rank,
               oi_cf_60_rank:oi_xg_adjusted_60_rank,
               skt_xg_adjusted_percent,
               ipp,
               sh_percent,
               oi_sh_percent) |>
        select(-toi) |>
        arrange(position) |>
        arrange(i_xg_adjusted_60_rank)

### ADD SKATER NAMES ###########################################################

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        skater_names <- tryCatch(
                
                # This is the action
                
                {skater_names_data <- read_json(paste0("https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22goals%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22assists%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=-1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=", current_season, "%20and%20seasonId%3E=", current_season))
                
                skater_names <- skater_names_data[["data"]] |>
                        tibble() |>
                        unnest_wider(1) |>
                        select(skater_id = playerId,
                               skater = skaterFullName)},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(skater_names) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at skater_names with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Add names to data

full_season <- full_season |>
        left_join(skater_names, by = "skater_id") |>
        select(skater_id,
               skater,
               position:oi_sh_percent)

five_gp <- five_gp |>
        left_join(skater_names, by = "skater_id") |>
        select(skater_id,
               skater,
               position:oi_sh_percent)

### DATA VALIDATION ############################################################

data_validation <- select(full_season, skater, skater_id) |>
        bind_rows(select(five_gp, skater, skater_id)) |>
        unique()

# Get current teams

retry_count <- 0
error_message <- NULL

while (retry_count < max_retries) {
        
        teams_data <- tryCatch(
                
                # This is the action
                
                {teams_json <- read_json(paste0("https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22goals%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22assists%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=-1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=", current_season, "%20and%20seasonId%3E=", current_season))
                
                teams_data <- teams_json[["data"]] |>
                        tibble() |>
                        unnest_wider(1) |>
                        select(skater_id = playerId,
                               team = teamAbbrevs) |>
                        mutate(team = str_sub(team, -3, -1))},
                
                # This deals with errors
                
                error = function(e) {
                        
                        error_message <<- e$message
                        retry_count <<- retry_count + 1
                        cat("Error caught: ", e$message, "\n")
                        
                        if (retry_count < max_retries) {
                                cat("Pausing before retry...\n")
                                Sys.sleep(sleep_time)}})
        
        # This ends the loop if the data was pulled
        
        if (is.null(teams_data) == FALSE) {break}
        
}

# This saves an error report and stops the script in the event of a failure

if (is.null(error_message) == FALSE) {
        
        error_report <- paste("Failed at teams_data with error message:", error_message)
        saveRDS(error_report, "error_report.rds")
        stop()
        
}

# Add teams data to data validation

data_validation <- data_validation |>
        left_join(teams_data, by = "skater_id") |>
        arrange(skater)

# Add position to data validation

skater_positions <- skater_names_data[["data"]] |>
        tibble() |>
        unnest_wider(1) |>
        select(skater_id = playerId,
               pos = positionCode) |>
        mutate(pos = if_else(pos == "D", "D", "F"))

data_validation <- data_validation |>
        left_join(skater_positions, by = "skater_id") 

# Fix for double Elias Pettersson

full_season <- full_season |>
        mutate(skater = if_else(skater_id == 8483678, "Elias Pettersson D", skater)) |>
        mutate(skater = if_else(skater_id == 8480012, "Elias Pettersson F", skater))

five_gp <- five_gp |>
        mutate(skater = if_else(skater_id == 8483678, "Elias Pettersson D", skater)) |>
        mutate(skater = if_else(skater_id == 8480012, "Elias Pettersson F", skater))

data_validation <- data_validation |>
        mutate(skater = if_else(skater_id == 8483678, "Elias Pettersson D", skater)) |>
        mutate(skater = if_else(skater_id == 8480012, "Elias Pettersson F", skater))

# Add team level xG data - full season

season_home_games_xg <- full_season_xg_oi |>
        group_by(game_id, date, home_team, away_team) |>
        filter(event_team_home == 1) |>
        summarise(home_xg = sum(xg_adj),
                  .groups = "drop")

season_away_games_xg <- full_season_xg_oi |>
        group_by(game_id, date, home_team, away_team) |>
        filter(event_team_home == 0) |>
        summarise(away_xg = sum(xg_adj),
                  .groups = "drop")

season_games_xg <- season_home_games_xg |>
        left_join(season_away_games_xg |> select(game_id, away_xg),
                  by = "game_id")

season_home_team_xg <- season_games_xg |>
        group_by(home_team) |>
        summarise(home_team_xg = sum(home_xg),
                  home_opp_xg = sum(away_xg),
                  .groups = "drop") |>
        rename(team = home_team)

season_away_team_xg <- season_games_xg |>
        group_by(away_team) |>
        summarise(away_team_xg = sum(away_xg),
                  away_opp_xg = sum(home_xg),
                  .groups = "drop") |>
        rename(team = away_team)

season_team_xg <- season_home_team_xg |>
        left_join(season_away_team_xg,
                  by = "team") |>
        mutate(xg_season = home_team_xg + away_team_xg) |>
        mutate(opp_xg_season = home_opp_xg + away_opp_xg) |>
        mutate(xg_perc_season = xg_season / (xg_season + opp_xg_season)) |>
        arrange(team) |>
        select(team,
               xg_season,
               opp_xg_season,
               xg_perc_season)

# Add team level xG data - last 5 GP

five_gp_team_xg <- list()
teams <- season_team_xg$team

for(i in 1:length(teams)) {
        
        loop_xg_data <- full_season_xg_oi |>
                filter(home_team == teams[i] | away_team == teams[i]) |>
                arrange(-game_id) |>
                mutate(game_number = cumsum(!duplicated(game_id))) |>
                filter(game_number <= 5)
        
        loop_five_gp_home_games_xg <- loop_xg_data |>
                group_by(game_id, date, home_team, away_team) |>
                filter(event_team_home == 1) |>
                summarise(home_xg = sum(xg_adj),
                          .groups = "drop")
        
        loop_five_gp_away_games_xg <- loop_xg_data |>
                group_by(game_id, date, home_team, away_team) |>
                filter(event_team_home == 0) |>
                summarise(away_xg = sum(xg_adj),
                          .groups = "drop")
        
        loop_five_gp_games_xg <- loop_five_gp_home_games_xg |>
                full_join(loop_five_gp_away_games_xg |> select(game_id, away_xg),
                          by = "game_id")
        
        loop_five_gp_home_team_xg <- loop_five_gp_games_xg |>
                group_by(home_team) |>
                summarise(home_team_xg = sum(home_xg),
                          home_opp_xg = sum(away_xg),
                          .groups = "drop") |>
                rename(team = home_team)
        
        loop_five_gp_away_team_xg <- loop_five_gp_games_xg |>
                group_by(away_team) |>
                summarise(away_team_xg = sum(away_xg),
                          away_opp_xg = sum(home_xg),
                          .groups = "drop") |>
                rename(team = away_team)
        
        loop_five_gp_team_xg <- loop_five_gp_home_team_xg |>
                full_join(loop_five_gp_away_team_xg,
                          by = "team") |>
                mutate(across(where(is.numeric), ~replace_na(., 0))) |>
                mutate(xg_five_gp = home_team_xg + away_team_xg) |>
                mutate(opp_xg_five_gp = home_opp_xg + away_opp_xg) |>
                mutate(xg_perc_five_gp = xg_five_gp / (xg_five_gp + opp_xg_five_gp))  |>
                arrange(team) |>
                select(team,
                       xg_five_gp,
                       opp_xg_five_gp,
                       xg_perc_five_gp)
        
        loop_five_gp_team_xg <- loop_five_gp_team_xg |>
                filter(team == teams[i])
        
        five_gp_team_xg[[i]] <- loop_five_gp_team_xg
        
}

five_gp_team_xg <- five_gp_team_xg |>
        bind_rows()

team_xg_data <- season_team_xg |>
        left_join(five_gp_team_xg, by = "team") |>
        mutate(logo = paste0("https://assets.nhle.com/logos/nhl/svg/", team, "_light.svg"))

### PUSH TO GOOGLE #############################################################

library(googledrive)
library(googlesheets4)

full_season <- full_season |>
        mutate(updated = current_date,
               toi_filter = toi_filter_season)

five_gp <- five_gp |>
        mutate(updated = current_date,
               toi_filter = toi_filter_5)

g_sheet <- "https://docs.google.com/spreadsheets/d/1FxlF_jSAyqgHbhx7SoD5ha5LxkJCssYx-spycz6Ev2A/edit?gid=0#gid=0"

gs4_auth(email = "18skaters@gmail.com")

sheet_write(full_season, 
            ss = g_sheet, 
            sheet = "full_season")

sheet_write(five_gp, 
            ss = g_sheet, 
            sheet = "five_gp")

sheet_write(data_validation,
            ss = g_sheet,
            sheet = "data_validation")

sheet_write(team_xg_data,
            ss = g_sheet,
            sheet = "team_xg_data")

