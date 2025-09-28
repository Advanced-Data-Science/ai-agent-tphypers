# =============================================================================
# AI Data Collection Agent - R Implementation
# Course: Data Analysis
# Assignment: Build an AI Agent for Data Collection
# =============================================================================

# Required Libraries
# Install if not already installed:
(c("httr", "jsonlite", "dplyr", "lubridate", "ggplot2", 
   "readr", "stringr", "purrr", "config", "logger"))

library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(ggplot2)
library(readr)
library(stringr)
library(purrr)
library(config)
library(logger)

# =============================================================================
# PART 2: API Fundamentals - First API Calls
# =============================================================================

# Exercise 2.1: Simple API call without authentication
get_cat_fact <- function() {
  tryCatch({
    response <- GET("https://catfact.ninja/fact")
    
    if (status_code(response) == 200) {
      content <- content(response, "parsed", encoding = "UTF-8")
      return(content$fact)
    } else {
      message(paste("Error:", status_code(response)))
      return(NULL)
    }
  }, error = function(e) {
    message(paste("An error occurred:", e$message))
    return(NULL)
  })
}

# Get multiple cat facts
get_multiple_cat_facts <- function(n = 5) {
  facts <- vector("character", n)
  
  for (i in 1:n) {
    fact <- get_cat_fact()
    if (!is.null(fact)) {
      facts[i] <- fact
    }
    Sys.sleep(1) # Respectful delay
  }
  
  # Remove any NULL entries
  facts <- facts[facts != ""]
  
  # Save to JSON
  facts_list <- list(
    collection_date = Sys.time(),
    total_facts = length(facts),
    facts = facts
  )
  
  write_json(facts_list, "cat_facts.json", pretty = TRUE)
  return(facts)
}

#Execute the above code to collect the catfacts:
get_multiple_cat_facts()

# Exercise 2.2: API with parameters
get_public_holidays <- function(country_code = "US", year = 2024) {
  url <- paste0("https://date.nager.at/api/v3/PublicHolidays/", year, "/", country_code)
  
  tryCatch({
    response <- GET(url)
    stop_for_status(response) # This will throw an error for bad status codes
    
    holidays <- content(response, "parsed", encoding = "UTF-8")
    return(holidays)
  }, error = function(e) {
    message(paste("Request failed:", e$message))
    return(NULL)
  })
}

# Compare holidays across countries (MODIFIED)
compare_holidays <- function(countries = c("US", "DE", "JP"), year = 2024) {
  
  # Task 1: Test with 3 different countries (now a parameter with default to US, DE, JP)
  cat(sprintf("\n--- Starting Holiday Comparison for %s (Year: %d) ---\n", 
              paste(countries, collapse=", "), year))
  
  results <- data.frame(
    country = character(),
    holiday_count = integer(),
    stringsAsFactors = FALSE
  )
  
  for (country in countries) {
    cat(sprintf("\nFetching data for %s...\n", country))
    holidays <- get_public_holidays(country, year)
    
    if (!is.null(holidays)) {
      count <- length(holidays)
      
      # Task 3: Create a summary comparing holiday counts
      results <- rbind(results, data.frame(country = country, holiday_count = count))
      message(paste(country, "has", count, "public holidays in", year))
      #printing holidays
      if (count > 0) {
        # Convert list of lists (JSON) to a data frame and select required columns
        holidays_df <- bind_rows(holidays) %>%
          select(date = date, name = name)
        
        cat(sprintf("Individual Holidays for %s (First 5):\n", country))
        
        # Print only the required columns, formatted neatly
        print(
          holidays_df %>%
            mutate(date = as.Date(date)) %>% # Convert to Date object for clean printing
            arrange(date) %>%
            head(5) %>%
            mutate(name = str_trunc(name, 40, "right")) # Truncate long names for console view
        )
        if (count > 5) {
          cat(sprintf("  ... (showing first 5 of %d holidays)\n", count))
        }
      } else {
        cat("  No holidays found to list.\n")
      }
      
    } else {
      message(paste("Failed to retrieve holidays for", country))
    }
    Sys.sleep(0.5) # Respectful delay
  }
  
  cat("\n--- FINAL HOLIDAY COUNT SUMMARY ---\n")
  print(results %>% arrange(desc(holiday_count)))
  
  return(results)
}

compare_holidays()

# =============================================================================
# PART 4: AI Data Collection Agent Class (R6 Implementation)
# =============================================================================

# Install R6 if needed: install.packages("R6")
library(R6)

# Configuration Manager
ConfigManager <- R6Class("ConfigManager",
                         public = list(
                           config = NULL,
                           
                           initialize = function(config_file = "config.json") {
                             if (file.exists(config_file)) {
                               self$config <- fromJSON(config_file)
                             } else {
                               self$config <- self$default_config()
                               self$save_config(config_file)
                             }
                           },
                           
                           default_config = function() {
                             list(
                               project_name = "AI Data Collection Agent",
                               collector_name = "Student Name",
                               base_delay = 1.0,
                               max_retries = 3,
                               quality_threshold = 0.7,
                               apis = list(
                                 weather = list(
                                   base_url = "http://api.openweathermap.org/data/2.5/weather",
                                   key = "YOUR_API_KEY_HERE",
                                   rate_limit = 60 # calls per minute
                                 )
                               ),
                               collection_params = list(
                                 max_records = 100,
                                 cities = c("New York", "London", "Tokyo", "Sydney", "Toronto")
                               )
                             )
                           },
                           
                           save_config = function(filename) {
                             write_json(self$config, filename, pretty = TRUE)
                           },
                           
                           get = function(key, default = NULL) {
                             result <- self$config[[key]]
                             if (is.null(result)) default else result
                           }
                         )
)

# Main Data Collection Agent
DataCollectionAgent <- R6Class("DataCollectionAgent",
                               public = list(
                                 config = NULL,
                                 data_store = NULL,
                                 collection_stats = NULL,
                                 delay_multiplier = 1.0,
                                 logger = NULL,
                                 
                                 initialize = function(config_file = "config.json") {
                                   # Load configuration
                                   self$config <- ConfigManager$new(config_file)
                                   
                                   # Initialize data storage
                                   self$data_store <- data.frame()
                                   
                                   # Initialize statistics
                                   self$collection_stats <- list(
                                     start_time = Sys.time(),
                                     total_requests = 0,
                                     successful_requests = 0,
                                     failed_requests = 0,
                                     apis_used = character(),
                                     quality_scores = numeric()
                                   )
                                   
                                   # Setup logging
                                   self$setup_logging()
                                   
                                   log_info("Data Collection Agent initialized")
                                 },
                                 
                                 setup_logging = function() {
                                   log_threshold(INFO)
                                   log_appender(appender_tee("data_collection.log"))
                                   log_formatter(formatter_glue)
                                 },
                                 
                                 run_collection = function() {
                                   log_info("Starting data collection process")
                                   
                                   tryCatch({
                                     while (!self$collection_complete()) {
                                       # Collect batch of data
                                       data <- self$collect_batch()
                                       
                                       if (!is.null(data) && nrow(data) > 0) {
                                         self$process_and_store(data)
                                       }
                                       
                                       # Assess performance and adapt strategy
                                       self$assess_performance()
                                       
                                       # Respectful delay
                                       self$respectful_delay()
                                     }
                                     
                                     log_info("Data collection completed successfully")
                                     
                                   }, error = function(e) {
                                     log_error("Collection failed: {e$message}")
                                   }, finally = {
                                     self$generate_final_report()
                                   })
                                 },
                                 
                                 collect_batch = function() {
                                   log_info("Collecting data batch")
                                   
                                   # Example: Weather data collection
                                   cities <- self$config$get("collection_params")$cities
                                   batch_data <- data.frame()
                                   
                                   for (city in cities) {
                                     city_data <- self$collect_weather_data(city)
                                     if (!is.null(city_data)) {
                                       batch_data <- rbind(batch_data, city_data)
                                     }
                                   }
                                   
                                   return(batch_data)
                                 },
                                 
                                 collect_weather_data = function(city) {
                                   api_key <- self$config$get("apis")$weather$key
                                   base_url <- self$config$get("apis")$weather$base_url
                                   
                                   if (api_key == "YOUR_API_KEY_HERE") {
                                     log_warn("API key not configured, using mock data")
                                     return(self$generate_mock_weather_data(city))
                                   }
                                   
                                   url <- paste0(base_url, "?q=", URLencode(city), "&appid=", api_key, "&units=metric")
                                   
                                   self$collection_stats$total_requests <- self$collection_stats$total_requests + 1
                                   
                                   tryCatch({
                                     response <- GET(url)
                                     
                                     if (status_code(response) == 200) {
                                       content <- content(response, "parsed", encoding = "UTF-8")
                                       
                                       weather_data <- data.frame(
                                         city = city,
                                         temperature = content$main$temp,
                                         humidity = content$main$humidity,
                                         pressure = content$main$pressure,
                                         description = content$weather[[1]]$description,
                                         timestamp = Sys.time(),
                                         api_source = "openweathermap",
                                         stringsAsFactors = FALSE
                                       )
                                       
                                       self$collection_stats$successful_requests <- self$collection_stats$successful_requests + 1
                                       log_info("Successfully collected weather data for {city}")
                                       
                                       return(weather_data)
                                       
                                     } else {
                                       self$collection_stats$failed_requests <- self$collection_stats$failed_requests + 1
                                       log_error("API request failed for {city}: Status {status_code(response)}")
                                       return(NULL)
                                     }
                                     
                                   }, error = function(e) {
                                     self$collection_stats$failed_requests <- self$collection_stats$failed_requests + 1
                                     log_error("Error collecting data for {city}: {e$message}")
                                     return(NULL)
                                   })
                                 },
                                 
                                 generate_mock_weather_data = function(city) {
                                   # Generate realistic mock weather data for testing
                                   data.frame(
                                     city = city,
                                     temperature = round(runif(1, -10, 35), 1),
                                     humidity = round(runif(1, 30, 90)),
                                     pressure = round(runif(1, 980, 1030)),
                                     description = sample(c("clear sky", "few clouds", "scattered clouds", "broken clouds", "shower rain", "rain"), 1),
                                     timestamp = Sys.time(),
                                     api_source = "mock_data",
                                     stringsAsFactors = FALSE
                                   )
                                 },
                                 
                                 process_and_store = function(data) {
                                   # Data validation
                                   validated_data <- self$validate_data(data)
                                   
                                   if (!is.null(validated_data) && nrow(validated_data) > 0) {
                                     # Add processing metadata
                                     validated_data$processed_at <- Sys.time()
                                     validated_data$quality_score <- self$calculate_record_quality(validated_data)
                                     
                                     # Store in main data store
                                     self$data_store <- rbind(self$data_store, validated_data)
                                     
                                     log_info("Processed and stored {nrow(validated_data)} records")
                                   }
                                 },
                                 
                                 validate_data = function(data) {
                                   if (is.null(data) || nrow(data) == 0) {
                                     return(NULL)
                                   }
                                   
                                   # Remove records with missing essential fields
                                   required_fields <- c("city", "temperature", "timestamp")
                                   complete_records <- complete.cases(data[required_fields])
                                   
                                   if (sum(complete_records) == 0) {
                                     log_warn("No complete records found in batch")
                                     return(NULL)
                                   }
                                   
                                   validated_data <- data[complete_records, ]
                                   
                                   # Additional validation rules
                                   validated_data <- validated_data %>%
                                     filter(
                                       temperature >= -50 & temperature <= 60,  # Reasonable temperature range
                                       humidity >= 0 & humidity <= 100,         # Valid humidity range
                                       !is.na(city)
                                     )
                                   
                                   log_info("Validated {nrow(validated_data)} out of {nrow(data)} records")
                                   
                                   return(validated_data)
                                 },
                                 
                                 calculate_record_quality = function(data) {
                                   # Calculate quality score based on completeness and validity
                                   if (nrow(data) == 0) return(0)
                                   
                                   quality_metrics <- list()
                                   
                                   # Completeness score
                                   complete_fields <- rowSums(!is.na(data)) / ncol(data)
                                   quality_metrics$completeness <- mean(complete_fields)
                                   
                                   # Validity score (example: reasonable temperature values)
                                   valid_temp <- sum(data$temperature >= -50 & data$temperature <= 60, na.rm = TRUE) / nrow(data)
                                   quality_metrics$validity <- valid_temp
                                   
                                   # Timeliness score (data should be recent)
                                   time_diff <- as.numeric(difftime(Sys.time(), max(data$timestamp, na.rm = TRUE), units = "hours"))
                                   timeliness <- max(0, 1 - (time_diff / 24))  # Score decreases after 24 hours
                                   quality_metrics$timeliness <- timeliness
                                   
                                   # Overall quality score
                                   overall_quality <- mean(unlist(quality_metrics))
                                   
                                   return(overall_quality)
                                 },
                                 
                                 assess_performance = function() {
                                   success_rate <- self$get_success_rate()
                                   quality_score <- self$get_current_quality_score()
                                   
                                   # Store quality score for tracking
                                   self$collection_stats$quality_scores <- c(self$collection_stats$quality_scores, quality_score)
                                   
                                   log_info("Current performance - Success rate: {round(success_rate, 3)}, Quality score: {round(quality_score, 3)}")
                                   
                                   # Adapt strategy based on performance
                                   if (success_rate < 0.7) {
                                     self$adjust_strategy("poor_success")
                                   } else if (quality_score < self$config$get("quality_threshold")) {
                                     self$adjust_strategy("poor_quality")
                                   } else if (success_rate > 0.9 && quality_score > 0.8) {
                                     self$adjust_strategy("excellent")
                                   }
                                 },
                                 
                                 adjust_strategy = function(reason) {
                                   log_info("Adjusting collection strategy due to: {reason}")
                                   
                                   switch(reason,
                                          "poor_success" = {
                                            self$delay_multiplier <- self$delay_multiplier * 1.5
                                            log_info("Increased delay multiplier to {self$delay_multiplier}")
                                          },
                                          "poor_quality" = {
                                            log_info("Implementing additional quality checks")
                                            # Could add more validation rules or switch to different APIs
                                          },
                                          "excellent" = {
                                            self$delay_multiplier <- max(0.5, self$delay_multiplier * 0.9)
                                            log_info("Decreased delay multiplier to {self$delay_multiplier}")
                                          }
                                   )
                                 },
                                 
                                 get_success_rate = function() {
                                   total <- self$collection_stats$total_requests
                                   if (total == 0) return(1)
                                   return(self$collection_stats$successful_requests / total)
                                 },
                                 
                                 get_current_quality_score = function() {
                                   if (nrow(self$data_store) == 0) return(0)
                                   return(mean(self$data_store$quality_score, na.rm = TRUE))
                                 },
                                 
                                 collection_complete = function() {
                                   max_records <- self$config$get("collection_params")$max_records
                                   current_records <- nrow(self$data_store)
                                   
                                   complete <- current_records >= max_records
                                   
                                   if (complete) {
                                     log_info("Collection target reached: {current_records}/{max_records} records")
                                   }
                                   
                                   return(complete)
                                 },
                                 
                                 respectful_delay = function() {
                                   base_delay <- self$config$get("base_delay")
                                   delay <- base_delay * self$delay_multiplier
                                   
                                   # Add jitter to avoid synchronized requests
                                   jitter <- runif(1, 0.8, 1.2)
                                   final_delay <- delay * jitter
                                   
                                   log_debug("Sleeping for {round(final_delay, 2)} seconds")
                                   Sys.sleep(final_delay)
                                 },
                                 
                                 generate_final_report = function() {
                                   log_info("Generating final collection report")
                                   
                                   # Create comprehensive report
                                   report <- list(
                                     collection_summary = self$generate_collection_summary(),
                                     data_quality_analysis = self$analyze_data_quality(),
                                     performance_metrics = self$calculate_performance_metrics(),
                                     recommendations = self$generate_recommendations()
                                   )
                                   
                                   # Save report as JSON
                                   write_json(report, "collection_report.json", pretty = TRUE)
                                   
                                   # Generate human-readable report
                                   self$create_readable_report(report)
                                   
                                   # Save collected data
                                   if (nrow(self$data_store) > 0) {
                                     write_csv(self$data_store, "collected_data.csv")
                                     log_info("Saved {nrow(self$data_store)} records to collected_data.csv")
                                   }
                                   
                                   log_info("Final report generated successfully")
                                 },
                                 
                                 generate_collection_summary = function() {
                                   end_time <- Sys.time()
                                   duration <- as.numeric(difftime(end_time, self$collection_stats$start_time, units = "mins"))
                                   
                                   list(
                                     start_time = self$collection_stats$start_time,
                                     end_time = end_time,
                                     duration_minutes = round(duration, 2),
                                     total_records_collected = nrow(self$data_store),
                                     total_api_requests = self$collection_stats$total_requests,
                                     successful_requests = self$collection_stats$successful_requests,
                                     failed_requests = self$collection_stats$failed_requests,
                                     success_rate = self$get_success_rate(),
                                     average_quality_score = self$get_current_quality_score()
                                   )
                                 },
                                 
                                 analyze_data_quality = function() {
                                   if (nrow(self$data_store) == 0) {
                                     return(list(message = "No data collected"))
                                   }
                                   
                                   analysis <- list(
                                     total_records = nrow(self$data_store),
                                     complete_records = sum(complete.cases(self$data_store)),
                                     missing_data_by_column = colSums(is.na(self$data_store)),
                                     quality_score_distribution = summary(self$data_store$quality_score),
                                     data_sources = table(self$data_store$api_source),
                                     temporal_coverage = list(
                                       earliest_record = min(self$data_store$timestamp, na.rm = TRUE),
                                       latest_record = max(self$data_store$timestamp, na.rm = TRUE)
                                     )
                                   )
                                   
                                   return(analysis)
                                 },
                                 
                                 calculate_performance_metrics = function() {
                                   list(
                                     overall_success_rate = self$get_success_rate(),
                                     quality_trend = if(length(self$collection_stats$quality_scores) > 1) {
                                       cor(seq_along(self$collection_stats$quality_scores), self$collection_stats$quality_scores)
                                     } else { NA },
                                     average_delay_multiplier = self$delay_multiplier,
                                     apis_used = unique(self$collection_stats$apis_used)
                                   )
                                 },
                                 
                                 generate_recommendations = function() {
                                   recommendations <- character()
                                   
                                   success_rate <- self$get_success_rate()
                                   quality_score <- self$get_current_quality_score()
                                   
                                   if (success_rate < 0.8) {
                                     recommendations <- c(recommendations, "Consider implementing retry logic or using alternative APIs")
                                   }
                                   
                                   if (quality_score < 0.7) {
                                     recommendations <- c(recommendations, "Implement additional data validation and cleaning procedures")
                                   }
                                   
                                   if (nrow(self$data_store) < self$config$get("collection_params")$max_records) {
                                     recommendations <- c(recommendations, "Collection target not reached - consider extending collection period")
                                   }
                                   
                                   if (length(recommendations) == 0) {
                                     recommendations <- c("Data collection performed well - maintain current practices")
                                   }
                                   
                                   return(recommendations)
                                 },
                                 
                                 create_readable_report = function(report) {
                                   # Create a formatted text report
                                   report_text <- paste0(
                                     "DATA COLLECTION AGENT - FINAL REPORT\n",
                                     "=====================================\n\n",
                                     "COLLECTION SUMMARY:\n",
                                     "- Duration: ", report$collection_summary$duration_minutes, " minutes\n",
                                     "- Total Records: ", report$collection_summary$total_records_collected, "\n",
                                     "- Success Rate: ", round(report$collection_summary$success_rate * 100, 1), "%\n",
                                     "- Average Quality Score: ", round(report$collection_summary$average_quality_score, 3), "\n\n",
                                     "DATA QUALITY:\n",
                                     "- Complete Records: ", report$data_quality_analysis$complete_records, "\n",
                                     "- Data Sources Used: ", paste(names(report$data_quality_analysis$data_sources), collapse = ", "), "\n\n",
                                     "RECOMMENDATIONS:\n",
                                     paste("- ", report$recommendations, collapse = "\n"), "\n"
                                   )
                                   
                                   writeLines(report_text, "collection_report.txt")
                                 }
                               )
)

# =============================================================================
# SPECIFIC SCENARIO IMPLEMENTATIONS
# =============================================================================

# Weather Data Agent (Scenario A)
WeatherAgent <- R6Class("WeatherAgent", inherit = DataCollectionAgent,
                        public = list(
                          initialize = function(config_file = "weather_config.json") {
                            super$initialize(config_file)
                            
                            # Weather-specific configuration
                            if (is.null(self$config$get("apis")$weather)) {
                              weather_config <- list(
                                apis = list(
                                  weather = list(
                                    openweather = list(
                                      base_url = "http://api.openweathermap.org/data/2.5/weather",
                                      key = "YOUR_OPENWEATHER_API_KEY",
                                      rate_limit = 60
                                    ),
                                    weatherapi = list(
                                      base_url = "http://api.weatherapi.com/v1/current.json",
                                      key = "YOUR_WEATHERAPI_KEY",
                                      rate_limit = 100
                                    )
                                  )
                                ),
                                collection_params = list(
                                  cities = c("New York", "London", "Tokyo", "Sydney", "Toronto", "Paris", "Berlin", "Mumbai", "São Paulo", "Cairo"),
                                  max_records = 50,
                                  collect_forecast = TRUE
                                )
                              )
                              
                              # Update configuration
                              self$config$config <- c(self$config$config, weather_config)
                              self$config$save_config("weather_config.json")
                            }
                          }
                        )
)

# News Sentiment Agent (Scenario B)
NewsAgent <- R6Class("NewsAgent", inherit = DataCollectionAgent,
                     public = list(
                       initialize = function(config_file = "news_config.json", topic = "climate change") {
                         super$initialize(config_file)
                         
                         self$topic <- topic
                         
                         # News-specific configuration
                         if (is.null(self$config$get("apis")$news)) {
                           news_config <- list(
                             apis = list(
                               news = list(
                                 newsapi = list(
                                   base_url = "https://newsapi.org/v2/everything",
                                   key = "YOUR_NEWSAPI_KEY",
                                   rate_limit = 1000
                                 )
                               )
                             ),
                             collection_params = list(
                               topic = topic,
                               language = "en",
                               max_records = 100,
                               days_back = 7
                             )
                           )
                           
                           self$config$config <- c(self$config$config, news_config)
                           self$config$save_config("news_config.json")
                         }
                       },
                       
                       collect_batch = function() {
                         log_info("Collecting news articles about: {self$topic}")
                         
                         # Mock news data for demonstration
                         return(self$generate_mock_news_data())
                       },
                       
                       generate_mock_news_data = function() {
                         headlines <- c(
                           "Climate Change Summit Reaches New Agreement",
                           "Renewable Energy Investment Hits Record High",
                           "Scientists Warn of Accelerating Global Warming",
                           "New Technology Could Reduce Carbon Emissions",
                           "Government Announces Climate Action Plan"
                         )
                         
                         sentiments <- c("positive", "positive", "negative", "positive", "neutral")
                         
                         data.frame(
                           headline = sample(headlines, 5, replace = TRUE),
                           sentiment = sample(sentiments, 5, replace = TRUE),
                           source = sample(c("Reuters", "BBC", "CNN", "AP News", "Guardian"), 5, replace = TRUE),
                           published_at = Sys.time() - runif(5, 0, 7*24*3600), # Random time in past week
                           topic = self$topic,
                           timestamp = Sys.time(),
                           api_source = "mock_news",
                           stringsAsFactors = FALSE
                         )
                       }
                     )
)

# GitHub Repository Agent (Scenario C)
GitHubAgent <- R6Class("GitHubAgent", inherit = DataCollectionAgent,
                       public = list(
                         initialize = function(config_file = "github_config.json", language = "python") {
                           super$initialize(config_file)
                           
                           self$language <- language
                           
                           # GitHub-specific configuration
                           if (is.null(self$config$get("apis")$github)) {
                             github_config <- list(
                               apis = list(
                                 github = list(
                                   base_url = "https://api.github.com",
                                   token = "YOUR_GITHUB_TOKEN",
                                   rate_limit = 5000
                                 )
                               ),
                               collection_params = list(
                                 language = language,
                                 min_stars = 100,
                                 max_records = 30,
                                 sort_by = "stars"
                               )
                             )
                             
                             self$config$config <- c(self$config$config, github_config)
                             self$config$save_config("github_config.json")
                           }
                         },
                         
                         collect_batch = function() {
                           log_info("Collecting GitHub repositories for language: {self$language}")
                           
                           # Mock GitHub data for demonstration
                           return(self$generate_mock_github_data())
                         },
                         
                         generate_mock_github_data = function() {
                           repo_names <- paste0(self$language, "-", c("framework", "library", "toolkit", "api", "cli"))
                           
                           data.frame(
                             repo_name = repo_names,
                             language = self$language,
                             stars = round(runif(5, 100, 10000)),
                             forks = round(runif(5, 50, 2000)),
                             open_issues = round(runif(5, 0, 500)),
                             created_at = Sys.time() - runif(5, 30, 1000) * 24 * 3600, # Random creation date
                             updated_at = Sys.time() - runif(5, 0, 30) * 24 * 3600,    # Random update date
                             timestamp = Sys.time(),
                             api_source = "mock_github",
                             stringsAsFactors = FALSE
                           )
                         }
                       )
)

# =============================================================================
# DOCUMENTATION AND METADATA GENERATION
# =============================================================================

# Metadata Generator
MetadataGenerator <- R6Class("MetadataGenerator",
                             public = list(
                               generate_metadata = function(data, collection_info) {
                                 metadata <- list(
                                   collection_info = list(
                                     collection_date = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                                     collector = collection_info$collector_name,
                                     agent_version = "1.0",
                                     total_records = nrow(data)
                                   ),
                                   data_description = list(
                                     variables = self$generate_variable_descriptions(data),
                                     data_types = sapply(data, class),
                                     missing_values = colSums(is.na(data))
                                   ),
                                   quality_metrics = list(
                                     completeness = sum(complete.cases(data)) / nrow(data),
                                     data_sources = if("api_source" %in% names(data)) table(data$api_source) else NULL
                                   ),
                                   technical_info = list(
                                     file_format = "CSV",
                                     encoding = "UTF-8",
                                     separator = ",",
                                     creation_timestamp = Sys.time()
                                   )
                                 )
                                 
                                 return(metadata)
                               },
                               
                               generate_variable_descriptions = function(data) {
                                 # Generate basic variable descriptions
                                 variables <- list()
                                 
                                 for (col_name in names(data)) {
                                   col_data <- data[[col_name]]
                                   
                                   desc <- list(
                                     name = col_name,
                                     type = class(col_data)[1],
                                     description = self$infer_description(col_name, col_data),
                                     missing_count = sum(is.na(col_data)),
                                     unique_values = if(is.character(col_data) || is.factor(col_data)) length(unique(col_data)) else NULL
                                   )
                                   
                                   # Add range for numeric variables
                                   if (is.numeric(col_data)) {
                                     desc$min_value <- min(col_data, na.rm = TRUE)
                                     desc$max_value <- max(col_data, na.rm = TRUE)
                                     desc$mean_value <- mean(col_data, na.rm = TRUE)
                                   }
                                   
                                   variables[[col_name]] <- desc
                                 }
                                 
                                 return(variables)
                               },
                               
                               infer_description = function(col_name, col_data) {
                                 # Basic description inference based on column name and data
                                 descriptions <- list(
                                   "city" = "City name where data was collected",
                                   "temperature" = "Temperature measurement in degrees Celsius",
                                   "humidity" = "Relative humidity percentage",
                                   "pressure" = "Atmospheric pressure in hPa",
                                   "description" = "Weather condition description",
                                   "timestamp" = "Date and time of data collection",
                                   "api_source" = "Source API used for data collection",
                                   "quality_score" = "Data quality assessment score (0-1)",
                                   "headline" = "News article headline",
                                   "sentiment" = "Sentiment classification of the content",
                                   "source" = "Publication or data source",
                                   "repo_name" = "GitHub repository name",
                                   "stars" = "Number of GitHub stars",
                                   "forks" = "Number of repository forks"
                                 )
                                 
                                 return(descriptions[[col_name]] %||% paste("Data field:", col_name))
                               },
                               
                               save_metadata = function(metadata, filename = "dataset_metadata.json") {
                                 write_json(metadata, filename, pretty = TRUE, auto_unbox = TRUE)
                                 message(paste("Metadata saved to", filename))
                               }
                             )
)

# =============================================================================
# QUALITY ASSURANCE AND VALIDATION
# =============================================================================

# Data Quality Assessor
QualityAssessor <- R6Class("QualityAssessor",
                           public = list(
                             assess_data_quality = function(data) {
                               if (nrow(data) == 0) {
                                 return(list(
                                   overall_score = 0,
                                   message = "No data to assess"
                                 ))
                               }
                               
                               metrics <- list(
                                 completeness = self$assess_completeness(data),
                                 consistency = self$assess_consistency(data),
                                 validity = self$assess_validity(data),
                                 timeliness = self$assess_timeliness(data)
                               )
                               
                               overall_score <- mean(unlist(metrics))
                               
                               return(list(
                                 overall_score = overall_score,
                                 detailed_metrics = metrics,
                                 recommendations = self$generate_quality_recommendations(metrics)
                               ))
                             },
                             
                             assess_completeness = function(data) {
                               # Calculate completeness as percentage of non-missing values
                               total_cells <- nrow(data) * ncol(data)
                               non_missing_cells <- sum(!is.na(data))
                               completeness <- non_missing_cells / total_cells
                               
                               return(completeness)
                             },
                             
                             assess_consistency = function(data) {
                               # Check for data consistency issues
                               consistency_score <- 1.0
                               
                               # Check for duplicate records
                               if (nrow(data) > 0) {
                                 duplicate_rate <- sum(duplicated(data)) / nrow(data)
                                 consistency_score <- consistency_score * (1 - duplicate_rate)
                               }
                               
                               # Check for consistent data types in character columns
                               char_cols <- sapply(data, is.character)
                               if (any(char_cols)) {
                                 for (col in names(data)[char_cols]) {
                                   # Check for mixed case issues or extra whitespace
                                   clean_values <- str_trim(str_to_lower(data[[col]]))
                                   original_unique <- length(unique(data[[col]][!is.na(data[[col]])]))
                                   clean_unique <- length(unique(clean_values[!is.na(clean_values)]))
                                   
                                   if (original_unique > clean_unique && original_unique > 0) {
                                     consistency_penalty <- 1 - (original_unique - clean_unique) / original_unique
                                     consistency_score <- consistency_score * consistency_penalty
                                   }
                                 }
                               }
                               
                               return(max(0, consistency_score))
                             },
                             
                             assess_validity = function(data) {
                               # Check for valid data ranges and formats
                               validity_scores <- numeric()
                               
                               # Temperature validity (if present)
                               if ("temperature" %in% names(data)) {
                                 temp_valid <- sum(data$temperature >= -50 & data$temperature <= 60, na.rm = TRUE)
                                 temp_total <- sum(!is.na(data$temperature))
                                 if (temp_total > 0) {
                                   validity_scores <- c(validity_scores, temp_valid / temp_total)
                                 }
                               }
                               
                               # Humidity validity (if present)
                               if ("humidity" %in% names(data)) {
                                 humid_valid <- sum(data$humidity >= 0 & data$humidity <= 100, na.rm = TRUE)
                                 humid_total <- sum(!is.na(data$humidity))
                                 if (humid_total > 0) {
                                   validity_scores <- c(validity_scores, humid_valid / humid_total)
                                 }
                               }
                               
                               # GitHub stars validity (if present)
                               if ("stars" %in% names(data)) {
                                 stars_valid <- sum(data$stars >= 0, na.rm = TRUE)
                                 stars_total <- sum(!is.na(data$stars))
                                 if (stars_total > 0) {
                                   validity_scores <- c(validity_scores, stars_valid / stars_total)
                                 }
                               }
                               
                               # Return average validity if we have metrics, otherwise return 1
                               if (length(validity_scores) > 0) {
                                 return(mean(validity_scores))
                               } else {
                                 return(1.0)
                               }
                             },
                             
                             assess_timeliness = function(data) {
                               # Assess data timeliness based on timestamp
                               if (!"timestamp" %in% names(data)) {
                                 return(1.0) # No timestamp to assess
                               }
                               
                               current_time <- Sys.time()
                               data_times <- as.POSIXct(data$timestamp)
                               
                               # Calculate average age of data in hours
                               time_diffs <- as.numeric(difftime(current_time, data_times, units = "hours"))
                               avg_age_hours <- mean(time_diffs, na.rm = TRUE)
                               
                               # Score decreases as data gets older (penalize data older than 24 hours)
                               timeliness_score <- max(0, 1 - (avg_age_hours / 24))
                               
                               return(timeliness_score)
                             },
                             
                             generate_quality_recommendations = function(metrics) {
                               recommendations <- character()
                               
                               if (metrics$completeness < 0.8) {
                                 recommendations <- c(recommendations, "Data completeness is below 80% - investigate missing data sources")
                               }
                               
                               if (metrics$consistency < 0.9) {
                                 recommendations <- c(recommendations, "Data consistency issues detected - implement data standardization")
                               }
                               
                               if (metrics$validity < 0.95) {
                                 recommendations <- c(recommendations, "Some invalid data values detected - strengthen validation rules")
                               }
                               
                               if (metrics$timeliness < 0.7) {
                                 recommendations <- c(recommendations, "Data may be outdated - consider more frequent collection")
                               }
                               
                               if (length(recommendations) == 0) {
                                 recommendations <- c("Data quality is good - maintain current practices")
                               }
                               
                               return(recommendations)
                             },
                             
                             generate_quality_report = function(data, output_file = "quality_report.html") {
                               quality_assessment <- self$assess_data_quality(data)
                               
                               # Create visualizations
                               plots <- self$create_quality_plots(data)
                               
                               # Generate HTML report
                               html_content <- paste0(
                                 "<html><head><title>Data Quality Report</title>",
                                 "<style>body{font-family:Arial,sans-serif;margin:40px;}",
                                 ".metric{background:#f5f5f5;padding:15px;margin:10px 0;border-radius:5px;}",
                                 ".score{font-size:24px;font-weight:bold;color:",
                                 if(quality_assessment$overall_score > 0.8) "#28a745" else if(quality_assessment$overall_score > 0.6) "#ffc107" else "#dc3545",
                                 ";}</style></head><body>",
                                 "<h1>Data Quality Assessment Report</h1>",
                                 "<div class='metric'><h2>Overall Quality Score</h2>",
                                 "<div class='score'>", round(quality_assessment$overall_score * 100, 1), "%</div></div>",
                                 "<div class='metric'><h2>Detailed Metrics</h2>",
                                 "<ul>",
                                 "<li>Completeness: ", round(quality_assessment$detailed_metrics$completeness * 100, 1), "%</li>",
                                 "<li>Consistency: ", round(quality_assessment$detailed_metrics$consistency * 100, 1), "%</li>",
                                 "<li>Validity: ", round(quality_assessment$detailed_metrics$validity * 100, 1), "%</li>",
                                 "<li>Timeliness: ", round(quality_assessment$detailed_metrics$timeliness * 100, 1), "%</li>",
                                 "</ul></div>",
                                 "<div class='metric'><h2>Recommendations</h2><ul>",
                                 paste("<li>", quality_assessment$recommendations, "</li>", collapse = ""),
                                 "</ul></div>",
                                 "<div class='metric'><h2>Data Summary</h2>",
                                 "<p>Total Records: ", nrow(data), "</p>",
                                 "<p>Total Variables: ", ncol(data), "</p>",
                                 "<p>Report Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "</p>",
                                 "</div></body></html>"
                               )
                               
                               writeLines(html_content, output_file)
                               message(paste("Quality report saved to", output_file))
                               
                               return(quality_assessment)
                             },
                             
                             create_quality_plots = function(data) {
                               plots <- list()
                               
                               # Missing data visualization
                               if (nrow(data) > 0) {
                                 missing_data <- data %>%
                                   summarise_all(~sum(is.na(.))) %>%
                                   gather(key = "variable", value = "missing_count") %>%
                                   mutate(missing_percentage = missing_count / nrow(data) * 100)
                                 
                                 p1 <- ggplot(missing_data, aes(x = reorder(variable, missing_percentage), y = missing_percentage)) +
                                   geom_bar(stat = "identity", fill = "steelblue") +
                                   coord_flip() +
                                   labs(title = "Missing Data by Variable", x = "Variables", y = "Missing Percentage") +
                                   theme_minimal()
                                 
                                 ggsave("missing_data_plot.png", p1, width = 10, height = 6)
                                 plots$missing_data <- p1
                               }
                               
                               return(plots)
                             }
                           )
)

# =============================================================================
# EXAMPLE USAGE AND MAIN EXECUTION
# =============================================================================

# Function to run a complete example
run_example <- function(scenario = "weather") {
  cat("Starting AI Data Collection Agent Example\n")
  cat("========================================\n\n")
  
  # Initialize the appropriate agent based on scenario
  agent <- switch(scenario,
                  "weather" = WeatherAgent$new(),
                  "news" = NewsAgent$new(),
                  "github" = GitHubAgent$new(),
                  DataCollectionAgent$new() # Default
  )
  
  cat("Agent initialized for scenario:", scenario, "\n")
  
  # Run the collection process
  agent$run_collection()
  
  # Generate additional documentation
  if (nrow(agent$data_store) > 0) {
    # Generate metadata
    metadata_gen <- MetadataGenerator$new()
    metadata <- metadata_gen$generate_metadata(
      agent$data_store, 
      list(collector_name = agent$config$get("collector_name"))
    )
    metadata_gen$save_metadata(metadata)
    
    # Generate quality assessment
    quality_assessor <- QualityAssessor$new()
    quality_report <- quality_assessor$generate_quality_report(agent$data_store)
    
    cat("\nCollection completed successfully!\n")
    cat("Data saved to: collected_data.csv\n")
    cat("Metadata saved to: dataset_metadata.json\n")
    cat("Quality report saved to: quality_report.html\n")
    cat("Collection log saved to: data_collection.log\n")
    
    # Print summary statistics
    cat("\nCollection Summary:\n")
    cat("- Records collected:", nrow(agent$data_store), "\n")
    cat("- Success rate:", round(agent$get_success_rate() * 100, 1), "%\n")
    cat("- Average quality score:", round(agent$get_current_quality_score(), 3), "\n")
    
  } else {
    cat("\nNo data was collected. Check configuration and API keys.\n")
  }
}

# =============================================================================
# CONFIGURATION HELPERS
# =============================================================================

# Function to create example configuration files
create_example_configs <- function() {
  # Weather configuration
  weather_config <- list(
    project_name = "Weather Data Collection",
    collector_name = "Student Name",
    base_delay = 1.0,
    max_retries = 3,
    quality_threshold = 0.7,
    apis = list(
      weather = list(
        openweather = list(
          base_url = "http://api.openweathermap.org/data/2.5/weather",
          key = "YOUR_OPENWEATHER_API_KEY_HERE",
          rate_limit = 60
        )
      )
    ),
    collection_params = list(
      cities = c("New York", "London", "Tokyo", "Sydney", "Toronto"),
      max_records = 25,
      units = "metric"
    )
  )
  
  write_json(weather_config, "weather_config.json", pretty = TRUE)
  
  # News configuration
  news_config <- list(
    project_name = "News Sentiment Analysis",
    collector_name = "Student Name",
    base_delay = 1.0,
    max_retries = 3,
    quality_threshold = 0.7,
    apis = list(
      news = list(
        newsapi = list(
          base_url = "https://newsapi.org/v2/everything",
          key = "YOUR_NEWSAPI_KEY_HERE",
          rate_limit = 1000
        )
      )
    ),
    collection_params = list(
      topic = "climate change",
      language = "en",
      max_records = 50,
      days_back = 7
    )
  )
  
  write_json(news_config, "news_config.json", pretty = TRUE)
  
  # GitHub configuration
  github_config <- list(
    project_name = "GitHub Repository Analysis",
    collector_name = "Student Name",
    base_delay = 1.0,
    max_retries = 3,
    quality_threshold = 0.7,
    apis = list(
      github = list(
        base_url = "https://api.github.com",
        token = "YOUR_GITHUB_TOKEN_HERE",
        rate_limit = 5000
      )
    ),
    collection_params = list(
      language = "python",
      min_stars = 100,
      max_records = 30,
      sort_by = "stars"
    )
  )
  
  write_json(github_config, "github_config.json", pretty = TRUE)
  
  cat("Example configuration files created:\n")
  cat("- weather_config.json\n")
  cat("- news_config.json\n")
  cat("- github_config.json\n\n")
  cat("Please edit these files with your actual API keys before running the agents.\n")
}

# Function to test API connections
test_api_connections <- function() {
  cat("Testing API connections...\n")
  cat("========================\n\n")
  
  # Test free APIs that don't require authentication
  cat("1. Testing Cat Facts API (no auth required):\n")
  fact <- get_cat_fact()
  if (!is.null(fact)) {
    cat("✓ Success:", substr(fact, 1, 50), "...\n")
  } else {
    cat("✗ Failed\n")
  }
  
  cat("\n2. Testing Public Holidays API (no auth required):\n")
  holidays <- get_public_holidays("US", 2024)
  if (!is.null(holidays) && length(holidays) > 0) {
    cat("✓ Success: Found", length(holidays), "holidays for US in 2024\n")
  } else {
    cat("✗ Failed\n")
  }
  
  cat("\n3. Testing configuration loading:\n")
  tryCatch({
    config_manager <- ConfigManager$new("test_config.json")
    cat("✓ Configuration system working\n")
  }, error = function(e) {
    cat("✗ Configuration error:", e$message, "\n")
  })
  
  cat("\nBasic API functionality test completed.\n")
  cat("For full functionality, configure your API keys in the config files.\n")
}

# =============================================================================
# MAIN EXECUTION EXAMPLES
# =============================================================================

# Example: Run weather data collection
run_weather_example <- function() {
  cat("Running Weather Data Collection Example\n")
  cat("======================================\n")
  
  # Create configuration if it doesn't exist
  if (!file.exists("weather_config.json")) {
    create_example_configs()
  }
  
  # Run the weather agent
  run_example("weather")
}

# Example: Run news sentiment analysis
run_news_example <- function() {
  cat("Running News Sentiment Analysis Example\n")
  cat("=====================================\n")
  
  # Create configuration if it doesn't exist
  if (!file.exists("news_config.json")) {
    create_example_configs()
  }
  
  # Run the news agent
  run_example("news")
}

# Example: Run GitHub repository analysis
run_github_example <- function() {
  cat("Running GitHub Repository Analysis Example\n")
  cat("=========================================\n")
  
  # Create configuration if it doesn't exist
  if (!file.exists("github_config.json")) {
    create_example_configs()
  }
  
  # Run the GitHub agent
  run_example("github")
}

# =============================================================================
# UTILITY FUNCTIONS FOR STUDENTS
# =============================================================================

# Function to setup the project directory
setup_project <- function(project_name = "ai_data_collection") {
  # Create directory structure
  dirs_to_create <- c(
    project_name,
    file.path(project_name, "data", "raw"),
    file.path(project_name, "data", "processed"),
    file.path(project_name, "data", "final"),
    file.path(project_name, "logs"),
    file.path(project_name, "reports"),
    file.path(project_name, "config")
  )
  
  for (dir in dirs_to_create) {
    if (!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE)
      cat("Created directory:", dir, "\n")
    }
  }
  
  # Create example files
  readme_content <- paste0(
    "# AI Data Collection Agent\n\n",
    "This project contains an AI-powered data collection agent.\n\n",
    "## Setup\n",
    "1. Install required R packages: httr, jsonlite, dplyr, lubridate, etc.\n",
    "2. Edit configuration files in the config/ directory\n",
    "3. Add your API keys to the configuration files\n",
    "4. Run the agent using the provided examples\n\n",
    "## Files\n",
    "- `ai_agent.R`: Main agent implementation\n",
    "- `config/`: Configuration files\n",
    "- `data/`: Collected data storage\n",
    "- `logs/`: Collection logs\n",
    "- `reports/`: Generated reports\n\n",
    "## Usage\n",
    "```r\n",
    "source('ai_agent.R')\n",
    "run_weather_example()\n",
    "```\n"
  )
  
  writeLines(readme_content, file.path(project_name, "README.md"))
  
  cat("Project setup completed!\n")
  cat("Directory structure created in:", project_name, "\n")
}

# Function to validate student implementation
validate_implementation <- function(agent) {
  cat("Validating AI Data Collection Agent Implementation\n")
  cat("================================================\n")
  
  validation_results <- list()
  
  # Check if agent is properly initialized
  validation_results$initialization <- !is.null(agent$config) && !is.null(agent$data_store)
  
  # Check if collection methods exist
  validation_results$collection_method <- "collect_batch" %in% ls(agent)
  
  # Check if quality assessment exists
  validation_results$quality_assessment <- "assess_performance" %in% ls(agent)
  
  # Check if respectful delays are implemented
  validation_results$respectful_delays <- "respectful_delay" %in% ls(agent)
  
  # Check if documentation is generated
  validation_results$documentation <- "generate_final_report" %in% ls(agent)
  
  # Print results
  for (check in names(validation_results)) {
    status <- if (validation_results[[check]]) "✓ PASS" else "✗ FAIL"
    cat(sprintf("%-25s: %s\n", str_to_title(gsub("_", " ", check)), status))
  }
  
  overall_score <- sum(unlist(validation_results)) / length(validation_results) * 100
  cat("\nOverall Implementation Score:", round(overall_score, 1), "%\n")
  
  if (overall_score >= 80) {
    cat("🎉 Excellent work! Your implementation meets the requirements.\n")
  } else if (overall_score >= 60) {
    cat("👍 Good progress! A few areas need improvement.\n")
  } else {
    cat("⚠️  More work needed to meet the assignment requirements.\n")
  }
  
  return(validation_results)
}

# =============================================================================
# QUICK START EXAMPLE
# =============================================================================

# Quick start function for students to test everything
quick_start <- function() {
  cat("AI Data Collection Agent - Quick Start\n")
  cat("=====================================\n\n")
  
  # Setup project
  cat("1. Setting up project structure...\n")
  setup_project()
  
  cat("\n2. Creating example configurations...\n")
  create_example_configs()
  
  cat("\n3. Testing basic API functionality...\n")
  test_api_connections()
  
  cat("\n4. Running weather data collection example...\n")
  run_weather_example()
  
  cat("\n🎉 Quick start completed!\n")
  cat("Check the generated files:\n")
  cat("- collected_data.csv: Your collected data\n")
  cat("- dataset_metadata.json: Data documentation\n")
  cat("- quality_report.html: Quality assessment\n")
  cat("- data_collection.log: Collection logs\n")
}

# =============================================================================
# INSTRUCTIONS FOR STUDENTS
# =============================================================================

cat("
=============================================================================
AI DATA COLLECTION AGENT - R IMPLEMENTATION
=============================================================================

GETTING STARTED:
1. Run quick_start() to set up everything and see a working example
2. Edit the configuration files with your actual API keys
3. Choose your scenario and run the appropriate example function

AVAILABLE FUNCTIONS:
- quick_start()              : Complete setup and example run
- create_example_configs()   : Create configuration file templates
- test_api_connections()     : Test basic API functionality
- run_weather_example()      : Run weather data collection
- run_news_example()         : Run news sentiment analysis
- run_github_example()       : Run GitHub repository analysis
- setup_project()            : Create project directory structure

EXAMPLE USAGE:
> source('this_file.R')
> quick_start()

Or for a specific scenario:
> run_weather_example()

CUSTOMIZATION:
- Edit the config JSON files with your API keys
- Modify the agent classes to add new functionality
- Implement additional data sources or collection strategies

For questions or issues, refer to the assignment documentation.
=============================================================================
")

# Uncomment the line below to run the quick start automatically
# quick_start()