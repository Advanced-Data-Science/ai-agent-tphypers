#Thatch Phypers DS Pinnacle 9/28/25

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

