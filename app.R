library(shiny)
library(shinydashboard)
library(rhandsontable)
library(data.table)
library(dplyr)
library(lubridate)
library(shinyalert)
library(openxlsx)
library(DBI)
library(RPostgres)
library(pool)
library(rsconnect)
library(httr)  # Added for API calls
library(jsonlite)  # Added for JSON parsing

#For environment variables I have created "File1.Renviron", "File2.Renviron", "File1.env" and "File2.env" files.

db_pool <- NULL

# Function to get or create the connection pool
get_db_pool <- function() {
  if (is.null(db_pool)) {
    db_pool <<- dbPool(
      drv = RPostgres::Postgres(),
      dbname = Sys.getenv("NEON_DB_NAME"),
      host = Sys.getenv("NEON_HOST"),
      user = Sys.getenv("NEON_USER"),
      password = Sys.getenv("NEON_PASSWORD"),
      port = Sys.getenv("NEON_PORT", "5432"),
      sslmode = Sys.getenv("NEON_SSLMODE"),
      sslrootcert = system.file("certs/ca-certificates.crt", package = "RPostgres"),
      channel_binding = "require",
      minSize = 1,
      maxSize = 5,
      idleTimeout = 300000
    )
  }
  return(db_pool)
}

# Function to restart Neon endpoint using API
restart_neon_endpoint <- function() {
  tryCatch({
    # Get API token from environment variable
    api_token <- Sys.getenv("API_KEY")
    if (api_token == "") {
      return(list(success = FALSE, message = "API_KEY environment variable not set"))
    }
    
    # API endpoint URL
    url <- "https://console.neon.tech/api/v2/projects/round-surf-06007254/endpoints/ep-rapid-cake-adk7jxn4/restart"
    
    # Make POST request
    response <- POST(
      url,
      add_headers(
        "accept" = "application/json",
        "authorization" = paste("Bearer", api_token)
      )
    )
    
    # Check response
    if (status_code(response) == 200) {
      return(list(success = TRUE, message = "Neon endpoint restart initiated successfully"))
    } else {
      error_msg <- content(response, "text", encoding = "UTF-8")
      return(list(success = FALSE, message = paste("API request failed:", status_code(response), error_msg)))
    }
  }, error = function(e) {
    return(list(success = FALSE, message = paste("Error calling Neon API:", e$message)))
  })
}

# NEW FUNCTION: Get Neon projects using the provided cURL command
get_neon_projects <- function(limit = 10) {
  tryCatch({
    # Get API token from environment variable
    api_token <- Sys.getenv("API_KEY")
    if (api_token == "") {
      return(list(success = FALSE, message = "API_KEY environment variable not set"))
    }
    
    # API endpoint URL - using the exact URL from cURL
    url <- "https://console.neon.tech/api/v2/projects"
    
    # Make GET request with parameters matching the cURL command
    response <- GET(
      url,
      query = list(limit = limit),
      add_headers(
        "accept" = "application/json",
        "authorization" = paste("Bearer", api_token)
      )
    )
    
    # Check response
    if (status_code(response) == 200) {
      # Parse JSON response
      projects_data <- content(response, "parsed", encoding = "UTF-8")
      return(list(success = TRUE, data = projects_data))
    } else {
      error_msg <- content(response, "text", encoding = "UTF-8")
      return(list(success = FALSE, message = paste("API request failed:", status_code(response), error_msg)))
    }
  }, error = function(e) {
    return(list(success = FALSE, message = paste("Error calling Neon API:", e$message)))
  })
}

# Function to format projects data for display
format_projects_data <- function(projects_data) {
  if (!is.null(projects_data$projects) && length(projects_data$projects) > 0) {
    # Extract relevant information from each project
    projects_list <- lapply(projects_data$projects, function(proj) {
      data.table(
        ID = proj$id %||% NA_character_,
        Name = proj$name %||% NA_character_,
        Platform = proj$platform_id %||% NA_character_,
        Region = proj$region_id %||% NA_character_,
        Created = proj$created_at %||% NA_character_,
        Updated = proj$updated_at %||% NA_character_,
        Status = proj$status %||% NA_character_
      )
    })
    
    # Combine all projects into one data.table
    if (length(projects_list) > 0) {
      return(rbindlist(projects_list, fill = TRUE))
    }
  }
  return(data.table(Message = "No projects found"))
}

# Helper function for safe list access
`%||%` <- function(x, y) if (!is.null(x)) x else y

# Close pool on app stop
onStop(function() {
  if (!is.null(db_pool)) {
    poolClose(db_pool)
  }
})

# Rest of your existing code remains the same...
# УЧЕТ

# Глобальная функция для проверки дат операций
validate_operation_dates <- function(df, date_col = "Дата операции") {
  tryCatch({
    # Проверяем, что столбец с датами существует
    if (!date_col %in% names(df)) {
      stop(paste("Столбец", date_col, "не найден в данных"))
    }
    
    # Создаем копию dataframe для модификаций
    modified_df <- copy(df)
    
    # Инициализируем переменные для ошибок
    error_messages <- character(0)
    error_rows <- integer(0)
    
    # Преобразуем даты в Date формат (пропускаем NA)
    dates <- tryCatch({
      as.Date(modified_df[[date_col]], format = "%Y-%m-%d")
    }, error = function(e) {
      stop("Некорректный формат даты. Используйте формат ГГГГ-ММ-ДД")
    })
    
    # Условие 1: дата не должна быть в будущем
    future_dates <- which(!is.na(dates) & dates > Sys.Date())
    if (length(future_dates) > 0) {
      error_messages <- c(error_messages, 
                          paste("Ошибка в строке (строках) ", paste(future_dates, collapse = ", "), 
                                ": Дата операции не может быть в будущем"))
      error_rows <- union(error_rows, future_dates)
    }
    
    # Условие 2: даты должны идти в хронологическом порядке
    if (nrow(modified_df) > 1) {
      # Создаем вектор с индексами всех не-NA дат
      non_na_indices <- which(!is.na(dates))
      
      if (length(non_na_indices) > 1) {
        # Проверяем порядок только для не-NA дат
        for (i in 2:length(non_na_indices)) {
          current_idx <- non_na_indices[i]
          prev_idx <- non_na_indices[i-1]
          
          if (dates[current_idx] < dates[prev_idx]) {
            error_messages <- c(error_messages, 
                                paste("Ошибка в строке (строках)", current_idx, 
                                      ": Дата операции не может быть раньше предыдущей непустой даты в строке", prev_idx))
            error_rows <- union(error_rows, current_idx)
          }
        }
      }
    }
    
    # Если есть ошибки
    if (length(error_messages) > 0) {
      # Очищаем ошибочные даты
      modified_df[error_rows, (date_col) := NA_character_]
      
      # Формируем итоговое сообщение
      final_message <- paste(error_messages, collapse = "\n\n")
      shinyalert("Ошибка в дате операции", final_message, type = "error")
      
      # Возвращаем исправленный dataframe и FALSE
      return(list(valid = FALSE, df = modified_df))
    }
    
    return(list(valid = TRUE, df = modified_df))
  }, error = function(e) {
    shinyalert("Ошибка в дате операции", e$message, type = "error")
    return(list(valid = FALSE, df = df))
  })
}

#***************************************

#ОСВ: 6110

DF6110 <- data.table(
  "Счет (субчет)" = as.character(c("6110.1.Доходы по финансовым активам, отражаемый в составе прибыли и убытка",
                                   "6110.2.Доходы по финансовым активам, отражаемый в  Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6110.1	Доходы по финансовым активам, отражаемые в составе прибыли и убытка

DF6110.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
  "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6110.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
  "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6110.2	Доходы по финансовым активам, отражаемые в Прочем совокупном доходе

DF6110.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
  "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6110.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Корреспонденция счетов: Счет № (дебет)" = as.character(NA),
  "Корреспонденция счетов: Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

ui <- fluidPage(
  dashboardPage(
    dashboardHeader(title = "МСФО"),
    dashboardSidebar(width = 1050,
                     sidebarMenu(
                       menuItem("Home", tabName = "home"),
                       menuItem("Учет", tabName = "Учет", 
                                menuItem("Доходы", tabName = "Profit", 
                                         menuItem("6000.Доход от реализации продукции и оказания услуг", tabName = "Prft6000"),
                                         menuItem("6100.Доход от финансирования", tabName = "Prft6100",
                                                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6100"),
                                                  menuItem("6110.Доходы по финансовым активам", tabName = "Prft6110",
                                                           menuSubItem("Оборотно-сальдовая ведомость", tabName = "table6110"),
                                                           menuSubItem("6110.1.Доходы по финансовым активам, отражаемые в составе прибыли и убытка", tabName = "table6110_1"),
                                                           menuSubItem("6110.2.Доходы по финансовым активам, отражаемые в Прочем совокупном доходе", tabName = "table6110_2"))
                                         )
                                )
                       ),
                       # Add System menu for Neon management
                       menuItem("System", tabName = "system",
                                menuItem("Database Management", tabName = "db_management"),
                                menuItem("Neon Projects", tabName = "neon_projects")  # NEW: Added Neon Projects tab
                       )
                     )
    ),
    
    dashboardBody(
      tags$style(
        '
        @media (min-width: 768px){
          .sidebar-mini.sidebar-collapse .main-header .logo {
              width: 230px; 
          }
          .sidebar-mini.sidebar-collapse .main-header .navbar {
              margin-left: 230px;
          }
        }
      '),
      tabItems(
        tabItem(tabName = "home",
                h2("Welcome to the Home Page")
        ),
        tabItem(tabName = "table6110",
                fluidRow(
                  column(
                    width = 12, br(),
                    dateRangeInput("dates6110", "Выберите период ОСВ:",
                                   start = Sys.Date(), end = Sys.Date(), separator = "-"),
                    uiOutput("nested_ui6110")),
                  column(
                    width = 12, br(),
                    tags$b("ОСВ: 6110.Доходы по финансовым активам"),
                    tags$div(style = "margin-bottom: 20px;"),
                    rHandsontableOutput("table6110Item1"),
                    downloadButton("download_df6110", "Загрузить данные"))
                )
        ),
        tabItem(tabName = "table6110_1",
                fluidRow(
                  column(
                    width = 12, br(),
                    tags$b("Журнал учета хозопераций: 6110.1.Доходы по финансовым активам, отражаемые в составе прибыли и убытка"),
                    tags$div(style = "margin-bottom: 20px;"),
                    rHandsontableOutput("table6110.1Item1"),
                    downloadButton("download_df6110.1", "Загрузить данные")
                  ),
                  column(
                    width = 12, br(),
                    tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
                    tags$div(style = "margin-bottom: 20px;"),
                    selectInput("choices6110.1", label=NULL,
                                choices = c(	"Выбор по дате операции", 
                                             "Выбор по номеру первичного документа", 
                                             "Выбор по статье дохода", 
                                             "Выбор по дате операции и номеру первичного документа", 
                                             "Выбор по дате операции и статье дохода")),
                    uiOutput("nested_ui6110.1")
                  ),
                  column(
                    width = 12, br(),
                    label=NULL,
                    rHandsontableOutput("table6110.1Item2"),
                    downloadButton("download_df6110.1_2", "Загрузить данные"))
                )
        ),
        tabItem(tabName = "table6110_2",
                fluidRow(
                  column(
                    width = 12, br(),
                    tags$b("Журнал учета хозопераций: 6110.2.Доходы по финансовым активам, отражаемые в Прочем совокупном доходе"),
                    tags$div(style = "margin-bottom: 20px;"),
                    rHandsontableOutput("table6110.2Item1"),
                    downloadButton("download_df6110.2", "Загрузить данные")
                  ),
                  column(
                    width = 12, br(),
                    tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
                    tags$div(style = "margin-bottom: 20px;"),
                    selectInput("choices6110.2", label=NULL,
                                choices = c(	"Выбор по дате операции", 
                                             "Выбор по номеру первичного документа", 
                                             "Выбор по статье дохода", 
                                             "Выбор по дате операции и номеру первичного документа", 
                                             "Выбор по дате операции и статье дохода")),
                    uiOutput("nested_ui6110.2")
                  ),
                  column(
                    width = 12, br(),
                    label=NULL,
                    rHandsontableOutput("table6110.2Item2"),
                    downloadButton("download_df6110.2_2", "Загрузить данные"))
                )
        ),
        # Database Management tab
        tabItem(tabName = "db_management",
                fluidRow(
                  box(
                    title = "Neon Database Management",
                    status = "primary",
                    solidHeader = TRUE,
                    width = 12,
                    p("Use this section to manage your Neon database connection."),
                    actionButton("restart_neon", "Restart Neon Endpoint", 
                                 class = "btn-warning",
                                 icon = icon("refresh")),
                    br(), br(),
                    verbatimTextOutput("restart_status")
                  )
                )
        ),
        # NEW: Neon Projects tab
        tabItem(tabName = "neon_projects",
                fluidRow(
                  box(
                    title = "Neon Projects",
                    status = "info",
                    solidHeader = TRUE,
                    width = 12,
                    p("View and manage your Neon projects using the Neon API."),
                    
                    # Controls for fetching projects
                    fluidRow(
                      column(6,
                             numericInput("projects_limit", "Number of projects to fetch:", 
                                          value = 10, min = 1, max = 100, step = 1)
                      ),
                      column(6,
                             actionButton("fetch_projects", "Fetch Projects", 
                                          class = "btn-primary",
                                          icon = icon("cloud-download")),
                             br(), br()
                      )
                    ),
                    
                    # Projects display
                    rHandsontableOutput("projects_table"),
                    br(),
                    
                    # Status and download
                    verbatimTextOutput("projects_status"),
                    downloadButton("download_projects", "Download Projects as CSV")
                  )
                )
        )
      )
    )
  )
)

server = function(input, output, session) {
  
  r <- reactiveValues(
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date())
  )
  
  data <- reactiveValues()
  
  # NEW: Reactive value for projects data
  projects_data <- reactiveVal(data.table(Message = "No projects loaded"))
  
  # Neon DB query functions using pool
  
  db_data_df6110 <- reactive({
    req(input$some_trigger)  # Only connect when needed
    pool <- get_db_pool()
    on.exit(poolClose(pool))
    dbGetQuery(pool, "SELECT * FROM DF6110;")
  })
  
  db_data_df6110_1 <- reactive({
    req(input$some_trigger)  # Only connect when needed
    pool <- get_db_pool()
    on.exit(poolClose(pool))
    dbGetQuery(pool, "SELECT * FROM DF6110.1;")
  })
  
  db_data_df6110_2 <- reactive({
    req(input$some_trigger)  # Only connect when needed
    pool <- get_db_pool()
    on.exit(poolClose(pool))
    dbGetQuery(pool, "SELECT * FROM DF6110.2;")
  })
  
  db_data_df6110_1_2 <- reactive({
    req(input$some_trigger)  # Only connect when needed
    pool <- get_db_pool()
    on.exit(poolClose(pool))
    dbGetQuery(pool, "SELECT * FROM DF6110.1_2;")
  })
  
  db_data_df6110_2_2 <- reactive({
    req(input$some_trigger)  # Only connect when needed
    pool <- get_db_pool()
    on.exit(poolClose(pool))
    dbGetQuery(pool, "SELECT * FROM DF6110.2_2;")
  })
  
  observe({
    # Loading data from Neon DB for each table
    data$df6110 <- tryCatch({
      db_result <- db_data_df6110()
      if(!is.null(db_result) && nrow(db_result) > 0) {
        as.data.table(db_result)
      } else {
        as.data.table(DF6110)
      }
    }, error = function(e) {
      as.data.table(DF6110)
    })
    
    data$df6110_1 <- tryCatch({
      db_result <- db_data_df6110_1()
      if(!is.null(db_result) && nrow(db_result) > 0) {
        as.data.table(db_result)
      } else {
        as.data.table(DF6110.1)
      }
    }, error = function(e) {
      as.data.table(DF6110.1)
    })
    
    data$df6110_2 <- tryCatch({
      db_result <- db_data_df6110_2()
      if(!is.null(db_result) && nrow(db_result) > 0) {
        as.data.table(db_result)
      } else {
        as.data.table(DF6110.2)
      }
    }, error = function(e) {
      as.data.table(DF6110.2)
    })
    
    data$df6110_1_2 <- tryCatch({
      db_result <- db_data_df6110_1_2()
      if(!is.null(db_result) && nrow(db_result) > 0) {
        as.data.table(db_result)
      } else {
        as.data.table(DF6110.1_2)
      }
    }, error = function(e) {
      as.data.table(DF6110.1_2)
    })
    
    data$df6110_2_2 <- tryCatch({
      db_result <- db_data_df6110_2_2()
      if(!is.null(db_result) && nrow(db_result) > 0) {
        as.data.table(db_result)
      } else {
        as.data.table(DF6110.2_2)
      }
    }, error = function(e) {
      as.data.table(DF6110.2_2)
    })
  })
  
  observe({
    data$df6110 <- as.data.table(DF6110)
    data$df6110.1 <- as.data.table(DF6110.1)
    data$df6110.2 <- as.data.table(DF6110.2)
    data$df6110.1_2 <- as.data.table(DF6110.1_2)
    data$df6110.2_2 <- as.data.table(DF6110.2_2)
  })
  
  observe({
    if(!is.null(input$table6110Item1))
      data$df6110 <- hot_to_r(input$table6110Item1)
  })
  
  observe({
    if(!is.null(input$table6110_1Item1)) {
      new_df <- hot_to_r(input$table6110_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6110.1 <- validation_result$df
      } else {
        data$df6110.1 <- new_df
      }
    }
  })
  
  observe({
    if(!is.null(input$table6110_2Item1)) {
      new_df <- hot_to_r(input$table6110_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6110.2 <- validation_result$df
      } else {
        data$df6110.2 <- new_df
      }
    }
  })
  
  #*****************************************
  
  #ОСВ: 6110
  
  observeEvent(input$dates6110, {
    start <- ymd(input$dates6110[[1]])
    end <- ymd(input$dates6110[[2]])
    
    tryCatch({
      if (start > end) {
        shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
        updateDateRangeInput(
          session, 
          "dates6110", 
          start = r$start,
          end = r$end
        )
      } else {
        r$start <- input$dates6110[[1]]
        r$end <- input$dates6110[[2]]
      }
    }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6110",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
  }, ignoreInit = TRUE)
  
  observe({
    if (!any(is.na(input$dates6110))) {
      from=as.Date(input$dates6110[1L])
      to=as.Date(input$dates6110[2L])
      if (from>to) to = from
      selectdates6110.1_5 <- seq.Date(from=from,
                                      to=to, by = "day")
      data$df6110.1_1 <- data$df6110.1[as.Date(data$df6110.1$`Дата операции`) %in% selectdates6110.1_5, ]
    } else {
      selectdates6110.1_6 <- unique(as.Date(data$df6110.1$`Дата операции`))
      data$df6110.1_1 <- data$df6110.1[data$df6110.1$`Дата операции` %in% selectdates6110.1_6, ]
    }
  })
  
  observe({
    if (!any(is.na(input$dates6110))) {
      from=as.Date(input$dates6110[1L])
      to=as.Date(input$dates6110[2L])
      if (from>to) to = from
      selectdates6110.2_5 <- seq.Date(from=from,
                                      to=to, by = "day")
      data$df6110.2_1 <- data$df6110.2[as.Date(data$df6110.2$`Дата операции`) %in% selectdates6110.2_5, ]
    } else {
      selectdates6110.2_6 <- unique(as.Date(data$df6110.2$`Дата операции`))
      data$df6110.2_1 <- data$df6110.2[data$df6110.2$`Дата операции` %in% selectdates6110.2_6, ]
    }
  })
  
  observe({
    data$df6110[1, 2:5] <- data$df6110.1_1[, list(
      `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
      Кредит = sum(`Кредит`, na.rm = TRUE),
      Дебет = sum(`Дебет`, na.rm = TRUE),
      `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
    ), by="Номер первичного документа"][, .(
      `Сальдо начальное` = sum(`Сальдо начальное`),
      Дебет = sum(Дебет),
      Кредит = sum(Кредит),
      `Сальдо конечное` = sum(`Сальдо конечное`)
    )]
  })
  
  observe({
    data$df6110[2, 2:5] <- data$df6110.2_1[, list(
      `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
      Кредит = sum(`Кредит`, na.rm = TRUE),
      Дебет = sum(`Дебет`, na.rm = TRUE),
      `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
    ), by="Номер первичного документа"][, .(
      `Сальдо начальное` = sum(`Сальдо начальное`),
      Дебет = sum(Дебет),
      Кредит = sum(Кредит),
      `Сальдо конечное` = sum(`Сальдо конечное`)
    )]
  })
  
  observe({ data$df6110[3, 2:5] <- data$df6110[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })
  
  output$nested_ui6110 <- renderUI({!any(is.na(input$dates6110))})
  
  output$table6110Item1 <- renderRHandsontable({
    rhandsontable(data$df6110, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
      hot_col(1, width = 550) |>
      hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
         if (row === 2) { td.style.fontWeight = 'bold';
         } Handsontable.renderers.TextRenderer.apply(this, arguments);
      }")
  })
  
  output$download_df6110 <- downloadHandler(
    filename = function() { "df6110.xlsx" },
    content = function(file) {
      write.xlsx(data$df6110, file)
    })
  
  #**************************************
  
  #6110.1
  
  observeEvent(input$dates6110.1, {
    start <- ymd(input$dates6110.1[[1]])
    end <- ymd(input$dates6110.1[[2]])
    
    tryCatch({  
      if (start > end) {
        shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
        updateDateRangeInput(
          session, 
          "dates6110.1", 
          start = r$start,
          end = r$end
        )
      } else {
        r$start <- input$dates6110.1[[1]]
        r$end <- input$dates6110.1[[2]]
      }
    }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6110.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
  }, ignoreInit = TRUE)
  
  observe({ if (!is.null(input$table6110.1Item1)) {
    data$df6110.1 <- hot_to_r(input$table6110.1Item1) 
    
    if (!any(is.na(input$dates6110.1)) && input$choices6110.1 == "Выбор по дате операции") {
      from=as.Date(input$dates6110.1[1L])
      to=as.Date(input$dates6110.1[2L])
      if (from>to) to = from
      selectdates6110.1_1 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.1_2 <- data$df6110.1[as.Date(data$df6110.1$"Дата операции") %in% selectdates6110.1_1, ]
    } else if (!is.null(input$text) && input$choices6110.1 == "Выбор по номеру первичного документа") {
      data$df6110.1_2 <- data$df6110.1[data$df6110.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6110.1 == "Выбор по статье дохода") {
      data$df6110.1_2 <- data$df6110.1[data$df6110.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6110.1) && !any(is.na(input$dates6110.1)) && !is.null(input$text) && input$choices6110.1 == "Выбор по дате операции и номеру первичного документа") {
      from=as.Date(input$dates6110.1[1L])
      to=as.Date(input$dates6110.1[2L])
      if (from>to) to = from
      selectdates6110.1_2 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.1_2 <- data$df6110.1[as.Date(data$df6110.1$"Дата операции") %in% selectdates6110.1_2 & data$df6110.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6110.1) && !any(is.na(input$dates6110.1)) && !is.null(input$text) && input$choices6110.1 == "Выбор по дате операции и статье дохода") {
      from=as.Date(input$dates6110.1[1L])
      to=as.Date(input$dates6110.1[2L])
      if (from>to) to = from
      selectdates6110.1_3 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.1_2 <- data$df6110.1[as.Date(data$df6110.1$"Дата операции") %in% selectdates6110.1_3 & data$df6110.1$"Счет № статьи дохода" == input$text, ]
    } else {
      selectdates6110.1_4 <- unique(data$df6110.1$"Дата операции")
      data$df6110.1_2 <- data$df6110.1[data$df6110.1$"Дата операции" %in% selectdates6110.1_4, ]
    }
  }
  })
  
  output$table6110.1Item1 <- renderRHandsontable({
    
    data$df6110.1[, `Сальдо конечное` := data$df6110.1[[6]] + data$df6110.1[[7]] - data$df6110.1[[8]]]
    
    rhandsontable(data$df6110.1, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6110.1 <- renderUI({
    if (input$choices6110.1 == "Выбор по дате операции") {
      dateRangeInput("dates6110.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6110.1 == "Выбор по номеру первичного документа") {
      textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6110.1 == "Выбор по статье дохода") {
      textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6110.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
        dateRangeInput("dates6110.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6110.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
        dateRangeInput("dates6110.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })
  
  output$table6110.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6110.1_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$download_df6110.1 <- downloadHandler(
    filename = function() { "df6110.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6110.1, file)
    })
  
  output$download_df6110.1_2 <- downloadHandler(
    filename = function() { "df6110.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6110.1_2, file)
    })
  
  #****************************************
  
  #6110.2
  
  observeEvent(input$dates6110.2, {
    start <- ymd(input$dates6110.2[[1]])
    end <- ymd(input$dates6110.2[[2]])
    
    tryCatch({  
      if (start > end) {
        shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
        updateDateRangeInput(
          session, 
          "dates6110.2", 
          start = r$start,
          end = r$end
        )
      } else {
        r$start <- input$dates6110.2[[1]]
        r$end <- input$dates6110.2[[2]]
      }
    }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6110.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
  }, ignoreInit = TRUE)
  
  observe({ if (!is.null(input$table6110.2Item1)) {
    data$df6110.2 <- hot_to_r(input$table6110.2Item1) 
    
    if (!any(is.na(input$dates6110.2)) && input$choices6110.2 == "Выбор по дате операции") {
      from=as.Date(input$dates6110.2[1L])
      to=as.Date(input$dates6110.2[2L])
      if (from>to) to = from
      selectdates6110.2_1 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.2_2 <- data$df6110.2[as.Date(data$df6110.2$"Дата операции") %in% selectdates6110.2_1, ]
    } else if (!is.null(input$text) && input$choices6110.2 == "Выбор по номеру первичного документа") {
      data$df6110.2_2 <- data$df6110.2[data$df6110.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6110.2 == "Выбор по статье дохода") {
      data$df6110.2_2 <- data$df6110.2[data$df6110.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6110.2) && !any(is.na(input$dates6110.2)) && !is.null(input$text) && input$choices6110.2 == "Выбор по дате операции и номеру первичного документа") {
      from=as.Date(input$dates6110.2[1L])
      to=as.Date(input$dates6110.2[2L])
      if (from>to) to = from
      selectdates6110.2_2 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.2_2 <- data$df6110.2[as.Date(data$df6110.2$"Дата операции") %in% selectdates6110.2_2 & data$df6110.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6110.2) && !any(is.na(input$dates6110.2)) && !is.null(input$text) && input$choices6110.2 == "Выбор по дате операции и статье дохода") {
      from=as.Date(input$dates6110.2[1L])
      to=as.Date(input$dates6110.2[2L])
      if (from>to) to = from
      selectdates6110.2_3 <- seq.Date(from=from, to=to, by = "day")
      data$df6110.2_2 <- data$df6110.2[as.Date(data$df6110.2$"Дата операции") %in% selectdates6110.2_3 & data$df6110.2$"Счет № статьи дохода" == input$text, ]
    } else {
      selectdates6110.2_4 <- unique(data$df6110.2$"Дата операции")
      data$df6110.2_2 <- data$df6110.2[data$df6110.2$"Дата операции" %in% selectdates6110.2_4, ]
    }
  }
  })
  
  output$table6110.2Item1 <- renderRHandsontable({
    
    data$df6110.2[, `Сальдо конечное` := data$df6110.2[[6]] + data$df6110.2[[7]] - data$df6110.2[[8]]]
    
    rhandsontable(data$df6110.2, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6110.2 <- renderUI({
    if (input$choices6110.2 == "Выбор по дате операции") {
      dateRangeInput("dates6110.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6110.2 == "Выбор по номеру первичного документа") {
      textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6110.2 == "Выбор по статье дохода") {
      textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6110.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
        dateRangeInput("dates6110.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6110.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
        dateRangeInput("dates6110.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })
  
  output$table6110.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6110.2_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$download_df6110.2 <- downloadHandler(
    filename = function() { "df6110.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6110.2, file)
    })
  
  output$download_df6110.2_2 <- downloadHandler(
    filename = function() { "df6110.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6110.2_2, file)
    })
  
  # Neon Restart functionality
  observeEvent(input$restart_neon, {
    # Show loading message
    output$restart_status <- renderText("Restarting Neon endpoint... Please wait.")
    
    # Call the restart function
    result <- restart_neon_endpoint()
    
    # Display result
    if (result$success) {
      output$restart_status <- renderText(result$message)
      shinyalert("Success", result$message, type = "success")
    } else {
      output$restart_status <- renderText(paste("Error:", result$message))
      shinyalert("Error", result$message, type = "error")
    }
  })
  
  # NEW: Neon Projects functionality
  observeEvent(input$fetch_projects, {
    # Show loading message
    output$projects_status <- renderText("Fetching Neon projects... Please wait.")
    
    # Call the function to get projects
    result <- get_neon_projects(limit = input$projects_limit)
    
    # Display result
    if (result$success) {
      # Format the projects data
      formatted_data <- format_projects_data(result$data)
      projects_data(formatted_data)
      
      output$projects_status <- renderText(paste("Successfully fetched", nrow(formatted_data), "projects"))
      shinyalert("Success", paste("Successfully fetched", nrow(formatted_data), "projects"), type = "success")
    } else {
      output$projects_status <- renderText(paste("Error:", result$message))
      projects_data(data.table(Message = "Error fetching projects"))
      shinyalert("Error", result$message, type = "error")
    }
  })
  
  # Render projects table
  output$projects_table <- renderRHandsontable({
    rhandsontable(projects_data(), 
                  colWidths = 150, 
                  height = 400, 
                  readOnly = TRUE, 
                  contextMenu = FALSE,
                  manualColumnResize = TRUE) |>
      hot_cols(renderer = "
        function(instance, td, row, col, prop, value, cellProperties) {
          Handsontable.renderers.TextRenderer.apply(this, arguments);
          if (value && typeof value === 'string' && value.includes('Error') || value === 'No projects loaded' || value === 'No projects found') {
            td.style.color = 'red';
            td.style.fontWeight = 'bold';
          }
        }
      ")
  })
  
  # Download projects data
  output$download_projects <- downloadHandler(
    filename = function() {
      paste("neon_projects_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(projects_data(), file, row.names = FALSE)
    }
  )
  
}
shinyApp(ui, server)