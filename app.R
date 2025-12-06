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
library(httr)
library(jsonlite)

# Упрощенная функция создания подключения
create_database_connection <- function() {
  tryCatch({
    database_url <- Sys.getenv("DATABASE_URL")
    
    if (database_url == "") {
      message("DATABASE_URL environment variable is empty")
      return(NULL)
    }
    
    message("Attempting to connect to database...")
    
    # Простой и надежный способ разбора URL
    conn_parts <- strsplit(gsub("postgresql://", "", database_url), "@")[[1]]
    if (length(conn_parts) != 2) {
      message("Invalid DATABASE_URL format")
      return(NULL)
    }
    
    user_pass <- strsplit(conn_parts[1], ":")[[1]]
    if (length(user_pass) != 2) {
      message("Invalid user:password format")
      return(NULL)
    }
    
    username <- user_pass[1]
    password <- user_pass[2]
    
    host_db <- strsplit(conn_parts[2], "/")[[1]]
    if (length(host_db) != 2) {
      message("Invalid host/database format")
      return(NULL)
    }
    
    host_port <- strsplit(host_db[1], ":")[[1]]
    host <- host_port[1]
    port <- ifelse(length(host_port) > 1, as.numeric(host_port[2]), 5432)
    dbname <- host_db[2]
    
    dbname <- strsplit(dbname, "\\?")[[1]][1]
    
    message(paste("Connecting to:", host, "port:", port, "database:", dbname))
    
    conn <- dbConnect(
      RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = username,
      password = password,
      sslmode = "require"
    )
    
    test_result <- dbGetQuery(conn, "SELECT 1 as test")
    message("Database connection established successfully")
    
    return(conn)
    
  }, error = function(e) {
    message("Database connection failed: ", e$message)
    return(NULL)
  })
}

# УЛУЧШЕННАЯ функция загрузки данных с ПРАВИЛЬНОЙ обработкой факторов
load_data_simple <- function(table_name, session_id) {
  message("Loading data from: ", table_name, " for session: ", session_id)
  
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("No database connection available")
      return(NULL)
    }
    
    # Определяем запрос в зависимости от таблицы
    if (table_name == "app_data_6120") {
      query <- "SELECT account_name, initial_balance, debit, credit, final_balance 
                FROM app_data_6120 
                WHERE session_id = $1 
                ORDER BY id"
    } else if (table_name == "app_data_6120_1") {
      query <- "SELECT operation_date, document_number, income_account, dividend_period, 
                       operation_description, accounting_method, initial_balance, credit, debit,
                       correspondence_debit, correspondence_credit, final_balance
                FROM app_data_6120_1 
                WHERE session_id = $1 
                ORDER BY id"
    } else if (table_name == "app_data_6120_2") {
      query <- "SELECT operation_date, document_number, income_account, dividend_period, 
                       operation_description, accounting_method, initial_balance, credit, debit,
                       correspondence_debit, correspondence_credit, final_balance
                FROM app_data_6120_2 
                WHERE session_id = $1 
                ORDER BY id"
    } else {
      stop("Unknown table: ", table_name)
    }
    
    message("Executing query: ", query)
    result <- dbGetQuery(conn, query, params = list(session_id))
    
    if (nrow(result) == 0) {
      message("No data found for session ", session_id, " in table ", table_name)
      return(NULL)
    }
    
    # Улучшенная обработка дат - преобразуем в правильный формат
    if ("operation_date" %in% names(result)) {
      result$operation_date <- as.character(result$operation_date)
      message("Converted operation_date to character. Sample: ", 
              paste(head(result$operation_date), collapse = ", "))
    }
    
    message("Successfully loaded ", nrow(result), " rows from ", table_name)
    message("Column names in loaded data: ", paste(names(result), collapse = ", "))
    message("First few rows:")
    print(head(result))
    
    return(result)
    
  }, error = function(e) {
    message("Error loading data from ", table_name, ": ", e$message)
    return(NULL)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Упрощенная функция сохранения данных с ПРАВИЛЬНОЙ обработкой факторов
save_data_simple <- function(data, table_name, session_id) {
  if (is.null(data) || nrow(data) == 0) {
    message("No data to save")
    return(FALSE)
  }
  
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("No database connection for save")
      return(FALSE)
    }
    
    dbExecute(conn, "BEGIN")
    
    # Удаляем старые данные
    delete_query <- paste("DELETE FROM", table_name, "WHERE session_id = $1")
    dbExecute(conn, delete_query, list(session_id))
    
    # Вставляем новые данные
    if (table_name == "app_data_6120") {
      insert_query <- paste(
        "INSERT INTO", table_name,
        "(session_id, account_name, initial_balance, debit, credit, final_balance) 
        VALUES ($1, $2, $3, $4, $5, $6)"
      )
      
      for(i in 1:nrow(data)) {
        dbExecute(conn, insert_query, list(
          session_id,
          as.character(data[i, 1]),
          as.numeric(data[i, 2] %||% 0),
          as.numeric(data[i, 3] %||% 0),
          as.numeric(data[i, 4] %||% 0),
          as.numeric(data[i, 5] %||% 0)
        ))
      }
    } else if (table_name %in% c("app_data_6120_1", "app_data_6120_2")) {
      insert_query <- paste(
        "INSERT INTO", table_name,
        "(session_id, operation_date, document_number, income_account, 
        dividend_period, operation_description, accounting_method, 
        initial_balance, credit, debit, correspondence_debit, 
        correspondence_credit, final_balance)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
      )
      
      for(i in 1:nrow(data)) {
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Правильное преобразование факторов в символы
        # Используем as.character() для factor колонок вместо прямого доступа
        corr_debit_val <- if ("Счет № (дебет)" %in% names(data)) {
          # Если это factor, преобразуем в character
          if (is.factor(data[[10]][i])) {
            as.character(data[[10]][i])
          } else {
            as.character(data[i, 10] %||% NA)
          }
        } else {
          as.character(data[i, 10] %||% NA)
        }
        
        corr_credit_val <- if ("Счет № (кредит)" %in% names(data)) {
          # Если это factor, преобразуем в character
          if (is.factor(data[[11]][i])) {
            as.character(data[[11]][i])
          } else {
            as.character(data[i, 11] %||% NA)
          }
        } else {
          as.character(data[i, 11] %||% NA)
        }
        
        dbExecute(conn, insert_query, list(
          session_id,
          as.character(data[i, 1] %||% NA),
          as.character(data[i, 2] %||% NA),
          as.character(data[i, 3] %||% NA),
          as.character(data[i, 4] %||% NA),
          as.character(data[i, 5] %||% NA),
          as.character(data[i, 6] %||% NA),
          as.numeric(data[i, 7] %||% 0),
          as.numeric(data[i, 8] %||% 0),
          as.numeric(data[i, 9] %||% 0),
          corr_debit_val,
          corr_credit_val,
          as.numeric(data[i, 12] %||% 0)
        ))
      }
    }
    
    dbExecute(conn, "COMMIT")
    message("Successfully saved ", nrow(data), " rows to ", table_name)
    return(TRUE)
    
  }, error = function(e) {
    try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
    message("Error saving data: ", e$message)
    return(FALSE)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# УЛУЧШЕННАЯ функция получения сессий с правильной сортировкой
get_recent_sessions <- function() {
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("No database connection in get_recent_sessions")
      return(character(0))
    }
    
    # Объединяем запросы для получения сессий с временными метками
    query <- "
      SELECT session_id, MAX(created_at) as last_activity
      FROM (
        SELECT session_id, created_at FROM app_data_6120 WHERE session_id IS NOT NULL AND session_id != ''
        UNION ALL
        SELECT session_id, created_at FROM app_data_6120_1 WHERE session_id IS NOT NULL AND session_id != ''
        UNION ALL
        SELECT session_id, created_at FROM app_data_6120_2 WHERE session_id IS NOT NULL AND session_id != ''
      ) AS all_sessions
      GROUP BY session_id
      ORDER BY last_activity DESC
      LIMIT 2
    "
    
    result <- dbGetQuery(conn, query)
    sessions <- result$session_id
    
    message("Found recent sessions: ", paste(sessions, collapse = ", "))
    return(sessions)
    
  }, error = function(e) {
    message("Error getting sessions: ", e$message)
    return(character(0))
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Инициализация базы данных
initialize_database_simple <- function() {
  conn <- NULL
  tryCatch({
    conn <- create_database_connection()
    if (is.null(conn)) {
      message("Cannot initialize database - no connection")
      return(FALSE)
    }
    
    tables <- list(
      app_data_6120 = "
        CREATE TABLE IF NOT EXISTS app_data_6120 (
          id SERIAL PRIMARY KEY,
          session_id VARCHAR(255) NOT NULL,
          account_name VARCHAR(500),
          initial_balance NUMERIC DEFAULT 0,
          debit NUMERIC DEFAULT 0,
          credit NUMERIC DEFAULT 0,
          final_balance NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
      
      app_data_6120_1 = "
        CREATE TABLE IF NOT EXISTS app_data_6120_1 (
          id SERIAL PRIMARY KEY,
          session_id VARCHAR(255) NOT NULL,
          operation_date DATE,
          document_number VARCHAR(255),
          income_account VARCHAR(255),
          dividend_period VARCHAR(255),
          operation_description TEXT,
          accounting_method VARCHAR(255),
          initial_balance NUMERIC DEFAULT 0,
          credit NUMERIC DEFAULT 0,
          debit NUMERIC DEFAULT 0,
          correspondence_debit VARCHAR(255),
          correspondence_credit VARCHAR(255),
          final_balance NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
      
      app_data_6120_2 = "
        CREATE TABLE IF NOT EXISTS app_data_6120_2 (
          id SERIAL PRIMARY KEY,
          session_id VARCHAR(255) NOT NULL,
          operation_date DATE,
          document_number VARCHAR(255),
          income_account VARCHAR(255),
          dividend_period VARCHAR(255),
          operation_description TEXT,
          accounting_method VARCHAR(255),
          initial_balance NUMERIC DEFAULT 0,
          credit NUMERIC DEFAULT 0,
          debit NUMERIC DEFAULT 0,
          correspondence_debit VARCHAR(255),
          correspondence_credit VARCHAR(255),
          final_balance NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    )
    
    for (table_name in names(tables)) {
      dbExecute(conn, tables[[table_name]])
    }
    
    message("Database initialized successfully")
    return(TRUE)
    
  }, error = function(e) {
    message("Database initialization error: ", e$message)
    return(FALSE)
  }, finally = {
    if (!is.null(conn)) {
      try(dbDisconnect(conn), silent = TRUE)
    }
  })
}

# Проверка подключения к базе данных
test_database_connection <- function() {
  conn <- create_database_connection()
  if (!is.null(conn)) {
    try(dbDisconnect(conn), silent = TRUE)
    return(TRUE)
  }
  return(FALSE)
}

# Helper function for null coalescing
`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y

#================================================
# БАЗОВЫЙ КОД

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

BaseAccountsList <- list("1010", "1020", "1030", "1040", "1050", "1060", "1070", "1080", "1111.1",
                         "1111.2", "1111.3", "1111.4", "1112.1", "1112.2", "1112.3", "1112.4", "1112.5", "1121.1", "1121.2",
                         "1121.3", "1122.1", "1122.2", "1122.3", "1122.4", "1123.1", "1123.2", "1123.3", "1123.4", "1123.5",
                         "1131.1", "1131.2", "1131.3", "1131.4", "1132.1", "1132.2", "1132.3", "1132.4", "1132.5", "1141.1",
                         "1141.2", "1141.3", "1141.4", "1141.5", "1142.1", "1142.2", "1142.3", "1142.4", "1142.5", "1143.1",
                         "1143.2", "1143.3", "1143.4", "1143.5", "1144.1", "1144.2", "1144.3", "1144.4", "1144.5", "1144.6",
                         "1145", "1146.1", "1146.2", "1146.3", "1147.1", "1147.2", "1147.3", "1151", "1152", "1153",
                         "1154", "1155", "1156", "1161", "1162", "1170", "1210", "1220", "1230", "1240",
                         "1250", "1260", "1270", "1280", "1311", "1312", "1313", "1314", "1315", "1316",
                         "1317", "1318", "1319", "1321", "1322", "1323", "1324", "1325", "1326", "1327",
                         "1330", "1341", "1342", "1343", "1351", "1352", "1360", "1370", "1410", "1420",
                         "1430", "1440", "1450", "1460", "1470", "1480", "1510", "1520", "1530", "1540",
                         "1550", "1610", "1620", "1630", "1711", "1712", "1713", "1714", "1715", "1716",
                         "1720", "1730", "1740", "1750", "2011.1", "2011.2", "2011.3", "2011.4", "2012.1", "2012.2",
                         "2012.3", "2012.4", "2012.5", "2021.1", "2021.2", "2021.3", "2022.1", "2022.2", "2022.3", "2022.4",
                         "2023.1", "2023.2", "2023.3", "2023.4", "2023.5", "2031.1", "2031.2", "2031.3", "2031.4", "2032.1",
                         "2032.2", "2032.3", "2032.4", "2032.5", "2033.1", "2033.2", "2033.3", "2034.1", "2034.2", "2034.3",
                         "2034.4", "2035.1", "2035.2", "2035.3", "2035.4", "2035.5", "2041.1", "2041.2", "2041.3", "2041.4",
                         "2041.5", "2042.1", "2042.2", "2042.3", "2042.4", "2042.5", "2043.1", "2043.2", "2043.3", "2043.4",
                         "2043.5", "2044.1", "2044.2", "2044.3", "2044.4", "2044.5", "2044.6", "2045", "2046.1", "2046.2",
                         "2046.3", "2047.1", "2047.2", "2047.3", "2051", "2052", "2053", "2054", "2055", "2060.1",
                         "2060.2", "2060.3", "2071", "2072", "2080", "2110", "2120", "2130", "2140", "2150",
                         "2160", "2170", "2180", "2210", "2221", "2222", "2230", "2311", "2312", "2313",
                         "2314", "2315", "2320", "2330", "2411", "2412", "2413", "2414", "2415", "2416",
                         "2417", "2418", "2421", "2422", "2423", "2424", "2425", "2426", "2427", "2428",
                         "2430", "2441", "2442", "2443", "2444", "2445", "2446", "2447", "2451", "2452",
                         "2453", "2454", "2455", "2456", "2457", "2460", "2510", "2520", "2530", "2540",
                         "2610", "2710", "2731", "2732", "2733", "2734", "2735", "2741", "2742", "2743",
                         "2744", "2745", "2750", "2761", "2762", "2763", "2764", "2765", "2766", "2767",
                         "2771", "2772", "2773", "2774", "2775", "2776", "2777", "2780", "2810", "2911",
                         "2912", "2913", "2914", "2915", "2916", "2920", "2930", "2940", "2950", "2960",
                         "2970", "2980", "2990", "3011.1", "3011.2", "3011.3", "3011.4", "3012.1", "3012.2", "3012.3",
                         "3012.4", "3012.5", "3021.1", "3021.2", "3021.3", "3021.4", "3022.1", "3022.2", "3022.3", "3022.4",
                         "3022.5", "3031.1", "3031.2", "3031.3", "3031.4", "3031.5", "3032.1", "3032.2", "3032.3", "3032.4",
                         "3032.5", "3033.1", "3033.2", "3033.3", "3033.4", "3033.5", "3034.1", "3034.2", "3034.3", "3034.4",
                         "3034.5", "3034.6", "3035", "3036.1", "3036.2", "3036.3", "3037.1", "3037.2", "3037.3", "3041",
                         "3042", "3051", "3052", "3053", "3054", "3061.1", "3061.2", "3061.3", "3061.4", "3062.1",
                         "3062.2", "3062.3", "3062.4", "3062.5", "3071.1", "3071.2", "3071.3", "3071.4", "3072.1", "3072.2",
                         "3072.3", "3072.4", "3072.5", "3081", "3082", "3110", "3120", "3130", "3140", "3150",
                         "3160", "3170", "3180", "3210", "3220", "3230", "3310", "3320", "3330", "3340",
                         "3350", "3360", "3370", "3380", "3410", "3420", "3430", "3440", "3450", "3511",
                         "3512", "3513", "3520", "3530", "3540", "3550", "3560", "4011.1", "4011.2", "4011.3",
                         "4011.4", "4012.1", "4012.2", "4012.3", "4012.4", "4012.5", "4021.1", "4021.2", "4021.3", "4021.4",
                         "4022.1", "4022.2", "4022.3", "4022.4", "4022.5", "4023.1", "4023.2", "4023.3", "4023.4", "4024.1",
                         "4024.2", "4024.3", "4024.4", "4025.1", "4025.2", "4025.3", "4025.4", "4025.5", "4031.1", "4031.2",
                         "4031.3", "4031.4", "4031.5", "4032.1", "4032.2", "4032.3", "4032.4", "4032.5", "4033.1", "4033.2",
                         "4033.4", "4033.5", "4034.1", "4034.2", "4034.3", "4034.4", "4034.5", "4034.6", "4035", "4036.1",
                         "4036.2", "4036.3", "4037.1", "4037.2", "4037.3", "4041", "4042", "4051", "4052", "4053",
                         "4054", "4061", "4062", "4110", "4120", "4130", "4140", "4150", "4160", "4210",
                         "4220", "4230", "4240", "4250", "4310", "4411", "4412", "4413", "4420", "4430",
                         "4440", "4450", "5010", "5020", "5030", "5110", "5210", "5310", "5410", "5420",
                         "5511", "5512", "5513", "5514", "5515", "5521", "5522", "5523", "5524", "5531",
                         "5532", "5541", "5542", "5543", "5544", "5545", "5610", "5620", "6010", "6020",
                         "6030", "6110.1", "6110.2", "6120.1", "6120.2", "6130.1", "6130.2", "6141.1", "6141.2", "6142.1",
                         "6142.2", "6143", "6150", "6160", "6171", "6172", "6210.1", "6210.2", "6220.1", "6220.2",
                         "6230.1", "6230.2", "6240.1", "6240.2", "6250.1", "6250.2", "6261.1", "6261.2", "6262.1", "6262.2",
                         "6263.1", "6263.2", "6264.1", "6264.2", "6271.1", "6271.2", "6272.1", "6272.2", "6273", "6274",
                         "6275.1", "6275.2", "6276.1", "6276.2", "6277.1", "6277.2", "6281.1", "6281.2", "6282", "6291",
                         "6292.1", "6292.2", "6293.1", "6293.2", "6294.1", "6294.2", "6310", "6320", "6330", "7010",
                         "7110", "7210", "7310.1", "7310.2", "7320.1", "7320.2", "7331.1", "7331.2", "7332", "7340.1",
                         "7340.2", "7350.1", "7350.2", "7410.1", "7410.2", "7420.1", "7420.2", "7430.1", "7430.2", "7440.1",
                         "7440.2", "7451.1", "7451.2", "7452.1", "7452.2", "7453.1", "7453.2", "7454.1", "7454.2", "7461.1",
                         "7461.2", "7462.1", "7462.2", "7463", "7464", "7465.1", "7465.2", "7466.1", "7466.2", "7467.1",
                         "7467.2", "7471.1", "7471.2", "7472", "7481", "7482.1", "7482.2", "7483.1", "7483.2", "7484.1",
                         "7484.2", "7485.1", "7485.2", "7510", "7520", "7530", "7610", "7620")

# ИСПРАВЛЕНИЕ: Создаем вектор из списка для использования в факторах
BaseAccountsVector <- unlist(BaseAccountsList)

#****************************
DF6120 <- data.table(
  "Счет (субчет)" = as.character(c(
    "6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
    "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
    "Итого"
  )),	
  "Сальдо начальное" = as.numeric(c(0, 0, 0)),
  "Дебет" = as.numeric(c(0, 0, 0)),
  "Кредит" = as.numeric(c(0, 0, 0)),
  "Сальдо конечное" = as.numeric(c(0, 0, 0)),
  stringsAsFactors = FALSE
)

# ИСПРАВЛЕНИЕ: Правильное создание факторов с использованием вектора
DF6120.1 <- data.table(
  "Дата операции" = as.character(character()),
  "Номер первичного документа" = character(),
  "Счет № статьи дохода" = character(),
  "Период, к которому относятся дивиденды" = character(),
  "Содержание операции" = character(),
  "Метод учета" = character(),
  "Сальдо начальное" = numeric(),
  "Кредит" = numeric(),
  "Дебет" = numeric(),
  "Счет № (дебет)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Сальдо конечное" = numeric()
)

DF6120.2 <- data.table(
  "Дата операции" = as.character(character()),
  "Номер первичного документа" = character(),
  "Счет № статьи дохода" = character(),
  "Период, к которому относятся дивиденды" = character(),
  "Содержание операции" = character(),
  "Метод учета" = character(),
  "Сальдо начальное" = numeric(),
  "Кредит" = numeric(),
  "Дебет" = numeric(),
  "Счет № (дебет)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Сальдо конечное" = numeric()
)

# Additional data tables for filtered views
DF6120.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся дивиденды" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE
)

DF6120.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся дивиденды" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = BaseAccountsVector, ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE
)

ui <- fluidPage(
  tags$head(
    tags$script(HTML("
      $(document).on('shiny:disconnected', function(event) {
        $('#connection-status').html('<span style=\"color: red;\">● Disconnected</span>');
        $('#connection-status').css('background-color', '#ffebee');
      });
      
      $(document).on('shiny:connected', function(event) {
        $('#connection-status').html('<span style=\"color: green;\">● Connected</span>');
        $('#connection-status').css('background-color', '#e8f5e8');
      });
    ")),
    tags$style(HTML("
      .connection-status {
        position: fixed;
        top: 10px;
        right: 10px;
        z-index: 9999;
        background: #e8f5e8;
        padding: 8px 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        font-size: 14px;
        font-weight: bold;
        border: 1px solid #4caf50;
      }
      .session-info {
        background: #e3f2fd;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        border: 1px solid #2196f3;
      }
      .loading-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(255, 255, 255, 0.8);
        z-index: 9999;
        display: flex;
        justify-content: center;
        align-items: center;
        flex-direction: column;
      }
    "))
  ),
  
  dashboardPage(
    dashboardHeader(
      title = "МСФО",
      tags$li(class = "dropdown",
        tags$div(class = "connection-status", id = "connection-status",
          tags$span(style = "color: green;", "● Connected")
        )
      )
    ),
    dashboardSidebar(
      width = 1050,
      sidebarMenu(
        menuItem("Home", tabName = "home", icon = icon("home")),
        menuItem("Учет", tabName = "Учет", icon = icon("calculator"),
          menuItem("Доходы", tabName = "Profit", 
            menuItem("6120.Доходы по дивидендам", tabName = "Prft6120",
              menuSubItem("Оборотно-сальдовая ведомость", tabName = "table6120"),
              menuSubItem("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка", tabName = "table6120_1"),
              menuSubItem("6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе", tabName = "table6120_2")
            )
          )
        )
      )
    ),
    dashboardBody(
      tags$style('
        @media (min-width: 768px){
          .sidebar-mini.sidebar-collapse .main-header .logo {
              width: 230px; 
          }
          .sidebar-mini.sidebar-collapse .main-header .navbar {
              margin-left: 230px;
          }
        }
      '),
      conditionalPanel(
        condition = "output.show_loading",
        tags$div(class = "loading-overlay",
          tags$h3("Загрузка данных..."),
          tags$p("Пожалуйста, подождите"),
          tags$br(),
          tags$div(class = "spinner-border text-primary", role = "status")
        )
      ),
      tabItems(
        tabItem(tabName = "home",
          h2("Добро пожаловать в систему МСФО"),
          fluidRow(
            box(width = 12, title = "Управление данными", status = "primary",
              actionButton("test_connection", "Тест подключения к базе данных", 
                         icon = icon("database"), class = "btn-info"),
              verbatimTextOutput("connection_status"),
              br(),
              div(class = "session-info",
                h4("Управление сессиями"),
                fluidRow(
                  column(12, 
                    # УЛУЧШЕННЫЙ ВЫБОР СЕССИЙ С АВТООБНОВЛЕНИЕМ
                    uiOutput("session_selector_ui")
                  )
                ),
                fluidRow(
                  column(6, actionButton("load_session_btn", "Загрузить сессию", 
                                       icon = icon("folder-open"), class = "btn-success", width = "100%")),
                  column(6, actionButton("save_session_btn", "Сохранить текущую сессию", 
                                       icon = icon("save"), class = "btn-warning", width = "100%"))
                ),
                # ДОБАВЛЕНА КНОПКА ДЛЯ ОТЛАДКИ
                fluidRow(
                  column(12, 
                    actionButton("debug_sessions", "Отладочная информация", 
                               icon = icon("bug"), class = "btn-info", width = "100%")
                  )
                )
              ),
              br(),
              wellPanel(
                h4("Текущая сессия"),
                textOutput("current_session_info")
              )
            )
          )
        ),
        tabItem(tabName = "table6120",
          fluidRow(
            column(width = 12, br(),
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                start = Sys.Date(), end = Sys.Date(), separator = "-")
            ),
            column(width = 12, br(),
              tags$b("ОСВ: 6120.Доходы по дивидендам"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120Item1"),
              downloadButton("download_df6120", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6120_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.1Item1"),
              downloadButton("download_df6120.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
              tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6120.1Item2"),
              downloadButton("download_df6120.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6120_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе"),
              tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120.2Item1"),
              downloadButton("download_df6120.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
              tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6120.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
                    "Выбор по номеру первичного документа", 
                    "Выбор по статье дохода", 
                    "Выбор по дате операции и номеру первичного документа", 
                    "Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6120.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6120.2Item2"),
              downloadButton("download_df6120.2_2", "Загрузить данные"))
           )
         )
       )
     )
   )
 )

server <- function(input, output, session) {
  # Generate unique session ID
  session_id <- reactiveVal({
    paste0("session_", as.integer(Sys.time()), "_", sample(1000:9999, 1))
  })
  
  # Initialize reactive values
  r <- reactiveValues(
    db_initialized = FALSE,
    show_loading = FALSE,
    sessions_loaded = FALSE,
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date()),
    # ДОБАВЛЕН РЕАКТИВНЫЙ СПИСОК СЕССИЙ
    session_list = character(0),
    # ДОБАВЛЕН ФЛАГ ОБНОВЛЕНИЯ СЕССИЙ
    sessions_updated = 0,
    # ФЛАГ ДЛЯ ПРЕДОТВРАЩЕНИЯ ПОВТОРНОЙ ИНИЦИАЛИЗАЦИИ
    db_init_notified = FALSE
  )
  
  data <- reactiveValues(
    df6120 = NULL,
    df6120.1 = NULL,
    df6120.2 = NULL,
    df6120.1_1 = NULL,
    df6120.2_1 = NULL,
    df6120.1_2 = NULL,
    df6120.2_2 = NULL
  )
  
  # Output for loading overlay
  output$show_loading <- reactive({
    r$show_loading
  })
  outputOptions(output, "show_loading", suspendWhenHidden = FALSE)
  
  # ИСПРАВЛЕННЫЙ ВЫБОР СЕССИЙ С АВТООБНОВЛЕНИЕМ
  output$session_selector_ui <- renderUI({
    # Зависимость от обновления списка сессий
    r$sessions_updated
    
    sessions <- r$session_list
    
    if (length(sessions) > 0) {
      choices <- setNames(sessions, sessions)
      choices <- c("Выберите сессию..." = "", choices)
    } else {
      choices <- c("Нет доступных сессий" = "")
    }
    
    selectInput("session_selector", "Выберите сессию для загрузки:", 
                choices = choices, width = "100%")
  })
  
  # Initialize data
  observe({
    data$df6120 <- copy(DF6120)
    data$df6120.1 <- copy(DF6120.1)
    data$df6120.2 <- copy(DF6120.2)
    data$df6120.1_2 <- copy(DF6120.1_2)
    data$df6120.2_2 <- copy(DF6120.2_2)
  })
  
  # Database initialization - ИСПРАВЛЕННАЯ ВЕРСИЯ
  observe({
    # Проверяем, не была ли уже показана нотификация
    if (!r$db_init_notified) {
      init_success <- initialize_database_simple()
      r$db_initialized <- init_success
      
      if (init_success && !r$db_init_notified) {
        showNotification("База данных инициализирована успешно.", 
                        type = "message", duration = 5)
        r$db_init_notified <- TRUE
        # Обновляем список сессий сразу после инициализации
        update_session_list()
      } else if (!init_success && !r$db_init_notified) {
        showNotification("Внимание: База данных недоступна. Работа в автономном режиме.", 
                        type = "warning", duration = 10)
        r$db_init_notified = TRUE
      }
    }
  })
  
  # НОВАЯ ФУНКЦИЯ: Обновление списка сессий
  update_session_list <- function() {
    tryCatch({
      sessions <- get_recent_sessions()
      r$session_list <- sessions
      # УВЕЛИЧИВАЕМ СЧЕТЧИК ДЛЯ ОБНОВЛЕНИЯ UI
      r$sessions_updated <- r$sessions_updated + 1
      message("Updated session list: ", paste(sessions, collapse = ", "))
    }, error = function(e) {
      message("Error updating session list: ", e$message)
      r$session_list <- character(0)
      r$sessions_updated <- r$sessions_updated + 1
    })
  }
  
  # Отладочная информация
  observeEvent(input$debug_sessions, {
    message("=== DEBUG SESSION INFORMATION ===")
    message("Current session ID: ", session_id())
    message("Database initialized: ", r$db_initialized)
    message("Sessions in list: ", paste(r$session_list, collapse = ", "))
    message("Sessions updated counter: ", r$sessions_updated)
    
    # Проверяем подключение к базе
    conn_test <- test_database_connection()
    message("Database connection test: ", conn_test)
    
    # Показываем уведомление с информацией
    shinyalert(
      title = "Отладочная информация",
      text = paste(
        "Текущая сессия:", session_id(),
        "\nБаза инициализирована:", r$db_initialized,
        "\nТест подключения:", conn_test,
        "\nДоступные сессии:", paste(r$session_list, collapse = ", "),
        "\nСчетчик обновлений:", r$sessions_updated,
        sep = "\n"
      ),
      type = "info"
    )
  })
  
  # Load session - УЛУЧШЕННАЯ ВЕРСИЯ С ПРАВИЛЬНОЙ ОБРАБОТКОЙ ФАКТОРОВ
  observeEvent(input$load_session_btn, {
    req(input$session_selector, input$session_selector != "")
    
    selected_session <- input$session_selector
    message("Попытка загрузить сеанс: ", selected_session, " сохраняя текущий идентификатор сеанса: ", session_id())
    
    r$show_loading <- TRUE
    
    tryCatch({
      showNotification(paste("Загрузка сессии:", selected_session), type = "message")
      
      # Load data from all tables
      loaded_data_6120 <- load_data_simple("app_data_6120", selected_session)
      loaded_data_6120_1 <- load_data_simple("app_data_6120_1", selected_session)
      loaded_data_6120_2 <- load_data_simple("app_data_6120_2", selected_session)
      
      # Debug information
      message("DEBUG: Loaded data summary:")
      message(" - df6120: ", if(!is.null(loaded_data_6120)) nrow(loaded_data_6120) else "NULL")
      message(" - df6120_1: ", if(!is.null(loaded_data_6120_1)) nrow(loaded_data_6120_1) else "NULL")
      message(" - df6120_2: ", if(!is.null(loaded_data_6120_2)) nrow(loaded_data_6120_2) else "NULL")
      
      # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Update the data table for df6120
      if (!is.null(loaded_data_6120)) {
        temp_data <- as.data.table(loaded_data_6120)
        message("ОТЛАДКА: столбцы df6120: ", paste(names(temp_data), collapse = ", "))
        
        # Check if we have the expected columns
        expected_cols <- c("account_name", "initial_balance", "debit", "credit", "final_balance")
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, 
                  c("account_name", "initial_balance", "debit", "credit", "final_balance"),
                  c("Счет (субчет)", "Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное"))
          data$df6120 <- temp_data
          message("ОТЛАДКА: Успешно обновлен df6120 с ", nrow(temp_data), " rows")
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df6120. Ожидалось: ", paste(expected_cols, collapse=", "), 
                  " Найдено:", paste(names(temp_data), collapse=", "))
          showNotification("Ошибка: несоответствие столбцов в таблице 6120", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df6120 не загружены")
        # Reset to default if no data
        data$df6120 <- copy(DF6120)
      }
      
      # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Update the data table for df6120.1 с правильной обработкой факторов
      if (!is.null(loaded_data_6120_1)) {
        temp_data <- as.data.table(loaded_data_6120_1)
        message("ОТЛАДКА: df6120_1 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                          "operation_description", "accounting_method", "initial_balance", 
                          "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                  c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                    "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                    "Сальдо начальное", "Кредит", "Дебет", 
                    "Счет № (дебет)", "Счет № (кредит)", 
                    "Сальдо конечное"))
          
          # Улучшенная обработка дат - сохраняем как строки для корректного отображения
          if ("Дата операции" %in% names(temp_data)) {
            temp_data[, `Дата операции` := as.character(`Дата операции`)]
          }
          
          # ИСПРАВЛЕНИЕ: Правильное преобразование столбцов в факторы
          if ("Счет № (дебет)" %in% names(temp_data)) {
            temp_data[, `Счет № (дебет)` := factor(`Счет № (дебет)`, levels = BaseAccountsVector, ordered = TRUE)]
          }
          
          if ("Счет № (кредит)" %in% names(temp_data)) {
            temp_data[, `Счет № (кредит)` := factor(`Счет № (кредит)`, levels = BaseAccountsVector, ordered = TRUE)]
          }
          
          data$df6120.1 <- temp_data
          message("ОТЛАДКА: успешно обновлен df6120.1 с ", nrow(temp_data), " rows")
          message("ОТЛАДКА: Даты в df6120.1: ", paste(head(temp_data$`Дата операции`), collapse = ", "))
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df6120_1. Ожидалось: ", paste(expected_cols, collapse=", "), 
                  " Найдено: ", paste(names(temp_data), collapse=", "))
          showNotification("Ошибка: несоответствие столбцов в таблице 6120.1", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df6120.1 не загружены")
        # Reset to default if no data
        data$df6120.1 <- copy(DF6120.1)
      }

      # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Update the data table for df6120.2 с правильной обработкой факторов      
      if (!is.null(loaded_data_6120_2)) {
        temp_data <- as.data.table(loaded_data_6120_2)
        message("ОТЛАДКА: df6120_2 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "document_number", "income_account", "dividend_period",
                          "operation_description", "accounting_method", "initial_balance", 
                          "credit", "debit", "correspondence_debit", "correspondence_credit", "final_balance")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                  c("Дата операции", "Номер первичного документа", "Счет № статьи дохода",
                    "Период, к которому относятся дивиденды", "Содержание операции", "Метод учета",
                    "Сальдо начальное", "Кредит", "Дебет", 
                    "Счет № (дебет)", "Счет № (кредит)", 
                    "Сальдо конечное"))
          
          # Улучшенная обработка дат - сохраняем как строки для корректного отображения
          if ("Дата операции" %in% names(temp_data)) {
            temp_data[, `Дата операции` := as.character(`Дата операции`)]
          }
          
          # ИСПРАВЛЕНИЕ: Правильное преобразование столбцов в факторы
          if ("Счет № (дебет)" %in% names(temp_data)) {
            temp_data[, `Счет № (дебет)` := factor(`Счет № (дебет)`, levels = BaseAccountsVector, ordered = TRUE)]
          }
          
          if ("Счет № (кредит)" %in% names(temp_data)) {
            temp_data[, `Счет № (кредит)` := factor(`Счет № (кредит)`, levels = BaseAccountsVector, ordered = TRUE)]
          }
          
          data$df6120.2 <- temp_data
          message("ОТЛАДКА: Успешно обновлено df6120.2 с ", nrow(temp_data), " rows")
          message("ОТЛАДКА: Даты в df6120.2: ", paste(head(temp_data$`Дата операции`), collapse = ", "))
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df6120_2. Ожидалось: ", paste(expected_cols, collapse=", "), 
                  " Найденно: ", paste(names(temp_data), collapse=", "))
          showNotification("Ошибка: несоответствие столбцов в таблице 6120.2", type = "error")
        }
      } else {
        message("ОТЛАДКА: Данные для df6120.2 не загружены")
        # Reset to default if no data
        data$df6120.2 <- copy(DF6120.2)
      }
      
      # НЕ меняем session_id - сохраняем текущий ID сессии
      message("Текущий идентификатор сеанса остается: ", session_id())
      
      # Force UI update by triggering reactive dependencies
      data$df6120 <- data$df6120
      data$df6120.1 <- data$df6120.1  
      data$df6120.2 <- data$df6120.2
      
      shinyalert("Успех", paste("Данные сессии загружены в текущую сессию:", session_id()), type = "success")
      
    }, error = function(e) {
      message("ОШИБКА в сеансе загрузки: ", e$message)
      shinyalert("Ошибка", paste("Ошибка при загрузке сессии:", e$message), type = "error")
    }, finally = {
      r$show_loading <- FALSE
    })
  })
  
  
  # Save session - сохраняем под текущим session_id
  observeEvent(input$save_session_btn, {
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить сессию.", type = "error")
      return()
    }
    
    current_session <- session_id()
    save_success <- TRUE
    error_messages <- c()
    
    if (!is.null(data$df6120)) {
      success <- save_data_simple(data$df6120, "app_data_6120", current_session)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120")
      }
    }
    
    if (!is.null(data$df6120.1)) {
      success <- save_data_simple(data$df6120.1, "app_data_6120_1", current_session)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.1")
      }
    }
    
    if (!is.null(data$df6120.2)) {
      success <- save_data_simple(data$df6120.2, "app_data_6120_2", current_session)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 6120.2")
      }
    }
    
    if (save_success) {
      shinyalert("Успех", paste("Сессия сохранена:", current_session), type = "success")
      # ОБНОВЛЯЕМ СПИСОК СЕССИЙ ПОСЛЕ СОХРАНЕНИЯ
      update_session_list()
    } else {
      error_msg <- paste("Ошибка сохранения данных:", paste(error_messages, collapse = "; "))
      shinyalert("Ошибка", error_msg, type = "error")
    }
  })
  
  # Test connection - ОБНОВЛЯЕМ СПИСОК СЕССИЙ
  observeEvent(input$test_connection, {
    if (test_database_connection()) {
      shinyalert("Успех", "Подключение к базе данных установлено успешно!", type = "success")
      r$db_initialized <- TRUE
      update_session_list()
    } else {
      shinyalert("Ошибка", "Не удалось подключиться к базе данных.", type = "error")
      r$db_initialized <- FALSE
    }
  })
  
  output$connection_status <- renderText({
    if (r$db_initialized) {
      "✅ База данных: Подключено"
    } else {
      "❌ База данных: Не подключено"
    }
  })
  
  output$current_session_info <- renderText({
    paste("ID текущей сессии:", session_id())
  })
  

#================================================
# БАЗОВЫЙ КОД

  observe({
    if(!is.null(input$table6120Item1))
      data$df6120 <- hot_to_r(input$table6120Item1)
  })

  observe({
    if(!is.null(input$table6120.1Item1)) {
      new_df <- hot_to_r(input$table6120.1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6120.1 <- validation_result$df
      } else {
        data$df6120.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6120.2Item1)) {
      new_df <- hot_to_r(input$table6120.2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6120.2 <- validation_result$df
      } else {
        data$df6120.2 <- new_df
      }
    }
  })

#*****************************************

#ОСВ: 6120

observeEvent(input$dates6120, {
    start <- ymd(input$dates6120[[1]])
    end <- ymd(input$dates6120[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120[[1]]
      r$end <- input$dates6120[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.1_1 <- data$df6120.1[as.Date(data$df6120.1$`Дата операции`) %in% selectdates6120.1_5, ]
    } else {
      selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
      data$df6120.1_1 <- data$df6120.1[data$df6120.1$`Дата операции` %in% selectdates6120.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.2_1 <- data$df6120.2[as.Date(data$df6120.2$`Дата операции`) %in% selectdates6120.2_5, ]
    } else {
      selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
      data$df6120.2_1 <- data$df6120.2[data$df6120.2$`Дата операции` %in% selectdates6120.2_6, ]
    }
  })

observe({
  data$df6120[1, 2:5] <- data$df6120.1_1[, list(
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
  data$df6120[2, 2:5] <- data$df6120.2_1[, list(
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

observe({ data$df6120[3, 2:5] <- data$df6120[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6120 <- renderUI({!any(is.na(input$dates6120))})

  output$table6120Item1 <- renderRHandsontable({
    rhandsontable(data$df6120, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
         if (row === 2) { td.style.fontWeight = 'bold';
         } Handsontable.renderers.TextRenderer.apply(this, arguments);
      }")
  })

  output$download_df6120 <- downloadHandler(
    filename = function() { "df6120.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120, file)
  })

#**************************************

#6120.1

observeEvent(input$dates6120.1, {
    start <- ymd(input$dates6120.1[[1]])
    end <- ymd(input$dates6120.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120.1[[1]]
      r$end <- input$dates6120.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6120.1Item1)) {
    data$df6120.1 <- hot_to_r(input$table6120.1Item1) 

    if (!any(is.na(input$dates6120.1)) && input$choices6120.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6120.1[1L])
      	to=as.Date(input$dates6120.1[2L])
      	if (from>to) to = from
      	selectdates6120.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_1, ]
    } else if (!is.null(input$text) && input$choices6120.1 == "Выбор по номеру первичного документа") {
      	data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6120.1 == "Выбор по статье дохода") {
      	data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6120.1) && !any(is.na(input$dates6120.1)) && !is.null(input$text) && input$choices6120.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6120.1[1L])
      	to=as.Date(input$dates6120.1[2L])
      	if (from>to) to = from
      	selectdates6120.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_2 & data$df6120.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6120.1) && !any(is.na(input$dates6120.1)) && !is.null(input$text) && input$choices6120.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6120.1[1L])
      	to=as.Date(input$dates6120.1[2L])
      	if (from>to) to = from
      	selectdates6120.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.1_2 <- data$df6120.1[as.Date(data$df6120.1$"Дата операции") %in% selectdates6120.1_3 & data$df6120.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6120.1_4 <- unique(data$df6120.1$"Дата операции")
        data$df6120.1_2 <- data$df6120.1[data$df6120.1$"Дата операции" %in% selectdates6120.1_4, ]
    }
}
})

  output$table6120.1Item1 <- renderRHandsontable({
    
   data$df6120.1[, `Сальдо конечное` := data$df6120.1[[7]] + data$df6120.1[[8]] - data$df6120.1[[9]]]

    rhandsontable(data$df6120.1, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6120.1 <- renderUI({
    if (input$choices6120.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6120.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6120.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6120.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6120.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6120.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6120.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6120.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6120.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6120.1_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6120.1 <- downloadHandler(
    filename = function() { "df6120.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1, file)
  })

  output$download_df6120.1_2 <- downloadHandler(
    filename = function() { "df6120.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.1_2, file)
  })

#****************************************

#6120.2

observeEvent(input$dates6120.2, {
    start <- ymd(input$dates6120.2[[1]])
    end <- ymd(input$dates6120.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6120.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6120.2[[1]]
      r$end <- input$dates6120.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6120.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6120.2Item1)) {
    data$df6120.2 <- hot_to_r(input$table6120.2Item1) 

    if (!any(is.na(input$dates6120.2)) && input$choices6120.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6120.2[1L])
      	to=as.Date(input$dates6120.2[2L])
      	if (from>to) to = from
      	selectdates6120.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_1, ]
    } else if (!is.null(input$text) && input$choices6120.2 == "Выбор по номеру первичного документа") {
      	data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6120.2 == "Выбор по статье дохода") {
      	data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6120.2) && !any(is.na(input$dates6120.2)) && !is.null(input$text) && input$choices6120.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6120.2[1L])
      	to=as.Date(input$dates6120.2[2L])
      	if (from>to) to = from
      	selectdates6120.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_2 & data$df6120.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6120.2) && !any(is.na(input$dates6120.2)) && !is.null(input$text) && input$choices6120.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6120.2[1L])
      	to=as.Date(input$dates6120.2[2L])
      	if (from>to) to = from
      	selectdates6120.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6120.2_2 <- data$df6120.2[as.Date(data$df6120.2$"Дата операции") %in% selectdates6120.2_3 & data$df6120.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6120.2_4 <- unique(data$df6120.2$"Дата операции")
        data$df6120.2_2 <- data$df6120.2[data$df6120.2$"Дата операции" %in% selectdates6120.2_4, ]
    }
}
})

  output$table6120.2Item1 <- renderRHandsontable({
    
   data$df6120.2[, `Сальдо конечное` := data$df6120.2[[7]] + data$df6120.2[[8]] - data$df6120.2[[9]]]

    rhandsontable(data$df6120.2, colWidths = 150, height = 300, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6120.2 <- renderUI({
    if (input$choices6120.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6120.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6120.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6120.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6120.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6120.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6120.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6120.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6120.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6120.2_2, colWidths = 150, height = 300, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6120.2 <- downloadHandler(
    filename = function() { "df6120.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2, file)
  })

  output$download_df6120.2_2 <- downloadHandler(
    filename = function() { "df6120.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6120.2_2, file)
  })

}
shinyApp(ui, server)
