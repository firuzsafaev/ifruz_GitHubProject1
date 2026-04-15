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
	library(digest)
	library(base64enc)
	library(zip)
	
	# УЛУЧШЕННАЯ функция для генерации сложных паролей и кодов
	generate_complex_string <- function(length = 12) {
	  lowercase <- letters
	  uppercase <- LETTERS
	  digits <- 0:9
	  symbols <- c('!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+', 
	               '[', ']', '{', '}', '|', ';', ':', ',', '.', '<', '>', '/', '?')
	  
	  all_chars <- c(lowercase, uppercase, digits, symbols)
	  
	  part1 <- sample(lowercase, 1)
	  part2 <- sample(uppercase, 1)
	  part3 <- sample(digits, 1)
	  part4 <- sample(symbols, 1)
	  
	  rest <- sample(all_chars, length - 4, replace = TRUE)
	  
	  complex_string <- paste0(c(part1, part2, part3, part4, rest), collapse = "")
	  complex_string <- paste0(sample(strsplit(complex_string, "")[[1]]), collapse = "")
	  
	  return(complex_string)
	}
	
	# Упрощенная функция создания подключения
	create_database_connection <- function() {
	  tryCatch({
	    database_url <- Sys.getenv("DATABASE_URL")
	    
	    if (database_url == "") {
	      message("DATABASE_URL environment variable is empty")
	      return(NULL)
	    }
	    
	    message("Attempting to connect to database...")
	    
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

	# Глобальная переменная для хранения пользовательских данных между сессиями
	.local_users_data <- reactiveValues(
	  data = list()
	)
	
	# Функция для загрузки пользователей из локального файла
	load_local_users <- function() {
	  users_file <- "local_users_data.RData"
	  if (file.exists(users_file)) {
	    load(users_file)
	    message("Local users data loaded from file")
	    return(loaded_users)
	  }
	  return(list())
	}
	
	# Функция для сохранения пользователей в локальный файл
	save_local_users <- function(users_data) {
	  save(users_data, file = "local_users_data.RData")
	  message("Local users data saved to file")
	}
	
	# Инициализация локального хранилища при запуске
	initialize_local_storage <- function() {
	  users_data <- load_local_users()
	  .local_users_data$data <- users_data
	  message("Local storage initialized with ", length(users_data), " users")
	}
	
	# Вызов инициализации при загрузке приложения
	initialize_local_storage()
	
	# Функция получения или создания пользователя
	get_or_create_user_simple <- function(username, is_new_session = FALSE) {
	  tryCatch({
	    # Временно создаем пользователя локально, если база недоступна
	    if (is.null(create_database_connection())) {
	      message("Database connection not available, checking local storage for user: ", username)
	      
	      # Проверяем, есть ли пользователь в локальном хранилище
	      if (!is.null(.local_users_data$data) && !is.null(.local_users_data$data[[username]])) {
	        message("User found in local storage: ", username)
	        user_data <- .local_users_data$data[[username]]
	        # Возвращаем пользователя БЕЗ пароля при последующих запросах
	        return(list(
	          username = user_data$username,
	          is_initialized = user_data$is_initialized,
	          is_new = FALSE,
	          password = NULL
	        ))
	      }
	      
	      # Если пользователя нет в локальном хранилище, создаем его
	      if (is_new_session) {
	        message("Creating new user locally: ", username)
	        password <- generate_complex_string(12)
        
	        user_data <- list(
	          username = username,
	          password = password,
	          is_initialized = TRUE
	        )
	        
	        # Сохраняем в локальное хранилище
	        .local_users_data$data[[username]] <- user_data
	        save_local_users(.local_users_data$data)
	        
	        return(list(
	          username = username,
	          is_initialized = TRUE,
	          is_new = TRUE,
	          password = password
	        ))
	      } else {
	        return(list(
	          username = username,
	          is_initialized = FALSE,
	          is_new = FALSE,
	          password = NULL
	        ))
	      }
	    }
	    
	    # Если база доступна, работаем с ней
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
    
	    # Создаем таблицу users если она не существует
	    dbExecute(conn, "
	      CREATE TABLE IF NOT EXISTS users (
	        id SERIAL PRIMARY KEY,
	        username VARCHAR(255) UNIQUE NOT NULL,
	        password VARCHAR(255) NOT NULL,
	        is_initialized BOOLEAN DEFAULT FALSE,
	        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )
	    ")
	    
	    # Пытаемся получить пользователя
	    query <- "SELECT username, password, is_initialized 
	              FROM users 
	              WHERE username = $1"
	    
	    result <- dbGetQuery(conn, query, params = list(username))
    
	    if (nrow(result) == 0) {
	      if (is_new_session) {
	        message("User not found, creating new user: ", username)
	        
	        password <- generate_complex_string(12)
	        
	        insert_query <- "
	          INSERT INTO users (username, password, is_initialized, created_at) 
	          VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
	          RETURNING username, password, is_initialized
	        "
	        
	        result <- dbGetQuery(conn, insert_query, 
	                           list(username, password, TRUE))
	        
	        message("New user created in database: ", username)
	        dbDisconnect(conn)
	        
	        return(list(
	          username = result$username,
	          is_initialized = result$is_initialized,
	          is_new = TRUE,
	          password = password
	        ))
	      } else {
	        dbDisconnect(conn)
	        return(list(
	          username = username,
	          is_initialized = FALSE,
	          is_new = FALSE,
	          password = NULL
	        ))
	      }
	    } else {
	      message("User found in database: ", username)
	      dbDisconnect(conn)
      
	      return(list(
	        username = result$username,
	        is_initialized = result$is_initialized,
	        is_new = FALSE,
	        password = NULL
	      ))
	    }
	    
	  }, error = function(e) {
	    message("Error in get_or_create_user_simple: ", e$message)
	    
	    if (!is.null(.local_users_data$data) && !is.null(.local_users_data$data[[username]])) {
	      user_data <- .local_users_data$data[[username]]
	      return(list(
	        username = user_data$username,
	        is_initialized = user_data$is_initialized,
	        is_new = FALSE,
	        password = NULL
	      ))
	    }
    
	    if (is_new_session) {
	      message("Creating new user locally after error: ", username)
	      password <- generate_complex_string(12)
	      
	      user_data <- list(
	        username = username,
	        password = password,
	        is_initialized = TRUE
	      )
	      
	      .local_users_data$data[[username]] <- user_data
	      save_local_users(.local_users_data$data)
	      
	      return(list(
	        username = username,
	        is_initialized = TRUE,
	        is_new = TRUE,
	        password = password
	      ))
	    } else {
	      return(list(
	        username = username,
	        is_initialized = FALSE,
	        is_new = FALSE,
        	password = NULL
	      ))
	    }
	  })
	}
	
	# Функция проверки пароля пользователя
	check_user_password <- function(username, password) {
	  tryCatch({
	    if (!is.null(.local_users_data$data) && !is.null(.local_users_data$data[[username]])) {
	      stored_password <- .local_users_data$data[[username]]$password
	      return(identical(password, stored_password))
	    }
	    
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    query <- "SELECT password FROM users WHERE username = $1"
	    result <- dbGetQuery(conn, query, params = list(username))
	    
	    dbDisconnect(conn)
	    
	    if (nrow(result) == 0) {
	      return(FALSE)
	    }
    
	    return(identical(password, result$password[1]))
	    
	  }, error = function(e) {
	    message("Error in check_user_password: ", e$message)
	    return(FALSE)
	  })
	}
	
	#**********
	
	# Специализированные функции для 7010_1
	load_7010_1_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7010_1 data from: ", table_name, " for session: ", session_id)
  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Remove duplicates
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7010_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    # Delete only rows of current session that belong to current user
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
   
	    # Insert new data from current user
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, 
	      operation_description, accounting_method, 
	      initial_balance, debit, credit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      # Operation time: if exists - keep, otherwise set current time
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # operation_description
	        as.character(data[[5]][i] %||% NA),  # accounting_method
	        as.numeric(data[[6]][i] %||% 0),     # initial_balance
	        as.numeric(data[[7]][i] %||% 0),     # debit
	        as.numeric(data[[8]][i] %||% 0),     # credit
	        as.character(data[[9]][i] %||% NA),  # correspondence_debit
	        as.character(data[[10]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[11]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7010_1_data: ", e$message)
	    return(FALSE)
	  })
	}
	
	# Специализированные функции для 7110_1
	load_7110_1_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7110_1 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
        	        ORDER BY id"
	    }
    
	    query <- sprintf(query, table_name)
    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Remove duplicates
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7110_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
    
	    dbExecute(conn, "BEGIN")
    
	    # Delete only rows of current session that belong to current user
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
	   
	    # Insert new data from current user
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, 
	      operation_description, accounting_method, 
	      initial_balance, debit, credit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)",
	      table_name
	    )

	    for (i in 1:nrow(data)) {
	      # Operation time: if exists - keep, otherwise set current time
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # operation_description
	        as.character(data[[5]][i] %||% NA),  # accounting_method
	        as.numeric(data[[6]][i] %||% 0),     # initial_balance
	        as.numeric(data[[7]][i] %||% 0),     # debit
	        as.numeric(data[[8]][i] %||% 0),     # credit
	        as.character(data[[9]][i] %||% NA),  # correspondence_debit
	        as.character(data[[10]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[11]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7110_1_data: ", e$message)
	    return(FALSE)
	  })
	}
	
	# Специализированные функции для 7210_1
	load_7210_1_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7210_1 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, 
	                       operation_description, accounting_method, initial_balance, debit, credit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Remove duplicates
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7210_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    # Delete only rows of current session that belong to current user
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
   
	    # Insert new data from current user
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, 
	      operation_description, accounting_method, 
	      initial_balance, debit, credit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      # Operation time: if exists - keep, otherwise set current time
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # operation_description
	        as.character(data[[5]][i] %||% NA),  # accounting_method
	        as.numeric(data[[6]][i] %||% 0),     # initial_balance
	        as.numeric(data[[7]][i] %||% 0),     # debit
	        as.numeric(data[[8]][i] %||% 0),     # credit
	        as.character(data[[9]][i] %||% NA),  # correspondence_debit
	        as.character(data[[10]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[11]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7210_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	# Специализированные функции для 7310.1
	load_7310_1_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7310.1 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Удаляем дубликаты
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7310_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    # Удаляем только те строки текущей сессии, которые принадлежат текущему пользователю
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
	   
	    # Вставляем новые данные от текущего пользователя
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, expense_account, 
	      expense_period, operation_description, accounting_method, 
	      initial_balance, credit, debit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      # Время проводки: если уже есть – оставляем, иначе ставим текущее
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # expense_account
	        as.character(data[[5]][i] %||% NA),  # expense_period
	        as.character(data[[6]][i] %||% NA),  # operation_description
	        as.character(data[[7]][i] %||% NA),  # accounting_method
	        as.numeric(data[[8]][i] %||% 0),     # initial_balance
	        as.numeric(data[[9]][i] %||% 0),     # credit
	        as.numeric(data[[10]][i] %||% 0),    # debit
	        as.character(data[[11]][i] %||% NA), # correspondence_debit
	        as.character(data[[12]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[13]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7310_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	# Специализированные функции для 7310.2
	load_7310_2_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7310.2 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Удаляем дубликаты
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7310_2_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
	   
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, expense_account, 
	      expense_period, operation_description, accounting_method, 
	      initial_balance, credit, debit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),
	        time_val,
	        as.character(data[[3]][i] %||% NA),
	        as.character(data[[4]][i] %||% NA),
	        as.character(data[[5]][i] %||% NA),
	        as.character(data[[6]][i] %||% NA),
	        as.character(data[[7]][i] %||% NA),
	        as.numeric(data[[8]][i] %||% 0),
	        as.numeric(data[[9]][i] %||% 0),
	        as.numeric(data[[10]][i] %||% 0),
	        as.character(data[[11]][i] %||% NA),
	        as.character(data[[12]][i] %||% NA),
	        as.numeric(data[[13]][i] %||% 0)
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7310_2_data: ", e$message)
	    return(FALSE)
	  })
	}

	# Специализированные функции для 7320.1
	load_7320_1_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7320.1 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Удаляем дубликаты
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7320_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    # Удаляем только те строки текущей сессии, которые принадлежат текущему пользователю
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
	   
	    # Вставляем новые данные от текущего пользователя
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, expense_account, 
	      expense_period, operation_description, accounting_method, 
	      initial_balance, credit, debit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      # Время проводки: если уже есть – оставляем, иначе ставим текущее
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # expense_account
	        as.character(data[[5]][i] %||% NA),  # expense_period
	        as.character(data[[6]][i] %||% NA),  # operation_description
	        as.character(data[[7]][i] %||% NA),  # accounting_method
	        as.numeric(data[[8]][i] %||% 0),     # initial_balance
	        as.numeric(data[[9]][i] %||% 0),     # credit
	        as.numeric(data[[10]][i] %||% 0),    # debit
	        as.character(data[[11]][i] %||% NA), # correspondence_debit
	        as.character(data[[12]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[13]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7320_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	# Специализированные функции для 7320.2
	load_7320_2_data <- function(table_name, session_id, username = NULL) {
	  message("Loading 7320.2 data from: ", table_name, " for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    if (!is.null(username)) {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 AND username = $2 
	                ORDER BY id"
	    } else {
	      query <- "SELECT operation_date, operation_time, document_number, expense_account, 
	                       expense_period, operation_description, accounting_method, initial_balance, credit, debit,
	                       correspondence_debit, correspondence_credit, final_balance, username
	                FROM %s 
	                WHERE session_id = $1 
	                ORDER BY id"
	    }
	    
	    query <- sprintf(query, table_name)
	    
	    if (!is.null(username)) {
	      result <- dbGetQuery(conn, query, params = list(session_id, username))
	    } else {
	      result <- dbGetQuery(conn, query, params = list(session_id))
	    }
	    
	    # Удаляем дубликаты
	    if (!is.null(result) && nrow(result) > 0) {
	      result <- result[!duplicated(result), ]
	    }
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	save_7320_2_data <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    # Удаляем только те строки текущей сессии, которые принадлежат текущему пользователю
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1 
	      AND username = $2
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id, username))
	   
	    # Вставляем новые данные от текущего пользователя
	    insert_query <- sprintf(
	      "INSERT INTO %s
	      (session_id, username, operation_date, operation_time, document_number, expense_account, 
	      expense_period, operation_description, accounting_method, 
	      initial_balance, credit, debit, correspondence_debit, 
	      correspondence_credit, final_balance, updated_at)
	      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP)",
	      table_name
	    )
	
	    for (i in 1:nrow(data)) {
	      # Время проводки: если уже есть – оставляем, иначе ставим текущее
	      time_val <- as.character(data[[2]][i] %||% NA)
	      if (is.na(time_val) || time_val == "") {
	        time_val <- as.character(lubridate::now("Asia/Tashkent"))
	      }
	
	      dbExecute(conn, insert_query, list(
	        session_id,
	        if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	        as.character(data[[1]][i] %||% NA),  # operation_date
	        time_val,                            # operation_time
	        as.character(data[[3]][i] %||% NA),  # document_number
	        as.character(data[[4]][i] %||% NA),  # expense_account
	        as.character(data[[5]][i] %||% NA),  # expense_period
	        as.character(data[[6]][i] %||% NA),  # operation_description
	        as.character(data[[7]][i] %||% NA),  # accounting_method
	        as.numeric(data[[8]][i] %||% 0),     # initial_balance
	        as.numeric(data[[9]][i] %||% 0),     # credit
	        as.numeric(data[[10]][i] %||% 0),    # debit
	        as.character(data[[11]][i] %||% NA), # correspondence_debit
	        as.character(data[[12]][i] %||% NA), # correspondence_credit
	        as.numeric(data[[13]][i] %||% 0)     # final_balance
	      ))
	    }
	   
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_7320_2_data: ", e$message)
	    return(FALSE)
	  })
	}

	#**********

	# функция загрузки данных - удаляем дубликаты (ОБЩАЯ ДИСПЕТЧЕРИЗАЦИЯ)
	load_data_simple <- function(table_name, session_id, username = NULL) {
	  message("Loading data from: ", table_name, " for session: ", session_id)
  
	# ДИСПЕТЧЕРИЗАЦИЯ ПО ГРУППАМ ТАБЛИЦ
	  if (table_name %in% c("app_data_7010_1")) {
	    return(load_7010_1_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7110_1")) {
	    return(load_7110_1_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7210_1")) {
	    return(load_7210_1_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7310_1")) {
	    return(load_7310_1_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7310_2")) {
	    return(load_7310_2_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7320_1")) {
	    return(load_7320_1_data(table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7320_2")) {
	    return(load_7320_2_data(table_name, session_id, username))
	  } else {
	    return(NULL)
	  }
	}

	# Функция сохранения данных с информацией о пользователе (ОБЩАЯ ДИСПЕТЧЕРИЗАЦИЯ)
	save_data_simple <- function(data, table_name, session_id, username) {
	  if (is.null(data) || nrow(data) == 0) {
	    return(FALSE)
	  }
  
	  # ДИСПЕТЧЕРИЗАЦИЯ ПО ГРУППАМ ТАБЛИЦ
	  if (table_name %in% c("app_data_7010_1")) {
	    return(save_7010_1_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7110_1")) {
	    return(save_7110_1_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7210_1")) {
	    return(save_7210_1_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7310_1")) {
	    return(save_7310_1_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7310_2")) {
	    return(save_7310_2_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7320_1")) {
	    return(save_7320_1_data(data, table_name, session_id, username))
	  } else if (table_name %in% c("app_data_7320_2")) {
	    return(save_7320_2_data(data, table_name, session_id, username))
	  } else {
	    return(FALSE)
	  }
	}

	#*******

	# СПЕЦИАЛИЗИРОВАННЫЕ ФУНКЦИИ ДЛЯ ЗАГРУЗКИ ОБЪЕДИНЕННЫХ ДАННЫХ
	load_merged_7010_1_data <- function(table_name, session_id) {
	  message("Loading merged 7010_1 data for session: ", session_id)
  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, 
	                		operation_description, accounting_method)	
			operation_date, operation_time, document_number, 
			operation_description, accounting_method, 
			initial_balance, debit, credit,
			correspondence_debit, correspondence_credit,
			final_balance, username
	              FROM %s 
	              WHERE session_id = $1 
	              ORDER BY 	operation_date, operation_time, 
				document_number, operation_description, 
				accounting_method, initial_balance, 
				debit, credit, correspondence_debit,
				correspondence_credit, final_balance, 
				username, updated_at DESC, id DESC"
	    
	    query <- sprintf(query, table_name)
	    result <- dbGetQuery(conn, query, params = list(session_id))
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	load_merged_7110_1_data <- function(table_name, session_id) {
	  message("Loading merged 7110_1 data for session: ", session_id)
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, 
	                		operation_description, accounting_method)	
			operation_date, operation_time, document_number, 
			operation_description, accounting_method, 
			initial_balance, debit, credit,
			correspondence_debit, correspondence_credit,
			final_balance, username
	              FROM %s 
	              WHERE session_id = $1 
	              ORDER BY 	operation_date, operation_time, 
				document_number, operation_description, 
				accounting_method, initial_balance, 
				debit, credit, correspondence_debit,
				correspondence_credit, final_balance, 
				username, updated_at DESC, id DESC"
	    
	    query <- sprintf(query, table_name)
	    result <- dbGetQuery(conn, query, params = list(session_id))
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}
	
	load_merged_7210_1_data <- function(table_name, session_id) {
	  message("Loading merged 7210_1 data for session: ", session_id)
		  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, 
	                		operation_description, accounting_method)	
			operation_date, operation_time, document_number, 
			operation_description, accounting_method, 
			initial_balance, debit, credit,
			correspondence_debit, correspondence_credit,
			final_balance, username
	              FROM %s 
	              WHERE session_id = $1 
	              ORDER BY 	operation_date, operation_time, 
				document_number, operation_description, 
				accounting_method, initial_balance, 
				debit, credit, correspondence_debit,
				correspondence_credit, final_balance, 
				username, updated_at DESC, id DESC"
	    
	    query <- sprintf(query, table_name)
	    result <- dbGetQuery(conn, query, params = list(session_id))
	    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
    
	    if ("operation_date" %in% names(result)) {
	      result$operation_date <- as.character(result$operation_date)
	    }
	    if ("operation_time" %in% names(result)) {
	      result$operation_time <- as.character(result$operation_time)
	    }
	    
	    dbDisconnect(conn)
	    return(result)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}

     load_merged_7310_1_data <- function(table_name, session_id) {
	message("Loading merged 7310.1 data for session: ", session_id)
	
	conn <- NULL
	tryCatch({
		conn <- create_database_connection()
		if (is.null(conn)) {
			return(NULL)
		}
		
		query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, expense_account, 
		            expense_period, operation_description, accounting_method)
		          operation_date, operation_time, document_number, expense_account, 
		          expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		          correspondence_debit, correspondence_credit, final_balance, username
		          FROM %s 
		          WHERE session_id = $1 
		          ORDER BY operation_date, operation_time, document_number, expense_account, 
		                   expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		                   correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC"
		
		query <- sprintf(query, table_name)
		result <- dbGetQuery(conn, query, params = list(session_id))
		
		if (nrow(result) == 0) {
			dbDisconnect(conn)
			return(NULL)
		}
		
		if ("operation_date" %in% names(result)) {
			result$operation_date <- as.character(result$operation_date)
		}
		if ("operation_time" %in% names(result)) {
			result$operation_time <- as.character(result$operation_time)
		}
		
		dbDisconnect(conn)
		return(result)
		
	}, error = function(e) {
		if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
		return(NULL)
	})
     }

     load_merged_7310_2_data <- function(table_name, session_id) {
	message("Loading merged 7310.2 data for session: ", session_id)
	
	conn <- NULL
	tryCatch({
		conn <- create_database_connection()
		if (is.null(conn)) {
			return(NULL)
		}
		
		query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, expense_account, 
		            expense_period, operation_description, accounting_method)
		          operation_date, operation_time, document_number, expense_account, 
		          expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		          correspondence_debit, correspondence_credit, final_balance, username
		          FROM %s 
		          WHERE session_id = $1 
		          ORDER BY operation_date, operation_time, document_number, expense_account, 
		                   expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		                   correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC"
		
		query <- sprintf(query, table_name)
		result <- dbGetQuery(conn, query, params = list(session_id))
		
		if (nrow(result) == 0) {
			dbDisconnect(conn)
			return(NULL)
		}
		
		if ("operation_date" %in% names(result)) {
			result$operation_date <- as.character(result$operation_date)
		}
		if ("operation_time" %in% names(result)) {
			result$operation_time <- as.character(result$operation_time)
		}
		
		dbDisconnect(conn)
		return(result)
		
	}, error = function(e) {
		if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
		return(NULL)
	})
     }

     load_merged_7320_1_data <- function(table_name, session_id) {
	message("Loading merged 7320.1 data for session: ", session_id)
	
	conn <- NULL
	tryCatch({
		conn <- create_database_connection()
		if (is.null(conn)) {
			return(NULL)
		}
		
		query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, expense_account, 
		            expense_period, operation_description, accounting_method)
		          operation_date, operation_time, document_number, expense_account, 
		          expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		          correspondence_debit, correspondence_credit, final_balance, username
		          FROM %s 
		          WHERE session_id = $1 
		          ORDER BY operation_date, operation_time, document_number, expense_account, 
		                   expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		                   correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC"
		
		query <- sprintf(query, table_name)
		result <- dbGetQuery(conn, query, params = list(session_id))
		
		if (nrow(result) == 0) {
			dbDisconnect(conn)
			return(NULL)
		}
		
		if ("operation_date" %in% names(result)) {
			result$operation_date <- as.character(result$operation_date)
		}
		if ("operation_time" %in% names(result)) {
			result$operation_time <- as.character(result$operation_time)
		}
		
		dbDisconnect(conn)
		return(result)
		
	}, error = function(e) {
		if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
		return(NULL)
	})
     }

     load_merged_7320_2_data <- function(table_name, session_id) {
	message("Loading merged 7320.2 data for session: ", session_id)
	
	conn <- NULL
	tryCatch({
		conn <- create_database_connection()
		if (is.null(conn)) {
			return(NULL)
		}
		
		query <- "SELECT DISTINCT ON (operation_date, operation_time, document_number, expense_account, 
		            expense_period, operation_description, accounting_method)
		          operation_date, operation_time, document_number, expense_account, 
		          expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		          correspondence_debit, correspondence_credit, final_balance, username
		          FROM %s 
		          WHERE session_id = $1 
		          ORDER BY operation_date, operation_time, document_number, expense_account, 
		                   expense_period, operation_description, accounting_method, initial_balance, credit, debit,
		                   correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC"
		
		query <- sprintf(query, table_name)
		result <- dbGetQuery(conn, query, params = list(session_id))
		
		if (nrow(result) == 0) {
			dbDisconnect(conn)
			return(NULL)
		}
		
		if ("operation_date" %in% names(result)) {
			result$operation_date <- as.character(result$operation_date)
		}
		if ("operation_time" %in% names(result)) {
			result$operation_time <- as.character(result$operation_time)
		}
		
		dbDisconnect(conn)
		return(result)
		
	}, error = function(e) {
		if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
		return(NULL)
	})
     }

	#*******

	# Функция загрузки ОБЪЕДИНЕННЫХ данных для сессии (всех пользователей, но без дубликатов) (ОБЩАЯ ДИСПЕТЧЕРИЗАЦИЯ)
	load_merged_session_data <- function(table_name, session_id) {
	  message("Loading merged data for session: ", session_id)
  
	# ДИСПЕТЧЕРИЗАЦИЯ ПО ГРУППАМ ТАБЛИЦ
	if (table_name %in% c("app_data_7010_1")) {
	    return(load_merged_7010_1_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7110_1")) {
	    return(load_merged_7110_1_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7210_1")) {
	    return(load_merged_7210_1_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7310_1")) {
	    return(load_merged_7310_1_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7310_2")) {
	    return(load_merged_7310_2_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7320_1")) {
	    return(load_merged_7320_1_data(table_name, session_id))
	  } else if (table_name %in% c("app_data_7320_2")) {
	    return(load_merged_7320_2_data(table_name, session_id))
	  } else {
	    return(NULL)
	  }
	}

	#*****

	# СПЕЦИАЛИЗИРОВАННЫЕ ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ОБЩИХ ДАННЫХ
	save_general_7010_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    # If data is NULL, delete all session records
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    # Insert new data
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, 
		document_number, operation_description, accounting_method, 
	        initial_balance, debit, credit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        # Operation time: if already exists - keep, otherwise set current time
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),  # operation_date
	          time_val,                            # operation_time
	          as.character(data[[3]][i] %||% NA),  # document_number
	          as.character(data[[4]][i] %||% NA),  # operation_description
	          as.character(data[[5]][i] %||% NA),  # accounting_method
	          as.numeric(data[[6]][i] %||% 0),     # initial_balance
	          as.numeric(data[[7]][i] %||% 0),     # debit
	          as.numeric(data[[8]][i] %||% 0),     # credit
	          as.character(data[[9]][i] %||% NA),  # correspondence_debit
	          as.character(data[[10]][i] %||% NA), # correspondence_credit
	          as.numeric(data[[11]][i] %||% 0)     # final_balance
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7010_1_data: ", e$message)
	    return(FALSE)
	  })
	}
	
	save_general_7110_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    # If data is NULL, delete all session records
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    # Insert new data
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, 
		document_number, operation_description, accounting_method, 
	        initial_balance, debit, credit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        # Operation time: if already exists - keep, otherwise set current time
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),  # operation_date
	          time_val,                            # operation_time
	          as.character(data[[3]][i] %||% NA),  # document_number
	          as.character(data[[4]][i] %||% NA),  # operation_description
	          as.character(data[[5]][i] %||% NA),  # accounting_method
	          as.numeric(data[[6]][i] %||% 0),     # initial_balance
	          as.numeric(data[[7]][i] %||% 0),     # debit
	          as.numeric(data[[8]][i] %||% 0),     # credit
	          as.character(data[[9]][i] %||% NA),  # correspondence_debit
	          as.character(data[[10]][i] %||% NA), # correspondence_credit
	          as.numeric(data[[11]][i] %||% 0)     # final_balance
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7110_1_data: ", e$message)
	    return(FALSE)
	  })
	}
	
	save_general_7210_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    # If data is NULL, delete all session records
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    # Insert new data
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, 
		document_number, operation_description, accounting_method, 
	        initial_balance, debit, credit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        # Operation time: if already exists - keep, otherwise set current time
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),  # operation_date
	          time_val,                            # operation_time
	          as.character(data[[3]][i] %||% NA),  # document_number
	          as.character(data[[4]][i] %||% NA),  # operation_description
	          as.character(data[[5]][i] %||% NA),  # accounting_method
	          as.numeric(data[[6]][i] %||% 0),     # initial_balance
	          as.numeric(data[[7]][i] %||% 0),     # debit
	          as.numeric(data[[8]][i] %||% 0),     # credit
	          as.character(data[[9]][i] %||% NA),  # correspondence_debit
	          as.character(data[[10]][i] %||% NA), # correspondence_credit
	          as.numeric(data[[11]][i] %||% 0)     # final_balance
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7210_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	save_general_7310_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    # Если данные NULL, удаляем все записи сессии
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    # Вставляем новые данные (общие для всех пользователей)
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, document_number, expense_account, 
	        expense_period, operation_description, accounting_method, 
	        initial_balance, credit, debit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        # Время проводки: если уже есть – оставляем, иначе ставим текущее
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),  # operation_date
	          time_val,                            # operation_time
	          as.character(data[[3]][i] %||% NA),  # document_number
	          as.character(data[[4]][i] %||% NA),  # expense_account
	          as.character(data[[5]][i] %||% NA),  # expense_period
	          as.character(data[[6]][i] %||% NA),  # operation_description
	          as.character(data[[7]][i] %||% NA),  # accounting_method
	          as.numeric(data[[8]][i] %||% 0),     # initial_balance
	          as.numeric(data[[9]][i] %||% 0),     # credit
	          as.numeric(data[[10]][i] %||% 0),    # debit
	          as.character(data[[11]][i] %||% NA), # correspondence_debit
	          as.character(data[[12]][i] %||% NA), # correspondence_credit
	          as.numeric(data[[13]][i] %||% 0)     # final_balance
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7310_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	save_general_7310_2_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, document_number, expense_account, 
	        expense_period, operation_description, accounting_method, 
	        initial_balance, credit, debit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),
	          time_val,
	          as.character(data[[3]][i] %||% NA),
	          as.character(data[[4]][i] %||% NA),
	          as.character(data[[5]][i] %||% NA),
	          as.character(data[[6]][i] %||% NA),
	          as.character(data[[7]][i] %||% NA),
	          as.numeric(data[[8]][i] %||% 0),
	          as.numeric(data[[9]][i] %||% 0),
	          as.numeric(data[[10]][i] %||% 0),
	          as.character(data[[11]][i] %||% NA),
	          as.character(data[[12]][i] %||% NA),
	          as.numeric(data[[13]][i] %||% 0)
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7310_2_data: ", e$message)
	    return(FALSE)
	  })
	}

	save_general_7320_1_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    # Если данные NULL, удаляем все записи сессии
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    # Вставляем новые данные (общие для всех пользователей)
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, document_number, expense_account, 
	        expense_period, operation_description, accounting_method, 
	        initial_balance, credit, debit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        # Время проводки: если уже есть – оставляем, иначе ставим текущее
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),  # operation_date
	          time_val,                            # operation_time
	          as.character(data[[3]][i] %||% NA),  # document_number
	          as.character(data[[4]][i] %||% NA),  # expense_account
	          as.character(data[[5]][i] %||% NA),  # expense_period
	          as.character(data[[6]][i] %||% NA),  # operation_description
	          as.character(data[[7]][i] %||% NA),  # accounting_method
	          as.numeric(data[[8]][i] %||% 0),     # initial_balance
	          as.numeric(data[[9]][i] %||% 0),     # credit
	          as.numeric(data[[10]][i] %||% 0),    # debit
	          as.character(data[[11]][i] %||% NA), # correspondence_debit
	          as.character(data[[12]][i] %||% NA), # correspondence_credit
	          as.numeric(data[[13]][i] %||% 0)     # final_balance
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7320_1_data: ", e$message)
	    return(FALSE)
	  })
	}

	save_general_7320_2_data <- function(data, table_name, session_id, username) {
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    data <- data.frame()
	  }
	  
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      message("Failed to connect to database")
	      return(FALSE)
	    }
	    
	    dbExecute(conn, "BEGIN")
	    
	    message(paste("Saving data to", table_name, "in session", session_id))
	    message(paste("Number of rows to save:", nrow(data)))
	    
	    delete_query <- sprintf("
	      DELETE FROM %s
	      WHERE session_id = $1
	    ", table_name)
	    dbExecute(conn, delete_query, list(session_id))
	    message("Deleted all data for session: ", session_id)
	    
	    if (nrow(data) > 0) {
	      insert_query <- sprintf(
	        "INSERT INTO %s
	        (session_id, username, operation_date, operation_time, document_number, expense_account, 
	        expense_period, operation_description, accounting_method, 
	        initial_balance, credit, debit, correspondence_debit, 
	        correspondence_credit, final_balance)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
	        table_name
	      )
	      
	      for (i in 1:nrow(data)) {
	        time_val <- as.character(data[[2]][i] %||% NA)
	        if (is.na(time_val) || time_val == "") {
	          time_val <- as.character(lubridate::now("Asia/Tashkent"))
	        }
	        
	        dbExecute(conn, insert_query, list(
	          session_id,
	          if (!is.null(data$Пользователь[i])) as.character(data$Пользователь[i]) else username,
	          as.character(data[[1]][i] %||% NA),
	          time_val,
	          as.character(data[[3]][i] %||% NA),
	          as.character(data[[4]][i] %||% NA),
	          as.character(data[[5]][i] %||% NA),
	          as.character(data[[6]][i] %||% NA),
	          as.character(data[[7]][i] %||% NA),
	          as.numeric(data[[8]][i] %||% 0),
	          as.numeric(data[[9]][i] %||% 0),
	          as.numeric(data[[10]][i] %||% 0),
	          as.character(data[[11]][i] %||% NA),
	          as.character(data[[12]][i] %||% NA),
	          as.numeric(data[[13]][i] %||% 0)
	        ))
	      }
	      message("Inserted ", nrow(data), " rows for session: ", session_id)
	    } else {
	      message("No new data to insert for session: ", session_id)
	    }
	    
	    dbExecute(conn, "COMMIT")
	    dbDisconnect(conn)
	    message("Successfully saved data for session: ", session_id)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) {
	      try(dbExecute(conn, "ROLLBACK"), silent = TRUE)
	      dbDisconnect(conn)
	    }
	    message("Error in save_general_7320_2_data: ", e$message)
	    return(FALSE)
	  })
	}

	#*****

	# Функция сохранения данных – с опцией per‑user
	save_data_general <- function(data, table_name, session_id, username) {

	  # Original general‑save logic (deletes ALL session data)
	  if (is.null(data)) {
	    message("No data to save for table: ", table_name)
	    data <- data.frame()
	  }
  
	# ДИСПЕТЧЕРИЗАЦИЯ ПО ГРУППАМ ТАБЛИЦ (unchanged)
	if (table_name %in% c("app_data_7010_1")) {
	    success <- save_general_7010_1_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7110_1")) {
	    success <- save_general_7110_1_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7210_1")) {
	    success <- save_general_7210_1_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7310_1")) {
	    success <- save_general_7310_1_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7310_2")) {
	    success <- save_general_7310_2_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7320_1")) {
	    success <- save_general_7320_1_data(data, table_name, session_id, username)
	  } else if (table_name %in% c("app_data_7320_2")) {
	    success <- save_general_7320_2_data(data, table_name, session_id, username)
	  } else {
	    return(FALSE)
	  }
  
	  if (success) {
	    # Обновляем метку времени обновления (unchanged)
	    conn <- NULL
	    tryCatch({
	      conn <- create_database_connection()
	      if (is.null(conn)) {
	        message("Failed to connect to database for timestamp update")
	        return(TRUE)
	      }
	      
	      update_query <- "
	        INSERT INTO updates (session_id, last_update, updated_by) 
	        VALUES ($1, CURRENT_TIMESTAMP, $2)
	        ON CONFLICT (session_id) 
	        DO UPDATE SET last_update = CURRENT_TIMESTAMP, updated_by = $2
	      "
	      dbExecute(conn, update_query, list(session_id, username))
	      message("Updated timestamp for session: ", session_id, " by user: ", username)
	      
	      dbDisconnect(conn)
	    }, error = function(e) {
	      if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	      message("Error updating timestamp: ", e$message)
	    })
	  }
	  
	  return(success)
	}

	get_previous_sessions <- function() {
	  current_date <- Sys.Date()
	  previous_dates <- seq(current_date - 31, current_date - 1, by = "day")
	  sessions <- paste("сессия", format(previous_dates, "%Y-%m-%d"))
	  return(sessions)
	}
	
	get_current_session_id <- function() {
	  current_date <- Sys.Date()
	  session_id <- paste("сессия", format(current_date, "%Y-%m-%d"))
	  return(session_id)
	}
	
	get_last_update <- function(session_id) {
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(NULL)
	    }
	    
	    query <- "SELECT last_update, updated_by FROM updates WHERE session_id = $1"
	    result <- dbGetQuery(conn, query, params = list(session_id))
    
	    if (nrow(result) == 0) {
	      dbDisconnect(conn)
	      return(NULL)
	    }
	    
	    dbDisconnect(conn)
	    return(list(
	      timestamp = result$last_update[1],
	      user = result$updated_by[1]
	    ))
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(NULL)
	  })
	}

	# СПЕЦИАЛИЗИРОВАННЫЕ ФУНКЦИИ ДЛЯ ИНИЦИАЛИЗАЦИИ ТАБЛИЦ

	initialize_7010_1_table <- function(conn) {
	  tables <- list(
	    app_data_7010_1 = "
	      CREATE TABLE IF NOT EXISTS app_data_7010_1 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }
	  
	  # Add columns if they don't exist
	  alter_queries <- list(
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7010_1 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  # Create indexes for better performance on merged queries
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7010_1_key ON app_data_7010_1 (operation_date, operation_time, document_number, 
	               operation_description, accounting_method, initial_balance, debit, credit, 
	               correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}
	
	initialize_7110_1_table <- function(conn) {
	  tables <- list(
	    app_data_7110_1 = "
	      CREATE TABLE IF NOT EXISTS app_data_7110_1 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }
	  
	  # Add columns if they don't exist
	  alter_queries <- list(
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7110_1 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  # Create indexes for better performance on merged queries
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7110_1_key ON app_data_7110_1 (operation_date, operation_time, document_number, 
	               operation_description, accounting_method, initial_balance, debit, credit, 
	               correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}
	
	initialize_7210_1_table <- function(conn) {
	  tables <- list(
	    app_data_7210_1 = "
	      CREATE TABLE IF NOT EXISTS app_data_7210_1 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }
	  
	  # Add columns if they don't exist
	  alter_queries <- list(
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7210_1 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  # Create indexes for better performance on merged queries
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7210_1_key ON app_data_7210_1 (operation_date, operation_time, document_number, 
	               operation_description, accounting_method, initial_balance, debit, credit, 
	               correspondence_debit, correspondence_credit, final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}

	initialize_7310_1_table <- function(conn) {
	  tables <- list(
	    app_data_7310_1 = "
	      CREATE TABLE IF NOT EXISTS app_data_7310_1 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        expense_account VARCHAR(255),
	        expense_period VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }

	  alter_queries <- list(
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS expense_account VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS expense_period VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7310_1 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7310_1_key ON app_data_7310_1 (operation_date, operation_time, document_number, 
						expense_account, expense_period, operation_description, accounting_method, 
						initial_balance, credit, debit, correspondence_debit, correspondence_credit, 
						final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}

	initialize_7310_2_table <- function(conn) {
	  tables <- list(
	    app_data_7310_2 = "
	      CREATE TABLE IF NOT EXISTS app_data_7310_2 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        expense_account VARCHAR(255),
	        expense_period VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }

	  alter_queries <- list(
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS expense_account VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS expense_period VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7310_2 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7310_2_key ON app_data_7310_2 (operation_date, operation_time, document_number, 
						expense_account, expense_period, operation_description, accounting_method, 
						initial_balance, credit, debit, correspondence_debit, correspondence_credit, 
						final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}

	initialize_7320_1_table <- function(conn) {
	  tables <- list(
	    app_data_7320_1 = "
	      CREATE TABLE IF NOT EXISTS app_data_7320_1 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        expense_account VARCHAR(255),
	        expense_period VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }

	  alter_queries <- list(
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS expense_account VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS expense_period VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7320_1 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7320_1_key ON app_data_7320_1 (operation_date, operation_time, document_number, 
						expense_account, expense_period, operation_description, accounting_method, 
						initial_balance, credit, debit, correspondence_debit, correspondence_credit, 
						final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}

	initialize_7320_2_table <- function(conn) {
	  tables <- list(
	    app_data_7320_2 = "
	      CREATE TABLE IF NOT EXISTS app_data_7320_2 (
	        id SERIAL PRIMARY KEY,
	        session_id VARCHAR(255) NOT NULL,
	        operation_date DATE,
	        operation_time TIMESTAMP,
	        document_number VARCHAR(255),
	        expense_account VARCHAR(255),
	        expense_period VARCHAR(255),
	        operation_description TEXT,
	        accounting_method VARCHAR(255),
	        initial_balance NUMERIC DEFAULT 0,
	        credit NUMERIC DEFAULT 0,
	        debit NUMERIC DEFAULT 0,
	        correspondence_debit VARCHAR(255),
	        correspondence_credit VARCHAR(255),
	        final_balance NUMERIC DEFAULT 0,
	        username VARCHAR(255),
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )"
	  )
	  
	  for (table_name in names(tables)) {
	    dbExecute(conn, tables[[table_name]])
	  }

	  alter_queries <- list(
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS operation_date DATE",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS operation_time TIMESTAMP",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS document_number VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS expense_account VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS expense_period VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS operation_description TEXT",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS accounting_method VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS initial_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS credit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS debit NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS correspondence_debit VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS correspondence_credit VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS final_balance NUMERIC DEFAULT 0",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS username VARCHAR(255)",
	    "ALTER TABLE app_data_7320_2 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
	  )
	  
	  for (alter_query in alter_queries) {
	    try(dbExecute(conn, alter_query), silent = TRUE)
	  }
	  
	  index_queries <- list(
	    "CREATE INDEX IF NOT EXISTS idx_7320_2_key ON app_data_7320_2 (operation_date, operation_time, document_number, 
						expense_account, expense_period, operation_description, accounting_method, 
						initial_balance, credit, debit, correspondence_debit, correspondence_credit, 
						final_balance, username, updated_at DESC, id DESC)"
	  )
	  
	  for (index_query in index_queries) {
	    try(dbExecute(conn, index_query), silent = TRUE)
	  }
	  
	  return(TRUE)
	}

	#*******

	initialize_database_simple <- function() {
	  conn <- NULL
	  tryCatch({
	    conn <- create_database_connection()
	    if (is.null(conn)) {
	      return(FALSE)
	    }
    
	    # Инициализируем таблицы для каждой группы

	    initialize_7010_1_table(conn)
	    initialize_7110_1_table(conn)
	    initialize_7210_1_table(conn)
	    initialize_7310_1_table(conn)
	    initialize_7310_2_table(conn)
	    initialize_7320_1_table(conn)
	    initialize_7320_2_table(conn)
    
	    # Инициализируем общие таблицы
	    dbExecute(conn, "
	      CREATE TABLE IF NOT EXISTS updates (
	        session_id VARCHAR(255) PRIMARY KEY,
	        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	        updated_by VARCHAR(255)
	      )")
	    
	    dbExecute(conn, "
	      CREATE TABLE IF NOT EXISTS users (
	        id SERIAL PRIMARY KEY,
	        username VARCHAR(255) UNIQUE NOT NULL,
	        password VARCHAR(255) NOT NULL,
	        is_initialized BOOLEAN DEFAULT FALSE,
	        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	      )")
	    
	    # Добавляем столбец updated_by, если его нет
	    try(dbExecute(conn, "ALTER TABLE updates ADD COLUMN IF NOT EXISTS updated_by VARCHAR(255)"), silent = TRUE)
	    
	    dbDisconnect(conn)
	    return(TRUE)
	    
	  }, error = function(e) {
	    if (!is.null(conn)) try(dbDisconnect(conn), silent = TRUE)
	    return(FALSE)
	  })
	}
	
	test_database_connection <- function() {
	  conn <- create_database_connection()
	  if (!is.null(conn)) {
	    dbDisconnect(conn)
	    return(TRUE)
	  }
	  return(FALSE)
	}
	
	`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y
	
	validate_operation_dates <- function(df, date_col = "Дата операции") {
	  tryCatch({
	    if (!date_col %in% names(df)) {
	      stop(paste("Столбец", date_col, "не найден в данных"))
	    }
	    
	    modified_df <- copy(df)
	    error_messages <- character(0)
	    error_rows <- integer(0)
	    
	    dates <- tryCatch({
	      as.Date(modified_df[[date_col]], format = "%Y-%m-%d")
	    }, error = function(e) {
	      stop("Некорректный формат даты. Используйте формат ГГГГ-ММ-ДД")
	    })
	    
	    future_dates <- which(!is.na(dates) & dates > Sys.Date())
	    if (length(future_dates) > 0) {
	      error_messages <- c(error_messages, 
	                          paste("Ошибка в строке (строках) ", paste(future_dates, collapse = ", "), 
	                                ": Дата операции не может быть в будущем"))
	      error_rows <- union(error_rows, future_dates)
	    }
    
	    if (nrow(modified_df) > 1) {
	      non_na_indices <- which(!is.na(dates))
	      
	      if (length(non_na_indices) > 1) {
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
	    
	    if (length(error_messages) > 0) {
	      modified_df[error_rows, (date_col) := NA_character_]
	      final_message <- paste(error_messages, collapse = "\n\n")
	      shinyalert("Ошибка в дате операции", final_message, type = "error")
	      return(list(valid = FALSE, df = modified_df))
	    }
	    
	    return(list(valid = TRUE, df = modified_df))
	  }, error = function(e) {
	    shinyalert("Ошибка в дате операции", e$message, type = "error")
	    return(list(valid = FALSE, df = df))
	  })
	}

	# БАЗОВЫЙ КОД

	#ОСВ: 7000

	DF7010_3 <- data.table(
		"Счет (субчет)" = as.character(c("7010.Себестоимость реализованной продукции и оказанных услуг")),	
		"Сальдо начальное" = as.numeric(c(0)),
		"Дебет" = as.numeric(c(0)),
		"Кредит" = as.numeric(c(0)),
		"Сальдо конечное" = as.numeric(c(0)),
		stringsAsFactors = FALSE)

	#7010

	DF7010_1 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),				#row 6
      		"Дебет" = as.numeric(NA),					#row 7
      		"Кредит" = as.numeric(NA),					#row 8
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7010_2 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#ОСВ: 7100

	DF7110_3 <- data.table(
		"Счет (субчет)" = as.character(c("7110.Расходы по реализации продукции и оказанию услуг")),	
		"Сальдо начальное" = as.numeric(c(0)),
		"Дебет" = as.numeric(c(0)),
		"Кредит" = as.numeric(c(0)),
		"Сальдо конечное" = as.numeric(c(0)),
		stringsAsFactors = FALSE)

	#7110

	DF7110_1 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),				#row 6
      		"Дебет" = as.numeric(NA),					#row 7
      		"Кредит" = as.numeric(NA),					#row 8
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7110_2 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#ОСВ: 7200

	DF7210_3 <- data.table(
		"Счет (субчет)" = as.character(c("7210.Административные расходы")),	
		"Сальдо начальное" = as.numeric(c(0)),
		"Дебет" = as.numeric(c(0)),
		"Кредит" = as.numeric(c(0)),
		"Сальдо конечное" = as.numeric(c(0)),
		stringsAsFactors = FALSE)

	#7210

	DF7210_1 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),				#row 6
      		"Дебет" = as.numeric(NA),					#row 7
      		"Кредит" = as.numeric(NA),					#row 8
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7210_2 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#7310.1

	DF7310.1 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),				#row 8
      		"Кредит" = as.numeric(0),					#row 9
      		"Дебет" = as.numeric(0),					#row 10
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7310.1_2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),
      		"Кредит" = as.numeric(0), 
      		"Дебет" = as.numeric(0),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#7310.2

	DF7310.2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),				#row 8
      		"Кредит" = as.numeric(0),					#row 9
      		"Дебет" = as.numeric(0),					#row 10
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7310.2_2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),
      		"Кредит" = as.numeric(0), 
      		"Дебет" = as.numeric(0),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#7320.1

	DF7320.1 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),				#row 8
      		"Кредит" = as.numeric(0),					#row 9
      		"Дебет" = as.numeric(0),					#row 10
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7320.1_2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),
      		"Кредит" = as.numeric(0), 
      		"Дебет" = as.numeric(0),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#7320.2

	DF7320.2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),				#row 8
      		"Кредит" = as.numeric(0),					#row 9
      		"Дебет" = as.numeric(0),					#row 10
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF7320.2_2 <- data.table(
      		"Дата операции" = as.character(NA),
 		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Счет № статьи расхода" = as.character(NA),
      		"Период расхода" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
      		"Сальдо начальное" = as.numeric(0),
      		"Кредит" = as.numeric(0), 
      		"Дебет" = as.numeric(0),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(0),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	#*********

	# Таблицы для панели "Управление данными"

	# Пустые таблицы для панели "Управление данными"
	DF_empty_debit <- data.table(
	  "Дата операции" = as.character(NA),
	  "Время проводки" = as.character(NA),
	  "Номер первичного документа" = as.character(NA),
	  "Содержание операции" = as.character(NA),
	  "Метод учета" = as.character(NA),
	  "Сальдо начальное" = as.numeric(NA),
	  "Кредит" = as.numeric(NA),
	  "Дебет" = as.numeric(NA),
	  "Счет № (дебет)" = as.character(NA),
	  "Счет № (кредит)" = as.character(NA),
	  "Сальдо конечное" = as.numeric(NA),
	  "Пользователь" = as.character(NA),
	  stringsAsFactors = FALSE)

	DF_empty_credit <- copy(DF_empty_debit)

	# Пустые таблицы для 7010
	DF_empty_debit_7010 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF_empty_credit_7010 <- copy(DF_empty_debit_7010)

	# Пустые таблицы для 7110
	DF_empty_debit_7110 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF_empty_credit_7110 <- copy(DF_empty_debit_7110)

	# Пустые таблицы для 7210
	DF_empty_debit_7210 <- data.table(
      		"Дата операции" = as.character(NA),
		"Время проводки" = as.character(NA),
      		"Учетный номер" = as.character(NA),
      		"Содержание операции" = as.character(NA),
      		"Метод учета" = as.character(NA),
		"Сальдо начальное" = as.numeric(NA),
      		"Дебет" = as.numeric(NA),
      		"Кредит" = as.numeric(NA),
     		"Счет № (дебет)" = as.character(NA),
      		"Счет № (кредит)" = as.character(NA),
      		"Сальдо конечное" = as.numeric(NA),
		"Пользователь" = as.character(NA),
                stringsAsFactors = FALSE)

	DF_empty_credit_7210 <- copy(DF_empty_debit_7210)

	# Пустые таблицы для 7310.1
	DF_empty_debit_7310_1 <- data.table(
	      "Дата операции" = as.character(NA),
	      "Время проводки" = as.character(NA),
	      "Учетный номер" = as.character(NA),
	      "Счет № статьи расхода" = as.character(NA),
	      "Период расхода" = as.character(NA),
	      "Содержание операции" = as.character(NA),
	      "Метод учета" = as.character(NA),
	      "Сальдо начальное" = as.numeric(0),
	      "Кредит" = as.numeric(0),
	      "Дебет" = as.numeric(0),
	      "Счет № (дебет)" = as.character(NA),
	      "Счет № (кредит)" = as.character(NA),
	      "Сальдо конечное" = as.numeric(0),
	      "Пользователь" = as.character(NA),
	      stringsAsFactors = FALSE)

	DF_empty_credit_7310_1 <- copy(DF_empty_debit_7310_1)

	# Пустые таблицы для 7310.2
	DF_empty_debit_7310_2 <- data.table(
	      "Дата операции" = as.character(NA),
	      "Время проводки" = as.character(NA),
	      "Учетный номер" = as.character(NA),
	      "Счет № статьи расхода" = as.character(NA),
	      "Период расхода" = as.character(NA),
	      "Содержание операции" = as.character(NA),
	      "Метод учета" = as.character(NA),
	      "Сальдо начальное" = as.numeric(0),
	      "Кредит" = as.numeric(0),
	      "Дебет" = as.numeric(0),
	      "Счет № (дебет)" = as.character(NA),
	      "Счет № (кредит)" = as.character(NA),
	      "Сальдо конечное" = as.numeric(0),
	      "Пользователь" = as.character(NA),
	      stringsAsFactors = FALSE)

	DF_empty_credit_7310_2 <- copy(DF_empty_debit_7310_2)

	# Пустые таблицы для 7320.1
	DF_empty_debit_7320_1 <- data.table(
	      "Дата операции" = as.character(NA),
	      "Время проводки" = as.character(NA),
	      "Учетный номер" = as.character(NA),
	      "Счет № статьи расхода" = as.character(NA),
	      "Период расхода" = as.character(NA),
	      "Содержание операции" = as.character(NA),
	      "Метод учета" = as.character(NA),
	      "Сальдо начальное" = as.numeric(0),
	      "Кредит" = as.numeric(0),
	      "Дебет" = as.numeric(0),
	      "Счет № (дебет)" = as.character(NA),
	      "Счет № (кредит)" = as.character(NA),
	      "Сальдо конечное" = as.numeric(0),
	      "Пользователь" = as.character(NA),
	      stringsAsFactors = FALSE)

	DF_empty_credit_7320_1 <- copy(DF_empty_debit_7320_1)

	# Пустые таблицы для 7320.2
	DF_empty_debit_7320_2 <- data.table(
	      "Дата операции" = as.character(NA),
	      "Время проводки" = as.character(NA),
	      "Учетный номер" = as.character(NA),
	      "Счет № статьи расхода" = as.character(NA),
	      "Период расхода" = as.character(NA),
	      "Содержание операции" = as.character(NA),
	      "Метод учета" = as.character(NA),
	      "Сальдо начальное" = as.numeric(0),
	      "Кредит" = as.numeric(0),
	      "Дебет" = as.numeric(0),
	      "Счет № (дебет)" = as.character(NA),
	      "Счет № (кредит)" = as.character(NA),
	      "Сальдо конечное" = as.numeric(0),
	      "Пользователь" = as.character(NA),
	      stringsAsFactors = FALSE)

	DF_empty_credit_7320_2 <- copy(DF_empty_debit_7320_2)


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
      
      Shiny.addCustomMessageHandler('data_updated', function(message) {
        if (message.session_id === Shiny.shinyapp.$values.session_id) {
          console.log('Данные обновлены, загружаем...');
          Shiny.setInputValue('force_reload', Math.random());
        }
      });
      
      // Обработчик для скачивания файлов
      Shiny.addCustomMessageHandler('downloadFile', function(message) {
        var link = document.createElement('a');
        link.href = message.url;
        link.download = message.filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      });
    ")),
    tags$style(HTML("
      /*  Fixed DashboardHeader */
      .skin-blue .main-header {
        position: fixed !important;
        width: 100%;
        z-index: 1030;
      }
      .skin-blue .main-sidebar {
        padding-top: 50px;
      }
      .content-wrapper, .right-side {
        padding-top: 50px;
      }
      
      /*  Light blue download buttons */
      .btn-download, 
      .btn-download:hover,
      .btn-download:focus,
      .btn-download:active {
        background-color: #add8e6 !important;
        border-color: #add8e6 !important;
        color: #000 !important;
      }
      
      /*  Light green save buttons */
      .btn-save,
      .btn-save:hover,
      .btn-save:focus,
      .btn-save:active {
        background-color: #90ee90 !important;
        border-color: #90ee90 !important;
        color: #000 !important;
      }
      
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
      .auth-status {
        margin-top: 20px;
        padding: 15px;
        border-radius: 5px;
        font-weight: bold;
      }
      .auth-success {
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
      }
      .auth-error {
        background-color: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
      }
      .auth-warning {
        background-color: #fff3cd;
        color: #856404;
        border: 1px solid #ffeaa7;
      }
      .user-info-box {
        background-color: #e8f5e9;
        border: 1px solid #4caf50;
        border-radius: 5px;
        padding: 15px;
        margin: 10px 0;
      }
      .debit-credit-panel {
        background-color: #f5f5f5;
        border: 1px solid #ddd;
        border-radius: 5px;
        padding: 15px;
        margin: 10px 0;
      }
      .debit-credit-table {
        margin-top: 10px;
        margin-bottom: 20px;
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
    dashboardSidebar(width = 1050,
      sidebarMenu(
        menuItem("Главная", tabName = "home", icon = icon("home"),
          menuItem("Авторизация", tabName = "auth"),
          menuItem("Управление данными", tabName = "home_dashboard"),
          menuItem("Загрузка данных", tabName = "data_load")
        ),
        menuItem("Учет", tabName = "Учет", icon = icon("calculator"),
          menuItem("Расходы", tabName = "Expenses",
            menuItem("7000.Себестоимость реализованной продукции и оказанных услуг", tabName = "Expenses7000",
              menuItem("7010.Себестоимость реализованной продукции и оказанных услуг", tabName = "table7010")),
            menuItem("7100.Расходы по реализации продукции и оказанию услуг", tabName = "Expenses7100",
              menuItem("7110.Расходы по реализации продукции и оказанию услуг", tabName = "table7110")),
            menuItem("7200.Административные расходы", tabName = "Expenses7200",
              menuItem("7210.Административные расходы", tabName = "table7210")),
            menuItem("7300.Расходы на финансирование", tabName = "Expenses7300",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table7300"),
              menuItem("7310.Расходы по финансовым обязательствам", tabName = "Expenses7310",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7310"),
                menuItem("7310.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7310_1"),
                menuItem("7310.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7310_2")
              ),
              menuItem("7320.Расходы по финансовой аренде", tabName = "Expenses7320",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7320"),
                menuItem("7320.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7320_1"),
                menuItem("7320.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7320_2")
              ),
              menuItem("7330.Расходы, связанные с инвестиционным имуществом", tabName = "Expenses7330",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7330"),
                menuItem("7331.Расходы от обесценения инвестиционного имущества", tabName = "Expenses7331",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7331"),
                  menuItem("7331.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7331_1"),
                  menuItem("7331.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7331_2")
                ),
                menuItem("7332.Расходы в результате реклассификации инвестиционной недвижимости", tabName = "table7332")
              ),
              menuItem("7340.Расходы от изменения справедливой стоимости финансовых инструментов", tabName = "Expenses7340",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7340"),
                menuItem("7340.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7340_1"),
                menuItem("7340.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7340_2")
              ),
              menuItem("7350.Прочие расходы на финансирование", tabName = "Expenses7350",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7350"),
                menuItem("7350.1.Прочие расходы на финансирование, отражаемые в составе прибыли и убытка", tabName = "table7350_1"),
                menuItem("7350.2.Прочие расходы на финансирование, отражаемые в Прочем совокупном доходе", tabName = "table7350_2")
              )
            ),
            menuItem("7400.Прочие расходы", tabName = "Expenses7400",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table7400"),
              menuItem("7410.Расходы по выбытию активов", tabName = "Expenses7410",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7410"),
                menuItem("7410.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7410_1"),
                menuItem("7410.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7410_2")
              ),
              menuItem("7420.Расходы, связанные с прекращаемой деятельностью", tabName = "Expenses7420",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7420"),
                menuItem("7420.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7420_1"),
                menuItem("7420.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7420_2")
              ),
              menuItem("7430.Расходы по операционной аренде (арендатор)", tabName = "Expenses7430",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7430"),
                menuItem("7430.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7430_1"),
                menuItem("7430.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7430_2")
              ),
              menuItem("7440.Расходы по обесценению дебиторской задолженности", tabName = "Expenses7440",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7440"),
                menuItem("7440.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7440_1"),
                menuItem("7440.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7440_2")
              ),
              menuItem("7450.Расходы в результате переоценки", tabName = "Expenses7450",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7450"),
                menuItem("7451.Расходы по переоценке основных средств (в т.ч. от обесценения)", tabName = "Expenses7451",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7451"),
                  menuItem("7451.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7451_1"),
                  menuItem("7451.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7451_2")
                ),
                menuItem("7452.Расходы по переоценке нематериальных активов (в т.ч. от обесценения)", tabName = "Expenses7452",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7452"),
                  menuItem("7452.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7452_1"),
                  menuItem("7452.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7452_2")
                ),
                menuItem("7453.Расходы по переоценке долгосрочных активов (для продажи или распределения собственникам)", tabName = "Expenses7453",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7453"),
                  menuItem("7453.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7453_1"),
                  menuItem("7453.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7453_2")
                ),
                menuItem("7454.Расходы по переоценке выбывающих групп (для продажи или распределения собственникам)", tabName = "Expenses7454",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7454"),
                  menuItem("7454.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7454_1"),
                  menuItem("7454.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7454_2")
                )
              ),
              menuItem("7460.Расходы по производным инструментам", tabName = "Expenses7460",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7460"),
                menuItem("7461.Расходы по хеджированию чистых инвестиций в зарубежные операции: эффективная часть", tabName = "Expenses7461",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7461"),
                  menuItem("7461.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7461_1"),
                  menuItem("7461.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7461_2")
                ),
                menuItem("7462.Расходы по инструментам хеджирования при хеджировании денежных потоков: эффективная часть", tabName = "Expenses7462",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7462"),
                  menuItem("7462.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7462_1"),
                  menuItem("7462.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7462_2")
                ),
                menuItem("7463.Расходы по хеджированию инвестиций в долевые инструменты: по инструментам хеджирования", tabName = "table7463"),
                menuItem("7464.Расходы по хеджированию инвестиций в долевые инструменты: по объекту хеджирования", tabName = "table7464"),
                menuItem("7465.Расходы в результате изменения величины временной стоимости опционов", tabName = "Expenses7465",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7465"),
                  menuItem("7465.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7465_1"),
                  menuItem("7465.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7465_2")
                ),
                menuItem("7466.Расходы в результате изменения стоимости форвардных элементов форвардных договоров", tabName = "Expenses7466",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7466"),
                  menuItem("7466.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7466_1"),
                  menuItem("7466.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7466_2")
                ),
                menuItem("7467.Расходы в результате изменения стоимости валютного базисного спрэда фининструмента", tabName = "Expenses7467",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7467"),
                  menuItem("7467.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7467_1"),
                  menuItem("7467.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7467_2")
                )
              ),
              menuItem("7470.Расходы в результате пересчета валюты", tabName = "Expenses7470",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7470"),
                menuItem("7471.Расходы в результате пересчета иностранной валюты: чистые курсовые разницы", tabName = "Expenses7471",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7471"),
                  menuItem("7471.1.Расходы (чистые курсовые разницы), отражаемые в составе прибыли и убытка", tabName = "table7471_1"),
                  menuItem("7471.2.Расходы (чистые курсовые разницы), отражаемые в Прочем совокупном доходе", tabName = "table7471_2")
                ),
                menuItem("7472.Расходы в результате пересчета финансовой отчетности в другую валюту", tabName = "table7472")
              ),
              menuItem("7480.Прочие расходы (в т.ч. в результате обесценения)", tabName = "Expenses7480",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table7480"),
                menuItem("7481.Расходы, возникающие в связи с изменениями в прочем совокупном доходе объекта инвестиций", tabName = "table7481"),
                menuItem("7482.Расходы в результате изменения справедливой стоимости биологических активов", tabName = "Expenses7482",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7482"),
                  menuItem("7482.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7482_1"),
                  menuItem("7482.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7482_2")
                ),
                menuItem("7483.Расходы по выпущенным договорам страхования и удерживаемым договорам перестрахования", tabName = "Expenses7483",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7483"),
                  menuItem("7483.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7483_1"),
                  menuItem("7483.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7483_2")
                ),
                menuItem("7484.Расходы от обесценения разведочных и оценочных активов, учитываемых по переоцененной стоимости", tabName = "Expenses7484",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7484"),
                  menuItem("7484.1.Расходы, отражаемые в составе прибыли и убытка", tabName = "table7484_1"),
                  menuItem("7484.2.Расходы, отражаемые в Прочем совокупном доходе", tabName = "table7484_2")
                ),
                menuItem("7485.Прочие расходы (в т.ч. в результате обесценения), не учтенные в других субсчетах", tabName = "Expenses7485",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table7485"),
                  menuItem("7485.1.Прочие расходы, отражаемые в составе прибыли и убытка", tabName = "table7485_1"),
                  menuItem("7485.2.Прочие расходы, отражаемые в Прочем совокупном доходе", tabName = "table7485_2")
                )
              )
            ),
            menuItem("7500.Доля в убытках объектов инвестиции, учитываемая по методу долевого участия", tabName = "Expenses7500",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table7500"),
              menuItem("7510.Доля в убытках материнской компании и компаний с совместным контролем или значительным влиянием", tabName = "table7510"),
              menuItem("7520.Доля в убытках дочерних компаний и других организации той же группы", tabName = "table7520"),
              menuItem("7530.Доля в убытках ассоциированных организаций и совместных предприятий", tabName = "table7530")
            ),
            menuItem("7600.Расходы по налогам и обязательным платежам", tabName = "Expenses7600",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table7600"),
              menuItem("7610.Расходы по налогам", tabName = "table7610"),
              menuItem("7620.Расходы по обязательным платежам", tabName = "table7620"))
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
        # Переименована вкладка "Home" в "Управление данными"
        tabItem(tabName = "home_dashboard",
          h2("Управление данными"),
          fluidRow(
            box(width = 12, title = NULL, status = "primary",
              actionButton("test_connection", "Тест подключения к базе данных", 
                         icon = icon("database"), class = "btn-info"),
              verbatimTextOutput("connection_status"),
              br(),
              div(class = "session-info",
                h4("Управление сессиями"),
                #  Выбор сессии и кнопки в одной горизонтальной линии
                fluidRow(
                  column(3, h5("Выберите сессию для загрузки:")),
                  column(3, uiOutput("session_selector_ui")),
                  column(3, actionButton("load_session_btn", "Загрузить сессию", 
                                       icon = icon("folder-open"), class = "btn-download", width = "100%")),
                  column(3, actionButton("save_session_btn", "Сохранить текущую сессию", 
                                       icon = icon("save"), class = "btn-save", width = "100%"))
                )
              ),
              br(),
              wellPanel(
                h4("Текущая сессия"),
                textOutput("current_session_info"),
                textOutput("current_session_date"),
                tags$div(style = "margin-top: 10px; font-size: 12px; color: #666;",
                  "Эта сессия общая для всех пользователей и действует с 00:00 до 23:59 текущих суток"
                )
              ),
              div(class = "debit-credit-panel",
                h4("Добавление операций"),
                #  Дебет в одной горизонтальной линии
                fluidRow(
                  column(12,
                    fluidRow(
                      column(2, h5("Выберите счет дебета:")),
                      column(2, selectInput("debit_select", label = NULL, 
                                            choices = c("", "7010_1", "7110_1", "7210_1", 
					    "7310.1", "7310.2", "7320.1", "7320.2"), selected = ""))
                    ),
                    div(class = "debit-credit-table",
                      rHandsontableOutput("debit_table")
                    )
                  )
                ),
                br(),
                #  Кредит в новой строке с шириной 12
                fluidRow(
                  column(12,
                    #  Кредит в одной горизонтальной линии
                    fluidRow(
                      column(2, h5("Выберите счет кредита:")),
                      column(2, selectInput("credit_select", label = NULL,
                                            choices = c("", "7010_1", "7110_1", "7210_1", 
					    "7310.1", "7310.2", "7320.1", "7320.2"), selected = ""))
                    ),
                    div(class = "debit-credit-table",
                      rHandsontableOutput("credit_table")
                    )
                  )
                )
              )
            )
          )
        ),
        tabItem(tabName = "auth",
          h2("Авторизация"),
          fluidRow(
            box(width = 12, title = NULL, status = "primary",
              div(class = "user-info-box",
                h4("Доступные пользователи"),
                tags$ul(
                  tags$li("пользователь_1"),
                  tags$li("пользователь_2"), 
                  tags$li("пользователь_3"),
                  tags$li("пользователь_4")
                ),
                p("Пароль генерируется автоматически при первом выборе пользователя. При последующих авторизациях необходимо вводить пароль вручную.")
              ),
              
              fluidRow(
                column(12,
                  selectInput("username", "Имя пользователя", 
                            choices = c("Выберите пользователя" = "",
                                      "пользователь_1" = "пользователь_1",
                                      "пользователь_2" = "пользователь_2",
                                      "пользователь_3" = "пользователь_3",
                                      "пользователь_4" = "пользователь_4")),
                  textInput("password", "Пароль", value = "", placeholder = "Введите пароль для авторизации"),
                  actionButton("login", "Вход", icon = icon("sign-in"), class = "btn-success"),
                  br(), br()
                )
              ),
              hr(),
              fluidRow(
                column(12,
                  uiOutput("auth_status_ui")
                )
              )
            )
          )
        ),
	# вкладка "Загрузка данных"
	tabItem(tabName = "data_load",
	  h2("Загрузка данных"),
	  fluidRow(
	    box(width = 12, title = NULL, status = "primary",
	      fluidRow(
	        column(2, h4("Выберите дату сессии:")),
	        column(2, dateInput("data_load_date", label = NULL,
	                  value = Sys.Date(), format = "yyyy-mm-dd",
	                  language = "ru")),
	        column(3, 
	          actionButton("load_selected_btn", "Загрузить отмеченные таблицы", 
	                    icon = icon("download"), class = "btn-download", width = "100%",
	                    style = "background-color: #add8e6; border-color: #add8e6; color: #000;")
	        ),
	        column(3,
	          actionButton("load_all_btn", "Загрузить все таблицы", 
	                    icon = icon("download"), class = "btn-primary", width = "100%")
	        )
	      ),
	      hr(),
	      h4("Выберите таблицы для загрузки:"),
	      fluidRow(
	        column(12,
	          tags$div(style = "margin-left: 40px;",
	            tags$h5("РАСХОДЫ"),
	            tags$div(style = "margin-left: 20px;",
	              tags$h5("7000.Себестоимость реализованной продукции и оказанных услуг"),
	              tags$div(style = "margin-left: 20px;",
	                checkboxInput("check_7010_1", "7010.Себестоимость реализованной продукции и оказанных услуг", value = FALSE)
	              ),
	              tags$h5("7100.Расходы по реализации продукции и оказанию услуг"),
	              tags$div(style = "margin-left: 20px;",
	                checkboxInput("check_7110_1", "7110.Расходы по реализации продукции и оказанию услуг", value = FALSE)
	              ),
	              tags$h5("7200.Административные расходы"),
	              tags$div(style = "margin-left: 20px;",
	                checkboxInput("check_7210_1", "7210.Административные расходы", value = FALSE)
	              ),
	              tags$h5("7300.Расходы на финансирование"),
	              tags$div(style = "margin-left: 20px;",
	                tags$h5("7310.Расходы по финансовым обязательствам"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7310_1", "7310.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7310_2", "7310.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7320.Расходы по финансовой аренде"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7320_1", "7320.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7320_2", "7320.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7330.Расходы, связанные с инвестиционным имуществом"),
	                tags$div(style = "margin-left: 20px;",
	                  tags$h5("7331.Расходы от обесценения инвестиционного имущества"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7331_1", "7331.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7331_2", "7331.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  checkboxInput("check_7332", "7332.Расходы в результате реклассификации инвестиционной недвижимости", value = FALSE)
	                ),
	                tags$h5("7340.Расходы от изменения справедливой стоимости финансовых инструментов"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7340_1", "7340.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7340_2", "7340.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7350.Прочие расходы на финансирование"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7350_1", "7350.1.Прочие расходы на финансирование, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7350_2", "7350.2.Прочие расходы на финансирование, отражаемые в Прочем совокупном доходе", value = FALSE)
	                )
	              ),
	              tags$h5("7400.Прочие расходы"),
	              tags$div(style = "margin-left: 20px;",
	                tags$h5("7410.Расходы по выбытию активов"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7410_1", "7410.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7410_2", "7410.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7420.Расходы, связанные с прекращаемой деятельностью"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7420_1", "7420.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7420_2", "7420.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7430.Расходы по операционной аренде (арендатор)"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7430_1", "7430.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7430_2", "7430.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7440.Расходы по обесценению дебиторской задолженности"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7440_1", "7440.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                  checkboxInput("check_7440_2", "7440.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                ),
	                tags$h5("7450.Расходы в результате переоценки"),
	                tags$div(style = "margin-left: 20px;",
	                  tags$h5("7451.Расходы по переоценке основных средств (в т.ч. от обесценения)"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7451_1", "7451.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7451_2", "7451.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7452.Расходы по переоценке нематериальных активов (в т.ч. от обесценения)"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7452_1", "7452.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7452_2", "7452.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7453.Расходы по переоценке долгосрочных активов (для продажи или распределения собственникам)"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7453_1", "7453.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7453_2", "7453.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7454.Расходы по переоценке выбывающих групп (для продажи или распределения собственникам)"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7454_1", "7454.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7454_2", "7454.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  )
	                ),
	                tags$h5("7460.Расходы по производным инструментам"),
	                tags$div(style = "margin-left: 20px;",
	                  tags$h5("7461.Расходы по хеджированию чистых инвестиций в зарубежные операции: эффективная часть"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7461_1", "7461.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7461_2", "7461.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7462.Расходы по инструментам хеджирования при хеджировании денежных потоков: эффективная часть"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7462_1", "7462.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7462_2", "7462.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  checkboxInput("check_7463", "7463.Расходы по хеджированию инвестиций в долевые инструменты: по инструментам хеджирования", value = FALSE),
	                  checkboxInput("check_7464", "7464.Расходы по хеджированию инвестиций в долевые инструменты: по объекту хеджирования", value = FALSE),
	                  tags$h5("7465.Расходы в результате изменения величины временной стоимости опционов"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7465_1", "7465.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7465_2", "7465.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7466.Расходы в результате изменения стоимости форвардных элементов форвардных договоров"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7466_1", "7466.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7466_2", "7466.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7467.Расходы в результате изменения стоимости валютного базисного спрэда фининструмента"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7467_1", "7467.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7467_2", "7467.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  )
	                ),
	                tags$h5("7470.Расходы в результате пересчета валюты"),
	                tags$div(style = "margin-left: 20px;",
	                  tags$h5("7471.Расходы в результате пересчета иностранной валюты: чистые курсовые разницы"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7471_1", "7471.1.Расходы (чистые курсовые разницы), отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7471_2", "7471.2.Расходы (чистые курсовые разницы), отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  checkboxInput("check_7472", "7472.Расходы в результате пересчета финансовой отчетности в другую валюту ", value = FALSE)
	                ),
	                tags$h5("7480.Прочие расходы (в т.ч. в результате обесценения)"),
	                tags$div(style = "margin-left: 20px;",
	                  checkboxInput("check_7481", "7481.Расходы, возникающие в связи с изменениями в прочем совокупном доходе объекта инвестиций", value = FALSE),
	                  tags$h5("7482.Расходы в результате изменения справедливой стоимости биологических активов"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7482_1", "7482.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7482_2", "7482.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7483.Расходы по выпущенным договорам страхования и удерживаемым договорам перестрахования"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7483_1", "7483.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7483_2", "7483.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7484.Расходы от обесценения разведочных и оценочных активов, учитываемых по переоцененной стоимости"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7484_1", "7484.1.Расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7484_2", "7484.2.Расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  ),
	                  tags$h5("7485.Прочие расходы (в т.ч. в результате обесценения), не учтенные в других субсчетах"),
	                  tags$div(style = "margin-left: 20px;",
	                    checkboxInput("check_7485_1", "7485.1.Прочие расходы, отражаемые в составе прибыли и убытка", value = FALSE),
	                    checkboxInput("check_7485_2", "7485.2.Прочие расходы, отражаемые в Прочем совокупном доходе", value = FALSE)
	                  )
	                )
	              ),
	              tags$h5("7500.Доля в убытках объектов инвестиции, учитываемая по методу долевого участия"),
	              tags$div(style = "margin-left: 20px;",
	                checkboxInput("check_7510", "7510.Доля в убытках материнской компании и компаний с совместным контролем или значительным влиянием", value = FALSE),
	                checkboxInput("check_7520", "7520.Доля в убытках дочерних компаний и других организации той же группы", value = FALSE),
	                checkboxInput("check_7530", "7530.Доля в убытках ассоциированных организаций и совместных предприятий", value = FALSE)
	              ),
	              tags$h5("7600.Расходы по налогам и обязательным платежам"),
	              tags$div(style = "margin-left: 20px;",
	                checkboxInput("check_7610", "7610.Расходы по налогам", value = FALSE),
	                checkboxInput("check_7620", "7620.Расходы по обязательным платежам", value = FALSE)
	              )
	            )  # закрывает tags$div(style = "margin-left: 20px;", ...) - РАСХОДЫ
	          )  # закрывает tags$div(style = "margin-left: 40px;", ...)
	        )  # закрывает column(12, ...)
	      )  # закрывает fluidRow внутри box
	    )  # закрывает box
	  )  # закрывает fluidRow на уровне box
	),  # закрывает tabItem (вкладка "Загрузка данных")
        tabItem(tabName = "table7010",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates7000", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui7000")),
            column(
	      width = 12, br(),
	      tags$b("ОСВ: 7010.Себестоимость реализованной продукции и оказанных услуг"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7010Item3"),
	      downloadButton("download_df7010_3", "Загрузить данные"),
	    ),
            column(
	      width = 12, br(),
	      tags$b("Журнал учета хозопераций: 7010.Себестоимость реализованной продукции и оказанных услуг"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7010Item1"),
              br(),
              actionButton("save_table7010_1", "Сохранить таблицу 7010_1", icon = icon("save"), class = "btn-save"),
	      downloadButton("download_df7010", "Загрузить данные")
	    ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции и/или учетному номеру"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices7010", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру",
					"Выбор по дате операции и учетному номеру")),
              uiOutput("nested_ui7010")
            ),
            column(
              width = 12, br(),
              rHandsontableOutput("table7010Item2"),
	      downloadButton("download_df7010_2", "Загрузить данные"))
	    )
	  ),
        tabItem(tabName = "table7110",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates7100", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui7100")),
            column(
	      width = 12, br(),
	      tags$b("ОСВ: 7110.Расходы по реализации продукции и оказанию услуг"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7110Item3"),
	      downloadButton("download_df7110_3", "Загрузить данные"),
	    ),
            column(
	      width = 12, br(),
	      tags$b("Журнал учета хозопераций: 7110.Расходы по реализации продукции и оказанию услуг"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7110Item1"),
              br(),
              actionButton("save_table7110_1", "Сохранить таблицу 7110_1", icon = icon("save"), class = "btn-save"),
	      downloadButton("download_df7110_1", "Загрузить данные")
	  ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции и/или учетному номеру"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices7110", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру",
					"Выбор по дате операции и учетному номеру")),
              uiOutput("nested_ui7110")
            ),
            column(
              width = 12, br(),
              rHandsontableOutput("table7110Item2"),
	      downloadButton("download_df7110_2", "Загрузить данные"))
	    )
	  ),
        tabItem(tabName = "table7210",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates7200", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui7200")),
            column(
	      width = 12, br(),
	      tags$b("ОСВ: 7210.Административные расходы"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7210Item3"),
	      downloadButton("download_df7210_3", "Загрузить данные"),
	    ),
            column(
	      width = 12, br(),
	      tags$b("Журнал учета хозопераций: 7210.Административные расходы"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table7210Item1"),
              br(),
              actionButton("save_table7210_1", "Сохранить таблицу 7210_1", icon = icon("save"), class = "btn-save"),
	      downloadButton("download_df7210_1", "Загрузить данные")
	  ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции и/или учетному номеру"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices7210", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру",
					"Выбор по дате операции и учетному номеру")),
              uiOutput("nested_ui7210")
            ),
            column(
              width = 12, br(),
              rHandsontableOutput("table7210Item2"),
	      downloadButton("download_df7210_2", "Загрузить данные"))
	      )
	    ),
          tabItem(tabName = "table7310_1",
            fluidRow(
              column(
                width = 12, br(),
                tags$b("Журнал учета хозопераций: 7310.1.Расходы по финансовым обязательствам, отражаемые в составе прибыли и убытка"),
	        tags$div(style = "margin-bottom: 20px;"),
                rHandsontableOutput("table7310.1Item1"),
                br(),
                actionButton("save_table7310_1", "Сохранить таблицу 7310.1", icon = icon("save"), class = "btn-save"),
	        downloadButton("download_df7310.1", "Загрузить данные")
              ),
              column(
                width = 12, br(),
                tags$b("Выборка данных по дате операции, номеру первичного документа или статье расхода"),
	        tags$div(style = "margin-bottom: 20px;"),
                selectInput("choices7310.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру", 
					"Выбор по статье расхода", 
					"Выбор по дате операции и учетному номеру", 
					"Выбор по дате операции и статье расхода")),
                uiOutput("nested_ui7310.1")),
              column(
                width = 12, br(),
                label=NULL,
                rHandsontableOutput("table7310.1Item2"),
	        downloadButton("download_df7310.1_2", "Загрузить данные"))
	      )
	    ),
          tabItem(tabName = "table7310_2",
            fluidRow(
              column(
                width = 12, br(),
                tags$b("Журнал учета хозопераций: 7310.2.Расходы по финансовым обязательствам, отражаемые в Прочем совокупном доходе"),
	        tags$div(style = "margin-bottom: 20px;"),
                rHandsontableOutput("table7310.2Item1"),
                br(),
                actionButton("save_table7310_2", "Сохранить таблицу 7310.2", icon = icon("save"), class = "btn-save"),
	        downloadButton("download_df7310.2", "Загрузить данные")
              ),
              column(
                width = 12, br(),
                tags$b("Выборка данных по дате операции, номеру первичного документа или статье расхода"),
	        tags$div(style = "margin-bottom: 20px;"),
                selectInput("choices7310.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру", 
					"Выбор по статье расхода", 
					"Выбор по дате операции и учетному номеру", 
					"Выбор по дате операции и статье расхода")),
                uiOutput("nested_ui7310.2")),
              column(
                width = 12, br(),
                label=NULL,
                rHandsontableOutput("table7310.2Item2"),
	        downloadButton("download_df7310.2_2", "Загрузить данные"))
	      )
	    ),
          tabItem(tabName = "table7320_1",
            fluidRow(
              column(
                width = 12, br(),
                tags$b("Журнал учета хозопераций: 7320.1.Расходы по финансовой аренде, отражаемые в составе прибыли и убытка"),
	        tags$div(style = "margin-bottom: 20px;"),
                rHandsontableOutput("table7320.1Item1"),
                br(),
                actionButton("save_table7320_1", "Сохранить таблицу 7320.1", icon = icon("save"), class = "btn-save"),
	        downloadButton("download_df7320.1", "Загрузить данные")
              ),
              column(
                width = 12, br(),
                tags$b("Выборка данных по дате операции, номеру первичного документа или статье расхода"),
	        tags$div(style = "margin-bottom: 20px;"),
                selectInput("choices7320.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру", 
					"Выбор по статье расхода", 
					"Выбор по дате операции и учетному номеру", 
					"Выбор по дате операции и статье расхода")),
                uiOutput("nested_ui7320.1")),
              column(
                width = 12, br(),
                label=NULL,
                rHandsontableOutput("table7320.1Item2"),
	        downloadButton("download_df7320.1_2", "Загрузить данные"))
	      )
	    ),
          tabItem(tabName = "table7320_2",
            fluidRow(
              column(
                width = 12, br(),
                tags$b("Журнал учета хозопераций: 7320.2.Расходы по финансовой аренде, отражаемые в Прочем совокупном доходе"),
	        tags$div(style = "margin-bottom: 20px;"),
                rHandsontableOutput("table7320.2Item1"),
                br(),
                actionButton("save_table7320_2", "Сохранить таблицу 7320.2", icon = icon("save"), class = "btn-save"),
	        downloadButton("download_df7320.2", "Загрузить данные")
              ),
              column(
                width = 12, br(),
                tags$b("Выборка данных по дате операции, номеру первичного документа или статье расхода"),
	        tags$div(style = "margin-bottom: 20px;"),
                selectInput("choices7320.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по учетному номеру", 
					"Выбор по статье расхода", 
					"Выбор по дате операции и учетному номеру", 
					"Выбор по дате операции и статье расхода")),
                uiOutput("nested_ui7320.2")),
              column(
                width = 12, br(),
                label=NULL,
                rHandsontableOutput("table7320.2Item2"),
	        downloadButton("download_df7320.2_2", "Загрузить данные"))
	      )
	    )
          )
        )
      )
    )

server <- function(input, output, session) {
  session_id <- reactiveVal(get_current_session_id())
  
  r <- reactiveValues(
    db_initialized = FALSE,
    show_loading = FALSE,
    sessions_loaded = FALSE,
    start = ymd(Sys.Date()),
    end = ymd(Sys.Date()),
    session_list = character(0),
    sessions_updated = 0,
    db_init_notified = FALSE,
    data_version = 0,
    last_checked = Sys.time(),
    current_session_last_update = NULL,
    user_authenticated = FALSE,
    current_user = NULL,
    auth_status = NULL,
    auth_status_type = NULL,
    current_user_data = NULL,
    debit_table_data = DF_empty_debit,
    credit_table_data = DF_empty_credit,
    debit_account_selected = "",
    credit_account_selected = "",
    # Добавлено для загрузки данных
    data_load_selected_date = NULL,
    data_load_tables = list()
  )
  
  data <- reactiveValues(
	    df7010_1 = NULL,
	    df7010_2 = NULL,
	    df7010_3 = NULL,
	    df7110_1 = NULL,
	    df7110_2 = NULL,
	    df7110_3 = NULL,
	    df7210_1 = NULL,
	    df7210_2 = NULL,
	    df7210_3 = NULL,
	    df7310.1 = NULL,
	    df7310.2 = NULL,
	    df7320.1 = NULL,
	    df7320.2 = NULL
	  )
  
  output$show_loading <- reactive({
    r$show_loading
  })
  outputOptions(output, "show_loading", suspendWhenHidden = FALSE)
  
  output$auth_status_ui <- renderUI({
    if (!is.null(r$auth_status)) {
      div(class = paste("auth-status", r$auth_status_type),
        HTML(r$auth_status)
      )
    }
  })
  
  update_auth_status <- function(message, type = "success") {
    r$auth_status <- message
    r$auth_status_type <- paste0("auth-", type)
  }
  
  reset_auth_status <- function() {
    r$auth_status <- NULL
    r$auth_status_type <- NULL
  }
  
  observeEvent(input$username, {
    req(input$username, input$username != "")
    
    reset_auth_status()
    
    r$show_loading <- TRUE
    
    tryCatch({
      user_info <- get_or_create_user_simple(input$username, is_new_session = TRUE)
      
      if (is.null(user_info)) {
        update_auth_status("Ошибка при работе с пользователем. Проверьте подключение к базе данных.", "error")
      } else {
        r$current_user_data <- user_info
        
        if (user_info$is_new && !is.null(user_info$password)) {
          updateTextInput(session, "password", value = user_info$password)
          update_auth_status(paste("Создан новый пользователь", input$username, ". Пароль показан выше."), "success")
        } else {
          updateTextInput(session, "password", value = "")
          update_auth_status(paste("Данные пользователя", input$username, "успешно загружены. Введите пароль для авторизации."), "success")
        }
      }
    }, error = function(e) {
      message("Ошибка при работе с пользователем: ", e$message)
      update_auth_status(paste("Ошибка: ", e$message), "error")
    }, finally = {
      r$show_loading <- FALSE
    })
  })
  
  observeEvent(input$login, {
    req(input$username, input$username != "", input$password)
    
    if (is.null(r$current_user_data)) {
      update_auth_status("Сначала выберите пользователя для загрузки данных.", "warning")
      return()
    }
    
    r$show_loading <- TRUE
    
    tryCatch({
      if (check_user_password(input$username, input$password)) {
        r$user_authenticated <- TRUE
        r$current_user <- input$username
        update_auth_status(paste("Пользователь", input$username, "успешно авторизован!"), "success")
      } else {
        update_auth_status("Имя пользователя и пароль не совпадают.", "error")
      }
    }, error = function(e) {
      message("Ошибка при проверке пароля: ", e$message)
      update_auth_status("Ошибка при проверке пароля. Попробуйте снова.", "error")
    }, finally = {
      r$show_loading <- FALSE
    })
  })
  
	  observeEvent(input$debit_select, {
	    req(input$debit_select != "")
    
	    r$debit_account_selected <- input$debit_select
    
	if (input$debit_select == "7010_1") {
	      r$debit_table_data <- copy(DF7010_1)
	    } else if (input$debit_select == "7110_1") {
	      r$debit_table_data <- copy(DF7110_1)
	    } else if (input$debit_select == "7210_1") {
	      r$debit_table_data <- copy(DF7210_1)
	    } else if (input$debit_select == "7310.1") {
	      r$debit_table_data <- copy(DF7310.1)
	    } else if (input$debit_select == "7310.2") {
	      r$debit_table_data <- copy(DF7310.2)
	    } else if (input$debit_select == "7320.1") {
	      r$debit_table_data <- copy(DF7320.1)
	    } else if (input$debit_select == "7320.2") {
	      r$debit_table_data <- copy(DF7320.2)
	    }
	  })
  
	  observeEvent(input$credit_select, {
	    req(input$credit_select != "")
    
	    r$credit_account_selected <- input$credit_select
    
	if (input$credit_select == "7010_1") {
	      r$credit_table_data <- copy(DF7010_1)
	    } else if (input$credit_select == "7110_1") {
	      r$credit_table_data <- copy(DF7110_1)
	    } else if (input$credit_select == "7210_1") {
	      r$credit_table_data <- copy(DF7210_1)
	    } else if (input$credit_select == "7310.1") {
	      r$credit_table_data <- copy(DF7310.1)
	    } else if (input$credit_select == "7310.2") {
	      r$credit_table_data <- copy(DF7310.2)
	    } else if (input$credit_select == "7320.1") {
	      r$credit_table_data <- copy(DF7320.1)
	    } else if (input$credit_select == "7320.2") {
	      r$credit_table_data <- copy(DF7320.2)
	    }
	  })
  
  observeEvent(input$debit_table, {
    if (!is.null(input$debit_table)) {
      r$debit_table_data <- hot_to_r(input$debit_table)
    }
  })
  
  observeEvent(input$credit_table, {
    if (!is.null(input$credit_table)) {
      r$credit_table_data <- hot_to_r(input$credit_table)
    }
  })
  
  output$debit_table <- renderRHandsontable({
    if (nrow(r$debit_table_data) > 0) {
      rhandsontable(r$debit_table_data, colWidths = 150, height = 400, 
                   allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date") |>
        hot_col(2, readOnly = TRUE) |>
        hot_col("Пользователь", readOnly = TRUE)
    }
  })
  
  output$credit_table <- renderRHandsontable({
    if (nrow(r$credit_table_data) > 0) {
      rhandsontable(r$credit_table_data, colWidths = 150, height = 400, 
                   allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
        hot_col(1, dateFormat = "YYYY-MM-DD", type = "date") |>
        hot_col(2, readOnly = TRUE) |>  # Время проводки только для чтения
        hot_col("Пользователь", readOnly = TRUE)
    }
  })
  
	# is_table_complete functions for each specific table
	is_table_complete_7010_1 <- function(table_data) {
	  if (is.null(table_data) || nrow(table_data) == 0) {
	    return(FALSE)
	  }
	
	  required_cols <- c("Дата операции", "Учетный номер", 
	                    "Содержание операции", "Метод учета", "Сальдо начальное", 
	                    "Дебет", "Кредит", "Счет № (дебет)", 
	                    "Счет № (кредит)", "Сальдо конечное")
	  
	  for (col in required_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
	        return(FALSE)
	      }
	    }
	  }
	  
	  numeric_cols <- c("Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное")
	  for (col in numeric_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (!is.numeric(value) || is.na(value)) {
	        return(FALSE)
	      }
	    }
	  }
	  return(TRUE)
	}
	
	is_table_complete_7110_1 <- function(table_data) {
	  if (is.null(table_data) || nrow(table_data) == 0) {
	    return(FALSE)
	  }
	
	  required_cols <- c("Дата операции", "Учетный номер", 
	                    "Содержание операции", "Метод учета", "Сальдо начальное", 
	                    "Дебет", "Кредит", "Счет № (дебет)", 
	                    "Счет № (кредит)", "Сальдо конечное")
	  
	  for (col in required_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
	        return(FALSE)
	      }
	    }
	  }
	  
	  numeric_cols <- c("Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное")
	  for (col in numeric_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (!is.numeric(value) || is.na(value)) {
	        return(FALSE)
	      }
	    }
	  }
	  return(TRUE)
	}
	
	is_table_complete_7210_1 <- function(table_data) {
	  if (is.null(table_data) || nrow(table_data) == 0) {
	    return(FALSE)
	  }
	
	  required_cols <- c("Дата операции", "Учетный номер", 
	                    "Содержание операции", "Метод учета", "Сальдо начальное", 
	                    "Дебет", "Кредит", "Счет № (дебет)", 
	                    "Счет № (кредит)", "Сальдо конечное")
	  
	  for (col in required_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
	        return(FALSE)
	      }
	    }
	  }
	  
	  numeric_cols <- c("Сальдо начальное", "Дебет", "Кредит", "Сальдо конечное")
	  for (col in numeric_cols) {
	    if (col %in% names(table_data)) {
	      value <- table_data[[col]][1]
	      if (!is.numeric(value) || is.na(value)) {
	        return(FALSE)
	      }
	    }
	  }
	  return(TRUE)
	}

        is_table_complete_7310_1 <- function(table_data) {
          if (is.null(table_data) || nrow(table_data) == 0) {
            return(FALSE)
          }
        
          required_cols <- c("Дата операции", "Учетный номер", "Счет № статьи расхода",
                             "Период расхода", "Содержание операции", "Метод учета",
                             "Сальдо начальное", "Кредит", "Дебет", "Счет № (дебет)",
			     "Счет № (кредит)", "Сальдо конечное")
          
          for (col in required_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
                return(FALSE)
              }
            }
          }
          
          numeric_cols <- c("Сальдо начальное", "Кредит", "Дебет", "Сальдо конечное")
          for (col in numeric_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (!is.numeric(value) || is.na(value)) {
                return(FALSE)
              }
            }
          }
          return(TRUE)
        }

        is_table_complete_7310_2 <- function(table_data) {
          if (is.null(table_data) || nrow(table_data) == 0) {
            return(FALSE)
          }
        
          required_cols <- c("Дата операции", "Учетный номер", "Счет № статьи расхода",
                             "Период расхода", "Содержание операции", "Метод учета",
                             "Сальдо начальное", "Кредит", "Дебет", "Счет № (дебет)",
			     "Счет № (кредит)", "Сальдо конечное")
          
          for (col in required_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
                return(FALSE)
              }
            }
          }
          
          numeric_cols <- c("Сальдо начальное", "Кредит", "Дебет", "Сальдо конечное")
          for (col in numeric_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (!is.numeric(value) || is.na(value)) {
                return(FALSE)
              }
            }
          }
          return(TRUE)
        }

        is_table_complete_7320_1 <- function(table_data) {
          if (is.null(table_data) || nrow(table_data) == 0) {
            return(FALSE)
          }
        
          required_cols <- c("Дата операции", "Учетный номер", "Счет № статьи расхода",
                             "Период расхода", "Содержание операции", "Метод учета",
                             "Сальдо начальное", "Кредит", "Дебет", "Счет № (дебет)",
			     "Счет № (кредит)", "Сальдо конечное")
          
          for (col in required_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
                return(FALSE)
              }
            }
          }
          
          numeric_cols <- c("Сальдо начальное", "Кредит", "Дебет", "Сальдо конечное")
          for (col in numeric_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (!is.numeric(value) || is.na(value)) {
                return(FALSE)
              }
            }
          }
          return(TRUE)
        }

        is_table_complete_7320_2 <- function(table_data) {
          if (is.null(table_data) || nrow(table_data) == 0) {
            return(FALSE)
          }
        
          required_cols <- c("Дата операции", "Учетный номер", "Счет № статьи расхода",
                             "Период расхода", "Содержание операции", "Метод учета",
                             "Сальдо начальное", "Кредит", "Дебет", "Счет № (дебет)",
			     "Счет № (кредит)", "Сальдо конечное")
          
          for (col in required_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (is.na(value) || (is.character(value) && nchar(trimws(value)) == 0)) {
                return(FALSE)
              }
            }
          }
          
          numeric_cols <- c("Сальдо начальное", "Кредит", "Дебет", "Сальдо конечное")
          for (col in numeric_cols) {
            if (col %in% names(table_data)) {
              value <- table_data[[col]][1]
              if (!is.numeric(value) || is.na(value)) {
                return(FALSE)
              }
            }
          }
          return(TRUE)
        }

	#*******
  
  output$session_selector_ui <- renderUI({
    r$sessions_updated
    
    sessions <- get_previous_sessions()
    r$session_list <- sessions
    
    if (length(sessions) > 0) {
      display_names <- sessions
      choices <- setNames(sessions, display_names)
      choices <- c("Выберите сессию..." = "", choices)
    } else {
      choices <- c("Нет доступных сессий" = "")
    }
    
    selectInput("session_selector", label = NULL, 
                choices = choices, width = "100%")
  })
  
	  # Инициализация данных из глобальных переменных
	  observe({
	    data$df7010_1 <- copy(DF7010_1)
	    data$df7010_2 <- copy(DF7010_2)
	    data$df7010_3 <- copy(DF7010_3)
	
	    data$df7110_1 <- copy(DF7110_1)
	    data$df7110_2 <- copy(DF7110_2)
	    data$df7110_3 <- copy(DF7110_3)
	
	    data$df7210_1 <- copy(DF7210_1)
	    data$df7210_2 <- copy(DF7210_2)
	    data$df7210_3 <- copy(DF7210_3)

	    data$df7310.1 <- copy(DF7310.1)
	    data$df7310.2 <- copy(DF7310.2)
	    data$df7320.1 <- copy(DF7320.1)
	    data$df7320.2 <- copy(DF7320.2)
	  })
  
  observe({
    if (!r$db_init_notified) {
      init_success <- initialize_database_simple()
      r$db_initialized <- init_success
      
      if (init_success && !r$db_init_notified) {
        showNotification("База данных инициализирована успешно.", 
                        type = "message", duration = 5)
        r$db_init_notified <- TRUE
        
        load_current_session_data()
      } else if (!init_success && !r$db_init_notified) {
        showNotification("Внимание: База данных недоступна. Работа в автономном режиме.", 
                        type = "warning", duration = 10)
        r$db_init_notified <- TRUE
      }
    }
  })
  
	# функция загрузки данных текущей сессии (расширена для новых таблиц)
	load_current_session_data <- function() {
		current_session <- session_id()
		message("Загрузка данных для текущей сессии: ", current_session)
    
	tryCatch({
	# Загружаем ОБЪЕДИНЕННЫЕ данные для текущей сессии (без дубликатов)
	loaded_data_7010_1 <- load_merged_session_data("app_data_7010_1", current_session)
	loaded_data_7110_1 <- load_merged_session_data("app_data_7110_1", current_session)
	loaded_data_7210_1 <- load_merged_session_data("app_data_7210_1", current_session)
	loaded_data_7310_1 <- load_merged_session_data("app_data_7310_1", current_session)
	loaded_data_7310_2 <- load_merged_session_data("app_data_7310_2", current_session)
	loaded_data_7320_1 <- load_merged_session_data("app_data_7320_1", current_session)
	loaded_data_7320_2 <- load_merged_session_data("app_data_7320_2", current_session)

	data$df7010_3 <- copy(DF7010_3)
	data$df7110_3 <- copy(DF7110_3)
	data$df7210_3 <- copy(DF7210_3)
          
	# Обработка данных 7010_1
	if (!is.null(loaded_data_7010_1)) {
	  temp_data <- as.data.table(loaded_data_7010_1)
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
	    
	    if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
	    if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
	    if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
	    if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]

	    data$df7010_1 <- temp_data
	  } else {
	    data$df7010_1 <- copy(DF7010_1)
	  }
	} else {
	  data$df7010_1 <- copy(DF7010_1)
	}
	
	# Обработка данных 7110_1
	if (!is.null(loaded_data_7110_1)) {
	  temp_data <- as.data.table(loaded_data_7110_1)
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
	    
	    if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
	    if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
	    if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
	    if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
	    
	    data$df7110_1 <- temp_data
	  } else {
	    data$df7110_1 <- copy(DF7110_1)
	  }
	} else {
	  data$df7110_1 <- copy(DF7110_1)
	}
	
	# Обработка данных 7210_1
	if (!is.null(loaded_data_7210_1)) {
	  temp_data <- as.data.table(loaded_data_7210_1)
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
	    
	    if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
	    if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
	    if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
	    if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
	    
	    data$df7210_1 <- temp_data
	  } else {
	    data$df7210_1 <- copy(DF7210_1)
	  }
	} else {
	  data$df7210_1 <- copy(DF7210_1)
	}

      # Обработка данных 7310.1
      if (!is.null(loaded_data_7310_1)) {
        temp_data <- as.data.table(loaded_data_7310_1)
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7310.1 <- temp_data
        } else {
          data$df7310.1 <- copy(DF7310.1)
        }
      } else {
        data$df7310.1 <- copy(DF7310.1)
      }

      # Обработка данных 7310.2
      if (!is.null(loaded_data_7310_2)) {
        temp_data <- as.data.table(loaded_data_7310_2)
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7310.2 <- temp_data
        } else {
          data$df7310.2 <- copy(DF7310.2)
        }
      } else {
        data$df7310.2 <- copy(DF7310.2)
      }

      # Обработка данных 7320.1
      if (!is.null(loaded_data_7320_1)) {
        temp_data <- as.data.table(loaded_data_7320_1)
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7320.1 <- temp_data
        } else {
          data$df7320.1 <- copy(DF7320.1)
        }
      } else {
        data$df7320.1 <- copy(DF7320.1)
      }

      # Обработка данных 7320.2
      if (!is.null(loaded_data_7320_2)) {
        temp_data <- as.data.table(loaded_data_7320_2)
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7320.2 <- temp_data
        } else {
          data$df7320.2 <- copy(DF7320.2)
        }
      } else {
        data$df7320.2 <- copy(DF7320.2)
      }

	#********
    
	update_info <- get_last_update(current_session)
	if (!is.null(update_info)) {
		r$current_session_last_update <- update_info$timestamp
		}
      
	message("Данные текущей сессии загружены")
      
	}, error = function(e) {
		message("Ошибка при загрузке данных текущей сессии: ", e$message)
	    })
	  }
  
	  current_session_data <- reactive({
	    r$data_version
	    load_current_session_data()
    
	    return(list(
	      df7010_1 = data$df7010_1,
	      df7010_3 = data$df7010_3,
	      df7110_1 = data$df7110_1,
	      df7110_3 = data$df7110_3,
	      df7210_1 = data$df7210_1,
	      df7210_3 = data$df7210_3,
	      df7310.1 = data$df7310.1,
	      df7310.2 = data$df7310.2,
	      df7320.1 = data$df7320.1,
	      df7320.2 = data$df7320.2
	    ))
	  })
  
	  observe({
	    invalidateLater(3000, session)
	    
	    if (!r$db_initialized) {
	      return()
	    }
    
	    current_session <- session_id()
	    
	    tryCatch({
	      update_info <- get_last_update(current_session)
	      
	      if (!is.null(update_info) && !is.null(r$current_session_last_update)) {
	        if (update_info$timestamp > r$current_session_last_update) {
	          message("Обнаружено обновление данных от пользователя ", update_info$user, ", загружаем...")
	          r$current_session_last_update <- update_info$timestamp
	          r$data_version <- r$data_version + 1
	          
	          session$sendCustomMessage("data_updated", 
	            list(session_id = current_session)
	          )
	        }
	      } else if (!is.null(update_info) && is.null(r$current_session_last_update)) {
	        r$current_session_last_update <- update_info$timestamp
	      }
	      
	    }, error = function(e) {
	      message("Ошибка при проверке обновлений: ", e$message)
	    })
	  })
  
	  observeEvent(input$force_reload, {
	    message("Принудительная перезагрузка данных по запросу клиента")
	    r$data_version <- r$data_version + 1
	  })
	  
		  # обработчик загрузки сессии (расширен для новых таблиц)
		  observeEvent(input$load_session_btn, {
		    req(input$session_selector, input$session_selector != "")
	    
		    selected_session <- input$session_selector
`		    message("Попытка загрузить сеанс: ", selected_session)
    
	r$show_loading <- TRUE
    
	tryCatch({
	showNotification(paste("Загрузка сессии:", selected_session), type = "message")
      
	# Загружаем ОБЪЕДИНЕННЫЕ данные из выбранной сессии (без дубликатов)
	loaded_data_7010_1 <- load_merged_session_data("app_data_7010_1", selected_session)
	loaded_data_7110_1 <- load_merged_session_data("app_data_7110_1", selected_session)
	loaded_data_7210_1 <- load_merged_session_data("app_data_7210_1", selected_session)
	loaded_data_7310_1 <- load_merged_session_data("app_data_7310_1", selected_session)
	loaded_data_7310_2 <- load_merged_session_data("app_data_7310_2", selected_session)
	loaded_data_7320_1 <- load_merged_session_data("app_data_7320_1", selected_session)
	loaded_data_7320_2 <- load_merged_session_data("app_data_7320_2", selected_session)

	data$df7010_3 <- copy(DF7010_3)
	data$df7110_3 <- copy(DF7110_3)
	data$df7210_3 <- copy(DF7210_3)
     
        # Обработка загруженных данных (аналогично load_current_session_data)
	# Обработка данных 7010_1
	if (!is.null(loaded_data_7010_1)) {
	  temp_data <- as.data.table(loaded_data_7010_1)
	  message("ОТЛАДКА: df7010_1 столбцы: ", paste(names(temp_data), collapse = ", "))
	  
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь")) 
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
	    
	    data$df7010_1 <- temp_data
	    message("ОТЛАДКА: успешно обновлен df7010_1 с ", nrow(temp_data), " rows")
	  } else {
	    message("ОТЛАДКА: Несоответствие столбцов в df7010_1.")
	    showNotification("Ошибка: несоответствие столбцов в таблице 7010_1", type = "error")
	  }
	} else {
	  message("ОТЛАДКА: данные для df7010_1 не загружены")
	  data$df7010_1 <- copy(DF7010_1)
	}
	
	# Обработка данных 7110_1
	if (!is.null(loaded_data_7110_1)) {
	  temp_data <- as.data.table(loaded_data_7110_1)
	  message("ОТЛАДКА: df7110_1 столбцы: ", paste(names(temp_data), collapse = ", "))
	  
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
	    
	    if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
	    if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
	    if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
	    if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
	    
	    data$df7110_1 <- temp_data
	    message("ОТЛАДКА: успешно обновлен df7110_1 с ", nrow(temp_data), " rows")
	  } else {
	    message("ОТЛАДКА: Несоответствие столбцов в df7110_1.")
	    showNotification("Ошибка: несоответствие столбцов в таблице 7110_1", type = "error")
	  }
	} else {
	  message("ОТЛАДКА: данные для df7110_1 не загружены")
	  data$df7110_1 <- copy(DF7110_1)
	}
	
	# Обработка данных 7210_1
	if (!is.null(loaded_data_7210_1)) {
	  temp_data <- as.data.table(loaded_data_7210_1)
	  message("ОТЛАДКА: df7210_1 столбцы: ", paste(names(temp_data), collapse = ", "))
	  
	  expected_cols <- c("operation_date", "operation_time", "document_number", "operation_description",
	                    "accounting_method", "initial_balance", "debit", "credit",
	                    "correspondence_debit", "correspondence_credit", "final_balance", "username")
	  
	  if (all(expected_cols %in% names(temp_data))) {
	    setnames(temp_data, expected_cols,
	            c("Дата операции", "Время проводки", "Учетный номер", "Содержание операции",
	              "Метод учета", "Сальдо начальное", "Дебет", "Кредит", 
	              "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
	    
	    if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
	    if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
	    if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
	    if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
	    
	    data$df7210_1 <- temp_data
	    message("ОТЛАДКА: успешно обновлен df7210_1 с ", nrow(temp_data), " rows")
	  } else {
	    message("ОТЛАДКА: Несоответствие столбцов в df7210_1.")
	    showNotification("Ошибка: несоответствие столбцов в таблице 7210_1", type = "error")
	  }
	} else {
	  message("ОТЛАДКА: данные для df7210_1 не загружены")
	  data$df7210_1 <- copy(DF7210_1)
	}

      # Обработка данных 7310.1
      if (!is.null(loaded_data_7310_1)) {
        temp_data <- as.data.table(loaded_data_7310_1)
        message("ОТЛАДКА: df7310.1 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7310.1 <- temp_data
          message("ОТЛАДКА: успешно обновлен df7310.1 с ", nrow(temp_data), " rows")
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df7310.1.")
          showNotification("Ошибка: несоответствие столбцов в таблице 7310.1", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df7310.1 не загружены")
        data$df7310.1 <- copy(DF7310.1)
      }

      # Обработка данных 7310.2
      if (!is.null(loaded_data_7310_2)) {
        temp_data <- as.data.table(loaded_data_7310_2)
        message("ОТЛАДКА: df7310.2 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7310.2 <- temp_data
          message("ОТЛАДКА: успешно обновлен df7310.2 с ", nrow(temp_data), " rows")
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df7310.2.")
          showNotification("Ошибка: несоответствие столбцов в таблице 7310.2", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df7310.2 не загружены")
        data$df7310.2 <- copy(DF7310.2)
      }

      # Обработка данных 7320.1
      if (!is.null(loaded_data_7320_1)) {
        temp_data <- as.data.table(loaded_data_7320_1)
        message("ОТЛАДКА: df7320.1 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7320.1 <- temp_data
          message("ОТЛАДКА: успешно обновлен df7320.1 с ", nrow(temp_data), " rows")
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df7320.1.")
          showNotification("Ошибка: несоответствие столбцов в таблице 7320.1", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df7320.1 не загружены")
        data$df7320.1 <- copy(DF7320.1)
      }

      # Обработка данных 7320.2
      if (!is.null(loaded_data_7320_2)) {
        temp_data <- as.data.table(loaded_data_7320_2)
        message("ОТЛАДКА: df7320.2 столбцы: ", paste(names(temp_data), collapse = ", "))
        
        expected_cols <- c("operation_date", "operation_time", "document_number", "expense_account",
                           "expense_period", "operation_description", "accounting_method",
                           "initial_balance", "credit", "debit", "correspondence_debit",
                           "correspondence_credit", "final_balance", "username")
        
        if (all(expected_cols %in% names(temp_data))) {
          setnames(temp_data, expected_cols,
                   c("Дата операции", "Время проводки", "Учетный номер",
                     "Счет № статьи расхода", "Период расхода",
                     "Содержание операции", "Метод учета", "Сальдо начальное", "Кредит",
                     "Дебет", "Счет № (дебет)", "Счет № (кредит)", "Сальдо конечное", "Пользователь"))
          
          if ("Дата операции" %in% names(temp_data)) temp_data[, `Дата операции` := as.character(`Дата операции`)]
          if ("Время проводки" %in% names(temp_data)) temp_data[, `Время проводки` := as.character(`Время проводки`)]
          if ("Счет № (дебет)" %in% names(temp_data)) temp_data[, `Счет № (дебет)` := as.character(`Счет № (дебет)`)]
          if ("Счет № (кредит)" %in% names(temp_data)) temp_data[, `Счет № (кредит)` := as.character(`Счет № (кредит)`)]
          
          data$df7320.2 <- temp_data
          message("ОТЛАДКА: успешно обновлен df7320.2 с ", nrow(temp_data), " rows")
        } else {
          message("ОТЛАДКА: Несоответствие столбцов в df7320.2.")
          showNotification("Ошибка: несоответствие столбцов в таблице 7320.2", type = "error")
        }
      } else {
        message("ОТЛАДКА: данные для df7320.2 не загружены")
        data$df7320.2 <- copy(DF7320.2)
      }
      
      shinyalert("Успех", paste("Данные сессии загружены в текущую сессию:", session_id()), type = "success")
      
    }, error = function(e) {
      message("ОШИБКА в сеансе загрузки: ", e$message)
      shinyalert("Ошибка", paste("Ошибка при загрузке сессии:", e$message), type = "error")
    }, finally = {
      r$show_loading <- FALSE
    })
  })
  
  observeEvent(input$save_session_btn, {
    if (!r$user_authenticated) {
      shinyalert("Ошибка", "Для сохранения сессии необходимо авторизоваться.", type = "error")
      return()
    }
    
    if (!r$db_initialized) {
      shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить сессию.", type = "error")
      return()
    }
    
	    # Определяем полноту таблиц дебета и кредита (включая новые)
	    debit_complete <- FALSE
	    if (r$debit_account_selected == "7010_1") {
	      debit_complete <- is_table_complete_7010_1(r$debit_table_data)
	    } else if (r$debit_account_selected == "7110_1") {
	      debit_complete <- is_table_complete_7110_1(r$debit_table_data)
	    } else if (r$debit_account_selected == "7210_1") {
	      debit_complete <- is_table_complete_7210_1(r$debit_table_data)
	    } else if (r$debit_account_selected == "7310.1") {
	      debit_complete <- is_table_complete_7310_1(r$debit_table_data)
	    }  else if (r$debit_account_selected == "7310.2") {
	      debit_complete <- is_table_complete_7310_2(r$debit_table_data)
	    } else if (r$debit_account_selected == "7320.1") {
	      debit_complete <- is_table_complete_7320_1(r$debit_table_data)
	    }  else if (r$debit_account_selected == "7320.2") {
	      debit_complete <- is_table_complete_7320_2(r$debit_table_data)
	    }
    
	    credit_complete <- FALSE
	    if (r$credit_account_selected == "7010_1") {
	      credit_complete <- is_table_complete_7010_1(r$credit_table_data)
	    } else if (r$credit_account_selected == "7110_1") {
	      credit_complete <- is_table_complete_7110_1(r$credit_table_data)
	    } else if (r$credit_account_selected == "7210_1") {
	      credit_complete <- is_table_complete_7210_1(r$credit_table_data)
	    } else if (r$credit_account_selected == "7310.1") {
	      credit_complete <- is_table_complete_7310_1(r$credit_table_data)
	    } else if (r$credit_account_selected == "7310.2") {
	      credit_complete <- is_table_complete_7310_2(r$credit_table_data)
	    } else if (r$credit_account_selected == "7320.1") {
	      credit_complete <- is_table_complete_7320_1(r$credit_table_data)
	    } else if (r$credit_account_selected == "7320.2") {
	      credit_complete <- is_table_complete_7320_2(r$credit_table_data)
	    }
   
	if (!debit_complete || !credit_complete) {
		shinyalert("Ошибка", 
                	paste("Обе таблицы (Дебет и Кредит) должны быть полностью заполнены.",
                      		"\nДебет заполнен:", ifelse(debit_complete, "Да", "Нет"),
                      		"\nКредит заполнен:", ifelse(credit_complete, "Да", "Нет")),
                	type = "error")
      	return()
    	}
    
    	if (r$debit_account_selected == "" || r$credit_account_selected == "") {
      	shinyalert("Ошибка", "Необходимо выбрать счета для Дебета и Кредита.", type = "error")
      	return()
    	}
    
    	current_session <- session_id()
    	save_success <- TRUE
    	error_messages <- c()
    
	# Удаление дубликатов (для всех таблиц)
	if (!is.null(data$df7010_1) && nrow(data$df7010_1) > 0) data$df7010_1 <- unique(data$df7010_1)
	if (!is.null(data$df7110_1) && nrow(data$df7110_1) > 0) data$df7110_1 <- unique(data$df7110_1)
	if (!is.null(data$df7210_1) && nrow(data$df7210_1) > 0) data$df7210_1 <- unique(data$df7210_1)
	if (!is.null(data$df7310.1) && nrow(data$df7310.1) > 0) data$df7310.1 <- unique(data$df7310.1)
	if (!is.null(data$df7310.2) && nrow(data$df7310.2) > 0) data$df7310.2 <- unique(data$df7310.2)
	if (!is.null(data$df7320.1) && nrow(data$df7320.1) > 0) data$df7320.1 <- unique(data$df7320.1)
	if (!is.null(data$df7320.2) && nrow(data$df7320.2) > 0) data$df7320.2 <- unique(data$df7320.2)

	# Обработка дебета
	if (r$debit_account_selected == "7010_1") {
	  if (!is.null(data$df7010_1)) {
	    debit_row <- copy(r$debit_table_data)
	    debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    debit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7010_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7010_1), function(i) {
	        all(as.character(debit_row[1, ]) == as.character(data$df7010_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7010_1 <- rbindlist(list(data$df7010_1, debit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7010_1 из таблицы Дебет")
	      } else {
	        message("Строка уже существует в df7010_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7010_1 <- debit_row
	    }
	  }
	} else if (r$debit_account_selected == "7110_1") {
	  if (!is.null(data$df7110_1)) {
	    debit_row <- copy(r$debit_table_data)
	    debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    debit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7110_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7110_1), function(i) {
	        all(as.character(debit_row[1, ]) == as.character(data$df7110_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7110_1 <- rbindlist(list(data$df7110_1, debit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7110_1 из таблицы Дебет")
	      } else {
	        message("Строка уже существует в df7110_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7110_1 <- debit_row
	    }
	  }
	} else if (r$debit_account_selected == "7210_1") {
	  if (!is.null(data$df7210_1)) {
	    debit_row <- copy(r$debit_table_data)
	    debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    debit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7210_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7210_1), function(i) {
	        all(as.character(debit_row[1, ]) == as.character(data$df7210_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7210_1 <- rbindlist(list(data$df7210_1, debit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7210_1 из таблицы Дебет")
	      } else {
	        message("Строка уже существует в df7210_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7210_1 <- debit_row
	    }
	  }
	} else if (r$debit_account_selected == "7310.1") {
          if (!is.null(data$df7310.1)) {
            debit_row <- copy(r$debit_table_data)
            debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            debit_row[, Пользователь := r$current_user]
          
            if (nrow(data$df7310.1) > 0) {
              is_duplicate <- any(sapply(1:nrow(data$df7310.1), function(i) {
                all(as.character(debit_row[1, ]) == as.character(data$df7310.1[i, ]), na.rm = TRUE)
              }))
            
              if (!is_duplicate) {
                data$df7310.1 <- rbindlist(list(data$df7310.1, debit_row), use.names = TRUE, fill = TRUE)
                message("Добавлена новая строка в df7310.1 из таблицы Дебет")
              } else {
                message("Строка уже существует в df7310.1, не добавляем дубликат")
              }
            } else {
              data$df7310.1 <- debit_row
            }
          }
        } else if (r$debit_account_selected == "7310.2") {
          if (!is.null(data$df7310.2)) {
            debit_row <- copy(r$debit_table_data)
            debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            debit_row[, Пользователь := r$current_user]
          
            if (nrow(data$df7310.2) > 0) {
              is_duplicate <- any(sapply(1:nrow(data$df7310.2), function(i) {
                all(as.character(debit_row[1, ]) == as.character(data$df7310.2[i, ]), na.rm = TRUE)
              }))
            
              if (!is_duplicate) {
                data$df7310.2 <- rbindlist(list(data$df7310.2, debit_row), use.names = TRUE, fill = TRUE)
                message("Добавлена новая строка в df7310.2 из таблицы Дебет")
              } else {
                message("Строка уже существует в df7310.2, не добавляем дубликат")
              }
            } else {
              data$df7310.2 <- debit_row
            }
          }
        } else if (r$debit_account_selected == "7320.1") {
          if (!is.null(data$df7320.1)) {
            debit_row <- copy(r$debit_table_data)
            debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            debit_row[, Пользователь := r$current_user]
          
            if (nrow(data$df7320.1) > 0) {
              is_duplicate <- any(sapply(1:nrow(data$df7320.1), function(i) {
                all(as.character(debit_row[1, ]) == as.character(data$df7320.1[i, ]), na.rm = TRUE)
              }))
            
              if (!is_duplicate) {
                data$df7320.1 <- rbindlist(list(data$df7320.1, debit_row), use.names = TRUE, fill = TRUE)
                message("Добавлена новая строка в df7320.1 из таблицы Дебет")
              } else {
                message("Строка уже существует в df7320.1, не добавляем дубликат")
              }
            } else {
              data$df7320.1 <- debit_row
            }
          }
        } else if (r$debit_account_selected == "7320.2") {
          if (!is.null(data$df7320.2)) {
            debit_row <- copy(r$debit_table_data)
            debit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            debit_row[, Пользователь := r$current_user]
          
            if (nrow(data$df7320.2) > 0) {
              is_duplicate <- any(sapply(1:nrow(data$df7320.2), function(i) {
                all(as.character(debit_row[1, ]) == as.character(data$df7320.2[i, ]), na.rm = TRUE)
              }))
            
              if (!is_duplicate) {
                data$df7320.2 <- rbindlist(list(data$df7320.2, debit_row), use.names = TRUE, fill = TRUE)
                message("Добавлена новая строка в df7320.2 из таблицы Дебет")
              } else {
                message("Строка уже существует в df7320.2, не добавляем дубликат")
              }
            } else {
              data$df7320.2 <- debit_row
            }
          }
        }

  	# Обработка кредита
	if (r$credit_account_selected == "7010_1") {
	  if (!is.null(data$df7010_1)) {
	    credit_row <- copy(r$credit_table_data)
	    credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    credit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7010_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7010_1), function(i) {
	        all(as.character(credit_row[1, ]) == as.character(data$df7010_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7010_1 <- rbindlist(list(data$df7010_1, credit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7010_1 из таблицы Кредит")
	      } else {
	        message("Строка уже существует в df7010_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7010_1 <- credit_row
	    }
	  }
	} else if (r$credit_account_selected == "7110_1") {
	  if (!is.null(data$df7110_1)) {
	    credit_row <- copy(r$credit_table_data)
	    credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    credit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7110_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7110_1), function(i) {
	        all(as.character(credit_row[1, ]) == as.character(data$df7110_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7110_1 <- rbindlist(list(data$df7110_1, credit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7110_1 из таблицы Кредит")
	      } else {
	        message("Строка уже существует в df7110_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7110_1 <- credit_row
	    }
	  }
	} else if (r$credit_account_selected == "7210_1") {
	  if (!is.null(data$df7210_1)) {
	    credit_row <- copy(r$credit_table_data)
	    credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
	    credit_row[, Пользователь := r$current_user]
	    
	    if (nrow(data$df7210_1) > 0) {
	      is_duplicate <- any(sapply(1:nrow(data$df7210_1), function(i) {
	        all(as.character(credit_row[1, ]) == as.character(data$df7210_1[i, ]), na.rm = TRUE)
	      }))
	      
	      if (!is_duplicate) {
	        data$df7210_1 <- rbindlist(list(data$df7210_1, credit_row), use.names = TRUE, fill = TRUE)
	        message("Добавлена новая строка в df7210_1 из таблицы Кредит")
	      } else {
	        message("Строка уже существует в df7210_1, не добавляем дубликат")
	      }
	    } else {
	      data$df7210_1 <- credit_row
	    }
	  }
	} else if (r$credit_account_selected == "7310.1") {
          if (!is.null(data$df7310.1)) {
            credit_row <- copy(r$credit_table_data)
            credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            credit_row[, Пользователь := r$current_user]
          
          if (nrow(data$df7310.1) > 0) {
            is_duplicate <- any(sapply(1:nrow(data$df7310.1), function(i) {
              all(as.character(credit_row[1, ]) == as.character(data$df7310.1[i, ]), na.rm = TRUE)
            }))
            
            if (!is_duplicate) {
              data$df7310.1 <- rbindlist(list(data$df7310.1, credit_row), use.names = TRUE, fill = TRUE)
              message("Добавлена новая строка в df7310.1 из таблицы Кредит")
            } else {
              message("Строка уже существует в df7310.1, не добавляем дубликат")
            }
          } else {
              data$df7310.1 <- credit_row
            }
          }
        } else if (r$credit_account_selected == "7310.2") {
          if (!is.null(data$df7310.2)) {
            credit_row <- copy(r$credit_table_data)
            credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            credit_row[, Пользователь := r$current_user]
          
          if (nrow(data$df7310.2) > 0) {
            is_duplicate <- any(sapply(1:nrow(data$df7310.2), function(i) {
              all(as.character(credit_row[1, ]) == as.character(data$df7310.2[i, ]), na.rm = TRUE)
            }))
            
            if (!is_duplicate) {
              data$df7310.2 <- rbindlist(list(data$df7310.2, credit_row), use.names = TRUE, fill = TRUE)
              message("Добавлена новая строка в df7310.2 из таблицы Кредит")
            } else {
              message("Строка уже существует в df7310.2, не добавляем дубликат")
            }
          } else {
              data$df7310.2 <- credit_row
            }
          }
        } else if (r$credit_account_selected == "7320.1") {
          if (!is.null(data$df7320.1)) {
            credit_row <- copy(r$credit_table_data)
            credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            credit_row[, Пользователь := r$current_user]
          
          if (nrow(data$df7320.1) > 0) {
            is_duplicate <- any(sapply(1:nrow(data$df7320.1), function(i) {
              all(as.character(credit_row[1, ]) == as.character(data$df7320.1[i, ]), na.rm = TRUE)
            }))
            
            if (!is_duplicate) {
              data$df7320.1 <- rbindlist(list(data$df7320.1, credit_row), use.names = TRUE, fill = TRUE)
              message("Добавлена новая строка в df7320.1 из таблицы Кредит")
            } else {
              message("Строка уже существует в df7320.1, не добавляем дубликат")
            }
          } else {
              data$df7320.1 <- credit_row
            }
          }
        } else if (r$credit_account_selected == "7320.2") {
          if (!is.null(data$df7320.2)) {
            credit_row <- copy(r$credit_table_data)
            credit_row[, `Сальдо конечное` := `Сальдо начальное` - `Кредит` + `Дебет`]
            credit_row[, Пользователь := r$current_user]
          
          if (nrow(data$df7320.2) > 0) {
            is_duplicate <- any(sapply(1:nrow(data$df7320.2), function(i) {
              all(as.character(credit_row[1, ]) == as.character(data$df7320.2[i, ]), na.rm = TRUE)
            }))
            
            if (!is_duplicate) {
              data$df7320.2 <- rbindlist(list(data$df7320.2, credit_row), use.names = TRUE, fill = TRUE)
              message("Добавлена новая строка в df7320.2 из таблицы Кредит")
            } else {
              message("Строка уже существует в df7320.2, не добавляем дубликат")
            }
          } else {
              data$df7320.2 <- credit_row
            }
          }
        }
    
    # Удаляем дубликаты еще раз после добавления новых строк

    if (!is.null(data$df7010_1) && nrow(data$df7010_1) > 0) {
      data$df7010_1 <- unique(data$df7010_1)
    }

    if (!is.null(data$df7110_1) && nrow(data$df7110_1) > 0) {
      data$df7110_1 <- unique(data$df7110_1)
    }

    if (!is.null(data$df7210_1) && nrow(data$df7210_1) > 0) {
      data$df7210_1 <- unique(data$df7210_1)
    }

    if (!is.null(data$df7310.1) && nrow(data$df7310.1) > 0) {
      data$df7310.1 <- unique(data$df7310.1)
    }

    if (!is.null(data$df7310.2) && nrow(data$df7310.2) > 0) {
      data$df7310.2 <- unique(data$df7310.2)
    }

    if (!is.null(data$df7320.1) && nrow(data$df7320.1) > 0) {
      data$df7320.1 <- unique(data$df7320.1)
    }

    if (!is.null(data$df7320.2) && nrow(data$df7320.2) > 0) {
      data$df7320.2 <- unique(data$df7320.2)
    }
   
    # Сохранение данных

    if (!is.null(data$df7010_1)) {
      success <- save_data_simple(data$df7010_1, "app_data_7010_1", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7010_1")
      }
    }

    if (!is.null(data$df7110_1)) {
      success <- save_data_simple(data$df7110_1, "app_data_7110_1", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7110_1")
      }
    }

    if (!is.null(data$df7210_1)) {
      success <- save_data_simple(data$df7210_1, "app_data_7210_1", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7210_1")
      }
    }

    if (!is.null(data$df7310.1)) {
      success <- save_data_simple(data$df7310.1, "app_data_7310_1", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7310.1")
      }
    }

    if (!is.null(data$df7310.2)) {
      success <- save_data_simple(data$df7310.2, "app_data_7310_2", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7310.2")
      }
    }

    if (!is.null(data$df7320.1)) {
      success <- save_data_simple(data$df7320.1, "app_data_7320_1", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7320.1")
      }
    }

    if (!is.null(data$df7320.2)) {
      success <- save_data_simple(data$df7320.2, "app_data_7320_2", current_session, r$current_user)
      if (!success) {
        save_success <- FALSE
        error_messages <- c(error_messages, "Ошибка сохранения таблицы 7320.2")
      }
    }


    if (save_success) {
      r$data_version <- r$data_version + 1
      update_info <- get_last_update(current_session)
      if (!is.null(update_info)) {
        r$current_session_last_update <- update_info$timestamp
      }
      
	# Reset table data based on specific account
	if (r$debit_account_selected == "7010_1") {
	  r$debit_table_data <- DF_empty_debit_7010
	} else if (r$debit_account_selected == "7110_1") {
	  r$debit_table_data <- DF_empty_debit_7110
	} else if (r$debit_account_selected == "7210_1") {
	  r$debit_table_data <- DF_empty_debit_7210
	} else if (r$debit_account_selected == "7310.1") {
	  r$debit_table_data <- DF_empty_debit_7310_1
	} else if (r$debit_account_selected == "7310.2") {
	  r$debit_table_data <- DF_empty_debit_7310_2
	} else if (r$debit_account_selected == "7320.1") {
	  r$debit_table_data <- DF_empty_debit_7320_1
	} else if (r$debit_account_selected == "7320.2") {
	  r$debit_table_data <- DF_empty_debit_7320_2
	} else {
	  r$debit_table_data <- DF_empty_debit
	}

	if (r$credit_account_selected == "7010_1") {
	  r$credit_table_data <- DF_empty_credit_7010
	} else if (r$credit_account_selected == "7110_1") {
	  r$credit_table_data <- DF_empty_credit_7110
	} else if (r$credit_account_selected == "7210_1") {
	  r$credit_table_data <- DF_empty_credit_7210
	} else if (r$credit_account_selected == "7310.1") {
	  r$credit_table_data <- DF_empty_credit_7310_1
	} else if (r$credit_account_selected == "7310.2") {
	  r$credit_table_data <- DF_empty_credit_7310_2
	} else if (r$credit_account_selected == "7320.1") {
	  r$credit_table_data <- DF_empty_credit_7320_1
	} else if (r$credit_account_selected == "7320.2") {
	  r$credit_table_data <- DF_empty_credit_7320_2
	} else {
	  r$credit_table_data <- DF_empty_credit
	}
      
      	r$debit_account_selected <- ""
      	r$credit_account_selected <- ""
      
      	updateSelectInput(session, "debit_select", selected = "")
      	updateSelectInput(session, "credit_select", selected = "")
      
      		shinyalert("Успех", paste("Изменения пользователя", r$current_user, "сохранены в сессии:", current_session), type = "success")
    	} else {
      		error_msg <- paste("Ошибка сохранения данных:", paste(error_messages, collapse = "; "))
      		shinyalert("Ошибка", error_msg, type = "error")
    		}
  	})

	#**********

	# Обработчик сохранения таблицы 7010_1 (per‑user saving)
	observeEvent(input$save_table7010_1, {
	  if (!r$user_authenticated) {
	    shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
	    return()
	  }
	  
	  if (!r$db_initialized) {
	    shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
	    return()
	  }
	  
	  current_session <- session_id()
	  
	  # Получаем текущие данные из таблицы (merged view)
	  if (!is.null(input$table7010_1Item1)) {
	    tryCatch({
	      # Обновляем данные из таблицы
	      new_df <- hot_to_r(input$table7010_1Item1)
	      validation_result <- validate_operation_dates(new_df)
	      if (!validation_result$valid) {
	        data$df7010_1 <- validation_result$df
	      } else {
	        data$df7010_1 <- new_df
	      }
	    }, error = function(e) {
	      message("Ошибка при обновлении данных из таблицы 7010_1: ", e$message)
	    })
	  }
	  
	  # Filter to keep only rows belonging to the current user
	  user_df <- data$df7010_1[data$df7010_1$Пользователь == r$current_user, ]

	  success <- save_data_simple(user_df, "app_data_7010_1", current_session, r$current_user)
	  
	  if (success) {
	    shinyalert("Успех", paste("Данные таблицы 7010_1 сохранены для пользователя", r$current_user), type = "success")
	    r$data_version <- r$data_version + 1
	    
	    # Обновляем timestamp
	    update_info <- get_last_update(current_session)
	    if (!is.null(update_info)) {
	      r$current_session_last_update <- update_info$timestamp
	    }
	  } else {
	    shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7010_1.", type = "error")
	  }
	})
	
	# Обработчик сохранения таблицы 7110_1 (per‑user saving)
	observeEvent(input$save_table7110_1, {
	  if (!r$user_authenticated) {
	    shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
	    return()
	  }
	  
	  if (!r$db_initialized) {
	    shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
	    return()
	  }
	  
	  current_session <- session_id()
	  
	  # Получаем текущие данные из таблицы (merged view)
	  if (!is.null(input$table7110_1Item1)) {
	    tryCatch({
	      # Обновляем данные из таблицы
	      new_df <- hot_to_r(input$table7110_1Item1)
	      validation_result <- validate_operation_dates(new_df)
	      if (!validation_result$valid) {
	        data$df7110_1 <- validation_result$df
	      } else {
	        data$df7110_1 <- new_df
	      }
	    }, error = function(e) {
	      message("Ошибка при обновлении данных из таблицы 7110_1: ", e$message)
	    })
	  }
	  
	  # Filter to keep only rows belonging to the current user
	  user_df <- data$df7110_1[data$df7110_1$Пользователь == r$current_user, ]
	  
	  success <- save_data_simple(user_df, "app_data_7110_1", current_session, r$current_user)
	  
	  if (success) {
	    shinyalert("Успех", paste("Данные таблицы 7110_1 сохранены для пользователя", r$current_user), type = "success")
	    r$data_version <- r$data_version + 1
	    
	    # Обновляем timestamp
	    update_info <- get_last_update(current_session)
	    if (!is.null(update_info)) {
	      r$current_session_last_update <- update_info$timestamp
	    }
	  } else {
	    shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7110_1.", type = "error")
	  }
	})
	
	# Обработчик сохранения таблицы 7210_1 (per‑user saving)
	observeEvent(input$save_table7210_1, {
	  if (!r$user_authenticated) {
	    shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
	    return()
	  }
	  
	  if (!r$db_initialized) {
	    shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
	    return()
	  }
	  
	  current_session <- session_id()
	  
	  # Получаем текущие данные из таблицы (merged view)
	  if (!is.null(input$table7210_1Item1)) {
	    tryCatch({
	      # Обновляем данные из таблицы
	      new_df <- hot_to_r(input$table7210_1Item1)
	      validation_result <- validate_operation_dates(new_df)
	      if (!validation_result$valid) {
	        data$df7210_1 <- validation_result$df
	      } else {
	        data$df7210_1 <- new_df
	      }
	    }, error = function(e) {
	      message("Ошибка при обновлении данных из таблицы 7210_1: ", e$message)
	    })
	  }
	  
	  # Filter to keep only rows belonging to the current user
	  user_df <- data$df7210_1[data$df7210_1$Пользователь == r$current_user, ]
	  
	  success <- save_data_simple(user_df, "app_data_7210_1", current_session, r$current_user)
	  
	  if (success) {
	    shinyalert("Успех", paste("Данные таблицы 7210_1 сохранены для пользователя", r$current_user), type = "success")
	    r$data_version <- r$data_version + 1
	    
	    # Обновляем timestamp
	    update_info <- get_last_update(current_session)
	    if (!is.null(update_info)) {
	      r$current_session_last_update <- update_info$timestamp
	    }
	  } else {
	    shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7210_1.", type = "error")
	  }
	})

      # Обработчик сохранения таблицы 7310.1
      observeEvent(input$save_table7310_1, {
        if (!r$user_authenticated) {
          shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
          return()
        }
        
        if (!r$db_initialized) {
          shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
          return()
        }
        
        current_session <- session_id()
        
        if (!is.null(input$table7310.1Item1)) {
          tryCatch({
            new_df <- hot_to_r(input$table7310.1Item1)
            validation_result <- validate_operation_dates(new_df)
            if (!validation_result$valid) {
              data$df7310.1 <- validation_result$df
            } else {
              data$df7310.1 <- new_df
            }
          }, error = function(e) {
            message("Ошибка при обновлении данных из таблицы 7310.1: ", e$message)
          })
        }
        
        user_df <- data$df7310.1[data$df7310.1$Пользователь == r$current_user, ]
        success <- save_data_simple(user_df, "app_data_7310_1", current_session, r$current_user)
        
        if (success) {
          shinyalert("Успех", paste("Данные таблицы 7310.1 сохранены для пользователя", r$current_user), type = "success")
          r$data_version <- r$data_version + 1
          update_info <- get_last_update(current_session)
          if (!is.null(update_info)) {
            r$current_session_last_update <- update_info$timestamp
          }
        } else {
          shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7310.1.", type = "error")
        }
      })

      # Обработчик сохранения таблицы 7310.2
      observeEvent(input$save_table7310_2, {
        if (!r$user_authenticated) {
          shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
          return()
        }
        
        if (!r$db_initialized) {
          shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
          return()
        }
        
        current_session <- session_id()
        
        if (!is.null(input$table7310.2Item1)) {
          tryCatch({
            new_df <- hot_to_r(input$table7310.2Item1)
            validation_result <- validate_operation_dates(new_df)
            if (!validation_result$valid) {
              data$df7310.2 <- validation_result$df
            } else {
              data$df7310.2 <- new_df
            }
          }, error = function(e) {
            message("Ошибка при обновлении данных из таблицы 7310.2: ", e$message)
          })
        }
        
        user_df <- data$df7310.2[data$df7310.2$Пользователь == r$current_user, ]
        success <- save_data_simple(user_df, "app_data_7310_2", current_session, r$current_user)
        
        if (success) {
          shinyalert("Успех", paste("Данные таблицы 7310.2 сохранены для пользователя", r$current_user), type = "success")
          r$data_version <- r$data_version + 1
          update_info <- get_last_update(current_session)
          if (!is.null(update_info)) {
            r$current_session_last_update <- update_info$timestamp
          }
        } else {
          shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7310.2.", type = "error")
        }
      })

      # Обработчик сохранения таблицы 7320.1
      observeEvent(input$save_table7320_1, {
        if (!r$user_authenticated) {
          shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
          return()
        }
        
        if (!r$db_initialized) {
          shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
          return()
        }
        
        current_session <- session_id()
        
        if (!is.null(input$table7320.1Item1)) {
          tryCatch({
            new_df <- hot_to_r(input$table7320.1Item1)
            validation_result <- validate_operation_dates(new_df)
            if (!validation_result$valid) {
              data$df7320.1 <- validation_result$df
            } else {
              data$df7320.1 <- new_df
            }
          }, error = function(e) {
            message("Ошибка при обновлении данных из таблицы 7320.1: ", e$message)
          })
        }
        
        user_df <- data$df7320.1[data$df7320.1$Пользователь == r$current_user, ]
        success <- save_data_simple(user_df, "app_data_7320_1", current_session, r$current_user)
        
        if (success) {
          shinyalert("Успех", paste("Данные таблицы 7320.1 сохранены для пользователя", r$current_user), type = "success")
          r$data_version <- r$data_version + 1
          update_info <- get_last_update(current_session)
          if (!is.null(update_info)) {
            r$current_session_last_update <- update_info$timestamp
          }
        } else {
          shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7320.1.", type = "error")
        }
      })

      # Обработчик сохранения таблицы 7320.2
      observeEvent(input$save_table7320_2, {
        if (!r$user_authenticated) {
          shinyalert("Ошибка", "Для сохранения необходимо авторизоваться.", type = "error")
          return()
        }
        
        if (!r$db_initialized) {
          shinyalert("Ошибка", "База данных недоступна. Невозможно сохранить таблицу.", type = "error")
          return()
        }
        
        current_session <- session_id()
        
        if (!is.null(input$table7320.2Item1)) {
          tryCatch({
            new_df <- hot_to_r(input$table7320.2Item1)
            validation_result <- validate_operation_dates(new_df)
            if (!validation_result$valid) {
              data$df7320.2 <- validation_result$df
            } else {
              data$df7320.2 <- new_df
            }
          }, error = function(e) {
            message("Ошибка при обновлении данных из таблицы 7320.2: ", e$message)
          })
        }
        
        user_df <- data$df7320.2[data$df7320.2$Пользователь == r$current_user, ]
        success <- save_data_simple(user_df, "app_data_7320_2", current_session, r$current_user)
        
        if (success) {
          shinyalert("Успех", paste("Данные таблицы 7320.2 сохранены для пользователя", r$current_user), type = "success")
          r$data_version <- r$data_version + 1
          update_info <- get_last_update(current_session)
          if (!is.null(update_info)) {
            r$current_session_last_update <- update_info$timestamp
          }
        } else {
          shinyalert("Ошибка", "Не удалось сохранить данные таблицы 7320.2.", type = "error")
        }
      })

	  observeEvent(input$test_connection, {
	    if (test_database_connection()) {
	      shinyalert("Успех", "Подключение к базе данных установлено успешно!", type = "success")
	      r$db_initialized <- TRUE
	      r$sessions_updated <- r$sessions_updated + 1
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
	  
	  output$current_session_date <- renderText({
	    paste("Дата сессии:", format(Sys.Date(), "%Y-%m-%d"))
	  })

#********

# БАЗОВЫЙ КОД


  observe({
    if(!is.null(input$table7010Item3))
      data$df7010_3 <- hot_to_r(input$table7010Item3)
  })

	observe({
	  if(!is.null(input$table7010_1Item1)) {
	    if (!r$user_authenticated) {
	      shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	      return()
	    }
	    
	    new_df <- hot_to_r(input$table7010_1Item1)
	    validation_result <- validate_operation_dates(new_df)
	    if (!validation_result$valid) {
	      data$df7010_1 <- validation_result$df
	    } else {
	      data$df7010_1 <- new_df
	    }
	  }
	})

  observe({
    if(!is.null(input$table7110Item3))
      data$df7110_3 <- hot_to_r(input$table7110Item3)
  })
	
	observe({
	  if(!is.null(input$table7110_1Item1)) {
	    if (!r$user_authenticated) {
	      shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	      return()
	    }
	    
	    new_df <- hot_to_r(input$table7110_1Item1)
	    validation_result <- validate_operation_dates(new_df)
	    if (!validation_result$valid) {
	      data$df7110_1 <- validation_result$df
	    } else {
	      data$df7110_1 <- new_df
	    }
	  }
	})

	observe({
	  if(!is.null(input$table7210Item3))
	     data$df7210_3 <- hot_to_r(input$table7210Item3)
	})
	
	observe({
	  if(!is.null(input$table7210_1Item1)) {
	    if (!r$user_authenticated) {
	      shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	      return()
	    }
	    
	    new_df <- hot_to_r(input$table7210_1Item1)
	    validation_result <- validate_operation_dates(new_df)
	    if (!validation_result$valid) {
	      data$df7210_1 <- validation_result$df
	    } else {
	      data$df7210_1 <- new_df
	    }
	  }
	})

	observe({
	        if(!is.null(input$table7310.1Item1)) {
	            if (!r$user_authenticated) {
	                shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	                return()
	            }
            
	            new_df <- hot_to_r(input$table7310.1Item1)
	            validation_result <- validate_operation_dates(new_df)
	            if (!validation_result$valid) {
	                data$df7310.1 <- validation_result$df
	            } else {
	                data$df7310.1 <- new_df
	            }
	        }
	    })

	observe({
	        if(!is.null(input$table7310.2Item1)) {
	            if (!r$user_authenticated) {
	                shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	                return()
	            }
            
	            new_df <- hot_to_r(input$table7310.2Item1)
	            validation_result <- validate_operation_dates(new_df)
	            if (!validation_result$valid) {
	                data$df7310.2 <- validation_result$df
	            } else {
	                data$df7310.2 <- new_df
	            }
	        }
	    })

	observe({
	        if(!is.null(input$table7320.1Item1)) {
	            if (!r$user_authenticated) {
	                shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	                return()
	            }
            
	            new_df <- hot_to_r(input$table7320.1Item1)
	            validation_result <- validate_operation_dates(new_df)
	            if (!validation_result$valid) {
	                data$df7320.1 <- validation_result$df
	            } else {
	                data$df7320.1 <- new_df
	            }
	        }
	    })

	observe({
	        if(!is.null(input$table7320.2Item1)) {
	            if (!r$user_authenticated) {
	                shinyalert("Ошибка", "Для внесения изменений необходимо авторизоваться.", type = "error")
	                return()
	            }
            
	            new_df <- hot_to_r(input$table7320.2Item1)
	            validation_result <- validate_operation_dates(new_df)
	            if (!validation_result$valid) {
	                data$df7320.2 <- validation_result$df
	            } else {
	                data$df7320.2 <- new_df
	            }
	        }
	    })
#*********

#ОСВ: 7000

observeEvent(input$dates7000, {
    start <- ymd(input$dates7000[[1]])
    end <- ymd(input$dates7000[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7000", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7000[[1]]
      r$end <- input$dates7000[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7000",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates7000))) {
      from=as.Date(input$dates7000[1L])
      to=as.Date(input$dates7000[2L])
      if (from>to) to = from
      selectdates7000_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df7010_4 <- data$df7010_1[as.Date(data$df7010_1$`Дата операции`) %in% selectdates7000_1, ]
    } else {
      selectdates7000_2 <- unique(as.Date(data$df7010_1$`Дата операции`))
      data$df7010_4 <- data$df7010_1[data$df7010_1$`Дата операции` %in% selectdates7000_2, ]
    }
  })

  observe({
    if(!is.null(input$table7010Item1) && !any(is.na(input$table7010Item1)))
      data$df7010_4 <- hot_to_r(input$table7010Item1)
  })

observe({
   if (nrow(data$df7010_4) > 0) {
  data$df7010_3[1, 2:5] <- data$df7010_4[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by="Учетный номер"][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
  } else {
    data$df7010_3[1, 2:5] <- 0
  }
})

  output$nested_ui7000 <- renderUI({!any(is.na(input$dates7000))})

  output$table7010Item3 <- renderRHandsontable({
    rhandsontable(data$df7010_3, colWidths = 150, height = 70, readOnly=TRUE, contextMenu = FALSE,
		fixedColumnsLeft = 1, manualColumnResize = TRUE, dragColumns = FALSE) |>
	hot_col(1, width = 450)
  })

  output$download_df7010_3 <- downloadHandler(
    filename = function() { "df7010_3.xlsx" },
    content = function(file) {
      write.xlsx(data$df7010_3, file)
  })

#**********

#7010

observeEvent(input$dates7010, {
    start <- ymd(input$dates7010[[1]])
    end <- ymd(input$dates7010[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7010", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7010[[1]]
      r$end <- input$dates7010[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7010",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

  observe({ 
    if (!is.null(input$table7010Item1)) {
      if (!r$user_authenticated) {
        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
        return()
      }
      
      data$df7010_1 <- hot_to_r(input$table7010Item1)

    if (!any(is.na(input$dates7010)) && input$choices7010 == "Выбор по дате операции") {
     	from=as.Date(input$dates7010[1L])
      	to=as.Date(input$dates7010[2L])
      	if (from>to) to = from
      	selectdates7010_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df7010_2 <- data$df7010_1[as.Date(data$df7010_1$"Дата операции") %in% selectdates7010_1, ]
    } else if (!is.null(input$text) && input$choices7010 == "Выбор по учетному номеру") {
      	data$df7010_2 <- data$df7010_1[data$df7010_1$"Учетный номер" == input$text, ]
    } else if (!is.null(input$dates7010) && !any(is.na(input$dates7010)) && !is.null(input$text) && input$choices7010 == "Выбор по дате операции и учетному номеру") {
     	from=as.Date(input$dates7010[1L])
      	to=as.Date(input$dates7010[2L])
      	if (from>to) to = from
      	selectdates7010_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df7010_2 <- data$df7010_1[as.Date(data$df7010_1$"Дата операции") %in% selectdates7010_2 & data$df7010_1$"Учетный номер" == input$text, ]
    } else {
        selectdates7010_4 <- unique(data$df7010_1$"Дата операции")
        data$df7010_2 <- data$df7010_1[data$df7010_1$"Дата операции" %in% selectdates7010_4, ]
    }
}
})  

  output$table7010Item1 <- renderRHandsontable({

    data$df7010_1[, `Сальдо конечное` := data$df7010_1[[6]] + data$df7010_1[[7]] - data$df7010_1[[8]]]

    rhandsontable(data$df7010_1, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2,
		manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date") |>
      hot_col("Пользователь", readOnly = TRUE)
  })

  output$nested_ui7010 <- renderUI({
    if (input$choices7010 == "Выбор по дате операции") {
      	dateRangeInput("dates7010", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices7010 == "Выбор по учетному номеру") {
      	textInput("text", "Укажите учетный номер:")
    } else if (input$choices7010 == "Выбор по дате операции и учетному номеру") {
      fluidRow(
       	dateRangeInput("dates7010", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите учетный номер:")
      )
    }
  })

  output$table7010Item2 <- renderRHandsontable({

    rhandsontable(data$df7010_2, colWidths = 150, height = 500, readOnly=TRUE, 
		contextMenu = FALSE, manualColumnResize = TRUE, dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df7010 <- downloadHandler(
    filename = function() { "df7010.xlsx" },
    content = function(file) {
      write.xlsx(data$df7010, file)
  })

  output$download_df7010_2 <- downloadHandler(
    filename = function() { "df7010_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df7010_2, file)
  })

#********

#ОСВ: 7100

observeEvent(input$dates7100, {
    start <- ymd(input$dates7100[[1]])
    end <- ymd(input$dates7100[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7100", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7100[[1]]
      r$end <- input$dates7100[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7100",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates7100))) {
      from=as.Date(input$dates7100[1L])
      to=as.Date(input$dates7100[2L])
      if (from>to) to = from
      selectdates7100_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df7110_4 <- data$df7110_1[as.Date(data$df7110_1$`Дата операции`) %in% selectdates7100_1, ]
    } else {
      selectdates7100_2 <- unique(as.Date(data$df7110_1$`Дата операции`))
      data$df7110_4 <- data$df7110_1[data$df7110_1$`Дата операции` %in% selectdates7100_2, ]
    }
  })

  observe({
    if(!is.null(input$table7110Item1) && !any(is.na(input$table7110Item1)))
      data$df7110_4 <- hot_to_r(input$table7110Item1)
  })

observe({
   if (nrow(data$df7110_4) > 0) {
  data$df7110_3[1, 2:5] <- data$df7110_4[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by="Учетный номер"][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
  } else {
    data$df7110_3[1, 2:5] <- 0
  }
})

  output$nested_ui7100 <- renderUI({!any(is.na(input$dates7100))})

  output$table7110Item3 <- renderRHandsontable({
    rhandsontable(data$df7110_3, colWidths = 150, height = 70, readOnly=TRUE, contextMenu = FALSE, 
		fixedColumnsLeft = 1, manualColumnResize = TRUE, dragColumns = FALSE) |>
	hot_col(1, width = 450)
  })

  output$download_df7110_3 <- downloadHandler(
    filename = function() { "df7110_3.xlsx" },
    content = function(file) {
      write.xlsx(data$df7110_3, file)
  })

#*******

#7110

observeEvent(input$dates7110, {
    start <- ymd(input$dates7110[[1]])
    end <- ymd(input$dates7110[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7110", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7110[[1]]
      r$end <- input$dates7110[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7110",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

  observe({ 
    if (!is.null(input$table7110Item1)) {
      if (!r$user_authenticated) {
        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
        return()
      }
      
      data$df7110_1 <- hot_to_r(input$table7110Item1)

    if (!any(is.na(input$dates7110)) && input$choices7110 == "Выбор по дате операции") {
     	from=as.Date(input$dates7110[1L])
      	to=as.Date(input$dates7110[2L])
      	if (from>to) to = from
      	selectdates7110_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df7110_2 <- data$df7110_1[as.Date(data$df7110_1$"Дата операции") %in% selectdates7110_1, ]
    } else if (!is.null(input$text) && input$choices7110 == "Выбор по учетному номеру") {
      	data$df7110_2 <- data$df7110_1[data$df7110_1$"Учетный номер" == input$text, ]
    } else if (!is.null(input$dates7110) && !any(is.na(input$dates7110)) && !is.null(input$text) && input$choices7110 == "Выбор по дате операции и учетному номеру") {
     	from=as.Date(input$dates7110[1L])
      	to=as.Date(input$dates7110[2L])
      	if (from>to) to = from
      	selectdates7110_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df7110_2 <- data$df7110_1[as.Date(data$df7110_1$"Дата операции") %in% selectdates7110_2 & data$df7110_1$"Учетный номер" == input$text, ]
    } else {
        selectdates7110_4 <- unique(data$df7110_1$"Дата операции")
        data$df7110_2 <- data$df7110_1[data$df7110_1$"Дата операции" %in% selectdates7110_4, ]
    }
}
})  

  output$table7110Item1 <- renderRHandsontable({

    data$df7110_1[, `Сальдо конечное` := data$df7110_1[[6]] + data$df7110_1[[7]] - data$df7110_1[[8]]]

    rhandsontable(data$df7110_1, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2, 
		manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date") |>
      hot_col("Пользователь", readOnly = TRUE)
  })

  output$nested_ui7110 <- renderUI({
    if (input$choices7110 == "Выбор по дате операции") {
      	dateRangeInput("dates7110", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices7110 == "Выбор по учетному номеру") {
      	textInput("text", "Укажите учетный номер:")
    } else if (input$choices7110 == "Выбор по дате операции и учетному номеру") {
      fluidRow(
       	dateRangeInput("dates7110", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите учетный номер:")
      )
    }
  })

  output$table7110Item2 <- renderRHandsontable({

    rhandsontable(data$df7110_2, colWidths = 150, height = 500, readOnly=TRUE, 
		contextMenu = FALSE, manualColumnResize = TRUE, dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df7110_1 <- downloadHandler(
    filename = function() { "df7110_1.xlsx" },
    content = function(file) {
      write.xlsx(data$df7110_1, file)
  })

  output$download_df7110_2 <- downloadHandler(
    filename = function() { "df7110_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df7110_2, file)
  })

#*********

#ОСВ: 7200

observeEvent(input$dates7200, {
    start <- ymd(input$dates7200[[1]])
    end <- ymd(input$dates7200[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7200", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7200[[1]]
      r$end <- input$dates7200[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7200",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates7200))) {
      from=as.Date(input$dates7200[1L])
      to=as.Date(input$dates7200[2L])
      if (from>to) to = from
      selectdates7200_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df7210_4 <- data$df7210_1[as.Date(data$df7210_1$`Дата операции`) %in% selectdates7200_1, ]
    } else {
      selectdates7200_2 <- unique(as.Date(data$df7210_1$`Дата операции`))
      data$df7210_4 <- data$df7210_1[data$df7210_1$`Дата операции` %in% selectdates7200_2, ]
    }
  })

  observe({
    if(!is.null(input$table7210Item1) && !any(is.na(input$table7210Item1)))
      data$df7210_4 <- hot_to_r(input$table7210Item1)
  })

observe({
   if (nrow(data$df7210_4) > 0) {
  data$df7210_3[1, 2:5] <- data$df7210_4[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by="Учетный номер"][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
  } else {
    data$df7210_3[1, 2:5] <- 0
  }
})

  output$nested_ui7200 <- renderUI({!any(is.na(input$dates7200))})

  output$table7210Item3 <- renderRHandsontable({
    rhandsontable(data$df7210_3, colWidths = 150, height = 70, readOnly=TRUE, contextMenu = FALSE, 
		fixedColumnsLeft = 1, manualColumnResize = TRUE, dragColumns = FALSE) |>
	hot_col(1, width = 300)
  })

  output$download_df7210_3 <- downloadHandler(
    filename = function() { "df7210_3.xlsx" },
    content = function(file) {
      write.xlsx(data$df7210_3, file)
  })

#*********

#7210

observeEvent(input$dates7210, {
    start <- ymd(input$dates7210[[1]])
    end <- ymd(input$dates7210[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates7210", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates7210[[1]]
      r$end <- input$dates7210[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates7210",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

  observe({ 
    if (!is.null(input$table7210Item1)) {
      if (!r$user_authenticated) {
        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
        return()
      }
      
      data$df7210_1 <- hot_to_r(input$table7210Item1)

    if (!any(is.na(input$dates7210)) && input$choices7210 == "Выбор по дате операции") {
     	from=as.Date(input$dates7210[1L])
      	to=as.Date(input$dates7210[2L])
      	if (from>to) to = from
      	selectdates7210_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df7210_2 <- data$df7210_1[as.Date(data$df7210_1$"Дата операции") %in% selectdates7210_1, ]
    } else if (!is.null(input$text) && input$choices7210 == "Выбор по учетному номеру") {
      	data$df7210_2 <- data$df7210_1[data$df7210_1$"Учетный номер" == input$text, ]
    } else if (!is.null(input$dates7210) && !any(is.na(input$dates7210)) && !is.null(input$text) && input$choices7210 == "Выбор по дате операции и учетному номеру") {
     	from=as.Date(input$dates7210[1L])
      	to=as.Date(input$dates7210[2L])
      	if (from>to) to = from
      	selectdates7210_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df7210_2 <- data$df7210_1[as.Date(data$df7210_1$"Дата операции") %in% selectdates7210_2 & data$df7210_1$"Учетный номер" == input$text, ]
    } else {
        selectdates7210_4 <- unique(data$df7210_1$"Дата операции")
        data$df7210_2 <- data$df7210_1[data$df7210_1$"Дата операции" %in% selectdates7210_4, ]
    }
}
})  

  output$table7210Item1 <- renderRHandsontable({

    data$df7210_1[, `Сальдо конечное` := data$df7210_1[[6]] + data$df7210_1[[7]] - data$df7210_1[[8]]]

    rhandsontable(data$df7210_1, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2, 
		manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date") |>
      hot_col("Пользователь", readOnly = TRUE)
  })

  output$nested_ui7210 <- renderUI({
    if (input$choices7210 == "Выбор по дате операции") {
      	dateRangeInput("dates7210", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices7210 == "Выбор по учетному номеру") {
      	textInput("text", "Укажите учетный номер:")
    } else if (input$choices7210 == "Выбор по дате операции и учетному номеру") {
      fluidRow(
       	dateRangeInput("dates7210", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите учетный номер:")
      )}
  })

  output$table7210Item2 <- renderRHandsontable({

    rhandsontable(data$df7210_2, colWidths = 150, height = 500, readOnly=TRUE, 
		contextMenu = FALSE, manualColumnResize = TRUE, dragColumns = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df7210_1 <- downloadHandler(
    filename = function() { "df7210_1.xlsx" },
    content = function(file) { write.xlsx(data$df7210_1, file) })

  output$download_df7210_2 <- downloadHandler(
    filename = function() { "df7210_2.xlsx" },
    content = function(file) { write.xlsx(data$df7210_2, file) })

	#**********

	#7310.1

	observeEvent(input$dates7310.1, {
	    start <- ymd(input$dates7310.1[[1]])
	    end <- ymd(input$dates7310.1[[2]])

	 tryCatch({  
	  if (start > end) {
	    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
	    updateDateRangeInput(
	      session, 
	      "dates7310.1", 
	        start = r$start,
	        end = r$end
	      )
	    } else {
	      r$start <- input$dates7310.1[[1]]
	      r$end <- input$dates7310.1[[2]]
	    }
	   }, error = function(e) {
	      updateDateRangeInput(session,
	                           "dates7310.1",
	                           start = ymd(Sys.Date()),
	                           end = ymd(Sys.Date()))
	      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
	                 type = "error")
	    })
	}, ignoreInit = TRUE)

	  observe({ 
	    if (!is.null(input$table7310.1Item1)) {
	      if (!r$user_authenticated) {
	        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
	        return()
	      }
	      
	      data$df7310.1 <- hot_to_r(input$table7310.1Item1)

	    if (!any(is.na(input$dates7310.1)) && input$choices7310.1 == "Выбор по дате операции") {
	     	from=as.Date(input$dates7310.1[1L])
	      	to=as.Date(input$dates7310.1[2L])
	      	if (from>to) to = from
	      	selectdates7310.1_1 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.1_2 <- data$df7310.1[as.Date(data$df7310.1$"Дата операции") %in% selectdates7310.1_1, ]
	    } else if (!is.null(input$text) && input$choices7310.1 == "Выбор по учетному номеру") {
	      	data$df7310.1_2 <- data$df7310.1[data$df7310.1$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$text) && input$choices7310.1 == "Выбор по статье расхода") {
	      	data$df7310.1_2 <- data$df7310.1[data$df7310.1$"Счет № статьи расхода" == input$text, ]
	    } else if (!is.null(input$dates7310.1) && !any(is.na(input$dates7310.1)) && !is.null(input$text) && input$choices7310.1 == "Выбор по дате операции и учетному номеру") {
	     	from=as.Date(input$dates7310.1[1L])
	      	to=as.Date(input$dates7310.1[2L])
	      	if (from>to) to = from
	      	selectdates7310.1_2 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.1_2 <- data$df7310.1[as.Date(data$df7310.1$"Дата операции") %in% selectdates7310.1_2 & data$df7310.1$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$dates7310.1) && !any(is.na(input$dates7310.1)) && !is.null(input$text) && input$choices7310.1 == "Выбор по дате операции и статье расхода") {
	     	from=as.Date(input$dates7310.1[1L])
	      	to=as.Date(input$dates7310.1[2L])
	      	if (from>to) to = from
	      	selectdates7310.1_3 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.1_2 <- data$df7310.1[as.Date(data$df7310.1$"Дата операции") %in% selectdates7310.1_3 & data$df7310.1$"Счет № статьи расхода" == input$text, ]
	    } else {
	        selectdates7310.1_4 <- unique(data$df7310.1$"Дата операции")
	        data$df7310.1_2 <- data$df7310.1[data$df7310.1$"Дата операции" %in% selectdates7310.1_4, ]
	    }
	}
	})

	  output$table7310.1Item1 <- renderRHandsontable({
	    
	   data$df7310.1[, `Сальдо конечное` := data$df7310.1[[8]] - data$df7310.1[[9]] + data$df7310.1[[10]]]

	    rhandsontable(data$df7310.1, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2, 
			manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })
	  
	  output$nested_ui7310.1 <- renderUI({
	    if (input$choices7310.1 == "Выбор по дате операции") {
	      	dateRangeInput("dates7310.1", "Выберите период времени:", format="yyyy-mm-dd",
	                     start = Sys.Date(), end = Sys.Date(), separator = "-")
	    } else if (input$choices7310.1 == "Выбор по учетному номеру") {
	      	textInput("text", "Укажите учетный номер:")
	    } else if (input$choices7310.1 == "Выбор по статье расхода") {
	      	textInput("text", "Укажите Счет № статьи расхода:")
	    } else if (input$choices7310.1 == "Выбор по дате операции и учетному номеру") {
	      fluidRow(
	       	dateRangeInput("dates7310.1", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите учетный номер:")
	      )
	    } else if (input$choices7310.1 == "Выбор по дате операции и статье расхода") {
	      fluidRow(
	       	dateRangeInput("dates7310.1", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите Счет № статьи расхода:")
	      )
	    }
	  })

	  output$table7310.1Item2 <- renderRHandsontable({
	    rhandsontable(data$df7310.1_2, colWidths = 150, height = 500, readOnly=TRUE, contextMenu = FALSE,
			manualColumnResize = TRUE, dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })

	  output$download_df7310.1 <- downloadHandler(
	    filename = function() { "df7310.1.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7310.1, file)
	  })

	  output$download_df7310.1_2 <- downloadHandler(
	    filename = function() { "df7310.1_2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7310.1_2, file)
	  })

	#******

	#7310.2

	observeEvent(input$dates7310.2, {
	    start <- ymd(input$dates7310.2[[1]])
	    end <- ymd(input$dates7310.2[[2]])

	 tryCatch({  
	  if (start > end) {
	    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
	    updateDateRangeInput(
	      session, 
	      "dates7310.2", 
	        start = r$start,
	        end = r$end
	      )
	    } else {
	      r$start <- input$dates7310.2[[1]]
	      r$end <- input$dates7310.2[[2]]
	    }
	   }, error = function(e) {
	      updateDateRangeInput(session,
	                           "dates7310.2",
	                           start = ymd(Sys.Date()),
	                           end = ymd(Sys.Date()))
	      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
	                 type = "error")
	    })
	}, ignoreInit = TRUE)

	  observe({ 
	    if (!is.null(input$table7310.2Item1)) {
	      if (!r$user_authenticated) {
	        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
	        return()
	      }
	      
	      data$df7310.2 <- hot_to_r(input$table7310.2Item1)

	    if (!any(is.na(input$dates7310.2)) && input$choices7310.2 == "Выбор по дате операции") {
	     	from=as.Date(input$dates7310.2[1L])
	      	to=as.Date(input$dates7310.2[2L])
	      	if (from>to) to = from
	      	selectdates7310.2_1 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.2_2 <- data$df7310.2[as.Date(data$df7310.2$"Дата операции") %in% selectdates7310.2_1, ]
	    } else if (!is.null(input$text) && input$choices7310.2 == "Выбор по учетному номеру") {
	      	data$df7310.2_2 <- data$df7310.2[data$df7310.2$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$text) && input$choices7310.2 == "Выбор по статье расхода") {
	      	data$df7310.2_2 <- data$df7310.2[data$df7310.2$"Счет № статьи расхода" == input$text, ]
	    } else if (!is.null(input$dates7310.2) && !any(is.na(input$dates7310.2)) && !is.null(input$text) && input$choices7310.2 == "Выбор по дате операции и учетному номеру") {
	     	from=as.Date(input$dates7310.2[1L])
	      	to=as.Date(input$dates7310.2[2L])
	      	if (from>to) to = from
	      	selectdates7310.2_2 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.2_2 <- data$df7310.2[as.Date(data$df7310.2$"Дата операции") %in% selectdates7310.2_2 & data$df7310.2$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$dates7310.2) && !any(is.na(input$dates7310.2)) && !is.null(input$text) && input$choices7310.2 == "Выбор по дате операции и статье расхода") {
	     	from=as.Date(input$dates7310.2[1L])
	      	to=as.Date(input$dates7310.2[2L])
	      	if (from>to) to = from
	      	selectdates7310.2_3 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7310.2_2 <- data$df7310.2[as.Date(data$df7310.2$"Дата операции") %in% selectdates7310.2_3 & data$df7310.2$"Счет № статьи расхода" == input$text, ]
	    } else {
	        selectdates7310.2_4 <- unique(data$df7310.2$"Дата операции")
	        data$df7310.2_2 <- data$df7310.2[data$df7310.2$"Дата операции" %in% selectdates7310.2_4, ]
	    }
	}
	})

	  output$table7310.2Item1 <- renderRHandsontable({
	    
	   data$df7310.2[, `Сальдо конечное` := data$df7310.2[[8]] - data$df7310.2[[9]] + data$df7310.2[[10]]]

	    rhandsontable(data$df7310.2, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2,
	 		manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })
	  
	  output$nested_ui7310.2 <- renderUI({
	    if (input$choices7310.2 == "Выбор по дате операции") {
	      	dateRangeInput("dates7310.2", "Выберите период времени:", format="yyyy-mm-dd",
	                     start = Sys.Date(), end = Sys.Date(), separator = "-")
	    } else if (input$choices7310.2 == "Выбор по учетному номеру") {
	      	textInput("text", "Укажите учетный номер:")
	    } else if (input$choices7310.2 == "Выбор по статье расхода") {
	      	textInput("text", "Укажите Счет № статьи расхода:")
	    } else if (input$choices7310.2 == "Выбор по дате операции и учетному номеру") {
	      fluidRow(
	       	dateRangeInput("dates7310.2", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите учетный номер:")
	      )
	    } else if (input$choices7310.2 == "Выбор по дате операции и статье расхода") {
	      fluidRow(
	       	dateRangeInput("dates7310.2", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите Счет № статьи расхода:")
	      )
	    }
	  })

	  output$table7310.2Item2 <- renderRHandsontable({
	    rhandsontable(data$df7310.2_2, colWidths = 150, height = 500, readOnly=TRUE, contextMenu = FALSE,
			manualColumnResize = TRUE, dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })

	  output$download_df7310.2 <- downloadHandler(
	    filename = function() { "df7310.2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7310.2, file)
	  })

	  output$download_df7310.2_2 <- downloadHandler(
	    filename = function() { "df7310.2_2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7310.2_2, file)
	  })

	#**********

	#7320.1

	observeEvent(input$dates7320.1, {
	    start <- ymd(input$dates7320.1[[1]])
	    end <- ymd(input$dates7320.1[[2]])

	 tryCatch({  
	  if (start > end) {
	    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
	    updateDateRangeInput(
	      session, 
	      "dates7320.1", 
	        start = r$start,
	        end = r$end
	      )
	    } else {
	      r$start <- input$dates7320.1[[1]]
	      r$end <- input$dates7320.1[[2]]
	    }
	   }, error = function(e) {
	      updateDateRangeInput(session,
	                           "dates7320.1",
	                           start = ymd(Sys.Date()),
	                           end = ymd(Sys.Date()))
	      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
	                 type = "error")
	    })
	}, ignoreInit = TRUE)

	  observe({ 
	    if (!is.null(input$table7320.1Item1)) {
	      if (!r$user_authenticated) {
	        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
	        return()
	      }
	      
	      data$df7320.1 <- hot_to_r(input$table7320.1Item1)

	    if (!any(is.na(input$dates7320.1)) && input$choices7320.1 == "Выбор по дате операции") {
	     	from=as.Date(input$dates7320.1[1L])
	      	to=as.Date(input$dates7320.1[2L])
	      	if (from>to) to = from
	      	selectdates7320.1_1 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.1_2 <- data$df7320.1[as.Date(data$df7320.1$"Дата операции") %in% selectdates7320.1_1, ]
	    } else if (!is.null(input$text) && input$choices7320.1 == "Выбор по учетному номеру") {
	      	data$df7320.1_2 <- data$df7320.1[data$df7320.1$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$text) && input$choices7320.1 == "Выбор по статье расхода") {
	      	data$df7320.1_2 <- data$df7320.1[data$df7320.1$"Счет № статьи расхода" == input$text, ]
	    } else if (!is.null(input$dates7320.1) && !any(is.na(input$dates7320.1)) && !is.null(input$text) && input$choices7320.1 == "Выбор по дате операции и учетному номеру") {
	     	from=as.Date(input$dates7320.1[1L])
	      	to=as.Date(input$dates7320.1[2L])
	      	if (from>to) to = from
	      	selectdates7320.1_2 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.1_2 <- data$df7320.1[as.Date(data$df7320.1$"Дата операции") %in% selectdates7320.1_2 & data$df7320.1$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$dates7320.1) && !any(is.na(input$dates7320.1)) && !is.null(input$text) && input$choices7320.1 == "Выбор по дате операции и статье расхода") {
	     	from=as.Date(input$dates7320.1[1L])
	      	to=as.Date(input$dates7320.1[2L])
	      	if (from>to) to = from
	      	selectdates7320.1_3 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.1_2 <- data$df7320.1[as.Date(data$df7320.1$"Дата операции") %in% selectdates7320.1_3 & data$df7320.1$"Счет № статьи расхода" == input$text, ]
	    } else {
	        selectdates7320.1_4 <- unique(data$df7320.1$"Дата операции")
	        data$df7320.1_2 <- data$df7320.1[data$df7320.1$"Дата операции" %in% selectdates7320.1_4, ]
	    }
	}
	})

	  output$table7320.1Item1 <- renderRHandsontable({
	    
	   data$df7320.1[, `Сальдо конечное` := data$df7320.1[[8]] - data$df7320.1[[9]] + data$df7320.1[[10]]]

	    rhandsontable(data$df7320.1, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2, 
			manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })
	  
	  output$nested_ui7320.1 <- renderUI({
	    if (input$choices7320.1 == "Выбор по дате операции") {
	      	dateRangeInput("dates7320.1", "Выберите период времени:", format="yyyy-mm-dd",
	                     start = Sys.Date(), end = Sys.Date(), separator = "-")
	    } else if (input$choices7320.1 == "Выбор по учетному номеру") {
	      	textInput("text", "Укажите учетный номер:")
	    } else if (input$choices7320.1 == "Выбор по статье расхода") {
	      	textInput("text", "Укажите Счет № статьи расхода:")
	    } else if (input$choices7320.1 == "Выбор по дате операции и учетному номеру") {
	      fluidRow(
	       	dateRangeInput("dates7320.1", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите учетный номер:")
	      )
	    } else if (input$choices7320.1 == "Выбор по дате операции и статье расхода") {
	      fluidRow(
	       	dateRangeInput("dates7320.1", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите Счет № статьи расхода:")
	      )
	    }
	  })

	  output$table7320.1Item2 <- renderRHandsontable({
	    rhandsontable(data$df7320.1_2, colWidths = 150, height = 500, readOnly=TRUE, contextMenu = FALSE,
			manualColumnResize = TRUE, dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })

	  output$download_df7320.1 <- downloadHandler(
	    filename = function() { "df7320.1.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7320.1, file)
	  })

	  output$download_df7320.1_2 <- downloadHandler(
	    filename = function() { "df7320.1_2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7320.1_2, file)
	  })

	#******

	#7320.2

	observeEvent(input$dates7320.2, {
	    start <- ymd(input$dates7320.2[[1]])
	    end <- ymd(input$dates7320.2[[2]])

	 tryCatch({  
	  if (start > end) {
	    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
	    updateDateRangeInput(
	      session, 
	      "dates7320.2", 
	        start = r$start,
	        end = r$end
	      )
	    } else {
	      r$start <- input$dates7320.2[[1]]
	      r$end <- input$dates7320.2[[2]]
	    }
	   }, error = function(e) {
	      updateDateRangeInput(session,
	                           "dates7320.2",
	                           start = ymd(Sys.Date()),
	                           end = ymd(Sys.Date()))
	      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
	                 type = "error")
	    })
	}, ignoreInit = TRUE)

	  observe({ 
	    if (!is.null(input$table7320.2Item1)) {
	      if (!r$user_authenticated) {
	        update_auth_status("Для внесения изменений необходимо авторизоваться!", "warning")
	        return()
	      }
	      
	      data$df7320.2 <- hot_to_r(input$table7320.2Item1)

	    if (!any(is.na(input$dates7320.2)) && input$choices7320.2 == "Выбор по дате операции") {
	     	from=as.Date(input$dates7320.2[1L])
	      	to=as.Date(input$dates7320.2[2L])
	      	if (from>to) to = from
	      	selectdates7320.2_1 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.2_2 <- data$df7320.2[as.Date(data$df7320.2$"Дата операции") %in% selectdates7320.2_1, ]
	    } else if (!is.null(input$text) && input$choices7320.2 == "Выбор по учетному номеру") {
	      	data$df7320.2_2 <- data$df7320.2[data$df7320.2$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$text) && input$choices7320.2 == "Выбор по статье расхода") {
	      	data$df7320.2_2 <- data$df7320.2[data$df7320.2$"Счет № статьи расхода" == input$text, ]
	    } else if (!is.null(input$dates7320.2) && !any(is.na(input$dates7320.2)) && !is.null(input$text) && input$choices7320.2 == "Выбор по дате операции и учетному номеру") {
	     	from=as.Date(input$dates7320.2[1L])
	      	to=as.Date(input$dates7320.2[2L])
	      	if (from>to) to = from
	      	selectdates7320.2_2 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.2_2 <- data$df7320.2[as.Date(data$df7320.2$"Дата операции") %in% selectdates7320.2_2 & data$df7320.2$"Учетный номер" == input$text, ]
	    } else if (!is.null(input$dates7320.2) && !any(is.na(input$dates7320.2)) && !is.null(input$text) && input$choices7320.2 == "Выбор по дате операции и статье расхода") {
	     	from=as.Date(input$dates7320.2[1L])
	      	to=as.Date(input$dates7320.2[2L])
	      	if (from>to) to = from
	      	selectdates7320.2_3 <- seq.Date(from=from, to=to, by = "day")
	      	data$df7320.2_2 <- data$df7320.2[as.Date(data$df7320.2$"Дата операции") %in% selectdates7320.2_3 & data$df7320.2$"Счет № статьи расхода" == input$text, ]
	    } else {
	        selectdates7320.2_4 <- unique(data$df7320.2$"Дата операции")
	        data$df7320.2_2 <- data$df7320.2[data$df7320.2$"Дата операции" %in% selectdates7320.2_4, ]
	    }
	}
	})

	  output$table7320.2Item1 <- renderRHandsontable({
	    
	   data$df7320.2[, `Сальдо конечное` := data$df7320.2[[8]] - data$df7320.2[[9]] + data$df7320.2[[10]]]

	    rhandsontable(data$df7320.2, colWidths = 150, height = 500, allowInvalid=FALSE, fixedColumnsLeft = 2,
	 		manualColumnResize = TRUE, language = 'ru-RU', dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })
	  
	  output$nested_ui7320.2 <- renderUI({
	    if (input$choices7320.2 == "Выбор по дате операции") {
	      	dateRangeInput("dates7320.2", "Выберите период времени:", format="yyyy-mm-dd",
	                     start = Sys.Date(), end = Sys.Date(), separator = "-")
	    } else if (input$choices7320.2 == "Выбор по учетному номеру") {
	      	textInput("text", "Укажите учетный номер:")
	    } else if (input$choices7320.2 == "Выбор по статье расхода") {
	      	textInput("text", "Укажите Счет № статьи расхода:")
	    } else if (input$choices7320.2 == "Выбор по дате операции и учетному номеру") {
	      fluidRow(
	       	dateRangeInput("dates7320.2", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите учетный номер:")
	      )
	    } else if (input$choices7320.2 == "Выбор по дате операции и статье расхода") {
	      fluidRow(
	       	dateRangeInput("dates7320.2", "Выберите период времени:",
	                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
	        textInput("text", "Укажите Счет № статьи расхода:")
	      )
	    }
	  })

	  output$table7320.2Item2 <- renderRHandsontable({
	    rhandsontable(data$df7320.2_2, colWidths = 150, height = 500, readOnly=TRUE, contextMenu = FALSE,
			manualColumnResize = TRUE, dragColumns = FALSE) |>
	      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
	  })

	  output$download_df7320.2 <- downloadHandler(
	    filename = function() { "df7320.2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7320.2, file)
	  })

	  output$download_df7320.2_2 <- downloadHandler(
	    filename = function() { "df7320.2_2.xlsx" },
	    content = function(file) {
	      write.xlsx(data$df7320.2_2, file)
	  })

	#******

	#ЗАГРУЗКА ДАННЫХ

	# Функция для получения данных по выбранной дате
	observeEvent(input$data_load_date, {
	  r$data_load_selected_date <- input$data_load_date
	  message("Выбрана дата для загрузки данных: ", r$data_load_selected_date)
	})

	# Функция для загрузки данных выбранных таблиц
	load_selected_tables_data <- function(session_date) {
	  session_id <- paste("сессия", format(session_date, "%Y-%m-%d"))
	  message("Загрузка данных для сессии: ", session_id)
  
	  tables_to_load <- list()
  
	  # Проверяем, какие таблицы выбраны

	  if (input$check_7010_1) {
	    tables_to_load$df7010_1 <- load_merged_session_data("app_data_7010_1", session_id)
	  }
	  if (input$check_7110_1) {
	    tables_to_load$df7110_1 <- load_merged_session_data("app_data_7110_1", session_id)
	  }
	  if (input$check_7210_1) {
	    tables_to_load$df7210_1 <- load_merged_session_data("app_data_7210_1", session_id)
	  }
	  if (input$check_7310_1) {
	    tables_to_load$df7310.1 <- load_merged_session_data("app_data_7310_1", session_id)
	  }
	  if (input$check_7310_2) {
	    tables_to_load$df7310.2 <- load_merged_session_data("app_data_7310_2", session_id)
	  }
	  if (input$check_7320_1) {
	    tables_to_load$df7320.1 <- load_merged_session_data("app_data_7320_1", session_id)
	  }
	  if (input$check_7320_2) {
	    tables_to_load$df7320.2 <- load_merged_session_data("app_data_7320_2", session_id)
	  }
	  return(tables_to_load)
	}

	# Обработчик для кнопки "Загрузить отмеченные таблицы"
	observeEvent(input$load_selected_btn, {
	  req(input$data_load_date)
  
	  # Проверяем, выбрана ли хотя бы одна таблица
		if (!any(c(input$check_7010_1, input$check_7110_1, input$check_7210_1,
			input$check_7310_1, input$check_7310_2,
			input$check_7320_1, input$check_7320_2))) {
	    shinyalert("Ошибка", "Не выбрана ни одна таблица для загрузки.", type = "error")
	    return()
	  }
  
	  r$show_loading <- TRUE
  
	  tryCatch({
	    # Загружаем данные выбранных таблиц
	    tables_data <- load_selected_tables_data(input$data_load_date)
	    
	    # Проверяем, есть ли данные
	    if (length(tables_data) == 0) {
	      shinyalert("Информация", "Нет данных для выбранных таблиц и даты.", type = "info")
	      return()
	    }
	    
	    # Создаем временную директорию для файлов
	    temp_dir <- tempdir()
	    zip_file <- file.path(temp_dir, paste0("selected_tables_", format(input$data_load_date, "%Y-%m-%d"), ".zip"))
	    
	    # Создаем список файлов для архивации
	    files_to_zip <- c()
	    
	    # Сохраняем каждую таблицу в отдельный файл Excel
	    for (table_name in names(tables_data)) {
	      if (!is.null(tables_data[[table_name]])) {
	        # Преобразуем данные в нужный формат
	        df <- as.data.table(tables_data[[table_name]])
        
        # Устанавливаем правильные имена столбцов в зависимости от таблицы
        if (table_name == "df7010_1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number", "operation_description",
                                "accounting_method", "initial_balance",
                                "debit", "credit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Содержание операции",
                                "Метод учета", "Сальдо начальное",
                                "Дебет", "Кредит",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7110_1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number", "operation_description",
                                "accounting_method", "initial_balance",
                                "debit", "credit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Содержание операции",
                                "Метод учета", "Сальдо начальное",
                                "Дебет", "Кредит",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7210_1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number", "operation_description",
                                "accounting_method", "initial_balance",
                                "debit", "credit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Содержание операции",
                                "Метод учета", "Сальдо начальное",
                                "Дебет", "Кредит",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7310.1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7310.2") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7320.1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7320.2") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        }
        
        # Сохраняем файл с автоматическим именем
        file_name <- paste0(table_name, ".xlsx")
        file_path <- file.path(temp_dir, file_name)
        write.xlsx(df, file_path)
        files_to_zip <- c(files_to_zip, file_path)
      }
    }
    
    	# Создаем ZIP-архив с помощью zip::zip
    	old_wd <- getwd()
    	setwd(temp_dir)
    	zip::zip(basename(zip_file), files = basename(files_to_zip), mode = "cherry-pick")
    	setwd(old_wd)
    
    	# Скачиваем файл без запроса разрешения через JavaScript
    	session$sendCustomMessage(
      		  "downloadFile",
      		  list(
        		url = paste0("data:application/zip;base64,", base64enc::base64encode(zip_file)),
        		filename = paste0("selected_tables_", format(input$data_load_date, "%Y-%m-%d"), ".zip")
      			)
    		)
    
    		shinyalert("Успех", "Выбранные таблицы успешно загружены и сохранены в ZIP-архиве.", type = "success")
    
	  }, error = function(e) {
	    message("Ошибка при загрузке выбранных таблиц: ", e$message)
	    shinyalert("Ошибка", paste("Ошибка при загрузке данных:", e$message), type = "error")
	  }, finally = {
	    r$show_loading <- FALSE
	  })
	})

	# Обработчик для кнопки "Загрузить все таблицы"
	observeEvent(input$load_all_btn, {
	  req(input$data_load_date)
  
	  r$show_loading <- TRUE
	  
	  tryCatch({
	    # Загружаем данные всех таблиц
	    session_id <- paste("сессия", format(input$data_load_date, "%Y-%m-%d"))
	    
	    tables_data <- list(
	      df7010_1 = load_merged_session_data("app_data_7010_1", session_id),
	      df7110_1 = load_merged_session_data("app_data_7110_1", session_id),
	      df7210_1 = load_merged_session_data("app_data_7210_1", session_id),
	      df7310_1 = load_merged_session_data("app_data_7310_1", session_id),
	      df7310_2 = load_merged_session_data("app_data_7310_2", session_id)б
	      df7320_1 = load_merged_session_data("app_data_7320_1", session_id),
	      df7320_2 = load_merged_session_data("app_data_7320_2", session_id)
	    )
	    
	    # Проверяем, есть ли данные
	    if (all(sapply(tables_data, is.null))) {
	      shinyalert("Информация", "Нет данных для выбранной даты.", type = "info")
	      return()
	    }
	    
	    # Создаем временную директорию для файлов
	    temp_dir <- tempdir()
	    zip_file <- file.path(temp_dir, paste0("all_tables_", format(input$data_load_date, "%Y-%m-%d"), ".zip"))
	    
	    # Создаем список файлов для архивации
	    files_to_zip <- c()
    
	# Сохраняем каждую таблицу в отдельный файл Excel
	for (table_name in names(tables_data)) {
		if (!is.null(tables_data[[table_name]])) {
			# Преобразуем данные в нужный формат
			df <- as.data.table(tables_data[[table_name]])
        
        # Устанавливаем правильные имена столбцов в зависимости от таблицы
	if (table_name == "df7010_1") {
		expected_cols <- c("operation_date", "operation_time",
				   "document_number", "operation_description",
				   "accounting_method", "initial_balance",
				   "debit", "credit", "correspondence_debit",
				   "correspondence_credit", "final_balance", "username")
		
		if (all(expected_cols %in% names(df))) {
			setnames(df, expected_cols,
				 c("Дата операции", "Время проводки",
				   "Учетный номер", "Содержание операции",
				   "Метод учета", "Сальдо начальное",
				   "Дебет", "Кредит",
				   "Счет № (дебет)", "Счет № (кредит)",
				   "Сальдо конечное", "Пользователь"))
	  }
	} else if (table_name == "df7110_1") {
		expected_cols <- c("operation_date", "operation_time",
				   "document_number", "operation_description",
				   "accounting_method", "initial_balance",
				   "debit", "credit", "correspondence_debit",
				   "correspondence_credit", "final_balance", "username")
		
		if (all(expected_cols %in% names(df))) {
			setnames(df, expected_cols,
				 c("Дата операции", "Время проводки",
				   "Учетный номер", "Содержание операции",
				   "Метод учета", "Сальдо начальное",
				   "Дебет", "Кредит",
				   "Счет № (дебет)", "Счет № (кредит)",
				   "Сальдо конечное", "Пользователь"))
	  }
	} else if (table_name == "df7210_1") {
		expected_cols <- c("operation_date", "operation_time",
				   "document_number", "operation_description",
				   "accounting_method", "initial_balance",
				   "debit", "credit", "correspondence_debit",
				   "correspondence_credit", "final_balance", "username")
		
		if (all(expected_cols %in% names(df))) {
			setnames(df, expected_cols,
				 c("Дата операции", "Время проводки",
				   "Учетный номер", "Содержание операции",
				   "Метод учета", "Сальдо начальное",
				   "Дебет", "Кредит",
				   "Счет № (дебет)", "Счет № (кредит)",
				   "Сальдо конечное", "Пользователь"))
	  }
	} else if (table_name == "df7310.1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7310.2") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7320.1") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        } else if (table_name == "df7320.2") {
                expected_cols <- c("operation_date", "operation_time",
                                "document_number",  "expense_account",
				"expense_period", "operation_description",
                                "accounting_method", "initial_balance",
                                "credit", "debit", "correspondence_debit",
                                "correspondence_credit", "final_balance", "username")
                
                if (all(expected_cols %in% names(df))) {
                        setnames(df, expected_cols,
                                c("Дата операции", "Время проводки",
                                "Учетный номер", "Счет № статьи расхода",
				"Период расхода",
				"Содержание операции", "Метод учета", 
				"Сальдо начальное", "Кредит", "Дебет",
                                "Счет № (дебет)", "Счет № (кредит)",
                                "Сальдо конечное", "Пользователь"))
          }
        }
        
        	# Сохраняем файл с автоматическим именем
        	file_name <- paste0(table_name, ".xlsx")
        	file_path <- file.path(temp_dir, file_name)
        	write.xlsx(df, file_path)
        	files_to_zip <- c(files_to_zip, file_path)
      		}
    	     }
    
    	# Создаем ZIP-архив с помощью zip::zip
    	old_wd <- getwd()
    	setwd(temp_dir)
    	zip::zip(basename(zip_file), files = basename(files_to_zip), mode = "cherry-pick")
    	setwd(old_wd)
    
    	# Скачиваем файл без запроса разрешения через JavaScript
    	session$sendCustomMessage(
      		  "downloadFile",
      		  list(
        		url = paste0("data:application/zip;base64,", base64enc::base64encode(zip_file)),
        		filename = paste0("all_tables_", format(input$data_load_date, "%Y-%m-%d"), ".zip")
      			)
    		     )
    
    		shinyalert("Успех", "Все таблицы успешно загружены и сохранены в ZIP-архиве.", type = "success")
    
  	}, error = function(e) {
    		message("Ошибка при загрузке всех таблиц: ", e$message)
    		shinyalert("Ошибка", paste("Ошибка при загрузке данных:", e$message), type = "error")
  	}, finally = {
    		r$show_loading <- FALSE
  	   })
	})

	}
	shinyApp(ui, server)
