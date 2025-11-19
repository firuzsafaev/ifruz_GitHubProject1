
#Доходы

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

#УЧЕТ

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

#****************************

#6000.Доход от реализации продукции и оказания услуг

#ОСВ: 6000

DF6000 <- data.table(
  "Счет (субчет)" = as.character(c("6010.Доход от реализации продукции и оказания услуг",
                                   "6020.Возврат проданной продукции",
                                   "6030.Скидки с цены и продаж")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6010 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6010_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6020 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер документа, по которому состоялся возврат проданной продукции" = as.character(NA),
  "Счет №, с которого списывался возвращенная продукция" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6020_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер документа, по которому состоялся возврат проданной продукции" = as.character(NA),
  "Счет №, с которого списывался возвращенная продукция" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6030 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6030_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#**********************************************

#ОСВ: 6100

DF6100 <- data.table(
  "Счет (субчет)" = as.character(c("6110.Доходы по финансовым активам",
                                   "6120.Доходы по дивидендам",
                                   "6130.Доходы от финансовой аренды",
                                   "6140.Доходы от операций с инвестиционным имуществом",
                                   "6150.Доход от изменения справедливой стоимости финансовых активов, в т.ч. инвестиций в долевые инструменты, оцениваемых по справедливой стоимости через прибыль и убытки",
                                   "6160.Доход от изменения справедливой стоимости финансовых активов, в т.ч. инвестиций в долевые инструменты, оцениваемых по справедливой стоимости через Прочий совокупный доход",
                                   "6170.Прочие доходы от финансирования",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6110

DF6110 <- data.table(
  "Счет (субчет)" = as.character(c("6110.1.Доходы по финансовым активам, отражаемые в составе прибыли и убытка",
                                   "6110.2.Доходы по финансовым активам, отражаемые в  Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6110_3 <- data.table(
  "Счет (субчет)" = as.character(c("6110.1.Доходы по финансовым активам, отражаемые в составе прибыли и убытка",
                                   "6110.2.Доходы по финансовым активам, отражаемые в  Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6110.1

DF6110.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6110.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6110.2

DF6110.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6110.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6120

DF6120 <- data.table(
  "Счет (субчет)" = as.character(c("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
                                   "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6120_3 <- data.table(
  "Счет (субчет)" = as.character(c("6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка",
                                   "6120.2.Доходы по дивидендам, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6120.1

DF6120.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся дивиденды" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

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
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6120.2

DF6120.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся дивиденды" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

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
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6130

DF6130 <- data.table(
  "Счет (субчет)" = as.character(c("6130.1.Доходы от финансовой аренды, отражаемые в составе прибыли и убытка",
                                   "6130.2.Доходы от финансовой аренды, отражаемые в  Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6130_3 <- data.table(
  "Счет (субчет)" = as.character(c("6130.1.Доходы от финансовой аренды, отражаемые в составе прибыли и убытка",
                                   "6130.2.Доходы от финансовой аренды, отражаемые в  Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6130.1

DF6130.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6130.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6130.2

DF6130.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6130.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6140

DF6140 <- data.table(
  "Счет (субчет)" = as.character(c("6141.Доходы от операционной аренды",
                                   "6142.Доходы от прироста стоимости инвестиционной недвижимости",
                                   "6143.Доходы от реклассификации",
                                   "Итого:")),
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6140_3 <- data.table(
  "Счет (субчет)" = as.character(c("6141.Доходы от операционной аренды",
                                   "6142.Доходы от прироста стоимости инвестиционной недвижимости",
                                   "6143.Доходы от реклассификации",
                                   "Итого:")),
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6141

DF6141 <- data.table(
  "Счет (субчет)" = as.character(c("6141.1.Доходы от операционной аренды, отражаемые в составе прибыли и убытка",
                                   "6141.2.Доходы от операционной аренды, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6141_2 <- data.table(
  "Счет (субчет)" = as.character(c("6141.1.Доходы от операционной аренды, отражаемые в составе прибыли и убытка",
                                   "6141.2.Доходы от операционной аренды, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6141_3 <- data.table(
  "Счет (субчет)" = as.character(c("6141.1.Доходы от операционной аренды, отражаемые в составе прибыли и убытка",
                                   "6141.2.Доходы от операционной аренды, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6141.1

DF6141.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6141.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6141.2

DF6141.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6141.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6142

DF6142 <- data.table(
  "Счет (субчет)" = as.character(c("6142.1.Доходы от прироста стоимости инвестиционного имущества, отражаемые в составе прибыли и убытка",
                                   "6142.2.Доходы от прироста стоимости инвестиционного имущества, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6142_2 <- data.table(
  "Счет (субчет)" = as.character(c("6142.1.Доходы от прироста стоимости инвестиционного имущества, отражаемые в составе прибыли и убытка",
                                   "6142.2.Доходы от прироста стоимости инвестиционного имущества, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)


DF6142_3 <- data.table(
  "Счет (субчет)" = as.character(c("6142.1.Доходы от прироста стоимости инвестиционного имущества, отражаемые в составе прибыли и убытка",
                                   "6142.2.Доходы от прироста стоимости инвестиционного имущества, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6142.1

DF6142.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6142.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6142.2

DF6142.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6142.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6143 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6143_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				
  "Кредит" = as.numeric(NA),					
  "Дебет" = as.numeric(NA),					
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6150 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6150_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6160 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6160_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA), 
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6170

DF6170 <- data.table(
  "Счет (субчет)" = as.character(c("6171.Прочие доходы от финансирования, отражаемые в составе прибыли и убытка",
                                   "6172.Прочие доходы от финансирования, отражаемые в Прочем совокупном доходе",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6170_2 <- data.table(
  "Счет (субчет)" = as.character(c("6171.Прочие доходы от финансирования, отражаемые в составе прибыли и убытка",
                                   "6172.Прочие доходы от финансирования, отражаемые в Прочем совокупном доходе",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6171 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)


DF6171_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6172 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6172_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#*********************************************

#ОСВ: 6200

DF6200 <- data.table(
  "Счет (субчет)" = as.character(c("6210.Доходы от выбытия активов",
                                   "6220.Доходы, связанные с прекращаемой деятельностью",
                                   "6230.Доходы от операционной аренды (арендодатель)",
                                   "6240.Доходы от безвозмездно полученных активов",
                                   "6250.Доходы от государственных субсидий",
                                   "6260.Доходы от переоценки",
                                   "6270.Доходы от производных инструментов",
                                   "6280.Доход в результате пересчета валюты",
                                   "6290.Прочие доходы ( в т.ч. восстановление убытка от обесценения)",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6210

DF6210 <- data.table(
  "Счет (субчет)" = as.character(c("6210.1.Доход от выбытия активов, отражаемый в составе прибыли и убытка",
                                   "6210.2.Доход от выбытия активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6210_3 <- data.table(
  "Счет (субчет)" = as.character(c("6210.1.Доход от выбытия активов, отражаемый в составе прибыли и убытка",
                                   "6210.2.Доход от выбытия активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6210.1

DF6210.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6210.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6210.2

DF6210.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6210.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6220

DF6220 <- data.table(
  "Счет (субчет)" = as.character(c("6220.1.Доходы в связи с прекращаемой деятельностью, отражаемые в составе прибыли и убытка",
                                   "6220.2.Доходы в связи с прекращаемой деятельностью, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6220_3 <- data.table(
  "Счет (субчет)" = as.character(c("6220.1.Доходы в связи с прекращаемой деятельностью, отражаемые в составе прибыли и убытка",
                                   "6220.2.Доходы в связи с прекращаемой деятельностью, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6220.1

DF6220.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6220.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6220.2

DF6220.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6220.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6230

DF6230 <- data.table(
  "Счет (субчет)" = as.character(c("6230.1.Доход от операционной аренды, отражаемый в составе прибыли и убытка",
                                   "6230.2.Доход от операционной аренды, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6230_3 <- data.table(
  "Счет (субчет)" = as.character(c("6230.1.Доход от операционной аренды, отражаемый в составе прибыли и убытка",
                                   "6230.2.Доход от операционной аренды, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6230.1

DF6230.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6230.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6230.2

DF6230.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 7
  "Кредит" = as.numeric(NA),					#row 8 
  "Дебет" = as.numeric(NA),					#row 9
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6230.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Период, к которому относятся доходы" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6240

DF6240 <- data.table(
  "Счет (субчет)" = as.character(c("6240.1.Доход от безвозмездно полученных активов, отражаемый в составе прибыли и убытка",
                                   "6240.2.Доход от безвозмездно полученных активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6240_3 <- data.table(
  "Счет (субчет)" = as.character(c("6240.1.Доход от безвозмездно полученных активов, отражаемый в составе прибыли и убытка",
                                   "6240.2.Доход от безвозмездно полученных активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6240.1

DF6240.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6240.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6240.2

DF6240.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6240.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6250

DF6250 <- data.table(
  "Счет (субчет)" = as.character(c("6250.1.Доходы от государственных субсидий, отражаемые в составе прибыли и убытка",
                                   "6250.2.Доходы от государственных субсидий, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6250_3 <- data.table(
  "Счет (субчет)" = as.character(c("6250.1.Доходы от государственных субсидий, отражаемые в составе прибыли и убытка",
                                   "6250.2.Доходы от государственных субсидий, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6250.1

DF6250.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6250.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6250.2

DF6250.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6250.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6260

DF6260 <- data.table(
  "Счет (субчет)" = as.character(c("6261.Доход от переоценки основных средств",
                                   "6262.Доход от переоценки нематериальных активов",
                                   "6263.Доход от переоценки долгосрочных активов, предназначенных для продажи или распределения собственникам",
                                   "6264.Доход от переоценки выбывающих групп, предназначенных для продажи или распределения собственникам",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6260_3 <- data.table(
  "Счет (субчет)" = as.character(c("6261.Доход от переоценки основных средств",
                                   "6262.Доход от переоценки нематериальных активов",
                                   "6263.Доход от переоценки долгосрочных активов, предназначенных для продажи или распределения собственникам",
                                   "6264.Доход от переоценки выбывающих групп, предназначенных для продажи или распределения собственникам",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6261

DF6261 <- data.table(
  "Счет (субчет)" = as.character(c("6261.1.Доход от переоценки основных средств, отражаемый в составе прибыли и убытка",
                                   "6261.2.Доход от переоценки основных средств, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6261_2 <- data.table(
  "Счет (субчет)" = as.character(c("6261.1.Доход от переоценки основных средств, отражаемый в составе прибыли и убытка",
                                   "6261.2.Доход от переоценки основных средств, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6261_3 <- data.table(
  "Счет (субчет)" = as.character(c("6261.1.Доход от переоценки основных средств, отражаемый в составе прибыли и убытка",
                                   "6261.2.Доход от переоценки основных средств, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6261.1

DF6261.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6261.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6261.2

DF6261.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6261.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6262

DF6262 <- data.table(
  "Счет (субчет)" = as.character(c("6262.1.Доход от переоценки нематериальных активов, отражаемый в составе прибыли и убытка",
                                   "6262.2.Доход от переоценки нематериальных активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6262_2 <- data.table(
  "Счет (субчет)" = as.character(c("6262.1.Доход от переоценки нематериальных активов, отражаемый в составе прибыли и убытка",
                                   "6262.2.Доход от переоценки нематериальных активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6262_3 <- data.table(
  "Счет (субчет)" = as.character(c("6262.1.Доход от переоценки нематериальных активов, отражаемый в составе прибыли и убытка",
                                   "6262.2.Доход от переоценки нематериальных активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6262.1

DF6262.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6262.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6262.2

DF6262.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6262.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6263

DF6263 <- data.table(
  "Счет (субчет)" = as.character(c("6263.1.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6263.2.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6263_2 <- data.table(
  "Счет (субчет)" = as.character(c("6263.1.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6263.2.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6263_3 <- data.table(
  "Счет (субчет)" = as.character(c("6263.1.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6263.2.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6263.1

DF6263.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6263.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6263.2

DF6263.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6263.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6264

DF6264 <- data.table(
  "Счет (субчет)" = as.character(c("6264.1.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6264.2.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6264_2 <- data.table(
  "Счет (субчет)" = as.character(c("6264.1.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6264.2.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6264_3 <- data.table(
  "Счет (субчет)" = as.character(c("6264.1.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в составе прибыли и убытка",
                                   "6264.2.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6264.1

DF6264.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6264.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6264.2

DF6264.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6264.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6270

DF6270 <- data.table(
  "Счет (субчет)" = as.character(c("6271.Доход от хеджирования чистых инвестиций в зарубежные операции: эффективная часть",
                                   "6272.Доход по инструментам хеджирования при хеджировании денежных потоков: эффективная часть",
                                   "6273.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через прочий совокупный доход: по инструментам хеджирования",
                                   "6274.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через прочий совокупный доход: по объекту хеджирования",
                                   "6275.Доход от изменения величины временной стоимости опционов",
                                   "6276.Доход от изменения стоимости форвардных элементов форвардных договоров",
                                   "6277.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, не являющегося инструментом хеджирования",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6270_3 <- data.table(
  "Счет (субчет)" = as.character(c("6271.Доход от хеджирования чистых инвестиций в зарубежные операции: эффективная часть",
                                   "6272.Доход по инструментам хеджирования при хеджировании денежных потоков: эффективная часть",
                                   "6273.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через прочий совокупный доход: по инструментам хеджирования",
                                   "6274.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через прочий совокупный доход: по объекту хеджирования",
                                   "6275.Доход от изменения величины временной стоимости опционов",
                                   "6276.Доход от изменения стоимости форвардных элементов форвардных договоров",
                                   "6277.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, не являющегося инструментом хеджирования",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6271

DF6271 <- data.table(
  "Счет (субчет)" = as.character(c("6271.1.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в составе прибыли и убытка",
                                   "6271.2.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6271_2 <- data.table(
  "Счет (субчет)" = as.character(c("6271.1.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в составе прибыли и убытка",
                                   "6271.2.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)


DF6271_3 <- data.table(
  "Счет (субчет)" = as.character(c("6271.1.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в составе прибыли и убытка",
                                   "6271.2.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6271.1

DF6271.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6271.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6271.2

DF6271.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6271.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6272

DF6272 <- data.table(
  "Счет (субчет)" = as.character(c("6272.1.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в  составе прибыли и убытка",
                                   "6272.2.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6272_2 <- data.table(
  "Счет (субчет)" = as.character(c("6272.1.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в  составе прибыли и убытка",
                                   "6272.2.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6272_3 <- data.table(
  "Счет (субчет)" = as.character(c("6272.1.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в  составе прибыли и убытка",
                                   "6272.2.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6272.1

DF6272.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6272.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6272.2

DF6272.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6272.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6273

DF6273 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6273_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6274

DF6274 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6274_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6275

DF6275 <- data.table(
  "Счет (субчет)" = as.character(c("6275.1.Доход от изменения величины временной стоимости опционов, отражаемый в составе прибыли и убытка",
                                   "6275.2.Доход от изменения величины временной стоимости опционов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6275_2 <- data.table(
  "Счет (субчет)" = as.character(c("6275.1.Доход от изменения величины временной стоимости опционов, отражаемый в составе прибыли и убытка",
                                   "6275.2.Доход от изменения величины временной стоимости опционов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)


DF6275_3 <- data.table(
  "Счет (субчет)" = as.character(c("6275.1.Доход от изменения величины временной стоимости опционов, отражаемый в составе прибыли и убытка",
                                   "6275.2.Доход от изменения величины временной стоимости опционов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6275.1

DF6275.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6275.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6275.2

DF6275.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6275.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6276

DF6276 <- data.table(
  "Счет (субчет)" = as.character(c("6276.1.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в составе прибыли и убытка",
                                   "6276.2.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6276_2 <- data.table(
  "Счет (субчет)" = as.character(c("6276.1.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в составе прибыли и убытка",
                                   "6276.2.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6276_3 <- data.table(
  "Счет (субчет)" = as.character(c("6276.1.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в составе прибыли и убытка",
                                   "6276.2.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6276.1

DF6276.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6276.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6276.2

DF6276.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6276.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6277

DF6277 <- data.table(
  "Счет (субчет)" = as.character(c("6277.1.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в составе прибыли и убытка",
                                   "6277.2.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6277_2 <- data.table(
  "Счет (субчет)" = as.character(c("6277.1.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в составе прибыли и убытка",
                                   "6277.2.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6277_3 <- data.table(
  "Счет (субчет)" = as.character(c("6277.1.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в составе прибыли и убытка",
                                   "6277.2.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6277.1

DF6277.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6277.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6277.2

DF6277.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6277.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6280

DF6280 <- data.table(
  "Счет (субчет)" = as.character(c("6281.Доход в результате пересчета иностранной валюты: чистые курсовые разницы",
                                   "6282.Доход в результате пересчета финансовой отчетности в другую валюту",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6280_3 <- data.table(
  "Счет (субчет)" = as.character(c("6281.Доход в результате пересчета иностранной валюты: чистые курсовые разницы",
                                   "6282.Доход в результате пересчета финансовой отчетности в другую валюту",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#ОСВ: 6281

DF6281 <- data.table(
  "Счет (субчет)" = as.character(c("6281.1.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в составе прибыли и убытка",
                                   "6281.2.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6281_2 <- data.table(
  "Счет (субчет)" = as.character(c("6281.1.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в составе прибыли и убытка",
                                   "6281.2.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6281_3 <- data.table(
  "Счет (субчет)" = as.character(c("6281.1.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в составе прибыли и убытка",
                                   "6281.2.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6281.1

DF6281.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6281.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6281.2

DF6281.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6281.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6282

DF6282 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6282_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6290

DF6290 <- data.table(
  "Счет (субчет)" = as.character(c("6291.Доход в связи с изменениями в прочем совокупном доходе объекта инвестиций",
                                   "6292.Доход от изменения справедливой стоимости биологических активов",
                                   "6293.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования",
                                   "6294.Прочие доходы ( в т.ч. восстановление убытка от обесценения), не учтенные в предыдущих субсчетах",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6290_3 <- data.table(
  "Счет (субчет)" = as.character(c("6291.Доход в связи с изменениями в прочем совокупном доходе объекта инвестиций",
                                   "6292.Доход от изменения справедливой стоимости биологических активов",
                                   "6293.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования",
                                   "6294.Прочие доходы ( в т.ч. восстановление убытка от обесценения), не учтенные в предыдущих субсчетах",
                                   "Итого:")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6291

DF6291 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта инвестиций" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта инвестиций" = as.character(NA),
  "Количество (или другой показатель) объекта инвестиций" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6291_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта инвестиций" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта инвестиций" = as.character(NA),
  "Количество (или другой показатель) объекта инвестиций" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA), 
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6292

DF6292 <- data.table(
  "Счет (субчет)" = as.character(c("6292.1.Доход от изменения справедливой стоимости биологических активов, отражаемый в составе прибыли и убытка",
                                   "6292.2.Доход от изменения справедливой стоимости биологических активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6292_2 <- data.table(
  "Счет (субчет)" = as.character(c("6292.1.Доход от изменения справедливой стоимости биологических активов, отражаемый в составе прибыли и убытка",
                                   "6292.2.Доход от изменения справедливой стоимости биологических активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6292_3 <- data.table(
  "Счет (субчет)" = as.character(c("6292.1.Доход от изменения справедливой стоимости биологических активов, отражаемый в составе прибыли и убытка",
                                   "6292.2.Доход от изменения справедливой стоимости биологических активов, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6292.1

DF6292.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6292.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6292.2

DF6292.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6292.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6293

DF6293 <- data.table(
  "Счет (субчет)" = as.character(c("6293.1.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в составе прибыли и убытка",
                                   "6293.2.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6293_2 <- data.table(
  "Счет (субчет)" = as.character(c("6293.1.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в составе прибыли и убытка",
                                   "6293.2.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6293_3 <- data.table(
  "Счет (субчет)" = as.character(c("6293.1.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в составе прибыли и убытка",
                                   "6293.2.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6293.1

DF6293.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6293.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6293.2

DF6293.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 6
  "Кредит" = as.numeric(NA),					#row 7 
  "Дебет" = as.numeric(NA),					#row 8
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6293.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#ОСВ: 6294

DF6294 <- data.table(
  "Счет (субчет)" = as.character(c("6294.1.Прочие доходы, отражаемые в составе прибыли и убытка",
                                   "6294.2.Прочие доходы, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)


DF6294_2 <- data.table(
  "Счет (субчет)" = as.character(c("6294.1.Прочие доходы, отражаемые в составе прибыли и убытка",
                                   "6294.2.Прочие доходы, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6294_3 <- data.table(
  "Счет (субчет)" = as.character(c("6294.1.Прочие доходы, отражаемые в составе прибыли и убытка",
                                   "6294.2.Прочие доходы, отражаемые в Прочем совокупном доходе",
                                   "Итого")),	
  "Сальдо начальное" = as.numeric(c(0)),
  "Дебет" = as.numeric(c(0)),
  "Кредит" = as.numeric(c(0)),
  "Сальдо конечное" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

#6294.1

DF6294.1 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6294.1_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#6294.2

DF6294.2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),				#row 9
  "Кредит" = as.numeric(NA),					#row 10 
  "Дебет" = as.numeric(NA),					#row 11
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

DF6294.2_2 <- data.table(
  "Дата операции" = as.character(NA),
  "Номер первичного документа" = as.character(NA),
  "Вид (название) объекта переоценки" = as.character(NA),
  "Счет № статьи дохода" = as.character(NA),
  "Содержание операции" = as.character(NA),
  "Величина измерения единицы объекта переоценки" = as.character(NA),
  "Количество (или другой показатель) объекта переоценки" = as.numeric(NA),
  "Метод учета" = as.character(NA),
  "Сальдо начальное" = as.numeric(NA),
  "Кредит" = as.numeric(NA),
  "Дебет" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Сальдо конечное" = as.numeric(NA),
  stringsAsFactors = FALSE)

#****************************************

#ОСВ: 6300

DF6300 <- data.table(
  "Счет (субчет)" = as.character(c(
    "6310.Доля в прибыли материнской компании и компаний с совместным контролем или значительным влиянием",
    "6320.Доля в прибыли дочерних компаний и других организации той же группы",
    "6330.Доля в прибыли ассоциированных организаций и совместных предприятий",
    "Итого:")),	
  "Сумма доли в прибыли объектов инвестиций за период" = as.numeric(c(0)),
  "Сумма доли прибыли в прочем совокупном доходе объектов инвестиций за период" = as.numeric(c(0)),
  stringsAsFactors = FALSE)

DF6310 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

DF6310_2 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

DF6320 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

DF6320_2 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

DF6330 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Счет № (кредит)" = factor(NA, levels = "BaseAccountsList", ordered = TRUE),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

DF6330_2 <- data.table(
  "Дата учета" = as.character(NA),
  "Номер учета (реквизиты) объекта инвестиции" = as.character(NA),
  "Период отчетности" = as.character(NA),
  "Период отчетности объекта инвестиции" = as.character(NA),
  "Метод учета" = as.character(NA),
  "Сумма прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли в прибыли объекта инвестиции за период" = as.numeric(NA),
  "Сумма прочего совокупного дохода объекта инвестиции за период" = as.numeric(NA),
  "Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период" = as.numeric(NA),
  "Счет № (дебет)" = as.character(NA),
  "Счет № (кредит)" = as.character(NA),
  "Примечание по учету" = as.character(NA),
  stringsAsFactors = FALSE)

#**********************************

ui <- fluidPage(
  dashboardPage(
    dashboardHeader(title = "МСФО"),
    dashboardSidebar(width = 1050,
      sidebarMenu(
        menuItem("Home", tabName = "home"),
        menuItem("Учет", tabName = "Учет", 
          menuItem("Доходы", tabName = "Profit", 
            menuItem("6000.Доход от реализации продукции и оказания услуг", tabName = "Prft6000",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table6000"),
              menuItem("6010.Доход от реализации продукции и оказания услуг", tabName = "table6010"),
              menuItem("6020.Возврат проданной продукции", tabName = "table6020"),
              menuItem("6030.Скидки с цены и продаж", tabName = "table6030")),
            menuItem("6100.Доход от финансирования", tabName = "Prft6100",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table6100"),
              menuItem("6110.Доходы по финансовым активам", tabName = "Prft6110",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6110"),
		menuItem("6110.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6110_1"),
                menuItem("6110.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6110_2")),
              menuItem("6120.Доходы по дивидендам", tabName = "Prft6120",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6120"),
		menuItem("6120.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6120_1"),
                menuItem("6120.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6120_2")),
              menuItem("6130.Доходы от финансовой аренды", tabName = "Prft6130",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6130"),
		menuItem("6130.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6130_1"),
                menuItem("6130.2.Доходы, отражаемые в  Прочем совокупном доходе", tabName = "table6130_2")),
              menuItem("6140.Доходы от операций с инвестиционным имуществом", tabName = "Prft6140",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6140"),
                menuItem("6141.Доходы от операционной аренды", tabName = "Prft6141",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6141"),
		  menuItem("6141.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6141_1"),
                  menuItem("6141.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6141_2")),
                menuItem("6142.Доходы от прироста стоимости инвестиционного имущества", tabName = "Prft6142",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6142"),
		  menuItem("6142.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6142_1"),
                  menuItem("6142.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6142_2")),
		menuItem("6143.Доходы от реклассификации", tabName = "table6143")),
              menuItem("6150.Доход, отражаемый в прибыле и убытке", tabName = "table6150"),
              menuItem("6160.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6160"),
              menuItem("6170.Прочие доходы от финансирования", tabName = "Prft6170",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6170"),
		menuItem("6171.Прочие доходы, отражаемые в составе прибыли и убытка", tabName = "table6171"),
		menuItem("6172.Прочие доходы, отражаемые в Прочем совокупном доходе", tabName = "table6172"))),
            menuItem("6200.Прочие доходы", tabName = "Prft6200",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table6200"),
              menuItem("6210.Доходы от выбытия активов", tabName = "Prft6210",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6210"),
		menuItem("6210.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6210_1"),
                menuItem("6210.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6210_2")),
              menuItem("6220.Доходы, связанные с прекращаемой деятельностью", tabName = "Prft6220",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6220"),
		menuItem("6220.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6220_1"),
                menuItem("6220.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6220_2")),
              menuItem("6230.Доходы от операционной аренды (кроме инвестиционного имущества)", tabName = "Prft6230",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6230"),
		menuItem("6230.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6230_1"),
                menuItem("6230.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6230_2")),
              menuItem("6240.Доходы от безвозмездно полученных активов", tabName = "Prft6240",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6240"),
		menuItem("6240.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6240_1"),
                menuItem("6240.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6240_2")),
              menuItem("6250.Доходы от государственных субсидий", tabName = "Prft6250",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6250"),
		menuItem("6250.1.Доходы, отражаемые в составе прибыли и убытка", tabName = "table6250_1"),
                menuItem("6250.2.Доходы, отражаемые в Прочем совокупном доходе", tabName = "table6250_2")),
              menuItem("6260.Доходы от переоценки", tabName = "Prft6260",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6260"),
                menuItem("6261.Доход от переоценки основных средств", tabName = "Prft6261",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6261"),
		  menuItem("6261.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6261_1"),
                  menuItem("6261.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6261_2")),
                menuItem("6262.Доход от переоценки нематериальных активов", tabName = "Prft6262",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6262"),
		  menuItem("6262.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6262_1"),
                  menuItem("6262.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6262_2")),
                menuItem("6263.Доход от переоценки долгосрочных активов (для продажи или распределения собственникам)", tabName = "Prft6263",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6263"),
		  menuItem("6263.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6263_1"),
                  menuItem("6263.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6263_2")),
                menuItem("6264.Доход от переоценки выбывающих групп (для продажи или распределения собственникам)", tabName = "Prft6264",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6264"),
		  menuItem("6264.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6264_1"),
                  menuItem("6264.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6264_2"))),
              menuItem("6270.Доходы от производных инструментов", tabName = "Prft6270",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6270"),
                menuItem("6271.Доход от хеджирования чистых инвестиций в зарубежные операции: эффективная часть", tabName = "Prft6271",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6271"),
		  menuItem("6271.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6271_1"),
                  menuItem("6271.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6271_2")),
                menuItem("6272.Доход по инструментам хеджирования при хеджировании денежных потоков: эффективная часть", tabName = "Prft6272",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6272"),
		  menuItem("6272.1.Доход, отражаемый в  составе прибыли и убытка", tabName = "table6272_1"),
                  menuItem("6272.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6272_2")),
                menuItem("6273.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости: по инструментам хеджирования", tabName = "table6273"),
                menuItem("6274.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости: по объекту хеджирования", tabName = "table6274"),
                menuItem("6275.Доход от изменения величины временной стоимости опционов", tabName = "Prft6275",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6275"),
		  menuItem("6275.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6275_1"),
                  menuItem("6275.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6275_2")),
                menuItem("6276.Доход от изменения стоимости форвардных элементов форвардных договоров", tabName = "Prft6276",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6276"),
		  menuItem("6276.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6276_1"),
                  menuItem("6276.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6276_2")),
                menuItem("6277.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, не являющегося инструментом хеджирования", tabName = "Prft6277",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6277"),
		  menuItem("6277.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6277_1"),
                  menuItem("6277.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6277_2"))),
              menuItem("6280.Доход в результате пересчета валюты", tabName = "Prft6280",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6280"),
                menuItem("6281.Доход в результате пересчета иностранной валюты: чистые курсовые разницы", tabName = "Prft6281",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6281"),
		  menuItem("6281.1.Доход (чистые курсовые разницы), отражаемый в составе прибыли и убытка", tabName = "table6281_1"),
                  menuItem("6281.2.Доход (чистые курсовые разницы), отражаемый в Прочем совокупном доходе", tabName = "table6281_2")),
                menuItem("6282.Доход в результате пересчета финансовой отчетности в другую валюту ", tabName = "table6282")),
              menuItem("6290.Прочие доходы (в т.ч. восстановление убытка от обесценения)", tabName = "Prft6290",
                menuItem("Оборотно-сальдовая ведомость", tabName = "table6290"),
                menuItem("6291.Доход в связи с изменениями в прочем совокупном доходе объекта инвестиций", tabName = "table6291"),
                menuItem("6292.Доход от изменения справедливой стоимости биологических активов", tabName = "Prft6292",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6292"),
		  menuItem("6292.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6292_1"),
                  menuItem("6292.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6292_2")),
                menuItem("6293.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования", tabName = "Prft6293",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6293"),
		  menuItem("6293.1.Доход, отражаемый в составе прибыли и убытка", tabName = "table6293_1"),
                  menuItem("6293.2.Доход, отражаемый в Прочем совокупном доходе", tabName = "table6293_2")),
                menuItem("6294.Прочие доходы (в т.ч. восстановление убытка от обесценения), не учтенные в предыдущих субсчетах", tabName = "Prft6294",
                  menuItem("Оборотно-сальдовая ведомость", tabName = "table6294"),
		  menuItem("6294.1.Прочие доходы, отражаемые в составе прибыли и убытка", tabName = "table6294_1"),
                  menuItem("6294.2.Прочие доходы, отражаемые в Прочем совокупном доходе", tabName = "table6294_2")))),
            menuItem("6300.Доля в прибыли объектов инвестиций, учитываемая по методу долевого участия", tabName = "Prft6300",
              menuItem("Оборотно-сальдовая ведомость", tabName = "table6300"),
              menuItem("6310.Доля в прибыли материнской компании и компаний с совместным контролем или значительным влиянием", tabName = "table6310"),
              menuItem("6320.Доля в прибыли дочерних компаний и других организации той же группы", tabName = "table6320"),
              menuItem("6330.Доля в прибыли ассоциированных организаций и совместных предприятий", tabName = "table6330"))
          )
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
        tabItem(tabName = "table6000",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6000", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6000")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6000.Долгосрочная кредиторская задолженность"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6000Item1"),
	      downloadButton("download_df6000", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6010",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6010.Доход от реализации продукции и оказания услуг"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6010Item1"),
	      downloadButton("download_df6010", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6010", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6010")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6010Item2"),
	      downloadButton("download_df6010_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6020",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6020.Возврат проданной продукции"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6020Item1"),
	      downloadButton("download_df6020", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате, номеру документа, по которому состоялся возврат проданной продукции, или статье возврата"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6020", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру документа, по которому состоялся возврат проданной продукции", 
					"Выбор по статье возврата", 
					"Выбор по дате операции и номеру документа, по которому состоялся возврат проданной продукции", 
					"Выбор по дате операции и статье возврата")),
              uiOutput("nested_ui6020")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6020Item2"),
	      downloadButton("download_df6020_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6030",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6030.Скидки с цены и продаж"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6030Item1"),
	      downloadButton("download_df6030", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6030", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6030")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6030Item2"),
	      downloadButton("download_df6030_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6100",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6100", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6100")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6100.Доход от финансирования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6100Item1"),
	      downloadButton("download_df6100", "Загрузить данные"))
          )
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
              rHandsontableOutput("table6110_1Item1"),
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
              rHandsontableOutput("table6110_2Item1"),
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
        tabItem(tabName = "table6120",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6120", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6120")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6120.Доходы по дивидендам"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120Item1"),
	      downloadButton("download_df6120", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6120_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6120.1.Доходы по дивидендам, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6120_1Item1"),
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
              rHandsontableOutput("table6120_2Item1"),
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
        ),
        tabItem(tabName = "table6130",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6130", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6130")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6130.Доходы от финансовой аренды"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6130Item1"),
	      downloadButton("download_df6130", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6130_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6130.1.Доходы от финансовой аренды, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6130_1Item1"),
	      downloadButton("download_df6130.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6130.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6130.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6130.1Item2"),
	      downloadButton("download_df6130.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6130_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6130.2.Доходы от финансовой аренды, отражаемые в  Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6130_2Item1"),
	      downloadButton("download_df6130.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6130.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6130.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6130.2Item2"),
	      downloadButton("download_df6130.2_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6140",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6140", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6140")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6140.Доходы от операций с инвестиционным имуществом"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6140Item1"),
	      downloadButton("download_df6140", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6141",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6141", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6141")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6141.Доходы от операционной аренды"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6141Item1"),
	      downloadButton("download_df6141", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6141_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6141.1.Доходы от операционной аренды, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6141_1Item1"),
	      downloadButton("download_df6141.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6141.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6141.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6141.1Item2"),
	      downloadButton("download_df6141.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6141_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6141.2.Доходы от операционной аренды, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6141_2Item1"),
	      downloadButton("download_df6141.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6141.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6141.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6141.2Item2"),
	      downloadButton("download_df6141.2_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6142",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6142", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6142")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6142.Доходы от прироста стоимости инвестиционного имущества"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6142Item1"),
	      downloadButton("download_df6142", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6142_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6142.1.Доходы от прироста стоимости инвестиционного имущества, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6142_1Item1"),
	      downloadButton("download_df6142.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6142.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6142.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6142.1Item2"),
	      downloadButton("download_df6142.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6142_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6142.2.Доходы от прироста стоимости инвестиционного имущества, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6142_2Item1"),
	      downloadButton("download_df6142.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6142.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6142.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6142.2Item2"),
	      downloadButton("download_df6142.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6143",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6143.Доходы от реклассификации"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6143Item1"),
	      downloadButton("download_df6143", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6143", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6143")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6143Item2"),
	      downloadButton("download_df6143_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6150",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6150.Доход от изменения справедливой стоимости финансовых активов, в т.ч. инвестиций в долевые инструменты, оцениваемых по справедливой стоимости через прибыль и убытки"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6150Item1"),
	      downloadButton("download_df6150", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6150", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6150")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6150Item2"),
	      downloadButton("download_df6150_2", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6160",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6160.Доход от изменения справедливой стоимости финансовых активов, в т.ч. инвестиций в долевые инструменты, оцениваемых по справедливой стоимости через прочий совокупный доход"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6160Item1"),
	      downloadButton("download_df6160", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6160", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6160")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6160Item2"),
	      downloadButton("download_df6160_2", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6170",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6170", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6170")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6170.Прочие доходы от финансирования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6170Item1"),
	      downloadButton("download_df6170", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6171",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6171.Прочие доходы от финансирования, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6171Item1"),
	      downloadButton("download_df6171", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6171", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6171")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6171Item2"),
	      downloadButton("download_df6171_2", "Загрузить данные")
            )
          )
        ),
        tabItem(tabName = "table6172",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6172.Прочие доходы от финансирования, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6172Item1"),
	      downloadButton("download_df6172", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6172", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6172")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6172Item2"),
	      downloadButton("download_df6172_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6200",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6200", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6200")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6200.Прочие доходы"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6200Item1"),
	      downloadButton("download_df6200", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6210",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6210", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6210")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6210.Доходы от выбытия активов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6210Item1"),
	      downloadButton("download_df6210", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6210_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6210.1.Доход от выбытия активов, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6210_1Item1"),
	      downloadButton("download_df6210.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6210.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6210.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6210.1Item2"),
	      downloadButton("download_df6210.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6210_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6210.2.Доход от выбытия активов, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6210_2Item1"),
	      downloadButton("download_df6210.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6210.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6210.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6210.2Item2"),
	      downloadButton("download_df6210.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6220",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6220", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6220")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6220.Доходы, связанные с прекращаемой деятельностью"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6220Item1"),
	      downloadButton("download_df6220", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6220_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6220.1.Доходы в связи с прекращаемой деятельностью, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6220_1Item1"),
	      downloadButton("download_df6220.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6220.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6220.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6220.1Item2"),
	      downloadButton("download_df6220.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6220_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6220.2.Доходы в связи с прекращаемой деятельностью, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6220_2Item1"),
	      downloadButton("download_df6220.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6220.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6220.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6220.2Item2"),
	      downloadButton("download_df6220.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6230",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6230", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6230")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6230.Доходы от операционной аренды (кроме инвестиционного имущества)"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6230Item1"),
	      downloadButton("download_df6230", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6230_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6230.1.Доход от операционной аренды, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6230_1Item1"),
	      downloadButton("download_df6230.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6230.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6230.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6230.1Item2"),
	      downloadButton("download_df6230.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6230_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6230.2.Доход от операционной аренды, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6230_2Item1"),
	      downloadButton("download_df6230.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6230.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6230.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6230.2Item2"),
	      downloadButton("download_df6230.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6240",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6240", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6240")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6240.Доходы от безвозмездно полученных активов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6240Item1"),
	      downloadButton("download_df6240", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6240_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6240.1.Доход от безвозмездно полученных активов, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6240_1Item1"),
	      downloadButton("download_df6240.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6240.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6240.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6240.1Item2"),
	      downloadButton("download_df6240.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6240_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6240.2.Доход от безвозмездно полученных активов, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6240_2Item1"),
	      downloadButton("download_df6240.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6240.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6240.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6240.2Item2"),
	      downloadButton("download_df6240.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6250",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6250", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6250")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6250.Доходы от государственных субсидий"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6250Item1"),
	      downloadButton("download_df6250", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6250_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6250.1.Доходы от государственных субсидий, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6250_1Item1"),
	      downloadButton("download_df6250.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6250.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6250.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6250.1Item2"),
	      downloadButton("download_df6250.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6250_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6250.2.Доходы от государственных субсидий, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6250_2Item1"),
	      downloadButton("download_df6250.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6250.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6250.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6250.2Item2"),
	      downloadButton("download_df6250.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6260",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6260", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6260")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6260.Доходы от переоценки"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6260Item1"),
	      downloadButton("download_df6260", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6261",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6261", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6261")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6261.Доход от переоценки основных средств"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6261Item1"),
	      downloadButton("download_df6261", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6261_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6261.1.Доход от переоценки основных средств, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6261_1Item1"),
	      downloadButton("download_df6261.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6261.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6261.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6261.1Item2"),
	      downloadButton("download_df6261.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6261_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6261.2.Доход от переоценки основных средств, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6261_2Item1"),
	      downloadButton("download_df6261.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6261.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6261.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6261.2Item2"),
	      downloadButton("download_df6261.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6262",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6262", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6262")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6262.Доход от переоценки нематериальных активов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6262Item1"),
	      downloadButton("download_df6262", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6262_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6262.1.Доход от переоценки нематериальных активов, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6262_1Item1"),
	      downloadButton("download_df6262.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6262.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6262.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6262.1Item2"),
	      downloadButton("download_df6262.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6262_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6262.2.Доход от переоценки нематериальных активов, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6262_2Item1"),
	      downloadButton("download_df6262.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6262.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6262.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6262.2Item2"),
	      downloadButton("download_df6262.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6263",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6263", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6263")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6263.Доход от переоценки долгосрочных активов, предназначенных для продажи или распределения собственникам"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6263Item1"),
	      downloadButton("download_df6263", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6263_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6263.1.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6263_1Item1"),
	      downloadButton("download_df6263.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6263.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6263.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6263.1Item2"),
	      downloadButton("download_df6263.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6263_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6263.2.Доход от переоценки долгосрочных активов для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6263_2Item1"),
	      downloadButton("download_df6263.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6263.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6263.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6263.2Item2"),
	      downloadButton("download_df6263.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6264",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6264", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6264")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6264.Доход от переоценки выбывающих групп, предназначенных для продажи или распределения собственникам"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6264Item1"),
	      downloadButton("download_df6264", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6264_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6264.1.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6264_1Item1"),
	      downloadButton("download_df6264.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6264.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6264.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6264.1Item2"),
	      downloadButton("download_df6264.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6264_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6264.2.Доход от переоценки выбывающих групп для продажи или распределения собственникам, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6264_2Item1"),
	      downloadButton("download_df6264.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6264.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6264.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6264.2Item2"),
	      downloadButton("download_df6264.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6270",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6270", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6270")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6270.Доходы от производных инструментов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6270Item1"),
	      downloadButton("download_df6270", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6271",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6271", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6271")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6271.Доход от хеджирования чистых инвестиций в зарубежные операции: эффективная часть"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6271Item1"),
	      downloadButton("download_df6271", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6271_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6271.1.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6271_1Item1"),
	      downloadButton("download_df6271.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6271.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6271.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6271.1Item2"),
	      downloadButton("download_df6271.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6271_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6271.2.Доход от хеджирования чистых инвестиций в зарубежные операции (эффективная часть), отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6271_2Item1"),
	      downloadButton("download_df6271.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6271.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6271.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6271.2Item2"),
	      downloadButton("download_df6271.2_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6272",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6272", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6272")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6272.Доход по инструментам хеджирования при хеджировании денежных потоков: эффективная часть"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6272Item1"),
	      downloadButton("download_df6272", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6272_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6272.1.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в  составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6272_1Item1"),
	      downloadButton("download_df6272.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6272.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6272.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6272.1Item2"),
	      downloadButton("download_df6272.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6272_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6272.2.Доход по инструментам хеджирования при хеджировании денежных потоков (эффективная часть), отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6272_2Item1"),
	      downloadButton("download_df6272.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6272.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6272.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6272.2Item2"),
	      downloadButton("download_df6272.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6273",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6273.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через Прочий совокупный доход: по инструментам хеджирования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6273Item1"),
	      downloadButton("download_df6273", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6273", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6273")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6273Item2"),
	      downloadButton("download_df6273_2", "Загрузить данные"))
          )
        ),
         tabItem(tabName = "table6274",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6274.Доход от хеджирования инвестиций в долевые инструменты, оцениваемые по справедливой стоимости через Прочий совокупный доход: по объекту хеджирования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6274Item1"),
	      downloadButton("download_df6274", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6274", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6274")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6274Item2"),
	      downloadButton("download_df6274_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6275",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6275", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6275")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6275.Доход от изменения величины временной стоимости опционов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6275Item1"),
	      downloadButton("download_df6275", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6275_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6275.1.Доход от изменения величины временной стоимости опционов, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6275_1Item1"),
	      downloadButton("download_df6275.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6275.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6275.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6275.1Item2"),
	      downloadButton("download_df6275.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6275_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6275.2.Доход от изменения величины временной стоимости опционов, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6275_2Item1"),
	      downloadButton("download_df6275.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6275.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6275.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6275.2Item2"),
	      downloadButton("download_df6275.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6276",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6276", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6276")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6276.Доход от изменения стоимости форвардных элементов форвардных договоров"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6276Item1"),
	      downloadButton("download_df6276", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6276_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6276.1.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6276_1Item1"),
	      downloadButton("download_df6276.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6276.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6276.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6276.1Item2"),
	      downloadButton("download_df6276.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6276_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6276.2.Доход от изменения стоимости форвардных элементов форвардных договоров, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6276_2Item1"),
	      downloadButton("download_df6276.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6276.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6276.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6276.2Item2"),
	      downloadButton("download_df6276.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6277",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6277", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6277")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6277.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, не являющегося инструментом хеджирования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6277Item1"),
	      downloadButton("download_df6277", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6277_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6277.1.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6277_1Item1"),
	      downloadButton("download_df6277.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6277.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6277.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6277.1Item2"),
	      downloadButton("download_df6277.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6277_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6277.2.Доход от изменения стоимости валютного базисного спрэда финансового инструмента, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6277_2Item1"),
	      downloadButton("download_df6277.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6277.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6277.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6277.2Item2"),
	      downloadButton("download_df6277.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6280",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6280", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6280")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6280.Доход в результате пересчета валюты"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6280Item1"),
	      downloadButton("download_df6280", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6281",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6281", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6281")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6281.Доход в результате пересчета иностранной валюты: чистые курсовые разницы"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6281Item1"),
	      downloadButton("download_df6281", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6281_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6281.1.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6281_1Item1"),
	      downloadButton("download_df6281.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6281.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6281.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6281.1Item2"),
	      downloadButton("download_df6281.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6281_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6281.2.Доход в результате пересчета иностранной валюты (чистые курсовые разницы), отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6281_2Item1"),
	      downloadButton("download_df6281.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6281.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6281.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6281.2Item2"),
	      downloadButton("download_df6281.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6282",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6282.Доход в результате пересчета финансовой отчетности в другую валюту"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6282Item1"),
	      downloadButton("download_df6282", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6282", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6282")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6282Item2"),
	      downloadButton("download_df6282_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6290",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6290", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6290")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6290.Прочие доходы (в т.ч. восстановление убытка от обесценения)"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6290Item1"),
	      downloadButton("download_df6290", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6291",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6291.Доход в связи с изменениями в прочем совокупном доходе объекта инвестиций"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6291Item1"),
	      downloadButton("download_df6291", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6291", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6291")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6291Item2"),
	      downloadButton("download_df6291_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6292",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6292", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6292")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6292.Доход от изменения справедливой стоимости биологических активов"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6292Item1"),
	      downloadButton("download_df6292", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6292_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6292.1.Доход от изменения справедливой стоимости биологических активов, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6292_1Item1"),
	      downloadButton("download_df6292.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6292.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6292.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6292.1Item2"),
	      downloadButton("download_df6292.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6292_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6292.2.Доход от изменения справедливой стоимости биологических активов, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6292_2Item1"),
	      downloadButton("download_df6292.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6292.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6292.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6292.2Item2"),
	      downloadButton("download_df6292.2_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6293",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6293", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6293")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6293.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6293Item1"),
	      downloadButton("download_df6293", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6293_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6293.1.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6293_1Item1"),
	      downloadButton("download_df6293.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6293.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6293.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6293.1Item2"),
	      downloadButton("download_df6293.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6293_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6293.2.Доход по выпущенным договорам страхования и удерживаемым договорам перестрахования, отражаемый в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6293_2Item1"),
	      downloadButton("download_df6293.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6293.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6293.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6293.2Item2"),
	      downloadButton("download_df6293.2_2", "Загрузить данные"))
           )
         ),
        tabItem(tabName = "table6294",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6294", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6294")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6294.Прочие доходы (в т.ч. восстановление убытка от обесценения), не учтенные в предыдущих субсчетах"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6294Item1"),
	      downloadButton("download_df6294", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6294_1",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6294.1.Прочие доходы, отражаемые в составе прибыли и убытка"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6294_1Item1"),
	      downloadButton("download_df6294.1", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6294.1", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6294.1")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6294.1Item2"),
	      downloadButton("download_df6294.1_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6294_2",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6294.2.Прочие доходы, отражаемые в Прочем совокупном доходе"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6294_2Item1"),
	      downloadButton("download_df6294.2", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате операции, номеру первичного документа или статье дохода"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6294.2", label=NULL,
                          choices = c(	"Выбор по дате операции", 
					"Выбор по номеру первичного документа", 
					"Выбор по статье дохода", 
					"Выбор по дате операции и номеру первичного документа", 
					"Выбор по дате операции и статье дохода")),
              uiOutput("nested_ui6294.2")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6294.2Item2"),
	      downloadButton("download_df6294.2_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6300",
          fluidRow(
            column(
              width = 12, br(),
              dateRangeInput("dates6300", "Выберите период ОСВ:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-"),
              uiOutput("nested_ui6300")),
            column(
              width = 12, br(),
              tags$b("ОСВ: 6300.Доля в прибыли объектов инвестиций, учитываемая по методу долевого участия"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6300Item1"),
	      downloadButton("download_df6300", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6310",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6310.Доля в прибыли материнской компании и компаний с совместным контролем или значительным влиянием"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6310Item1"),
	      downloadButton("download_df6310", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате и/или номеру учета (реквизитам) объекта инвестиции"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6310", label=NULL,
                          choices = c(	"Выбор по дате учета", 
					"Выбор по номеру учета объекта инвестиции", 
					"Выбор по дате и номеру учета объекта инвестиции")),
              uiOutput("nested_ui6310")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6310Item2"),
	      downloadButton("download_df6310_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6320",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6320.Доля в прибыли дочерних компаний и других организации той же группы"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6320Item1"),
	      downloadButton("download_df6320", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате и/или номеру учета (реквизитам) объекта инвестиции"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6320", label=NULL,
                          choices = c(	"Выбор по дате учета", 
					"Выбор по номеру учета объекта инвестиции", 
					"Выбор по дате и номеру учета объекта инвестиции")),
              uiOutput("nested_ui6320")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6320Item2"),
	      downloadButton("download_df6320_2", "Загрузить данные"))
          )
        ),
        tabItem(tabName = "table6330",
          fluidRow(
            column(
              width = 12, br(),
              tags$b("Журнал учета хозопераций: 6330.Доля в прибыли ассоциированных организаций и совместных предприятий"),
	      tags$div(style = "margin-bottom: 20px;"),
              rHandsontableOutput("table6330Item1"),
	      downloadButton("download_df6330", "Загрузить данные")
            ),
            column(
              width = 12, br(),
              tags$b("Выборка данных по дате и/или номеру учета (реквизитам) объекта инвестиции"),
	      tags$div(style = "margin-bottom: 20px;"),
              selectInput("choices6330", label=NULL,
                          choices = c(	"Выбор по дате учета", 
					"Выбор по номеру учета объекта инвестиции", 
					"Выбор по дате и номеру учета объекта инвестиции")),
              uiOutput("nested_ui6330")
            ),
            column(
              width = 12, br(),
              label=NULL,
              rHandsontableOutput("table6330Item2"),
	      downloadButton("download_df6330_2", "Загрузить данные"))
            )
          )
        )
      )
    )
  )

#**********************************

 server = function(input, output, session) {

   r <- reactiveValues(
     start = ymd(Sys.Date()),
     end = ymd(Sys.Date())
   )
 
   data <- reactiveValues()

  observe({
    data$df6000 <- as.data.table(DF6000)
    data$df6010 <- as.data.table(DF6010)
    data$df6020 <- as.data.table(DF6020)
    data$df6030 <- as.data.table(DF6030)

    data$df6010_2 <- as.data.table(DF6010_2)
    data$df6020_2 <- as.data.table(DF6020_2)
    data$df6030_2 <- as.data.table(DF6030_2)

    data$df6100 <- as.data.table(DF6100)
    data$df6110 <- as.data.table(DF6110)
    data$df6110.1 <- as.data.table(DF6110.1)
    data$df6110.2 <- as.data.table(DF6110.2)
    data$df6120 <- as.data.table(DF6120)
    data$df6120.1 <- as.data.table(DF6120.1)
    data$df6120.2 <- as.data.table(DF6120.2)
    data$df6130 <- as.data.table(DF6130)
    data$df6130.1 <- as.data.table(DF6130.1)
    data$df6130.2 <- as.data.table(DF6130.2)
    data$df6140 <- as.data.table(DF6140)
    data$df6141 <- as.data.table(DF6141)
    data$df6141.1 <- as.data.table(DF6141.1)
    data$df6141.2 <- as.data.table(DF6141.2)
    data$df6142 <- as.data.table(DF6142)
    data$df6142.1 <- as.data.table(DF6142.1)
    data$df6142.2 <- as.data.table(DF6142.2)
    data$df6143 <- as.data.table(DF6143)
    data$df6141 <- as.data.table(DF6141)
    data$df6142 <- as.data.table(DF6142)
    data$df6143 <- as.data.table(DF6143)
    data$df6150 <- as.data.table(DF6150)
    data$df6160 <- as.data.table(DF6160)
    data$df6170 <- as.data.table(DF6170)
    data$df6171 <- as.data.table(DF6171)
    data$df6172 <- as.data.table(DF6172)

    data$df6110.1_2 <- as.data.table(DF6110.1_2)
    data$df6110.2_2 <- as.data.table(DF6110.2_2)
    data$df6110_3 <- as.data.table(DF6110_3)
    data$df6120.1_2 <- as.data.table(DF6120.1_2)
    data$df6120.2_2 <- as.data.table(DF6120.2_2)
    data$df6120_3 <- as.data.table(DF6120_3)
    data$df6130.1_2 <- as.data.table(DF6130.1_2)
    data$df6130.2_2 <- as.data.table(DF6130.2_2)
    data$df6130_3 <- as.data.table(DF6130_3)
    data$df6140_3 <- as.data.table(DF6140_3)
    data$df6141_2 <- as.data.table(DF6141_2)
    data$df6141_3 <- as.data.table(DF6141_3)
    data$df6141.1_2 <- as.data.table(DF6141.1_2)
    data$df6141.2_2 <- as.data.table(DF6141.2_2)
    data$df6142_2 <- as.data.table(DF6142_2)
    data$df6142_3 <- as.data.table(DF6142_3)
    data$df6142.1_2 <- as.data.table(DF6142.1_2)
    data$df6142.2_2 <- as.data.table(DF6142.2_2)
    data$df6143_2 <- as.data.table(DF6143_2)
    data$df6150_2 <- as.data.table(DF6150_2)
    data$df6160_2 <- as.data.table(DF6160_2)
    data$df6170_2 <- as.data.table(DF6170_2)
    data$df6171_2 <- as.data.table(DF6171_2)
    data$df6172_2 <- as.data.table(DF6172_2)

    data$df6200 <- as.data.table(DF6200)
    data$df6210 <- as.data.table(DF6210)
    data$df6210.1 <- as.data.table(DF6210.1)
    data$df6210.2 <- as.data.table(DF6210.2)
    data$df6220 <- as.data.table(DF6220)
    data$df6220.1 <- as.data.table(DF6220.1)
    data$df6220.2 <- as.data.table(DF6220.2)
    data$df6230 <- as.data.table(DF6230)
    data$df6230.1 <- as.data.table(DF6230.1)
    data$df6230.2 <- as.data.table(DF6230.2)
    data$df6240 <- as.data.table(DF6240)
    data$df6240.1 <- as.data.table(DF6240.1)
    data$df6240.2 <- as.data.table(DF6240.2)
    data$df6250 <- as.data.table(DF6250)
    data$df6250.1 <- as.data.table(DF6250.1)
    data$df6250.2 <- as.data.table(DF6250.2)
    data$df6260 <- as.data.table(DF6260)
    data$df6261 <- as.data.table(DF6261)
    data$df6261.1 <- as.data.table(DF6261.1)
    data$df6261.2 <- as.data.table(DF6261.2)
    data$df6262 <- as.data.table(DF6262)
    data$df6262.1 <- as.data.table(DF6262.1)
    data$df6262.2 <- as.data.table(DF6262.2)
    data$df6263 <- as.data.table(DF6263)
    data$df6263.1 <- as.data.table(DF6263.1)
    data$df6263.2 <- as.data.table(DF6263.2)
    data$df6264 <- as.data.table(DF6264)
    data$df6264.1 <- as.data.table(DF6264.1)
    data$df6264.2 <- as.data.table(DF6264.2)
    data$df6270 <- as.data.table(DF6270)
    data$df6271 <- as.data.table(DF6271)
    data$df6271.1 <- as.data.table(DF6271.1)
    data$df6271.2 <- as.data.table(DF6271.2)
    data$df6272 <- as.data.table(DF6272)
    data$df6272.1 <- as.data.table(DF6272.1)
    data$df6272.2 <- as.data.table(DF6272.2)
    data$df6273 <- as.data.table(DF6273)
    data$df6274 <- as.data.table(DF6274)
    data$df6275 <- as.data.table(DF6275)
    data$df6275.1 <- as.data.table(DF6275.1)
    data$df6275.2 <- as.data.table(DF6275.2)
    data$df6276 <- as.data.table(DF6276)
    data$df6276.1 <- as.data.table(DF6276.1)
    data$df6276.2 <- as.data.table(DF6276.2)
    data$df6277 <- as.data.table(DF6277)
    data$df6277.1 <- as.data.table(DF6277.1)
    data$df6277.2 <- as.data.table(DF6277.2)
    data$df6280 <- as.data.table(DF6280)
    data$df6281 <- as.data.table(DF6281)
    data$df6281.1 <- as.data.table(DF6281.1)
    data$df6281.2 <- as.data.table(DF6281.2)
    data$df6282 <- as.data.table(DF6282)
    data$df6290 <- as.data.table(DF6290)
    data$df6291 <- as.data.table(DF6291)
    data$df6292 <- as.data.table(DF6292)
    data$df6292.1 <- as.data.table(DF6292.1)
    data$df6292.2 <- as.data.table(DF6292.2)
    data$df6293 <- as.data.table(DF6293)
    data$df6293.1 <- as.data.table(DF6293.1)
    data$df6293.2 <- as.data.table(DF6293.2)
    data$df6294 <- as.data.table(DF6294)
    data$df6294.1 <- as.data.table(DF6294.1)
    data$df6294.2 <- as.data.table(DF6294.2)

    data$df6210_3 <- as.data.table(DF6210_3)
    data$df6210.1_2 <- as.data.table(DF6210.1_2)
    data$df6210.2_2 <- as.data.table(DF6210.2_2)
    data$df6220_3 <- as.data.table(DF6220_3)
    data$df6220.1_2 <- as.data.table(DF6220.1_2)
    data$df6220.2_2 <- as.data.table(DF6220.2_2)
    data$df6230_3 <- as.data.table(DF6230_3)
    data$df6230.1_2 <- as.data.table(DF6230.1_2)
    data$df6230.2_2 <- as.data.table(DF6230.2_2)
    data$df6240_3 <- as.data.table(DF6240_3)
    data$df6240.1_2 <- as.data.table(DF6240.1_2)
    data$df6240.2_2 <- as.data.table(DF6240.2_2)
    data$df6250_3 <- as.data.table(DF6250_3)
    data$df6250.1_2 <- as.data.table(DF6250.1_2)
    data$df6250.2_2 <- as.data.table(DF6250.2_2)
    data$df6260_3 <- as.data.table(DF6260_3)
    data$df6261_2 <- as.data.table(DF6261_2)
    data$df6261_3 <- as.data.table(DF6261_3)
    data$df6261.1_2 <- as.data.table(DF6261.1_2)
    data$df6261.2_2 <- as.data.table(DF6261.2_2)
    data$df6262_2 <- as.data.table(DF6262_2)
    data$df6262_3 <- as.data.table(DF6262_3)
    data$df6262.1_2 <- as.data.table(DF6262.1_2)
    data$df6262.2_2 <- as.data.table(DF6262.2_2)
    data$df6263_2 <- as.data.table(DF6263_2)
    data$df6263_3 <- as.data.table(DF6263_3)
    data$df6263.1_2 <- as.data.table(DF6263.1_2)
    data$df6263.2_2 <- as.data.table(DF6263.2_2)
    data$df6264_2 <- as.data.table(DF6264_2)
    data$df6264_3 <- as.data.table(DF6264_3)
    data$df6264.1_2 <- as.data.table(DF6264.1_2)
    data$df6264.2_2 <- as.data.table(DF6264.2_2)
    data$df6270_3 <- as.data.table(DF6270_3)
    data$df6271_2 <- as.data.table(DF6271_2)
    data$df6271_3 <- as.data.table(DF6271_3)
    data$df6271.1_2 <- as.data.table(DF6271.1_2)
    data$df6271.2_2 <- as.data.table(DF6271.2_2)
    data$df6272_2 <- as.data.table(DF6272_2)
    data$df6272_3 <- as.data.table(DF6272_3)
    data$df6272.1_2 <- as.data.table(DF6272.1_2)
    data$df6272.2_2 <- as.data.table(DF6272.2_2)
    data$df6273_2 <- as.data.table(DF6273_2)
    data$df6274_2 <- as.data.table(DF6274_2)
    data$df6275_2 <- as.data.table(DF6275_2)
    data$df6275_3 <- as.data.table(DF6275_3)
    data$df6275.1_2 <- as.data.table(DF6275.1_2)
    data$df6275.2_2 <- as.data.table(DF6275.2_2)
    data$df6276_2 <- as.data.table(DF6276_2)
    data$df6276_3 <- as.data.table(DF6276_3)
    data$df6276.1_2 <- as.data.table(DF6276.1_2)
    data$df6276.2_2 <- as.data.table(DF6276.2_2)
    data$df6277_2 <- as.data.table(DF6277_2)
    data$df6277_3 <- as.data.table(DF6277_3)
    data$df6277.1_2 <- as.data.table(DF6277.1_2)
    data$df6277.2_2 <- as.data.table(DF6277.2_2)
    data$df6280_3 <- as.data.table(DF6280_3)
    data$df6281_2 <- as.data.table(DF6281_2)
    data$df6281_3 <- as.data.table(DF6281_3)
    data$df6281.1_2 <- as.data.table(DF6281.1_2)
    data$df6281.2_2 <- as.data.table(DF6281.2_2)
    data$df6282_2 <- as.data.table(DF6282_2)
    data$df6290_3 <- as.data.table(DF6290_3)
    data$df6291_2 <- as.data.table(DF6291_2)
    data$df6292_2 <- as.data.table(DF6292_2)
    data$df6292_3 <- as.data.table(DF6292_3)
    data$df6292.1_2 <- as.data.table(DF6292.1_2)
    data$df6292.2_2 <- as.data.table(DF6292.2_2)
    data$df6293_2 <- as.data.table(DF6293_2)
    data$df6293_3 <- as.data.table(DF6293_3)
    data$df6293.1_2 <- as.data.table(DF6293.1_2)
    data$df6293.2_2 <- as.data.table(DF6293.2_2)
    data$df6294_2 <- as.data.table(DF6294_2)
    data$df6294_3 <- as.data.table(DF6294_3)
    data$df6294.1_2 <- as.data.table(DF6294.1_2)
    data$df6294.2_2 <- as.data.table(DF6294.2_2)

    data$df6300 <- as.data.table(DF6300)
    data$df6310 <- as.data.table(DF6310)
    data$df6320 <- as.data.table(DF6320)
    data$df6330 <- as.data.table(DF6330)

    data$df6310_2 <- as.data.table(DF6310_2)
    data$df6320_2 <- as.data.table(DF6320_2)
    data$df6330_2 <- as.data.table(DF6330_2)
  })

  observe({
    if(!is.null(input$table6000Item1))
      data$df6000 <- hot_to_r(input$table6000Item1)
  })

  observe({
    if(!is.null(input$table6010Item1)) {
      new_df <- hot_to_r(input$table6010Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6010 <- validation_result$df
      } else {
        data$df6010 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6020Item1)) {
      new_df <- hot_to_r(input$table6020Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6020 <- validation_result$df
      } else {
        data$df6020 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6030Item1)) {
      new_df <- hot_to_r(input$table6030Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6030 <- validation_result$df
      } else {
        data$df6030 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6100Item1))
      data$df6100 <- hot_to_r(input$table6100Item1)
  })

  observe({
    if(!is.null(input$table6110Item1))
      data$df6110 <- hot_to_r(input$table6110Item1)
  })

  observe({
    if(!is.null(input$table6110Item1))
      data$df6110_3 <- hot_to_r(input$table6110Item1)
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

  observe({
    if(!is.null(input$table6120Item1))
      data$df6120 <- hot_to_r(input$table6120Item1)
  })

  observe({
    if(!is.null(input$table6120Item1))
      data$df6120_3 <- hot_to_r(input$table6120Item1)
  })

  observe({
    if(!is.null(input$table6120_1Item1)) {
      new_df <- hot_to_r(input$table6120_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6120.1 <- validation_result$df
      } else {
        data$df6120.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6120_2Item1)) {
      new_df <- hot_to_r(input$table6120_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6120.2 <- validation_result$df
      } else {
        data$df6120.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6130Item1))
      data$df6130 <- hot_to_r(input$table6130Item1)
  })

  observe({
    if(!is.null(input$table6130Item1))
      data$df6130_3 <- hot_to_r(input$table6130Item1)
  })

  observe({
    if(!is.null(input$table6130_1Item1)) {
      new_df <- hot_to_r(input$table6130_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6130.1 <- validation_result$df
      } else {
        data$df6130.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6130_2Item1)) {
      new_df <- hot_to_r(input$table6130_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6130.2 <- validation_result$df
      } else {
        data$df6130.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6140Item1))
      data$df6140 <- hot_to_r(input$table6140Item1)
  })

  observe({
    if(!is.null(input$table6140Item1))
      data$df6140_3 <- hot_to_r(input$table6140Item1)
  })

  observe({
    if(!is.null(input$table6141Item1))
      data$df6141 <- hot_to_r(input$table6141Item1)
  })

  observe({
    if(!is.null(input$table6141Item1))
      data$df6141_2 <- hot_to_r(input$table6141Item1)
  })

  observe({
    if(!is.null(input$table6141_1Item1)) {
      new_df <- hot_to_r(input$table6141_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6141.1 <- validation_result$df
      } else {
        data$df6141.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6141_2Item1)) {
      new_df <- hot_to_r(input$table6141_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6141.2 <- validation_result$df
      } else {
        data$df6141.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6142Item1))
      data$df6142 <- hot_to_r(input$table6142Item1)
  })

  observe({
    if(!is.null(input$table6142Item1))
      data$df6142_2 <- hot_to_r(input$table6142Item1)
  })

  observe({
    if(!is.null(input$table6142_1Item1)) {
      new_df <- hot_to_r(input$table6142_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6142.1 <- validation_result$df
      } else {
        data$df6142.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6142_2Item1)) {
      new_df <- hot_to_r(input$table6142_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6142.2 <- validation_result$df
      } else {
        data$df6142.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6143Item1)) {
      new_df <- hot_to_r(input$table6143Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6143 <- validation_result$df
      } else {
        data$df6143 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6150Item1)) {
      new_df <- hot_to_r(input$table6150Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6150 <- validation_result$df
      } else {
        data$df6150 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6160Item1)) {
      new_df <- hot_to_r(input$table6160Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6160 <- validation_result$df
      } else {
        data$df6160 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6170Item1))
      data$df6170 <- hot_to_r(input$table6170Item1)
  })

  observe({
    if(!is.null(input$table6171Item1)) {
      new_df <- hot_to_r(input$table6171Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6171 <- validation_result$df
      } else {
        data$df6171 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6172Item1)) {
      new_df <- hot_to_r(input$table6172Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6172 <- validation_result$df
      } else {
        data$df6172 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6200Item1))
      data$df6200 <- hot_to_r(input$table6200Item1)
  })

  observe({
    if(!is.null(input$table6210Item1))
      data$df6210 <- hot_to_r(input$table6210Item1)
  })

  observe({
    if(!is.null(input$table6210Item1))
      data$df6210_3 <- hot_to_r(input$table6210Item1)
  })

  observe({
    if(!is.null(input$table6210_1Item1)) {
      new_df <- hot_to_r(input$table6210_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6210.1 <- validation_result$df
      } else {
        data$df6210.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6210_2Item1)) {
      new_df <- hot_to_r(input$table6210_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6210.2 <- validation_result$df
      } else {
        data$df6210.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6220Item1))
      data$df6220 <- hot_to_r(input$table6220Item1)
  })

  observe({
    if(!is.null(input$table6220Item1))
      data$df6220_3 <- hot_to_r(input$table6220Item1)
  })

  observe({
    if(!is.null(input$table6220_1Item1)) {
      new_df <- hot_to_r(input$table6220_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6220.1 <- validation_result$df
      } else {
        data$df6220.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6220_2Item1)) {
      new_df <- hot_to_r(input$table6220_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6220.2 <- validation_result$df
      } else {
        data$df6220.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6230Item1))
      data$df6230 <- hot_to_r(input$table6230Item1)
  })

  observe({
    if(!is.null(input$table6230Item1))
      data$df6230_3 <- hot_to_r(input$table6230Item1)
  })

  observe({
    if(!is.null(input$table6230_1Item1)) {
      new_df <- hot_to_r(input$table6230_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6230.1 <- validation_result$df
      } else {
        data$df6230.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6230_2Item1)) {
      new_df <- hot_to_r(input$table6230_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6230.2 <- validation_result$df
      } else {
        data$df6230.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6240Item1))
      data$df6240 <- hot_to_r(input$table6240Item1)
  })

  observe({
    if(!is.null(input$table6240Item1))
      data$df6240_3 <- hot_to_r(input$table6240Item1)
  })

  observe({
    if(!is.null(input$table6240_1Item1)) {
      new_df <- hot_to_r(input$table6240_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6240.1 <- validation_result$df
      } else {
        data$df6240.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6240_2Item1)) {
      new_df <- hot_to_r(input$table6240_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6240.2 <- validation_result$df
      } else {
        data$df6240.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6250Item1))
      data$df6250 <- hot_to_r(input$table6250Item1)
  })

  observe({
    if(!is.null(input$table6250Item1))
      data$df6250_3 <- hot_to_r(input$table6250Item1)
  })

  observe({
    if(!is.null(input$table6250_1Item1)) {
      new_df <- hot_to_r(input$table6250_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6250.1 <- validation_result$df
      } else {
        data$df6250.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6250_2Item1)) {
      new_df <- hot_to_r(input$table6250_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6250.2 <- validation_result$df
      } else {
        data$df6250.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6260Item1))
      data$df6260 <- hot_to_r(input$table6260Item1)
  })

  observe({
    if(!is.null(input$table6260Item1))
      data$df6260_3 <- hot_to_r(input$table6260Item1)
  })

  observe({
    if(!is.null(input$table6261Item1))
      data$df6261 <- hot_to_r(input$table6261Item1)
  })

  observe({
    if(!is.null(input$table6261Item1))
      data$df6261_2 <- hot_to_r(input$table6261Item1)
  })

  observe({
    if(!is.null(input$table6261Item1))
      data$df6261_3 <- hot_to_r(input$table6261Item1)
  })

  observe({
    if(!is.null(input$table6261_1Item1)) {
      new_df <- hot_to_r(input$table6261_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6261.1 <- validation_result$df
      } else {
        data$df6261.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6261_2Item1)) {
      new_df <- hot_to_r(input$table6261_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6261.2 <- validation_result$df
      } else {
        data$df6261.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6262Item1))
      data$df6262 <- hot_to_r(input$table6262Item1)
  })

  observe({
    if(!is.null(input$table6262Item1))
      data$df6262_2 <- hot_to_r(input$table6262Item1)
  })

  observe({
    if(!is.null(input$table6262Item1))
      data$df6262_3 <- hot_to_r(input$table6262Item1)
  })

  observe({
    if(!is.null(input$table6262_1Item1)) {
      new_df <- hot_to_r(input$table6262_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6262.1 <- validation_result$df
      } else {
        data$df6262.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6262_2Item1)) {
      new_df <- hot_to_r(input$table6262_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6262.2 <- validation_result$df
      } else {
        data$df6262.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6263Item1))
      data$df6263 <- hot_to_r(input$table6263Item1)
  })

  observe({
    if(!is.null(input$table6263Item1))
      data$df6263_2 <- hot_to_r(input$table6263Item1)
  })

  observe({
    if(!is.null(input$table6263Item1))
      data$df6263_3 <- hot_to_r(input$table6263Item1)
  })

  observe({
    if(!is.null(input$table6263_1Item1)) {
      new_df <- hot_to_r(input$table6263_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6263.1 <- validation_result$df
      } else {
        data$df6263.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6263_2Item1)) {
      new_df <- hot_to_r(input$table6263_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6263.2 <- validation_result$df
      } else {
        data$df6263.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6264Item1))
      data$df6264 <- hot_to_r(input$table6264Item1)
  })

  observe({
    if(!is.null(input$table6264Item1))
      data$df6264_2 <- hot_to_r(input$table6264Item1)
  })

  observe({
    if(!is.null(input$table6264Item1))
      data$df6264_3 <- hot_to_r(input$table6264Item1)
  })

  observe({
    if(!is.null(input$table6264_1Item1)) {
      new_df <- hot_to_r(input$table6264_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6264.1 <- validation_result$df
      } else {
        data$df6264.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6264_2Item1)) {
      new_df <- hot_to_r(input$table6264_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6264.2 <- validation_result$df
      } else {
        data$df6264.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6270Item1))
      data$df6270 <- hot_to_r(input$table6270Item1)
  })

  observe({
    if(!is.null(input$table6270Item1))
      data$df6270_3 <- hot_to_r(input$table6270Item1)
  })

  observe({
    if(!is.null(input$table6271Item1))
      data$df6271 <- hot_to_r(input$table6271Item1)
  })

  observe({
    if(!is.null(input$table6271Item1))
      data$df6271_2 <- hot_to_r(input$table6271Item1)
  })

  observe({
    if(!is.null(input$table6271Item1))
      data$df6271_3 <- hot_to_r(input$table6271Item1)
  })

  observe({
    if(!is.null(input$table6271_1Item1)) {
      new_df <- hot_to_r(input$table6271_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6271.1 <- validation_result$df
      } else {
        data$df6271.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6271_2Item1)) {
      new_df <- hot_to_r(input$table6271_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6271.2 <- validation_result$df
      } else {
        data$df6271.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6272Item1))
      data$df6272 <- hot_to_r(input$table6272Item1)
  })

  observe({
    if(!is.null(input$table6272Item1))
      data$df6272_2 <- hot_to_r(input$table6272Item1)
  })

  observe({
    if(!is.null(input$table6272Item1))
      data$df6272_3 <- hot_to_r(input$table6272Item1)
  })

  observe({
    if(!is.null(input$table6272_1Item1)) {
      new_df <- hot_to_r(input$table6272_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6272.1 <- validation_result$df
      } else {
        data$df6272.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6272_2Item1)) {
      new_df <- hot_to_r(input$table6272_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6272.2 <- validation_result$df
      } else {
        data$df6272.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6273Item1)) {
      new_df <- hot_to_r(input$table6273Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6273 <- validation_result$df
      } else {
        data$df6273 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6274Item1)) {
      new_df <- hot_to_r(input$table6274Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6274 <- validation_result$df
      } else {
        data$df6274 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6275Item1))
      data$df6275 <- hot_to_r(input$table6275Item1)
  })

  observe({
    if(!is.null(input$table6275Item1))
      data$df6275_2 <- hot_to_r(input$table6275Item1)
  })

  observe({
    if(!is.null(input$table6275Item1))
      data$df6275_3 <- hot_to_r(input$table6275Item1)
  })

  observe({
    if(!is.null(input$table6275_1Item1)) {
      new_df <- hot_to_r(input$table6275_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6275.1 <- validation_result$df
      } else {
        data$df6275.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6275_2Item1)) {
      new_df <- hot_to_r(input$table6275_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6275.2 <- validation_result$df
      } else {
        data$df6275.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6276Item1))
      data$df6276 <- hot_to_r(input$table6276Item1)
  })

  observe({
    if(!is.null(input$table6276Item1))
      data$df6276_2 <- hot_to_r(input$table6276Item1)
  })

  observe({
    if(!is.null(input$table6276Item1))
      data$df6276_3 <- hot_to_r(input$table6276Item1)
  })

  observe({
    if(!is.null(input$table6276_1Item1)) {
      new_df <- hot_to_r(input$table6276_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6276.1 <- validation_result$df
      } else {
        data$df6276.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6276_2Item1)) {
      new_df <- hot_to_r(input$table6276_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6276.2 <- validation_result$df
      } else {
        data$df6276.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6277Item1))
      data$df6277 <- hot_to_r(input$table6277Item1)
  })

  observe({
    if(!is.null(input$table6277Item1))
      data$df6277_2 <- hot_to_r(input$table6277Item1)
  })

  observe({
    if(!is.null(input$table6277Item1))
      data$df6277_3 <- hot_to_r(input$table6277Item1)
  })

  observe({
    if(!is.null(input$table6277_1Item1)) {
      new_df <- hot_to_r(input$table6277_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6277.1 <- validation_result$df
      } else {
        data$df6277.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6277_2Item1)) {
      new_df <- hot_to_r(input$table6277_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6277.2 <- validation_result$df
      } else {
        data$df6277.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6280Item1))
      data$df6280 <- hot_to_r(input$table6280Item1)
  })

  observe({
    if(!is.null(input$table6280Item1))
      data$df6280_3 <- hot_to_r(input$table6280Item1)
  })

  observe({
    if(!is.null(input$table6281Item1))
      data$df6281 <- hot_to_r(input$table6281Item1)
  })

  observe({
    if(!is.null(input$table6281Item1))
      data$df6281_2 <- hot_to_r(input$table6281Item1)
  })

  observe({
    if(!is.null(input$table6281Item1))
      data$df6281_3 <- hot_to_r(input$table6281Item1)
  })

  observe({
    if(!is.null(input$table6281_1Item1)) {
      new_df <- hot_to_r(input$table6281_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6281.1 <- validation_result$df
      } else {
        data$df6281.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6281_2Item1)) {
      new_df <- hot_to_r(input$table6281_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6281.2 <- validation_result$df
      } else {
        data$df6281.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6282Item1)) {
      new_df <- hot_to_r(input$table6282Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6282 <- validation_result$df
      } else {
        data$df6282 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6290Item1))
      data$df6290 <- hot_to_r(input$table6290Item1)
  })

  observe({
    if(!is.null(input$table6290Item1))
      data$df6290_3 <- hot_to_r(input$table6290Item1)
  })

  observe({
    if(!is.null(input$table6291Item1)) {
      new_df <- hot_to_r(input$table6291Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6291 <- validation_result$df
      } else {
        data$df6291 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6292Item1))
      data$df6292 <- hot_to_r(input$table6292Item1)
  })

  observe({
    if(!is.null(input$table6292Item1))
      data$df6292_2 <- hot_to_r(input$table6292Item1)
  })

  observe({
    if(!is.null(input$table6292Item1))
      data$df6292_3 <- hot_to_r(input$table6292Item1)
  })

  observe({
    if(!is.null(input$table6292_1Item1)) {
      new_df <- hot_to_r(input$table6292_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6292.1 <- validation_result$df
      } else {
        data$df6292.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6292_2Item1)) {
      new_df <- hot_to_r(input$table6292_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6292.2 <- validation_result$df
      } else {
        data$df6292.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6293Item1))
      data$df6293 <- hot_to_r(input$table6293Item1)
  })

  observe({
    if(!is.null(input$table6293Item1))
      data$df6293_2 <- hot_to_r(input$table6293Item1)
  })

  observe({
    if(!is.null(input$table6293Item1))
      data$df6293_3 <- hot_to_r(input$table6293Item1)
  })

  observe({
    if(!is.null(input$table6293_1Item1)) {
      new_df <- hot_to_r(input$table6293_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6293.1 <- validation_result$df
      } else {
        data$df6293.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6293_2Item1)) {
      new_df <- hot_to_r(input$table6293_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6293.2 <- validation_result$df
      } else {
        data$df6293.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6294Item1))
      data$df6294 <- hot_to_r(input$table6294Item1)
  })

  observe({
    if(!is.null(input$table6294Item1))
      data$df6294_2 <- hot_to_r(input$table6294Item1)
  })

  observe({
    if(!is.null(input$table6294Item1))
      data$df6294_3 <- hot_to_r(input$table6294Item1)
  })

  observe({
    if(!is.null(input$table6294_1Item1)) {
      new_df <- hot_to_r(input$table6294_1Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6294.1 <- validation_result$df
      } else {
        data$df6294.1 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6294_2Item1)) {
      new_df <- hot_to_r(input$table6294_2Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6294.2 <- validation_result$df
      } else {
        data$df6294.2 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6300Item1))
      data$df6300 <- hot_to_r(input$table6300Item1)
  })

  observe({
    if(!is.null(input$table6310Item1)) {
      new_df <- hot_to_r(input$table6310Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6310 <- validation_result$df
      } else {
        data$df6310 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6320Item1)) {
      new_df <- hot_to_r(input$table6320Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6320 <- validation_result$df
      } else {
        data$df6320 <- new_df
      }
    }
  })

  observe({
    if(!is.null(input$table6330Item1)) {
      new_df <- hot_to_r(input$table6330Item1)
      validation_result <- validate_operation_dates(new_df)
      if (!validation_result$valid) {
        data$df6330 <- validation_result$df
      } else {
        data$df6330 <- new_df
      }
    }
  })

#****************************************

#***********************************

#ОСВ: 6000

observeEvent(input$dates6000, {
    start <- ymd(input$dates6000[[1]])
    end <- ymd(input$dates6000[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6000",
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6000[[1]]
      r$end <- input$dates6000[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6000",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6000))) {
      from=as.Date(input$dates6000[1L])
      to=as.Date(input$dates6000[2L])
      if (from>to) to = from
      selectdates6010_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6010_1 <- data$df6010[as.Date(data$df6010$`Дата операции`) %in% selectdates6010_1, ]
    } else {
      selectdates6010_2 <- unique(as.Date(data$df6010$`Дата операции`))
      data$df6010_1 <- data$df6010[data$df6010$`Дата операции` %in% selectdates6010_2, ]
    }
  })

  observe({
    if(!is.null(input$table6010Item1) && !any(is.na(input$table6010Item1)))
      data$df6010_1<- hot_to_r(input$table6010Item1)
  })

observe({
    if (!any(is.na(input$dates6000))) {
      from=as.Date(input$dates6000[1L])
      to=as.Date(input$dates6000[2L])
      if (from>to) to = from
      selectdates6020_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6020_1 <- data$df6020[as.Date(data$df6020$`Дата операции`) %in% selectdates6020_1, ]
    } else {
      selectdates6020_2 <- unique(as.Date(data$df6020$`Дата операции`))
      data$df6020_1 <- data$df6020[data$df6020$`Дата операции` %in% selectdates6020_2, ]
    }
  })

  observe({
    if(!is.null(input$table6020Item1) && !any(is.na(input$table6020Item1)))
      data$df6020_1 <- hot_to_r(input$table6020Item1)
  })

observe({
    if (!any(is.na(input$dates6000))) {
      from=as.Date(input$dates6000[1L])
      to=as.Date(input$dates6000[2L])
      if (from>to) to = from
      selectdates6030_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6030_1 <- data$df6030[as.Date(data$df6030$`Дата операции`) %in% selectdates6030_1, ]
    } else {
      selectdates6030_2 <- unique(as.Date(data$df6030$`Дата операции`))
      data$df6030_1 <- data$df6030[data$df6030$`Дата операции` %in% selectdates6030_2, ]
    }
  })

  observe({
    if(!is.null(input$table6030Item1) && !any(is.na(input$table6030Item1)))
      data$df6030_1 <- hot_to_r(input$table6030Item1)
  })

observe({
  data$df6000[1, 2:5] <- data$df6010_1[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by=`Номер первичного документа`][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
})

observe({
  data$df6000[2, 2:5] <- data$df6020_1[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by=`Номер документа, по которому состоялся возврат проданной продукции`][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
})

observe({
  data$df6000[3, 2:5] <- data$df6030_1[, list(
    `Сальдо начальное` = sum(`Сальдо начальное`[1L], na.rm = TRUE),
    Кредит = sum(`Кредит`, na.rm = TRUE),
    Дебет = sum(`Дебет`, na.rm = TRUE),
    `Сальдо конечное` = sum(`Сальдо конечное`[.N], na.rm = TRUE)
  ), by=`Номер первичного документа`][, .(
    `Сальдо начальное` = sum(`Сальдо начальное`),
    Дебет = sum(Дебет),
    Кредит = sum(Кредит),
    `Сальдо конечное` = sum(`Сальдо конечное`)
  )]
})

  output$nested_ui6000 <- renderUI({!any(is.na(input$dates6000))})

  output$table6000Item1 <- renderRHandsontable({
    rhandsontable(data$df6000, colWidths = 150, height = 100, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400)
  })

  output$download_df6000 <- downloadHandler(
    filename = function() { "df6000.xlsx" },
    content = function(file) {
      write.xlsx(data$df6000, file)
  })

#***********************************

#6010

observeEvent(input$dates6010, {
    start <- ymd(input$dates6010[[1]])
    end <- ymd(input$dates6010[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6010", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6010[[1]]
      r$end <- input$dates6010[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6010",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6010Item1)) {
    data$df6010 <- hot_to_r(input$table6010Item1) 

    if (!any(is.na(input$dates6010)) && input$choices6010 == "Выбор по дате операции") {
     	from=as.Date(input$dates6010[1L])
      	to=as.Date(input$dates6010[2L])
      	if (from>to) to = from
      	selectdates6010_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6010_2 <- data$df6010[as.Date(data$df6010$"Дата операции") %in% selectdates6010_1, ]
    } else if (!is.null(input$text) && input$choices6010 == "Выбор по номеру первичного документа") {
      	data$df6010_2 <- data$df6010[data$df6010$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6010 == "Выбор по статье дохода") {
      	data$df6010_2 <- data$df6010[data$df6010$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6010) && !any(is.na(input$dates6010)) && !is.null(input$text) && input$choices6010 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6010[1L])
      	to=as.Date(input$dates6010[2L])
      	if (from>to) to = from
      	selectdates6010_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6010_2 <- data$df6010[as.Date(data$df6010$"Дата операции") %in% selectdates6010_2 & data$df6010$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6010) && !any(is.na(input$dates6010)) && !is.null(input$text) && input$choices6010 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6010[1L])
      	to=as.Date(input$dates6010[2L])
      	if (from>to) to = from
      	selectdates6010_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6010_2 <- data$df6010[as.Date(data$df6010$"Дата операции") %in% selectdates6010_3 & data$df6010$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6010_4 <- unique(data$df6010$"Дата операции")
        data$df6010_2 <- data$df6010[data$df6010$"Дата операции" %in% selectdates6010_4, ]
    }
}
})  

  output$table6010Item1 <- renderRHandsontable({
    
   data$df6010[, `Сальдо конечное` := data$df6010[[6]] + data$df6010[[7]] - data$df6010[[8]]]

    rhandsontable(data$df6010, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6010 <- renderUI({
    if (input$choices6010 == "Выбор по дате операции") {
      	dateRangeInput("dates6010", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6010 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6010 == "Выбор по статье дохода") {
      	textInput("text", "Укажите статью дохода:")
    } else if (input$choices6010 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6010", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6010 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6010", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите статью дохода:")
      )
    }
  })

  output$table6010Item2 <- renderRHandsontable({
    rhandsontable(data$df6010_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6010 <- downloadHandler(
    filename = function() { "df6010.xlsx" },
    content = function(file) {
      write.xlsx(data$df6010, file)
  })

  output$download_df6010_2 <- downloadHandler(
    filename = function() { "df6010_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6010_2, file)
  })

observeEvent(input$dates6020, {
    start <- ymd(input$dates6020[[1]])
    end <- ymd(input$dates6020[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6020", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6020[[1]]
      r$end <- input$dates6020[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6020",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6020Item1)) {
    data$df6020 <- hot_to_r(input$table6020Item1) 

    if (!any(is.na(input$dates6020)) && input$choices6020 == "Выбор по дате операции") {
     	from=as.Date(input$dates6020[1L])
      	to=as.Date(input$dates6020[2L])
      	if (from>to) to = from
      	selectdates6020_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6020_2 <- data$df6020[as.Date(data$df6020$"Дата операции") %in% selectdates6020_1, ]
    } else if (!is.null(input$text) && input$choices6020 == "Выбор по номеру документа, по которому состоялся возврат проданной продукции") {
      	data$df6020_2 <- data$df6020[data$df6020$"Номер документа, по которому состоялся возврат проданной продукции" == input$text, ]
    } else if (!is.null(input$text) && input$choices6020 == "Выбор по статье возврата") {
      	data$df6020_2 <- data$df6020[data$df6020$"Счет №, с которого списывался возвращенная продукция" == input$text, ]
    } else if (!is.null(input$dates6020) && !any(is.na(input$dates6020)) && !is.null(input$text) && input$choices6020 == "Выбор по дате операции и номеру документа, по которому состоялся возврат проданной продукции") {
     	from=as.Date(input$dates6020[1L])
      	to=as.Date(input$dates6020[2L])
      	if (from>to) to = from
      	selectdates6020_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6020_2 <- data$df6020[as.Date(data$df6020$"Дата операции") %in% selectdates6020_2 & data$df6020$"Номер документа, по которому состоялся возврат проданной продукции" == input$text, ]
    } else if (!is.null(input$dates6020) && !any(is.na(input$dates6020)) && !is.null(input$text) && input$choices6020 == "Выбор по дате операции и статье возврата") {
     	from=as.Date(input$dates6020[1L])
      	to=as.Date(input$dates6020[2L])
      	if (from>to) to = from
      	selectdates6020_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6020_2 <- data$df6020[as.Date(data$df6020$"Дата операции") %in% selectdates6020_3 & data$df6020$"Счет №, с которого списывался возвращенная продукция" == input$text, ]
    } else {
        selectdates6020_4 <- unique(data$df6020$"Дата операции")
        data$df6020_2 <- data$df6020[data$df6020$"Дата операции" %in% selectdates6020_4, ]
    }
}
})  

  output$table6020Item1 <- renderRHandsontable({
    
   data$df6020[, `Сальдо конечное` := data$df6020[[6]] + data$df6020[[7]] - data$df6020[[8]]]

    rhandsontable(data$df6020, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6020 <- renderUI({
    if (input$choices6020 == "Выбор по дате операции") {
      	dateRangeInput("dates6020", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6020 == "Выбор по номеру документа, по которому состоялся возврат проданной продукции") {
      	textInput("text", "Укажите номер документа, по которому состоялся возврат проданной продукции:")
    } else if (input$choices6020 == "Выбор по статье возврата") {
      	textInput("text", "Укажите статью возврата:")
    } else if (input$choices6020 == "Выбор по дате операции и номеру документа, по которому состоялся возврат проданной продукции") {
      fluidRow(
       	dateRangeInput("dates6020", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер документа, по которому состоялся возврат проданной продукции:")
      )
    } else if (input$choices6020 == "Выбор по дате операции и статье возврата") {
      fluidRow(
       	dateRangeInput("dates6020", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите статью возврата:")
      )
    }
  })

  output$table6020Item2 <- renderRHandsontable({
    rhandsontable(data$df6020_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6020 <- downloadHandler(
    filename = function() { "df6020.xlsx" },
    content = function(file) {
      write.xlsx(data$df6020, file)
  })

  output$download_df6020_2 <- downloadHandler(
    filename = function() { "df6020_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6020_2, file)
  })

observeEvent(input$dates6030, {
    start <- ymd(input$dates6030[[1]])
    end <- ymd(input$dates6030[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6030", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6030[[1]]
      r$end <- input$dates6030[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6030",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6030Item1)) {
    data$df6030 <- hot_to_r(input$table6030Item1) 

    if (!any(is.na(input$dates6030)) && input$choices6030 == "Выбор по дате операции") {
     	from=as.Date(input$dates6030[1L])
      	to=as.Date(input$dates6030[2L])
      	if (from>to) to = from
      	selectdates6030_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6030_2 <- data$df6030[as.Date(data$df6030$"Дата операции") %in% selectdates6030_1, ]
    } else if (!is.null(input$text) && input$choices6030 == "Выбор по номеру первичного документа") {
      	data$df6030_2 <- data$df6030[data$df6030$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6030 == "Выбор по статье дохода") {
      	data$df6030_2 <- data$df6030[data$df6030$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6030) && !any(is.na(input$dates6030)) && !is.null(input$text) && input$choices6030 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6030[1L])
      	to=as.Date(input$dates6030[2L])
      	if (from>to) to = from
      	selectdates6030_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6030_2 <- data$df6030[as.Date(data$df6030$"Дата операции") %in% selectdates6030_2 & data$df6030$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6030) && !any(is.na(input$dates6030)) && !is.null(input$text) && input$choices6030 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6030[1L])
      	to=as.Date(input$dates6030[2L])
      	if (from>to) to = from
      	selectdates6030_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6030_2 <- data$df6030[as.Date(data$df6030$"Дата операции") %in% selectdates6030_3 & data$df6030$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6030_4 <- unique(data$df6030$"Дата операции")
        data$df6030_2 <- data$df6030[data$df6030$"Дата операции" %in% selectdates6030_4, ]
    }
}
})  

  output$table6030Item1 <- renderRHandsontable({
    
   data$df6030[, `Сальдо конечное` := data$df6030[[6]] + data$df6030[[7]] - data$df6030[[8]]]

    rhandsontable(data$df6030, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6030 <- renderUI({
    if (input$choices6030 == "Выбор по дате операции") {
      	dateRangeInput("dates6030", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6030 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6030 == "Выбор по статье дохода") {
      	textInput("text", "Укажите статью дохода:")
    } else if (input$choices6030 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6030", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6030 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6030", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите статью дохода:")
      )
    }
  })

  output$table6030Item2 <- renderRHandsontable({
    rhandsontable(data$df6030_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6030 <- downloadHandler(
    filename = function() { "df6030.xlsx" },
    content = function(file) {
      write.xlsx(data$df6030, file)
  })

  output$download_df6030_2 <- downloadHandler(
    filename = function() { "df6030_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6030_2, file)
  })

#*****************************************

#ОСВ: 6100

observeEvent(input$dates6100, {
    start <- ymd(input$dates6100[[1]])
    end <- ymd(input$dates6100[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6100", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6100[[1]]
      r$end <- input$dates6100[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6100",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

#************************************

#ОСВ: 6100 to compile 6110

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6110.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6110.1_4 <- data$df6110.1[as.Date(data$df6110.1$`Дата операции`) %in% selectdates6110.1_7, ]
    } else {
      selectdates6110.1_8 <- unique(as.Date(data$df6110.1$`Дата операции`))
      data$df6110.1_4 <- data$df6110.1[data$df6110.1$`Дата операции` %in% selectdates6110.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6110.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6110.2_4 <- data$df6110.2[as.Date(data$df6110.2$`Дата операции`) %in% selectdates6110.2_7, ]
    } else {
      selectdates6110.2_8 <- unique(as.Date(data$df6110.2$`Дата операции`))
      data$df6110.2_4 <- data$df6110.2[data$df6110.2$`Дата операции` %in% selectdates6110.2_8, ]
    }
  })

observe({
  data$df6110_3[1, 2:5] <- data$df6110.1_4[, list(
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
  data$df6110_3[2, 2:5] <- data$df6110.2_4[, list(
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

observe({ data$df6100[1, 2:5] <- data$df6110_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6100 to compile 6120

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6120.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.1_4 <- data$df6120.1[as.Date(data$df6120.1$`Дата операции`) %in% selectdates6120.1_7, ]
    } else {
      selectdates6120.1_8 <- unique(as.Date(data$df6120.1$`Дата операции`))
      data$df6120.1_4 <- data$df6120.1[data$df6120.1$`Дата операции` %in% selectdates6120.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6120.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.2_4 <- data$df6120.2[as.Date(data$df6120.2$`Дата операции`) %in% selectdates6120.2_7, ]
    } else {
      selectdates6120.2_8 <- unique(as.Date(data$df6120.2$`Дата операции`))
      data$df6120.2_4 <- data$df6120.2[data$df6120.2$`Дата операции` %in% selectdates6120.2_8, ]
    }
  })

observe({
  data$df6120_3[1, 2:5] <- data$df6120.1_4[, list(
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
  data$df6120_3[2, 2:5] <- data$df6120.2_4[, list(
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

observe({ data$df6100[2, 2:5] <- data$df6120_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6100 to compile 6130

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6130.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6130.1_4 <- data$df6130.1[as.Date(data$df6130.1$`Дата операции`) %in% selectdates6130.1_7, ]
    } else {
      selectdates6130.1_8 <- unique(as.Date(data$df6130.1$`Дата операции`))
      data$df6130.1_4 <- data$df6130.1[data$df6130.1$`Дата операции` %in% selectdates6130.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6130.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6130.2_4 <- data$df6130.2[as.Date(data$df6130.2$`Дата операции`) %in% selectdates6130.2_7, ]
    } else {
      selectdates6130.2_8 <- unique(as.Date(data$df6130.2$`Дата операции`))
      data$df6130.2_4 <- data$df6130.2[data$df6130.2$`Дата операции` %in% selectdates6130.2_8, ]
    }
  })

observe({
  data$df6130_3[1, 2:5] <- data$df6130.1_4[, list(
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
  data$df6130_3[2, 2:5] <- data$df6130.2_4[, list(
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

observe({ data$df6100[3, 2:5] <- data$df6130_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6100 to compile 6140

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6141.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.1_4 <- data$df6141.1[as.Date(data$df6141.1$`Дата операции`) %in% selectdates6141.1_9, ]
    } else {
      selectdates6141.1_10 <- unique(as.Date(data$df6141.1$`Дата операции`))
      data$df6141.1_4 <- data$df6141.1[data$df6141.1$`Дата операции` %in% selectdates6141.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6141.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.2_4 <- data$df6141.2[as.Date(data$df6141.2$`Дата операции`) %in% selectdates6141.2_9, ]
    } else {
      selectdates6141.2_10 <- unique(as.Date(data$df6141.2$`Дата операции`))
      data$df6141.2_4 <- data$df6141.2[data$df6141.2$`Дата операции` %in% selectdates6141.2_10, ]
    }
  })

observe({
  data$df6141_3[1, 2:5] <- data$df6141.1_4[, list(
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
  data$df6141_3[2, 2:5] <- data$df6141.2_4[, list(
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

observe({ data$df6140_3[1, 2:5] <- data$df6141_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6142.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.1_4 <- data$df6142.1[as.Date(data$df6142.1$`Дата операции`) %in% selectdates6142.1_9, ]
    } else {
      selectdates6142.1_10 <- unique(as.Date(data$df6142.1$`Дата операции`))
      data$df6142.1_4 <- data$df6142.1[data$df6142.1$`Дата операции` %in% selectdates6142.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6142.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.2_4 <- data$df6142.2[as.Date(data$df6142.2$`Дата операции`) %in% selectdates6142.2_9, ]
    } else {
      selectdates6142.2_10 <- unique(as.Date(data$df6142.2$`Дата операции`))
      data$df6142.2_4 <- data$df6142.2[data$df6142.2$`Дата операции` %in% selectdates6142.2_10, ]
    }
  })

observe({
  data$df6142_3[1, 2:5] <- data$df6142.1_4[, list(
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
  data$df6142_3[2, 2:5] <- data$df6142.2_4[, list(
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

observe({ data$df6140_3[2, 2:5] <- data$df6142_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6143_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6143_3 <- data$df6143[as.Date(data$df6143$`Дата операции`) %in% selectdates6143_7, ]
    } else {
      selectdates6143_8 <- unique(as.Date(data$df6143$`Дата операции`))
      data$df6143_3 <- data$df6143[data$df6143$`Дата операции` %in% selectdates6143_8, ]
    }
  })

observe({
  data$df6140_3[3, 2:5] <- data$df6143_3[, list(
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

observe({ data$df6100[4, 2:5] <- data$df6140_3[, .SD[1:3, lapply(.SD, sum)], .SDcols = 2:5] })

#*************************************************

#ОСВ: 6100 to compile 6150 thru 6170

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6150_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6150_3 <- data$df6150[as.Date(data$df6150$`Дата операции`) %in% selectdates6150_5, ]
    } else {
      selectdates6150_6 <- unique(as.Date(data$df6150$`Дата операции`))
      data$df6150_3 <- data$df6150[data$df6150$`Дата операции` %in% selectdates6150_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6160_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6160_3 <- data$df6160[as.Date(data$df6160$`Дата операции`) %in% selectdates6160_5, ]
    } else {
      selectdates6160_6 <- unique(as.Date(data$df6160$`Дата операции`))
      data$df6160_3 <- data$df6160[data$df6160$`Дата операции` %in% selectdates6160_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6171_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6171_3 <- data$df6171[as.Date(data$df6171$`Дата операции`) %in% selectdates6171_7, ]
    } else {
      selectdates6171_8 <- unique(as.Date(data$df6171$`Дата операции`))
      data$df6171_3 <- data$df6171[data$df6171$`Дата операции` %in% selectdates6171_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6100))) {
      from=as.Date(input$dates6100[1L])
      to=as.Date(input$dates6100[2L])
      if (from>to) to = from
      selectdates6172_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6172_3 <- data$df6172[as.Date(data$df6172$`Дата операции`) %in% selectdates6172_7, ]
    } else {
      selectdates6172_8 <- unique(as.Date(data$df6172$`Дата операции`))
      data$df6172_3 <- data$df6172[data$df6172$`Дата операции` %in% selectdates6172_8, ]
    }
  })

observe({
  data$df6100[5, 2:5] <- data$df6150_3[, list(
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
  data$df6100[6, 2:5] <- data$df6160_3[, list(
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
  data$df6170_2[1, 2:5] <- data$df6171_3[, list(
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
  data$df6170_2[2, 2:5] <- data$df6172_3[, list(
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

observe({ data$df6100[7, 2:5] <- data$df6170_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6100[8, 2:5] <- data$df6100[, .SD[1:7, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6100 <- renderUI({!any(is.na(input$dates6100))})

  output$table6100Item1 <- renderRHandsontable({
    rhandsontable(data$df6100, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 7) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6100 <- downloadHandler(
    filename = function() { "df6100.xlsx" },
    content = function(file) {
      write.xlsx(data$df6100, file)
  })

#****************************************

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
     data$df6110.1_3 <- data$df6110.1[as.Date(data$df6110.1$`Дата операции`) %in% selectdates6110.1_5, ]
    } else {
      selectdates6110.1_6 <- unique(as.Date(data$df6110.1$`Дата операции`))
      data$df6110.1_3 <- data$df6110.1[data$df6110.1$`Дата операции` %in% selectdates6110.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6110))) {
      from=as.Date(input$dates6110[1L])
      to=as.Date(input$dates6110[2L])
      if (from>to) to = from
      selectdates6110.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6110.2_3 <- data$df6110.2[as.Date(data$df6110.2$`Дата операции`) %in% selectdates6110.2_5, ]
    } else {
      selectdates6110.2_6 <- unique(as.Date(data$df6110.2$`Дата операции`))
      data$df6110.2_3 <- data$df6110.2[data$df6110.2$`Дата операции` %in% selectdates6110.2_6, ]
    }
  })

observe({
  data$df6110[1, 2:5] <- data$df6110.1_3[, list(
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
  data$df6110[2, 2:5] <- data$df6110.2_3[, list(
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

observe({ if (!is.null(input$table6110_1Item1)) {
    data$df6110.1 <- hot_to_r(input$table6110_1Item1) 

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

  output$table6110_1Item1 <- renderRHandsontable({
    
   data$df6110.1[, `Сальдо конечное` := data$df6110.1[[6]] + data$df6110.1[[7]] - data$df6110.1[[8]]]

    rhandsontable(data$df6110.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
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
    rhandsontable(data$df6110.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
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

observe({ if (!is.null(input$table6110_2Item1)) {
    data$df6110.2 <- hot_to_r(input$table6110_2Item1) 

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

  output$table6110_2Item1 <- renderRHandsontable({
    
   data$df6110.2[, `Сальдо конечное` := data$df6110.2[[6]] + data$df6110.2[[7]] - data$df6110.2[[8]]]

    rhandsontable(data$df6110.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
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
    rhandsontable(data$df6110.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
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

#****************************************

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
     data$df6120.1_3 <- data$df6120.1[as.Date(data$df6120.1$`Дата операции`) %in% selectdates6120.1_5, ]
    } else {
      selectdates6120.1_6 <- unique(as.Date(data$df6120.1$`Дата операции`))
      data$df6120.1_3 <- data$df6120.1[data$df6120.1$`Дата операции` %in% selectdates6120.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6120))) {
      from=as.Date(input$dates6120[1L])
      to=as.Date(input$dates6120[2L])
      if (from>to) to = from
      selectdates6120.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6120.2_3 <- data$df6120.2[as.Date(data$df6120.2$`Дата операции`) %in% selectdates6120.2_5, ]
    } else {
      selectdates6120.2_6 <- unique(as.Date(data$df6120.2$`Дата операции`))
      data$df6120.2_3 <- data$df6120.2[data$df6120.2$`Дата операции` %in% selectdates6120.2_6, ]
    }
  })

observe({
  data$df6120[1, 2:5] <- data$df6120.1_3[, list(
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
  data$df6120[2, 2:5] <- data$df6120.2_3[, list(
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

observe({ if (!is.null(input$table6120_1Item1)) {
    data$df6120.1 <- hot_to_r(input$table6120_1Item1) 

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

  output$table6120_1Item1 <- renderRHandsontable({
    
   data$df6120.1[, `Сальдо конечное` := data$df6120.1[[7]] + data$df6120.1[[8]] - data$df6120.1[[9]]]

    rhandsontable(data$df6120.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
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
    rhandsontable(data$df6120.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
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

observe({ if (!is.null(input$table6120_2Item1)) {
    data$df6120.2 <- hot_to_r(input$table6120_2Item1) 

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

  output$table6120_2Item1 <- renderRHandsontable({
    
   data$df6120.2[, `Сальдо конечное` := data$df6120.2[[7]] + data$df6120.2[[8]] - data$df6120.2[[9]]]

    rhandsontable(data$df6120.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
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
    rhandsontable(data$df6120.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
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

#****************************************

#ОСВ: 6130

observeEvent(input$dates6130, {
    start <- ymd(input$dates6130[[1]])
    end <- ymd(input$dates6130[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6130", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6130[[1]]
      r$end <- input$dates6130[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6130",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6130))) {
      from=as.Date(input$dates6130[1L])
      to=as.Date(input$dates6130[2L])
      if (from>to) to = from
      selectdates6130.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6130.1_3 <- data$df6130.1[as.Date(data$df6130.1$`Дата операции`) %in% selectdates6130.1_5, ]
    } else {
      selectdates6130.1_6 <- unique(as.Date(data$df6130.1$`Дата операции`))
      data$df6130.1_3 <- data$df6130.1[data$df6130.1$`Дата операции` %in% selectdates6130.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6130))) {
      from=as.Date(input$dates6130[1L])
      to=as.Date(input$dates6130[2L])
      if (from>to) to = from
      selectdates6130.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6130.2_3 <- data$df6130.2[as.Date(data$df6130.2$`Дата операции`) %in% selectdates6130.2_5, ]
    } else {
      selectdates6130.2_6 <- unique(as.Date(data$df6130.2$`Дата операции`))
      data$df6130.2_3 <- data$df6130.2[data$df6130.2$`Дата операции` %in% selectdates6130.2_6, ]
    }
  })

observe({
  data$df6130[1, 2:5] <- data$df6130.1_3[, list(
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
  data$df6130[2, 2:5] <- data$df6130.2_3[, list(
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

observe({ data$df6130[3, 2:5] <- data$df6130[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6130 <- renderUI({!any(is.na(input$dates6130))})

  output$table6130Item1 <- renderRHandsontable({
    rhandsontable(data$df6130, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 550) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6130 <- downloadHandler(
    filename = function() { "df6130.xlsx" },
    content = function(file) {
      write.xlsx(data$df6130, file)
  })

#**************************************

#6130.1

observeEvent(input$dates6130.1, {
    start <- ymd(input$dates6130.1[[1]])
    end <- ymd(input$dates6130.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6130.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6130.1[[1]]
      r$end <- input$dates6130.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6130.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6130_1Item1)) {
    data$df6130.1 <- hot_to_r(input$table6130_1Item1) 

    if (!any(is.na(input$dates6130.1)) && input$choices6130.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6130.1[1L])
      	to=as.Date(input$dates6130.1[2L])
      	if (from>to) to = from
      	selectdates6130.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.1_2 <- data$df6130.1[as.Date(data$df6130.1$"Дата операции") %in% selectdates6130.1_1, ]
    } else if (!is.null(input$text) && input$choices6130.1 == "Выбор по номеру первичного документа") {
      	data$df6130.1_2 <- data$df6130.1[data$df6130.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6130.1 == "Выбор по статье дохода") {
      	data$df6130.1_2 <- data$df6130.1[data$df6130.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6130.1) && !any(is.na(input$dates6130.1)) && !is.null(input$text) && input$choices6130.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6130.1[1L])
      	to=as.Date(input$dates6130.1[2L])
      	if (from>to) to = from
      	selectdates6130.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.1_2 <- data$df6130.1[as.Date(data$df6130.1$"Дата операции") %in% selectdates6130.1_2 & data$df6130.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6130.1) && !any(is.na(input$dates6130.1)) && !is.null(input$text) && input$choices6130.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6130.1[1L])
      	to=as.Date(input$dates6130.1[2L])
      	if (from>to) to = from
      	selectdates6130.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.1_2 <- data$df6130.1[as.Date(data$df6130.1$"Дата операции") %in% selectdates6130.1_3 & data$df6130.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6130.1_4 <- unique(data$df6130.1$"Дата операции")
        data$df6130.1_2 <- data$df6130.1[data$df6130.1$"Дата операции" %in% selectdates6130.1_4, ]
    }
}
})

  output$table6130_1Item1 <- renderRHandsontable({
    
   data$df6130.1[, `Сальдо конечное` := data$df6130.1[[7]] + data$df6130.1[[8]] - data$df6130.1[[9]]]

    rhandsontable(data$df6130.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6130.1 <- renderUI({
    if (input$choices6130.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6130.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6130.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6130.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6130.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6130.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6130.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6130.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6130.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6130.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6130.1 <- downloadHandler(
    filename = function() { "df6130.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6130.1, file)
  })

  output$download_df6130.1_2 <- downloadHandler(
    filename = function() { "df6130.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6130.1_2, file)
  })

#****************************************

#6130.2

observeEvent(input$dates6130.2, {
    start <- ymd(input$dates6130.2[[1]])
    end <- ymd(input$dates6130.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6130.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6130.2[[1]]
      r$end <- input$dates6130.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6130.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6130_2Item1)) {
    data$df6130.2 <- hot_to_r(input$table6130_2Item1) 

    if (!any(is.na(input$dates6130.2)) && input$choices6130.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6130.2[1L])
      	to=as.Date(input$dates6130.2[2L])
      	if (from>to) to = from
      	selectdates6130.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.2_2 <- data$df6130.2[as.Date(data$df6130.2$"Дата операции") %in% selectdates6130.2_1, ]
    } else if (!is.null(input$text) && input$choices6130.2 == "Выбор по номеру первичного документа") {
      	data$df6130.2_2 <- data$df6130.2[data$df6130.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6130.2 == "Выбор по статье дохода") {
      	data$df6130.2_2 <- data$df6130.2[data$df6130.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6130.2) && !any(is.na(input$dates6130.2)) && !is.null(input$text) && input$choices6130.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6130.2[1L])
      	to=as.Date(input$dates6130.2[2L])
      	if (from>to) to = from
      	selectdates6130.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.2_2 <- data$df6130.2[as.Date(data$df6130.2$"Дата операции") %in% selectdates6130.2_2 & data$df6130.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6130.2) && !any(is.na(input$dates6130.2)) && !is.null(input$text) && input$choices6130.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6130.2[1L])
      	to=as.Date(input$dates6130.2[2L])
      	if (from>to) to = from
      	selectdates6130.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6130.2_2 <- data$df6130.2[as.Date(data$df6130.2$"Дата операции") %in% selectdates6130.2_3 & data$df6130.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6130.2_4 <- unique(data$df6130.2$"Дата операции")
        data$df6130.2_2 <- data$df6130.2[data$df6130.2$"Дата операции" %in% selectdates6130.2_4, ]
    }
}
})

  output$table6130_2Item1 <- renderRHandsontable({
    
   data$df6130.2[, `Сальдо конечное` := data$df6130.2[[7]] + data$df6130.2[[8]] - data$df6130.2[[9]]]

    rhandsontable(data$df6130.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6130.2 <- renderUI({
    if (input$choices6130.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6130.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6130.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6130.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6130.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6130.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6130.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6130.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6130.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6130.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6130.2 <- downloadHandler(
    filename = function() { "df6130.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6130.2, file)
  })

  output$download_df6130.2_2 <- downloadHandler(
    filename = function() { "df6130.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6130.2_2, file)
  })

#*****************************************

#ОСВ: 6140

observeEvent(input$dates6140, {
    start <- ymd(input$dates6140[[1]])
    end <- ymd(input$dates6140[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6140", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6140[[1]]
      r$end <- input$dates6140[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6140",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6140))) {
      from=as.Date(input$dates6140[1L])
      to=as.Date(input$dates6140[2L])
      if (from>to) to = from
      selectdates6141.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.1_3 <- data$df6141.1[as.Date(data$df6141.1$`Дата операции`) %in% selectdates6141.1_7, ]
    } else {
      selectdates6141.1_8 <- unique(as.Date(data$df6141.1$`Дата операции`))
      data$df6141.1_3 <- data$df6141.1[data$df6141.1$`Дата операции` %in% selectdates6141.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6140))) {
      from=as.Date(input$dates6140[1L])
      to=as.Date(input$dates6140[2L])
      if (from>to) to = from
      selectdates6141.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.2_3 <- data$df6141.2[as.Date(data$df6141.2$`Дата операции`) %in% selectdates6141.2_7, ]
    } else {
      selectdates6141.2_8 <- unique(as.Date(data$df6141.2$`Дата операции`))
      data$df6141.2_3 <- data$df6141.2[data$df6141.2$`Дата операции` %in% selectdates6141.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6140))) {
      from=as.Date(input$dates6140[1L])
      to=as.Date(input$dates6140[2L])
      if (from>to) to = from
      selectdates6142.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.1_3 <- data$df6142.1[as.Date(data$df6142.1$`Дата операции`) %in% selectdates6142.1_7, ]
    } else {
      selectdates6142.1_8 <- unique(as.Date(data$df6142.1$`Дата операции`))
      data$df6142.1_3 <- data$df6142.1[data$df6142.1$`Дата операции` %in% selectdates6142.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6140))) {
      from=as.Date(input$dates6140[1L])
      to=as.Date(input$dates6140[2L])
      if (from>to) to = from
      selectdates6142.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.2_3 <- data$df6142.2[as.Date(data$df6142.2$`Дата операции`) %in% selectdates6142.2_7, ]
    } else {
      selectdates6142.2_8 <- unique(as.Date(data$df6142.2$`Дата операции`))
      data$df6142.2_3 <- data$df6142.2[data$df6142.2$`Дата операции` %in% selectdates6142.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6140))) {
      from=as.Date(input$dates6140[1L])
      to=as.Date(input$dates6140[2L])
      if (from>to) to = from
      selectdates6143_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6143_1 <- data$df6143[as.Date(data$df6143$`Дата операции`) %in% selectdates6143_5, ]
    } else {
      selectdates6143_6 <- unique(as.Date(data$df6143$`Дата операции`))
      data$df6143_1 <- data$df6143[data$df6143$`Дата операции` %in% selectdates6143_6, ]
    }
  })

observe({
  data$df6141_2[1, 2:5] <- data$df6141.1_3[, list(
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
  data$df6141_2[2, 2:5] <- data$df6141.2_3[, list(
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

observe({ data$df6140[1, 2:5] <- data$df6141_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6142_2[1, 2:5] <- data$df6142.1_3[, list(
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
  data$df6142_2[2, 2:5] <- data$df6142.2_3[, list(
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

observe({ data$df6140[2, 2:5] <- data$df6142_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6140[3, 2:5] <- data$df6143_1[, list(
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

observe({ data$df6140[4, 2:5] <- data$df6140[, .SD[1:3, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6140 <- renderUI({!any(is.na(input$dates6140))})

  output$table6140Item1 <- renderRHandsontable({
    rhandsontable(data$df6140, colWidths = 150, height = 150, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 3) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6140 <- downloadHandler(
    filename = function() { "df6140.xlsx" },
    content = function(file) {
      write.xlsx(data$df6140, file)
  })

#*****************************************

#ОСВ: 6141

observeEvent(input$dates6141, {
    start <- ymd(input$dates6141[[1]])
    end <- ymd(input$dates6141[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6141", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6141[[1]]
      r$end <- input$dates6141[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6141",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6141))) {
      from=as.Date(input$dates6141[1L])
      to=as.Date(input$dates6141[2L])
      if (from>to) to = from
      selectdates6141.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.1_1 <- data$df6141.1[as.Date(data$df6141.1$`Дата операции`) %in% selectdates6141.1_5, ]
    } else {
      selectdates6141.1_6 <- unique(as.Date(data$df6141.1$`Дата операции`))
      data$df6141.1_1 <- data$df6141.1[data$df6141.1$`Дата операции` %in% selectdates6141.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6141))) {
      from=as.Date(input$dates6141[1L])
      to=as.Date(input$dates6141[2L])
      if (from>to) to = from
      selectdates6141.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6141.2_1 <- data$df6141.2[as.Date(data$df6141.2$`Дата операции`) %in% selectdates6141.2_5, ]
    } else {
      selectdates6141.2_6 <- unique(as.Date(data$df6141.2$`Дата операции`))
      data$df6141.2_1 <- data$df6141.2[data$df6141.2$`Дата операции` %in% selectdates6141.2_6, ]
    }
  })

observe({
  data$df6141[1, 2:5] <- data$df6141.1_1[, list(
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
  data$df6141[2, 2:5] <- data$df6141.2_1[, list(
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

observe({ data$df6141[3, 2:5] <- data$df6141[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6141 <- renderUI({!any(is.na(input$dates6141))})

  output$table6141Item1 <- renderRHandsontable({
    rhandsontable(data$df6141, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 550) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6141 <- downloadHandler(
    filename = function() { "df6141.xlsx" },
    content = function(file) {
      write.xlsx(data$df6141, file)
  })

#**************************************

#6141.1

observeEvent(input$dates6141.1, {
    start <- ymd(input$dates6141.1[[1]])
    end <- ymd(input$dates6141.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6141.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6141.1[[1]]
      r$end <- input$dates6141.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6141.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6141_1Item1)) {
    data$df6141.1 <- hot_to_r(input$table6141_1Item1) 

    if (!any(is.na(input$dates6141.1)) && input$choices6141.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6141.1[1L])
      	to=as.Date(input$dates6141.1[2L])
      	if (from>to) to = from
      	selectdates6141.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.1_2 <- data$df6141.1[as.Date(data$df6141.1$"Дата операции") %in% selectdates6141.1_1, ]
    } else if (!is.null(input$text) && input$choices6141.1 == "Выбор по номеру первичного документа") {
      	data$df6141.1_2 <- data$df6141.1[data$df6141.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6141.1 == "Выбор по статье дохода") {
      	data$df6141.1_2 <- data$df6141.1[data$df6141.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6141.1) && !any(is.na(input$dates6141.1)) && !is.null(input$text) && input$choices6141.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6141.1[1L])
      	to=as.Date(input$dates6141.1[2L])
      	if (from>to) to = from
      	selectdates6141.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.1_2 <- data$df6141.1[as.Date(data$df6141.1$"Дата операции") %in% selectdates6141.1_2 & data$df6141.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6141.1) && !any(is.na(input$dates6141.1)) && !is.null(input$text) && input$choices6141.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6141.1[1L])
      	to=as.Date(input$dates6141.1[2L])
      	if (from>to) to = from
      	selectdates6141.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.1_2 <- data$df6141.1[as.Date(data$df6141.1$"Дата операции") %in% selectdates6141.1_3 & data$df6141.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6141.1_4 <- unique(data$df6141.1$"Дата операции")
        data$df6141.1_2 <- data$df6141.1[data$df6141.1$"Дата операции" %in% selectdates6141.1_4, ]
    }
}
})

  output$table6141_1Item1 <- renderRHandsontable({
    
   data$df6141.1[, `Сальдо конечное` := data$df6141.1[[7]] + data$df6141.1[[8]] - data$df6141.1[[9]]]

    rhandsontable(data$df6141.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6141.1 <- renderUI({
    if (input$choices6141.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6141.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6141.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6141.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6141.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6141.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6141.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6141.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6141.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6141.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6141.1 <- downloadHandler(
    filename = function() { "df6141.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6141.1, file)
  })

  output$download_df6141.1_2 <- downloadHandler(
    filename = function() { "df6141.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6141.1_2, file)
  })

#****************************************

#6141.2

observeEvent(input$dates6141.2, {
    start <- ymd(input$dates6141.2[[1]])
    end <- ymd(input$dates6141.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6141.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6141.2[[1]]
      r$end <- input$dates6141.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6141.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6141_2Item1)) {
    data$df6141.2 <- hot_to_r(input$table6141_2Item1) 

    if (!any(is.na(input$dates6141.2)) && input$choices6141.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6141.2[1L])
      	to=as.Date(input$dates6141.2[2L])
      	if (from>to) to = from
      	selectdates6141.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.2_2 <- data$df6141.2[as.Date(data$df6141.2$"Дата операции") %in% selectdates6141.2_1, ]
    } else if (!is.null(input$text) && input$choices6141.2 == "Выбор по номеру первичного документа") {
      	data$df6141.2_2 <- data$df6141.2[data$df6141.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6141.2 == "Выбор по статье дохода") {
      	data$df6141.2_2 <- data$df6141.2[data$df6141.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6141.2) && !any(is.na(input$dates6141.2)) && !is.null(input$text) && input$choices6141.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6141.2[1L])
      	to=as.Date(input$dates6141.2[2L])
      	if (from>to) to = from
      	selectdates6141.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.2_2 <- data$df6141.2[as.Date(data$df6141.2$"Дата операции") %in% selectdates6141.2_2 & data$df6141.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6141.2) && !any(is.na(input$dates6141.2)) && !is.null(input$text) && input$choices6141.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6141.2[1L])
      	to=as.Date(input$dates6141.2[2L])
      	if (from>to) to = from
      	selectdates6141.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6141.2_2 <- data$df6141.2[as.Date(data$df6141.2$"Дата операции") %in% selectdates6141.2_3 & data$df6141.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6141.2_4 <- unique(data$df6141.2$"Дата операции")
        data$df6141.2_2 <- data$df6141.2[data$df6141.2$"Дата операции" %in% selectdates6141.2_4, ]
    }
}
})

  output$table6141_2Item1 <- renderRHandsontable({
    
   data$df6141.2[, `Сальдо конечное` := data$df6141.2[[7]] + data$df6141.2[[8]] - data$df6141.2[[9]]]

    rhandsontable(data$df6141.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6141.2 <- renderUI({
    if (input$choices6141.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6141.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6141.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6141.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6141.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6141.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6141.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6141.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6141.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6141.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6141.2 <- downloadHandler(
    filename = function() { "df6141.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6141.2, file)
  })

  output$download_df6141.2_2 <- downloadHandler(
    filename = function() { "df6141.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6141.2_2, file)
  })

#*****************************************

#ОСВ: 6142

observeEvent(input$dates6142, {
    start <- ymd(input$dates6142[[1]])
    end <- ymd(input$dates6142[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6142", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6142[[1]]
      r$end <- input$dates6142[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6142",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6142))) {
      from=as.Date(input$dates6142[1L])
      to=as.Date(input$dates6142[2L])
      if (from>to) to = from
      selectdates6142.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.1_1 <- data$df6142.1[as.Date(data$df6142.1$`Дата операции`) %in% selectdates6142.1_5, ]
    } else {
      selectdates6142.1_6 <- unique(as.Date(data$df6142.1$`Дата операции`))
      data$df6142.1_1 <- data$df6142.1[data$df6142.1$`Дата операции` %in% selectdates6142.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6142))) {
      from=as.Date(input$dates6142[1L])
      to=as.Date(input$dates6142[2L])
      if (from>to) to = from
      selectdates6142.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6142.2_1 <- data$df6142.2[as.Date(data$df6142.2$`Дата операции`) %in% selectdates6142.2_5, ]
    } else {
      selectdates6142.2_6 <- unique(as.Date(data$df6142.2$`Дата операции`))
      data$df6142.2_1 <- data$df6142.2[data$df6142.2$`Дата операции` %in% selectdates6142.2_6, ]
    }
  })

observe({
  data$df6142[1, 2:5] <- data$df6142.1_1[, list(
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
  data$df6142[2, 2:5] <- data$df6142.2_1[, list(
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

observe({ data$df6142[3, 2:5] <- data$df6142[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6142 <- renderUI({!any(is.na(input$dates6142))})

  output$table6142Item1 <- renderRHandsontable({
    rhandsontable(data$df6142, colWidths = 150, height = 170, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6142 <- downloadHandler(
    filename = function() { "df6142.xlsx" },
    content = function(file) {
      write.xlsx(data$df6142, file)
  })

#**************************************

#6142.1

observeEvent(input$dates6142.1, {
    start <- ymd(input$dates6142.1[[1]])
    end <- ymd(input$dates6142.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6142.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6142.1[[1]]
      r$end <- input$dates6142.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6142.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6142_1Item1)) {
    data$df6142.1 <- hot_to_r(input$table6142_1Item1) 

    if (!any(is.na(input$dates6142.1)) && input$choices6142.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6142.1[1L])
      	to=as.Date(input$dates6142.1[2L])
      	if (from>to) to = from
      	selectdates6142.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.1_2 <- data$df6142.1[as.Date(data$df6142.1$"Дата операции") %in% selectdates6142.1_1, ]
    } else if (!is.null(input$text) && input$choices6142.1 == "Выбор по номеру первичного документа") {
      	data$df6142.1_2 <- data$df6142.1[data$df6142.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6142.1 == "Выбор по статье дохода") {
      	data$df6142.1_2 <- data$df6142.1[data$df6142.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6142.1) && !any(is.na(input$dates6142.1)) && !is.null(input$text) && input$choices6142.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6142.1[1L])
      	to=as.Date(input$dates6142.1[2L])
      	if (from>to) to = from
      	selectdates6142.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.1_2 <- data$df6142.1[as.Date(data$df6142.1$"Дата операции") %in% selectdates6142.1_2 & data$df6142.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6142.1) && !any(is.na(input$dates6142.1)) && !is.null(input$text) && input$choices6142.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6142.1[1L])
      	to=as.Date(input$dates6142.1[2L])
      	if (from>to) to = from
      	selectdates6142.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.1_2 <- data$df6142.1[as.Date(data$df6142.1$"Дата операции") %in% selectdates6142.1_3 & data$df6142.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6142.1_4 <- unique(data$df6142.1$"Дата операции")
        data$df6142.1_2 <- data$df6142.1[data$df6142.1$"Дата операции" %in% selectdates6142.1_4, ]
    }
}
})

  output$table6142_1Item1 <- renderRHandsontable({
    
   data$df6142.1[, `Сальдо конечное` := data$df6142.1[[6]] + data$df6142.1[[7]] - data$df6142.1[[8]]]

    rhandsontable(data$df6142.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6142.1 <- renderUI({
    if (input$choices6142.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6142.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6142.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6142.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6142.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6142.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6142.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6142.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6142.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6142.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6142.1 <- downloadHandler(
    filename = function() { "df6142.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6142.1, file)
  })

  output$download_df6142.1_2 <- downloadHandler(
    filename = function() { "df6142.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6142.1_2, file)
  })

#****************************************

#6142.2

observeEvent(input$dates6142.2, {
    start <- ymd(input$dates6142.2[[1]])
    end <- ymd(input$dates6142.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6142.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6142.2[[1]]
      r$end <- input$dates6142.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6142.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6142_2Item1)) {
    data$df6142.2 <- hot_to_r(input$table6142_2Item1) 

    if (!any(is.na(input$dates6142.2)) && input$choices6142.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6142.2[1L])
      	to=as.Date(input$dates6142.2[2L])
      	if (from>to) to = from
      	selectdates6142.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.2_2 <- data$df6142.2[as.Date(data$df6142.2$"Дата операции") %in% selectdates6142.2_1, ]
    } else if (!is.null(input$text) && input$choices6142.2 == "Выбор по номеру первичного документа") {
      	data$df6142.2_2 <- data$df6142.2[data$df6142.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6142.2 == "Выбор по статье дохода") {
      	data$df6142.2_2 <- data$df6142.2[data$df6142.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6142.2) && !any(is.na(input$dates6142.2)) && !is.null(input$text) && input$choices6142.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6142.2[1L])
      	to=as.Date(input$dates6142.2[2L])
      	if (from>to) to = from
      	selectdates6142.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.2_2 <- data$df6142.2[as.Date(data$df6142.2$"Дата операции") %in% selectdates6142.2_2 & data$df6142.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6142.2) && !any(is.na(input$dates6142.2)) && !is.null(input$text) && input$choices6142.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6142.2[1L])
      	to=as.Date(input$dates6142.2[2L])
      	if (from>to) to = from
      	selectdates6142.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6142.2_2 <- data$df6142.2[as.Date(data$df6142.2$"Дата операции") %in% selectdates6142.2_3 & data$df6142.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6142.2_4 <- unique(data$df6142.2$"Дата операции")
        data$df6142.2_2 <- data$df6142.2[data$df6142.2$"Дата операции" %in% selectdates6142.2_4, ]
    }
}
})

  output$table6142_2Item1 <- renderRHandsontable({
    
   data$df6142.2[, `Сальдо конечное` := data$df6142.2[[6]] + data$df6142.2[[7]] - data$df6142.2[[8]]]

    rhandsontable(data$df6142.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6142.2 <- renderUI({
    if (input$choices6142.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6142.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6142.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6142.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6142.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6142.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6142.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6142.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6142.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6142.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6142.2 <- downloadHandler(
    filename = function() { "df6142.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6142.2, file)
  })

  output$download_df6142.2_2 <- downloadHandler(
    filename = function() { "df6142.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6142.2_2, file)
  })

#****************************************

#6143

observeEvent(input$dates6143, {
    start <- ymd(input$dates6143[[1]])
    end <- ymd(input$dates6143[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6143", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6143[[1]]
      r$end <- input$dates6143[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6143",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6143Item1) && !any(is.na(input$dates6143))) {
    data$df6143 <- hot_to_r(input$table6143Item1) 

    if (!any(is.na(input$dates6143)) && input$choices6143 == "Выбор по дате операции") {
     	from=as.Date(input$dates6143[1L])
      	to=as.Date(input$dates6143[2L])
      	if (from>to) to = from
      	selectdates6143_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6143_2 <- data$df6143[as.Date(data$df6143$"Дата операции") %in% selectdates6143_1, ]
    } else if (!is.null(input$text) && input$choices6143 == "Выбор по номеру первичного документа") {
      	data$df6143_2 <- data$df6143[data$df6143$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6143 == "Выбор по статье дохода") {
      	data$df6143_2 <- data$df6143[data$df6143$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6143) && !any(is.na(input$dates6143)) && !is.null(input$text) && input$choices6143 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6143[1L])
      	to=as.Date(input$dates6143[2L])
      	if (from>to) to = from
      	selectdates6143_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6143_2 <- data$df6143[as.Date(data$df6143$"Дата операции") %in% selectdates6143_2 & data$df6143$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6143) && !any(is.na(input$dates6143)) && !is.null(input$text) && input$choices6143 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6143[1L])
      	to=as.Date(input$dates6143[2L])
      	if (from>to) to = from
      	selectdates6143_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6143_2 <- data$df6143[as.Date(data$df6143$"Дата операции") %in% selectdates6143_3 & data$df6143$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6143_4 <- unique(data$df6143$"Дата операции")
        data$df6143_2 <- data$df6143[data$df6143$"Дата операции" %in% selectdates6143_4, ]
    }
}
})  

  output$table6143Item1 <- renderRHandsontable({

   data$df6143[, `Сальдо конечное` := data$df6143[[6]] + data$df6143[[7]] - data$df6143[[8]]]

    rhandsontable(data$df6143, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6143 <- renderUI({
    if (input$choices6143 == "Выбор по дате операции") {
      	dateRangeInput("dates6143", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6143 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6143 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6143 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6143", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6143 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6143", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6143Item2 <- renderRHandsontable({
    rhandsontable(data$df6143_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6143 <- downloadHandler(
    filename = function() { "df6143.xlsx" },
    content = function(file) {
      write.xlsx(data$df6143, file)
  })

  output$download_df6143_2 <- downloadHandler(
    filename = function() { "df6143_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6143_2, file)
  })

#****************************************

#6150

observeEvent(input$dates6150, {
    start <- ymd(input$dates6150[[1]])
    end <- ymd(input$dates6150[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6150", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6150[[1]]
      r$end <- input$dates6150[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6150",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6150Item1) && !any(is.na(input$dates6150))) {
    data$df6150 <- hot_to_r(input$table6150Item1) 

    if (!any(is.na(input$dates6150)) && input$choices6150 == "Выбор по дате операции") {
     	from=as.Date(input$dates6150[1L])
      	to=as.Date(input$dates6150[2L])
      	if (from>to) to = from
      	selectdates6150_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6150_2 <- data$df6150[as.Date(data$df6150$"Дата операции") %in% selectdates6150_1, ]
    } else if (!is.null(input$text) && input$choices6150 == "Выбор по номеру первичного документа") {
      	data$df6150_2 <- data$df6150[data$df6150$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6150 == "Выбор по статье дохода") {
      	data$df6150_2 <- data$df6150[data$df6150$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6150) && !any(is.na(input$dates6150)) && !is.null(input$text) && input$choices6150 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6150[1L])
      	to=as.Date(input$dates6150[2L])
      	if (from>to) to = from
      	selectdates6150_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6150_2 <- data$df6150[as.Date(data$df6150$"Дата операции") %in% selectdates6150_2 & data$df6150$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6150) && !any(is.na(input$dates6150)) && !is.null(input$text) && input$choices6150 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6150[1L])
      	to=as.Date(input$dates6150[2L])
      	if (from>to) to = from
      	selectdates6150_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6150_2 <- data$df6150[as.Date(data$df6150$"Дата операции") %in% selectdates6150_3 & data$df6150$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6150_4 <- unique(data$df6150$"Дата операции")
        data$df6150_2 <- data$df6150[data$df6150$"Дата операции" %in% selectdates6150_4, ]
    }
}
})  

  output$table6150Item1 <- renderRHandsontable({

   data$df6150[, `Сальдо конечное` := data$df6150[[6]] + data$df6150[[7]] - data$df6150[[8]]]

    rhandsontable(data$df6150, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6150 <- renderUI({
    if (input$choices6150 == "Выбор по дате операции") {
      	dateRangeInput("dates6150", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6150 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6150 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6150 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6150", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6150 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6150", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6150Item2 <- renderRHandsontable({
    rhandsontable(data$df6150_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6150 <- downloadHandler(
    filename = function() { "df6150.xlsx" },
    content = function(file) {
      write.xlsx(data$df6150, file)
  })

  output$download_df6150_2 <- downloadHandler(
    filename = function() { "df6150_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6150_2, file)
  })

#****************************************

#6160

observeEvent(input$dates6160, {
    start <- ymd(input$dates6160[[1]])
    end <- ymd(input$dates6160[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6160", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6160[[1]]
      r$end <- input$dates6160[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6160",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6160Item1) && !any(is.na(input$dates6160))) {
    data$df6160 <- hot_to_r(input$table6160Item1) 

    if (!any(is.na(input$dates6160)) && input$choices6160 == "Выбор по дате операции") {
     	from=as.Date(input$dates6160[1L])
      	to=as.Date(input$dates6160[2L])
      	if (from>to) to = from
      	selectdates6160_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6160_2 <- data$df6160[as.Date(data$df6160$"Дата операции") %in% selectdates6160_1, ]
    } else if (!is.null(input$text) && input$choices6160 == "Выбор по номеру первичного документа") {
      	data$df6160_2 <- data$df6160[data$df6160$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6160 == "Выбор по статье дохода") {
      	data$df6160_2 <- data$df6160[data$df6160$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6160) && !any(is.na(input$dates6160)) && !is.null(input$text) && input$choices6160 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6160[1L])
      	to=as.Date(input$dates6160[2L])
      	if (from>to) to = from
      	selectdates6160_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6160_2 <- data$df6160[as.Date(data$df6160$"Дата операции") %in% selectdates6160_2 & data$df6160$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6160) && !any(is.na(input$dates6160)) && !is.null(input$text) && input$choices6160 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6160[1L])
      	to=as.Date(input$dates6160[2L])
      	if (from>to) to = from
      	selectdates6160_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6160_2 <- data$df6160[as.Date(data$df6160$"Дата операции") %in% selectdates6160_3 & data$df6160$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6160_4 <- unique(data$df6160$"Дата операции")
        data$df6160_2 <- data$df6160[data$df6160$"Дата операции" %in% selectdates6160_4, ]
    }
}
})  

  output$table6160Item1 <- renderRHandsontable({

   data$df6160[, `Сальдо конечное` := data$df6160[[6]] + data$df6160[[7]] - data$df6160[[8]]]

    rhandsontable(data$df6160, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6160 <- renderUI({
    if (input$choices6160 == "Выбор по дате операции") {
      	dateRangeInput("dates6160", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6160 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6160 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6160 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6160", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6160 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6160", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6160Item2 <- renderRHandsontable({
    rhandsontable(data$df6160_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6160 <- downloadHandler(
    filename = function() { "df6160.xlsx" },
    content = function(file) {
      write.xlsx(data$df6160, file)
  })

  output$download_df6160_2 <- downloadHandler(
    filename = function() { "df6160_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6160_2, file)
  })

#*****************************************

#ОСВ: 6170

observeEvent(input$dates6170, {
    start <- ymd(input$dates6170[[1]])
    end <- ymd(input$dates6170[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6170", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6170[[1]]
      r$end <- input$dates6170[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6170",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6170))) {
      from=as.Date(input$dates6170[1L])
      to=as.Date(input$dates6170[2L])
      if (from>to) to = from
      selectdates6171_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6171_1 <- data$df6171[as.Date(data$df6171$`Дата операции`) %in% selectdates6171_5, ]
    } else {
      selectdates6171_6 <- unique(as.Date(data$df6171$`Дата операции`))
      data$df6171_1 <- data$df6171[data$df6171$`Дата операции` %in% selectdates6171_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6170))) {
      from=as.Date(input$dates6170[1L])
      to=as.Date(input$dates6170[2L])
      if (from>to) to = from
      selectdates6172_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6172_1 <- data$df6172[as.Date(data$df6172$`Дата операции`) %in% selectdates6172_5, ]
    } else {
      selectdates6172_6 <- unique(as.Date(data$df6172$`Дата операции`))
      data$df6172_1 <- data$df6172[data$df6172$`Дата операции` %in% selectdates6172_6, ]
    }
  })

observe({
  data$df6170[1, 2:5] <- data$df6171_1[, list(
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
  data$df6170[2, 2:5] <- data$df6172_1[, list(
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

observe({ data$df6170[3, 2:5] <- data$df6170[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6170 <- renderUI({!any(is.na(input$dates6170))})

  output$table6170Item1 <- renderRHandsontable({
    rhandsontable(data$df6170, colWidths = 150, height = 150, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 350) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6170 <- downloadHandler(
    filename = function() { "df6170.xlsx" },
    content = function(file) {
      write.xlsx(data$df6170, file)
  })

#*******************************************

#6171

observeEvent(input$dates6171, {
    start <- ymd(input$dates6171[[1]])
    end <- ymd(input$dates6171[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6171", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6171[[1]]
      r$end <- input$dates6171[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6171",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6171Item1) && !any(is.na(input$dates6171))) {
    data$df6171 <- hot_to_r(input$table6171Item1) 

    if (!any(is.na(input$dates6171)) && input$choices6171 == "Выбор по дате операции") {
     	from=as.Date(input$dates6171[1L])
      	to=as.Date(input$dates6171[2L])
      	if (from>to) to = from
      	selectdates6171_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6171_2 <- data$df6171[as.Date(data$df6171$"Дата операции") %in% selectdates6171_1, ]
    } else if (!is.null(input$text) && input$choices6171 == "Выбор по номеру первичного документа") {
      	data$df6171_2 <- data$df6171[data$df6171$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6171 == "Выбор по статье дохода") {
      	data$df6171_2 <- data$df6171[data$df6171$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6171) && !any(is.na(input$dates6171)) && !is.null(input$text) && input$choices6171 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6171[1L])
      	to=as.Date(input$dates6171[2L])
      	if (from>to) to = from
      	selectdates6171_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6171_2 <- data$df6171[as.Date(data$df6171$"Дата операции") %in% selectdates6171_2 & data$df6171$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6171) && !any(is.na(input$dates6171)) && !is.null(input$text) && input$choices6171 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6171[1L])
      	to=as.Date(input$dates6171[2L])
      	if (from>to) to = from
      	selectdates6171_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6171_2 <- data$df6171[as.Date(data$df6171$"Дата операции") %in% selectdates6171_3 & data$df6171$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6171_4 <- unique(data$df6171$"Дата операции")
        data$df6171_2 <- data$df6171[data$df6171$"Дата операции" %in% selectdates6171_4, ]
    }
}
})  

  output$table6171Item1 <- renderRHandsontable({

   data$df6171[, `Сальдо конечное` := data$df6171[[7]] + data$df6171[[8]] - data$df6171[[9]]]

    rhandsontable(data$df6171, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6171 <- renderUI({
    if (input$choices6171 == "Выбор по дате операции") {
      	dateRangeInput("dates6171", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6171 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6171 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6171 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6171", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6171 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6171", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6171Item2 <- renderRHandsontable({
    rhandsontable(data$df6171_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6171 <- downloadHandler(
    filename = function() { "df6171.xlsx" },
    content = function(file) {
      write.xlsx(data$df6171, file)
  })

  output$download_df6171_2 <- downloadHandler(
    filename = function() { "df6171_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6171_2, file)
  })

#****************************************

#6172

observeEvent(input$dates6172, {
    start <- ymd(input$dates6172[[1]])
    end <- ymd(input$dates6172[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6172", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6172[[1]]
      r$end <- input$dates6172[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6172",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6172Item1) && !any(is.na(input$dates6172))) {
    data$df6172 <- hot_to_r(input$table6172Item1) 

    if (!any(is.na(input$dates6172)) && input$choices6172 == "Выбор по дате операции") {
     	from=as.Date(input$dates6172[1L])
      	to=as.Date(input$dates6172[2L])
      	if (from>to) to = from
      	selectdates6172_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6172_2 <- data$df6172[as.Date(data$df6172$"Дата операции") %in% selectdates6172_1, ]
    } else if (!is.null(input$text) && input$choices6172 == "Выбор по номеру первичного документа") {
      	data$df6172_2 <- data$df6172[data$df6172$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6172 == "Выбор по статье дохода") {
      	data$df6172_2 <- data$df6172[data$df6172$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6172) && !any(is.na(input$dates6172)) && !is.null(input$text) && input$choices6172 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6172[1L])
      	to=as.Date(input$dates6172[2L])
      	if (from>to) to = from
      	selectdates6172_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6172_2 <- data$df6172[as.Date(data$df6172$"Дата операции") %in% selectdates6172_2 & data$df6172$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6172) && !any(is.na(input$dates6172)) && !is.null(input$text) && input$choices6172 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6172[1L])
      	to=as.Date(input$dates6172[2L])
      	if (from>to) to = from
      	selectdates6172_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6172_2 <- data$df6172[as.Date(data$df6172$"Дата операции") %in% selectdates6172_3 & data$df6172$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6172_4 <- unique(data$df6172$"Дата операции")
        data$df6172_2 <- data$df6172[data$df6172$"Дата операции" %in% selectdates6172_4, ]
    }
}
})  

  output$table6172Item1 <- renderRHandsontable({

   data$df6172[, `Сальдо конечное` := data$df6172[[7]] + data$df6172[[8]] - data$df6172[[9]]]

    rhandsontable(data$df6172, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6172 <- renderUI({
    if (input$choices6172 == "Выбор по дате операции") {
      	dateRangeInput("dates6172", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6172 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6172 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6172 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6172", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6172 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6172", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6172Item2 <- renderRHandsontable({
    rhandsontable(data$df6172_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6172 <- downloadHandler(
    filename = function() { "df6172.xlsx" },
    content = function(file) {
      write.xlsx(data$df6172, file)
  })

  output$download_df6172_2 <- downloadHandler(
    filename = function() { "df6172_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6172_2, file)
  })

#**************************************

#ОСВ: 6200

observeEvent(input$dates6200, {
    start <- ymd(input$dates6200[[1]])
    end <- ymd(input$dates6200[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6200", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6200[[1]]
      r$end <- input$dates6200[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6200",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

#************************************

#ОСВ: 6200 to compile 6210

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6210.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6210.1_4 <- data$df6210.1[as.Date(data$df6210.1$`Дата операции`) %in% selectdates6210.1_7, ]
    } else {
      selectdates6210.1_8 <- unique(as.Date(data$df6210.1$`Дата операции`))
      data$df6210.1_4 <- data$df6210.1[data$df6210.1$`Дата операции` %in% selectdates6210.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6210.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6210.2_4 <- data$df6210.2[as.Date(data$df6210.2$`Дата операции`) %in% selectdates6210.2_7, ]
    } else {
      selectdates6210.2_8 <- unique(as.Date(data$df6210.2$`Дата операции`))
      data$df6210.2_4 <- data$df6210.2[data$df6210.2$`Дата операции` %in% selectdates6210.2_8, ]
    }
  })

observe({
  data$df6210_3[1, 2:5] <- data$df6210.1_4[, list(
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
  data$df6210_3[2, 2:5] <- data$df6210.2_4[, list(
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

observe({ data$df6200[1, 2:5] <- data$df6210_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6200 to compile 6220

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6220.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6220.1_4 <- data$df6220.1[as.Date(data$df6220.1$`Дата операции`) %in% selectdates6220.1_7, ]
    } else {
      selectdates6220.1_8 <- unique(as.Date(data$df6220.1$`Дата операции`))
      data$df6220.1_4 <- data$df6220.1[data$df6220.1$`Дата операции` %in% selectdates6220.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6220.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6220.2_4 <- data$df6220.2[as.Date(data$df6220.2$`Дата операции`) %in% selectdates6220.2_7, ]
    } else {
      selectdates6220.2_8 <- unique(as.Date(data$df6220.2$`Дата операции`))
      data$df6220.2_4 <- data$df6220.2[data$df6220.2$`Дата операции` %in% selectdates6220.2_8, ]
    }
  })

observe({
  data$df6220_3[1, 2:5] <- data$df6220.1_4[, list(
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
  data$df6220_3[2, 2:5] <- data$df6220.2_4[, list(
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

observe({ data$df6200[2, 2:5] <- data$df6220_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6200 to compile 6230

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6230.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6230.1_4 <- data$df6230.1[as.Date(data$df6230.1$`Дата операции`) %in% selectdates6230.1_7, ]
    } else {
      selectdates6230.1_8 <- unique(as.Date(data$df6230.1$`Дата операции`))
      data$df6230.1_4 <- data$df6230.1[data$df6230.1$`Дата операции` %in% selectdates6230.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6230.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6230.2_4 <- data$df6230.2[as.Date(data$df6230.2$`Дата операции`) %in% selectdates6230.2_7, ]
    } else {
      selectdates6230.2_8 <- unique(as.Date(data$df6230.2$`Дата операции`))
      data$df6230.2_4 <- data$df6230.2[data$df6230.2$`Дата операции` %in% selectdates6230.2_8, ]
    }
  })

observe({
  data$df6230_3[1, 2:5] <- data$df6230.1_4[, list(
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
  data$df6230_3[2, 2:5] <- data$df6230.2_4[, list(
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

observe({ data$df6200[3, 2:5] <- data$df6230_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************


#ОСВ: 6200 to compile 6240

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6240.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6240.1_4 <- data$df6240.1[as.Date(data$df6240.1$`Дата операции`) %in% selectdates6240.1_7, ]
    } else {
      selectdates6240.1_8 <- unique(as.Date(data$df6240.1$`Дата операции`))
      data$df6240.1_4 <- data$df6240.1[data$df6240.1$`Дата операции` %in% selectdates6240.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6240.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6240.2_4 <- data$df6240.2[as.Date(data$df6240.2$`Дата операции`) %in% selectdates6240.2_7, ]
    } else {
      selectdates6240.2_8 <- unique(as.Date(data$df6240.2$`Дата операции`))
      data$df6240.2_4 <- data$df6240.2[data$df6240.2$`Дата операции` %in% selectdates6240.2_8, ]
    }
  })

observe({
  data$df6240_3[1, 2:5] <- data$df6240.1_4[, list(
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
  data$df6240_3[2, 2:5] <- data$df6240.2_4[, list(
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

observe({ data$df6200[4, 2:5] <- data$df6240_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6200 to compile 6250

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6250.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6250.1_4 <- data$df6250.1[as.Date(data$df6250.1$`Дата операции`) %in% selectdates6250.1_7, ]
    } else {
      selectdates6250.1_8 <- unique(as.Date(data$df6250.1$`Дата операции`))
      data$df6250.1_4 <- data$df6250.1[data$df6250.1$`Дата операции` %in% selectdates6250.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6250.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6250.2_4 <- data$df6250.2[as.Date(data$df6250.2$`Дата операции`) %in% selectdates6250.2_7, ]
    } else {
      selectdates6250.2_8 <- unique(as.Date(data$df6250.2$`Дата операции`))
      data$df6250.2_4 <- data$df6250.2[data$df6250.2$`Дата операции` %in% selectdates6250.2_8, ]
    }
  })

observe({
  data$df6250_3[1, 2:5] <- data$df6250.1_4[, list(
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
  data$df6250_3[2, 2:5] <- data$df6250.2_4[, list(
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

observe({ data$df6200[5, 2:5] <- data$df6250_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6200 to compile 6260

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6261.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.1_4 <- data$df6261.1[as.Date(data$df6261.1$`Дата операции`) %in% selectdates6261.1_9, ]
    } else {
      selectdates6261.1_10 <- unique(as.Date(data$df6261.1$`Дата операции`))
      data$df6261.1_4 <- data$df6261.1[data$df6261.1$`Дата операции` %in% selectdates6261.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6261.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.2_4 <- data$df6261.2[as.Date(data$df6261.2$`Дата операции`) %in% selectdates6261.2_9, ]
    } else {
      selectdates6261.2_10 <- unique(as.Date(data$df6261.2$`Дата операции`))
      data$df6261.2_4 <- data$df6261.2[data$df6261.2$`Дата операции` %in% selectdates6261.2_10, ]
    }
  })

observe({
  data$df6261_3[1, 2:5] <- data$df6261.1_4[, list(
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
  data$df6261_3[2, 2:5] <- data$df6261.2_4[, list(
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

observe({ data$df6260_3[1, 2:5] <- data$df6261_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6262.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.1_4 <- data$df6262.1[as.Date(data$df6262.1$`Дата операции`) %in% selectdates6262.1_9, ]
    } else {
      selectdates6262.1_10 <- unique(as.Date(data$df6262.1$`Дата операции`))
      data$df6262.1_4 <- data$df6262.1[data$df6262.1$`Дата операции` %in% selectdates6262.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6262.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.2_4 <- data$df6262.2[as.Date(data$df6262.2$`Дата операции`) %in% selectdates6262.2_9, ]
    } else {
      selectdates6262.2_10 <- unique(as.Date(data$df6262.2$`Дата операции`))
      data$df6262.2_4 <- data$df6262.2[data$df6262.2$`Дата операции` %in% selectdates6262.2_10, ]
    }
  })

observe({
  data$df6262_3[1, 2:5] <- data$df6262.1_4[, list(
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
  data$df6262_3[2, 2:5] <- data$df6262.2_4[, list(
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

observe({ data$df6260_3[2, 2:5] <- data$df6262_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6263.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.1_4 <- data$df6263.1[as.Date(data$df6263.1$`Дата операции`) %in% selectdates6263.1_9, ]
    } else {
      selectdates6263.1_10 <- unique(as.Date(data$df6263.1$`Дата операции`))
      data$df6263.1_4 <- data$df6263.1[data$df6263.1$`Дата операции` %in% selectdates6263.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6263.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.2_4 <- data$df6263.2[as.Date(data$df6263.2$`Дата операции`) %in% selectdates6263.2_9, ]
    } else {
      selectdates6263.2_10 <- unique(as.Date(data$df6263.2$`Дата операции`))
      data$df6263.2_4 <- data$df6263.2[data$df6263.2$`Дата операции` %in% selectdates6263.2_10, ]
    }
  })

observe({
  data$df6263_3[1, 2:5] <- data$df6263.1_4[, list(
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
  data$df6263_3[2, 2:5] <- data$df6263.2_4[, list(
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

observe({ data$df6260_3[3, 2:5] <- data$df6263_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6264.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.1_4 <- data$df6264.1[as.Date(data$df6264.1$`Дата операции`) %in% selectdates6264.1_9, ]
    } else {
      selectdates6264.1_10 <- unique(as.Date(data$df6264.1$`Дата операции`))
      data$df6264.1_4 <- data$df6264.1[data$df6264.1$`Дата операции` %in% selectdates6264.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6264.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.2_4 <- data$df6264.2[as.Date(data$df6264.2$`Дата операции`) %in% selectdates6264.2_9, ]
    } else {
      selectdates6264.2_10 <- unique(as.Date(data$df6264.2$`Дата операции`))
      data$df6264.2_4 <- data$df6264.2[data$df6264.2$`Дата операции` %in% selectdates6264.2_10, ]
    }
  })

observe({
  data$df6264_3[1, 2:5] <- data$df6264.1_4[, list(
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
  data$df6264_3[2, 2:5] <- data$df6264.2_4[, list(
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

observe({ data$df6260_3[4, 2:5] <- data$df6264_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6200[6, 2:5] <- data$df6260_3[, .SD[1:4, lapply(.SD, sum)], .SDcols = 2:5] })

#************************************

#ОСВ: 6200 to compile 6270

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6271.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.1_4 <- data$df6271.1[as.Date(data$df6271.1$`Дата операции`) %in% selectdates6271.1_9, ]
    } else {
      selectdates6271.1_10 <- unique(as.Date(data$df6271.1$`Дата операции`))
      data$df6271.1_4 <- data$df6271.1[data$df6271.1$`Дата операции` %in% selectdates6271.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6271.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.2_4 <- data$df6271.2[as.Date(data$df6271.2$`Дата операции`) %in% selectdates6271.2_9, ]
    } else {
      selectdates6271.2_10 <- unique(as.Date(data$df6271.2$`Дата операции`))
      data$df6271.2_4 <- data$df6271.2[data$df6271.2$`Дата операции` %in% selectdates6271.2_10, ]
    }
  })

observe({
  data$df6271_3[1, 2:5] <- data$df6271.1_4[, list(
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
  data$df6271_3[2, 2:5] <- data$df6271.2_4[, list(
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

observe({ data$df6270_3[1, 2:5] <- data$df6271_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6272.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.1_4 <- data$df6272.1[as.Date(data$df6272.1$`Дата операции`) %in% selectdates6272.1_9, ]
    } else {
      selectdates6272.1_10 <- unique(as.Date(data$df6272.1$`Дата операции`))
      data$df6272.1_4 <- data$df6272.1[data$df6272.1$`Дата операции` %in% selectdates6272.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6272.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.2_4 <- data$df6272.2[as.Date(data$df6272.2$`Дата операции`) %in% selectdates6272.2_9, ]
    } else {
      selectdates6272.2_10 <- unique(as.Date(data$df6272.2$`Дата операции`))
      data$df6272.2_4 <- data$df6272.2[data$df6272.2$`Дата операции` %in% selectdates6272.2_10, ]
    }
  })

observe({
  data$df6272_3[1, 2:5] <- data$df6272.1_4[, list(
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
  data$df6272_3[2, 2:5] <- data$df6272.2_4[, list(
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

observe({ data$df6270_3[2, 2:5] <- data$df6272_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6273_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6273_4 <- data$df6273[as.Date(data$df6273$`Дата операции`) %in% selectdates6273_7, ]
    } else {
      selectdates6273_8 <- unique(as.Date(data$df6273$`Дата операции`))
      data$df6273_4 <- data$df6273[data$df6273$`Дата операции` %in% selectdates6273_8, ]
    }
  })

observe({
  data$df6270_3[3, 2:5] <- data$df6273_4[, list(
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
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6274_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6274_4 <- data$df6274[as.Date(data$df6274$`Дата операции`) %in% selectdates6274_7, ]
    } else {
      selectdates6274_8 <- unique(as.Date(data$df6274$`Дата операции`))
      data$df6274_4 <- data$df6274[data$df6274$`Дата операции` %in% selectdates6274_8, ]
    }
  })

observe({
  data$df6270_3[4, 2:5] <- data$df6274_4[, list(
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
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6275.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.1_4 <- data$df6275.1[as.Date(data$df6275.1$`Дата операции`) %in% selectdates6275.1_9, ]
    } else {
      selectdates6275.1_10 <- unique(as.Date(data$df6275.1$`Дата операции`))
      data$df6275.1_4 <- data$df6275.1[data$df6275.1$`Дата операции` %in% selectdates6275.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6275.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.2_4 <- data$df6275.2[as.Date(data$df6275.2$`Дата операции`) %in% selectdates6275.2_9, ]
    } else {
      selectdates6275.2_10 <- unique(as.Date(data$df6275.2$`Дата операции`))
      data$df6275.2_4 <- data$df6275.2[data$df6275.2$`Дата операции` %in% selectdates6275.2_10, ]
    }
  })

observe({
  data$df6275_3[1, 2:5] <- data$df6275.1_4[, list(
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
  data$df6275_3[2, 2:5] <- data$df6275.2_4[, list(
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

observe({ data$df6270_3[5, 2:5] <- data$df6275_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6276.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.1_4 <- data$df6276.1[as.Date(data$df6276.1$`Дата операции`) %in% selectdates6276.1_9, ]
    } else {
      selectdates6276.1_10 <- unique(as.Date(data$df6276.1$`Дата операции`))
      data$df6276.1_4 <- data$df6276.1[data$df6276.1$`Дата операции` %in% selectdates6276.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6276.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.2_4 <- data$df6276.2[as.Date(data$df6276.2$`Дата операции`) %in% selectdates6276.2_9, ]
    } else {
      selectdates6276.2_10 <- unique(as.Date(data$df6276.2$`Дата операции`))
      data$df6276.2_4 <- data$df6276.2[data$df6276.2$`Дата операции` %in% selectdates6276.2_10, ]
    }
  })

observe({
  data$df6276_3[1, 2:5] <- data$df6276.1_4[, list(
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
  data$df6276_3[2, 2:5] <- data$df6276.2_4[, list(
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

observe({ data$df6270_3[6, 2:5] <- data$df6276_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6277.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.1_4 <- data$df6277.1[as.Date(data$df6277.1$`Дата операции`) %in% selectdates6277.1_9, ]
    } else {
      selectdates6277.1_10 <- unique(as.Date(data$df6277.1$`Дата операции`))
      data$df6277.1_4 <- data$df6277.1[data$df6277.1$`Дата операции` %in% selectdates6277.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6277.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.2_4 <- data$df6277.2[as.Date(data$df6277.2$`Дата операции`) %in% selectdates6277.2_9, ]
    } else {
      selectdates6277.2_10 <- unique(as.Date(data$df6277.2$`Дата операции`))
      data$df6277.2_4 <- data$df6277.2[data$df6277.2$`Дата операции` %in% selectdates6277.2_10, ]
    }
  })

observe({
  data$df6277_3[1, 2:5] <- data$df6277.1_4[, list(
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
  data$df6277_3[2, 2:5] <- data$df6277.2_4[, list(
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

observe({ data$df6270_3[7, 2:5] <- data$df6277_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6200[7, 2:5] <- data$df6270_3[, .SD[1:7, lapply(.SD, sum)], .SDcols = 2:5] })

#***********************************

#ОСВ: 6200 to compile 6280


observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6281.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.1_4 <- data$df6281.1[as.Date(data$df6281.1$`Дата операции`) %in% selectdates6281.1_9, ]
    } else {
      selectdates6281.1_10 <- unique(as.Date(data$df6281.1$`Дата операции`))
      data$df6281.1_4 <- data$df6281.1[data$df6281.1$`Дата операции` %in% selectdates6281.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6281.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.2_4 <- data$df6281.2[as.Date(data$df6281.2$`Дата операции`) %in% selectdates6281.2_9, ]
    } else {
      selectdates6281.2_10 <- unique(as.Date(data$df6281.2$`Дата операции`))
      data$df6281.2_4 <- data$df6281.2[data$df6281.2$`Дата операции` %in% selectdates6281.2_10, ]
    }
  })

observe({
  data$df6281_3[1, 2:5] <- data$df6281.1_4[, list(
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
  data$df6281_3[2, 2:5] <- data$df6281.2_4[, list(
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

observe({ data$df6280_3[1, 2:5] <- data$df6281_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6282_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6282_4 <- data$df6282[as.Date(data$df6282$`Дата операции`) %in% selectdates6282_7, ]
    } else {
      selectdates6282_8 <- unique(as.Date(data$df6282$`Дата операции`))
      data$df6282_4 <- data$df6282[data$df6282$`Дата операции` %in% selectdates6282_8, ]
    }
  })

observe({
  data$df6280_3[2, 2:5] <- data$df6282_4[, list(
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

observe({ data$df6200[8, 2:5] <- data$df6280_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

#***********************************

#ОСВ: 6200 to compile 6290

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6291_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6291_4 <- data$df6291[as.Date(data$df6291$`Дата операции`) %in% selectdates6291_7, ]
    } else {
      selectdates6291_8 <- unique(as.Date(data$df6291$`Дата операции`))
      data$df6291_4 <- data$df6291[data$df6291$`Дата операции` %in% selectdates6291_8, ]
    }
  })

observe({
  data$df6290_3[1, 2:5] <- data$df6291_4[, list(
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
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6292.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.1_4 <- data$df6292.1[as.Date(data$df6292.1$`Дата операции`) %in% selectdates6292.1_9, ]
    } else {
      selectdates6292.1_10 <- unique(as.Date(data$df6292.1$`Дата операции`))
      data$df6292.1_4 <- data$df6292.1[data$df6292.1$`Дата операции` %in% selectdates6292.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6292.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.2_4 <- data$df6292.2[as.Date(data$df6292.2$`Дата операции`) %in% selectdates6292.2_9, ]
    } else {
      selectdates6292.2_10 <- unique(as.Date(data$df6292.2$`Дата операции`))
      data$df6292.2_4 <- data$df6292.2[data$df6292.2$`Дата операции` %in% selectdates6292.2_10, ]
    }
  })

observe({
  data$df6292_3[1, 2:5] <- data$df6292.1_4[, list(
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
  data$df6292_3[2, 2:5] <- data$df6292.2_4[, list(
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

observe({ data$df6290_3[2, 2:5] <- data$df6292_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6293.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.1_4 <- data$df6293.1[as.Date(data$df6293.1$`Дата операции`) %in% selectdates6293.1_9, ]
    } else {
      selectdates6293.1_10 <- unique(as.Date(data$df6293.1$`Дата операции`))
      data$df6293.1_4 <- data$df6293.1[data$df6293.1$`Дата операции` %in% selectdates6293.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6293.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.2_4 <- data$df6293.2[as.Date(data$df6293.2$`Дата операции`) %in% selectdates6293.2_9, ]
    } else {
      selectdates6293.2_10 <- unique(as.Date(data$df6293.2$`Дата операции`))
      data$df6293.2_4 <- data$df6293.2[data$df6293.2$`Дата операции` %in% selectdates6293.2_10, ]
    }
  })

observe({
  data$df6293_3[1, 2:5] <- data$df6293.1_4[, list(
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
  data$df6293_3[2, 2:5] <- data$df6293.2_4[, list(
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

observe({ data$df6290_3[3, 2:5] <- data$df6293_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6294.1_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.1_4 <- data$df6294.1[as.Date(data$df6294.1$`Дата операции`) %in% selectdates6294.1_9, ]
    } else {
      selectdates6294.1_10 <- unique(as.Date(data$df6294.1$`Дата операции`))
      data$df6294.1_4 <- data$df6294.1[data$df6294.1$`Дата операции` %in% selectdates6294.1_10, ]
    }
  })

observe({
    if (!any(is.na(input$dates6200))) {
      from=as.Date(input$dates6200[1L])
      to=as.Date(input$dates6200[2L])
      if (from>to) to = from
      selectdates6294.2_9 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.2_4 <- data$df6294.2[as.Date(data$df6294.2$`Дата операции`) %in% selectdates6294.2_9, ]
    } else {
      selectdates6294.2_10 <- unique(as.Date(data$df6294.2$`Дата операции`))
      data$df6294.2_4 <- data$df6294.2[data$df6294.2$`Дата операции` %in% selectdates6294.2_10, ]
    }
  })

observe({
  data$df6294_3[1, 2:5] <- data$df6294.1_4[, list(
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
  data$df6294_3[2, 2:5] <- data$df6294.2_4[, list(
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

observe({ data$df6290_3[4, 2:5] <- data$df6294_3[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6200[9, 2:5] <- data$df6290_3[, .SD[1:4, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6200[10, 2:5] <- data$df6200[, .SD[1:9, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6200 <- renderUI({!any(is.na(input$dates6200))})

  output$table6200Item1 <- renderRHandsontable({
    rhandsontable(data$df6200, colWidths = 150, height = 270, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 9) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6200 <- downloadHandler(
    filename = function() { "df6200.xlsx" },
    content = function(file) {
      write.xlsx(data$df6200, file)
  })

#***********************************

#ОСВ: 6210

observeEvent(input$dates6210, {
    start <- ymd(input$dates6210[[1]])
    end <- ymd(input$dates6210[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6210", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6210[[1]]
      r$end <- input$dates6210[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6210",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6210))) {
      from=as.Date(input$dates6210[1L])
      to=as.Date(input$dates6210[2L])
      if (from>to) to = from
      selectdates6210.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6210.1_1 <- data$df6210.1[as.Date(data$df6210.1$`Дата операции`) %in% selectdates6210.1_5, ]
    } else {
      selectdates6210.1_6 <- unique(as.Date(data$df6210.1$`Дата операции`))
      data$df6210.1_1 <- data$df6210.1[data$df6210.1$`Дата операции` %in% selectdates6210.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6210))) {
      from=as.Date(input$dates6210[1L])
      to=as.Date(input$dates6210[2L])
      if (from>to) to = from
      selectdates6210.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6210.2_1 <- data$df6210.2[as.Date(data$df6210.2$`Дата операции`) %in% selectdates6210.2_5, ]
    } else {
      selectdates6210.2_6 <- unique(as.Date(data$df6210.2$`Дата операции`))
      data$df6210.2_1 <- data$df6210.2[data$df6210.2$`Дата операции` %in% selectdates6210.2_6, ]
    }
  })

observe({
  data$df6210[1, 2:5] <- data$df6210.1_1[, list(
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
  data$df6210[2, 2:5] <- data$df6210.2_1[, list(
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

observe({ data$df6210[3, 2:5] <- data$df6210[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6210 <- renderUI({!any(is.na(input$dates6210))})

  output$table6210Item1 <- renderRHandsontable({
    rhandsontable(data$df6210, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 550) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6210 <- downloadHandler(
    filename = function() { "df6210.xlsx" },
    content = function(file) {
      write.xlsx(data$df6210, file)
  })

#**************************************

#6210.1

observeEvent(input$dates6210.1, {
    start <- ymd(input$dates6210.1[[1]])
    end <- ymd(input$dates6210.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6210.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6210.1[[1]]
      r$end <- input$dates6210.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6210.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6210_1Item1)) {
    data$df6210.1 <- hot_to_r(input$table6210_1Item1) 

    if (!any(is.na(input$dates6210.1)) && input$choices6210.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6210.1[1L])
      	to=as.Date(input$dates6210.1[2L])
      	if (from>to) to = from
      	selectdates6210.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.1_2 <- data$df6210.1[as.Date(data$df6210.1$"Дата операции") %in% selectdates6210.1_1, ]
    } else if (!is.null(input$text) && input$choices6210.1 == "Выбор по номеру первичного документа") {
      	data$df6210.1_2 <- data$df6210.1[data$df6210.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6210.1 == "Выбор по статье дохода") {
      	data$df6210.1_2 <- data$df6210.1[data$df6210.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6210.1) && !any(is.na(input$dates6210.1)) && !is.null(input$text) && input$choices6210.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6210.1[1L])
      	to=as.Date(input$dates6210.1[2L])
      	if (from>to) to = from
      	selectdates6210.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.1_2 <- data$df6210.1[as.Date(data$df6210.1$"Дата операции") %in% selectdates6210.1_2 & data$df6210.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6210.1) && !any(is.na(input$dates6210.1)) && !is.null(input$text) && input$choices6210.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6210.1[1L])
      	to=as.Date(input$dates6210.1[2L])
      	if (from>to) to = from
      	selectdates6210.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.1_2 <- data$df6210.1[as.Date(data$df6210.1$"Дата операции") %in% selectdates6210.1_3 & data$df6210.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6210.1_4 <- unique(data$df6210.1$"Дата операции")
        data$df6210.1_2 <- data$df6210.1[data$df6210.1$"Дата операции" %in% selectdates6210.1_4, ]
    }
}
})

  output$table6210_1Item1 <- renderRHandsontable({
    
   data$df6210.1[, `Сальдо конечное` := data$df6210.1[[6]] + data$df6210.1[[7]] - data$df6210.1[[8]]]

    rhandsontable(data$df6210.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6210.1 <- renderUI({
    if (input$choices6210.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6210.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6210.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6210.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6210.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6210.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6210.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6210.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6210.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6210.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6210.1 <- downloadHandler(
    filename = function() { "df6210.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6210.1, file)
  })

  output$download_df6210.1_2 <- downloadHandler(
    filename = function() { "df6210.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6210.1_2, file)
  })

#****************************************

#6210.2

observeEvent(input$dates6210.2, {
    start <- ymd(input$dates6210.2[[1]])
    end <- ymd(input$dates6210.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6210.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6210.2[[1]]
      r$end <- input$dates6210.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6210.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6210_2Item1)) {
    data$df6210.2 <- hot_to_r(input$table6210_2Item1) 

    if (!any(is.na(input$dates6210.2)) && input$choices6210.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6210.2[1L])
      	to=as.Date(input$dates6210.2[2L])
      	if (from>to) to = from
      	selectdates6210.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.2_2 <- data$df6210.2[as.Date(data$df6210.2$"Дата операции") %in% selectdates6210.2_1, ]
    } else if (!is.null(input$text) && input$choices6210.2 == "Выбор по номеру первичного документа") {
      	data$df6210.2_2 <- data$df6210.2[data$df6210.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6210.2 == "Выбор по статье дохода") {
      	data$df6210.2_2 <- data$df6210.2[data$df6210.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6210.2) && !any(is.na(input$dates6210.2)) && !is.null(input$text) && input$choices6210.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6210.2[1L])
      	to=as.Date(input$dates6210.2[2L])
      	if (from>to) to = from
      	selectdates6210.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.2_2 <- data$df6210.2[as.Date(data$df6210.2$"Дата операции") %in% selectdates6210.2_2 & data$df6210.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6210.2) && !any(is.na(input$dates6210.2)) && !is.null(input$text) && input$choices6210.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6210.2[1L])
      	to=as.Date(input$dates6210.2[2L])
      	if (from>to) to = from
      	selectdates6210.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6210.2_2 <- data$df6210.2[as.Date(data$df6210.2$"Дата операции") %in% selectdates6210.2_3 & data$df6210.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6210.2_4 <- unique(data$df6210.2$"Дата операции")
        data$df6210.2_2 <- data$df6210.2[data$df6210.2$"Дата операции" %in% selectdates6210.2_4, ]
    }
}
})

  output$table6210_2Item1 <- renderRHandsontable({
    
   data$df6210.2[, `Сальдо конечное` := data$df6210.2[[6]] + data$df6210.2[[7]] - data$df6210.2[[8]]]

    rhandsontable(data$df6210.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6210.2 <- renderUI({
    if (input$choices6210.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6210.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6210.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6210.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6210.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6210.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6210.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6210.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6210.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6210.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6210.2 <- downloadHandler(
    filename = function() { "df6210.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6210.2, file)
  })

  output$download_df6210.2_2 <- downloadHandler(
    filename = function() { "df6210.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6210.2_2, file)
  })

#*****************************************

#ОСВ: 6220

observeEvent(input$dates6220, {
    start <- ymd(input$dates6220[[1]])
    end <- ymd(input$dates6220[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6220", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6220[[1]]
      r$end <- input$dates6220[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6220",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6220))) {
      from=as.Date(input$dates6220[1L])
      to=as.Date(input$dates6220[2L])
      if (from>to) to = from
      selectdates6220.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6220.1_1 <- data$df6220.1[as.Date(data$df6220.1$`Дата операции`) %in% selectdates6220.1_5, ]
    } else {
      selectdates6220.1_6 <- unique(as.Date(data$df6220.1$`Дата операции`))
      data$df6220.1_1 <- data$df6220.1[data$df6220.1$`Дата операции` %in% selectdates6220.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6220))) {
      from=as.Date(input$dates6220[1L])
      to=as.Date(input$dates6220[2L])
      if (from>to) to = from
      selectdates6220.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6220.2_1 <- data$df6220.2[as.Date(data$df6220.2$`Дата операции`) %in% selectdates6220.2_5, ]
    } else {
      selectdates6220.2_6 <- unique(as.Date(data$df6220.2$`Дата операции`))
      data$df6220.2_1 <- data$df6220.2[data$df6220.2$`Дата операции` %in% selectdates6220.2_6, ]
    }
  })

observe({
  data$df6220[1, 2:5] <- data$df6220.1_1[, list(
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
  data$df6220[2, 2:5] <- data$df6220.2_1[, list(
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

observe({ data$df6220[3, 2:5] <- data$df6220[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6220 <- renderUI({!any(is.na(input$dates6220))})

  output$table6220Item1 <- renderRHandsontable({
    rhandsontable(data$df6220, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6220 <- downloadHandler(
    filename = function() { "df6220.xlsx" },
    content = function(file) {
      write.xlsx(data$df6220, file)
  })

#**************************************

#6220.1

observeEvent(input$dates6220.1, {
    start <- ymd(input$dates6220.1[[1]])
    end <- ymd(input$dates6220.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6220.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6220.1[[1]]
      r$end <- input$dates6220.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6220.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6220_1Item1)) {
    data$df6220.1 <- hot_to_r(input$table6220_1Item1) 

    if (!any(is.na(input$dates6220.1)) && input$choices6220.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6220.1[1L])
      	to=as.Date(input$dates6220.1[2L])
      	if (from>to) to = from
      	selectdates6220.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.1_2 <- data$df6220.1[as.Date(data$df6220.1$"Дата операции") %in% selectdates6220.1_1, ]
    } else if (!is.null(input$text) && input$choices6220.1 == "Выбор по номеру первичного документа") {
      	data$df6220.1_2 <- data$df6220.1[data$df6220.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6220.1 == "Выбор по статье дохода") {
      	data$df6220.1_2 <- data$df6220.1[data$df6220.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6220.1) && !any(is.na(input$dates6220.1)) && !is.null(input$text) && input$choices6220.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6220.1[1L])
      	to=as.Date(input$dates6220.1[2L])
      	if (from>to) to = from
      	selectdates6220.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.1_2 <- data$df6220.1[as.Date(data$df6220.1$"Дата операции") %in% selectdates6220.1_2 & data$df6220.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6220.1) && !any(is.na(input$dates6220.1)) && !is.null(input$text) && input$choices6220.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6220.1[1L])
      	to=as.Date(input$dates6220.1[2L])
      	if (from>to) to = from
      	selectdates6220.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.1_2 <- data$df6220.1[as.Date(data$df6220.1$"Дата операции") %in% selectdates6220.1_3 & data$df6220.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6220.1_4 <- unique(data$df6220.1$"Дата операции")
        data$df6220.1_2 <- data$df6220.1[data$df6220.1$"Дата операции" %in% selectdates6220.1_4, ]
    }
}
})

  output$table6220_1Item1 <- renderRHandsontable({
    
   data$df6220.1[, `Сальдо конечное` := data$df6220.1[[6]] + data$df6220.1[[7]] - data$df6220.1[[8]]]

    rhandsontable(data$df6220.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6220.1 <- renderUI({
    if (input$choices6220.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6220.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6220.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6220.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6220.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6220.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6220.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6220.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6220.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6220.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6220.1 <- downloadHandler(
    filename = function() { "df6220.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6220.1, file)
  })

  output$download_df6220.1_2 <- downloadHandler(
    filename = function() { "df6220.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6220.1_2, file)
  })

#****************************************

#6220.2

observeEvent(input$dates6220.2, {
    start <- ymd(input$dates6220.2[[1]])
    end <- ymd(input$dates6220.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6220.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6220.2[[1]]
      r$end <- input$dates6220.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6220.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6220_2Item1)) {
    data$df6220.2 <- hot_to_r(input$table6220_2Item1) 

    if (!any(is.na(input$dates6220.2)) && input$choices6220.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6220.2[1L])
      	to=as.Date(input$dates6220.2[2L])
      	if (from>to) to = from
      	selectdates6220.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.2_2 <- data$df6220.2[as.Date(data$df6220.2$"Дата операции") %in% selectdates6220.2_1, ]
    } else if (!is.null(input$text) && input$choices6220.2 == "Выбор по номеру первичного документа") {
      	data$df6220.2_2 <- data$df6220.2[data$df6220.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6220.2 == "Выбор по статье дохода") {
      	data$df6220.2_2 <- data$df6220.2[data$df6220.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6220.2) && !any(is.na(input$dates6220.2)) && !is.null(input$text) && input$choices6220.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6220.2[1L])
      	to=as.Date(input$dates6220.2[2L])
      	if (from>to) to = from
      	selectdates6220.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.2_2 <- data$df6220.2[as.Date(data$df6220.2$"Дата операции") %in% selectdates6220.2_2 & data$df6220.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6220.2) && !any(is.na(input$dates6220.2)) && !is.null(input$text) && input$choices6220.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6220.2[1L])
      	to=as.Date(input$dates6220.2[2L])
      	if (from>to) to = from
      	selectdates6220.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6220.2_2 <- data$df6220.2[as.Date(data$df6220.2$"Дата операции") %in% selectdates6220.2_3 & data$df6220.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6220.2_4 <- unique(data$df6220.2$"Дата операции")
        data$df6220.2_2 <- data$df6220.2[data$df6220.2$"Дата операции" %in% selectdates6220.2_4, ]
    }
}
})

  output$table6220_2Item1 <- renderRHandsontable({
    
   data$df6220.2[, `Сальдо конечное` := data$df6220.2[[6]] + data$df6220.2[[7]] - data$df6220.2[[8]]]

    rhandsontable(data$df6220.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6220.2 <- renderUI({
    if (input$choices6220.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6220.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6220.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6220.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6220.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6220.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6220.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6220.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6220.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6220.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6220.2 <- downloadHandler(
    filename = function() { "df6220.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6220.2, file)
  })

  output$download_df6220.2_2 <- downloadHandler(
    filename = function() { "df6220.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6220.2_2, file)
  })

#*****************************************

#ОСВ: 6230

observeEvent(input$dates6230, {
    start <- ymd(input$dates6230[[1]])
    end <- ymd(input$dates6230[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6230", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6230[[1]]
      r$end <- input$dates6230[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6230",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6230))) {
      from=as.Date(input$dates6230[1L])
      to=as.Date(input$dates6230[2L])
      if (from>to) to = from
      selectdates6230.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6230.1_1 <- data$df6230.1[as.Date(data$df6230.1$`Дата операции`) %in% selectdates6230.1_5, ]
    } else {
      selectdates6230.1_6 <- unique(as.Date(data$df6230.1$`Дата операции`))
      data$df6230.1_1 <- data$df6230.1[data$df6230.1$`Дата операции` %in% selectdates6230.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6230))) {
      from=as.Date(input$dates6230[1L])
      to=as.Date(input$dates6230[2L])
      if (from>to) to = from
      selectdates6230.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6230.2_1 <- data$df6230.2[as.Date(data$df6230.2$`Дата операции`) %in% selectdates6230.2_5, ]
    } else {
      selectdates6230.2_6 <- unique(as.Date(data$df6230.2$`Дата операции`))
      data$df6230.2_1 <- data$df6230.2[data$df6230.2$`Дата операции` %in% selectdates6230.2_6, ]
    }
  })

observe({
  data$df6230[1, 2:5] <- data$df6230.1_1[, list(
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
  data$df6230[2, 2:5] <- data$df6230.2_1[, list(
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

observe({ data$df6230[3, 2:5] <- data$df6230[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6230 <- renderUI({!any(is.na(input$dates6230))})

  output$table6230Item1 <- renderRHandsontable({
    rhandsontable(data$df6230, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6230 <- downloadHandler(
    filename = function() { "df6230.xlsx" },
    content = function(file) {
      write.xlsx(data$df6230, file)
  })

#**************************************

#6230.1

observeEvent(input$dates6230.1, {
    start <- ymd(input$dates6230.1[[1]])
    end <- ymd(input$dates6230.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6230.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6230.1[[1]]
      r$end <- input$dates6230.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6230.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6230_1Item1)) {
    data$df6230.1 <- hot_to_r(input$table6230_1Item1) 

    if (!any(is.na(input$dates6230.1)) && input$choices6230.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6230.1[1L])
      	to=as.Date(input$dates6230.1[2L])
      	if (from>to) to = from
      	selectdates6230.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.1_2 <- data$df6230.1[as.Date(data$df6230.1$"Дата операции") %in% selectdates6230.1_1, ]
    } else if (!is.null(input$text) && input$choices6230.1 == "Выбор по номеру первичного документа") {
      	data$df6230.1_2 <- data$df6230.1[data$df6230.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6230.1 == "Выбор по статье дохода") {
      	data$df6230.1_2 <- data$df6230.1[data$df6230.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6230.1) && !any(is.na(input$dates6230.1)) && !is.null(input$text) && input$choices6230.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6230.1[1L])
      	to=as.Date(input$dates6230.1[2L])
      	if (from>to) to = from
      	selectdates6230.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.1_2 <- data$df6230.1[as.Date(data$df6230.1$"Дата операции") %in% selectdates6230.1_2 & data$df6230.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6230.1) && !any(is.na(input$dates6230.1)) && !is.null(input$text) && input$choices6230.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6230.1[1L])
      	to=as.Date(input$dates6230.1[2L])
      	if (from>to) to = from
      	selectdates6230.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.1_2 <- data$df6230.1[as.Date(data$df6230.1$"Дата операции") %in% selectdates6230.1_3 & data$df6230.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6230.1_4 <- unique(data$df6230.1$"Дата операции")
        data$df6230.1_2 <- data$df6230.1[data$df6230.1$"Дата операции" %in% selectdates6230.1_4, ]
    }
}
})

  output$table6230_1Item1 <- renderRHandsontable({
    
   data$df6230.1[, `Сальдо конечное` := data$df6230.1[[7]] + data$df6230.1[[8]] - data$df6230.1[[9]]]

    rhandsontable(data$df6230.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6230.1 <- renderUI({
    if (input$choices6230.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6230.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6230.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6230.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6230.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6230.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6230.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6230.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6230.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6230.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6230.1 <- downloadHandler(
    filename = function() { "df6230.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6230.1, file)
  })

  output$download_df6230.1_2 <- downloadHandler(
    filename = function() { "df6230.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6230.1_2, file)
  })

#****************************************

#6230.2

observeEvent(input$dates6230.2, {
    start <- ymd(input$dates6230.2[[1]])
    end <- ymd(input$dates6230.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6230.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6230.2[[1]]
      r$end <- input$dates6230.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6230.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6230_2Item1)) {
    data$df6230.2 <- hot_to_r(input$table6230_2Item1) 

    if (!any(is.na(input$dates6230.2)) && input$choices6230.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6230.2[1L])
      	to=as.Date(input$dates6230.2[2L])
      	if (from>to) to = from
      	selectdates6230.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.2_2 <- data$df6230.2[as.Date(data$df6230.2$"Дата операции") %in% selectdates6230.2_1, ]
    } else if (!is.null(input$text) && input$choices6230.2 == "Выбор по номеру первичного документа") {
      	data$df6230.2_2 <- data$df6230.2[data$df6230.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6230.2 == "Выбор по статье дохода") {
      	data$df6230.2_2 <- data$df6230.2[data$df6230.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6230.2) && !any(is.na(input$dates6230.2)) && !is.null(input$text) && input$choices6230.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6230.2[1L])
      	to=as.Date(input$dates6230.2[2L])
      	if (from>to) to = from
      	selectdates6230.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.2_2 <- data$df6230.2[as.Date(data$df6230.2$"Дата операции") %in% selectdates6230.2_2 & data$df6230.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6230.2) && !any(is.na(input$dates6230.2)) && !is.null(input$text) && input$choices6230.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6230.2[1L])
      	to=as.Date(input$dates6230.2[2L])
      	if (from>to) to = from
      	selectdates6230.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6230.2_2 <- data$df6230.2[as.Date(data$df6230.2$"Дата операции") %in% selectdates6230.2_3 & data$df6230.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6230.2_4 <- unique(data$df6230.2$"Дата операции")
        data$df6230.2_2 <- data$df6230.2[data$df6230.2$"Дата операции" %in% selectdates6230.2_4, ]
    }
}
})

  output$table6230_2Item1 <- renderRHandsontable({
    
   data$df6230.2[, `Сальдо конечное` := data$df6230.2[[7]] + data$df6230.2[[8]] - data$df6230.2[[9]]]

    rhandsontable(data$df6230.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6230.2 <- renderUI({
    if (input$choices6230.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6230.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6230.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6230.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6230.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6230.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6230.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6230.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6230.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6230.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6230.2 <- downloadHandler(
    filename = function() { "df6230.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6230.2, file)
  })

  output$download_df6230.2_2 <- downloadHandler(
    filename = function() { "df6230.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6230.2_2, file)
  })

#*****************************************

#ОСВ: 6240

observeEvent(input$dates6240, {
    start <- ymd(input$dates6240[[1]])
    end <- ymd(input$dates6240[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6240", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6240[[1]]
      r$end <- input$dates6240[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6240",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6240))) {
      from=as.Date(input$dates6240[1L])
      to=as.Date(input$dates6240[2L])
      if (from>to) to = from
      selectdates6240.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6240.1_1 <- data$df6240.1[as.Date(data$df6240.1$`Дата операции`) %in% selectdates6240.1_5, ]
    } else {
      selectdates6240.1_6 <- unique(as.Date(data$df6240.1$`Дата операции`))
      data$df6240.1_1 <- data$df6240.1[data$df6240.1$`Дата операции` %in% selectdates6240.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6240))) {
      from=as.Date(input$dates6240[1L])
      to=as.Date(input$dates6240[2L])
      if (from>to) to = from
      selectdates6240.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6240.2_1 <- data$df6240.2[as.Date(data$df6240.2$`Дата операции`) %in% selectdates6240.2_5, ]
    } else {
      selectdates6240.2_6 <- unique(as.Date(data$df6240.2$`Дата операции`))
      data$df6240.2_1 <- data$df6240.2[data$df6240.2$`Дата операции` %in% selectdates6240.2_6, ]
    }
  })

observe({
  data$df6240[1, 2:5] <- data$df6240.1_1[, list(
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
  data$df6240[2, 2:5] <- data$df6240.2_1[, list(
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

observe({ data$df6240[3, 2:5] <- data$df6240[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6240 <- renderUI({!any(is.na(input$dates6240))})

  output$table6240Item1 <- renderRHandsontable({
    rhandsontable(data$df6240, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6240 <- downloadHandler(
    filename = function() { "df6240.xlsx" },
    content = function(file) {
      write.xlsx(data$df6240, file)
  })

#**************************************

#6240.1

observeEvent(input$dates6240.1, {
    start <- ymd(input$dates6240.1[[1]])
    end <- ymd(input$dates6240.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6240.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6240.1[[1]]
      r$end <- input$dates6240.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6240.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6240_1Item1)) {
    data$df6240.1 <- hot_to_r(input$table6240_1Item1) 

    if (!any(is.na(input$dates6240.1)) && input$choices6240.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6240.1[1L])
      	to=as.Date(input$dates6240.1[2L])
      	if (from>to) to = from
      	selectdates6240.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.1_2 <- data$df6240.1[as.Date(data$df6240.1$"Дата операции") %in% selectdates6240.1_1, ]
    } else if (!is.null(input$text) && input$choices6240.1 == "Выбор по номеру первичного документа") {
      	data$df6240.1_2 <- data$df6240.1[data$df6240.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6240.1 == "Выбор по статье дохода") {
      	data$df6240.1_2 <- data$df6240.1[data$df6240.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6240.1) && !any(is.na(input$dates6240.1)) && !is.null(input$text) && input$choices6240.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6240.1[1L])
      	to=as.Date(input$dates6240.1[2L])
      	if (from>to) to = from
      	selectdates6240.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.1_2 <- data$df6240.1[as.Date(data$df6240.1$"Дата операции") %in% selectdates6240.1_2 & data$df6240.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6240.1) && !any(is.na(input$dates6240.1)) && !is.null(input$text) && input$choices6240.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6240.1[1L])
      	to=as.Date(input$dates6240.1[2L])
      	if (from>to) to = from
      	selectdates6240.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.1_2 <- data$df6240.1[as.Date(data$df6240.1$"Дата операции") %in% selectdates6240.1_3 & data$df6240.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6240.1_4 <- unique(data$df6240.1$"Дата операции")
        data$df6240.1_2 <- data$df6240.1[data$df6240.1$"Дата операции" %in% selectdates6240.1_4, ]
    }
}
})

  output$table6240_1Item1 <- renderRHandsontable({
    
   data$df6240.1[, `Сальдо конечное` := data$df6240.1[[6]] + data$df6240.1[[7]] - data$df6240.1[[8]]]

    rhandsontable(data$df6240.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6240.1 <- renderUI({
    if (input$choices6240.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6240.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6240.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6240.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6240.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6240.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6240.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6240.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6240.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6240.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6240.1 <- downloadHandler(
    filename = function() { "df6240.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6240.1, file)
  })

  output$download_df6240.1_2 <- downloadHandler(
    filename = function() { "df6240.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6240.1_2, file)
  })

#****************************************

#6240.2

observeEvent(input$dates6240.2, {
    start <- ymd(input$dates6240.2[[1]])
    end <- ymd(input$dates6240.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6240.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6240.2[[1]]
      r$end <- input$dates6240.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6240.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6240_2Item1)) {
    data$df6240.2 <- hot_to_r(input$table6240_2Item1) 

    if (!any(is.na(input$dates6240.2)) && input$choices6240.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6240.2[1L])
      	to=as.Date(input$dates6240.2[2L])
      	if (from>to) to = from
      	selectdates6240.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.2_2 <- data$df6240.2[as.Date(data$df6240.2$"Дата операции") %in% selectdates6240.2_1, ]
    } else if (!is.null(input$text) && input$choices6240.2 == "Выбор по номеру первичного документа") {
      	data$df6240.2_2 <- data$df6240.2[data$df6240.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6240.2 == "Выбор по статье дохода") {
      	data$df6240.2_2 <- data$df6240.2[data$df6240.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6240.2) && !any(is.na(input$dates6240.2)) && !is.null(input$text) && input$choices6240.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6240.2[1L])
      	to=as.Date(input$dates6240.2[2L])
      	if (from>to) to = from
      	selectdates6240.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.2_2 <- data$df6240.2[as.Date(data$df6240.2$"Дата операции") %in% selectdates6240.2_2 & data$df6240.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6240.2) && !any(is.na(input$dates6240.2)) && !is.null(input$text) && input$choices6240.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6240.2[1L])
      	to=as.Date(input$dates6240.2[2L])
      	if (from>to) to = from
      	selectdates6240.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6240.2_2 <- data$df6240.2[as.Date(data$df6240.2$"Дата операции") %in% selectdates6240.2_3 & data$df6240.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6240.2_4 <- unique(data$df6240.2$"Дата операции")
        data$df6240.2_2 <- data$df6240.2[data$df6240.2$"Дата операции" %in% selectdates6240.2_4, ]
    }
}
})

  output$table6240_2Item1 <- renderRHandsontable({
    
   data$df6240.2[, `Сальдо конечное` := data$df6240.2[[6]] + data$df6240.2[[7]] - data$df6240.2[[8]]]

    rhandsontable(data$df6240.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6240.2 <- renderUI({
    if (input$choices6240.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6240.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6240.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6240.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6240.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6240.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6240.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6240.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6240.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6240.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6240.2 <- downloadHandler(
    filename = function() { "df6240.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6240.2, file)
  })

  output$download_df6240.2_2 <- downloadHandler(
    filename = function() { "df6240.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6240.2_2, file)
  })

#*****************************************

#ОСВ: 6250

observeEvent(input$dates6250, {
    start <- ymd(input$dates6250[[1]])
    end <- ymd(input$dates6250[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6250", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6250[[1]]
      r$end <- input$dates6250[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6250",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6250))) {
      from=as.Date(input$dates6250[1L])
      to=as.Date(input$dates6250[2L])
      if (from>to) to = from
      selectdates6250.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6250.1_1 <- data$df6250.1[as.Date(data$df6250.1$`Дата операции`) %in% selectdates6250.1_5, ]
    } else {
      selectdates6250.1_6 <- unique(as.Date(data$df6250.1$`Дата операции`))
      data$df6250.1_1 <- data$df6250.1[data$df6250.1$`Дата операции` %in% selectdates6250.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6250))) {
      from=as.Date(input$dates6250[1L])
      to=as.Date(input$dates6250[2L])
      if (from>to) to = from
      selectdates6250.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6250.2_1 <- data$df6250.2[as.Date(data$df6250.2$`Дата операции`) %in% selectdates6250.2_5, ]
    } else {
      selectdates6250.2_6 <- unique(as.Date(data$df6250.2$`Дата операции`))
      data$df6250.2_1 <- data$df6250.2[data$df6250.2$`Дата операции` %in% selectdates6250.2_6, ]
    }
  })

observe({
  data$df6250[1, 2:5] <- data$df6250.1_1[, list(
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
  data$df6250[2, 2:5] <- data$df6250.2_1[, list(
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

observe({ data$df6250[3, 2:5] <- data$df6250[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6250 <- renderUI({!any(is.na(input$dates6250))})

  output$table6250Item1 <- renderRHandsontable({
    rhandsontable(data$df6250, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6250 <- downloadHandler(
    filename = function() { "df6250.xlsx" },
    content = function(file) {
      write.xlsx(data$df6250, file)
  })

#**************************************

#6250.1

observeEvent(input$dates6250.1, {
    start <- ymd(input$dates6250.1[[1]])
    end <- ymd(input$dates6250.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6250.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6250.1[[1]]
      r$end <- input$dates6250.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6250.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6250_1Item1)) {
    data$df6250.1 <- hot_to_r(input$table6250_1Item1) 

    if (!any(is.na(input$dates6250.1)) && input$choices6250.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6250.1[1L])
      	to=as.Date(input$dates6250.1[2L])
      	if (from>to) to = from
      	selectdates6250.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.1_2 <- data$df6250.1[as.Date(data$df6250.1$"Дата операции") %in% selectdates6250.1_1, ]
    } else if (!is.null(input$text) && input$choices6250.1 == "Выбор по номеру первичного документа") {
      	data$df6250.1_2 <- data$df6250.1[data$df6250.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6250.1 == "Выбор по статье дохода") {
      	data$df6250.1_2 <- data$df6250.1[data$df6250.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6250.1) && !any(is.na(input$dates6250.1)) && !is.null(input$text) && input$choices6250.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6250.1[1L])
      	to=as.Date(input$dates6250.1[2L])
      	if (from>to) to = from
      	selectdates6250.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.1_2 <- data$df6250.1[as.Date(data$df6250.1$"Дата операции") %in% selectdates6250.1_2 & data$df6250.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6250.1) && !any(is.na(input$dates6250.1)) && !is.null(input$text) && input$choices6250.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6250.1[1L])
      	to=as.Date(input$dates6250.1[2L])
      	if (from>to) to = from
      	selectdates6250.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.1_2 <- data$df6250.1[as.Date(data$df6250.1$"Дата операции") %in% selectdates6250.1_3 & data$df6250.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6250.1_4 <- unique(data$df6250.1$"Дата операции")
        data$df6250.1_2 <- data$df6250.1[data$df6250.1$"Дата операции" %in% selectdates6250.1_4, ]
    }
}
})

  output$table6250_1Item1 <- renderRHandsontable({
    
   data$df6250.1[, `Сальдо конечное` := data$df6250.1[[6]] + data$df6250.1[[7]] - data$df6250.1[[8]]]

    rhandsontable(data$df6250.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6250.1 <- renderUI({
    if (input$choices6250.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6250.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6250.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6250.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6250.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6250.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6250.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6250.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6250.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6250.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6250.1 <- downloadHandler(
    filename = function() { "df6250.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6250.1, file)
  })

  output$download_df6250.1_2 <- downloadHandler(
    filename = function() { "df6250.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6250.1_2, file)
  })

#****************************************

#6250.2

observeEvent(input$dates6250.2, {
    start <- ymd(input$dates6250.2[[1]])
    end <- ymd(input$dates6250.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6250.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6250.2[[1]]
      r$end <- input$dates6250.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6250.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6250_2Item1)) {
    data$df6250.2 <- hot_to_r(input$table6250_2Item1) 

    if (!any(is.na(input$dates6250.2)) && input$choices6250.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6250.2[1L])
      	to=as.Date(input$dates6250.2[2L])
      	if (from>to) to = from
      	selectdates6250.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.2_2 <- data$df6250.2[as.Date(data$df6250.2$"Дата операции") %in% selectdates6250.2_1, ]
    } else if (!is.null(input$text) && input$choices6250.2 == "Выбор по номеру первичного документа") {
      	data$df6250.2_2 <- data$df6250.2[data$df6250.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6250.2 == "Выбор по статье дохода") {
      	data$df6250.2_2 <- data$df6250.2[data$df6250.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6250.2) && !any(is.na(input$dates6250.2)) && !is.null(input$text) && input$choices6250.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6250.2[1L])
      	to=as.Date(input$dates6250.2[2L])
      	if (from>to) to = from
      	selectdates6250.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.2_2 <- data$df6250.2[as.Date(data$df6250.2$"Дата операции") %in% selectdates6250.2_2 & data$df6250.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6250.2) && !any(is.na(input$dates6250.2)) && !is.null(input$text) && input$choices6250.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6250.2[1L])
      	to=as.Date(input$dates6250.2[2L])
      	if (from>to) to = from
      	selectdates6250.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6250.2_2 <- data$df6250.2[as.Date(data$df6250.2$"Дата операции") %in% selectdates6250.2_3 & data$df6250.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6250.2_4 <- unique(data$df6250.2$"Дата операции")
        data$df6250.2_2 <- data$df6250.2[data$df6250.2$"Дата операции" %in% selectdates6250.2_4, ]
    }
}
})

  output$table6250_2Item1 <- renderRHandsontable({
    
   data$df6250.2[, `Сальдо конечное` := data$df6250.2[[6]] + data$df6250.2[[7]] - data$df6250.2[[8]]]

    rhandsontable(data$df6250.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6250.2 <- renderUI({
    if (input$choices6250.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6250.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6250.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6250.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6250.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6250.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6250.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6250.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6250.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6250.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6250.2 <- downloadHandler(
    filename = function() { "df6250.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6250.2, file)
  })

  output$download_df6250.2_2 <- downloadHandler(
    filename = function() { "df6250.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6250.2_2, file)
  })

#*****************************************

#ОСВ: 6260

observeEvent(input$dates6260, {
    start <- ymd(input$dates6260[[1]])
    end <- ymd(input$dates6260[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6260", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6260[[1]]
      r$end <- input$dates6260[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6260",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6261.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.1_3 <- data$df6261.1[as.Date(data$df6261.1$`Дата операции`) %in% selectdates6261.1_7, ]
    } else {
      selectdates6261.1_8 <- unique(as.Date(data$df6261.1$`Дата операции`))
      data$df6261.1_3 <- data$df6261.1[data$df6261.1$`Дата операции` %in% selectdates6261.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6261.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.2_3 <- data$df6261.2[as.Date(data$df6261.2$`Дата операции`) %in% selectdates6261.2_7, ]
    } else {
      selectdates6261.2_8 <- unique(as.Date(data$df6261.2$`Дата операции`))
      data$df6261.2_3 <- data$df6261.2[data$df6261.2$`Дата операции` %in% selectdates6261.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6262.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.1_3 <- data$df6262.1[as.Date(data$df6262.1$`Дата операции`) %in% selectdates6262.1_7, ]
    } else {
      selectdates6262.1_8 <- unique(as.Date(data$df6262.1$`Дата операции`))
      data$df6262.1_3 <- data$df6262.1[data$df6262.1$`Дата операции` %in% selectdates6262.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6262.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.2_3 <- data$df6262.2[as.Date(data$df6262.2$`Дата операции`) %in% selectdates6262.2_7, ]
    } else {
      selectdates6262.2_8 <- unique(as.Date(data$df6262.2$`Дата операции`))
      data$df6262.2_3 <- data$df6262.2[data$df6262.2$`Дата операции` %in% selectdates6262.2_8, ]
    }
  })


observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6263.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.1_3 <- data$df6263.1[as.Date(data$df6263.1$`Дата операции`) %in% selectdates6263.1_7, ]
    } else {
      selectdates6263.1_8 <- unique(as.Date(data$df6263.1$`Дата операции`))
      data$df6263.1_3 <- data$df6263.1[data$df6263.1$`Дата операции` %in% selectdates6263.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6263.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.2_3 <- data$df6263.2[as.Date(data$df6263.2$`Дата операции`) %in% selectdates6263.2_7, ]
    } else {
      selectdates6263.2_8 <- unique(as.Date(data$df6263.2$`Дата операции`))
      data$df6263.2_3 <- data$df6263.2[data$df6263.2$`Дата операции` %in% selectdates6263.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6264.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.1_3 <- data$df6264.1[as.Date(data$df6264.1$`Дата операции`) %in% selectdates6264.1_7, ]
    } else {
      selectdates6264.1_8 <- unique(as.Date(data$df6264.1$`Дата операции`))
      data$df6264.1_3 <- data$df6264.1[data$df6264.1$`Дата операции` %in% selectdates6264.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6260))) {
      from=as.Date(input$dates6260[1L])
      to=as.Date(input$dates6260[2L])
      if (from>to) to = from
      selectdates6264.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.2_3 <- data$df6264.2[as.Date(data$df6264.2$`Дата операции`) %in% selectdates6264.2_7, ]
    } else {
      selectdates6264.2_8 <- unique(as.Date(data$df6264.2$`Дата операции`))
      data$df6264.2_3 <- data$df6264.2[data$df6264.2$`Дата операции` %in% selectdates6264.2_8, ]
    }
  })

observe({
  data$df6261_2[1, 2:5] <- data$df6261.1_3[, list(
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
  data$df6261_2[2, 2:5] <- data$df6261.2_3[, list(
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

observe({ data$df6260[1, 2:5] <- data$df6261_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6262_2[1, 2:5] <- data$df6262.1_3[, list(
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
  data$df6262_2[2, 2:5] <- data$df6262.2_3[, list(
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

observe({ data$df6260[2, 2:5] <- data$df6262_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6263_2[1, 2:5] <- data$df6263.1_3[, list(
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
  data$df6263_2[2, 2:5] <- data$df6263.2_3[, list(
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

observe({ data$df6260[3, 2:5] <- data$df6263_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6264_2[1, 2:5] <- data$df6264.1_3[, list(
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
  data$df6264_2[2, 2:5] <- data$df6264.2_3[, list(
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

observe({ data$df6260[4, 2:5] <- data$df6264_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })


observe({ data$df6260[5, 2:5] <- data$df6260[, .SD[1:4, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6260 <- renderUI({!any(is.na(input$dates6260))})

  output$table6260Item1 <- renderRHandsontable({
    rhandsontable(data$df6260, colWidths = 150, height = 200, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 4) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6260 <- downloadHandler(
    filename = function() { "df6260.xlsx" },
    content = function(file) {
      write.xlsx(data$df6260, file)
  })

#*****************************************

#ОСВ: 6261

observeEvent(input$dates6261, {
    start <- ymd(input$dates6261[[1]])
    end <- ymd(input$dates6261[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6261", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6261[[1]]
      r$end <- input$dates6261[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6261",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6261))) {
      from=as.Date(input$dates6261[1L])
      to=as.Date(input$dates6261[2L])
      if (from>to) to = from
      selectdates6261.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.1_1 <- data$df6261.1[as.Date(data$df6261.1$`Дата операции`) %in% selectdates6261.1_5, ]
    } else {
      selectdates6261.1_6 <- unique(as.Date(data$df6261.1$`Дата операции`))
      data$df6261.1_1 <- data$df6261.1[data$df6261.1$`Дата операции` %in% selectdates6261.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6261))) {
      from=as.Date(input$dates6261[1L])
      to=as.Date(input$dates6261[2L])
      if (from>to) to = from
      selectdates6261.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6261.2_1 <- data$df6261.2[as.Date(data$df6261.2$`Дата операции`) %in% selectdates6261.2_5, ]
    } else {
      selectdates6261.2_6 <- unique(as.Date(data$df6261.2$`Дата операции`))
      data$df6261.2_1 <- data$df6261.2[data$df6261.2$`Дата операции` %in% selectdates6261.2_6, ]
    }
  })

observe({
  data$df6261[1, 2:5] <- data$df6261.1_1[, list(
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
  data$df6261[2, 2:5] <- data$df6261.2_1[, list(
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

observe({ data$df6261[3, 2:5] <- data$df6261[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6261 <- renderUI({!any(is.na(input$dates6261))})

  output$table6261Item1 <- renderRHandsontable({
    rhandsontable(data$df6261, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6261 <- downloadHandler(
    filename = function() { "df6261.xlsx" },
    content = function(file) {
      write.xlsx(data$df6261, file)
  })

#**************************************

#6261.1

observeEvent(input$dates6261.1, {
    start <- ymd(input$dates6261.1[[1]])
    end <- ymd(input$dates6261.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6261.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6261.1[[1]]
      r$end <- input$dates6261.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6261.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6261_1Item1)) {
    data$df6261.1 <- hot_to_r(input$table6261_1Item1) 

    if (!any(is.na(input$dates6261.1)) && input$choices6261.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6261.1[1L])
      	to=as.Date(input$dates6261.1[2L])
      	if (from>to) to = from
      	selectdates6261.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.1_2 <- data$df6261.1[as.Date(data$df6261.1$"Дата операции") %in% selectdates6261.1_1, ]
    } else if (!is.null(input$text) && input$choices6261.1 == "Выбор по номеру первичного документа") {
      	data$df6261.1_2 <- data$df6261.1[data$df6261.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6261.1 == "Выбор по статье дохода") {
      	data$df6261.1_2 <- data$df6261.1[data$df6261.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6261.1) && !any(is.na(input$dates6261.1)) && !is.null(input$text) && input$choices6261.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6261.1[1L])
      	to=as.Date(input$dates6261.1[2L])
      	if (from>to) to = from
      	selectdates6261.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.1_2 <- data$df6261.1[as.Date(data$df6261.1$"Дата операции") %in% selectdates6261.1_2 & data$df6261.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6261.1) && !any(is.na(input$dates6261.1)) && !is.null(input$text) && input$choices6261.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6261.1[1L])
      	to=as.Date(input$dates6261.1[2L])
      	if (from>to) to = from
      	selectdates6261.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.1_2 <- data$df6261.1[as.Date(data$df6261.1$"Дата операции") %in% selectdates6261.1_3 & data$df6261.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6261.1_4 <- unique(data$df6261.1$"Дата операции")
        data$df6261.1_2 <- data$df6261.1[data$df6261.1$"Дата операции" %in% selectdates6261.1_4, ]
    }
}
})

  output$table6261_1Item1 <- renderRHandsontable({
    
   data$df6261.1[, `Сальдо конечное` := data$df6261.1[[9]] + data$df6261.1[[10]] - data$df6261.1[[11]]]

    rhandsontable(data$df6261.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6261.1 <- renderUI({
    if (input$choices6261.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6261.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6261.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6261.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6261.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6261.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6261.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6261.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6261.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6261.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6261.1 <- downloadHandler(
    filename = function() { "df6261.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6261.1, file)
  })

  output$download_df6261.1_2 <- downloadHandler(
    filename = function() { "df6261.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6261.1_2, file)
  })

#****************************************

#6261.2

observeEvent(input$dates6261.2, {
    start <- ymd(input$dates6261.2[[1]])
    end <- ymd(input$dates6261.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6261.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6261.2[[1]]
      r$end <- input$dates6261.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6261.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6261_2Item1)) {
    data$df6261.2 <- hot_to_r(input$table6261_2Item1) 

    if (!any(is.na(input$dates6261.2)) && input$choices6261.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6261.2[1L])
      	to=as.Date(input$dates6261.2[2L])
      	if (from>to) to = from
      	selectdates6261.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.2_2 <- data$df6261.2[as.Date(data$df6261.2$"Дата операции") %in% selectdates6261.2_1, ]
    } else if (!is.null(input$text) && input$choices6261.2 == "Выбор по номеру первичного документа") {
      	data$df6261.2_2 <- data$df6261.2[data$df6261.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6261.2 == "Выбор по статье дохода") {
      	data$df6261.2_2 <- data$df6261.2[data$df6261.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6261.2) && !any(is.na(input$dates6261.2)) && !is.null(input$text) && input$choices6261.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6261.2[1L])
      	to=as.Date(input$dates6261.2[2L])
      	if (from>to) to = from
      	selectdates6261.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.2_2 <- data$df6261.2[as.Date(data$df6261.2$"Дата операции") %in% selectdates6261.2_2 & data$df6261.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6261.2) && !any(is.na(input$dates6261.2)) && !is.null(input$text) && input$choices6261.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6261.2[1L])
      	to=as.Date(input$dates6261.2[2L])
      	if (from>to) to = from
      	selectdates6261.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6261.2_2 <- data$df6261.2[as.Date(data$df6261.2$"Дата операции") %in% selectdates6261.2_3 & data$df6261.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6261.2_4 <- unique(data$df6261.2$"Дата операции")
        data$df6261.2_2 <- data$df6261.2[data$df6261.2$"Дата операции" %in% selectdates6261.2_4, ]
    }
}
})

  output$table6261_2Item1 <- renderRHandsontable({
    
   data$df6261.2[, `Сальдо конечное` := data$df6261.2[[9]] + data$df6261.2[[10]] - data$df6261.2[[11]]]

    rhandsontable(data$df6261.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6261.2 <- renderUI({
    if (input$choices6261.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6261.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6261.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6261.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6261.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6261.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6261.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6261.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6261.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6261.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6261.2 <- downloadHandler(
    filename = function() { "df6261.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6261.2, file)
  })

  output$download_df6261.2_2 <- downloadHandler(
    filename = function() { "df6261.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6261.2_2, file)
  })

#*****************************************

#ОСВ: 6262

observeEvent(input$dates6262, {
    start <- ymd(input$dates6262[[1]])
    end <- ymd(input$dates6262[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6262", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6262[[1]]
      r$end <- input$dates6262[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6262",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6262))) {
      from=as.Date(input$dates6262[1L])
      to=as.Date(input$dates6262[2L])
      if (from>to) to = from
      selectdates6262.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.1_1 <- data$df6262.1[as.Date(data$df6262.1$`Дата операции`) %in% selectdates6262.1_5, ]
    } else {
      selectdates6262.1_6 <- unique(as.Date(data$df6262.1$`Дата операции`))
      data$df6262.1_1 <- data$df6262.1[data$df6262.1$`Дата операции` %in% selectdates6262.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6262))) {
      from=as.Date(input$dates6262[1L])
      to=as.Date(input$dates6262[2L])
      if (from>to) to = from
      selectdates6262.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6262.2_1 <- data$df6262.2[as.Date(data$df6262.2$`Дата операции`) %in% selectdates6262.2_5, ]
    } else {
      selectdates6262.2_6 <- unique(as.Date(data$df6262.2$`Дата операции`))
      data$df6262.2_1 <- data$df6262.2[data$df6262.2$`Дата операции` %in% selectdates6262.2_6, ]
    }
  })

observe({
  data$df6262[1, 2:5] <- data$df6262.1_1[, list(
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
  data$df6262[2, 2:5] <- data$df6262.2_1[, list(
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

observe({ data$df6262[3, 2:5] <- data$df6262[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6262 <- renderUI({!any(is.na(input$dates6262))})

  output$table6262Item1 <- renderRHandsontable({
    rhandsontable(data$df6262, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 400) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6262 <- downloadHandler(
    filename = function() { "df6262.xlsx" },
    content = function(file) {
      write.xlsx(data$df6262, file)
  })

#**************************************

#6262.1

observeEvent(input$dates6262.1, {
    start <- ymd(input$dates6262.1[[1]])
    end <- ymd(input$dates6262.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6262.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6262.1[[1]]
      r$end <- input$dates6262.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6262.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6262_1Item1)) {
    data$df6262.1 <- hot_to_r(input$table6262_1Item1) 

    if (!any(is.na(input$dates6262.1)) && input$choices6262.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6262.1[1L])
      	to=as.Date(input$dates6262.1[2L])
      	if (from>to) to = from
      	selectdates6262.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.1_2 <- data$df6262.1[as.Date(data$df6262.1$"Дата операции") %in% selectdates6262.1_1, ]
    } else if (!is.null(input$text) && input$choices6262.1 == "Выбор по номеру первичного документа") {
      	data$df6262.1_2 <- data$df6262.1[data$df6262.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6262.1 == "Выбор по статье дохода") {
      	data$df6262.1_2 <- data$df6262.1[data$df6262.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6262.1) && !any(is.na(input$dates6262.1)) && !is.null(input$text) && input$choices6262.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6262.1[1L])
      	to=as.Date(input$dates6262.1[2L])
      	if (from>to) to = from
      	selectdates6262.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.1_2 <- data$df6262.1[as.Date(data$df6262.1$"Дата операции") %in% selectdates6262.1_2 & data$df6262.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6262.1) && !any(is.na(input$dates6262.1)) && !is.null(input$text) && input$choices6262.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6262.1[1L])
      	to=as.Date(input$dates6262.1[2L])
      	if (from>to) to = from
      	selectdates6262.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.1_2 <- data$df6262.1[as.Date(data$df6262.1$"Дата операции") %in% selectdates6262.1_3 & data$df6262.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6262.1_4 <- unique(data$df6262.1$"Дата операции")
        data$df6262.1_2 <- data$df6262.1[data$df6262.1$"Дата операции" %in% selectdates6262.1_4, ]
    }
}
})

  output$table6262_1Item1 <- renderRHandsontable({
    
   data$df6262.1[, `Сальдо конечное` := data$df6262.1[[9]] + data$df6262.1[[10]] - data$df6262.1[[11]]]

    rhandsontable(data$df6262.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6262.1 <- renderUI({
    if (input$choices6262.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6262.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6262.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6262.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6262.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6262.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6262.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6262.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6262.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6262.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6262.1 <- downloadHandler(
    filename = function() { "df6262.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6262.1, file)
  })

  output$download_df6262.1_2 <- downloadHandler(
    filename = function() { "df6262.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6262.1_2, file)
  })

#****************************************

#6262.2

observeEvent(input$dates6262.2, {
    start <- ymd(input$dates6262.2[[1]])
    end <- ymd(input$dates6262.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6262.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6262.2[[1]]
      r$end <- input$dates6262.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6262.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6262_2Item1)) {
    data$df6262.2 <- hot_to_r(input$table6262_2Item1) 

    if (!any(is.na(input$dates6262.2)) && input$choices6262.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6262.2[1L])
      	to=as.Date(input$dates6262.2[2L])
      	if (from>to) to = from
      	selectdates6262.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.2_2 <- data$df6262.2[as.Date(data$df6262.2$"Дата операции") %in% selectdates6262.2_1, ]
    } else if (!is.null(input$text) && input$choices6262.2 == "Выбор по номеру первичного документа") {
      	data$df6262.2_2 <- data$df6262.2[data$df6262.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6262.2 == "Выбор по статье дохода") {
      	data$df6262.2_2 <- data$df6262.2[data$df6262.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6262.2) && !any(is.na(input$dates6262.2)) && !is.null(input$text) && input$choices6262.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6262.2[1L])
      	to=as.Date(input$dates6262.2[2L])
      	if (from>to) to = from
      	selectdates6262.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.2_2 <- data$df6262.2[as.Date(data$df6262.2$"Дата операции") %in% selectdates6262.2_2 & data$df6262.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6262.2) && !any(is.na(input$dates6262.2)) && !is.null(input$text) && input$choices6262.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6262.2[1L])
      	to=as.Date(input$dates6262.2[2L])
      	if (from>to) to = from
      	selectdates6262.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6262.2_2 <- data$df6262.2[as.Date(data$df6262.2$"Дата операции") %in% selectdates6262.2_3 & data$df6262.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6262.2_4 <- unique(data$df6262.2$"Дата операции")
        data$df6262.2_2 <- data$df6262.2[data$df6262.2$"Дата операции" %in% selectdates6262.2_4, ]
    }
}
})

  output$table6262_2Item1 <- renderRHandsontable({
    
   data$df6262.2[, `Сальдо конечное` := data$df6262.2[[9]] + data$df6262.2[[10]] - data$df6262.2[[11]]]

    rhandsontable(data$df6262.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6262.2 <- renderUI({
    if (input$choices6262.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6262.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6262.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6262.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6262.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6262.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6262.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6262.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6262.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6262.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6262.2 <- downloadHandler(
    filename = function() { "df6262.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6262.2, file)
  })

  output$download_df6262.2_2 <- downloadHandler(
    filename = function() { "df6262.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6262.2_2, file)
  })

#*****************************************

#ОСВ: 6263

observeEvent(input$dates6263, {
    start <- ymd(input$dates6263[[1]])
    end <- ymd(input$dates6263[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6263", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6263[[1]]
      r$end <- input$dates6263[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6263",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6263))) {
      from=as.Date(input$dates6263[1L])
      to=as.Date(input$dates6263[2L])
      if (from>to) to = from
      selectdates6263.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.1_1 <- data$df6263.1[as.Date(data$df6263.1$`Дата операции`) %in% selectdates6263.1_5, ]
    } else {
      selectdates6263.1_6 <- unique(as.Date(data$df6263.1$`Дата операции`))
      data$df6263.1_1 <- data$df6263.1[data$df6263.1$`Дата операции` %in% selectdates6263.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6263))) {
      from=as.Date(input$dates6263[1L])
      to=as.Date(input$dates6263[2L])
      if (from>to) to = from
      selectdates6263.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6263.2_1 <- data$df6263.2[as.Date(data$df6263.2$`Дата операции`) %in% selectdates6263.2_5, ]
    } else {
      selectdates6263.2_6 <- unique(as.Date(data$df6263.2$`Дата операции`))
      data$df6263.2_1 <- data$df6263.2[data$df6263.2$`Дата операции` %in% selectdates6263.2_6, ]
    }
  })

observe({
  data$df6263[1, 2:5] <- data$df6263.1_1[, list(
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
  data$df6263[2, 2:5] <- data$df6263.2_1[, list(
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

observe({ data$df6263[3, 2:5] <- data$df6263[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6263 <- renderUI({!any(is.na(input$dates6263))})

  output$table6263Item1 <- renderRHandsontable({
    rhandsontable(data$df6263, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6263 <- downloadHandler(
    filename = function() { "df6263.xlsx" },
    content = function(file) {
      write.xlsx(data$df6263, file)
  })

#**************************************

#6263.1

observeEvent(input$dates6263.1, {
    start <- ymd(input$dates6263.1[[1]])
    end <- ymd(input$dates6263.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6263.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6263.1[[1]]
      r$end <- input$dates6263.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6263.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6263_1Item1)) {
    data$df6263.1 <- hot_to_r(input$table6263_1Item1) 

    if (!any(is.na(input$dates6263.1)) && input$choices6263.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6263.1[1L])
      	to=as.Date(input$dates6263.1[2L])
      	if (from>to) to = from
      	selectdates6263.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.1_2 <- data$df6263.1[as.Date(data$df6263.1$"Дата операции") %in% selectdates6263.1_1, ]
    } else if (!is.null(input$text) && input$choices6263.1 == "Выбор по номеру первичного документа") {
      	data$df6263.1_2 <- data$df6263.1[data$df6263.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6263.1 == "Выбор по статье дохода") {
      	data$df6263.1_2 <- data$df6263.1[data$df6263.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6263.1) && !any(is.na(input$dates6263.1)) && !is.null(input$text) && input$choices6263.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6263.1[1L])
      	to=as.Date(input$dates6263.1[2L])
      	if (from>to) to = from
      	selectdates6263.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.1_2 <- data$df6263.1[as.Date(data$df6263.1$"Дата операции") %in% selectdates6263.1_2 & data$df6263.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6263.1) && !any(is.na(input$dates6263.1)) && !is.null(input$text) && input$choices6263.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6263.1[1L])
      	to=as.Date(input$dates6263.1[2L])
      	if (from>to) to = from
      	selectdates6263.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.1_2 <- data$df6263.1[as.Date(data$df6263.1$"Дата операции") %in% selectdates6263.1_3 & data$df6263.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6263.1_4 <- unique(data$df6263.1$"Дата операции")
        data$df6263.1_2 <- data$df6263.1[data$df6263.1$"Дата операции" %in% selectdates6263.1_4, ]
    }
}
})

  output$table6263_1Item1 <- renderRHandsontable({
    
   data$df6263.1[, `Сальдо конечное` := data$df6263.1[[9]] + data$df6263.1[[10]] - data$df6263.1[[11]]]

    rhandsontable(data$df6263.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6263.1 <- renderUI({
    if (input$choices6263.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6263.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6263.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6263.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6263.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6263.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6263.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6263.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6263.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6263.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6263.1 <- downloadHandler(
    filename = function() { "df6263.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6263.1, file)
  })

  output$download_df6263.1_2 <- downloadHandler(
    filename = function() { "df6263.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6263.1_2, file)
  })

#****************************************

#6263.2

observeEvent(input$dates6263.2, {
    start <- ymd(input$dates6263.2[[1]])
    end <- ymd(input$dates6263.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6263.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6263.2[[1]]
      r$end <- input$dates6263.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6263.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6263_2Item1)) {
    data$df6263.2 <- hot_to_r(input$table6263_2Item1) 

    if (!any(is.na(input$dates6263.2)) && input$choices6263.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6263.2[1L])
      	to=as.Date(input$dates6263.2[2L])
      	if (from>to) to = from
      	selectdates6263.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.2_2 <- data$df6263.2[as.Date(data$df6263.2$"Дата операции") %in% selectdates6263.2_1, ]
    } else if (!is.null(input$text) && input$choices6263.2 == "Выбор по номеру первичного документа") {
      	data$df6263.2_2 <- data$df6263.2[data$df6263.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6263.2 == "Выбор по статье дохода") {
      	data$df6263.2_2 <- data$df6263.2[data$df6263.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6263.2) && !any(is.na(input$dates6263.2)) && !is.null(input$text) && input$choices6263.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6263.2[1L])
      	to=as.Date(input$dates6263.2[2L])
      	if (from>to) to = from
      	selectdates6263.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.2_2 <- data$df6263.2[as.Date(data$df6263.2$"Дата операции") %in% selectdates6263.2_2 & data$df6263.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6263.2) && !any(is.na(input$dates6263.2)) && !is.null(input$text) && input$choices6263.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6263.2[1L])
      	to=as.Date(input$dates6263.2[2L])
      	if (from>to) to = from
      	selectdates6263.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6263.2_2 <- data$df6263.2[as.Date(data$df6263.2$"Дата операции") %in% selectdates6263.2_3 & data$df6263.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6263.2_4 <- unique(data$df6263.2$"Дата операции")
        data$df6263.2_2 <- data$df6263.2[data$df6263.2$"Дата операции" %in% selectdates6263.2_4, ]
    }
}
})

  output$table6263_2Item1 <- renderRHandsontable({
    
   data$df6263.2[, `Сальдо конечное` := data$df6263.2[[9]] + data$df6263.2[[10]] - data$df6263.2[[11]]]

    rhandsontable(data$df6263.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6263.2 <- renderUI({
    if (input$choices6263.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6263.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6263.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6263.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6263.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6263.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6263.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6263.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6263.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6263.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6263.2 <- downloadHandler(
    filename = function() { "df6263.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6263.2, file)
  })

  output$download_df6263.2_2 <- downloadHandler(
    filename = function() { "df6263.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6263.2_2, file)
  })

#*****************************************

#ОСВ: 6264

observeEvent(input$dates6264, {
    start <- ymd(input$dates6264[[1]])
    end <- ymd(input$dates6264[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6264", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6264[[1]]
      r$end <- input$dates6264[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6264",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6264))) {
      from=as.Date(input$dates6264[1L])
      to=as.Date(input$dates6264[2L])
      if (from>to) to = from
      selectdates6264.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.1_1 <- data$df6264.1[as.Date(data$df6264.1$`Дата операции`) %in% selectdates6264.1_5, ]
    } else {
      selectdates6264.1_6 <- unique(as.Date(data$df6264.1$`Дата операции`))
      data$df6264.1_1 <- data$df6264.1[data$df6264.1$`Дата операции` %in% selectdates6264.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6264))) {
      from=as.Date(input$dates6264[1L])
      to=as.Date(input$dates6264[2L])
      if (from>to) to = from
      selectdates6264.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6264.2_1 <- data$df6264.2[as.Date(data$df6264.2$`Дата операции`) %in% selectdates6264.2_5, ]
    } else {
      selectdates6264.2_6 <- unique(as.Date(data$df6264.2$`Дата операции`))
      data$df6264.2_1 <- data$df6264.2[data$df6264.2$`Дата операции` %in% selectdates6264.2_6, ]
    }
  })

observe({
  data$df6264[1, 2:5] <- data$df6264.1_1[, list(
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
  data$df6264[2, 2:5] <- data$df6264.2_1[, list(
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

observe({ data$df6264[3, 2:5] <- data$df6264[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6264 <- renderUI({!any(is.na(input$dates6264))})

  output$table6264Item1 <- renderRHandsontable({
    rhandsontable(data$df6264, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6264 <- downloadHandler(
    filename = function() { "df6264.xlsx" },
    content = function(file) {
      write.xlsx(data$df6264, file)
  })

#**************************************

#6264.1

observeEvent(input$dates6264.1, {
    start <- ymd(input$dates6264.1[[1]])
    end <- ymd(input$dates6264.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6264.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6264.1[[1]]
      r$end <- input$dates6264.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6264.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6264_1Item1)) {
    data$df6264.1 <- hot_to_r(input$table6264_1Item1) 

    if (!any(is.na(input$dates6264.1)) && input$choices6264.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6264.1[1L])
      	to=as.Date(input$dates6264.1[2L])
      	if (from>to) to = from
      	selectdates6264.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.1_2 <- data$df6264.1[as.Date(data$df6264.1$"Дата операции") %in% selectdates6264.1_1, ]
    } else if (!is.null(input$text) && input$choices6264.1 == "Выбор по номеру первичного документа") {
      	data$df6264.1_2 <- data$df6264.1[data$df6264.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6264.1 == "Выбор по статье дохода") {
      	data$df6264.1_2 <- data$df6264.1[data$df6264.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6264.1) && !any(is.na(input$dates6264.1)) && !is.null(input$text) && input$choices6264.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6264.1[1L])
      	to=as.Date(input$dates6264.1[2L])
      	if (from>to) to = from
      	selectdates6264.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.1_2 <- data$df6264.1[as.Date(data$df6264.1$"Дата операции") %in% selectdates6264.1_2 & data$df6264.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6264.1) && !any(is.na(input$dates6264.1)) && !is.null(input$text) && input$choices6264.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6264.1[1L])
      	to=as.Date(input$dates6264.1[2L])
      	if (from>to) to = from
      	selectdates6264.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.1_2 <- data$df6264.1[as.Date(data$df6264.1$"Дата операции") %in% selectdates6264.1_3 & data$df6264.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6264.1_4 <- unique(data$df6264.1$"Дата операции")
        data$df6264.1_2 <- data$df6264.1[data$df6264.1$"Дата операции" %in% selectdates6264.1_4, ]
    }
}
})

  output$table6264_1Item1 <- renderRHandsontable({
    
   data$df6264.1[, `Сальдо конечное` := data$df6264.1[[9]] + data$df6264.1[[10]] - data$df6264.1[[11]]]

    rhandsontable(data$df6264.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6264.1 <- renderUI({
    if (input$choices6264.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6264.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6264.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6264.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6264.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6264.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6264.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6264.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6264.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6264.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6264.1 <- downloadHandler(
    filename = function() { "df6264.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6264.1, file)
  })

  output$download_df6264.1_2 <- downloadHandler(
    filename = function() { "df6264.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6264.1_2, file)
  })

#****************************************

#6264.2

observeEvent(input$dates6264.2, {
    start <- ymd(input$dates6264.2[[1]])
    end <- ymd(input$dates6264.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6264.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6264.2[[1]]
      r$end <- input$dates6264.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6264.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6264_2Item1)) {
    data$df6264.2 <- hot_to_r(input$table6264_2Item1) 

    if (!any(is.na(input$dates6264.2)) && input$choices6264.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6264.2[1L])
      	to=as.Date(input$dates6264.2[2L])
      	if (from>to) to = from
      	selectdates6264.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.2_2 <- data$df6264.2[as.Date(data$df6264.2$"Дата операции") %in% selectdates6264.2_1, ]
    } else if (!is.null(input$text) && input$choices6264.2 == "Выбор по номеру первичного документа") {
      	data$df6264.2_2 <- data$df6264.2[data$df6264.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6264.2 == "Выбор по статье дохода") {
      	data$df6264.2_2 <- data$df6264.2[data$df6264.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6264.2) && !any(is.na(input$dates6264.2)) && !is.null(input$text) && input$choices6264.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6264.2[1L])
      	to=as.Date(input$dates6264.2[2L])
      	if (from>to) to = from
      	selectdates6264.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.2_2 <- data$df6264.2[as.Date(data$df6264.2$"Дата операции") %in% selectdates6264.2_2 & data$df6264.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6264.2) && !any(is.na(input$dates6264.2)) && !is.null(input$text) && input$choices6264.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6264.2[1L])
      	to=as.Date(input$dates6264.2[2L])
      	if (from>to) to = from
      	selectdates6264.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6264.2_2 <- data$df6264.2[as.Date(data$df6264.2$"Дата операции") %in% selectdates6264.2_3 & data$df6264.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6264.2_4 <- unique(data$df6264.2$"Дата операции")
        data$df6264.2_2 <- data$df6264.2[data$df6264.2$"Дата операции" %in% selectdates6264.2_4, ]
    }
}
})

  output$table6264_2Item1 <- renderRHandsontable({
    
   data$df6264.2[, `Сальдо конечное` := data$df6264.2[[9]] + data$df6264.2[[10]] - data$df6264.2[[11]]]

    rhandsontable(data$df6264.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6264.2 <- renderUI({
    if (input$choices6264.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6264.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6264.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6264.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6264.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6264.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6264.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6264.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6264.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6264.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6264.2 <- downloadHandler(
    filename = function() { "df6264.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6264.2, file)
  })

  output$download_df6264.2_2 <- downloadHandler(
    filename = function() { "df6264.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6264.2_2, file)
  })

#*****************************************

#ОСВ: 6270

observeEvent(input$dates6270, {
    start <- ymd(input$dates6270[[1]])
    end <- ymd(input$dates6270[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6270", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6270[[1]]
      r$end <- input$dates6270[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6270",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6271.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.1_3 <- data$df6271.1[as.Date(data$df6271.1$`Дата операции`) %in% selectdates6271.1_7, ]
    } else {
      selectdates6271.1_8 <- unique(as.Date(data$df6271.1$`Дата операции`))
      data$df6271.1_3 <- data$df6271.1[data$df6271.1$`Дата операции` %in% selectdates6271.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6271.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.2_3 <- data$df6271.2[as.Date(data$df6271.2$`Дата операции`) %in% selectdates6271.2_7, ]
    } else {
      selectdates6271.2_8 <- unique(as.Date(data$df6271.2$`Дата операции`))
      data$df6271.2_3 <- data$df6271.2[data$df6271.2$`Дата операции` %in% selectdates6271.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6272.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.1_3 <- data$df6272.1[as.Date(data$df6272.1$`Дата операции`) %in% selectdates6272.1_7, ]
    } else {
      selectdates6272.1_8 <- unique(as.Date(data$df6272.1$`Дата операции`))
      data$df6272.1_3 <- data$df6272.1[data$df6272.1$`Дата операции` %in% selectdates6272.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6272.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.2_3 <- data$df6272.2[as.Date(data$df6272.2$`Дата операции`) %in% selectdates6272.2_7, ]
    } else {
      selectdates6272.2_8 <- unique(as.Date(data$df6272.2$`Дата операции`))
      data$df6272.2_3 <- data$df6272.2[data$df6272.2$`Дата операции` %in% selectdates6272.2_8, ]
    }
  })


observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6273_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6273_1 <- data$df6273[as.Date(data$df6273$`Дата операции`) %in% selectdates6273_5, ]
    } else {
      selectdates6273_6 <- unique(as.Date(data$df6273$`Дата операции`))
      data$df6273_1 <- data$df6273[data$df6273$`Дата операции` %in% selectdates6273_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6274_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6274_1 <- data$df6274[as.Date(data$df6274$`Дата операции`) %in% selectdates6274_5, ]
    } else {
      selectdates6274_6 <- unique(as.Date(data$df6274$`Дата операции`))
      data$df6274_1 <- data$df6274[data$df6274$`Дата операции` %in% selectdates6274_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6275.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.1_3 <- data$df6275.1[as.Date(data$df6275.1$`Дата операции`) %in% selectdates6275.1_7, ]
    } else {
      selectdates6275.1_8 <- unique(as.Date(data$df6275.1$`Дата операции`))
      data$df6275.1_3 <- data$df6275.1[data$df6275.1$`Дата операции` %in% selectdates6275.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6275.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.2_3 <- data$df6275.2[as.Date(data$df6275.2$`Дата операции`) %in% selectdates6275.2_7, ]
    } else {
      selectdates6275.2_8 <- unique(as.Date(data$df6275.2$`Дата операции`))
      data$df6275.2_3 <- data$df6275.2[data$df6275.2$`Дата операции` %in% selectdates6275.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6276.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.1_3 <- data$df6276.1[as.Date(data$df6276.1$`Дата операции`) %in% selectdates6276.1_7, ]
    } else {
      selectdates6276.1_8 <- unique(as.Date(data$df6276.1$`Дата операции`))
      data$df6276.1_3 <- data$df6276.1[data$df6276.1$`Дата операции` %in% selectdates6276.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6276.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.2_3 <- data$df6276.2[as.Date(data$df6276.2$`Дата операции`) %in% selectdates6276.2_7, ]
    } else {
      selectdates6276.2_8 <- unique(as.Date(data$df6276.2$`Дата операции`))
      data$df6276.2_3 <- data$df6276.2[data$df6276.2$`Дата операции` %in% selectdates6276.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6277.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.1_3 <- data$df6277.1[as.Date(data$df6277.1$`Дата операции`) %in% selectdates6277.1_7, ]
    } else {
      selectdates6277.1_8 <- unique(as.Date(data$df6277.1$`Дата операции`))
      data$df6277.1_3 <- data$df6277.1[data$df6277.1$`Дата операции` %in% selectdates6277.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6270))) {
      from=as.Date(input$dates6270[1L])
      to=as.Date(input$dates6270[2L])
      if (from>to) to = from
      selectdates6277.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.2_3 <- data$df6277.2[as.Date(data$df6277.2$`Дата операции`) %in% selectdates6277.2_7, ]
    } else {
      selectdates6277.2_8 <- unique(as.Date(data$df6277.2$`Дата операции`))
      data$df6277.2_3 <- data$df6277.2[data$df6277.2$`Дата операции` %in% selectdates6277.2_8, ]
    }
  })

observe({
  data$df6271_2[1, 2:5] <- data$df6271.1_3[, list(
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
  data$df6271_2[2, 2:5] <- data$df6271.2_3[, list(
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

observe({ data$df6270[1, 2:5] <- data$df6271_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6272_2[1, 2:5] <- data$df6272.1_3[, list(
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
  data$df6272_2[2, 2:5] <- data$df6272.2_3[, list(
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

observe({ data$df6270[2, 2:5] <- data$df6272_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6270[3, 2:5] <- data$df6273_1[, list(
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
  data$df6270[4, 2:5] <- data$df6274_1[, list(
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
  data$df6275_2[1, 2:5] <- data$df6275.1_3[, list(
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
  data$df6275_2[2, 2:5] <- data$df6275.2_3[, list(
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

observe({ data$df6270[5, 2:5] <- data$df6275_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6276_2[1, 2:5] <- data$df6276.1_3[, list(
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
  data$df6276_2[2, 2:5] <- data$df6276.2_3[, list(
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

observe({ data$df6270[6, 2:5] <- data$df6276_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6277_2[1, 2:5] <- data$df6277.1_3[, list(
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
  data$df6277_2[2, 2:5] <- data$df6277.2_3[, list(
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

observe({ data$df6270[7, 2:5] <- data$df6277_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6270[8, 2:5] <- data$df6270[, .SD[1:7, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6270 <- renderUI({!any(is.na(input$dates6270))})

  output$table6270Item1 <- renderRHandsontable({
    rhandsontable(data$df6270, colWidths = 150, height = 400, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 7) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6270 <- downloadHandler(
    filename = function() { "df6270.xlsx" },
    content = function(file) {
      write.xlsx(data$df6270, file)
  })

#*****************************************

#ОСВ: 6271

observeEvent(input$dates6271, {
    start <- ymd(input$dates6271[[1]])
    end <- ymd(input$dates6271[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6271", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6271[[1]]
      r$end <- input$dates6271[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6271",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6271))) {
      from=as.Date(input$dates6271[1L])
      to=as.Date(input$dates6271[2L])
      if (from>to) to = from
      selectdates6271.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.1_1 <- data$df6271.1[as.Date(data$df6271.1$`Дата операции`) %in% selectdates6271.1_5, ]
    } else {
      selectdates6271.1_6 <- unique(as.Date(data$df6271.1$`Дата операции`))
      data$df6271.1_1 <- data$df6271.1[data$df6271.1$`Дата операции` %in% selectdates6271.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6271))) {
      from=as.Date(input$dates6271[1L])
      to=as.Date(input$dates6271[2L])
      if (from>to) to = from
      selectdates6271.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6271.2_1 <- data$df6271.2[as.Date(data$df6271.2$`Дата операции`) %in% selectdates6271.2_5, ]
    } else {
      selectdates6271.2_6 <- unique(as.Date(data$df6271.2$`Дата операции`))
      data$df6271.2_1 <- data$df6271.2[data$df6271.2$`Дата операции` %in% selectdates6271.2_6, ]
    }
  })

observe({
  data$df6271[1, 2:5] <- data$df6271.1_1[, list(
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
  data$df6271[2, 2:5] <- data$df6271.2_1[, list(
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

observe({ data$df6271[3, 2:5] <- data$df6271[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6271 <- renderUI({!any(is.na(input$dates6271))})

  output$table6271Item1 <- renderRHandsontable({
    rhandsontable(data$df6271, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6271 <- downloadHandler(
    filename = function() { "df6271.xlsx" },
    content = function(file) {
      write.xlsx(data$df6271, file)
  })

#**************************************

#6271.1

observeEvent(input$dates6271.1, {
    start <- ymd(input$dates6271.1[[1]])
    end <- ymd(input$dates6271.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6271.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6271.1[[1]]
      r$end <- input$dates6271.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6271.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6271_1Item1)) {
    data$df6271.1 <- hot_to_r(input$table6271_1Item1) 

    if (!any(is.na(input$dates6271.1)) && input$choices6271.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6271.1[1L])
      	to=as.Date(input$dates6271.1[2L])
      	if (from>to) to = from
      	selectdates6271.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.1_2 <- data$df6271.1[as.Date(data$df6271.1$"Дата операции") %in% selectdates6271.1_1, ]
    } else if (!is.null(input$text) && input$choices6271.1 == "Выбор по номеру первичного документа") {
      	data$df6271.1_2 <- data$df6271.1[data$df6271.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6271.1 == "Выбор по статье дохода") {
      	data$df6271.1_2 <- data$df6271.1[data$df6271.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6271.1) && !any(is.na(input$dates6271.1)) && !is.null(input$text) && input$choices6271.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6271.1[1L])
      	to=as.Date(input$dates6271.1[2L])
      	if (from>to) to = from
      	selectdates6271.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.1_2 <- data$df6271.1[as.Date(data$df6271.1$"Дата операции") %in% selectdates6271.1_2 & data$df6271.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6271.1) && !any(is.na(input$dates6271.1)) && !is.null(input$text) && input$choices6271.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6271.1[1L])
      	to=as.Date(input$dates6271.1[2L])
      	if (from>to) to = from
      	selectdates6271.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.1_2 <- data$df6271.1[as.Date(data$df6271.1$"Дата операции") %in% selectdates6271.1_3 & data$df6271.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6271.1_4 <- unique(data$df6271.1$"Дата операции")
        data$df6271.1_2 <- data$df6271.1[data$df6271.1$"Дата операции" %in% selectdates6271.1_4, ]
    }
}
})

  output$table6271_1Item1 <- renderRHandsontable({
    
   data$df6271.1[, `Сальдо конечное` := data$df6271.1[[6]] + data$df6271.1[[7]] - data$df6271.1[[8]]]

    rhandsontable(data$df6271.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6271.1 <- renderUI({
    if (input$choices6271.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6271.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6271.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6271.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6271.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6271.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6271.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6271.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6271.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6271.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6271.1 <- downloadHandler(
    filename = function() { "df6271.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6271.1, file)
  })

  output$download_df6271.1_2 <- downloadHandler(
    filename = function() { "df6271.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6271.1_2, file)
  })

#****************************************

#6270

#****************************************

#6271.2

observeEvent(input$dates6271.2, {
    start <- ymd(input$dates6271.2[[1]])
    end <- ymd(input$dates6271.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6271.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6271.2[[1]]
      r$end <- input$dates6271.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6271.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6271_2Item1)) {
    data$df6271.2 <- hot_to_r(input$table6271_2Item1) 

    if (!any(is.na(input$dates6271.2)) && input$choices6271.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6271.2[1L])
      	to=as.Date(input$dates6271.2[2L])
      	if (from>to) to = from
      	selectdates6271.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.2_2 <- data$df6271.2[as.Date(data$df6271.2$"Дата операции") %in% selectdates6271.2_1, ]
    } else if (!is.null(input$text) && input$choices6271.2 == "Выбор по номеру первичного документа") {
      	data$df6271.2_2 <- data$df6271.2[data$df6271.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6271.2 == "Выбор по статье дохода") {
      	data$df6271.2_2 <- data$df6271.2[data$df6271.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6271.2) && !any(is.na(input$dates6271.2)) && !is.null(input$text) && input$choices6271.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6271.2[1L])
      	to=as.Date(input$dates6271.2[2L])
      	if (from>to) to = from
      	selectdates6271.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.2_2 <- data$df6271.2[as.Date(data$df6271.2$"Дата операции") %in% selectdates6271.2_2 & data$df6271.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6271.2) && !any(is.na(input$dates6271.2)) && !is.null(input$text) && input$choices6271.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6271.2[1L])
      	to=as.Date(input$dates6271.2[2L])
      	if (from>to) to = from
      	selectdates6271.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6271.2_2 <- data$df6271.2[as.Date(data$df6271.2$"Дата операции") %in% selectdates6271.2_3 & data$df6271.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6271.2_4 <- unique(data$df6271.2$"Дата операции")
        data$df6271.2_2 <- data$df6271.2[data$df6271.2$"Дата операции" %in% selectdates6271.2_4, ]
    }
}
})

  output$table6271_2Item1 <- renderRHandsontable({
    
   data$df6271.2[, `Сальдо конечное` := data$df6271.2[[6]] + data$df6271.2[[7]] - data$df6271.2[[8]]]

    rhandsontable(data$df6271.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6271.2 <- renderUI({
    if (input$choices6271.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6271.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6271.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6271.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6271.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6271.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6271.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6271.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6271.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6271.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6271.2 <- downloadHandler(
    filename = function() { "df6271.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6271.2, file)
  })

  output$download_df6271.2_2 <- downloadHandler(
    filename = function() { "df6271.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6271.2_2, file)
  })

#*****************************************

#ОСВ: 6272

observeEvent(input$dates6272, {
    start <- ymd(input$dates6272[[1]])
    end <- ymd(input$dates6272[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6272", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6272[[1]]
      r$end <- input$dates6272[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6272",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6272))) {
      from=as.Date(input$dates6272[1L])
      to=as.Date(input$dates6272[2L])
      if (from>to) to = from
      selectdates6272.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.1_1 <- data$df6272.1[as.Date(data$df6272.1$`Дата операции`) %in% selectdates6272.1_5, ]
    } else {
      selectdates6272.1_6 <- unique(as.Date(data$df6272.1$`Дата операции`))
      data$df6272.1_1 <- data$df6272.1[data$df6272.1$`Дата операции` %in% selectdates6272.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6272))) {
      from=as.Date(input$dates6272[1L])
      to=as.Date(input$dates6272[2L])
      if (from>to) to = from
      selectdates6272.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6272.2_1 <- data$df6272.2[as.Date(data$df6272.2$`Дата операции`) %in% selectdates6272.2_5, ]
    } else {
      selectdates6272.2_6 <- unique(as.Date(data$df6272.2$`Дата операции`))
      data$df6272.2_1 <- data$df6272.2[data$df6272.2$`Дата операции` %in% selectdates6272.2_6, ]
    }
  })

observe({
  data$df6272[1, 2:5] <- data$df6272.1_1[, list(
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
  data$df6272[2, 2:5] <- data$df6272.2_1[, list(
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

observe({ data$df6272[3, 2:5] <- data$df6272[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6272 <- renderUI({!any(is.na(input$dates6272))})

  output$table6272Item1 <- renderRHandsontable({
    rhandsontable(data$df6272, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6272 <- downloadHandler(
    filename = function() { "df6272.xlsx" },
    content = function(file) {
      write.xlsx(data$df6272, file)
  })

#**************************************

#6272.1

observeEvent(input$dates6272.1, {
    start <- ymd(input$dates6272.1[[1]])
    end <- ymd(input$dates6272.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6272.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6272.1[[1]]
      r$end <- input$dates6272.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6272.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6272_1Item1)) {
    data$df6272.1 <- hot_to_r(input$table6272_1Item1) 

    if (!any(is.na(input$dates6272.1)) && input$choices6272.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6272.1[1L])
      	to=as.Date(input$dates6272.1[2L])
      	if (from>to) to = from
      	selectdates6272.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.1_2 <- data$df6272.1[as.Date(data$df6272.1$"Дата операции") %in% selectdates6272.1_1, ]
    } else if (!is.null(input$text) && input$choices6272.1 == "Выбор по номеру первичного документа") {
      	data$df6272.1_2 <- data$df6272.1[data$df6272.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6272.1 == "Выбор по статье дохода") {
      	data$df6272.1_2 <- data$df6272.1[data$df6272.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6272.1) && !any(is.na(input$dates6272.1)) && !is.null(input$text) && input$choices6272.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6272.1[1L])
      	to=as.Date(input$dates6272.1[2L])
      	if (from>to) to = from
      	selectdates6272.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.1_2 <- data$df6272.1[as.Date(data$df6272.1$"Дата операции") %in% selectdates6272.1_2 & data$df6272.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6272.1) && !any(is.na(input$dates6272.1)) && !is.null(input$text) && input$choices6272.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6272.1[1L])
      	to=as.Date(input$dates6272.1[2L])
      	if (from>to) to = from
      	selectdates6272.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.1_2 <- data$df6272.1[as.Date(data$df6272.1$"Дата операции") %in% selectdates6272.1_3 & data$df6272.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6272.1_4 <- unique(data$df6272.1$"Дата операции")
        data$df6272.1_2 <- data$df6272.1[data$df6272.1$"Дата операции" %in% selectdates6272.1_4, ]
    }
}
})

  output$table6272_1Item1 <- renderRHandsontable({
    
   data$df6272.1[, `Сальдо конечное` := data$df6272.1[[6]] + data$df6272.1[[7]] - data$df6272.1[[8]]]

    rhandsontable(data$df6272.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6272.1 <- renderUI({
    if (input$choices6272.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6272.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6272.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6272.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6272.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6272.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6272.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6272.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6272.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6272.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6272.1 <- downloadHandler(
    filename = function() { "df6272.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6272.1, file)
  })

  output$download_df6272.1_2 <- downloadHandler(
    filename = function() { "df6272.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6272.1_2, file)
  })

#****************************************

#6272.2

observeEvent(input$dates6272.2, {
    start <- ymd(input$dates6272.2[[1]])
    end <- ymd(input$dates6272.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6272.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6272.2[[1]]
      r$end <- input$dates6272.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6272.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6272_2Item1)) {
    data$df6272.2 <- hot_to_r(input$table6272_2Item1) 

    if (!any(is.na(input$dates6272.2)) && input$choices6272.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6272.2[1L])
      	to=as.Date(input$dates6272.2[2L])
      	if (from>to) to = from
      	selectdates6272.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.2_2 <- data$df6272.2[as.Date(data$df6272.2$"Дата операции") %in% selectdates6272.2_1, ]
    } else if (!is.null(input$text) && input$choices6272.2 == "Выбор по номеру первичного документа") {
      	data$df6272.2_2 <- data$df6272.2[data$df6272.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6272.2 == "Выбор по статье дохода") {
      	data$df6272.2_2 <- data$df6272.2[data$df6272.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6272.2) && !any(is.na(input$dates6272.2)) && !is.null(input$text) && input$choices6272.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6272.2[1L])
      	to=as.Date(input$dates6272.2[2L])
      	if (from>to) to = from
      	selectdates6272.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.2_2 <- data$df6272.2[as.Date(data$df6272.2$"Дата операции") %in% selectdates6272.2_2 & data$df6272.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6272.2) && !any(is.na(input$dates6272.2)) && !is.null(input$text) && input$choices6272.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6272.2[1L])
      	to=as.Date(input$dates6272.2[2L])
      	if (from>to) to = from
      	selectdates6272.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6272.2_2 <- data$df6272.2[as.Date(data$df6272.2$"Дата операции") %in% selectdates6272.2_3 & data$df6272.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6272.2_4 <- unique(data$df6272.2$"Дата операции")
        data$df6272.2_2 <- data$df6272.2[data$df6272.2$"Дата операции" %in% selectdates6272.2_4, ]
    }
}
})

  output$table6272_2Item1 <- renderRHandsontable({
    
   data$df6272.2[, `Сальдо конечное` := data$df6272.2[[6]] + data$df6272.2[[7]] - data$df6272.2[[8]]]

    rhandsontable(data$df6272.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6272.2 <- renderUI({
    if (input$choices6272.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6272.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6272.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6272.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6272.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6272.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6272.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6272.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6272.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6272.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6272.2 <- downloadHandler(
    filename = function() { "df6272.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6272.2, file)
  })

  output$download_df6272.2_2 <- downloadHandler(
    filename = function() { "df6272.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6272.2_2, file)
  })

#***********************************************

#6273

observeEvent(input$dates6273, {
    start <- ymd(input$dates6273[[1]])
    end <- ymd(input$dates6273[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6273", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6273[[1]]
      r$end <- input$dates6273[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6273",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6273Item1) && !any(is.na(input$dates6273))) {
    data$df6273 <- hot_to_r(input$table6273Item1) 

    if (!any(is.na(input$dates6273)) && input$choices6273 == "Выбор по дате операции") {
     	from=as.Date(input$dates6273[1L])
      	to=as.Date(input$dates6273[2L])
      	if (from>to) to = from
      	selectdates6273_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6273_2 <- data$df6273[as.Date(data$df6273$"Дата операции") %in% selectdates6273_1, ]
    } else if (!is.null(input$text) && input$choices6273 == "Выбор по номеру первичного документа") {
      	data$df6273_2 <- data$df6273[data$df6273$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6273 == "Выбор по статье дохода") {
      	data$df6273_2 <- data$df6273[data$df6273$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6273) && !any(is.na(input$dates6273)) && !is.null(input$text) && input$choices6273 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6273[1L])
      	to=as.Date(input$dates6273[2L])
      	if (from>to) to = from
      	selectdates6273_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6273_2 <- data$df6273[as.Date(data$df6273$"Дата операции") %in% selectdates6273_2 & data$df6273$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6273) && !any(is.na(input$dates6273)) && !is.null(input$text) && input$choices6273 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6273[1L])
      	to=as.Date(input$dates6273[2L])
      	if (from>to) to = from
      	selectdates6273_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6273_2 <- data$df6273[as.Date(data$df6273$"Дата операции") %in% selectdates6273_3 & data$df6273$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6273_4 <- unique(data$df6273$"Дата операции")
        data$df6273_2 <- data$df6273[data$df6273$"Дата операции" %in% selectdates6273_4, ]
    }
}
})  

  output$table6273Item1 <- renderRHandsontable({

   data$df6273[, `Сальдо конечное` := data$df6273[[6]] + data$df6273[[7]] - data$df6273[[8]]]

    rhandsontable(data$df6273, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6273 <- renderUI({
    if (input$choices6273 == "Выбор по дате операции") {
      	dateRangeInput("dates6273", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6273 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6273 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6273 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6273", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6273 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6273", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6273Item2 <- renderRHandsontable({
    rhandsontable(data$df6273_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6273 <- downloadHandler(
    filename = function() { "df6273.xlsx" },
    content = function(file) {
      write.xlsx(data$df6273, file)
  })

  output$download_df6273_2 <- downloadHandler(
    filename = function() { "df6273_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6273_2, file)
  })

#***********************************************

#6274

observeEvent(input$dates6274, {
    start <- ymd(input$dates6274[[1]])
    end <- ymd(input$dates6274[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6274", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6274[[1]]
      r$end <- input$dates6274[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6274",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6274Item1) && !any(is.na(input$dates6274))) {
    data$df6274 <- hot_to_r(input$table6274Item1) 

    if (!any(is.na(input$dates6274)) && input$choices6274 == "Выбор по дате операции") {
     	from=as.Date(input$dates6274[1L])
      	to=as.Date(input$dates6274[2L])
      	if (from>to) to = from
      	selectdates6274_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6274_2 <- data$df6274[as.Date(data$df6274$"Дата операции") %in% selectdates6274_1, ]
    } else if (!is.null(input$text) && input$choices6274 == "Выбор по номеру первичного документа") {
      	data$df6274_2 <- data$df6274[data$df6274$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6274 == "Выбор по статье дохода") {
      	data$df6274_2 <- data$df6274[data$df6274$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6274) && !any(is.na(input$dates6274)) && !is.null(input$text) && input$choices6274 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6274[1L])
      	to=as.Date(input$dates6274[2L])
      	if (from>to) to = from
      	selectdates6274_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6274_2 <- data$df6274[as.Date(data$df6274$"Дата операции") %in% selectdates6274_2 & data$df6274$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6274) && !any(is.na(input$dates6274)) && !is.null(input$text) && input$choices6274 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6274[1L])
      	to=as.Date(input$dates6274[2L])
      	if (from>to) to = from
      	selectdates6274_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6274_2 <- data$df6274[as.Date(data$df6274$"Дата операции") %in% selectdates6274_3 & data$df6274$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6274_4 <- unique(data$df6274$"Дата операции")
        data$df6274_2 <- data$df6274[data$df6274$"Дата операции" %in% selectdates6274_4, ]
    }
}
})  

  output$table6274Item1 <- renderRHandsontable({

   data$df6274[, `Сальдо конечное` := data$df6274[[6]] + data$df6274[[7]] - data$df6274[[8]]]

    rhandsontable(data$df6274, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6274 <- renderUI({
    if (input$choices6274 == "Выбор по дате операции") {
      	dateRangeInput("dates6274", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6274 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6274 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6274 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6274", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6274 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6274", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6274Item2 <- renderRHandsontable({
    rhandsontable(data$df6274_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6274 <- downloadHandler(
    filename = function() { "df6274.xlsx" },
    content = function(file) {
      write.xlsx(data$df6274, file)
  })

  output$download_df6274_2 <- downloadHandler(
    filename = function() { "df6274_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6274_2, file)
  })

#*****************************************

#ОСВ: 6275

observeEvent(input$dates6275, {
    start <- ymd(input$dates6275[[1]])
    end <- ymd(input$dates6275[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6275", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6275[[1]]
      r$end <- input$dates6275[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6275",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6275))) {
      from=as.Date(input$dates6275[1L])
      to=as.Date(input$dates6275[2L])
      if (from>to) to = from
      selectdates6275.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.1_1 <- data$df6275.1[as.Date(data$df6275.1$`Дата операции`) %in% selectdates6275.1_5, ]
    } else {
      selectdates6275.1_6 <- unique(as.Date(data$df6275.1$`Дата операции`))
      data$df6275.1_1 <- data$df6275.1[data$df6275.1$`Дата операции` %in% selectdates6275.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6275))) {
      from=as.Date(input$dates6275[1L])
      to=as.Date(input$dates6275[2L])
      if (from>to) to = from
      selectdates6275.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6275.2_1 <- data$df6275.2[as.Date(data$df6275.2$`Дата операции`) %in% selectdates6275.2_5, ]
    } else {
      selectdates6275.2_6 <- unique(as.Date(data$df6275.2$`Дата операции`))
      data$df6275.2_1 <- data$df6275.2[data$df6275.2$`Дата операции` %in% selectdates6275.2_6, ]
    }
  })

observe({
  data$df6275[1, 2:5] <- data$df6275.1_1[, list(
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
  data$df6275[2, 2:5] <- data$df6275.2_1[, list(
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

observe({ data$df6275[3, 2:5] <- data$df6275[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6275 <- renderUI({!any(is.na(input$dates6275))})

  output$table6275Item1 <- renderRHandsontable({
    rhandsontable(data$df6275, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6275 <- downloadHandler(
    filename = function() { "df6275.xlsx" },
    content = function(file) {
      write.xlsx(data$df6275, file)
  })

#**************************************

#6275.1

observeEvent(input$dates6275.1, {
    start <- ymd(input$dates6275.1[[1]])
    end <- ymd(input$dates6275.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6275.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6275.1[[1]]
      r$end <- input$dates6275.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6275.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6275_1Item1)) {
    data$df6275.1 <- hot_to_r(input$table6275_1Item1) 

    if (!any(is.na(input$dates6275.1)) && input$choices6275.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6275.1[1L])
      	to=as.Date(input$dates6275.1[2L])
      	if (from>to) to = from
      	selectdates6275.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.1_2 <- data$df6275.1[as.Date(data$df6275.1$"Дата операции") %in% selectdates6275.1_1, ]
    } else if (!is.null(input$text) && input$choices6275.1 == "Выбор по номеру первичного документа") {
      	data$df6275.1_2 <- data$df6275.1[data$df6275.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6275.1 == "Выбор по статье дохода") {
      	data$df6275.1_2 <- data$df6275.1[data$df6275.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6275.1) && !any(is.na(input$dates6275.1)) && !is.null(input$text) && input$choices6275.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6275.1[1L])
      	to=as.Date(input$dates6275.1[2L])
      	if (from>to) to = from
      	selectdates6275.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.1_2 <- data$df6275.1[as.Date(data$df6275.1$"Дата операции") %in% selectdates6275.1_2 & data$df6275.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6275.1) && !any(is.na(input$dates6275.1)) && !is.null(input$text) && input$choices6275.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6275.1[1L])
      	to=as.Date(input$dates6275.1[2L])
      	if (from>to) to = from
      	selectdates6275.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.1_2 <- data$df6275.1[as.Date(data$df6275.1$"Дата операции") %in% selectdates6275.1_3 & data$df6275.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6275.1_4 <- unique(data$df6275.1$"Дата операции")
        data$df6275.1_2 <- data$df6275.1[data$df6275.1$"Дата операции" %in% selectdates6275.1_4, ]
    }
}
})

  output$table6275_1Item1 <- renderRHandsontable({
    
   data$df6275.1[, `Сальдо конечное` := data$df6275.1[[6]] + data$df6275.1[[7]] - data$df6275.1[[8]]]

    rhandsontable(data$df6275.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6275.1 <- renderUI({
    if (input$choices6275.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6275.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6275.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6275.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6275.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6275.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6275.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6275.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6275.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6275.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6275.1 <- downloadHandler(
    filename = function() { "df6275.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6275.1, file)
  })

  output$download_df6275.1_2 <- downloadHandler(
    filename = function() { "df6275.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6275.1_2, file)
  })

#****************************************

#6275.2

observeEvent(input$dates6275.2, {
    start <- ymd(input$dates6275.2[[1]])
    end <- ymd(input$dates6275.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6275.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6275.2[[1]]
      r$end <- input$dates6275.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6275.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6275_2Item1)) {
    data$df6275.2 <- hot_to_r(input$table6275_2Item1) 

    if (!any(is.na(input$dates6275.2)) && input$choices6275.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6275.2[1L])
      	to=as.Date(input$dates6275.2[2L])
      	if (from>to) to = from
      	selectdates6275.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.2_2 <- data$df6275.2[as.Date(data$df6275.2$"Дата операции") %in% selectdates6275.2_1, ]
    } else if (!is.null(input$text) && input$choices6275.2 == "Выбор по номеру первичного документа") {
      	data$df6275.2_2 <- data$df6275.2[data$df6275.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6275.2 == "Выбор по статье дохода") {
      	data$df6275.2_2 <- data$df6275.2[data$df6275.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6275.2) && !any(is.na(input$dates6275.2)) && !is.null(input$text) && input$choices6275.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6275.2[1L])
      	to=as.Date(input$dates6275.2[2L])
      	if (from>to) to = from
      	selectdates6275.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.2_2 <- data$df6275.2[as.Date(data$df6275.2$"Дата операции") %in% selectdates6275.2_2 & data$df6275.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6275.2) && !any(is.na(input$dates6275.2)) && !is.null(input$text) && input$choices6275.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6275.2[1L])
      	to=as.Date(input$dates6275.2[2L])
      	if (from>to) to = from
      	selectdates6275.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6275.2_2 <- data$df6275.2[as.Date(data$df6275.2$"Дата операции") %in% selectdates6275.2_3 & data$df6275.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6275.2_4 <- unique(data$df6275.2$"Дата операции")
        data$df6275.2_2 <- data$df6275.2[data$df6275.2$"Дата операции" %in% selectdates6275.2_4, ]
    }
}
})

  output$table6275_2Item1 <- renderRHandsontable({
    
   data$df6275.2[, `Сальдо конечное` := data$df6275.2[[6]] + data$df6275.2[[7]] - data$df6275.2[[8]]]

    rhandsontable(data$df6275.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6275.2 <- renderUI({
    if (input$choices6275.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6275.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6275.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6275.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6275.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6275.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6275.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6275.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6275.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6275.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6275.2 <- downloadHandler(
    filename = function() { "df6275.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6275.2, file)
  })

  output$download_df6275.2_2 <- downloadHandler(
    filename = function() { "df6275.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6275.2_2, file)
  })

#*****************************************

#ОСВ: 6276

observeEvent(input$dates6276, {
    start <- ymd(input$dates6276[[1]])
    end <- ymd(input$dates6276[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6276", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6276[[1]]
      r$end <- input$dates6276[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6276",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6276))) {
      from=as.Date(input$dates6276[1L])
      to=as.Date(input$dates6276[2L])
      if (from>to) to = from
      selectdates6276.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.1_1 <- data$df6276.1[as.Date(data$df6276.1$`Дата операции`) %in% selectdates6276.1_5, ]
    } else {
      selectdates6276.1_6 <- unique(as.Date(data$df6276.1$`Дата операции`))
      data$df6276.1_1 <- data$df6276.1[data$df6276.1$`Дата операции` %in% selectdates6276.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6276))) {
      from=as.Date(input$dates6276[1L])
      to=as.Date(input$dates6276[2L])
      if (from>to) to = from
      selectdates6276.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6276.2_1 <- data$df6276.2[as.Date(data$df6276.2$`Дата операции`) %in% selectdates6276.2_5, ]
    } else {
      selectdates6276.2_6 <- unique(as.Date(data$df6276.2$`Дата операции`))
      data$df6276.2_1 <- data$df6276.2[data$df6276.2$`Дата операции` %in% selectdates6276.2_6, ]
    }
  })

observe({
  data$df6276[1, 2:5] <- data$df6276.1_1[, list(
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
  data$df6276[2, 2:5] <- data$df6276.2_1[, list(
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

observe({ data$df6276[3, 2:5] <- data$df6276[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6276 <- renderUI({!any(is.na(input$dates6276))})

  output$table6276Item1 <- renderRHandsontable({
    rhandsontable(data$df6276, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6276 <- downloadHandler(
    filename = function() { "df6276.xlsx" },
    content = function(file) {
      write.xlsx(data$df6276, file)
  })

#**************************************

#6276.1

observeEvent(input$dates6276.1, {
    start <- ymd(input$dates6276.1[[1]])
    end <- ymd(input$dates6276.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6276.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6276.1[[1]]
      r$end <- input$dates6276.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6276.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6276_1Item1)) {
    data$df6276.1 <- hot_to_r(input$table6276_1Item1) 

    if (!any(is.na(input$dates6276.1)) && input$choices6276.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6276.1[1L])
      	to=as.Date(input$dates6276.1[2L])
      	if (from>to) to = from
      	selectdates6276.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.1_2 <- data$df6276.1[as.Date(data$df6276.1$"Дата операции") %in% selectdates6276.1_1, ]
    } else if (!is.null(input$text) && input$choices6276.1 == "Выбор по номеру первичного документа") {
      	data$df6276.1_2 <- data$df6276.1[data$df6276.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6276.1 == "Выбор по статье дохода") {
      	data$df6276.1_2 <- data$df6276.1[data$df6276.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6276.1) && !any(is.na(input$dates6276.1)) && !is.null(input$text) && input$choices6276.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6276.1[1L])
      	to=as.Date(input$dates6276.1[2L])
      	if (from>to) to = from
      	selectdates6276.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.1_2 <- data$df6276.1[as.Date(data$df6276.1$"Дата операции") %in% selectdates6276.1_2 & data$df6276.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6276.1) && !any(is.na(input$dates6276.1)) && !is.null(input$text) && input$choices6276.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6276.1[1L])
      	to=as.Date(input$dates6276.1[2L])
      	if (from>to) to = from
      	selectdates6276.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.1_2 <- data$df6276.1[as.Date(data$df6276.1$"Дата операции") %in% selectdates6276.1_3 & data$df6276.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6276.1_4 <- unique(data$df6276.1$"Дата операции")
        data$df6276.1_2 <- data$df6276.1[data$df6276.1$"Дата операции" %in% selectdates6276.1_4, ]
    }
}
})

  output$table6276_1Item1 <- renderRHandsontable({
    
   data$df6276.1[, `Сальдо конечное` := data$df6276.1[[6]] + data$df6276.1[[7]] - data$df6276.1[[8]]]

    rhandsontable(data$df6276.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6276.1 <- renderUI({
    if (input$choices6276.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6276.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6276.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6276.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6276.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6276.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6276.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6276.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6276.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6276.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6276.1 <- downloadHandler(
    filename = function() { "df6276.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6276.1, file)
  })

  output$download_df6276.1_2 <- downloadHandler(
    filename = function() { "df6276.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6276.1_2, file)
  })

#****************************************

#6276.2

observeEvent(input$dates6276.2, {
    start <- ymd(input$dates6276.2[[1]])
    end <- ymd(input$dates6276.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6276.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6276.2[[1]]
      r$end <- input$dates6276.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6276.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6276_2Item1)) {
    data$df6276.2 <- hot_to_r(input$table6276_2Item1) 

    if (!any(is.na(input$dates6276.2)) && input$choices6276.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6276.2[1L])
      	to=as.Date(input$dates6276.2[2L])
      	if (from>to) to = from
      	selectdates6276.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.2_2 <- data$df6276.2[as.Date(data$df6276.2$"Дата операции") %in% selectdates6276.2_1, ]
    } else if (!is.null(input$text) && input$choices6276.2 == "Выбор по номеру первичного документа") {
      	data$df6276.2_2 <- data$df6276.2[data$df6276.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6276.2 == "Выбор по статье дохода") {
      	data$df6276.2_2 <- data$df6276.2[data$df6276.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6276.2) && !any(is.na(input$dates6276.2)) && !is.null(input$text) && input$choices6276.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6276.2[1L])
      	to=as.Date(input$dates6276.2[2L])
      	if (from>to) to = from
      	selectdates6276.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.2_2 <- data$df6276.2[as.Date(data$df6276.2$"Дата операции") %in% selectdates6276.2_2 & data$df6276.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6276.2) && !any(is.na(input$dates6276.2)) && !is.null(input$text) && input$choices6276.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6276.2[1L])
      	to=as.Date(input$dates6276.2[2L])
      	if (from>to) to = from
      	selectdates6276.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6276.2_2 <- data$df6276.2[as.Date(data$df6276.2$"Дата операции") %in% selectdates6276.2_3 & data$df6276.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6276.2_4 <- unique(data$df6276.2$"Дата операции")
        data$df6276.2_2 <- data$df6276.2[data$df6276.2$"Дата операции" %in% selectdates6276.2_4, ]
    }
}
})

  output$table6276_2Item1 <- renderRHandsontable({
    
   data$df6276.2[, `Сальдо конечное` := data$df6276.2[[6]] + data$df6276.2[[7]] - data$df6276.2[[8]]]

    rhandsontable(data$df6276.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6276.2 <- renderUI({
    if (input$choices6276.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6276.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6276.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6276.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6276.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6276.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6276.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6276.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6276.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6276.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6276.2 <- downloadHandler(
    filename = function() { "df6276.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6276.2, file)
  })

  output$download_df6276.2_2 <- downloadHandler(
    filename = function() { "df6276.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6276.2_2, file)
  })

#*****************************************

#ОСВ: 6277

observeEvent(input$dates6277, {
    start <- ymd(input$dates6277[[1]])
    end <- ymd(input$dates6277[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6277", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6277[[1]]
      r$end <- input$dates6277[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6277",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6277))) {
      from=as.Date(input$dates6277[1L])
      to=as.Date(input$dates6277[2L])
      if (from>to) to = from
      selectdates6277.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.1_1 <- data$df6277.1[as.Date(data$df6277.1$`Дата операции`) %in% selectdates6277.1_5, ]
    } else {
      selectdates6277.1_6 <- unique(as.Date(data$df6277.1$`Дата операции`))
      data$df6277.1_1 <- data$df6277.1[data$df6277.1$`Дата операции` %in% selectdates6277.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6277))) {
      from=as.Date(input$dates6277[1L])
      to=as.Date(input$dates6277[2L])
      if (from>to) to = from
      selectdates6277.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6277.2_1 <- data$df6277.2[as.Date(data$df6277.2$`Дата операции`) %in% selectdates6277.2_5, ]
    } else {
      selectdates6277.2_6 <- unique(as.Date(data$df6277.2$`Дата операции`))
      data$df6277.2_1 <- data$df6277.2[data$df6277.2$`Дата операции` %in% selectdates6277.2_6, ]
    }
  })

observe({
  data$df6277[1, 2:5] <- data$df6277.1_1[, list(
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
  data$df6277[2, 2:5] <- data$df6277.2_1[, list(
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

observe({ data$df6277[3, 2:5] <- data$df6277[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6277 <- renderUI({!any(is.na(input$dates6277))})

  output$table6277Item1 <- renderRHandsontable({
    rhandsontable(data$df6277, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6277 <- downloadHandler(
    filename = function() { "df6277.xlsx" },
    content = function(file) {
      write.xlsx(data$df6277, file)
  })

#**************************************

#6277.1

observeEvent(input$dates6277.1, {
    start <- ymd(input$dates6277.1[[1]])
    end <- ymd(input$dates6277.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6277.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6277.1[[1]]
      r$end <- input$dates6277.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6277.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6277_1Item1)) {
    data$df6277.1 <- hot_to_r(input$table6277_1Item1) 

    if (!any(is.na(input$dates6277.1)) && input$choices6277.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6277.1[1L])
      	to=as.Date(input$dates6277.1[2L])
      	if (from>to) to = from
      	selectdates6277.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.1_2 <- data$df6277.1[as.Date(data$df6277.1$"Дата операции") %in% selectdates6277.1_1, ]
    } else if (!is.null(input$text) && input$choices6277.1 == "Выбор по номеру первичного документа") {
      	data$df6277.1_2 <- data$df6277.1[data$df6277.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6277.1 == "Выбор по статье дохода") {
      	data$df6277.1_2 <- data$df6277.1[data$df6277.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6277.1) && !any(is.na(input$dates6277.1)) && !is.null(input$text) && input$choices6277.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6277.1[1L])
      	to=as.Date(input$dates6277.1[2L])
      	if (from>to) to = from
      	selectdates6277.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.1_2 <- data$df6277.1[as.Date(data$df6277.1$"Дата операции") %in% selectdates6277.1_2 & data$df6277.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6277.1) && !any(is.na(input$dates6277.1)) && !is.null(input$text) && input$choices6277.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6277.1[1L])
      	to=as.Date(input$dates6277.1[2L])
      	if (from>to) to = from
      	selectdates6277.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.1_2 <- data$df6277.1[as.Date(data$df6277.1$"Дата операции") %in% selectdates6277.1_3 & data$df6277.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6277.1_4 <- unique(data$df6277.1$"Дата операции")
        data$df6277.1_2 <- data$df6277.1[data$df6277.1$"Дата операции" %in% selectdates6277.1_4, ]
    }
}
})

  output$table6277_1Item1 <- renderRHandsontable({
    
   data$df6277.1[, `Сальдо конечное` := data$df6277.1[[6]] + data$df6277.1[[7]] - data$df6277.1[[8]]]

    rhandsontable(data$df6277.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6277.1 <- renderUI({
    if (input$choices6277.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6277.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6277.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6277.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6277.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6277.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6277.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6277.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6277.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6277.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6277.1 <- downloadHandler(
    filename = function() { "df6277.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6277.1, file)
  })

  output$download_df6277.1_2 <- downloadHandler(
    filename = function() { "df6277.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6277.1_2, file)
  })

#****************************************

#6277.2

observeEvent(input$dates6277.2, {
    start <- ymd(input$dates6277.2[[1]])
    end <- ymd(input$dates6277.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6277.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6277.2[[1]]
      r$end <- input$dates6277.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6277.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6277_2Item1)) {
    data$df6277.2 <- hot_to_r(input$table6277_2Item1) 

    if (!any(is.na(input$dates6277.2)) && input$choices6277.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6277.2[1L])
      	to=as.Date(input$dates6277.2[2L])
      	if (from>to) to = from
      	selectdates6277.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.2_2 <- data$df6277.2[as.Date(data$df6277.2$"Дата операции") %in% selectdates6277.2_1, ]
    } else if (!is.null(input$text) && input$choices6277.2 == "Выбор по номеру первичного документа") {
      	data$df6277.2_2 <- data$df6277.2[data$df6277.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6277.2 == "Выбор по статье дохода") {
      	data$df6277.2_2 <- data$df6277.2[data$df6277.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6277.2) && !any(is.na(input$dates6277.2)) && !is.null(input$text) && input$choices6277.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6277.2[1L])
      	to=as.Date(input$dates6277.2[2L])
      	if (from>to) to = from
      	selectdates6277.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.2_2 <- data$df6277.2[as.Date(data$df6277.2$"Дата операции") %in% selectdates6277.2_2 & data$df6277.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6277.2) && !any(is.na(input$dates6277.2)) && !is.null(input$text) && input$choices6277.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6277.2[1L])
      	to=as.Date(input$dates6277.2[2L])
      	if (from>to) to = from
      	selectdates6277.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6277.2_2 <- data$df6277.2[as.Date(data$df6277.2$"Дата операции") %in% selectdates6277.2_3 & data$df6277.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6277.2_4 <- unique(data$df6277.2$"Дата операции")
        data$df6277.2_2 <- data$df6277.2[data$df6277.2$"Дата операции" %in% selectdates6277.2_4, ]
    }
}
})

  output$table6277_2Item1 <- renderRHandsontable({
    
   data$df6277.2[, `Сальдо конечное` := data$df6277.2[[6]] + data$df6277.2[[7]] - data$df6277.2[[8]]]

    rhandsontable(data$df6277.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6277.2 <- renderUI({
    if (input$choices6277.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6277.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6277.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6277.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6277.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6277.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6277.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6277.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6277.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6277.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6277.2 <- downloadHandler(
    filename = function() { "df6277.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6277.2, file)
  })

  output$download_df6277.2_2 <- downloadHandler(
    filename = function() { "df6277.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6277.2_2, file)
  })

#*****************************************

#ОСВ: 6280

observeEvent(input$dates6280, {
    start <- ymd(input$dates6280[[1]])
    end <- ymd(input$dates6280[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6280", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6280[[1]]
      r$end <- input$dates6280[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6280",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6280))) {
      from=as.Date(input$dates6280[1L])
      to=as.Date(input$dates6280[2L])
      if (from>to) to = from
      selectdates6281.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.1_3 <- data$df6281.1[as.Date(data$df6281.1$`Дата операции`) %in% selectdates6281.1_7, ]
    } else {
      selectdates6281.1_8 <- unique(as.Date(data$df6281.1$`Дата операции`))
      data$df6281.1_3 <- data$df6281.1[data$df6281.1$`Дата операции` %in% selectdates6281.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6280))) {
      from=as.Date(input$dates6280[1L])
      to=as.Date(input$dates6280[2L])
      if (from>to) to = from
      selectdates6281.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.2_3 <- data$df6281.2[as.Date(data$df6281.2$`Дата операции`) %in% selectdates6281.2_7, ]
    } else {
      selectdates6281.2_8 <- unique(as.Date(data$df6281.2$`Дата операции`))
      data$df6281.2_3 <- data$df6281.2[data$df6281.2$`Дата операции` %in% selectdates6281.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6280))) {
      from=as.Date(input$dates6280[1L])
      to=as.Date(input$dates6280[2L])
      if (from>to) to = from
      selectdates6282_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6282_1 <- data$df6281.2[as.Date(data$df6281.2$`Дата операции`) %in% selectdates6282_5, ]
    } else {
      selectdates6282_6 <- unique(as.Date(data$df6281.2$`Дата операции`))
      data$df6282_1 <- data$df6281.2[data$df6281.2$`Дата операции` %in% selectdates6282_6, ]
    }
  })

observe({
  data$df6281_2[1, 2:5] <- data$df6281.1_3[, list(
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
  data$df6281_2[2, 2:5] <- data$df6281.2_3[, list(
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

observe({ data$df6280[1, 2:5] <- data$df6281_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6280[2, 2:5] <- data$df6282_1[, list(
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

observe({ data$df6280[3, 2:5] <- data$df6280[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6280 <- renderUI({!any(is.na(input$dates6280))})

  output$table6280Item1 <- renderRHandsontable({
    rhandsontable(data$df6280, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 550) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6280 <- downloadHandler(
    filename = function() { "df6280.xlsx" },
    content = function(file) {
      write.xlsx(data$df6280, file)
  })

#*****************************************

#ОСВ: 6281

observeEvent(input$dates6281, {
    start <- ymd(input$dates6281[[1]])
    end <- ymd(input$dates6281[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6281", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6281[[1]]
      r$end <- input$dates6281[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6281",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6281))) {
      from=as.Date(input$dates6281[1L])
      to=as.Date(input$dates6281[2L])
      if (from>to) to = from
      selectdates6281.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.1_1 <- data$df6281.1[as.Date(data$df6281.1$`Дата операции`) %in% selectdates6281.1_5, ]
    } else {
      selectdates6281.1_6 <- unique(as.Date(data$df6281.1$`Дата операции`))
      data$df6281.1_1 <- data$df6281.1[data$df6281.1$`Дата операции` %in% selectdates6281.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6281))) {
      from=as.Date(input$dates6281[1L])
      to=as.Date(input$dates6281[2L])
      if (from>to) to = from
      selectdates6281.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6281.2_1 <- data$df6281.2[as.Date(data$df6281.2$`Дата операции`) %in% selectdates6281.2_5, ]
    } else {
      selectdates6281.2_6 <- unique(as.Date(data$df6281.2$`Дата операции`))
      data$df6281.2_1 <- data$df6281.2[data$df6281.2$`Дата операции` %in% selectdates6281.2_6, ]
    }
  })

observe({
  data$df6281[1, 2:5] <- data$df6281.1_1[, list(
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
  data$df6281[2, 2:5] <- data$df6281.2_1[, list(
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

observe({ data$df6281[3, 2:5] <- data$df6281[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6281 <- renderUI({!any(is.na(input$dates6281))})

  output$table6281Item1 <- renderRHandsontable({
    rhandsontable(data$df6281, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6281 <- downloadHandler(
    filename = function() { "df6281.xlsx" },
    content = function(file) {
      write.xlsx(data$df6281, file)
  })

#**************************************

#6281.1

observeEvent(input$dates6281.1, {
    start <- ymd(input$dates6281.1[[1]])
    end <- ymd(input$dates6281.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6281.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6281.1[[1]]
      r$end <- input$dates6281.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6281.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6281_1Item1)) {
    data$df6281.1 <- hot_to_r(input$table6281_1Item1) 

    if (!any(is.na(input$dates6281.1)) && input$choices6281.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6281.1[1L])
      	to=as.Date(input$dates6281.1[2L])
      	if (from>to) to = from
      	selectdates6281.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.1_2 <- data$df6281.1[as.Date(data$df6281.1$"Дата операции") %in% selectdates6281.1_1, ]
    } else if (!is.null(input$text) && input$choices6281.1 == "Выбор по номеру первичного документа") {
      	data$df6281.1_2 <- data$df6281.1[data$df6281.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6281.1 == "Выбор по статье дохода") {
      	data$df6281.1_2 <- data$df6281.1[data$df6281.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6281.1) && !any(is.na(input$dates6281.1)) && !is.null(input$text) && input$choices6281.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6281.1[1L])
      	to=as.Date(input$dates6281.1[2L])
      	if (from>to) to = from
      	selectdates6281.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.1_2 <- data$df6281.1[as.Date(data$df6281.1$"Дата операции") %in% selectdates6281.1_2 & data$df6281.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6281.1) && !any(is.na(input$dates6281.1)) && !is.null(input$text) && input$choices6281.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6281.1[1L])
      	to=as.Date(input$dates6281.1[2L])
      	if (from>to) to = from
      	selectdates6281.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.1_2 <- data$df6281.1[as.Date(data$df6281.1$"Дата операции") %in% selectdates6281.1_3 & data$df6281.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6281.1_4 <- unique(data$df6281.1$"Дата операции")
        data$df6281.1_2 <- data$df6281.1[data$df6281.1$"Дата операции" %in% selectdates6281.1_4, ]
    }
}
})

  output$table6281_1Item1 <- renderRHandsontable({
    
   data$df6281.1[, `Сальдо конечное` := data$df6281.1[[6]] + data$df6281.1[[7]] - data$df6281.1[[8]]]

    rhandsontable(data$df6281.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6281.1 <- renderUI({
    if (input$choices6281.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6281.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6281.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6281.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6281.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6281.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6281.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6281.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6281.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6281.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6281.1 <- downloadHandler(
    filename = function() { "df6281.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6281.1, file)
  })

  output$download_df6281.1_2 <- downloadHandler(
    filename = function() { "df6281.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6281.1_2, file)
  })

#****************************************

#6281.2

observeEvent(input$dates6281.2, {
    start <- ymd(input$dates6281.2[[1]])
    end <- ymd(input$dates6281.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6281.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6281.2[[1]]
      r$end <- input$dates6281.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6281.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6281_2Item1)) {
    data$df6281.2 <- hot_to_r(input$table6281_2Item1) 

    if (!any(is.na(input$dates6281.2)) && input$choices6281.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6281.2[1L])
      	to=as.Date(input$dates6281.2[2L])
      	if (from>to) to = from
      	selectdates6281.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.2_2 <- data$df6281.2[as.Date(data$df6281.2$"Дата операции") %in% selectdates6281.2_1, ]
    } else if (!is.null(input$text) && input$choices6281.2 == "Выбор по номеру первичного документа") {
      	data$df6281.2_2 <- data$df6281.2[data$df6281.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6281.2 == "Выбор по статье дохода") {
      	data$df6281.2_2 <- data$df6281.2[data$df6281.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6281.2) && !any(is.na(input$dates6281.2)) && !is.null(input$text) && input$choices6281.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6281.2[1L])
      	to=as.Date(input$dates6281.2[2L])
      	if (from>to) to = from
      	selectdates6281.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.2_2 <- data$df6281.2[as.Date(data$df6281.2$"Дата операции") %in% selectdates6281.2_2 & data$df6281.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6281.2) && !any(is.na(input$dates6281.2)) && !is.null(input$text) && input$choices6281.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6281.2[1L])
      	to=as.Date(input$dates6281.2[2L])
      	if (from>to) to = from
      	selectdates6281.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6281.2_2 <- data$df6281.2[as.Date(data$df6281.2$"Дата операции") %in% selectdates6281.2_3 & data$df6281.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6281.2_4 <- unique(data$df6281.2$"Дата операции")
        data$df6281.2_2 <- data$df6281.2[data$df6281.2$"Дата операции" %in% selectdates6281.2_4, ]
    }
}
})

  output$table6281_2Item1 <- renderRHandsontable({
    
   data$df6281.2[, `Сальдо конечное` := data$df6281.2[[6]] + data$df6281.2[[7]] - data$df6281.2[[8]]]

    rhandsontable(data$df6281.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6281.2 <- renderUI({
    if (input$choices6281.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6281.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6281.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6281.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6281.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6281.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6281.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6281.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6281.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6281.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6281.2 <- downloadHandler(
    filename = function() { "df6281.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6281.2, file)
  })

  output$download_df6281.2_2 <- downloadHandler(
    filename = function() { "df6281.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6281.2_2, file)
  })

#**********************************

#6282

observeEvent(input$dates6282, {
    start <- ymd(input$dates6282[[1]])
    end <- ymd(input$dates6282[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6282", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6282[[1]]
      r$end <- input$dates6282[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6282",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6282Item1) && !any(is.na(input$dates6282))) {
    data$df6282 <- hot_to_r(input$table6282Item1) 

    if (!any(is.na(input$dates6282)) && input$choices6282 == "Выбор по дате операции") {
     	from=as.Date(input$dates6282[1L])
      	to=as.Date(input$dates6282[2L])
      	if (from>to) to = from
      	selectdates6282_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6282_2 <- data$df6282[as.Date(data$df6282$"Дата операции") %in% selectdates6282_1, ]
    } else if (!is.null(input$text) && input$choices6282 == "Выбор по номеру первичного документа") {
      	data$df6282_2 <- data$df6282[data$df6282$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6282 == "Выбор по статье дохода") {
      	data$df6282_2 <- data$df6282[data$df6282$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6282) && !any(is.na(input$dates6282)) && !is.null(input$text) && input$choices6282 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6282[1L])
      	to=as.Date(input$dates6282[2L])
      	if (from>to) to = from
      	selectdates6282_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6282_2 <- data$df6282[as.Date(data$df6282$"Дата операции") %in% selectdates6282_2 & data$df6282$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6282) && !any(is.na(input$dates6282)) && !is.null(input$text) && input$choices6282 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6282[1L])
      	to=as.Date(input$dates6282[2L])
      	if (from>to) to = from
      	selectdates6282_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6282_2 <- data$df6282[as.Date(data$df6282$"Дата операции") %in% selectdates6282_3 & data$df6282$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6282_4 <- unique(data$df6282$"Дата операции")
        data$df6282_2 <- data$df6282[data$df6282$"Дата операции" %in% selectdates6282_4, ]
    }
}
})  

  output$table6282Item1 <- renderRHandsontable({

   data$df6282[, `Сальдо конечное` := data$df6282[[6]] + data$df6282[[7]] - data$df6282[[8]]]

    rhandsontable(data$df6282, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6282 <- renderUI({
    if (input$choices6282 == "Выбор по дате операции") {
      	dateRangeInput("dates6282", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6282 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6282 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6282 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6282", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6282 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6282", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })
  
  output$table6282Item2 <- renderRHandsontable({
    rhandsontable(data$df6282_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6282 <- downloadHandler(
    filename = function() { "df6282.xlsx" },
    content = function(file) {
      write.xlsx(data$df6282, file)
  })

  output$download_df6282_2 <- downloadHandler(
    filename = function() { "df6282_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6282_2, file)
  })

#**********************************

#6290

#ОСВ: 6290

observeEvent(input$dates6290, {
    start <- ymd(input$dates6290[[1]])
    end <- ymd(input$dates6290[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6290", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6290[[1]]
      r$end <- input$dates6290[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6290",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6291_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6291_1 <- data$df6291[as.Date(data$df6291$`Дата операции`) %in% selectdates6291_5, ]
    } else {
      selectdates6291_6 <- unique(as.Date(data$df6291$`Дата операции`))
      data$dfdf6291_1 <- data$df6291[data$df6291$`Дата операции` %in% selectdates6291_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6292.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.1_3 <- data$df6292.1[as.Date(data$df6292.1$`Дата операции`) %in% selectdates6292.1_7, ]
    } else {
      selectdates6292.1_8 <- unique(as.Date(data$df6292.1$`Дата операции`))
      data$df6292.1_3 <- data$df6292.1[data$df6292.1$`Дата операции` %in% selectdates6292.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6292.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.2_3 <- data$df6292.2[as.Date(data$df6292.2$`Дата операции`) %in% selectdates6292.2_7, ]
    } else {
      selectdates6292.2_8 <- unique(as.Date(data$df6292.2$`Дата операции`))
      data$df6292.2_3 <- data$df6292.2[data$df6292.2$`Дата операции` %in% selectdates6292.2_8, ]
    }
  })


observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6293.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.1_3 <- data$df6293.1[as.Date(data$df6293.1$`Дата операции`) %in% selectdates6293.1_7, ]
    } else {
      selectdates6293.1_8 <- unique(as.Date(data$df6293.1$`Дата операции`))
      data$df6293.1_3 <- data$df6293.1[data$df6293.1$`Дата операции` %in% selectdates6293.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6293.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.2_3 <- data$df6293.2[as.Date(data$df6293.2$`Дата операции`) %in% selectdates6293.2_7, ]
    } else {
      selectdates6293.2_8 <- unique(as.Date(data$df6293.2$`Дата операции`))
      data$df6293.2_3 <- data$df6293.2[data$df6293.2$`Дата операции` %in% selectdates6293.2_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6294.1_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.1_3 <- data$df6294.1[as.Date(data$df6294.1$`Дата операции`) %in% selectdates6294.1_7, ]
    } else {
      selectdates6294.1_8 <- unique(as.Date(data$df6294.1$`Дата операции`))
      data$df6294.1_3 <- data$df6294.1[data$df6294.1$`Дата операции` %in% selectdates6294.1_8, ]
    }
  })

observe({
    if (!any(is.na(input$dates6290))) {
      from=as.Date(input$dates6290[1L])
      to=as.Date(input$dates6290[2L])
      if (from>to) to = from
      selectdates6294.2_7 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.2_3 <- data$df6294.2[as.Date(data$df6294.2$`Дата операции`) %in% selectdates6294.2_7, ]
    } else {
      selectdates6294.2_8 <- unique(as.Date(data$df6294.2$`Дата операции`))
      data$df6294.2_3 <- data$df6294.2[data$df6294.2$`Дата операции` %in% selectdates6294.2_8, ]
    }
  })

observe({
  data$df6290[1, 2:5] <- data$df6291_1[, list(
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
  data$df6292_2[1, 2:5] <- data$df6292.1_3[, list(
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
  data$df6292_2[2, 2:5] <- data$df6292.2_3[, list(
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

observe({ data$df6290[2, 2:5] <- data$df6292_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6293_2[1, 2:5] <- data$df6293.1_3[, list(
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
  data$df6293_2[2, 2:5] <- data$df6293.2_3[, list(
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

observe({ data$df6290[3, 2:5] <- data$df6293_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({
  data$df6294_2[1, 2:5] <- data$df6294.1_3[, list(
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
  data$df6294_2[2, 2:5] <- data$df6294.2_3[, list(
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

observe({ data$df6290[4, 2:5] <- data$df6294_2[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

observe({ data$df6290[5, 2:5] <- data$df6290[, .SD[1:4, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6290 <- renderUI({!any(is.na(input$dates6290))})

  output$table6290Item1 <- renderRHandsontable({
    rhandsontable(data$df6290, colWidths = 150, height = 220, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 500) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 4) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6290 <- downloadHandler(
    filename = function() { "df6290.xlsx" },
    content = function(file) {
      write.xlsx(data$df6290, file)
  })

#**********************************

#6291

observeEvent(input$dates6291, {
    start <- ymd(input$dates6291[[1]])
    end <- ymd(input$dates6291[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6291", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6291[[1]]
      r$end <- input$dates6291[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6291",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6291Item1)) {
    data$df6291 <- hot_to_r(input$table6291Item1) 

    if (!any(is.na(input$dates6291)) && input$choices6291 == "Выбор по дате операции") {
     	from=as.Date(input$dates6291[1L])
      	to=as.Date(input$dates6291[2L])
      	if (from>to) to = from
      	selectdates6291_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6291_2 <- data$df6291[as.Date(data$df6291$"Дата операции") %in% selectdates6291_1, ]
    } else if (!is.null(input$text) && input$choices6291 == "Выбор по номеру первичного документа") {
      	data$df6291_2 <- data$df6291[data$df6291$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6291 == "Выбор по статье дохода") {
      	data$df6291_2 <- data$df6291[data$df6291$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6291) && !any(is.na(input$dates6291)) && !is.null(input$text) && input$choices6291 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6291[1L])
      	to=as.Date(input$dates6291[2L])
      	if (from>to) to = from
      	selectdates6291_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6291_2 <- data$df6291[as.Date(data$df6291$"Дата операции") %in% selectdates6291_2 & data$df6291$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6291) && !any(is.na(input$dates6291)) && !is.null(input$text) && input$choices6291 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6291[1L])
      	to=as.Date(input$dates6291[2L])
      	if (from>to) to = from
      	selectdates6291_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6291_2 <- data$df6291[as.Date(data$df6291$"Дата операции") %in% selectdates6291_3 & data$df6291$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6291_4 <- unique(data$df6291$"Дата операции")
        data$df6291_2 <- data$df6291[data$df6291$"Дата операции" %in% selectdates6291_4, ]
    }
}
})  

  output$table6291Item1 <- renderRHandsontable({
    
   data$df6291[, `Сальдо конечное` := data$df6291[[9]] + data$df6291[[10]] - data$df6291[[11]]]

    rhandsontable(data$df6291, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6291 <- renderUI({
    if (input$choices6291 == "Выбор по дате операции") {
      	dateRangeInput("dates6291", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6291 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6291 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6291 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6291", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6291 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6291", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6291Item2 <- renderRHandsontable({
    rhandsontable(data$df6291_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6291 <- downloadHandler(
    filename = function() { "df6291.xlsx" },
    content = function(file) {
      write.xlsx(data$df6291, file)
  })

  output$download_df6291_2 <- downloadHandler(
    filename = function() { "df6291_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6291_2, file)
  })

#*****************************************

#ОСВ: 6292

observeEvent(input$dates6292, {
    start <- ymd(input$dates6292[[1]])
    end <- ymd(input$dates6292[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6292", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6292[[1]]
      r$end <- input$dates6292[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6292",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6292))) {
      from=as.Date(input$dates6292[1L])
      to=as.Date(input$dates6292[2L])
      if (from>to) to = from
      selectdates6292.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.1_1 <- data$df6292.1[as.Date(data$df6292.1$`Дата операции`) %in% selectdates6292.1_5, ]
    } else {
      selectdates6292.1_6 <- unique(as.Date(data$df6292.1$`Дата операции`))
      data$df6292.1_1 <- data$df6292.1[data$df6292.1$`Дата операции` %in% selectdates6292.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6292))) {
      from=as.Date(input$dates6292[1L])
      to=as.Date(input$dates6292[2L])
      if (from>to) to = from
      selectdates6292.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6292.2_1 <- data$df6292.2[as.Date(data$df6292.2$`Дата операции`) %in% selectdates6292.2_5, ]
    } else {
      selectdates6292.2_6 <- unique(as.Date(data$df6292.2$`Дата операции`))
      data$df6292.2_1 <- data$df6292.2[data$df6292.2$`Дата операции` %in% selectdates6292.2_6, ]
    }
  })

observe({
  data$df6292[1, 2:5] <- data$df6292.1_1[, list(
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
  data$df6292[2, 2:5] <- data$df6292.2_1[, list(
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

observe({ data$df6292[3, 2:5] <- data$df6292[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6292 <- renderUI({!any(is.na(input$dates6292))})

  output$table6292Item1 <- renderRHandsontable({
    rhandsontable(data$df6292, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 450) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6292 <- downloadHandler(
    filename = function() { "df6292.xlsx" },
    content = function(file) {
      write.xlsx(data$df6292, file)
  })

#**************************************

#6292.1

observeEvent(input$dates6292.1, {
    start <- ymd(input$dates6292.1[[1]])
    end <- ymd(input$dates6292.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6292.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6292.1[[1]]
      r$end <- input$dates6292.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6292.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6292_1Item1)) {
    data$df6292.1 <- hot_to_r(input$table6292_1Item1) 

    if (!any(is.na(input$dates6292.1)) && input$choices6292.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6292.1[1L])
      	to=as.Date(input$dates6292.1[2L])
      	if (from>to) to = from
      	selectdates6292.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.1_2 <- data$df6292.1[as.Date(data$df6292.1$"Дата операции") %in% selectdates6292.1_1, ]
    } else if (!is.null(input$text) && input$choices6292.1 == "Выбор по номеру первичного документа") {
      	data$df6292.1_2 <- data$df6292.1[data$df6292.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6292.1 == "Выбор по статье дохода") {
      	data$df6292.1_2 <- data$df6292.1[data$df6292.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6292.1) && !any(is.na(input$dates6292.1)) && !is.null(input$text) && input$choices6292.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6292.1[1L])
      	to=as.Date(input$dates6292.1[2L])
      	if (from>to) to = from
      	selectdates6292.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.1_2 <- data$df6292.1[as.Date(data$df6292.1$"Дата операции") %in% selectdates6292.1_2 & data$df6292.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6292.1) && !any(is.na(input$dates6292.1)) && !is.null(input$text) && input$choices6292.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6292.1[1L])
      	to=as.Date(input$dates6292.1[2L])
      	if (from>to) to = from
      	selectdates6292.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.1_2 <- data$df6292.1[as.Date(data$df6292.1$"Дата операции") %in% selectdates6292.1_3 & data$df6292.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6292.1_4 <- unique(data$df6292.1$"Дата операции")
        data$df6292.1_2 <- data$df6292.1[data$df6292.1$"Дата операции" %in% selectdates6292.1_4, ]
    }
}
})

  output$table6292_1Item1 <- renderRHandsontable({
    
   data$df6292.1[, `Сальдо конечное` := data$df6292.1[[9]] + data$df6292.1[[10]] - data$df6292.1[[11]]]

    rhandsontable(data$df6292.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6292.1 <- renderUI({
    if (input$choices6292.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6292.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6292.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6292.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6292.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6292.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6292.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6292.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6292.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6292.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6292.1 <- downloadHandler(
    filename = function() { "df6292.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6292.1, file)
  })

  output$download_df6292.1_2 <- downloadHandler(
    filename = function() { "df6292.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6292.1_2, file)
  })

#****************************************

#6292.2

observeEvent(input$dates6292.2, {
    start <- ymd(input$dates6292.2[[1]])
    end <- ymd(input$dates6292.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6292.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6292.2[[1]]
      r$end <- input$dates6292.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6292.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6292_2Item1)) {
    data$df6292.2 <- hot_to_r(input$table6292_2Item1) 

    if (!any(is.na(input$dates6292.2)) && input$choices6292.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6292.2[1L])
      	to=as.Date(input$dates6292.2[2L])
      	if (from>to) to = from
      	selectdates6292.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.2_2 <- data$df6292.2[as.Date(data$df6292.2$"Дата операции") %in% selectdates6292.2_1, ]
    } else if (!is.null(input$text) && input$choices6292.2 == "Выбор по номеру первичного документа") {
      	data$df6292.2_2 <- data$df6292.2[data$df6292.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6292.2 == "Выбор по статье дохода") {
      	data$df6292.2_2 <- data$df6292.2[data$df6292.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6292.2) && !any(is.na(input$dates6292.2)) && !is.null(input$text) && input$choices6292.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6292.2[1L])
      	to=as.Date(input$dates6292.2[2L])
      	if (from>to) to = from
      	selectdates6292.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.2_2 <- data$df6292.2[as.Date(data$df6292.2$"Дата операции") %in% selectdates6292.2_2 & data$df6292.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6292.2) && !any(is.na(input$dates6292.2)) && !is.null(input$text) && input$choices6292.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6292.2[1L])
      	to=as.Date(input$dates6292.2[2L])
      	if (from>to) to = from
      	selectdates6292.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6292.2_2 <- data$df6292.2[as.Date(data$df6292.2$"Дата операции") %in% selectdates6292.2_3 & data$df6292.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6292.2_4 <- unique(data$df6292.2$"Дата операции")
        data$df6292.2_2 <- data$df6292.2[data$df6292.2$"Дата операции" %in% selectdates6292.2_4, ]
    }
}
})

  output$table6292_2Item1 <- renderRHandsontable({
    
   data$df6292.2[, `Сальдо конечное` := data$df6292.2[[9]] + data$df6292.2[[10]] - data$df6292.2[[11]]]

    rhandsontable(data$df6292.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6292.2 <- renderUI({
    if (input$choices6292.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6292.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6292.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6292.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6292.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6292.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6292.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6292.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6292.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6292.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6292.2 <- downloadHandler(
    filename = function() { "df6292.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6292.2, file)
  })

  output$download_df6292.2_2 <- downloadHandler(
    filename = function() { "df6292.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6292.2_2, file)
  })

#*****************************************

#ОСВ: 6293

observeEvent(input$dates6293, {
    start <- ymd(input$dates6293[[1]])
    end <- ymd(input$dates6293[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6293", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6293[[1]]
      r$end <- input$dates6293[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6293",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6293))) {
      from=as.Date(input$dates6293[1L])
      to=as.Date(input$dates6293[2L])
      if (from>to) to = from
      selectdates6293.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.1_1 <- data$df6293.1[as.Date(data$df6293.1$`Дата операции`) %in% selectdates6293.1_5, ]
    } else {
      selectdates6293.1_6 <- unique(as.Date(data$df6293.1$`Дата операции`))
      data$df6293.1_1 <- data$df6293.1[data$df6293.1$`Дата операции` %in% selectdates6293.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6293))) {
      from=as.Date(input$dates6293[1L])
      to=as.Date(input$dates6293[2L])
      if (from>to) to = from
      selectdates6293.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6293.2_1 <- data$df6293.2[as.Date(data$df6293.2$`Дата операции`) %in% selectdates6293.2_5, ]
    } else {
      selectdates6293.2_6 <- unique(as.Date(data$df6293.2$`Дата операции`))
      data$df6293.2_1 <- data$df6293.2[data$df6293.2$`Дата операции` %in% selectdates6293.2_6, ]
    }
  })

observe({
  data$df6293[1, 2:5] <- data$df6293.1_1[, list(
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
  data$df6293[2, 2:5] <- data$df6293.2_1[, list(
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

observe({ data$df6293[3, 2:5] <- data$df6293[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6293 <- renderUI({!any(is.na(input$dates6293))})

  output$table6293Item1 <- renderRHandsontable({
    rhandsontable(data$df6293, colWidths = 150, height = 160, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 600) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6293 <- downloadHandler(
    filename = function() { "df6293.xlsx" },
    content = function(file) {
      write.xlsx(data$df6293, file)
  })

#**************************************

#6293.1

observeEvent(input$dates6293.1, {
    start <- ymd(input$dates6293.1[[1]])
    end <- ymd(input$dates6293.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6293.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6293.1[[1]]
      r$end <- input$dates6293.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6293.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6293_1Item1)) {
    data$df6293.1 <- hot_to_r(input$table6293_1Item1) 

    if (!any(is.na(input$dates6293.1)) && input$choices6293.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6293.1[1L])
      	to=as.Date(input$dates6293.1[2L])
      	if (from>to) to = from
      	selectdates6293.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.1_2 <- data$df6293.1[as.Date(data$df6293.1$"Дата операции") %in% selectdates6293.1_1, ]
    } else if (!is.null(input$text) && input$choices6293.1 == "Выбор по номеру первичного документа") {
      	data$df6293.1_2 <- data$df6293.1[data$df6293.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6293.1 == "Выбор по статье дохода") {
      	data$df6293.1_2 <- data$df6293.1[data$df6293.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6293.1) && !any(is.na(input$dates6293.1)) && !is.null(input$text) && input$choices6293.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6293.1[1L])
      	to=as.Date(input$dates6293.1[2L])
      	if (from>to) to = from
      	selectdates6293.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.1_2 <- data$df6293.1[as.Date(data$df6293.1$"Дата операции") %in% selectdates6293.1_2 & data$df6293.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6293.1) && !any(is.na(input$dates6293.1)) && !is.null(input$text) && input$choices6293.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6293.1[1L])
      	to=as.Date(input$dates6293.1[2L])
      	if (from>to) to = from
      	selectdates6293.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.1_2 <- data$df6293.1[as.Date(data$df6293.1$"Дата операции") %in% selectdates6293.1_3 & data$df6293.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6293.1_4 <- unique(data$df6293.1$"Дата операции")
        data$df6293.1_2 <- data$df6293.1[data$df6293.1$"Дата операции" %in% selectdates6293.1_4, ]
    }
}
})

  output$table6293_1Item1 <- renderRHandsontable({
    
   data$df6293.1[, `Сальдо конечное` := data$df6293.1[[6]] + data$df6293.1[[7]] - data$df6293.1[[8]]]

    rhandsontable(data$df6293.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6293.1 <- renderUI({
    if (input$choices6293.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6293.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6293.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6293.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6293.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6293.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6293.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6293.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6293.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6293.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6293.1 <- downloadHandler(
    filename = function() { "df6293.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6293.1, file)
  })

  output$download_df6293.1_2 <- downloadHandler(
    filename = function() { "df6293.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6293.1_2, file)
  })

#****************************************

#6293.2

observeEvent(input$dates6293.2, {
    start <- ymd(input$dates6293.2[[1]])
    end <- ymd(input$dates6293.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6293.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6293.2[[1]]
      r$end <- input$dates6293.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6293.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6293_2Item1)) {
    data$df6293.2 <- hot_to_r(input$table6293_2Item1) 

    if (!any(is.na(input$dates6293.2)) && input$choices6293.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6293.2[1L])
      	to=as.Date(input$dates6293.2[2L])
      	if (from>to) to = from
      	selectdates6293.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.2_2 <- data$df6293.2[as.Date(data$df6293.2$"Дата операции") %in% selectdates6293.2_1, ]
    } else if (!is.null(input$text) && input$choices6293.2 == "Выбор по номеру первичного документа") {
      	data$df6293.2_2 <- data$df6293.2[data$df6293.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6293.2 == "Выбор по статье дохода") {
      	data$df6293.2_2 <- data$df6293.2[data$df6293.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6293.2) && !any(is.na(input$dates6293.2)) && !is.null(input$text) && input$choices6293.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6293.2[1L])
      	to=as.Date(input$dates6293.2[2L])
      	if (from>to) to = from
      	selectdates6293.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.2_2 <- data$df6293.2[as.Date(data$df6293.2$"Дата операции") %in% selectdates6293.2_2 & data$df6293.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6293.2) && !any(is.na(input$dates6293.2)) && !is.null(input$text) && input$choices6293.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6293.2[1L])
      	to=as.Date(input$dates6293.2[2L])
      	if (from>to) to = from
      	selectdates6293.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6293.2_2 <- data$df6293.2[as.Date(data$df6293.2$"Дата операции") %in% selectdates6293.2_3 & data$df6293.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6293.2_4 <- unique(data$df6293.2$"Дата операции")
        data$df6293.2_2 <- data$df6293.2[data$df6293.2$"Дата операции" %in% selectdates6293.2_4, ]
    }
}
})

  output$table6293_2Item1 <- renderRHandsontable({
    
   data$df6293.2[, `Сальдо конечное` := data$df6293.2[[6]] + data$df6293.2[[7]] - data$df6293.2[[8]]]

    rhandsontable(data$df6293.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6293.2 <- renderUI({
    if (input$choices6293.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6293.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6293.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6293.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6293.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6293.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6293.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6293.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6293.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6293.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6293.2 <- downloadHandler(
    filename = function() { "df6293.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6293.2, file)
  })

  output$download_df6293.2_2 <- downloadHandler(
    filename = function() { "df6293.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6293.2_2, file)
  })

#*****************************************

#ОСВ: 6294

observeEvent(input$dates6294, {
    start <- ymd(input$dates6294[[1]])
    end <- ymd(input$dates6294[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6294", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6294[[1]]
      r$end <- input$dates6294[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6294",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6294))) {
      from=as.Date(input$dates6294[1L])
      to=as.Date(input$dates6294[2L])
      if (from>to) to = from
      selectdates6294.1_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.1_1 <- data$df6294.1[as.Date(data$df6294.1$`Дата операции`) %in% selectdates6294.1_5, ]
    } else {
      selectdates6294.1_6 <- unique(as.Date(data$df6294.1$`Дата операции`))
      data$df6294.1_1 <- data$df6294.1[data$df6294.1$`Дата операции` %in% selectdates6294.1_6, ]
    }
  })

observe({
    if (!any(is.na(input$dates6294))) {
      from=as.Date(input$dates6294[1L])
      to=as.Date(input$dates6294[2L])
      if (from>to) to = from
      selectdates6294.2_5 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6294.2_1 <- data$df6294.2[as.Date(data$df6294.2$`Дата операции`) %in% selectdates6294.2_5, ]
    } else {
      selectdates6294.2_6 <- unique(as.Date(data$df6294.2$`Дата операции`))
      data$df6294.2_1 <- data$df6294.2[data$df6294.2$`Дата операции` %in% selectdates6294.2_6, ]
    }
  })

observe({
  data$df6294[1, 2:5] <- data$df6294.1_1[, list(
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
  data$df6294[2, 2:5] <- data$df6294.2_1[, list(
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

observe({ data$df6294[3, 2:5] <- data$df6294[, .SD[1:2, lapply(.SD, sum)], .SDcols = 2:5] })

  output$nested_ui6294 <- renderUI({!any(is.na(input$dates6294))})

  output$table6294Item1 <- renderRHandsontable({
    rhandsontable(data$df6294, colWidths = 150, height = 120, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 450) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 2) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6294 <- downloadHandler(
    filename = function() { "df6294.xlsx" },
    content = function(file) {
      write.xlsx(data$df6294, file)
  })

#**************************************

#6294.1

observeEvent(input$dates6294.1, {
    start <- ymd(input$dates6294.1[[1]])
    end <- ymd(input$dates6294.1[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6294.1", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6294.1[[1]]
      r$end <- input$dates6294.1[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6294.1",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6294_1Item1)) {
    data$df6294.1 <- hot_to_r(input$table6294_1Item1) 

    if (!any(is.na(input$dates6294.1)) && input$choices6294.1 == "Выбор по дате операции") {
     	from=as.Date(input$dates6294.1[1L])
      	to=as.Date(input$dates6294.1[2L])
      	if (from>to) to = from
      	selectdates6294.1_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.1_2 <- data$df6294.1[as.Date(data$df6294.1$"Дата операции") %in% selectdates6294.1_1, ]
    } else if (!is.null(input$text) && input$choices6294.1 == "Выбор по номеру первичного документа") {
      	data$df6294.1_2 <- data$df6294.1[data$df6294.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6294.1 == "Выбор по статье дохода") {
      	data$df6294.1_2 <- data$df6294.1[data$df6294.1$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6294.1) && !any(is.na(input$dates6294.1)) && !is.null(input$text) && input$choices6294.1 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6294.1[1L])
      	to=as.Date(input$dates6294.1[2L])
      	if (from>to) to = from
      	selectdates6294.1_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.1_2 <- data$df6294.1[as.Date(data$df6294.1$"Дата операции") %in% selectdates6294.1_2 & data$df6294.1$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6294.1) && !any(is.na(input$dates6294.1)) && !is.null(input$text) && input$choices6294.1 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6294.1[1L])
      	to=as.Date(input$dates6294.1[2L])
      	if (from>to) to = from
      	selectdates6294.1_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.1_2 <- data$df6294.1[as.Date(data$df6294.1$"Дата операции") %in% selectdates6294.1_3 & data$df6294.1$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6294.1_4 <- unique(data$df6294.1$"Дата операции")
        data$df6294.1_2 <- data$df6294.1[data$df6294.1$"Дата операции" %in% selectdates6294.1_4, ]
    }
}
})

  output$table6294_1Item1 <- renderRHandsontable({
    
   data$df6294.1[, `Сальдо конечное` := data$df6294.1[[9]] + data$df6294.1[[10]] - data$df6294.1[[11]]]

    rhandsontable(data$df6294.1, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6294.1 <- renderUI({
    if (input$choices6294.1 == "Выбор по дате операции") {
      	dateRangeInput("dates6294.1", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6294.1 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6294.1 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6294.1 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6294.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6294.1 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6294.1", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6294.1Item2 <- renderRHandsontable({
    rhandsontable(data$df6294.1_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6294.1 <- downloadHandler(
    filename = function() { "df6294.1.xlsx" },
    content = function(file) {
      write.xlsx(data$df6294.1, file)
  })

  output$download_df6294.1_2 <- downloadHandler(
    filename = function() { "df6294.1_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6294.1_2, file)
  })

#****************************************

#6294.2

observeEvent(input$dates6294.2, {
    start <- ymd(input$dates6294.2[[1]])
    end <- ymd(input$dates6294.2[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6294.2", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6294.2[[1]]
      r$end <- input$dates6294.2[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6294.2",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6294_2Item1)) {
    data$df6294.2 <- hot_to_r(input$table6294_2Item1) 

    if (!any(is.na(input$dates6294.2)) && input$choices6294.2 == "Выбор по дате операции") {
     	from=as.Date(input$dates6294.2[1L])
      	to=as.Date(input$dates6294.2[2L])
      	if (from>to) to = from
      	selectdates6294.2_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.2_2 <- data$df6294.2[as.Date(data$df6294.2$"Дата операции") %in% selectdates6294.2_1, ]
    } else if (!is.null(input$text) && input$choices6294.2 == "Выбор по номеру первичного документа") {
      	data$df6294.2_2 <- data$df6294.2[data$df6294.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$text) && input$choices6294.2 == "Выбор по статье дохода") {
      	data$df6294.2_2 <- data$df6294.2[data$df6294.2$"Счет № статьи дохода" == input$text, ]
    } else if (!is.null(input$dates6294.2) && !any(is.na(input$dates6294.2)) && !is.null(input$text) && input$choices6294.2 == "Выбор по дате операции и номеру первичного документа") {
     	from=as.Date(input$dates6294.2[1L])
      	to=as.Date(input$dates6294.2[2L])
      	if (from>to) to = from
      	selectdates6294.2_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.2_2 <- data$df6294.2[as.Date(data$df6294.2$"Дата операции") %in% selectdates6294.2_2 & data$df6294.2$"Номер первичного документа" == input$text, ]
    } else if (!is.null(input$dates6294.2) && !any(is.na(input$dates6294.2)) && !is.null(input$text) && input$choices6294.2 == "Выбор по дате операции и статье дохода") {
     	from=as.Date(input$dates6294.2[1L])
      	to=as.Date(input$dates6294.2[2L])
      	if (from>to) to = from
      	selectdates6294.2_3 <- seq.Date(from=from, to=to, by = "day")
      	data$df6294.2_2 <- data$df6294.2[as.Date(data$df6294.2$"Дата операции") %in% selectdates6294.2_3 & data$df6294.2$"Счет № статьи дохода" == input$text, ]
    } else {
        selectdates6294.2_4 <- unique(data$df6294.2$"Дата операции")
        data$df6294.2_2 <- data$df6294.2[data$df6294.2$"Дата операции" %in% selectdates6294.2_4, ]
    }
}
})

  output$table6294_2Item1 <- renderRHandsontable({
    
   data$df6294.2[, `Сальдо конечное` := data$df6294.2[[9]] + data$df6294.2[[10]] - data$df6294.2[[11]]]

    rhandsontable(data$df6294.2, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })
  
  output$nested_ui6294.2 <- renderUI({
    if (input$choices6294.2 == "Выбор по дате операции") {
      	dateRangeInput("dates6294.2", "Выберите период времени:", format="yyyy-mm-dd",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6294.2 == "Выбор по номеру первичного документа") {
      	textInput("text", "Укажите номер первичного документа:")
    } else if (input$choices6294.2 == "Выбор по статье дохода") {
      	textInput("text", "Укажите Счет № статьи дохода:")
    } else if (input$choices6294.2 == "Выбор по дате операции и номеру первичного документа") {
      fluidRow(
       	dateRangeInput("dates6294.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер первичного документа:")
      )
    } else if (input$choices6294.2 == "Выбор по дате операции и статье дохода") {
      fluidRow(
       	dateRangeInput("dates6294.2", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите Счет № статьи дохода:")
      )
    }
  })

  output$table6294.2Item2 <- renderRHandsontable({
    rhandsontable(data$df6294.2_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE, manualColumnResize = TRUE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")
  })

  output$download_df6294.2 <- downloadHandler(
    filename = function() { "df6294.2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6294.2, file)
  })

  output$download_df6294.2_2 <- downloadHandler(
    filename = function() { "df6294.2_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6294.2_2, file)
  })

#**************************************

#ОСВ: 6300

observeEvent(input$dates6300, {
    start <- ymd(input$dates6300[[1]])
    end <- ymd(input$dates6300[[2]])

 tryCatch({
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6300", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6300[[1]]
      r$end <- input$dates6300[[2]]
      }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6300",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
 }, ignoreInit = TRUE)

observe({
    if (!any(is.na(input$dates6300))) {
      from=as.Date(input$dates6300[1L])
      to=as.Date(input$dates6300[2L])
      if (from>to) to = from
      selectdates6310_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6310_1 <- data$df6310[as.Date(data$df6310$`Дата учета`) %in% selectdates6310_1, ]
    } else {
      selectdates6310_2 <- unique(as.Date(data$df6310$`Дата учета`))
      data$df6310_1 <- data$df6310[data$df6310$`Дата учета` %in% selectdates6310_2, ]
    }
  })

  observe({
    if(!is.null(input$table6310Item1) && !any(is.na(input$table6310Item1)))
      data$df6310_1<- hot_to_r(input$table6310Item1)
  })

observe({
    if (!any(is.na(input$dates6300))) {
      from=as.Date(input$dates6300[1L])
      to=as.Date(input$dates6300[2L])
      if (from>to) to = from
      selectdates6320_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6320_1 <- data$df6320[as.Date(data$df6320$`Дата учета`) %in% selectdates6320_1, ]
    } else {
      selectdates6320_2 <- unique(as.Date(data$df6320$`Дата учета`))
      data$df6320_1 <- data$df6320[data$df6320$`Дата учета` %in% selectdates6320_2, ]
    }
  })

  observe({
    if(!is.null(input$table6320Item1) && !any(is.na(input$table6320Item1)))
      data$df6320_1<- hot_to_r(input$table6320Item1)
  })

observe({
    if (!any(is.na(input$dates6300))) {
      from=as.Date(input$dates6300[1L])
      to=as.Date(input$dates6300[2L])
      if (from>to) to = from
      selectdates6330_1 <- seq.Date(from=from,
                               to=to, by = "day")
     data$df6330_1 <- data$df6330[as.Date(data$df6330$`Дата учета`) %in% selectdates6330_1, ]
    } else {
      selectdates6330_2 <- unique(as.Date(data$df6330$`Дата учета`))
      data$df6330_1 <- data$df6330[data$df6330$`Дата учета` %in% selectdates6330_2, ]
    }
  })

  observe({
    if(!is.null(input$table6330Item1) && !any(is.na(input$table6330Item1)))
      data$df6330_1<- hot_to_r(input$table6330Item1)
  })

observe({
  data$df6300[1, 2:3] <- data$df6310_1[, list(
    `Сумма доли в прибыли объектов инвестиций за период` = sum(`Сумма доли в прибыли объекта инвестиции за период`),
    `Сумма доли прибыли в прочем совокупном доходе объектов инвестиций за период` = sum(`Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период`)
  )]
})

observe({
  data$df6300[2, 2:3] <- data$df6320_1[, list(
    `Сумма доли в прибыли объектов инвестиций за период` = sum(`Сумма доли в прибыли объекта инвестиции за период`),
    `Сумма доли прибыли в прочем совокупном доходе объектов инвестиций за период` = sum(`Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период`)
  )]
})

observe({
  data$df6300[3, 2:3] <- data$df6330_1[, list(
    `Сумма доли в прибыли объектов инвестиций за период` = sum(`Сумма доли в прибыли объекта инвестиции за период`),
    `Сумма доли прибыли в прочем совокупном доходе объектов инвестиций за период` = sum(`Сумма доли прибыли в прочем совокупном доходе объекта инвестиции за период`)
  )]
})

observe({ data$df6300[4, 2:3] <- data$df6300[, .SD[1:3, lapply(.SD, sum)], .SDcols=2:3]})

  output$nested_ui6300 <- renderUI({!any(is.na(input$dates6300))})

  output$table6300Item1 <- renderRHandsontable({
    rhandsontable(data$df6300, colWidths = 150, height = 220, readOnly=TRUE, contextMenu = FALSE, fixedColumnsLeft = 1, manualColumnResize = TRUE) |>
	hot_col(1, width = 550) |>
	hot_cols(column = 1, renderer = "function(instance, td, row, col, prop, value) {
	  if (row === 3) { td.style.fontWeight = 'bold';
	  } Handsontable.renderers.TextRenderer.apply(this, arguments);
	}")
  })

  output$download_df6300 <- downloadHandler(
    filename = function() { "df6300.xlsx" },
    content = function(file) {
      write.xlsx(data$df6300, file)
  })

#********************************************

#6310

observeEvent(input$dates6310, {
    start <- ymd(input$dates6310[[1]])
    end <- ymd(input$dates6310[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6310", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6310[[1]]
      r$end <- input$dates6310[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6310",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6310Item1) && !any(is.na(input$dates6310))) {
    data$df6310 <- hot_to_r(input$table6310Item1) 

    if (!any(is.na(input$dates6310)) && input$choices6310 == "Выбор по дате учета") {
     	from=as.Date(input$dates6310[1L])
      	to=as.Date(input$dates6310[2L])
      	if (from>to) to = from
      	selectdates6310_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6310_2 <- data$df6310[as.Date(data$df6310$"Дата учета") %in% selectdates6310_1, ]
    } else if (!is.null(input$text) && input$choices6310 == "Выбор по номеру учета объекта инвестиции") {
      	data$df6310_2 <- data$df6310[data$df6310$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else if (!is.null(input$dates6310) && !any(is.na(input$dates6310)) && !is.null(input$text) && input$choices6310 == "Выбор по дате и номеру учета объекта инвестиции") {
     	from=as.Date(input$dates6310[1L])
      	to=as.Date(input$dates6310[2L])
      	if (from>to) to = from
      	selectdates6310_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6310_2 <- data$df6310[as.Date(data$df6310$"Дата учета") %in% selectdates6310_2 & data$df6310$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else {
        selectdates6310_4 <- unique(data$df6310$"Дата учета")
        data$df6310_2 <- data$df6310[data$df6310$"Дата учета" %in% selectdates6310_4, ]
    }
}
})  

  output$table6310Item1 <- renderRHandsontable({

    rhandsontable(data$df6310, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6310 <- renderUI({
    if (input$choices6310 == "Выбор по дате учета") {
      	dateRangeInput("dates6310", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6310 == "Выбор по номеру учета объекта инвестиции") {
      	textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
    } else if (input$choices6310 == "Выбор по дате и номеру учета объекта инвестиции") {
      fluidRow(
       	dateRangeInput("dates6310", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
      )
    } 
  })

  output$table6310Item2 <- renderRHandsontable({
    rhandsontable(data$df6310_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6310 <- downloadHandler(
    filename = function() { "df6310.xlsx" },
    content = function(file) {
      write.xlsx(data$df6310, file)
  })

  output$download_df6310_2 <- downloadHandler(
    filename = function() { "df6310_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6310_2, file)
  })

#********************************************

#6320

observeEvent(input$dates6320, {
    start <- ymd(input$dates6320[[1]])
    end <- ymd(input$dates6320[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6320", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6320[[1]]
      r$end <- input$dates6320[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6320",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6320Item1) && !any(is.na(input$dates6320))) {
    data$df6320 <- hot_to_r(input$table6320Item1) 

    if (!any(is.na(input$dates6320)) && input$choices6320 == "Выбор по дате учета") {
     	from=as.Date(input$dates6320[1L])
      	to=as.Date(input$dates6320[2L])
      	if (from>to) to = from
      	selectdates6320_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6320_2 <- data$df6320[as.Date(data$df6320$"Дата учета") %in% selectdates6320_1, ]
    } else if (!is.null(input$text) && input$choices6320 == "Выбор по номеру учета объекта инвестиции") {
      	data$df6320_2 <- data$df6320[data$df6320$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else if (!is.null(input$dates6320) && !any(is.na(input$dates6320)) && !is.null(input$text) && input$choices6320 == "Выбор по дате и номеру учета объекта инвестиции") {
     	from=as.Date(input$dates6320[1L])
      	to=as.Date(input$dates6320[2L])
      	if (from>to) to = from
      	selectdates6320_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6320_2 <- data$df6320[as.Date(data$df6320$"Дата учета") %in% selectdates6320_2 & data$df6320$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else {
        selectdates6320_4 <- unique(data$df6320$"Дата учета")
        data$df6320_2 <- data$df6320[data$df6320$"Дата учета" %in% selectdates6320_4, ]
    }
}
})  

  output$table6320Item1 <- renderRHandsontable({

    rhandsontable(data$df6320, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6320 <- renderUI({
    if (input$choices6320 == "Выбор по дате учета") {
      	dateRangeInput("dates6320", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6320 == "Выбор по номеру учета объекта инвестиции") {
      	textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
    } else if (input$choices6320 == "Выбор по дате и номеру учета объекта инвестиции") {
      fluidRow(
       	dateRangeInput("dates6320", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
      )
    } 
  })

  output$table6320Item2 <- renderRHandsontable({
    rhandsontable(data$df6320_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6320 <- downloadHandler(
    filename = function() { "df6320.xlsx" },
    content = function(file) {
      write.xlsx(data$df6320, file)
  })

  output$download_df6320_2 <- downloadHandler(
    filename = function() { "df6320_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6320_2, file)
  })

#********************************************

#6330

observeEvent(input$dates6330, {
    start <- ymd(input$dates6330[[1]])
    end <- ymd(input$dates6330[[2]])

 tryCatch({  
  if (start > end) {
    shinyalert("Ошибка при вводе: конечная дата предшествует начальной дате", type = "error")
    updateDateRangeInput(
      session, 
      "dates6330", 
        start = r$start,
        end = r$end
      )
    } else {
      r$start <- input$dates6330[[1]]
      r$end <- input$dates6330[[2]]
    }
   }, error = function(e) {
      updateDateRangeInput(session,
                           "dates6330",
                           start = ymd(Sys.Date()),
                           end = ymd(Sys.Date()))
      shinyalert("Диапазон дат не может быть пустым! Переход на текущую дату.",
                 type = "error")
    })
}, ignoreInit = TRUE)

observe({ if (!is.null(input$table6330Item1) && !any(is.na(input$dates6330))) {
    data$df6330 <- hot_to_r(input$table6330Item1) 

    if (!any(is.na(input$dates6330)) && input$choices6330 == "Выбор по дате учета") {
     	from=as.Date(input$dates6330[1L])
      	to=as.Date(input$dates6330[2L])
      	if (from>to) to = from
      	selectdates6330_1 <- seq.Date(from=from, to=to, by = "day")
      	data$df6330_2 <- data$df6330[as.Date(data$df6330$"Дата учета") %in% selectdates6330_1, ]
    } else if (!is.null(input$text) && input$choices6330 == "Выбор по номеру учета объекта инвестиции") {
      	data$df6330_2 <- data$df6330[data$df6330$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else if (!is.null(input$dates6330) && !any(is.na(input$dates6330)) && !is.null(input$text) && input$choices6330 == "Выбор по дате и номеру учета объекта инвестиции") {
     	from=as.Date(input$dates6330[1L])
      	to=as.Date(input$dates6330[2L])
      	if (from>to) to = from
      	selectdates6330_2 <- seq.Date(from=from, to=to, by = "day")
      	data$df6330_2 <- data$df6330[as.Date(data$df6330$"Дата учета") %in% selectdates6330_2 & data$df6330$"Номер учета (реквизиты) объекта инвестиции" == input$text, ]
    } else {
        selectdates6330_4 <- unique(data$df6330$"Дата учета")
        data$df6330_2 <- data$df6330[data$df6330$"Дата учета" %in% selectdates6330_4, ]
    }
}
})  

  output$table6330Item1 <- renderRHandsontable({

    rhandsontable(data$df6330, colWidths = 150, height = 600, allowInvalid=FALSE, fixedColumnsLeft = 2) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })
  
  output$nested_ui6330 <- renderUI({
    if (input$choices6330 == "Выбор по дате учета") {
      	dateRangeInput("dates6330", "Выберите период времени:",
                     start = Sys.Date(), end = Sys.Date(), separator = "-")
    } else if (input$choices6330 == "Выбор по номеру учета объекта инвестиции") {
      	textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
    } else if (input$choices6330 == "Выбор по дате и номеру учета объекта инвестиции") {
      fluidRow(
       	dateRangeInput("dates6330", "Выберите период времени:",
                       start = Sys.Date(), end = Sys.Date(), separator = "-"),
        textInput("text", "Укажите номер учета (реквизиты) объекта инвестиции:")
      )
    } 
  })

  output$table6330Item2 <- renderRHandsontable({
    rhandsontable(data$df6330_2, colWidths = 150, height = 600, readOnly=TRUE, contextMenu = FALSE) |>
      hot_col(1, dateFormat = "YYYY-MM-DD", type = "date")|>
      hot_cols(manualColumnResize = TRUE)
  })

  output$download_df6330 <- downloadHandler(
    filename = function() { "df6330.xlsx" },
    content = function(file) {
      write.xlsx(data$df6330, file)
  })

  output$download_df6330_2 <- downloadHandler(
    filename = function() { "df6330_2.xlsx" },
    content = function(file) {
      write.xlsx(data$df6330_2, file)
  })

}
shinyApp(ui, server)
