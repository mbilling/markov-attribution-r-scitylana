# Minimum config
# Set BigQuery data set ID, e.g. "scitylana-1048:scitylana"
datasetId <- ""
# Set Google Analytics view ID, e.g. "14409649"
viewId <- ""

####################################################################################
# Additional config

# Looback steps in the user journey. Integer between 1 and infinte
journeyLookback = 5

####################################################################################
install_load <- function (package1, ...) {
  packages <- c(package1, ...)
  for (package in packages) {
    if (package %in% rownames(installed.packages()))
      do.call(library, list(package))
    else {
      install.packages(
        package,
        repos = c(
          "https://cloud.r-project.org",
          "http://owi.usgs.gov/R/"
        ),
        dependencies = NA,
        type = getOption("pkgType")
      )
      do.call(library, list(package))
    }
  }
}

# Auto Install/Load package
install_load("bigrquery",
             "ChannelAttribution",
             "reshape",
             "ggplot2",
             "stringr",
             "scales")

# Load Scitylana data from BigQuery
bqInfo <- strsplit(datasetId, ':')
bqBilling <- bqInfo[[1]][1]
bqTable <- bqInfo[[1]][2]

conversionMetric = "M_transactionRevenue"
sourceDimension  = "channel"
conversionMetricColumn = "sum(M_transactionRevenue)"
sourceDimensionColumn = "channelGrouping"

sql <- paste0('
  select sl_userId, 
    sl_sessionId as sessionCount,
    ',conversionMetricColumn,' as ',conversionMetric,',  
    ',sourceDimensionColumn,' as ',sourceDimension,'
    from  `', bqTable, ".", viewId, '`
  where 
    sl_sessionId != "(not set)"
    and sl_userId in (
      select 
        sl_userId
      from
        `', bqTable, ".", viewId, '`
      where 
        sl_userId != "(not set)" 
      group by
        sl_userId
      having 
        ',conversionMetricColumn,' > 0
    )
  group by
    sl_userId,
    sl_sessionId, 
    ',sourceDimensionColumn,'
  
  order by
    sl_sessionId,
    min(sl_timeStamp) ')

tb <- bq_project_query(bqBilling, sql)
dataset <- bq_table_download(tb)



####################################################################################
#Validate dataset
N = nrow(dataset)
if(N==0) stop('Dataset is empty')

#Order the data by our primary key, user id
dataset <- dataset[order(dataset$sl_userId, dataset$sessionCount), ]

sl_userId = -1
outputRowCount = 1
convPath = ""
lastSource = ""

total_conversions = c()
total_conversion_value = c()

convPaths = data.frame(
  path = rep("", N),
  total_conversions = rep(NA, N),
  total_conversion_value = rep(NA, N),
  stringsAsFactors = FALSE
)

converted = FALSE
for (row in 1:N) {
  sl_userIdNew = paste0(dataset[row,]["sl_userId"][1, ])
  
  # New user?
  if (sl_userIdNew != sl_userId) {
    if (row == 1)
      sl_userId = sl_userIdNew
    converted = FALSE
    lastSource = ""
    
  }
  
  # Focus only on 1st conversion - skip the rest
  if (converted == FALSE) {
    src = dataset[row,][sourceDimension][1, ]
    source = gsub(pattern = "\\(not set\\)/",
                  replacement = "",
                  x = src)
    
    
    # Workaround, ChannelAttribution package doesn't tolerate spaces in sources, replace spaces with ^
    source = gsub(pattern = " ",
                  replacement = "^",
                  x = source)
    
    if (row == 1)
      sl_userId = sl_userIdNew
    
    # handle user switch
    if (sl_userIdNew != sl_userId) {
      sl_userId = sl_userIdNew
      convPath = ""
      lastSource = ""
      
    }
    
    # Build converted path
    if (convPath == "") {
      convPath = paste0(source)
    } else {
      if (source != lastSource) {
        convPath = paste(convPath, source, sep = " > ")
      }
    }
    
    # Found conversion path?
    revenue = strtoi(dataset[row,][conversionMetric][1, ], base = 0L)
    if (is.na(revenue))
      revenue = 0
    if (revenue > 0) {
      # ignore repeat source
      convPaths[outputRowCount,] <- list(convPath, 1, revenue)
      outputRowCount = outputRowCount + 1
      converted = TRUE
    }
    lastSource = source
  }
}

# Trim exceeding rows
convPaths = head(convPaths, outputRowCount - N)

if(nrow(convPaths) == 1) stop('The users found in this dataset has no conversions')

# Aggregate per path
convPathsAggr <- aggregate(. ~ path, convPaths, sum)

# Generate last, linear and first touch models
H <- heuristic_models(convPathsAggr, 'path', 'total_conversions', var_value='total_conversion_value')

# Train Markov Model
M <- markov_model(convPathsAggr,
                  'path',
                  'total_conversions',
                  var_value = 'total_conversion_value',
                  order = journeyLookback)

# Merge by channel if we compare
M <- merge(H, M, by='channel_name') 

# Rename ^ back to space
M$channel_name <- str_replace_all(M$channel_name, "\\^", " ")

# Write to file
write.csv(M, file = paste0("markov_attribution_", viewId, "_", format(Sys.time(), "%Y%m%d_%H%M%S"),  ".csv"))


