
library(quantmod)
library(Quandl)
library(lubridate)
library(dplyr)

Quandl.auth('j1DfWirwCV4FCJTc7Xyi')

calc <- function(symbol) {
  my.xts <- suppressWarnings(getSymbols(symbol, auto.assign=F))
  return (my.xts)
}

getsplit <- function(symbol_row) {
  
  result <- tryCatch(
    {
    symbol_row$SymbolY <- paste0(strtrim(symbol_row$NSE, 9), '.NS', sep = '') #Yahoo symbol
    xts.split <- suppressWarnings(getSplits(symbol_row$SymbolY, auto.assign=F))
    if (!is.na (xts.split[1])) { 
      my.split <- data.frame(date = index(xts.split), ratio=coredata(xts.split), row.names=NULL)
      colnames(my.split) <- c('date', 'ratio')
      my.split$NSE <- symbol_row$NSE
      my.split$Mstarname <- symbol_row$Mstarname
      my.split
    }
    else {
      my.split <- NULL
    }
    
  }, error = function(cond) { return (NULL)}, 
     warning = function(cond) {return (NULL)}, finally={}
  )
  return (result)
}

adj_price <- function(qdl.df, mstarname, sp.df) {
  work.df <- qdl.df
  work.df$adj_close <- work.df$Close
  work.df$Date <- ymd(work.df$Date)
  
  spsub <- subset(sp.df, Mstarname==mstarname)
  if (nrow(spsub) > 0 ) {
    spsub$dt <- mdy(spsub$date)
    for (i in 1:nrow(spsub)) {
      idx <- (work.df$Date < spsub[i,]$dt)
      work.df[idx, ]$adj_close <- work.df[idx, ]$adj_close * spsub[i,]$ratio
    }
  }
  return (work.df)
}

getQuandl <- function(mstarname, fetch_sym, sp.df) {
  result <- tryCatch(
  {
    qdl.df <- Quandl(fetch_sym, authcode="j1DfWirwCV4FCJTc7Xyi")
    cmp  <- qdl.df[1,'Close']
    # quick sanity checks
    if (as.numeric(difftime (today(), ymd(qdl.df[1,]$Date), units='days')) > 5) { return (NULL)}
    if (nrow(qdl.df) < 245) { return (NULL)}
    adj.df  <- adj_price(qdl.df, mstarname, sp.df) # adjust the price for splits etc.
    p6 = adj.df[122, 'adj_close']
    p12 = adj.df[245, 'adj_close']
    d52 = max(adj.df[1:245, 'adj_close'])
    
    r6m <- (cmp - p6) / p6 * 100.0
    r12m <- (cmp - p12) / p12 * 100.0
    d52w <- (d52 - cmp) / d52  * 100.0
    
    my.df <- data.frame(name=mstarname, cmp=cmp, p6=p6, p12=p12, hi52=d52, r6m=r6m, r12m=r12m, d52w=d52w)
    
  }, error = function(cond) { return (NULL)}, 
     warning = function(cond) {return (NULL)}, finally={}
  )
  return (result)

}


main_splits <- function() {
  ind <- read.csv('master_list.csv', stringsAsFactors = FALSE)
  #ind <- ind[1:20,]
  alldata <- NULL # just to initialize 
  for (i in 1:nrow(ind)) {
    alldata <- rbind(alldata, getsplit(ind[i,]))
  }
  return (alldata)
}


main_buckets <- function() {
  sp.df <- read.csv('splits.csv')
  ind <- read.csv('master_list.csv', stringsAsFactors = FALSE)
  rets.df <- NULL
  #ind <- ind[1:100,]

  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Mstarname'] 
    fetchsym <- paste0('NSE/',ind[i, 'NSE'])
    if (! (sym %in% rets.df$name)) {
      rets.df <- rbind(rets.df, getQuandl(sym, fetchsym, sp.df))
      if (sym %in% rets.df$name) { print (paste(i, 'Got from Quandl NSE ', sym))  }                           
    }
  }
  
  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Mstarname']
    fetchsym <- paste0('GOOG/NSE_',ind[i, 'NSE'])
    if (! (sym %in% rets.df$name)) {
      rets.df <- rbind(rets.df, getQuandl(sym, fetchsym, sp.df))
      if (sym %in% rets.df$name) { print (paste(i, 'Got from Quandl GOOG ', sym)) }
      
    }
  }

  
  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Mstarname'] 
    bom <- sub('BOM:', '',ind[i, 'BSE'])
    fetchsym <- paste0('BSE/BOM',bom) # Need fix here
    if (! (sym %in% rets.df$name)) {
      print (paste(i, 'getting from .BSE. ', sym))
      rets.df <- rbind(rets.df, getQuandl(sym, fetchsym, sp.df))
    }
  }
  
  rets.df$b6 <- ntile(- rets.df$r6m, 10)
  rets.df$b12 <- ntile(- rets.df$r12m, 10)
  rets.df$b52 <- ntile(rets.df$d52w, 10)
  rets.df$ball <- rets.df$b6 + rets.df$b12 + rets.df$b52
  rets.df <- rets.df[order(rets.df$ball),]



  return (rets.df)
}

main_buckets_US <- function() {
  ind <- read.csv('master_us.csv', stringsAsFactors = FALSE)
  rets.df <- NULL
  
  #ind <- ind[1:20,]
  
  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Symbol']
    fetchsym <- paste0('YAHOO/',sym)
    print (paste('getting from Quandl Yahoo ', sym))
    rets.df <- rbind(rets.df, getQuandl_US(sym, fetchsym, 'Adjusted Close'))
  }
  
  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Symbol'] 
    fetchsym <- paste0('GOOG/NYSE_',sym)
    if (! (sym %in% rets.df$symbol)) {
      print (paste('getting from GOOG NYSE ', sym))
      rets.df <- rbind(rets.df, getQuandl_US(sym, fetchsym, 'Close'))
    }
  }
  
  for (i in 1:nrow(ind)) {
    sym <- ind[i, 'Symbol'] 
    fetchsym <- paste0('GOOG/NASDAQ_',sym)
    if (! (sym %in% rets.df$symbol)) {
      print (paste('getting from NASDAQ ', sym))
      rets.df <- rbind(rets.df, getQuandl_US(sym, fetchsym, 'Close'))
    }
  }
  
  
  return (rets.df)
}

# For reference ...
main_monthly_hist <- function() {
  sp.df <- read.csv('splits.csv')
  rets.df <- getQuandl_monthly('KOTAKBANK', sp.df)
  #ind <- ind[1:2,]
  for (i in 1:nrow(ind)) {
    print (ind[i, 'Symbol'])
    rets.df <- rbind(rets.df, getQuandl_monthly(ind[i,'SymbolQ'], sp.df))
  }
  return (rets.df)
}
