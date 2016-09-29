library(RMySQL)
library(stringr)

startTime=as.character(Sys.time())
con <- dbConnect(RMySQL::MySQL(), host = "192.168.1.249",
                 user = "root", password = "root",dbname='meituan_comment')
dbSendQuery(con,'SET NAMES gbk')


main<-function(plat){


#plat='order_eleme_valid_update'
if(plat=='order_eleme_valid_update'){
  platname='饿了么'
}
  
if(plat=='mobile_baidu_order'){
  platname='百度外卖'
}
if(plat=='meituan_order_valid'){
  platname='美团'
}
if(plat=='order_app'){
  platname='App'
}
if(plat=='order_weixin'){
  platname='微信'
}




sql<-str_c("SELECT xiadan_time,username,address,userphone,short_shopname,shopname,city_name,area,shifu FROM ",plat)

#获取到meituan_order_valid用户数据
myresult<-dbFetch(dbSendQuery(con,sql),n=-1)

#计算M值，与用户总金额相关
RMF_M<-aggregate(as.numeric(myresult$shifu),list(myresult$userphone),sum)
names(RMF_M)<-c('M-phone','M')


#去重的第二种方法
# myreslt<-as.data.frame(unique(myresult$userphone))
# myresult_dup<- myresult[!duplicated(myresult$userphone),] 
# iris<-iris[!duplicated(iris$Species),]

#合并到新的数据框
new_data_frame<-data.frame('','','','','','','','','','','','','','',stringsAsFactors = FALSE)
names(new_data_frame)<-c('xiadan_time','username','address','userphone','short_shopname','shopname','city_name','area','shifu','R_score','F_score','M_score','RMF','levels')
new_data_frame<-cbind(new_data_frame,RMF_M)
new_data_frame$R_score<-as.numeric(new_data_frame$R_score)
new_data_frame$F_score<-as.numeric(new_data_frame$F_score)
new_data_frame$M_score<-as.numeric(new_data_frame$M_score)

#M_score<-0
#进行M分数的计算
new_data_frame[new_data_frame$M<=28,"M_score"]<-1
new_data_frame[new_data_frame$M>28&new_data_frame$M<=70,"M_score"]<-2
new_data_frame[new_data_frame$M>70&new_data_frame$M<=110,"M_score"]<-3
new_data_frame[new_data_frame$M>110&new_data_frame$M<=180,"M_score"]<-4
new_data_frame[new_data_frame$M>180,"M_score"]<-5
#进行F分数的计算
RMF_F<-as.data.frame(table(myresult$userphone))
names(RMF_F)<-c("F-phone","F")
new_data_frame<-cbind(new_data_frame,RMF_F)
new_data_frame[new_data_frame$F==1,"F_score"]<-1
new_data_frame[new_data_frame$F==2,"F_score"]<-2
new_data_frame[new_data_frame$F==3,"F_score"]<-3
new_data_frame[new_data_frame$F>=4&new_data_frame$F<=5,"F_score"]<-4
new_data_frame[new_data_frame$F>=6,"F_score"]<-5
#进行R分数的计算,涉及到时间的距离计算
Order_R<-myresult[order(myresult$userphone,decreasing = TRUE),]
#all time change the unix
unix_time<-as.numeric(as.POSIXct(Order_R$xiadan_time, format="%Y-%m-%d"))
today_unix_times<-as.numeric(as.POSIXct(Sys.Date(),format="%Y-%m-%d"))
Order_R<-cbind(Order_R,time_diff=today_unix_times-unix_time)


extract_max_time<-as.data.frame(aggregate(Order_R$time_diff,list(Order_R$userphone),max),stringAsFactors=FALSE)
names(extract_max_time)<-c('phone','time')

#将分数差附在最后数据框之后
new_data_frame<-new_data_frame[order(new_data_frame$`M-phone`,decreasing = FALSE),]
new_data_frame<-cbind(new_data_frame,tt=extract_max_time$time)
new_data_frame$R_score<-1
new_data_frame<-na.omit(new_data_frame)


second_month<-30*24*3600
new_data_frame[new_data_frame$tt<2592000,"R_score"]=5
new_data_frame[new_data_frame$tt>=1*second_month&new_data_frame$tt<=3*second_month,"R_score"]=4
new_data_frame[new_data_frame$tt>3*second_month&new_data_frame$tt<=4*second_month,"R_score"]=3
new_data_frame[new_data_frame$tt>4*second_month&new_data_frame$tt<=7*second_month,"R_score"]=2
new_data_frame[new_data_frame$tt>7*second_month&new_data_frame$tt<=12*second_month,"R_score"]=1



new_data_frame$RMF=new_data_frame$R_score+new_data_frame$F_score*3+new_data_frame$M_score*4
new_data_frame[new_data_frame$RMF>=1&new_data_frame$RMF<=16,"levels"]<-'普通会员'
new_data_frame[new_data_frame$RMF>=17&new_data_frame$RMF<=25,"levels"]<-'高级会员'
new_data_frame[new_data_frame$RMF>=26&new_data_frame$RMF<=35,"levels"]<-'VIP会员'
new_data_frame[new_data_frame$RMF>=36&new_data_frame$RMF<=40,"levels"]<-'至尊VIP'
new_data_frame[new_data_frame$RMF>=1&new_data_frame$RMF<=16,"F"]<-'半年一次八折'
new_data_frame[new_data_frame$RMF>=17&new_data_frame$RMF<=25,"F"]<-'3个月一次8折'
new_data_frame[new_data_frame$RMF>=26&new_data_frame$RMF<=35,"F"]<-'2个月一次8折'
new_data_frame[new_data_frame$RMF>=36&new_data_frame$RMF<=40,"F"]<-'1个月一次8折'

#如果需要导出数据，执行下面的语句
# write.csv(new_data_frame,str_c('d:/RMF_',plat,'.csv'))
# 
# new_data_frame<-cbind(new_data_frame,platname)
# colnames(new_data_frame)
# 
# new_data_frame$xiadan_time<-NULL
# new_data_frame$shifu<-NULL
# new_data_frame$M<-NULL
# 
# dbWriteTable(con,'member_rfm',new_data_frame,append=TRUE,row.names=FALSE)

#---------------------------------------------------------------------------------------
#Add Data into database
insert_sql<-sprintf('INSERT INTO `member_rfm`  values ("%s","%s","%s",
                    "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")',startTime,'2016-08-29',
                    new_data_frame$username,new_data_frame$address,new_data_frame$userphone,new_data_frame$short_shopname,new_data_frame$shopname,platname
                    ,new_data_frame$city_name,new_data_frame$area,new_data_frame$R_score,new_data_frame$F_score,new_data_frame$M_score,
                    new_data_frame$RMF,new_data_frame$levels,new_data_frame$`M-phone`,new_data_frame$`F-phone`,new_data_frame$F,new_data_frame$tt,1,0
                    )

for(i in 1:length(insert_sql)){
  
  tryCatch(dbSendQuery(con,insert_sql[i]),error=function(e) print(e))
  
}

update_tt_sql<-sprintf('update `member_rfm` set `tt`=%s where `M-phone`=%s',new_data_frame$tt,new_data_frame$`M-phone`)
for(i in 1:length(update_tt_sql)){
  tryCatch(dbSendQuery(con,update_tt_sql[i]),error=function(e) print(e))
}

}

plat<-c('order_eleme_valid_update','mobile_baidu_order','meituan_order_valid',
        'order_app','order_weixin')
lapply(plat,main)

update_buy_das_sql<-'update member_rfm set buy_day=tt/86400'
dbSendQuery(con,update_buy_das_sql)



#计算首次购买时间

plat='meituan_order_valid'
if(plat=='order_eleme_valid_update'){
  platname='饿了么'
}

if(plat=='mobile_baidu_order'){
  platname='百度外卖'
}
if(plat=='meituan_order_valid'){
  platname='美团'
}


plat.userphone.sql<-sprintf("select userphone from member_rfm where plat='%s'",platname)
plat.userphone<-dbSendQuery(con,plat.userphone.sql)
plat.userphone<-dbFetch(plat.userphone,n=-1)


query.time.by_userphone<-sprintf("select min(xiadan_time),userphone from %s WHERE userphone='%s'",
                                 plat,plat.userphone$userphone)

userphone<-c()
first_time<-c()
firsttime.df<-data.frame(first_time,userphone)

for(query in query.time.by_userphone){
  print(query)
  newdf<-rbind(firsttime.df,dbFetch(dbSendQuery(con,query),n=-1))
  
}

query.time<-dbFetch(dbSendQuery(con,query.time))





















