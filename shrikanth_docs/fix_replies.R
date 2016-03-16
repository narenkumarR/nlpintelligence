library(readxl)
library(plyr)
mails = read_excel('campaign_replies.xls',sheet = 1)
mails = mails[!is.na(mails$Email),]
mails$Time = as.POSIXct(mails$Time,format = "%a, %d %b %Y %H:%M:%S %z")

mails1 <- mails[order(mails$Email,mails$Subject,mails$Time),]
mails1 = ddply(mails,.(Email,Subject),transform,mail_order = seq(1,length(Email)))
mails1$first_mail = 0
mails1[which(mails1$mail_order==1),'first_mail'] = 1
mails1 = mails1[which(mails1$Email!='error' & mails1$Email != ''),]
# write.csv(mails1,'campaign_replies1.csv',quote=TRUE,row.names=F)
write.xlsx(mails1,file = 'campaign_replies1.xlsx')

