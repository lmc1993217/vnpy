# -*- coding: utf-8 -*-
import os
import csv
import datetime


for root, dirs, files in os.walk(r'D:\TBData'):
    for f in files:
        if '888' not in f:
            continue
        targetCsv = os.path.join(root,f)
        outputPath = os.path.join(root,'dealed')
        outputCsv = os.path.join(outputPath,f)

        csvfile = file(targetCsv,'rb')
        csvfile_out = file(outputCsv,'wb')
        reader=csv.reader(csvfile)
        writer = csv.writer(csvfile_out)
        writer.writerow(('Date','Time','Open','High','Low','Close','TotalVolume','OpenInterest'))
        for line in reader:
            
        	#a,b,c,d,e,f,g,h = line
            date,time,o,h,l,c,volume,openInterest = line
            date = datetime.date(int(date[:4]),int(date[4:6]),int(date[6:]))
            time = float(time)
            time *= 10000
            time = int(time)
            time = datetime.time(time/100,time%100)
            dt = datetime.datetime.combine(datetime.date.today(), time) + datetime.timedelta(minutes=1)
            time = dt.time()
            writer.writerow((date,time,o,h,l,c,volume,openInterest))
        #	try:
        #		aa = a.split(' ')
        #		day = aa[0]
        #		m1,m2 = aa[1].split(':')
        #		n1 = int(m1)
        #		n2 = int(m2)
        #		n1 += (n2+1)/60
        #		if n1==24:
        #			h1=0
        #		else:
        #			h1=n1
        #		n2 = (n2+1)%60
        #		aaa = '%02d:%02d:00' %(h1,n2)
        #		writer.writerow((day,aaa,b,c,d,e,f,h))
        #	except:
        #		writer.writerow((a,b,c,d,e,f,g,h))
        csvfile.close()
        csvfile_out.close()