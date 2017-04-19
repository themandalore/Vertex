# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 10:13:01 2016

@author: nfett
"""

import zipfile,os,urllib
import time,requests

from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2016, 1, 1)
end_date = date(2017, 4, 17)
    
''' note default ES[0] is front month, ES[1] is one co out and so on'''
products=['ES'] 
'''date format- yyyymmdd'''
'''enter armada or rapid'''
dataset = 'rapid' 
dirname =  'S:\\NFett\\Liquidity\\Liquidity_Report\\Vertex_Data\\'
                   
def getHistory(product_id = '', date=''): 
    #payload = {"symb" : product_id, "helm_date" : date, "helm_query" : 'HELM_BOOK_TRD'}
    y = 0
    x=0
    while y < 1 and x< 20:
        response = requests.get(url)
        data = response.json()                
        print (data['RTickLead']['fExtnDesc']+'...')
        if  data['RTickLead']['fExtnDesc'] == 'READY':
            y = 2
            return data
        if  'DATA UNAVAILABLE' in  data['RTickLead']['fExtnDesc']:
            break
        else:
            x = x +1
            if x == 20:
                print ('API connection timeout...')
            time.sleep(5)
for product_id in products:
    print (product_id)      
    for date_f in daterange(start_date, end_date):
        date = date_f.strftime("%Y%m%d")
        print (date)
        filename =  'vertex_'+dataset+'_'+date+'_'+product_id[:2]+'.csv'
        nfile = 'vertex_'+dataset+'_'+date+'_'+product_id[:2]+'.csv'
        if os.path.exists(dirname + nfile):
            print ('File already exists')
        else:        
            if dataset == 'rapid':
                url='http://restv3.vertex-analytics.com:8080/ECHO.eco/serv=FAST/user=CFTC/vers=201/form=3/type=7/symb='+product_id+'/helm_date='+date+'/helm_query=HELM_OREC_ALL'
            else:
                url='http://restv3.vertex-analytics.com:8080/ECHO.eco/serv=FAST/user=CFTC/vers=201/form=3/type=7/symb='+product_id+'/helm_date='+date+'/helm_query=HELM_BOOK_QUO/helm_depth=10'
            print (url)
            all_data = getHistory(product_id, date)
            if all_data == None:
                print ('Data Error, retry selections')
            else:
                url = all_data['RTickLead']['fLeadPath']
                print ('Zip URL: ',url)
                print "downloading..."
                urllib.urlretrieve(url, dirname+"code.zip")
                try:
                    zfile = zipfile.ZipFile(dirname+'code.zip')
                    for name in zfile.namelist():
                      print "Decompressing " + filename + " to " + dirname+ "...."
                      zfile.extract(name, dirname)
                      os.rename(dirname + name,dirname + nfile)
                except:
                    print ('Bad Zip file, try again...(usually contract is non-existent or very thin)')
        
    
    
    
    
