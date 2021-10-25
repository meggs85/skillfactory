import sqlite3
import os, os.path
import pandas as pd
import time as tm
from datetime import datetime, date, time
import numpy as np
import pathlib as pl
from sys import platform

#cache_file_name = 'way-prokat_78.sdb'
#cache_file_name = 'way-prokat_81.sdb'
cache_file_name = 'way-prokat_84.sdb'

if platform == "linux" or platform == "linux2":
    # linux
    pass
elif platform == "darwin":
    # OS X
    dbPath = f"/Users/{os.environ['USER']}/Yandex.Disk.localized/dashboard_wayprokat/db/" + cache_file_name
elif platform == "win32":
    dbPath = f"{os.environ['USERPROFILE']}\\YandexDisk\\dashboard_wayprokat\\db\\" + cache_file_name

#path = pl.Path(dbPath)
#last_modified = path.stat().st_mtime
#print(datetime.utcfromtimestamp(last_modified).strftime('%Y-%m-%d %H:%M:%S'))

print("последнее обновление БД: %s" % tm.ctime(os.path.getmtime(dbPath)))



DEBUG = False

if not os.path.exists(dbPath):
    print('файл БД {} не найден'.format(dbPath))
    sys.exit(-1)

con = sqlite3.connect(dbPath)

def SelectDealsCreated(date1, date2, ignore_statuses):
    global con
    if DEBUG: print('SelectDealsCreated()')
    curs = con.cursor()
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    #and (ASSIGNED_BY_ID = {})
    _from = '(Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID)'
    cmd = 'select * from {} where (DATE_CREATE between \'{}\' and \'{}\') and (STAGE_ID not {})'.format(_from, date1, date2_, GlueINCond(ignore_statuses))
    if DEBUG: print(cmd)
    return pd.DataFrame(curs.execute(cmd).fetchall())

def GlueINCondINT(lst):
    in_cond = 'IN(' 
    for i in range(0, len(lst)):
        in_cond += str(lst[i])
        if i< len(lst)-1:
            in_cond += ', '
    in_cond += ')'
    return in_cond

def GlueINCond(lst):
    in_cond = 'IN(' 
    for i in range(0, len(lst)):
        in_cond += '\''
        in_cond += lst[i]
        in_cond += '\''
        if i< len(lst)-1:
            in_cond += ', '
    in_cond += ')'
    return in_cond
    
def GlueINCondBrackets(lst):
    in_cond = 'IN(' 
    for i in range(0, len(lst)):
        in_cond += '\'['
        in_cond += str(lst[i])
        in_cond += ']\''
        if i< len(lst)-1:
            in_cond += ', '
    in_cond += ')'
    return in_cond 


def SelectStartArendaDealsBetween(date1, date2, status_list, manager=None):
    global con
    if DEBUG: print('SelectStartArendaDealsBetween()')
    #date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день, т к Ч:М:С прижимаются к 00:00:00
    curs = con.cursor()
    if manager == None:
        managerCond = ''
    else:
        managerCond = 'and Deals.ASSIGNED_BY_ID = {}'.format(manager)
    _from = '(Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID)'
    in_set_success = GlueINCond(status_list)
    cmd = 'select * from {} where (Deals.STAGE_ID {}) and (DealUserFields.UF_CRM_DATESTART between \'{}\' and \'{}\') {}'.format(_from, in_set_success, date1+ pd.DateOffset(days=-1), date2, managerCond)
    data = pd.DataFrame(curs.execute(cmd).fetchall())
    if DEBUG: print(cmd)
    if DEBUG: display(data)
    return data
    
'''    
def SelectSuccessDealsBefore(date2, status_list, manager=None):
    global con
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    if manager == None:
        managerCond = ''
    else:
        managerCond = 'and Deals.ASSIGNED_BY_ID = {}'.format(manager)
    _from = '(Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID)'
    in_set_success = GlueINCond(status_list)
    cmd = 'select * from {} where (Deals.STAGE_ID {}) and Deals.CLOSEDATE<\'{}\' {}'.format(_from, in_set_success, date2_, managerCond)
    if DEBUG: print(cmd)
    data = curs.execute(cmd).fetchall()
    if (len(data)==0):
        return pd.DataFrame()
    df = pd.DataFrame(data)    
    return df
'''

def GetPrevDeals(contact_ids, dateBefore,  status_list, manager=None):
    global con
    if DEBUG: print('GetPrevDeals()', status_list)
    if (len(contact_ids)==0):
        return pd.DataFrame()
    
    curs = con.cursor()
    if manager == None:
        managerCond = ''
    else:
        managerCond = 'and Deals.ASSIGNED_BY_ID = {}'.format(manager)

    
    _from = '(Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID)'
    in_set_contact_ids = GlueINCondINT(contact_ids)
    in_set_success = GlueINCond(status_list)
    date_cond = '(Deals.CLOSED=1) and (Deals.CLOSEDATE < \'{}\')'.format(dateBefore)
    contact_cond = 'and (Deals.CONTACT_ID {})'.format(in_set_contact_ids)
    status_cond = 'and (Deals.STAGE_ID {})'.format(in_set_success)
    cmd = 'select * from {} where  {} {} {} {}'.format(_from, date_cond, contact_cond, status_cond, managerCond)
    if DEBUG: print(cmd)
    data = curs.execute(cmd).fetchall()
    if (len(data)==0): 
        return pd.DataFrame()
    return pd.DataFrame(data) 


def GetUserFieldsCount():
    global con
    curs = con.cursor()
    cmd = "SELECT * from DealUserFields LIMIT 1"
    data = pd.DataFrame(curs.execute(cmd).fetchall())
    return len(data.columns)

def SelectDealsReachedStatus(date1, date2, status_list, ignore_statuses, manager=None):
    global con
    if DEBUG: print('SelectDealsReachedStatus()', status_list)
    #date1 = np.datetime64(date1)
    #date2 = np.datetime64(date2)
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    if manager == None:
        managerCond = ''
    else:
        managerCond = 'and Deals.ASSIGNED_BY_ID = {}'.format(manager)
    _from = '(Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID) join Events_Deals on Events_Deals.EntityId=Deals.ID'
    in_set_success = GlueINCond(status_list)
    
    
    cmd = 'select * from {} where Events_Deals.StatusTitle {} and (Deals.STAGE_ID not {}) and (PreviousStatusID not {}) and (Events_Deals.UpdateDate between \'{}\' and \'{}\') {} order by Events_Deals.UpdateDate'.format(_from, in_set_success, GlueINCond(ignore_statuses), in_set_success, date1, date2_, managerCond)
    if DEBUG: print(cmd)
    data = pd.DataFrame(curs.execute(cmd).fetchall())
    if (len(data)==0): 
        return data
    data = data.groupby(0).head(1)
    if DEBUG: print('initial')
    if DEBUG: display(data)

    in_ids = GlueINCondINT(data[0].to_list())
    cmd2 = 'select * from {} where (Deals.ID {}) and (Events_Deals.StatusTitle {}) and (Events_Deals.UpdateDate < \'{}\') {}'.format(_from, in_ids, in_set_success, date1, managerCond)
    if DEBUG: print(cmd2)
    curs2 = con.cursor()
    substract = pd.DataFrame(curs2.execute(cmd2).fetchall())
    if (len(substract)==0): 
        return data
    substract = substract.groupby(0).head(1)
    if DEBUG: print('substract')
    if DEBUG: display(substract)
    data = data[~data[0].isin(substract[0])]
    if DEBUG: print('result')
    if DEBUG: display(data)
    return data
    
    
def SelectDealsStatusTransitions(date1, date2):
    global con
    if DEBUG: print('SelectDealsStatusTransitions()')
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    _from = 'Events_Deals join Deals on Events_Deals.EntityId=Deals.ID'
    cmd = 'select Events_Deals.EntityId, Events_Deals.UpdateDate, Events_Deals.PreviousStatusId, Events_Deals.StatusTitle, Deals.DATE_CREATE, Deals.ASSIGNED_BY_ID from {} where  (UpdateDate between \'{}\' and \'{}\') order by EntityId,UpdateDate'.format(_from, date1, date2_)
    df = pd.DataFrame(curs.execute(cmd).fetchall())
    if (len(df)==0):
        return pd.DataFrame(columns = ['deal_id', 'date', 't1', 't2', 'deal_date_create', 'manager_id'])
    df.columns = ['deal_id', 'date', 't1', 't2', 'deal_date_create', 'manager_id']
    df['deal_date_create'] = pd.to_datetime(df['deal_date_create'])
    return  df[~((pd.isnull(df.t1)) & ((df.deal_date_create < date1) | (df.deal_date_create > date2_)))]
    
    
def SelectGoodsDealsRent(date1, date2):
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    
    _from = 'Deals d join DealUserFields as duf on d.ID=duf.DEAL_ID join DealProducts dp on d.ID=dp.OWNER_ID join Products p on p.ID=dp.PRODUCT_ID join ProductSections ps on (p.CATALOG_ID=ps.CATALOG_ID and p.SECTION_ID=ps.ID)'
    
    cmd = 'select \
    d.ID as deal_id, \
    p.ID as product_id, \
    p.NAME as product_name, \
    p.DESCRIPTION as description, \
    ps.NAME as section_name, \
    duf.UF_CRM_DATESTART as date_rent_start, \
    duf.UF_CRM_DATEEND as date_rent_end,  \
    d.CATEGORY_ID as dir, \
    d.STAGE_ID as stage_id \
    from {} where  (date_rent_end >= DATE(\'{}\') and date_rent_start <= DATE(\'{}\')) \
    ORDER BY deal_id, product_id  '.format(_from, date1, date2_)
    
    #print(cmd)
    df = pd.DataFrame(curs.execute(cmd).fetchall())
    if (len(df)==0):
        return pd.DataFrame(columns = ['deal_id', 'product_id', 'product_name', 'description', 'section_name', 'date_rent_start', 'date_rent_end', 'dir', 'stage_id'])
    df.columns = ['deal_id', 'product_id', 'product_name', 'description', 'section_name',  'date_rent_start', 'date_rent_end', 'dir', 'stage_id']
    df['date_rent_start'] = pd.to_datetime(df['date_rent_start'])
    df['date_rent_end'] = pd.to_datetime(df['date_rent_end'])
    df.drop_duplicates(subset=['deal_id', 'product_id'], keep='last', inplace=True)
    return  df    
    
def SelectGoodsDealsStatusTransitions(date1, date2):
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    
    _from = 'Events_Deals ed join Deals d on ed.EntityId=d.ID join DealUserFields as duf on d.ID=duf.DEAL_ID join DealProducts dp on d.ID=dp.OWNER_ID join Products p on p.ID=dp.PRODUCT_ID join ProductSections ps on (p.CATALOG_ID=ps.CATALOG_ID and p.SECTION_ID=ps.ID)'
    
    cmd = 'select \
    DATE(ed.UpdateDate) as date, \
    ed.PreviousStatusId as s1, \
    ed.StatusTitle as s2, \
    d.ID as deal_id, \
    p.ID as product_id, \
    p.NAME as product_name, \
    p.DESCRIPTION as description, \
    ps.NAME as section_name, \
    duf.UF_CRM_DATESTART as date_rent_start, \
    duf.UF_CRM_DATEEND as date_rent_end,  \
    d.CATEGORY_ID as dir \
    from {} where  (ed.UpdateDate between \'{}\' and \'{}\') \
    ORDER BY deal_id, product_id, date '.format(_from, date1, date2_)
    
    #print(cmd)
    df = pd.DataFrame(curs.execute(cmd).fetchall())
    if (len(df)==0):
        return pd.DataFrame(columns = ['date', 's1', 's2', 'deal_id', 'product_id', 'product_name', 'description', 'section_name', 'date_rent_start', 'date_rent_end', 'dir'])
    df.columns = ['date', 's1', 's2', 'deal_id', 'product_id', 'product_name', 'description', 'section_name', 'date_rent_start', 'date_rent_end', 'dir']
    df['date_rent_start'] = pd.to_datetime(df['date_rent_start'])
    df['date_rent_end'] = pd.to_datetime(df['date_rent_end'])
    df['date'] = pd.to_datetime(df['date'])
    df.drop_duplicates(subset=['deal_id', 'product_id'], keep='last', inplace=True)
    return df
    #return  df[~((pd.isnull(df.t1)) & ((df.deal_date_create < date1) | (df.deal_date_create > date2_)))]


def SelectStatuses():
    global con
    curs = con.cursor()
    _from = 'Statuses'
    cmd = 'select NAME, NAME_INIT, STATUS_ID, SORT, Phase, CATEGORY_ID from {} where (ENTITY_ID LIKE \'DEAL_STAGE%\') order by CATEGORY_ID, Phase, SORT'.format(_from)
    df = pd.DataFrame(curs.execute(cmd).fetchall())
    return df

'''    
def SelectDistinctManagersIds(date1, date2, status_list, ignore_statuses):
    global con
    date2_ = pd.to_datetime(date2) + pd.DateOffset(days=1) #для системных DATETIME полей в Б24 вторую дату нужно сдвигать на 1 день
    curs = con.cursor()
    managerCond = ''
    _from = '((Deals join DealUserFields on DealUserFields.DEAL_ID=Deals.ID) join Events_Deals  on Events_Deals.EntityId=Deals.ID)'
    cmd = 'select DISTINCT Deals.ASSIGNED_BY_ID from {} where (Events_Deals.UpdateDate between \'{}\' and \'{}\') and (Events_Deals.StatusTitle {}) and (Deals.STAGE_ID not {}) {}'.format(_from, date1,date2_,GlueINCond(status_list), GlueINCond(ignore_statuses), managerCond)
    if DEBUG: print (cmd)
    data = curs.execute(cmd).fetchall()
    if len(data)>0:
        return pd.DataFrame(data).groupby(0).head(1)
    return pd.DataFrame(data)
'''
        
def GetManagers():
    global con
    curs = con.cursor()
    dep_cond = ''# 'UF_DEPARTMENT ' + GlueINCondBrackets(departments)
    active_cond = '' #and ACTIVE=1
    data = curs.execute('select * from Users').fetchall()
    return pd.DataFrame(data)