#-*- coding:utf-8 -*-
import requests
import pymysql
import logging.handlers
import sys
from  datetime  import  *
import time
from multiprocessing.dummy import Pool
reload(sys)
sys.setdefaultencoding('utf8')
SESSIONID_hk = \
    ['60fb71d5-bc69-4542-9ddf-26ea58a299f5',
     'c0f39887-a298-413b-b558-bbe5ed2caca8',
     '61e99e7e-37c6-4a85-8ffb-b85d5ef32372']
#香港 经纬度
lat = [21.7598596,23.3100761,22.4295795]
lng = [114.7155784,114.3932406,112.7521101]
global saveString_hk1
global saveString_hk2
global saveString_hk3
saveString_hk1 = []
saveString_hk2 = []
saveString_hk3 = []

handler = logging.handlers.RotatingFileHandler('/home/tst_hk7.txt', maxBytes=1024*1024*1024*1024,backupCount = 10) # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter

logger = logging.getLogger('logging')  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)

logger.info('first info message')

def changeLoca(s, longitude, latitude,sessionId):
    url = 'http://ios.skout.com/api/1/me/location'
    headers = {'User-Agent': 'SKOUT_SDK4/42120 (iPhone; iOS 10.1.1; Scale/2.00)',
                'Accept-Language': 'zh-cn',
                'session_id': sessionId,
               'Connection': 'close',
                'Accept-Encoding': 'gzip, deflate'}
    body = {'application_code': '6166025fd1e4ec9e2654488b84fd700f',
            'longitude': longitude, #经度
            'latitude': latitude,   #纬度
            'rand_token': '1200791925',
            'city_name': 'New%20York',
            'country_name': 'united%20States',
            'location_locale': 'en',
            'provider': 'Google',
            'state_name': 'New%20York'
            }
    for i in range(1,10):
        try:
            res = s.post(url, data=body, headers=headers,timeout=30.0)
        except:
            logger.info('change time out!')
            continue
        try:
        #操作成功
            temp = res.json()
            #改位置后休眠10秒 防止出错
            time.sleep(6)
            break
        except:
            logger.info('change location fail!!!!!?')
            continue

def getUserBuzz(s,sessionId,userId,xh):
    global inhk1
    global inhk2
    global inhk3
    url = 'http://ios.skout.com/api/1/users/'+str(userId)+'/buzzes?rand_token=1727809466&since_id=0&application_code=6166025fd1e4ec9e2654488b84fd700f&count=200'
    headers = {'Accept': '*/*',
               'Accept-Language': 'zh-cn',
               'Accept-Encoding': 'gzip, deflate',
               'User-Agent': 'SKOUT_SDK4/42120 CFNetwork/808.1.4 Darwin/16.1.0',
               'session_id': sessionId,
               'api_version': '52',
                'Connection': 'close'
               }
    gett = False
    buzzCnt = 0

    for i in range(1,10):
        try:
            res = requests.get(url, headers=headers, timeout=30.0)
            content = res.json()
            buzzList = content['elements']
            buzzCnt = buzzCnt + len(buzzList)
            gett = True
            break
        except Exception as e:
            logger.info("get buzz fail or json analysis fail, retry..."+str(userId)+e.message)
            continue

    current_time = time.localtime(time.time())
    if gett == False:

        if xh == 1:
            saveString = (
                'INSERT INTO realtime_distance_hk1(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                    userId, 0, 1, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk1.append(saveString)
        elif xh == 2:
            saveString = (
                'INSERT INTO realtime_distance_hk2(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                    userId, 0, 1, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk2.append(saveString)
        elif xh == 3:
            saveString = (
                'INSERT INTO realtime_distance_hk3(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                    userId, 0, 1, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk3.append(saveString)
        return userId

    if len(buzzList) == 0: #没有动态
        ifnobuzz = 1
        if xh == 1:
            saveString = (
            'INSERT INTO realtime_distance_hk1(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk1.append(saveString)
        elif xh == 2:
            saveString = (
            'INSERT INTO realtime_distance_hk2(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk2.append(saveString)
        elif xh == 3:
            saveString = (
            'INSERT INTO realtime_distance_hk3(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, -1, -1, "", "", 0,current_time.tm_mday, current_time.tm_hour,""))
            saveString_hk3.append(saveString)
        return userId

    ifnobuzz = 0
    buzz0 = buzzList[0]
    singleBuzz = buzz0['buzz']
    creator = singleBuzz['creator']
    traveling = creator['traveling']
    location = creator['location']
    distance_mi = location['distance']
    distance_km = location['distance_km']
    state = location['state']
    state = state.replace('\'', '')
    country = location['country']
    country = country.replace('\'','')
    city = location['city']
    city = city.replace('\'',' ')
    if traveling == True:
        travelingInt = 1
    elif traveling == False:
        travelingInt = 0

    if xh == 1:
        saveString = (
            'INSERT INTO realtime_distance_hk1(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, distance_km, distance_mi, city, country, travelingInt,current_time.tm_mday,current_time.tm_hour,state))
        saveString_hk1.append(saveString)
    elif xh == 2:
        saveString = (
            'INSERT INTO realtime_distance_hk2(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, distance_km, distance_mi, city, country, travelingInt, current_time.tm_mday,current_time.tm_hour,state))
        saveString_hk2.append(saveString)
    elif xh == 3:
        saveString = (
            'INSERT INTO realtime_distance_hk3(user_id,ifnobuzz,iferror,distance_km,distance_mi,city,country,traveling,time_day,time_hour,state) VALUES(\'%s\',%d,%d,%f,%f,\'%s\',\'%s\',%d,%d,%d,\'%s\')' % (
                userId, ifnobuzz, 0, distance_km, distance_mi, city, country, travelingInt,current_time.tm_mday,current_time.tm_hour,state))
        saveString_hk3.append(saveString)

    return userId
def Bar(arg):
    a = 1

if __name__ == '__main__':


    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='zhuzhu66',db='skout_realtime', charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    cur.execute("USE skout_realtime")
    tableIs = {}

    cur.execute("SELECT user_id from userinfo_hk where id > 30000 AND id <= 35000")
    userIDList = cur.fetchall()

    s = requests.Session()

    while True:
        current_time = time.localtime(time.time())
        if current_time.tm_min < 55 and current_time.tm_min != 0:
            logger.info('wait...min:' + str(current_time.tm_min))
            time.sleep(300)
        #if True:
        if ((current_time.tm_min == 0) and (current_time.tm_sec == 0)):
            #清空mysql操作语句
            saveString_hk1 = []
            saveString_hk2 = []
            saveString_hk3 = []
            t_start = time.time()
            current_time = time.localtime(time.time())
            logger.info('start! day %d hour %d' % (current_time.tm_mday,current_time.tm_hour))

            changeLoca(s, lng[0], lat[0], SESSIONID_hk[0])
            changeLoca(s, lng[1], lat[1], SESSIONID_hk[1])
            changeLoca(s, lng[2], lat[2], SESSIONID_hk[2])

            t_start = time.time()
            pool = Pool(processes=240)  # 设置进程数量

            for i in range(0, 2500):
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[0], userIDList[i]['user_id'], 1,),
                                 callback=Bar)  # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[1], userIDList[i]['user_id'], 2,),
                                 callback=Bar)
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[2], userIDList[i]['user_id'], 3,),
                                 callback=Bar)

            pool.close()
            pool.join()  # 关闭之后要计入它，作用：防止主程序在子进程结束前关闭

            t_end = time.time()
            t = t_end - t_start
            logger.info('the program time1 is :%s' % t)

            time.sleep(60)
            logger.info('sleep....')

            t_start = time.time()
            pool = Pool(processes=240)  # 设置进程数量
            for i in range(2500, 5000):
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[0], userIDList[i]['user_id'], 1,),
                                 callback=Bar)  # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[1], userIDList[i]['user_id'], 2,),
                                 callback=Bar)
                pool.apply_async(func=getUserBuzz, args=(s, SESSIONID_hk[2], userIDList[i]['user_id'], 3,),
                                 callback=Bar)
            pool.close()
            pool.join()  # 关闭之后要计入它，作用：防止主程序在子进程结束前关闭

            t_end = time.time()
            t = t_end - t_start
            logger.info('the program time2 is :%s' % t)

            logger.info('down search! day %d hour %d' % (current_time.tm_mday, current_time.tm_hour))
            print 'the program time is :%s' % t

            for i in saveString_hk1:
                try:
                    cur.execute(i)
                    cur.connection.commit()
                except Exception as e:
                    logger.info('save fail??????? '+str(i)+ ' 1 '+ e.message)

            for i in saveString_hk2:
                try:
                    cur.execute(i)
                    cur.connection.commit()
                except Exception as e:
                    logger.info('save fail??????? ' + str(i) + ' 2 ' + e.message)

            for i in saveString_hk3:
                try:
                    cur.execute(i)
                    cur.connection.commit()
                except Exception as e:
                    logger.info('save fail??????? ' + str(i) + ' 3 ' + e.message)


            logger.info('down! day %d hour %d' % (current_time.tm_mday, current_time.tm_hour))




