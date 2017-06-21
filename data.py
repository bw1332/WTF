# coding=utf8
# install reqests/yaml/json/ 
# sudo apt-get install python-mysqldb
import requests, time, json, yaml, re, time, MySQLdb
from time import strptime

# return request_id if exist, @para token
def executeCrawler(user_id, name, token):
  r = requests.post('https://api.apifier.com/v1/' + user_id + '/crawlers/' + name + '/execute?token=' + token)
  ret = r.json()
  return ret['_id'] if ret.has_key('_id') else None

# return running status
# expect to return SUCCESSED
def checkCrawler(reuqest_id, period):
  if (reuqest_id == None):
    return
  while True:
    time.sleep(period)
    ret = requests.get('https://api.apifier.com/v1/execs/' + reuqest_id).json()
    status = ret['status'] if ret.has_key('status') else None
    if (status != 'RUNNING'):
      return status


# fetch results
def fetchJsonResults(request_id):
  if (request_id == None):
      return
  r = requests.get('https://api.apifier.com/v1/execs/' + request_id + '/results?format=json&simplified=1&offset=0&limit=1&desc=1&attachment=1&bom=0')
  size = r.headers.get('x-apifier-pagination-total')
  offset = 0
  limit = 99 if int(size) > 99 else int(size)
  connection = connectDB('localhost', 'new', 'new', 'TEST')
  connection.set_character_set('utf8')

  while int(size) - offset > 0:
      r = requests.get('https://api.apifier.com/v1/execs/' + request_id + '/results?format=json&simplified=1&offset=' + str(offset) + '&limit=' + str(limit) + '&desc=1&attachment=1&bom=0')
      # 瞎jb写的 hard code 改到配置文件去
     
      for one in r.json():
        print(one)
        
        city = transfer(one['city'])
        state = transfer(one['state'])
        # check state
        state_id = selectDB(connection, getIdByNameSQL(state, 'name', 'location'))
        if state_id == None and state != None and len(state) > 0:
          print("state")
          insertDB(connection, insertSQL([state], ['name'], ['str'], 'location'))
        state_id = selectDB(connection, getIdByNameSQL(state, 'name', 'location'))
        # check city
        city_id = selectDB(connection, getIdByNameSQL(city, 'name', 'location'))
        if city_id == None and city != None and len(city) > 0:
          print("city")
          insertDB(connection, insertSQL([city, state_id], ['name', 'parent'], ['str', 'int'],'location'))
        city_id = selectDB(connection, getIdByNameSQL(city, 'name', 'location'))

        address_id = None
        if one['address'] != None:
          print("address")
          insertDB(connection, insertSQL([one['address']], [address], ['str'], 'address'))
          address_id = getIdByNameSQL(connection, getIdByNameSQL(one['address'], 'address', 'address'))

        category_id = selectDB(connection, getIdByNameSQL(one['category'], 'name', 'category'))

        sources = [one['subject'], category_id, address_id, state_id, city_id, one['contact_name'], one['contact_phone'], one['contact_email'], one['date'], one['description']]
        columns = ['title', 'category_id', 'address_id', 'state_id', 'city_id', 'contact_name', 'contact_phone', 'contact_email', 'created_at', 'job_detail']
        types = ['str', 'int', 'int', 'int', 'int', 'str', 'str', 'str', 'date', 'str']
        print("job")
        insertDB(connection, insertSQL(sources, columns, types, 'job'))
       

      offset += limit
  return

# read from config file
def conf(path):
  global ID, TOKEN, CRAWLERS, COLUMNS, PERIOD, MAP
  with open(path) as conf:
    y = yaml.load(conf)
    ID = y.get('id');
    TOKEN = y.get('token')
    CRAWLERS = y.get('crawlers')
    COLUMNS = y.get('columns')
    PERIOD = y.get('period')
    MAP = y.get('map')
  return

# database insert
# types includes INT, STR, DATE
# format for date is same as mysql yyyy-mm-dd HH:MM:SS
def insertSQL(sources, columns, types, table):
    if  (not isinstance(sources, list)) or (not isinstance(columns, list)) or (not isinstance(types, list)) or (not isinstance(table, str)) or len(sources) != len(columns) or len(types) != len(sources):
      return "False1"
    sql = u"INSERT INTO " + table + " ("
    for i, tar in enumerate(columns):
      if i < len(columns) - 1:
        sql += str(tar) + ", "
      else:
        sql += str(tar)
    sql += ") VALUES ("
    for i, e in enumerate(sources):

      if e == None:
        sql += "null" 
      elif types[i].lower() == "int":
        # add digit 
        sql += ''.join(re.findall("\d+", str(e)))
      elif  types[i].lower() == "str":
          sql += ("'" + e + "'")
      elif  types[i].lower() == "date":
        #
        if re.match("^\d{4}-\d{1,2}-\d{1,2}(\s+\d{2}:\d{2}(:\d{2})?)?$", str(e)):
          try:
            if re.match("^\d{4}-\d{1,2}-\d{1,2}$", str(e)):
              strptime(str(e), "%Y-%m-%d")
            elif re.match("^\d{4}-\d{1,2}-\d{1,2}\s+\d{2}:\d{2}$", str(e)):
              strptime(str(e), "%Y-%m-%d %H:%M")
            elif re.match("^\d{4}-\d{1,2}-\d{1,2}\s+\d{2}:\d{2}:\d{2}$", str(e)):
              strptime(str(e), "%Y-%m-%d %H:%M:%S")
            else:
              return "False2"
          except Exception:
             return "False3"
          sql += ("'" + str(e) + "'")
        else:
          return "False4"
      else:
        return "False5"
      if i < len(sources) - 1:
        sql += ","
    sql += ");"
    #sql = sql.encode('utf8')
    return sql

def connectDB(host, user, passwd, db):
  try: 
    return MySQLdb.connect(host, user, passwd, db)
  except Exception:
    return None


def insertDB(connect, sql):
  print(sql)
  if connect != None and sql != None:
    #try:
      cursor = connect.cursor()
      cursor.execute(sql)
      connect.commit()
    #except Exception:
    #print("Error: unable to insert data")

def getIdByNameSQL(name, column, table):
  if (isinstance(name, str)):
      name = name.strip()
  if name != None and len(name) > 0 and isinstance(column, str) and isinstance(table, str):
    print len(name)
    return 'select id from ' + str(table) + ' where ' + str(column) + ' = \'' + name + '\';'
  else:
    return None

def selectDB(connect, sql):
  print(sql)
  if connect != None and sql != None:
    try:
      cursor = connect.cursor()
      cursor.execute(sql)
      results = cursor.fetchone()
      return results[0] if results !=None else None
    except Exception:
      print("Error: unable to fecth data")
  else:
   return None

# 
def insertJson(json, con):
  columns = ['created_at', 'title', 'category_id','address_id','contact_phone','contact_name','contact_email', 'city_id', 'state_id']
  sources = [json['date'],json['subject'], '1', '1',json['contact_phone'], json['contact_name'],json['contact_email'], '1','1']
  types = ['date', 'str', 'int', 'int','int','str','str','int','int']
  sql = insertSQL(sources, columns, types, 'job')
  print(sql)
  insertDB(con, sql)

# transfer using the MAP
def transfer(ustring):
  if MAP.has_key(ustring):
    return MAP[ustring]
  elif re.match(u"[\u4e00-\u9fa5\u3040-\u309f\u30a0-\u30ff]+", ustring) != None:
    return None
  else:
    return ustring



if __name__=='__main__':  
  # id = executeCrawler('MTvuABJdiQtWayfKy', 'testloop', '3RQaxLLEo2wsPdc7AwW8KpK7p')
  # if (checkCrawler(id, 10) == 'SUCCEEDED'):
  #   fetchJsonResults(id)
  conf('data_conf.yaml')
  for crawler in CRAWLERS:
    id = executeCrawler(ID, crawler.get('name'), crawler.get('token'))
    print("wat")
    if checkCrawler(id, PERIOD) == 'SUCCEEDED':
      fetchJsonResults(id)
    else:
      print("FAIL")  


#   sources = ["123", "avdss", "1234-122-11"]
#   columns = ["_ID","_NAME", "_WHNE"]
#   types = ["int","str","date"]
#   print((not isinstance(types, str)))
#   print((not isinstance(sources, list)) or (not isinstance(columns, list)) or (not isinstance(types, str)) or (not isinstance(table, str)) or len(sources) != len(columns) or len(types) != len(sources))
#   res = insertSQL(["223", "avdss", "2014-2-3 12:11"], ["_ID","_NAME", "_WHNE"],["int","str","date"], "test")
#   print(res)
