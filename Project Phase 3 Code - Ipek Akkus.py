# İpek Akkuş 30800

import mysql.connector
from mysql.connector import errorcode

def create_connection():
    try:
        cnx = mysql.connector.connect(
            user="root", password="Fener*190702134", database="mydb",
        )
        print("Connection established with the database")
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:   
            print(err)
        #cnx.close()
        return None
    


def columns_string(columns):
    temp = ""
    for col in columns:
        temp += col + ", "
    temp = temp.strip()[:-1]
    return temp


def values_string(columns):
    temp = ""
    for col in columns:
        temp += "%(" + col + ")s, "
    temp = temp.strip()[:-1]
    return temp


def filter_generation(query, condition=None, filters=None):
    """'Generates filters based on a dictionary array which contains {field,value,operator} keys or directly with given condition string"""
    if condition != None:
        query += condition

    elif filters != None:
        query += " Where "
        try:
            for filter in filters:
                query += (
                    filter["field"]
                    + " "
                    + filter["operator"]
                    + " {}".format(filter["value"])
                    + " AND "
                )
        except:
            print("The filters array has items which are not a dictionary object")
            return False

        # To get rid of excessive AND at the end, strip and do not take last 4 char
        query = query.strip()[:-4]
    return query


def set_query_generation(query, values=None):
    """'Generates value setter based on a dictionary which has the column name as keys and corresponding values"""

    print(values)
    if values != None:
        query += " SET "
        try:
            for key, val in values.items():
                query += key + " = " + " '{}'".format(val) + " , "
        except:
            print("The values is not a dictionary")

        # To get rid of excessive AND at the end, strip and do not take last 4 char
        query = query.strip()[:-2]
    return query


def create_operation(cursor, TABLES):
    """Create tables with a given dictionary object which has table names as keys and corresponding create statements"""
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end="")
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    return True


def insert_operation(cursor, table_name, data):
    """Inserts data given which contains column names as the keys and corresponding values"""

    # get column names for generating query string
    columns = data.keys()
    values = data.values()

    """
    Example query string:
        "INSERT INTO salaries "
        "(emp_no, salary, from_date, to_date) "
        "VALUES (%(emp_no)s, %(salary)s, %(from_date)s, %(to_date)s)")
    """

    # query string is constructed here
    query = (
        "INSERT INTO " + table_name + " (" + columns_string(columns) + ") "
        "VALUES (" + ", ".join(["%s"]*len(columns)) + ")"
    )

    print(f"{query}".format(data))

    try:
        print("Inserting {}: ".format(table_name))
        cursor.execute(query, list(values))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            print("This instance already exists. \n", data)
        else:
            print(err.msg)
    else:
        print("OK\n")

    return True

def update_operation(cursor, table_name, values, condition=None, filters=None):
    """Updates instances with given values which contains column names as keys and filter array which has dictionaries that contains filter info"""
    query = """UPDATE {}""".format(table_name)
    query = set_query_generation(query, values=values)
    query = filter_generation(query=query, condition=condition, filters=filters)
    print(query,"\n")
    cursor.execute(query)
    print("Update operation successful","\n")
    return True


def delete_operation(cursor, table_name, condition=None, filters=None):
    """Deletes instances with given filter array that contains dictionaries that has filter info"""
    query = """DELETE FROM {} """.format(table_name)
    query = filter_generation(query, condition, filters)
    print(query,"\n")
    cursor.execute(query)
    print("Deletion successfull","\n")
    return True


def select_operation(cursor, table_name, condition=None, filters=None):
    """Selects instances with given filter array that contains dictionaries that has filter info"""
    query = """SELECT * FROM {} """.format(table_name)
    query = filter_generation(query=query, condition=condition, filters=filters)
    cursor.execute(query)
    data = cursor.fetchall()
    return data




insert_address_list = [
    {"AddressId":14,"Address":"Ayazağa Cendere Caddesi 109C Sarıyer","City":"İstanbul","Country":"Turkey"},
    {"AddressId":15,"Address":"Gümüşsuyu Tak-ı Zafer Caddesi Beyoğlu","City":"İstanbul","Country":"Turkey"},
    {"AddressId":17,"Address":"Huzur Mahallesi Maslak Ayazağa Caddesi 4/A Sarıyer","City":"İstanbul","Country":"Turkey"},
    {"AddressId":18,"Address":"Marmara Mahallesi Ulusum Caddesi 34/6 G Beylikdüzü","City":"İstanbul","Country":"Turkey"},
    {"AddressId":19,"Address":"Mehmet Akif Tabiat Parkı Bahçeköy Sarıyer","City":"İstanbul","Country":"Turkey"},
    {"AddressId":20,"Address":"Oran Mahallesi Kudüs Caddesi No:3/317 Çankaya","City":"Ankara","Country":"Turkey"},
    {"AddressId":21,"Address":"100. Yıl 428. Sokak Balgat","City":"Ankara","Country":"Turkey"},
    {"AddressId":22,"Address":"Anafartalar Altınsoy Caddesi no:4 Sıhhiye Altındağ","City":"Ankara","Country":"Turkey"},
    {"AddressId":23,"Address":"Sarıabalı Aspendos Yolu Serik","City":"Antalya","Country":"Turkey"},
    {"AddressId":24,"Address":"Kuşadası-Söke Yolu 6. kilometre Kuşadası","City":"Aydın","Country":"Turkey"},
]

cnx = create_connection()
cursor = cnx.cursor()

for address in insert_address_list:
    insert_operation(cursor,table_name="Address",data=address)

cnx.commit()


insert_venue_list = [
    {"VenueId":15,"Name":"BtcTürk Vadi Açıhava","Capacity":2200,"AddressId":14},
    {"VenueId":16,"Name":"AKM Türk Telekom Opera Sahnesi","Capacity":2040,"AddressId":15},
    {"VenueId":17,"Name":"Jolly Joker Vadi İstanbul","Capacity":2000,"AddressId":14},
    {"VenueId":18,"Name":"Volkswagen Arena","Capacity":4500,"AddressId":17},
    {"VenueId":19,"Name":"Mask Beach","Capacity":800,"AddressId":18},
    {"VenueId":20,"Name":"Life Park","Capacity":3000,"AddressId":19},
    {"VenueId":21,"Name":"Oran Açıkhava Sahnesi","Capacity":2500,"AddressId":20},
    {"VenueId":22,"Name":"ODTÜ MD Vişnelik","Capacity":6000,"AddressId":21},
    {"VenueId":23,"Name":"CerModern","Capacity":360,"AddressId":22},
    {"VenueId":24,"Name":"Aspendos Antik Tiyatro","Capacity":12000,"AddressId":23},
    {"VenueId":25,"Name":"Kuşadası Altın Güvercin Amfi Tiyatro","Capacity":2600,"AddressId":24}
]

for venue in insert_venue_list:
    insert_operation(cursor,"venue",venue)
cnx.commit()


venue_query = select_operation(cursor,"Venue",None,filters=[{"field":"Capacity","operator":">=","value":5000}])
print(venue_query,"\n")

cnx.commit()

address_query = select_operation(cursor,"Address",None,filters=[{"field":"City","operator":"in","value":'("İzmir","Aydın")'}])
print(address_query,"\n")

print(select_operation(cursor,"Address",None,None),"\n")
update_operation(cursor,table_name="Address",values={"Country": "Türkiye"},filters=[{"field":"Country","operator":"=","value":"'Turkey'"}])
print(select_operation(cursor,"Address",None,None),"\n")
cnx.commit()

#print(select_operation(cursor,"Concert",None,None),"\n")


print(select_operation(cursor,"Venue",None,None),"\n")
delete_operation(cursor,"Venue",None,filters=[{"field":"Capacity","operator":"<=","value":"1000"}])
print(select_operation(cursor,"Venue",None,None),"\n")
cnx.commit()


