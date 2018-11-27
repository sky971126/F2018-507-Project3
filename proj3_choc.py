import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)
statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)


statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewDate' TEXT NOT NULL,
        'CocoaPercent' REAL NOT NULL,
        'CompanyLocationId' Integer NOT NULL,
        'Rating' REAL NOT NULL,
        'BeanType' TEXT,
        'BroadBeanOriginId' INTEGER,
        FOREIGN KEY ('CompanyLocationId') REFERENCES Countries('Id'),
        FOREIGN KEY ('BroadBeanOriginId') REFERENCES Countries('Id')
    );
'''
cur.execute(statement)
statement = '''
    CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT,
        'SubRegion' TEXT,
        'Population' INTEGER NOT NULL,
        'Area' REAL
    );
'''
cur.execute(statement)

data_f = open(COUNTRIESJSON, "r", encoding='utf-8')
data = data_f.read()
data_f.close()
COUNTRY_LIST = json.loads(data)
for i in COUNTRY_LIST:
    insertion = None
    if i["area"] == None:
        insertion = (None, i["alpha2Code"], i["alpha3Code"], i["name"], i["region"], i["subregion"], int(i["population"]), None)
    else:
        insertion = (None, i["alpha2Code"], i["alpha3Code"], i["name"], i["region"], i["subregion"], int(i["population"]), float(i["area"]))
    statement = "INSERT INTO 'Countries' VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    cur.execute(statement, insertion)

conn.commit()

def get_country_id(name):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = '''
        SELECT c.Id
        FROM Countries c
        WHERE c.EnglishName = "''' + name + '"'
    try:
        result = cur.execute(statement)
        return result.fetchone()
    except:
        return None

with open (BARSCSV, 'r', encoding = 'utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    ignore_first_row = 1
    for row in spamreader:
        if ignore_first_row:
            ignore_first_row = 0
            continue
        if row[8] == "Unknown" or get_country_id(row[8]) == None:
            insertion = (None, row[0], row[1], row[2], row[3], float(row[4][:-1]), + int(get_country_id(row[5])[0]), float(row[6]), row[7], None)
        else:
            insertion = (None, row[0], row[1], row[2], row[3], float(row[4][:-1]), + int(get_country_id(row[5])[0]), float(row[6]), row[7], int(get_country_id(row[8])[0]))        
        statement = "INSERT INTO 'Bars' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        cur.execute(statement, insertion)

conn.commit()

conn.close()

# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = None
    commands = command.split()
    sell_country = None
    source_country = None
    sell_region = None
    source_region = None
    use_rate = 1
    ranking = "ratings"
    use_seller = 1
    use_top = 1
    limit = 10
    if commands[0] == "bars":
        for i in commands[1:]:
            if i.startswith("sellcountry"):
                sell_country = i[12:]
            elif i.startswith("sourcecountry"):
                source_country = i[14:]
            elif i.startswith("sellregion"):
                sell_region = i[11:]
            elif i.startswith("sourceregion"):
                source_region = i[13:]
            elif i == "ratings":
                pass
            elif i == "cocoa":
                use_rate = 0
            elif i.startswith("top"):
                limit = int(i[4:])
            elif i.startswith("bottom"):
                use_top = 0
                limit = int(i[7:])
            else:
                print("Command not recognized: " + command)
                return None
        statement = """
            SELECT b.SpecificBeanBarName, b.Company, c1.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName
            FROM Bars AS b 
            LEFT JOIN Countries AS c1
                ON b.CompanyLocationId = c1.Id
            LEFT JOIN Countries AS c2
                ON b.BroadBeanOriginId = c2.Id
        """
        
        if sell_country:
            statement += """
                WHERE c1.Alpha2 = '""" + sell_country + "'"            
        elif source_country:
            statement += """
                WHERE c2.Alpha2 = '""" + source_country + "'"
        elif sell_region:
            statement += """
                WHERE c1.Region = '""" + sell_region + "'"
        elif source_region:
            statement += """
                WHERE c2.Region = '""" + source_region + "'"

        if use_rate:
            statement += """
                ORDER BY b.Rating
            """
        else:
            statement += """
                ORDER BY b.CocoaPercent
            """
        
        if not use_top:
            statement += "ASC"
        else:
            statement += "DESC"
        result = cur.execute(statement).fetchmany(limit)

    elif commands[0] == "companies":
        for i in commands[1:]:
            if i.startswith("country"):
                sell_country = i[8:]
            elif i.startswith("region"):
                sell_region = i[7:]
            elif i == "ratings":
                pass
            elif i == "cocoa":
                ranking = "cocoa"
            elif i == "bars_sold":
                ranking = "bars_sold"
            elif i.startswith("top"):
                limit = int(i[4:])
            elif i.startswith("bottom"):
                use_top = 0
                limit = int(i[7:])
            else:
                print("Command not recognized: " + command)
                return None                
        if ranking == "ratings":
            statement = """
            SELECT b.Company, c.EnglishName, ROUND(AVG(Rating), 1)
            """
        elif ranking == "cocoa":
            statement = """
            SELECT b.Company, c.EnglishName, ROUND(AVG(CocoaPercent), 0)
            """
        elif ranking == "bars_sold":
            statement = """
            SELECT b.Company, c.EnglishName, COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        statement += """
            FROM Bars AS b 
            JOIN Countries AS c
            ON b.CompanyLocationId = c.Id
        """
        if sell_country:
            statement += """
            WHERE c.Alpha2 = '""" + sell_country + "'"
        elif sell_region:
            statement += """
            WHERE c.Region = '""" + sell_region + "'"

        statement += """
                GROUP BY Company
                HAVING COUNT(*)>4
        """
        if ranking == "ratings":
            statement += """
                ORDER BY AVG(Rating)
            """
        elif ranking == "cocoa":
            statement += """
                ORDER BY AVG(CocoaPercent)
            """
        elif ranking == "bars_sold":
            statement += """
                ORDER BY COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        if not use_top:
            statement += " ASC"
        else:
            statement += " DESC"
        print(statement)
        result = cur.execute(statement).fetchmany(limit)

    elif commands[0] == "countries":
        for i in commands[1:]:
            if i.startswith("region"):
                sell_region = i[7:]
            elif i == "sources":
                use_seller = 0
            elif i == "sellers":
                pass
            elif i == "ratings":
                pass
            elif i == "cocoa":
                ranking = "cocoa"
            elif i == "bars_sold":
                ranking = "bars_sold"
            elif i.startswith("top"):
                limit = int(i[4:])
            elif i.startswith("bottom"):
                use_top = 0
                limit = int(i[7:])
            else:
                print("Command not recognized: " + command)
                return None
        if ranking == "ratings":
            statement = """
            SELECT c.EnglishName, c.Region, ROUND(AVG(Rating), 1)
            """
        elif ranking == "cocoa":
            statement = """
            SELECT c.EnglishName, c.Region, ROUND(AVG(CocoaPercent), 0)
            """
        elif ranking == "bars_sold":
            statement = """
            SELECT c.EnglishName, c.Region, COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        if use_seller:
            statement += """
            FROM Bars AS b 
            LEFT JOIN Countries AS c
            ON b.CompanyLocationId = c.Id
            """
        else:
            statement += """
            FROM Bars AS b 
            LEFT JOIN Countries AS c
            ON b.BroadBeanOriginId = c.Id
            """
        
        if sell_region:
            statement+= """
            WHERE c.Region = '""" + sell_region + "'"


        statement += """
                GROUP BY c.EnglishName
                HAVING COUNT(*) > 4
        """
        
        if ranking == "ratings":
            statement += """
                ORDER BY AVG(Rating)
            """
        elif ranking == "cocoa":
            statement += """
                ORDER BY AVG(CocoaPercent)
            """
        elif ranking == "bars_sold":
            statement += """
                ORDER BY COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        if not use_top:
            statement += " ASC"
        else:
            statement += " DESC"

        result = cur.execute(statement).fetchmany(limit)


    elif commands[0] == "regions":
        for i in commands[1:]:
            if i == "sources":
                use_seller = 0
            elif i == "sellers":
                pass
            elif i == "ratings":
                pass
            elif i == "cocoa":
                ranking = "cocoa"
            elif i == "bars_sold":
                ranking = "bars_sold"
            elif i.startswith("top"):
                limit = int(i[4:])
            elif i.startswith("bottom"):
                use_top = 0
                limit = int(i[7:])
            else:
                print("Command not recognized: " + command)
                return None
        if ranking == "ratings":
            statement = """
            SELECT c.Region, ROUND(AVG(Rating), 1)
            """
        elif ranking == "cocoa":
            statement = """
            SELECT c.Region, ROUND(AVG(CocoaPercent), 0)
            """
        elif ranking == "bars_sold":
            statement = """
            SELECT c.Region, COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        if use_seller:
            statement += """
            FROM Bars AS b 
            JOIN Countries AS c
            ON b.CompanyLocationId = c.Id
            """
        else:
            statement += """
            FROM Bars AS b 
            JOIN Countries AS c
            ON b.BroadBeanOriginId = c.Id
            """

        if sell_region:
            statement += """
            WHERE c.Region = '""" + sell_region + "'"

        statement += """
                GROUP BY c.Region
                HAVING COUNT(*) > 4
        """
        
        if ranking == "ratings":
            statement += """
                ORDER BY AVG(Rating)
            """
        elif ranking == "cocoa":
            statement += """
                ORDER BY AVG(CocoaPercent)
            """
        elif ranking == "bars_sold":
            statement += """
                ORDER BY COUNT(*)
            """
        else:
            print("Invalid Ranking")
            return None

        if not use_top:
            statement += " ASC"
        else:
            statement += " DESC"
        print(statement)
        result = cur.execute(statement).fetchmany(limit)
    else:
        print("Command not recognized: " + command)
        return None

    result_list = []
    for i in result:
        result_list.append(i)
    return result_list

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'exit':
            print("bye")
            break
        elif response == 'help':
            print(help_text)
        else:
            results = process_command(response)
            if results:
                for i in results:
                    count = 1
                    for j in i:
                        if j:
                            if count == 5:
                                count += 1
                                print(int(j), "%   ", end="")
                            else:
                                count += 1
                                if len(str(j)) > 13:
                                    print(str(j)[:12] + "... ", end="")
                                else:
                                    print("{0:16}".format(str(j)), end="")
                        else:
                            print("Unknown", end="")
                    print()
                print()

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
