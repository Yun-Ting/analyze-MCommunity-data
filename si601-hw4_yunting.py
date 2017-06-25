import sqlite3 as sqlite
import urllib2
import re
import json

json_data= json.dumps({"searchCriteria":{"givenNameSearchType":"starts with","snSearchType":"starts with","uniqnameSearchType":"is equal","emailSearchType":"is equal","titleSearchType":
    "starts with","affiliationSearchType":"starts with","phoneSearchType":"ends with","cnSearchType":"starts with","ownerSearchType":"is equal","title":"Arthur F Thurnau Professor","searchForm":"People"}})
url = "https://mcommunity-beta.dsc.umich.edu/miPeople/services/person/search.json"
request = urllib2.Request(url, json_data, {'Content-Type': 'application/json', 'Content-Length': len(json_data)})
response = json.loads(urllib2.urlopen(request).read())

# connect to sqilte with connect and cursor
with sqlite.connect(r'step1_output.db') as con:
    cur = con.cursor()
    #create affiliation and professor table
    cur.execute("DROP TABLE if exists professor")
    cur.execute("DROP TABLE if exists affiliation")
    cur.execute("CREATE TABLE professor(P_ID INTEGER PRIMARY KEY AUTOINCREMENT, P_NAME VARCHAR(128), P_TITLE VARCHAR(128))")
    cur.execute("CREATE TABLE affiliation(A_ID INTEGER PRIMARY KEY AUTOINCREMENT, A_DES VARCHAR(128) UNIQUE)")

    # create and populate the professor_affiliation link table
    cur.execute("DROP TABLE if exists professor_affiliation")
    cur.execute("CREATE TABLE professor_affiliation (L_PRO_ID INTEGER, L_AFF_ID INTEGER)")

    # create a school table (step 6)
    cur.execute("DROP TABLE if exists school")
    cur.execute("CREATE TABLE school (S_ID INTEGER PRIMARY KEY AUTOINCREMENT, S_NAME VARCHAR(1028) UNIQUE)")

    # create a dept_school table (step 6)
    cur.execute("DROP TABLE if exists department")
    cur.execute("CREATE TABLE department (D_ID INTEGER PRIMARY KEY AUTOINCREMENT, D_NAME VARCHAR(1028) UNIQUE, D_SCHOOL_NAME VARCHAR(1028))")
    con.commit()

    person_list = response['searchResults']['person']
    #create a professor_column_list
    pro_column_list = []
    for person in person_list:
        pro_tuple = ((person['displayName']).encode('ascii', 'ignore'), (', '.join(person['title'])).encode('ascii', 'ignore'))
        pro_column_list.append(pro_tuple)
        for affiliation in person['affiliation']:
            cur.execute("INSERT OR IGNORE INTO affiliation (A_DES) values (?)", (affiliation, ))
            con.commit()
            result = cur.fetchall()

    cur.executemany("INSERT INTO professor (P_NAME, P_TITLE) values (?,?)", pro_column_list)
    con.commit()

    for person in person_list:
        cur.execute("SELECT P_ID FROM professor WHERE P_NAME=?", ((person['displayName']).encode('ascii', 'ignore'),))
        P_ID = cur.fetchall()[0][0]

        for affiliation in person['affiliation']:
            cur.execute("SELECT A_ID FROM affiliation WHERE A_DES=?",  (affiliation, ))
            A_ID = cur.fetchall()[0][0]
            professor_affiliation_columns= (P_ID, A_ID)
            cur.execute("INSERT INTO professor_affiliation (L_PRO_ID, L_AFF_ID) values (?,?)", professor_affiliation_columns)
    con.commit()

#school - department
cur.execute("SELECT professor.P_TITLE FROM professor")
result = cur.fetchall()

def retrieve_school():
    for each_tuple in result:
        # find all schools & colleges this professor affiliated with
        list_of_school = []
        if re.findall(r'.*College of Engineering.*', each_tuple[0], re.I):
            list_of_school.append("College of Engineering")
        if re.findall(r'.*College of Lit.*|.*College of LSA.*|.*Department of Asian.*|.*College of LS&A.*|.*ecology.*|.*environment.*|.*Chemistry.*', each_tuple[0], re.I):
            list_of_school.append("LSA")
        if re.findall(r'.*College of Arch.*', each_tuple[0], re.I):
            list_of_school.append("College of Arch")
        if re.findall(r'.*School of social work.*', each_tuple[0], re.I):
            list_of_school.append("School of social work")
        if re.findall(r'.*School of business.*', each_tuple[0], re.I):
            list_of_school.append("School of Business")
        if re.findall(r'.*School of public policy.*', each_tuple[0], re.I):
            list_of_school.append("School of Public Policy")
        if re.findall(r'.*School of Education.*', each_tuple[0], re.I):
            list_of_school.append("School of Education")
        if re.findall(r'.*School of Music.*', each_tuple[0], re.I):
            list_of_school.append("School of Music, Theater & Dance")
        if re.findall(r'.*School of Art and Design.*', each_tuple[0], re.I):
            list_of_school.append("School of Art and Design")
        if re.findall(r'.*School of Information.*', each_tuple[0], re.I):
            list_of_school.append("School of Information")
        if re.findall(r'.*School of Kinesiology.*', each_tuple[0], re.I):
            list_of_school.append("School of Kinesiology")

        for index in range(0, len(list_of_school)):
            cur.execute("INSERT OR IGNORE INTO school (S_NAME) values (?)", (list_of_school[index], ))
            con.commit()

retrieve_school()

cur.execute("SELECT affiliation.A_DES FROM affiliation")
result = cur.fetchall()

def dump_non_department(result, cur, con):
    dept_list = []
    for line in result:
        if re.findall(r'school|college|Alumni|Retiree|Vice President|ofc|library dean|VP', line[0], re.I):
            continue
        else:
            dept_list.append(line[0])
    dept_list.append("LSA UG: Residential College - Faculty and Staff")

    for index in range(0, len(dept_list)):
        cur.execute("INSERT OR IGNORE INTO department (D_NAME) values (?)", (dept_list[index],))
        con.commit()

dump_non_department(result, cur, con)


# select schools out of the school table, and find the school of the department
cur.execute("SELECT department.D_NAME FROM department")
result = cur.fetchall()

def pair(result):
    for each_tuple in result:
        if re.findall(r'LSA', each_tuple[0]):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of LSA' WHERE D_NAME=?", each_tuple)
        elif re.findall(r'CoE|Engr|engin|EECS|eng', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='College of Engineering' WHERE D_NAME=?", each_tuple)
        if re.findall(r'arch & urban', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='College of Arch&Urban Planning' WHERE D_NAME=?", each_tuple)
        if re.findall(r'pub pol', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of Public Policy' WHERE D_NAME=?", each_tuple)
        if re.findall(r'health management', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of Public Health' WHERE D_NAME=?", each_tuple)
        if re.findall(r'health management', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of Public Health' WHERE D_NAME=?", each_tuple)
        if re.findall(r'Research', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Institute Research on Women & Gender' WHERE D_NAME=?", each_tuple)
        if re.findall(r'SOE', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of Public Education' WHERE D_NAME=?", each_tuple)
        if re.findall(r'Nanotechnology', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Nanotechnology Institute' WHERE D_NAME=?", each_tuple)
        if re.findall(r'RCGD|institute for social research|SRC', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Institute for Social Research' WHERE D_NAME=?", each_tuple)
        if re.findall(r'Society of Fellows', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Society of Fellows' WHERE D_NAME=?", each_tuple)
        if re.findall(r'Library', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='University Library' WHERE D_NAME=?", each_tuple)
        if re.findall(r'Immu', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Medical School' WHERE D_NAME=?", each_tuple)
        if re.findall(r'SMTD', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='School of Music, Theater & Dance' WHERE D_NAME=?", each_tuple)
        if re.findall(r'mcit', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Medical Center Information Technology' WHERE D_NAME=?", each_tuple)
        if re.findall(r'ca&up', each_tuple[0], re.I):
            cur.execute("UPDATE department SET D_SCHOOL_NAME='Carreers at the U' WHERE D_NAME=?", each_tuple)
    con.commit()
pair(result)


cur.execute("SELECT d.D_SCHOOL_NAME, d.D_NAME, COUNT(p.P_NAME) FROM department d, professor p, professor_affiliation pa, affiliation a WHERE p.P_ID=pa.L_PRO_ID AND a.A_ID=pa.L_AFF_ID AND a.A_DES=d.D_NAME GROUP BY d.D_NAME ORDER BY d.D_SCHOOL_NAME")
result = cur.fetchall()

step6_file = open('step6_output.csv', 'w')
for i in result:
    step6_file.write(i[0] + ', ' + i[1] +", " + str(i[2]) + '\n')

cur.execute("SELECT DISTINCT p.P_NAME FROM department d1, department d2, professor p, professor_affiliation pa1, professor_affiliation pa2, affiliation a1, affiliation a2 WHERE p.P_ID=pa1.L_PRO_ID AND p.P_ID=pa2.L_PRO_ID AND a1.A_ID=pa1.L_AFF_ID AND a2.A_ID=pa2.L_AFF_ID AND a1.A_DES=d1.D_NAME AND a2.A_DES=d2.D_NAME AND d1.D_NAME!=d2.D_NAME ORDER BY p.P_ID")
result = cur.fetchall()

print len(result)