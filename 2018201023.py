import csv
import sys
import re
from collections import OrderedDict
# from temp import *
from temp import readFile,read_File,col_name_output,show_output,DISTINCT_query,print_uniques,aggregate_funcs,two_table_join,connectors

def main():
    dictionary = {}
    read_File(dictionary)
    split_query(str(sys.argv[1]),dictionary)	

def handle_where(wherequery,columnNames,tabless,dictionary):
    a = wherequery.split(" ")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tabless[0]]
        # print("hh"+columnNames[0])

    col_name_output(columnNames,tabless,dictionary)
    # print(tabless[0])
    tName = tabless[0] + '.csv'
    # print(tName)
    fileData = []
    readFile(tName,fileData)
    temp11 = 0
    for data in fileData:
        str1 = connectors(a,tabless,dictionary,data)
        # print(data[dictionary[tabless[0]].index('B')])
        for col in columnNames:
            # print(eval(str1))
            if eval(str1):
                temp11 = 1
                print(data[dictionary[tabless[0]].index(col)]),
                # print(dictionary[tabless[0]].index(col))
        if temp11 == 1:
            temp11 = 0
            print

def split_query(query,dictionary):
    query_initial = (re.sub(' +',' ',query)).strip();
    if not query_initial.replace(' ','').endswith(';'):
        print 'Error, Missing Semicolon...!"'
        sys.exit()
    query_final = query_initial.split(';')
    query = query_final[0]

    if "from" not in query:
        sys.exit("Incorrect Syntax...!")
    else:
        obj1 = query.split('from');

    obj1[0] = (re.sub(' +',' ',obj1[0])).strip();

    objects = []
    objects.append(0)

    if "select" not in obj1[0].lower():
        sys.exit("Incorrect Syntax...!")
    objects.append(obj1[0][7:])

    objects[1] = (re.sub(' +',' ',objects[1])).strip();
    l = []
    l.append("select")

    if "distinct" in objects[1] and "distinct(" not in objects[1]:
        objects[1] = objects[1][9:]
        l.append("distinct")

    l.append(objects[1])
    objects[1] = l 

    object3 = ""
    if "distinct" in objects[1][1] and "distinct(" not in objects[1][1]:
        object3 = objects[1][1];
        object3 = (re.sub(' +',' ',object3)).strip()
        objects[1][1] = objects[1][2]

    colss = objects[1][1];
    colss = (re.sub(' +',' ',colss)).strip()
    columnNames = colss.split(',');
    for i in range(len(columnNames)):
        columnNames[columnNames.index(columnNames[i])] = (re.sub(' +',' ',columnNames[i])).strip();
    obj1[1] = (re.sub(' +',' ',obj1[1])).strip();
    temp = obj1[1].split('where');
    objects.append(temp)
    
    tableStr = objects[2][0]
    tableStr = (re.sub(' +',' ',tableStr)).strip();
    tabless = tableStr.split(',')
    for i in range(len(tabless)):
        tabless[tabless.index(tabless[i])] = (re.sub(' +',' ',tabless[i])).strip();
    for i in range(len(tabless)):
        if tabless[i] not in dictionary.keys():
            sys.exit("Table not found...!")

    if len(objects[2]) > 1 and len(tabless) == 1:
        objects[2][1] = (re.sub(' +',' ',objects[2][1])).strip();
        handle_where(objects[2][1],columnNames,tabless,dictionary)
        return
    elif len(objects[2]) > 1 and len(tabless) > 1:
        objects[2][1] = (re.sub(' +',' ',objects[2][1])).strip();
        handle_where_join(objects[2][1],columnNames,tabless,dictionary)
        return

    if(len(tabless) > 1):
        two_table_join(columnNames,tabless,dictionary)
        return

    if object3 == "distinct":
        DISTINCT_query(columnNames,tabless,dictionary)
        return
    
    if len(columnNames) == 1:
        for col in columnNames:
            if '(' in col and ')' in col:
                names = []
                names.append("")
                names.append("")
                a1 = col.split('(');
                names[0] = (re.sub(' +',' ',a1[0])).strip()
                names[1] = (re.sub(' +',' ',a1[1].split(')')[0])).strip()
                aggregate_funcs(names[0],names[1],tabless[0],dictionary)
                return

            elif '(' in col or ')' in col:
                sys.exit("Syntax error")

    selectColumns(columnNames,tabless,dictionary);


def handle_where_join(wherequery,columnNames,tabless,dictionary):
    l1 = []
    l2 = []
    # tabless.reverse()
    fileData = []
    readFile(tabless[0] + '.csv',l1)
    readFile(tabless[1] + '.csv',l2)
    for item1 in l1:
        for item2 in l2:
            fileData.append(item1 + item2)
    dictionary["joins"] = []
    for i in range(len(dictionary[tabless[0]])):
        dictionary["joins"].append(tabless[0] + '.' + dictionary[tabless[0]][i])
    for i in range(len(dictionary[tabless[1]])):
        dictionary["joins"].append(tabless[1] + '.' + dictionary[tabless[1]][i])

    dictionary["test"] = dictionary[tabless[0]] + dictionary[tabless[1]]

    tabless.remove(tabless[0])
    tabless.remove(tabless[0])
    tabless.insert(0,"joins")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tabless[0]]

    for i in columnNames:
        print (i),
    print

    a = wherequery.split(" ")

    temp11 = 0
    for data in fileData:
        str1 = connectors(a,tabless,dictionary,data)
        for col in columnNames:
            if eval(str1):
                temp11 = 1
                if '.' in col:
                    print (data[dictionary[tabless[0]].index(col)]),
                else:
                    print (data[dictionary["test"].index(col)]),
        if temp11 == 1:
            temp11 = 0
            print

    del dictionary['joins']

def selectColumns(colNames,tabless,dictionary):
    fileData = []
    if len(colNames) == 1 and colNames[0] == '*':
        colNames = dictionary[tabless[0]]

    for i in colNames:
        if i not in dictionary[tabless[0]]:
            sys.exit("Error in query...!")

    col_name_output(colNames,tabless,dictionary)
    tName = tabless[0] + '.csv'
    readFile(tName,fileData)
    show_output(fileData,colNames,tabless,dictionary)


if __name__ == "__main__":
    main()