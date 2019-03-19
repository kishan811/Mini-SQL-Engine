import csv
import sys
import re
from collections import OrderedDict

# dictionary = {}
# read_File(dictionary)

def readFile(tabname,fileData):
        with open(tabname,'rb') as f:
            reader = csv.reader(f)
            for r in reader:
                fileData.append(r)


def read_File(dictionary):
    f = open('metadata.txt','r')
    temp11 = 0
    for line in f:
        if line.strip() == "<begin_table>":
            temp11 = 1
            continue
        if temp11 == 1:
            tableName = line.strip()
            dictionary[tableName] = [];
            temp11 = 0
            continue
        if not line.strip() == '<end_table>':
            dictionary[tableName].append(line.strip());	


def col_name_output(columnNames,tableNames,dictionary):
    printhead = " Printing table data :- \n"
    # Table headers
    string = []
    string.append("")
    for x in columnNames:
        for tab in tableNames:
            if x in dictionary[tab]:
                if not string[0] == "":
                    string[0] += ','
                string[0] += tab + '.' + x
    print (printhead+str(string))


def show_output(fileData,columnNames,tableNames,dictionary):
    # print fileData
    # print(fileData)
    # l=len(columnNames)
    for data in fileData:
        print("\n")
        for x in range(len(columnNames)):
        # p=""
        # k=1
        # for x in data:
        #     if k==l:
        #         p=p+x
        #     else:
        #         p=p+x+","
        #         k=k+1
        # print p
            print(data[dictionary[tableNames[0]].index(columnNames[x])]),
            # print("sgsgsgsg")


def DISTINCT_query(columnNames,tabless,dictionary):
    temp = []
    unique_val=[]
    temp11 = 0
    col_name_output(columnNames,tabless,dictionary)
    for table in tabless:
        tName = table + '.csv'
        with open(tName,'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                # print("\n")
                for col in columnNames:
                    unique_val.append('init')
                    unique_val[0] = row[dictionary[tabless[0]].index(col)]
                    if unique_val[0] not in temp:
                        temp.append(unique_val[0])
                        temp11 = 1
                        print(unique_val[0]),
                if temp11 == 1 :
                    temp11 = 0
                    print

def print_uniques(colList,columnName,tableName,dictionary):
    printhead = " Printing table data :- \n"
    str1 = tableName + '.' + columnName
    print (printhead + (str(str1)))
    colList = list(OrderedDict.fromkeys(colList))
    col_len = len(colList)
    for col in range(col_len):
        print (colList[col])


def aggregate_funcs(func,columnName,tableName,dictionary):
    if columnName not in dictionary[tableName]:
        sys.exit("error")
    if columnName == '*':
        sys.exit("error")
    fileData = []
    colList = []
    tName = tableName + '.csv'
    readFile(tName,fileData)
    for data in fileData:
        colList.append(int(data[dictionary[tableName].index(columnName)]))

    # printhead = 'na'
    if func.lower() == 'avg':
        printhead = float(sum(colList))/len(colList)
    elif func.lower() == 'max':
        printhead = max(colList)
    elif func.lower() == 'sum':
        printhead = sum(colList)
    elif func.lower() == 'min':
        printhead = min(colList)
    elif func.lower() == 'distinct':
        print_uniques(colList,columnName,tableName,dictionary);
    else :
        printhead =  "Error! Unknown function name : (Enter max,min,avg or sum only)", '"You entered: ' + str(func) + '"'

    print (printhead)


def two_table_join(columnNames,tabless,dictionary):
    # tabless.reverse()
    # print(tabless)
    fileData = []
    l1 = []
    l2 = []
    dictionary["joins"] = []
    readFile(tabless[0] + '.csv',l1)
    readFile(tabless[1] + '.csv',l2)

    for item1 in l1:
        for item2 in l2:
            fileData.append(item1 + item2)

    for x in dictionary[tabless[0]]:
        dictionary["joins"].append(tabless[0] + '.' + x)
    for x in dictionary[tabless[1]]:
        dictionary["joins"].append(tabless[1] + '.' + x)

    dictionary["test"] = dictionary[tabless[0]] + dictionary[tabless[1]]

    # tabless.remove(tabless[0])
    # tabless.remove(tabless[0])
    tabless.insert(0,"joins")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tabless[0]]

    for i in range(len(columnNames)):
        print (columnNames[i]),
    print
    for data in fileData:
        for col in columnNames:
            if '.' in col:
                print (data[dictionary[tabless[0]].index(col)]),
            else:
                print (data[dictionary["test"].index(col)]),
        print

def connectors(a,tabless,dictionary,data):
    str1 = []
    str1.append("")
    for i in a:
        # print i
        if i == '=':
            str1[0] += i*2
        elif i in dictionary[tabless[0]] :
            str1[0] += data[dictionary[tabless[0]].index(i)]
            # print(str1[0])
            # print(data[dictionary[tabless[0]].index(i)])

        elif i.lower() == 'and' or i.lower() == 'or':
            str1[0] += ' ' + i.lower() + ' '
        else:
            str1[0] += i
        # print str
    return str1[0]