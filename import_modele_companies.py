import csv
import sys
import getopt
import os

def help():
    print('usage is :', os.path.basename(__file__), '-u <Database User Name> -p <Database Password> -i <Input CSV file>')


def add_raw_data(strCompany,strYear,strIndicator,floatRawData,intPage,strDocument,strComment):
    # Todo : ajouter le code qui vérifie la cohérence de la donnée, vérifie que la donnée n'existe pas déjà en base, puis l'ajoute à la base
    print("UserName : "+strUsername)
    print("Password : "+strPassword)
    print(strComment)
    if not strCompany in listCompanies :
        answer = input("Warning, company name '" + strCompany + "' unknown, are you sure you want to add it ? Known companies are " + str(listCompanies))
    if not strYear in listYears :
        answer = input("Warning, year '" + strYear + "' unknown, are you sure you want to add it ? Known years are " + str(listYears))
    if not strIndicator in listIndicators :
        answer = input("Warning, indicator '"  + strIndicator + "' unknown, are you sure you want to add it ? Known years are " + str(listIndicators))


# Main starts here

# Constant
listCompanies=["Mercedes","Volswagen","Bayer AG","Adidas"]
listYears=["2015","2016","2017"]
listIndicators=["Turnover","Net profit Whole of group","Ordinary dividend"]

# Variable to fill with given arguments
strUsername = None
strPassword = None
inputcsvfile = None

try:
    opts, args = getopt.getopt(sys.argv[1:],"hu:p:i:",["username=","password=","inputcsvfile="])
except getopt.GetoptError:
    help()
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        help()
        sys.exit()
    elif opt in ("-u", "--username"):
        strUsername = arg
    elif opt in ("-p", "--password"):
        strPassword = arg
    elif opt in ("-i", "--inputcsvfile"):
        inputcsvfile = arg

if not strUsername or not strPassword or not inputcsvfile :
    print("At least one argument is missing ", strUsername, strPassword, inputcsvfile)
    help()
    sys.exit(3)

print("UserName : "+strUsername)
print("Password : "+strPassword)

with open(inputcsvfile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            line_count += 1
            add_raw_data(row[0], row[1], row[4], row[5], row[7], row[2], row[9])

print(line_count, " parcourues")
