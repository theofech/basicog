import csv
import sys
import getopt
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

UnitsConvertor = { "Millions\nD’euros" : 1000000,
                   "humain" : 1,
                   "milliers d’euros" : 1000}

def printError(details) :
    print(details)

def valueInUnit(value, unit):
    return float(value) * UnitsConvertor[unit]

def help():
    print('usage is :', os.path.basename(__file__), '-u <Database User Name> -p <Database Password> -i <Input CSV file>')


def add_raw_data(strCompany,strYear,strIndicator,floatRawData,intPage,strDocument,strComment):
    # Todo : ajouter le code qui vérifie la cohérence de la donnée, vérifie que la donnée n'existe pas déjà en base, puis l'ajoute à la base

    #Check company name
    dbCompany = listCompanies.loc[listCompanies['short_name'] == strCompany]
    if dbCompany.shape[0] != 1 :
        printError("Warning, company name '" + strCompany + "' unknown, are you sure you want to add it ? Known companies are \n" + str(listCompanies['short_name']))
        return
    idCompany = dbCompany.iloc[0]['id_company']

    #Check indicator name
    dbIndicator = listIndicators.loc[listIndicators['basic_collection_guide_name'] == strIndicator]
    if dbIndicator.shape[0] != 1 :
        printError("Warning, indicator '"  + strIndicator + "' unknown, are you sure you want to add it ? Known indicators are \n" + str(listIndicators['basic_collection_guide_name']))
        return
    idIndicator = dbIndicator.iloc[0]['id_indicator']

    #Check year
    year = int(strYear)
    if not (year >= 2009 and year <= 2020) :
        printError("Warning, year '"  + str(year) + "' unknown, are you sure you want to add it ? Year should be between 2009 and 2020")
        return

    #Check consistency with Orbis value if it exists
    sqlRequest = "SELECT * FROM modele_companies.values_orbis_positived"
    sqlRequest += " WHERE id_company = " + str(idCompany)
    sqlRequest += " AND id_indicator = " + str(idIndicator)
    sqlRequest += " AND data_year = " + strYear
    dbOrbisValue = pd.read_sql(sql=sqlRequest,con=engineAzureGlobal)
    if dbOrbisValue.shape[0] == 1 and dbOrbisValue.iloc[0]['data_value'] != floatRawData :
        printError("Orbis (" + str(dbOrbisValue.iloc[0]['data_value']) + ") and manually collected (" + str(floatRawData) + ") values are not equal" )
        return
    elif dbOrbisValue.shape[0] > 1 :
        printError("C'est bizarre, y'a plusieurs réponse :" + str(dbOrbisValue))
        return

    #Check consistency with Orbis value if it exists
    sqlRequest = "SELECT * FROM modele_companies.values_annualreport"
    sqlRequest += " WHERE id_company = " + str(idCompany)
    sqlRequest += " AND id_indicator = " + str(idIndicator)
    sqlRequest += " AND data_year = " + strYear
    dbAnnualReportValue = pd.read_sql(sql=sqlRequest,con=engineAzureGlobal)
    if dbAnnualReportValue.shape[0] == 1 :
        if dbAnnualReportValue.iloc[0]['data_value'] != floatRawData :
            printError("Database (" + str(dbAnnualReportValue.iloc[0]['data_value']) + ") and csv (" + str(floatRawData) + ") values are not equal" )
        return
    elif dbAnnualReportValue.shape[0] > 1 :
        printError("C'est bizarre, y'a plusieurs réponse :" + str(dbAnnualReportValue))
        return

    #dataToInsert = pd.DataFrame(columns=['id_company', 'id_indicator', 'data_year', 'data_value', 'unit', 'date_in', 'date_obsolete', 'source_doc', 'source_page', 'source_label', 'comment_methodo', 'comment_context', 'comment_version', 'reporter'])
    #dataToInsert.loc[1] = [idCompany, idIndicator, year, floatRawData, 'EUR', None, None, strDocument, intPage, None, strComment, None, None, "Léonard"]
    #dataToInsert.to_sql(name="modele_companies.values_annualreport", index=False, if_exists='append', con=engineAzureGlobal)
    sqlRequest = "INSERT INTO modele_companies.values_annualreport (id_company, id_indicator, data_year, data_value, unit, date_in, date_obsolete, source_doc, source_page, source_label, comment_methodo, comment_context, comment_version, reporter) VALUES (" + str(idCompany) + "," + str(idIndicator) + "," + str(year) + "," + str(floatRawData) + ",'EUR',NULL,NULL,0,NULL,0,'" + strComment + "',0,0,'Léonard')"
    engineAzureGlobal.execute(text(sqlRequest))


# Main starts here
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

engineAzureGlobal = create_engine('postgresql://'+strUsername+'@basicog:'+strPassword+'@basicog.postgres.database.azure.com:5432/bdd_og')

listIndicators = pd.read_sql(sql="SELECT * FROM modele_companies.indicators WHERE basic_collection_guide_name IS NOT NULL",con=engineAzureGlobal)
listCompanies = pd.read_sql(sql="SELECT * FROM modele_companies.companies",con=engineAzureGlobal)

with open(inputcsvfile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names, from CSV file, are {", ".join(row)}')
            line_count += 1
        else:
            line_count += 1
            if (row[5]): #Treat the line only if value is filled
                try:
                    add_raw_data(row[0], row[1], row[4], valueInUnit(row[5], row[6]), row[7], row[2], row[9])
                except Exception as e:
                    printError("error on row :" + str(row) + "\n" + str(e))

print(line_count, " parcourues")
