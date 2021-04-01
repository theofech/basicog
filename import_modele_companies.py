
"""
This script aims to add to a BASIC's database all DAX30 data mannually collected from documents.
Data are read from a given CSV file whose columns should be :
"entreprise";"année";"document";"lien";"indicateur";"valeur";"unité";"page";"nom";"commentaire méthodo";"titre document"
"""

import csv
import sys
import getopt
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import date

UnitsConvertor = { "Millions D’euros" : 1000000,
                   "humain" : 1,
                   "milliers d’euros" : 1000,
                   "Millions de dollars" : 1000000,
                   "Milliers de dollars" : 1000}

UnitsInDataBase = { "Chiffre d'affaires" : "EUR",
                    "Bénéfice net (Ensemble du groupe)" : "EUR",
                    "Bénéfice net (Part du groupe)" : "EUR",
                    "Dépenses de personnel" : "EUR",
                    "Salaires et traitements" : "EUR",
                    "Nombre d'employés" : "ETP",
                    "Nombre de membres du conseil d'administration" : "Nb",
                    "Nombre de femmes membres du conseil d'administration" : "Nb",
                    "Rémunération du dirigeant" : "EUR",
                    "Dividende ordinaire versé en numéraire" : "EUR",
                    "Dividendes ordinaires versé en action" : "EUR",
                    "Dividende exceptionnel" : "EUR",
                    "Rachat d'actions" : "EUR" }

def printError(details) :
    print(details)
    return details

def valueInUnit(value, unit):
    return float(value) * UnitsConvertor[unit]

def help():
    print('usage is :', os.path.basename(__file__), '-u <Database User Name> -p <Database Password> -i <Input CSV file> [ -f ]')
    print('-f or --force to overwrite existing database values')
    print('-h to print this help')

def add_raw_data(strCompany,strYear,strIndicator,floatRawData,strPages,strDocument,strComment,strLabel,unitStoredInDatabase):
    global sqlRequest

    #Check company name
    dbCompany = listCompanies.loc[listCompanies['surnom'] == strCompany]
    if dbCompany.shape[0] != 1 :
        return printError("Warning, company name '" + strCompany + "' unknown, are you sure you want to add it ? Known companies are \n" + str(listCompanies['surnom']))
    idCompany = dbCompany.iloc[0]['id']

    #Check indicator name
    dbIndicator = listIndicators.loc[listIndicators['nom_basic_guide_de_collecte'] == strIndicator]
    if dbIndicator.shape[0] != 1 :
        return printError("Warning, indicator '"  + strIndicator + "' unknown, are you sure you want to add it ? Known indicators are \n" + str(listIndicators['nom_basic_guide_de_collecte']))
    idIndicator = dbIndicator.iloc[0]['id']

    #Check year
    year = int(strYear)
    if not (year >= 2009 and year <= 2020) :
        return printError("Warning, year '"  + str(year) + "' unknown, are you sure you want to add it ? Year should be between 2009 and 2020")

    #Check consistency with Orbis value if it exists
    sqlRequest = "SELECT * FROM modele_entreprises.t1_donnees_orbis_traitees"
    sqlRequest += " WHERE id_company = " + str(idCompany)
    sqlRequest += " AND id_indicator = " + str(idIndicator)
    sqlRequest += " AND data_year = " + strYear
    dbOrbisValue = pd.read_sql(sql=sqlRequest,con=engineAzureGlobal)
    if dbOrbisValue.shape[0] == 1 and dbOrbisValue.iloc[0]['data_value'] != floatRawData :
        diff = abs((dbOrbisValue.iloc[0]['data_value'] - floatRawData) / dbOrbisValue.iloc[0]['data_value'])
        if diff > 0.03 and not strComment.startswith("Vérifié") :
            return printError("Orbis (" + str(dbOrbisValue.iloc[0]['data_value']) + ") and manually collected (" + str(floatRawData) + ") values are not equal, difference of " + str(diff*100) + "%" )
        strComment += "(différence de valeur avec Orbis de )" + str(diff*100) + "%)"
    elif dbOrbisValue.shape[0] > 1 :
        return printError("C'est bizarre, y'a plusieurs réponse :" + str(dbOrbisValue))

    #Check consistency with database value if it exists
    sqlRequest = "SELECT * FROM monde.rapports_annuels"
    sqlRequest += " WHERE id_entreprise = " + str(idCompany)
    sqlRequest += " AND id_indicateur = " + str(idIndicator)
    sqlRequest += " AND annee = " + strYear
    sqlRequest += " AND date_obsolete IS NULL"
    dbAnnualReportValue = pd.read_sql(sql=sqlRequest,con=engineAzureGlobal)
    if dbAnnualReportValue.shape[0] > 1 :
        return printError("C'est bizarre, y'a plusieurs réponse :" + str(dbAnnualReportValue))
    elif dbAnnualReportValue.shape[0] == 1 :
        #Overwrite the value in database
        if overwrite :
            print("overwrite existing value")
            sqlRequest = "UPDATE monde.rapports_annuels SET valeur = " + str(floatRawData) + ", unite = '" + unitStoredInDatabase + "', date_entree = '" + today + "', source_nom_document = '" + strDocument + "', source_page = '{" + strPages + "}', source_texte = '" + strLabel + "', commentaire_methode = '" + strComment + "' WHERE id_values = " + str(dbAnnualReportValue.iloc[0]['id_values'])
            engineAzureGlobal.execute(text(sqlRequest))
        elif dbAnnualReportValue.iloc[0]['valeur'] != floatRawData :
            return printError("Database (" + str(dbAnnualReportValue.iloc[0]['valeur']) + ") and csv (" + str(floatRawData) + ") values are not equal" )
        return ''

    #Insert the new value
    print("insert new value")
    sqlRequest = "INSERT INTO monde.rapports_annuels (id_entreprise, id_indicateur, annee, valeur, unite, date_entree, date_obsolete, source_nom_document, source_page, source_texte, commentaire_methode, commentaire_contexte, commentaire_version, rapporteur) VALUES (" + str(idCompany) + "," + str(idIndicator) + "," + str(year) + "," + str(floatRawData) + ",'" + unitStoredInDatabase + "','" + today + "',NULL,'" + strDocument + "','{" + strPages + "}','" + strLabel + "','" + strComment + "',0,0,'Léonard')"
    engineAzureGlobal.execute(text(sqlRequest))
    return ''

# Main starts here
# Variable to fill with given arguments
strUsername = None
strPassword = None
inputcsvfile = None
overwrite=False
today=date.today().strftime("%m/%d/%y")

try:
    opts, args = getopt.getopt(sys.argv[1:],"fhu:p:i:",["username=","password=","inputcsvfile=", "force"])
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
    elif opt in ("-f", "--force"):
        overwrite=True

if not strUsername or not strPassword or not inputcsvfile :
    print("At least one argument is missing ", strUsername, strPassword, inputcsvfile)
    help()
    sys.exit(3)

engineAzureGlobal = create_engine('postgresql://'+strUsername+'@basicog:'+strPassword+'@basicog.postgres.database.azure.com:5432/bdd_og')

listIndicators = pd.read_sql(sql="SELECT * FROM modele_entreprises.indicateurs WHERE nom_basic_guide_de_collecte IS NOT NULL",con=engineAzureGlobal)
listCompanies = pd.read_sql(sql="SELECT * FROM modele_entreprises.entreprises",con=engineAzureGlobal)

sqlRequest = ''
dataInError = []
headerRow = []
with open(inputcsvfile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:
        print("Treating line", line_count)
        if line_count == 0:
            headerRow = row
            line_count += 1
        else:
            line_count += 1
            errorMessage = ''
            if row[5] or row[9]: #Treat the line only if value is filled
                if row[9] and not row[5]:
                    row[5] = 0
                    row[6] = "humain"
                try:
                    unitStoredInDatabase = UnitsInDataBase[row[4]]
                    if unitStoredInDatabase == "EUR" and "dollars" in row[6] :
                        unitStoredInDatabase = "USD"
                    errorMessage = add_raw_data(row[0], row[1], row[4], valueInUnit(row[5], row[6]), row[7], row[10], row[9], row[8], unitStoredInDatabase)
                except Exception as e:
                    errorMessage = printError("exception on row :" + str(row) + ".\n Last sql request was :" + sqlRequest + "\n Exception :" + str(e))

            #Save row with error to be able to write them in a new csv file at the end
            if errorMessage:
                row.append(errorMessage)
                dataInError.append(row)

with open('dataInError.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    headerRow.append("import script error message")
    csvwriter.writerow(headerRow)
    csvwriter.writerows(dataInError)
print(line_count, " parcourues")
