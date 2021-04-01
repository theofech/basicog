
"""
This script aims to add to a BASIC's Zotero library all DAX30 documents from a given folder,
taking document's information from the csv file where important data where collected.
CSV file's columns should be :
"entreprise";"année";"document";"lien";"indicateur";"valeur";"unité";"page";"nom";"commentaire méthodo";"titre document"
"""

from pyzotero import zotero
import csv
import sys
import getopt
import os.path
import re

def help():
    print('usage is :', os.path.basename(__file__), '-f <folder containing documents> -i <Input CSV file> -k <Zotero API key>')
    print('-h to print this help')

# Main starts here
# Variable to fill with given arguments
inputcsvfile = None
inputfolder = None
key = None

try:
    opts, args = getopt.getopt(sys.argv[1:],"f:hi:k:",["inputcsvfile=", "folder=", "key="])
except getopt.GetoptError:
    help()
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        help()
        sys.exit()
    elif opt in ("-f", "--folder"):
        inputfolder = arg
    elif opt in ("-i", "--inputcsvfile"):
        inputcsvfile = arg
    elif opt in ("-k", "--key"):
        key = arg

if not inputcsvfile or not inputfolder or not key :
    print("At least one argument is missing ", inputfolder, inputcsvfile)
    help()
    sys.exit(3)

zotDAX30 = zotero.Zotero('2756354', 'group' , key)

dataInError = []
headerRow = []
documentsDone = set()
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
            companyName = row[0]
            documentTitle = row[10]

            # Check line is valid
            if not companyName :
                continue
            # Check line doesn't correspond to an already added document
            if (companyName+documentTitle) in documentsDone :
                print("document :" + companyName + documentTitle + " already added during this run")
                continue
            documentFilePath = inputfolder + '/' + row[2]
            # Check line contains an existing file name
            if not os.path.isfile(documentFilePath):
                print("file :" + documentFilePath + " doesn't exist. Corresponding CSV line : " + str(row))
                break

            resp = zotDAX30.items(q=companyName + " " + documentTitle)
            documentAlreadyInZotero = False
            for item in resp :
                if item['data']['title'] == documentTitle and item['data']['institution'] == companyName:
                    documentAlreadyInZotero = True
            if documentAlreadyInZotero :
                print("document :" + companyName + documentTitle + " already in Zotero")
                documentsDone.add(companyName+documentTitle)
                continue

            newDocument = zotDAX30.item_template('report')
            newDocument['title'] = documentTitle
            newDocument['url'] = row[3]
            newDocument['creators'][0]['lastName'] = companyName
            newDocument['institution'] = companyName
            newDocument['date'] = row[1]
            newDocument['language'] = "English"
            newDocument['reportType'] = re.sub("[0-9/\-]*", '', documentTitle).strip()
            resp = zotDAX30.create_items([newDocument])
            if len(resp['success']) != 1 :
                print("Creation of file :" + str(newDocument) + " failed. Response is :" + str(resp))
                break
            resp = zotDAX30.attachment_simple([documentFilePath], resp['success']['0'])
            if len(resp['failure']) == 1 :
                print("Failure during attachment upload. Result : ", resp)
                break
            documentsDone.add(companyName+documentTitle)
