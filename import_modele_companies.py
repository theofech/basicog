import sys, getopt #https://www.tutorialspoint.com/python/python_command_line_arguments.htm

print(sys.argv)
strUsername= sys.argv[1]
strPassword= sys.argv[2]


print("UserName : "+strUsername)
print("Password : "+strPassword)




listCompanies=["Mercedes","Volswagen","Bayer AG","Adidas"]
listYears=["2015","2016","2017"]
listIndicators=["Turnover","Net profit Whole of group","Ordinary dividend"]

strChosenCompany=listCompanies[0]
strChosenYear=listYears[0]
strChosenIndicator=listIndicators[0]

def change_list_element(list,element,increment):
	if element in list:
		print(list[list.index(element)+increment])
		return list[list.index(element)+increment]
	else:
		print("Element non dans la liste")
		exit

def add_raw_data(strCompany,strYear,strIndicator,floatRawData,intPage,strDocument,strComment):
	print(strComment)

