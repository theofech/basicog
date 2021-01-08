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


