listCompanies=["Mercedes","Volswagen","Bayer AG","Adidas"]
listYears=["2015","2016","2017"]
listIndicators=["Turnover","Net profit Whole of group","Ordinary dividend"]

strChosenCompanie=listCompanies[0]
strChosenYear=listYear[0]
strChosenIndicator=listIndicator[0]

def change_list_element(list,element,increment):
	if element in list:
  	return list[list.index(element)+increment]
	else:
		print("Fin de la liste")
		exit
		
  
