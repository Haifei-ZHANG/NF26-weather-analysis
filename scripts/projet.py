#################
#  Projet ASOS  #
#    TD2-5-6    #
# Guyot - Zhang #
#################

import question1
import question2
import question3

menu = str()

while menu.lower() != "q":
	print("""
	     ====== MENU ======""")
	
	menu = input("""
	1  : Repondre a la question 1
	2  : Repondre a la question 2
	3  : Repondre a la question 3
	
	Q  : Quitter\n
	     ==================\n""")
	
	# QUESTION 1 #
	if menu == "1":
		print("""
	     === Question 1 ===\n""")
		question1.reponseQuestion1()
		continue
	
	# QUESTION 2 #
	if menu in ("1", "2"):
		print("""
	     === Question 2 ===\n""")
		question2.reponseQuestion2()
		continue
	
	# QUESTION 3 #
	if menu in ("1", "2", "3"):
		print("""
	     === Question 3 ===\n""")
		question3.reponseQuestion3()
		continue
	
	if menu.lower() not in ("1", "2", "3", "q"):
		print("\nVeuillez entrer un chiffre entre 1 et 3, ou bien entrer Q pour quitter.\n")
		continue
	
	# SORTIE #
	if menu.lower() == "q":
		print("""
	     *** AU REVOIR ! ***\n""")