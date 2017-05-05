#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B APPLICATIONS***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Which industry has the most number of Data Scientist positions?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6) ${MENU} Which top 5 employers file the most petitions each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9) ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10) ${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11) ${MENU} Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12) ${MENU} Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13) ${MENU} Export result for option no 12 to MySql database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
 	start-all.sh
        case $opt in
        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
        pig -x local /home/shubha/PROJECT/codes/PIG/Pigproject1a.pig
        show_menu;
        ;;

        2) clear;
        option_picked "1 b) Find top 5 job titles who are having highest growth in applications. ";
	pig -x local pig -x local /home/shubha/PROJECT/codes/PIG/Pigproject1b.pig
        show_menu;
        ;;
            
        3) clear;
        option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
	pig -x local /home/shubha/PROJECT/codes/PIG/Pigproject2a.pig
        show_menu;
        ;;
	
        4) clear;
        option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.";
        pig -x local /home/shubha/PROJECT/codes/PIG/Pigproject2b.pig
        show_menu;
        ;;
            
	5) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
        (cd /home/shubha/PROJECT/codes/MAPREDUCE ; query3.sh)        
        show_menu;
        ;;
                    
        6) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
	HIVE/hive -f Query4.sql > Q4op
	HIVE/cat Q4op
        show_menu;
        ;;
        
        7) clear;
        option_picked "5) Find the most popular top 10 job positions for H1B visa applications for each year?";
	HIVE/hive -f Query5.sql >Q5op
	HIVE/cat Q4op
        show_menu;
        ;;
        
	8) clear;
        option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph        		depicting the pattern of All the cases over the period of time.";
	HIVE/hive -f q6.sql >Q6op
	HIVE/cat Q6op
	show_menu;
        ;;

	9) clear;
        option_picked "7) Create a bar graph to depict the number of applications for each year";
	(cd /home/shubha/PROJECT/codes/MAPREDUCE ; query7.sh)
         show_menu;
         ;;

        10) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange  		output in descending order";
	HIVE/hive -f average.sql >Q8op
	HIVE/cat Q8op
        show_menu;
        ;;

	11) clear;
	option_picked "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total 		petitions filed more than 1000?"
	(cd /home/shubha/PROJECT/codes/MAPREDUCE ; query9.sh)
        show_menu;
        ;;

	12) clear;
	option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions 		filed more than 1000?"
	(cd /home/shubha/PROJECT/codes/MAPREDUCE ; query10.sh)
        show_menu;
        ;;

        13) clear;
	option_picked "11) Export result for question no 10 to MySql database."
        bash /home/shubha/PROJECT/codes/query12sqoop.sh
        show_menu;
        ;;
	 \n) exit;
	;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
        esac
	fi
        done
