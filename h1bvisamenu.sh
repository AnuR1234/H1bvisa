#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}********************H1B APPLICATIONS********************************************${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 1)${MENU} Is the number of petitions with Data Engineer job title increasing over time?  ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 2)${MENU} Find top 5 job titles who are having highest avg growth in applications.${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 3)${MENU} Which part of the US has the most Data Engineer jobs for each year?  ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 4)${MENU} find top 5 locations in the US who have got certified visa for each year. ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 5)${MENU} Which industry has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 6)${MENU} Which top 5 employers file the most petitions each year?  ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 7)${MENU}Find the most popular top 10 job positions for H1B visa applications for each year?${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 8)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over period of time. ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 9)${MENU} Create a bar graph to depict the number of applications for each year.  ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 10)${MENU} Find the average Prevailing Wage for each Job for each Year Arrange the output in descending order.  ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 11)${MENU} Which are  the employers along with the number of petitions who have the success rate more than 70%  in petitions? ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 12)${MENU} Which are the job positions along with the number of petitions which have the success rate more than 70%  in petitions? ${NORMAL}"
    echo -e "${MENU}#->${NUMBER} 13)${MENU} Export result for question no 10 to MySql database.  ${NORMAL}"

    echo -e "${MENU}**********************************************************************************${NORMAL}"
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
start-all.sh | zenity --progress --width 150 --title="Hadoop Services Starting" --pulsate --auto-close #--percentage
yad --info --title="Project" --text '<span foreground="red" font="14">\t\t\tWelcome To BigData Project\n</span><span font="12">\n<b>\tAnalysis And Summarization Of H1B Applicants</b>\n</span>' --width=450 --height=10 --button="gtk-cancel:252" --button="gtk-ok:0" --center --timeout 3
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "1) Is the number of petitions with Data Engineer job title increasing over time?";
		hadoop fs -rmr /h1bvisa/Qn1a.out
		hadoop jar '/home/hduser/JarFiles/qn1a.jar' Qn1a /niit/h1b1visa /h1bvisa/Qn1a.out
		hadoop fs -cat /h1bvisa/Qn1a.out/p* 
        show_menu;
        ;;
	2) clear;
        option_picked "2) Find top 5 job titles who are having highest growth in applications. ";
        
		hive -e "select * from h1bvisa.totalavg" > /home/hduser/Desktop/1b.txt | zenity --progress --title="Hive Job Running" --pulsate --auto-close 
        show_menu;
        ;;
	 3) clear;
        option_picked "3) Which part of the US has the most Data Engineer jobs for each year?";
		pig -x local '/home/hduser/Documents/H1bvisaOP/Pig/2a.pig'
        show_menu;
        ;;
	4) clear;
        option_picked 2 "4) find top 5 locations in the US who have got certified visa for each year.";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e " select year, worksite,count(case_status) as total_case_status from h1bvisa.h1b_final where year =$var and 			  case_status='CERTIFIED' group by worksite,year order by total_case_status desc limit 5;" 
        show_menu;
        ;;  

	 5) clear;
        option_picked "5) Which industry has the most number of Data Scientist positions?";
                
		hadoop fs -rmr /h1bvisa/qns3.out
		hadoop jar '/home/hduser/JarFiles/3.jar' qns3 /niit/h1b1visa /h1bvisa/qns3.out | zenity --progress --title="Mapreduce Job Running" --pulsate --auto-close 
	echo  "The industry which has the most number of Data Scientist positions:"
		hadoop fs -cat /h1bvisa/qns3.out/p* 
		sleep 5
        show_menu;
        ;;
	6) clear;
	option_picked "6)Which top 5 employers file the most petitions each year?";
		rm -r /home/hduser/Desktop/6.txt	
	pig -x local '/home/hduser/Documents/H1bvisaOP/Pig/4a.pig/4.pig'  | zenity --progress --title="Pig Job Running" --pulsate --auto-close
	echo -e "The top 5 employers file the most petitions each year:\n"
	cat /home/hduser/Desktop/6.txt/p*
	sleep 5	
	show_menu;
	;;
	7) clear;
	 option_picked "5) Find the most popular top 10 job positions for H1B visa applications for each year?";
		 echo -e "${MENU}**${NUMBER} 1)${MENU}Top 10 job positions for all 6 years ${NORMAL}"
		 echo -e "${MENU}**${NUMBER} 2)${MENU}Top 10 job positions for the choosen year ${NORMAL}"
		 echo -e "${MENU}**${NUMBER} 3)${MENU}Top 10 certified job positions for all 6 years ${NORMAL}"
		 echo -e "${MENU}**${NUMBER} 4)${MENU}Top 10 certified job positions for choosen year ${NORMAL}"

	read n
	case $n in

	1) echo "Top 10 job positions for all 6 years :"

					hadoop fs -rmr /h1bvisa/qns5a.out
					hadoop jar '/home/hduser/JarFiles/5a.jar' qns5a /niit/h1b1visa /h1bvisa/qns5a.out | zenity --progress 						--title="Mapreduce Job Running" --pulsate --auto-close
					echo -e "\n Resulatant most popular top 10 job positions for H1B visa applications for all 6 years?\n"
					hadoop fs -cat /h1bvisa/qns5a.out/p*
					sleep 5
					;;
	2) echo "Top 10 job positions for the choosen year:"

					echo "Enter the year (2011,2012,2013,2014,2015,2016)"
        				read year
       					echo "You've selected ${year}"
					rm -r /home/hduser/Desktop/5a.txt
					hive -e "select * from h1bvisa.5aout where year = $year limit 10;"> /home/hduser/Desktop/5a.txt | zenity --progress --title="Hive Job Running" --pulsate --auto-close
					echo -e "\Resultant the most popular top 10 job positions for H1B visa applications for year $year?\n";
					cat /home/hduser/Desktop/5a.txt
					sleep 5
					;;

	3) echo "Top 10 certified job positions for all 6 years:"

					hadoop fs -rmr /h1bvisa/qns5b.out
					hadoop jar '/home/hduser/JarFiles/5b.jar' qns5b /niit/h1b1visa /h1bvisa/qns5b.out | zenity --progress --title="Mapreduce Job Running" --pulsate --auto-close
					echo -e "\nResultant the most popular top 10 certified job positions for H1B visa applications for all 6 years?\n";
					hadoop fs -cat /h1bvisa/qns5b.out/p*
					sleep 5
					;;
	4) echo "Top 10 certified job positions for choosen year:"
	
					
					echo "Enter the year (2011,2012,2013,2014,2015,2016)"
        				read year
       					echo "You've selected ${year}"
					rm -r /home/hduser/Desktop/5b.txt
					hive -e "select * from h1bvisa.5bout where year = $year limit 10; " > /home/hduser/Desktop/5b.txt | zenity --progress --title="Hive Job Running" --pulsate --auto-close
					echo -e "\nResultant the most popular top 10 certified job positions for H1B visa applications for year $year?\n";
					cat /home/hduser/Desktop/5b.txt
					sleep 5
	;;
 	*) echo "Please Select one among the option[1-4]";;
                esac
                show_menu;
                    ;;
	 8) clear;
        option_picked "6) Find the percentage and the count of each case status on total applications for each year.";
		 pig -x local '/home/hduser/Documents/H1bvisaOP/Pig/6output.pig/6.pig'

        show_menu;
        ;;
	9) clear;
        option_picked "7) the number of applications for each year ";
     	 				hadoop fs -rmr /h1bvisa/qns7.out
					hadoop jar '/home/hduser/JarFiles/qns7.jar' qns7 /niit/h1b1visa /h1bvisa/qns7.out | zenity --progress 						--title="Job Running" --pulsate --auto-close
					echo -e "\n Resulatant most popular top 10 job positions for H1B visa applications for all 6 years?\n"
					hadoop fs -cat /h1bvisa/qns7.out/p*
					sleep 5
        show_menu;
        ;;
	10) clear;
      option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
		echo -e "Enter the year(2011,2012,2013,2014,2015,2016)"
		read year
		echo -e "Enter the choice Full time/ Part time.(Y/N)"
		read var
        hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from h1bvisa.h1b_final where full_time_position = '$var' and year = $year and case_status in('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc limit 5;"
        show_menu;
        ;;
	11) clear;
        option_picked "9) Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?";
		rm -r /home/hduser/Desktop/9.txt
	hive -e "select * from h1bvisa.total1 limit 10;"> /home/hduser/Desktop/9.txt | zenity --progress --title="Hive Job Running" --pulsate --auto-close
		cat /home/hduser/Desktop/9.txt
		sleep 5
                show_menu;
                    ;;
	12) clear;
        option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000";
	rm -r /home/hduser/Desktop/10.txt
       	pig -x local '/home/hduser/Documents/H1bvisaOP/Pig/10output.pig/10.pig'> /home/hduser/Desktop/10.txt| zenity --progress --title="Pig Job 	Running" --pulsate --auto-close 
	cat /home/hduser/Desktop/10.txt
	sleep 5
        show_menu;
        ;;
	13) clear;
        option_picked "11) Export result for question no 10 to MySql database.";
	hadoop fs -rm -r -f /Pig/Qns10.out
	hadoop fs -mkdir -p /Pig/Qns10.out
	hadoop fs -put /home/hduser/Documents/H1bvisaOP/Pig/10output.pig/p* /Pig/Qns10.out
	mysql -u root -p -e 'create database if not exists h1bvisa_job_success_rate;use h1bvisa_job_success_rate;create table if not exists job_success_rate(job_title varchar(100),success_rate float,petitions int);';	

       sqoop export --connect jdbc:mysql://localhost/h1bvisa_job_success_rate --username root --password 'root' --table job_success_rate --update-mode  allowinsert --update-key job_title   --export-dir /Pig/Qns10.out/p*  --input-fields-terminated-by '\t' ;
	echo -e '\n\nDisplay contents from MySQL Database.\n\n'
	echo -e '\n The top 10 job positions that have  success rate more than 70% in petitions and total petitions filed more than 1000?\n\n'
	mysql -u root -p -e 'select * from h1bvisa_job_success_rate.job_success_rate limit 10';
        show_menu;
        ;;	

esac
fi
done

	
