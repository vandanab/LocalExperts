def figure_latex(list_of_figs):
  for i in list_of_figs:
    title = i["t"]
    path = i["p"]
    print "\\begin{figure}[!htb]"
    print "\centering"
    print "\includegraphics[scale=0.5]{",path,"}"
    print "\caption{",title,"}"
    print "\end{figure}"

def generate_table_latex(list_of_files):
  for i in list_of_files:
    title = i["t"]
    fn = i["f"]
    f = open(fn, "r")
    csvreader = csv.csvreader(f)
    c = 0
    print "\\begin{table*}[ht]"
    print "\centering"
    print title
    print "\\begin{tabular}{|l|l|}"
    print "\hline"
    print "\\textbf{Twitter Handle} & \\textbf{Description}\\\\"
    print "\hline"
    for row in csvreader:
      if c > 10:
        break
      print row[0]," &\\textit{ \"",row[2],"\"}\\\\"
      c += 1
    f.close()
    print "\hline"
    print "\end{tabular}"
    print "\end{table*}"