paste -d " " <(head -1 accident.csv | tr "," "\n" | awk '{print "self."$1}') <(head -1 accident.csv | tr "," "\n" | awk '{print "= @"$1"@"}' | sed "s/@/'/g" | sed "s/ //g" | awk 'BEGIN{FS=OFS="_"} { $2 = toupper(substr($2,1,1)) substr($2,2) } 1' | sed "s@'@\"@g" | sed "s@_@@g")


head -1 accident.csv | tr "," "\n" | awk '{print "self."$1}' | awk '{print "{"$1"}"}'

paste -d " " <(head -1 rtd.csv | tr "," "\n" | awk '{print "@"$1"@:"}' | sed "s/@/\"/g") <(head -1 rtd.csv | tr "," "\n" | awk '{print "@TEXT@,"}' | sed "s/@/\"/g")