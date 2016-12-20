with open('result.csv', 'r') as result, open("testdata.csv", 'r') as testdata:
    k = 0.0
    b = 0.0
    result.readline()
    testdata.readline()
    while 1:
        b = b+1
        r1 = result.readline()
        if  r1 == '':
            break
        else:
            r = r1.strip().split(" ")
            t = testdata.readline().strip().split(",")
            print  t[23],r[1:],t[23] in r[1:]
            if t[23] in r[1:]:
                k = k+1
    print "Accuracy:",k/b
