#!/usr/bin/env python3

import xml.etree.ElementTree

def getQPS ():
    """
    Extract gps and QMpH from bsbm result XML
    """
    lines = {}
    qmps = {}

    for prefix in ("", "-gc", "-nv"):
        execSet = []
        qmph = []
        for i in range(1,8):
            execName = "quit" + prefix + "-0" + str(i)
            fileName = execName + "-logs/" + execName + ".xml"

            e = xml.etree.ElementTree.parse(fileName).getroot()
            subset = {}
            qmph.append(float(e.find('querymix').find('qmph').text))
            for qu in e.find('queries').findall('query'):
                #print (qu.get('nr'))
                nr = qu.get('nr')
                if qu.find('qps') != None:
                    #print(qu.find('qps').text)
                    subset[int(nr)] = float(qu.find('qps').text)
            execSet.append(subset)
        #print (execSet)
        evalset = {}
        for i in range(1,15):
            if (i == 8):
                continue
            summe = 0
            count = 0
            for a in execSet :
                summe += a[i]
                count += 1
            avg = float(summe)/float(count)

            devSum = float(0)
            for a in execSet :
                devSum += (float(a[i])-avg)*(float(a[i])-avg)
            dev = devSum/float(count)
            evalset[i] = (avg, dev)
            print (avg, dev)
        lines[prefix] = evalset

        qmphsumme = 0
        qmphcount = 0
        for a in qmph:
            qmphsumme += a
            qmphcount += 1
        qmphavg = float(qmphsumme)/float(qmphcount)
        qmphdevSum = float(0)
        for a in qmph :
            qmphdevSum += (float(a)-qmphavg)*(float(a)-qmphavg)
        qmphdev = qmphdevSum/float(qmphcount)

        print("qmph", prefix, qmphavg, qmphdev)

    #print(lines)


if __name__ == "__main__":

    getQPS()
