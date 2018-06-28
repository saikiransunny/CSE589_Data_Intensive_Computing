#!/usr/bin/python

import sys
import re
import string

# input comes from STDIN (standard input)
for text in sys.stdin:
    paras = text.split("\n")
    for line in paras:
        line = line.strip().lower()
        line = re.sub(r'[^\w\s]','',line)
        stopwords = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be',
     'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves',
      'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 
      'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves',
       'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which',
        'those', 'i', 'after', 'few', 'whom', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than', 'rt']
        # split the line into words
        words = line.split()
        filtered_line = [word for word in words if word not in stopwords]

        # increase counters
        for index1 in list(range(0,len(filtered_line)-1)):
            for index2 in list(range(index1,len(filtered_line)-1)) :
                if filtered_line[index1] != filtered_line[index2]:
                    print '%s\t%s' % (str(filtered_line[index1]+','+filtered_line[index2]), 1)