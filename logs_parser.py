#!/usr/local/bin/python
import heapq, colored_traceback
from datetime import datetime

# logsFile = 'apiSocial_SubscriptionResourceV3SIT_logs.txt'
# fileToWrite = 'parsed_logs.txt'

def main(logsFile, fileToWrite, k):
	lineNumber_timeDiffs = findTimeDiffs(logsFile)
	lineNumber_timeDiffs_subset = findTopKTimeDiffs(lineNumber_timeDiffs, k)
	writeTopKTimeDiffLogs(fileToWrite, logsFile, lineNumber_timeDiffs_subset)


def findTimeDiffs(logsFile):
	with open(logsFile) as f:
		lineNumber_timeDiffs = {}
		prevLine = f.readline()
		prevTime = parseDate(prevLine[:12])
		i = 0
		for i, currentLine in enumerate(f):
			currentTime = parseDate(currentLine[:12])
			if(currentTime is not None) and (prevTime is not None):
				timeDiff = currentTime - prevTime
				timeDiffSec = timeDiff.total_seconds()
				lineNumber_timeDiffs[i] = timeDiffSec
			if(currentTime is not None):
				prevTime = currentTime
		return lineNumber_timeDiffs


def parseDate(substring):
	#time = None
	try:
		time = datetime.strptime(substring, '%H:%M:%S.%f')
	except ValueError:
		return None
	else: 
		return time


def findTopKTimeDiffs(dictnry, k):
	topKKeys = heapq.nlargest(k, dictnry, key = dictnry.get)
	dictnrySubset = dict([(key, dictnry[key]) for key in topKKeys])
	return dictnrySubset


def writeTopKTimeDiffLogs(fileToWrite, logsFile, dictnrySubset):
	with open(fileToWrite, 'w') as result:
		with open(logsFile) as f:
			for i, currentLine in enumerate(f):
				if(i in dictnrySubset):
					result.write(dictnrySubset[i] + ' ' + currentLine + '\n')



if __name__ == '__main__':
	colored_traceback.add_hook()
	main('apiSocial_SubscriptionResourceV3SIT_logs.txt', 'parsed_logs.txt', 10)
