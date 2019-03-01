from datetime import datetime, date


def getDateObject(d, string=False):
	'''
	Input : '01/20/12' , output : python object of '01/20/2012'	
	:param d:
	:param string:
	'''

	# format is 05/01/12
	if not string:
		x = d.split('/', 2)
		d = '%s%s20%s' % (x[0], x[1], x[2])
		return datetime.strptime(d, '%m%d%Y')
	else:
		# format is 01APR2012
		try:
			date = datetime.strptime(d, '%d%b%Y')
		except:
			date = datetime.strptime(d, '%m/%d/%Y')
		return date


def getDaysDiff(d1, d2):
	d = d1 - d2
	return d.days

def getDateFromUnix(d, obj=False):
	obj = datetime.utcfromtimestamp( int(d) )
	if obj:
		return obj
	else:
		return obj.strftime('%Y-%m-%d')